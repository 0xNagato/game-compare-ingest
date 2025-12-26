-- 0001_init.sql
-- GameCompare base schema (idempotent). Extensions, core tables, constraints, and search indexes.

DO $$ BEGIN
  CREATE EXTENSION IF NOT EXISTS citext;
  CREATE EXTENSION IF NOT EXISTS pg_trgm;
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'Extension create skipped: %', SQLERRM;
END $$;

-- NOTE: Original schema used a dedicated "gamecompare" namespace. Project has migrated
-- to a single public schema. All objects below are created directly in public.

-- Currencies
CREATE TABLE IF NOT EXISTS currencies (
  id BIGSERIAL PRIMARY KEY,
  code citext NOT NULL UNIQUE,
  name text NOT NULL,
  minor_unit smallint NOT NULL DEFAULT 2
);

-- Countries
CREATE TABLE IF NOT EXISTS countries (
  id BIGSERIAL PRIMARY KEY,
  code2 char(2) NOT NULL UNIQUE,
  name text NOT NULL,
  currency_id BIGINT NOT NULL REFERENCES currencies(id)
);

-- Jurisdictions
CREATE TABLE IF NOT EXISTS jurisdictions (
  id BIGSERIAL PRIMARY KEY,
  country_id BIGINT NOT NULL REFERENCES countries(id) ON DELETE CASCADE,
  region_code text
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_jurisdictions_country_national
  ON jurisdictions(country_id)
  WHERE region_code IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_jurisdictions_country_region
  ON jurisdictions(country_id, region_code)
  WHERE region_code IS NOT NULL;

-- Tax rules
CREATE TABLE IF NOT EXISTS tax_rules (
  id BIGSERIAL PRIMARY KEY,
  jurisdiction_id BIGINT NOT NULL REFERENCES jurisdictions(id) ON DELETE CASCADE,
  effective_from timestamptz NOT NULL,
  effective_to timestamptz,
  rate_basis_points integer NOT NULL CHECK (rate_basis_points >= 0),
  inclusive boolean NOT NULL DEFAULT true
);
CREATE INDEX IF NOT EXISTS idx_tax_rules_jurisdiction_from_to
  ON tax_rules (jurisdiction_id, effective_from, COALESCE(effective_to, 'infinity'::timestamptz));

-- Products (cold) + subtypes
CREATE TABLE IF NOT EXISTS products (
  id BIGSERIAL PRIMARY KEY,
  slug citext UNIQUE,
  kind text NOT NULL CHECK (kind IN ('software','hardware')),
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_products_slug_trgm ON products USING gin (slug ext.gin_trgm_ops);

CREATE TABLE IF NOT EXISTS software (
  product_id BIGINT PRIMARY KEY REFERENCES products(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS hardware (
  product_id BIGINT PRIMARY KEY REFERENCES products(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS video_game_titles (
  id BIGSERIAL PRIMARY KEY,
  product_id BIGINT NOT NULL UNIQUE REFERENCES software(product_id) ON DELETE CASCADE,
  name text NOT NULL,
  slug citext UNIQUE
);
CREATE INDEX IF NOT EXISTS idx_video_game_titles_name_trgm ON video_game_titles USING gin (name ext.gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_video_game_titles_slug_trgm ON video_game_titles USING gin (slug ext.gin_trgm_ops);

CREATE TABLE IF NOT EXISTS platforms (
  id BIGSERIAL PRIMARY KEY,
  name text NOT NULL UNIQUE,
  slug citext UNIQUE
);
CREATE INDEX IF NOT EXISTS idx_platforms_name_trgm ON platforms USING gin (name ext.gin_trgm_ops);

CREATE TABLE IF NOT EXISTS video_games (
  id BIGSERIAL PRIMARY KEY,
  title_id BIGINT NOT NULL REFERENCES video_game_titles(id) ON DELETE CASCADE,
  platform_id BIGINT NOT NULL REFERENCES platforms(id),
  edition text,
  -- ratings/genres are part of base schema so we don't need follow-up migrations
  average_rating real,
  rating_count bigint,
  rating_updated_at timestamptz,
  genres text[]
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_video_games_title_platform_null
  ON video_games(title_id, platform_id)
  WHERE edition IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_video_games_title_platform_edition
  ON video_games(title_id, platform_id, edition)
  WHERE edition IS NOT NULL;

CREATE TABLE IF NOT EXISTS game_consoles (
  id BIGSERIAL PRIMARY KEY,
  product_id BIGINT NOT NULL UNIQUE REFERENCES hardware(product_id) ON DELETE CASCADE,
  model text NOT NULL,
  variant text
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_game_consoles_product_model_null
  ON game_consoles(product_id, model)
  WHERE variant IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_game_consoles_product_model_variant
  ON game_consoles(product_id, model, variant)
  WHERE variant IS NOT NULL;

-- Commerce layer
CREATE TABLE IF NOT EXISTS sellables (
  id BIGSERIAL PRIMARY KEY,
  kind text NOT NULL CHECK (kind IN ('software','hardware')),
  product_id BIGINT NOT NULL REFERENCES products(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS retailers (
  id BIGSERIAL PRIMARY KEY,
  name text NOT NULL UNIQUE,
  slug citext UNIQUE
);
CREATE INDEX IF NOT EXISTS idx_retailers_name_trgm ON retailers USING gin (name ext.gin_trgm_ops);

CREATE TABLE IF NOT EXISTS offers (
  id BIGSERIAL PRIMARY KEY,
  sellable_id BIGINT NOT NULL REFERENCES sellables(id) ON DELETE CASCADE,
  retailer_id BIGINT NOT NULL REFERENCES retailers(id) ON DELETE CASCADE,
  sku text
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_offers_sellable_retailer_null
  ON offers(sellable_id, retailer_id)
  WHERE sku IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_offers_sellable_retailer_sku
  ON offers(sellable_id, retailer_id, sku)
  WHERE sku IS NOT NULL;

CREATE TABLE IF NOT EXISTS offer_jurisdictions (
  id BIGSERIAL PRIMARY KEY,
  offer_id BIGINT NOT NULL REFERENCES offers(id) ON DELETE CASCADE,
  jurisdiction_id BIGINT NOT NULL REFERENCES jurisdictions(id) ON DELETE CASCADE,
  currency_id BIGINT NOT NULL REFERENCES currencies(id),
  UNIQUE (offer_id, jurisdiction_id)
);
CREATE INDEX IF NOT EXISTS idx_offer_jurisdictions_offer ON offer_jurisdictions (offer_id);
CREATE INDEX IF NOT EXISTS idx_offer_jurisdictions_jurisdiction ON offer_jurisdictions (jurisdiction_id);

-- Providers chain
CREATE TABLE IF NOT EXISTS providers (
  id BIGSERIAL PRIMARY KEY,
  name text NOT NULL UNIQUE,
  kind text NOT NULL CHECK (kind IN ('retailer_api','catalog','storefront','media')),
  slug citext UNIQUE
);
CREATE INDEX IF NOT EXISTS idx_providers_name_trgm ON providers USING gin (name ext.gin_trgm_ops);

CREATE TABLE IF NOT EXISTS retailer_providers (
  id BIGSERIAL PRIMARY KEY,
  retailer_id BIGINT NOT NULL REFERENCES retailers(id) ON DELETE CASCADE,
  provider_id BIGINT NOT NULL REFERENCES providers(id) ON DELETE CASCADE,
  credentials jsonb,
  jurisdiction_id BIGINT REFERENCES jurisdictions(id),
  UNIQUE (retailer_id, provider_id)
);

CREATE TABLE IF NOT EXISTS provider_items (
  id BIGSERIAL PRIMARY KEY,
  provider_id BIGINT NOT NULL REFERENCES providers(id) ON DELETE CASCADE,
  external_item_id text NOT NULL CHECK (length(external_item_id) > 0),
  payload jsonb,
  UNIQUE (provider_id, external_item_id)
);
CREATE INDEX IF NOT EXISTS idx_provider_items_provider ON provider_items (provider_id);
CREATE INDEX IF NOT EXISTS idx_provider_items_external_trgm ON provider_items USING gin (external_item_id ext.gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_provider_items_payload_name_trgm ON provider_items USING gin ((payload->>'name') ext.gin_trgm_ops);

CREATE TABLE IF NOT EXISTS provider_offers (
  id BIGSERIAL PRIMARY KEY,
  provider_item_id BIGINT NOT NULL REFERENCES provider_items(id) ON DELETE CASCADE,
  offer_id BIGINT NOT NULL REFERENCES offers(id) ON DELETE CASCADE,
  confidence real,
  UNIQUE (provider_item_id, offer_id)
);
CREATE INDEX IF NOT EXISTS idx_provider_offers_offer ON provider_offers (offer_id);

CREATE TABLE IF NOT EXISTS provider_ingest_runs (
  id BIGSERIAL PRIMARY KEY,
  provider_id BIGINT NOT NULL REFERENCES providers(id) ON DELETE CASCADE,
  started_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz,
  status text NOT NULL CHECK (status IN ('queued','running','success','error')),
  meta jsonb
);

-- Optional link to Spatie media (external table untouched)
CREATE TABLE IF NOT EXISTS provider_media_links (
  id BIGSERIAL PRIMARY KEY,
  provider_item_id BIGINT NOT NULL REFERENCES provider_items(id) ON DELETE CASCADE,
  media_id BIGINT,
  url text,
  CHECK (media_id IS NOT NULL OR url IS NOT NULL)
);
CREATE INDEX IF NOT EXISTS idx_provider_media_links_item ON provider_media_links (provider_item_id);

-- Users & Alerts
CREATE TABLE IF NOT EXISTS users (
  id BIGSERIAL PRIMARY KEY,
  email citext UNIQUE,
  name text
);

CREATE TABLE IF NOT EXISTS alerts (
  id BIGSERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  offer_jurisdiction_id BIGINT NOT NULL REFERENCES offer_jurisdictions(id) ON DELETE CASCADE,
  op text NOT NULL CHECK (op IN ('lt','lte','gt','gte')),
  threshold_minor BIGINT NOT NULL,
  active boolean NOT NULL DEFAULT true,
  last_triggered_at timestamptz,
  settings jsonb,
  UNIQUE (user_id, offer_jurisdiction_id, op)
);
CREATE INDEX IF NOT EXISTS idx_alerts_active_oj ON alerts (offer_jurisdiction_id) WHERE active;
CREATE INDEX IF NOT EXISTS idx_alerts_active_user ON alerts (user_id) WHERE active;

-- Prices (parent partitioned table)
CREATE TABLE IF NOT EXISTS prices (
  id BIGSERIAL,
  offer_jurisdiction_id BIGINT NOT NULL REFERENCES offer_jurisdictions(id) ON DELETE CASCADE,
  provider_item_id BIGINT REFERENCES provider_items(id) ON DELETE SET NULL,
  recorded_at timestamptz NOT NULL,
  amount_minor BIGINT NOT NULL,
  tax_inclusive boolean NOT NULL DEFAULT true,
  fx_minor_per_unit BIGINT,
  btc_sats_per_unit BIGINT,
  meta jsonb,
  PRIMARY KEY (id, recorded_at)
) PARTITION BY RANGE (recorded_at);
CREATE INDEX IF NOT EXISTS idx_prices_parent_oj_recorded_at ON ONLY prices (offer_jurisdiction_id, recorded_at);
CREATE INDEX IF NOT EXISTS idx_prices_parent_recorded_at ON ONLY prices (recorded_at);

-- Hot read table
CREATE TABLE IF NOT EXISTS current_price (
  offer_jurisdiction_id BIGINT PRIMARY KEY REFERENCES offer_jurisdictions(id) ON DELETE CASCADE,
  amount_minor BIGINT NOT NULL,
  recorded_at timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_current_price_recorded_at ON current_price (recorded_at);
