-- 0001_full_consolidated_schema.sql
-- Consolidated idempotent schema creation (corrected)
-- NOTE: This file supersedes earlier numbered squashed migrations by providing
-- a dependency-ordered, idempotent build of the entire schema. Safe to re-run.

-- =============================
-- EXTENSIONS & ENUMS
-- =============================
-- Ensure schemas exist first
CREATE SCHEMA IF NOT EXISTS ext;
DO $$ BEGIN CREATE EXTENSION IF NOT EXISTS citext WITH SCHEMA public; EXCEPTION WHEN OTHERS THEN NULL; END $$;
DO $$ BEGIN CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA ext; EXCEPTION WHEN OTHERS THEN NULL; END $$;
DO $$ BEGIN CREATE TYPE cmp_op AS ENUM ('above','below'); EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN CREATE TYPE sellable_kind AS ENUM ('software','hardware'); EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- =============================
-- LOOKUPS
-- =============================
CREATE TABLE IF NOT EXISTS currencies (
  id bigserial PRIMARY KEY,
  code text NOT NULL UNIQUE,
  name text NOT NULL,
  minor_unit smallint NOT NULL
);
ALTER TABLE currencies ADD COLUMN IF NOT EXISTS minor_unit smallint;
DO $$
BEGIN
  BEGIN
    UPDATE currencies SET minor_unit = 2 WHERE minor_unit IS NULL;
    ALTER TABLE currencies ALTER COLUMN minor_unit SET NOT NULL;
  EXCEPTION WHEN undefined_column THEN
    NULL;
  END;
EXCEPTION WHEN undefined_table THEN NULL;
END $$;
CREATE TABLE IF NOT EXISTS countries (
  id bigserial PRIMARY KEY,
  iso2 char(2) NOT NULL UNIQUE,
  iso3 char(3) NOT NULL UNIQUE,
  name text NOT NULL,
  currency_id bigint REFERENCES currencies(id)
);
CREATE TABLE IF NOT EXISTS country_iso_map (
  code2 char(2) PRIMARY KEY,
  iso2 char(2) NOT NULL,
  iso3 char(3) NOT NULL
);
INSERT INTO country_iso_map(code2, iso2, iso3) VALUES
  ('US','US','USA'),('GB','GB','GBR'),('CA','CA','CAN'),('DE','DE','DEU'),('FR','FR','FRA'),('ES','ES','ESP'),('IT','IT','ITA'),('NL','NL','NLD'),('BE','BE','BEL'),('PT','PT','PRT'),('IE','IE','IRL'),('FI','FI','FIN'),('GR','GR','GRC'),('AT','AT','AUT'),('SE','SE','SWE'),('NO','NO','NOR'),('DK','DK','DNK'),('CH','CH','CHE'),('JP','JP','JPN'),('KR','KR','KOR'),('AU','AU','AUS'),('NZ','NZ','NZL'),('BR','BR','BRA'),('MX','MX','MEX'),('AR','AR','ARG'),('ZA','ZA','ZAF'),('TR','TR','TUR'),('UA','UA','UKR'),('HK','HK','HKG'),('TW','TW','TWN'),('SG','SG','SGP'),('MY','MY','MYS'),('TH','TH','THA'),('ID','ID','IDN'),('PH','PH','PHL'),('VN','VN','VNM'),('CN','CN','CHN'),('IL','IL','ISR'),('CL','CL','CHL'),('CO','CO','COL'),('PE','PE','PER'),('UY','UY','URY'),('KZ','KZ','KAZ'),('SA','SA','SAU'),('AE','AE','ARE'),('EG','EG','EGY')
ON CONFLICT (code2) DO UPDATE SET iso2=EXCLUDED.iso2, iso3=EXCLUDED.iso3;
CREATE OR REPLACE FUNCTION countries_iso_autofill()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.code2 IS NOT NULL THEN
    IF NEW.iso2 IS NULL THEN
      SELECT m.iso2 INTO NEW.iso2 FROM country_iso_map m WHERE m.code2 = NEW.code2;
    END IF;
    IF NEW.iso3 IS NULL THEN
      SELECT m.iso3 INTO NEW.iso3 FROM country_iso_map m WHERE m.code2 = NEW.code2;
    END IF;
  END IF;
  RETURN NEW;
END $$;
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'countries_iso_autofill_trg') THEN
    CREATE TRIGGER countries_iso_autofill_trg
    BEFORE INSERT ON countries
    FOR EACH ROW EXECUTE FUNCTION countries_iso_autofill();
  END IF;
END $$;
CREATE TABLE IF NOT EXISTS jurisdictions (
  id bigserial PRIMARY KEY,
  country_id bigint NOT NULL REFERENCES countries(id) ON DELETE CASCADE,
  region_code text,
  region_key text GENERATED ALWAYS AS (coalesce(region_code,'')) STORED
);
DO $$ BEGIN IF NOT EXISTS (
  SELECT 1 FROM pg_constraint WHERE conname='jurisdictions_country_region_key_uq' AND conrelid='public.jurisdictions'::regclass
) THEN ALTER TABLE jurisdictions ADD CONSTRAINT jurisdictions_country_region_key_uq UNIQUE (country_id, region_key); END IF; END $$;

CREATE TABLE IF NOT EXISTS tax_rules (
  id bigserial PRIMARY KEY,
  jurisdiction_id bigint NOT NULL REFERENCES jurisdictions(id) ON DELETE CASCADE,
  effective_from date NOT NULL,
  effective_to date,
  rate_basis_points integer NOT NULL,
  inclusive boolean NOT NULL DEFAULT true,
  notes text,
  UNIQUE (jurisdiction_id, effective_from)
);

-- =============================
-- USERS
-- =============================
CREATE TABLE IF NOT EXISTS users (
  id bigserial PRIMARY KEY,
  email citext NOT NULL UNIQUE,
  name text,
  timezone text NOT NULL DEFAULT 'UTC',
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

-- =============================
-- PRODUCTS & SPLITS
-- =============================
CREATE TABLE IF NOT EXISTS products (
  id bigserial PRIMARY KEY,
  slug citext NOT NULL UNIQUE,
  name text NOT NULL,
  category text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS software (
  product_id bigint PRIMARY KEY REFERENCES products(id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS hardware (
  product_id bigint PRIMARY KEY REFERENCES products(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS products_slug_trgm_idx ON products USING gin ((slug::text) ext.gin_trgm_ops);
CREATE INDEX IF NOT EXISTS products_name_trgm_idx ON products USING gin ((name) ext.gin_trgm_ops);

-- =============================
-- PLATFORMS
-- =============================
CREATE TABLE IF NOT EXISTS platforms (
  id bigserial PRIMARY KEY,
  code text UNIQUE,
  name text NOT NULL UNIQUE,
  family text
);
CREATE INDEX IF NOT EXISTS platforms_name_trgm_idx ON platforms USING gin ((name) ext.gin_trgm_ops);

-- =============================
-- RETAILERS & PROVIDERS
-- =============================
CREATE TABLE IF NOT EXISTS retailers (
  id bigserial PRIMARY KEY,
  slug citext UNIQUE,
  name text NOT NULL
);
CREATE TABLE IF NOT EXISTS providers (
  id bigserial PRIMARY KEY,
  slug citext UNIQUE,
  name text NOT NULL,
  -- kind added to align with ingestion pipeline expectations (retailer_api|catalog|storefront|media)
  kind text,
  api_source text,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS retailer_providers (
  id bigserial PRIMARY KEY,
  retailer_id bigint NOT NULL REFERENCES retailers(id) ON DELETE CASCADE,
  provider_id bigint NOT NULL REFERENCES providers(id) ON DELETE CASCADE,
  UNIQUE (retailer_id, provider_id)
);

-- =============================
-- VIDEO GAME TITLES & GAMES
-- =============================
CREATE TABLE IF NOT EXISTS video_games (
  id bigserial PRIMARY KEY,
  title_id bigint NOT NULL,
  platform_id bigint NOT NULL REFERENCES platforms(id),
  edition text,
  sellable_id bigint,
  slug citext UNIQUE,
  release_date date,
  metadata jsonb,
  average_rating real,
  rating_count bigint,
  rating_updated_at timestamptz,
  genres text[],
  display_title text,
  developer text,
  region_codes text[],
  popularity_score numeric NOT NULL DEFAULT 0,
  rating numeric NOT NULL DEFAULT 0,
  synopsis text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_video_games_title_platform_null
  ON video_games (title_id, platform_id)
  WHERE edition IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_video_games_title_platform_edition
  ON video_games (title_id, platform_id, (coalesce(edition,'')))
  WHERE edition IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_video_games_title_id ON video_games (title_id);

CREATE UNIQUE INDEX IF NOT EXISTS uq_vg_sellable_platform_edition
  ON video_games (sellable_id, platform_id, (coalesce(edition,'')))
  WHERE sellable_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS video_game_titles (
  id bigserial PRIMARY KEY,
  product_id bigint UNIQUE REFERENCES software(product_id) ON DELETE CASCADE,
  video_game_id bigint REFERENCES video_games(id),
  title text NOT NULL,
  normalized_title text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
DO $$ BEGIN
  BEGIN
    ALTER TABLE public.video_game_titles ALTER COLUMN product_id DROP NOT NULL;
  EXCEPTION WHEN undefined_column THEN
    NULL;
  END;
EXCEPTION WHEN undefined_table THEN NULL; END $$;
CREATE INDEX IF NOT EXISTS titles_title_trgm_idx ON video_game_titles USING gin ((title) ext.gin_trgm_ops);

-- =============================
-- GAME CONSOLES
-- =============================
CREATE TABLE IF NOT EXISTS game_consoles (
  id bigserial PRIMARY KEY,
  product_id bigint NOT NULL REFERENCES hardware(product_id) ON DELETE CASCADE,
  model text NOT NULL,
  variant text,
  slug citext UNIQUE,
  release_date date,
  metadata jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
-- Use a UNIQUE INDEX for expression-based uniqueness (constraints cannot include expressions)
CREATE UNIQUE INDEX IF NOT EXISTS uq_game_consoles_product_model_variant
  ON game_consoles (product_id, model, (coalesce(variant,'')));
CREATE INDEX IF NOT EXISTS consoles_model_trgm_idx ON game_consoles USING gin ((model) ext.gin_trgm_ops);

-- =============================
-- SELLABLES & OFFERS
-- =============================
CREATE TABLE IF NOT EXISTS sellables (
  id bigserial PRIMARY KEY,
  kind sellable_kind NOT NULL,
  software_title_id bigint REFERENCES video_game_titles(id) ON DELETE CASCADE,
  console_id bigint REFERENCES game_consoles(id) ON DELETE CASCADE,
  CHECK ((kind='software' AND software_title_id IS NOT NULL AND console_id IS NULL) OR (kind='hardware' AND console_id IS NOT NULL AND software_title_id IS NULL))
);
CREATE TABLE IF NOT EXISTS offers (
  id bigserial PRIMARY KEY,
  sellable_id bigint NOT NULL REFERENCES sellables(id) ON DELETE CASCADE,
  retailer_id bigint NOT NULL REFERENCES retailers(id) ON DELETE CASCADE,
  sku text,
  is_active boolean NOT NULL DEFAULT true,
  metadata jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS offers_active_idx ON offers (sellable_id) WHERE is_active;
-- Use a UNIQUE INDEX for expression-based uniqueness (constraints cannot include expressions)
CREATE UNIQUE INDEX IF NOT EXISTS uq_offers_sellable_retailer_sku
  ON offers (sellable_id, retailer_id, (coalesce(sku,'')));

-- =============================
-- OFFER JURISDICTIONS
-- =============================
CREATE TABLE IF NOT EXISTS offer_jurisdictions (
  id bigserial PRIMARY KEY,
  offer_id bigint NOT NULL REFERENCES offers(id) ON DELETE CASCADE,
  jurisdiction_id bigint NOT NULL REFERENCES jurisdictions(id) ON DELETE CASCADE,
  currency_id bigint NOT NULL REFERENCES currencies(id)
);
DO $$ BEGIN IF NOT EXISTS (
  SELECT 1 FROM pg_constraint WHERE conname='offer_jurisdictions_offer_jurisdiction_uq' AND conrelid='public.offer_jurisdictions'::regclass
) THEN ALTER TABLE offer_jurisdictions ADD CONSTRAINT offer_jurisdictions_offer_jurisdiction_uq UNIQUE (offer_id, jurisdiction_id); END IF; END $$;

-- =============================
-- PROVIDER OBJECTS
-- =============================
CREATE TABLE IF NOT EXISTS provider_items (
  id bigserial PRIMARY KEY,
  provider_id bigint NOT NULL REFERENCES providers(id) ON DELETE CASCADE,
  external_id text,
  kind text,
  metadata jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_provider_items_provider_external ON provider_items(provider_id, external_id);
CREATE TABLE IF NOT EXISTS provider_offers (
  id bigserial PRIMARY KEY,
  provider_item_id bigint NOT NULL REFERENCES provider_items(id) ON DELETE CASCADE,
  offer_id bigint REFERENCES offers(id) ON DELETE SET NULL,
  metadata jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS provider_ingest_runs (
  id bigserial PRIMARY KEY,
  provider_id bigint NOT NULL REFERENCES providers(id) ON DELETE CASCADE,
  started_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz,
  status text,
  stats jsonb
);

-- =============================
-- PRICES (PARTITIONED)
-- =============================
CREATE TABLE IF NOT EXISTS prices (
  id bigserial,
  offer_jurisdiction_id bigint NOT NULL REFERENCES offer_jurisdictions(id) ON DELETE CASCADE,
  provider_item_id bigint REFERENCES provider_items(id) ON DELETE SET NULL,
  recorded_at timestamptz NOT NULL,
  amount_minor bigint NOT NULL CHECK (amount_minor >= 0),
  tax_inclusive boolean NOT NULL,
  fx_minor_per_unit bigint,
  btc_sats_per_unit bigint,
  meta jsonb,
  PRIMARY KEY (id, recorded_at)
) PARTITION BY RANGE(recorded_at);
CREATE INDEX IF NOT EXISTS prices_series_idx ON ONLY prices (offer_jurisdiction_id, recorded_at);
CREATE INDEX IF NOT EXISTS prices_recorded_at_brin ON ONLY prices USING brin (recorded_at);
CREATE OR REPLACE FUNCTION ensure_price_partition(ts timestamptz) RETURNS void LANGUAGE plpgsql AS $$ DECLARE start_month date := date_trunc('month', ts)::date; next_month date := (date_trunc('month', ts) + interval '1 month')::date; part_name text := format('prices_%s', to_char(start_month,'YYYY_MM')); BEGIN IF to_regclass(part_name) IS NULL THEN EXECUTE format('CREATE TABLE %I PARTITION OF prices FOR VALUES FROM (%L) TO (%L);', part_name, start_month, next_month); EXECUTE format('CREATE INDEX IF NOT EXISTS %I_oj_recorded_at ON %I (offer_jurisdiction_id, recorded_at);', part_name, part_name); EXECUTE format('CREATE INDEX IF NOT EXISTS %I_recorded_at ON %I (recorded_at);', part_name, part_name); EXECUTE format('CREATE INDEX IF NOT EXISTS %I_recorded_at_brin ON %I USING brin (recorded_at);', part_name, part_name); EXECUTE format('CREATE INDEX IF NOT EXISTS %I_oj_recorded_at_desc ON %I (offer_jurisdiction_id, recorded_at DESC) INCLUDE (amount_minor);', part_name, part_name); END IF; END $$;
DO $$ DECLARE i int; base date := date_trunc('month', now())::date - interval '12 months'; future int := 18; BEGIN FOR i IN 0..future LOOP PERFORM ensure_price_partition((base + (i || ' months')::interval)::timestamptz); END LOOP; END $$;

-- =============================
-- CURRENT PRICE
-- =============================
CREATE TABLE IF NOT EXISTS current_price (
  offer_jurisdiction_id bigint PRIMARY KEY REFERENCES offer_jurisdictions(id) ON DELETE CASCADE,
  amount_minor bigint NOT NULL,
  recorded_at timestamptz NOT NULL
);

-- =============================
-- ALERTS
-- =============================
CREATE TABLE IF NOT EXISTS alerts (
  id bigserial PRIMARY KEY,
  user_id bigint NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  product_id bigint NOT NULL REFERENCES products(id) ON DELETE CASCADE,
  region_code varchar(2) NOT NULL,
  threshold_btc numeric(18,8) NOT NULL,
  comparison_operator varchar(255) NOT NULL DEFAULT 'below',
  channel varchar(255) NOT NULL,
  is_active boolean NOT NULL DEFAULT true,
  last_triggered_at timestamptz,
  settings jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (comparison_operator IN ('below','above')),
  CHECK (channel IN ('email','discord'))
);
CREATE INDEX IF NOT EXISTS alerts_product_id_region_code_is_active_index
  ON alerts (product_id, region_code, is_active);
CREATE INDEX IF NOT EXISTS alerts_user_id_is_active_index
  ON alerts (user_id, is_active);

-- =============================
-- GAME SOURCES & PROVIDERS
-- =============================
CREATE TABLE IF NOT EXISTS video_game_sources (
  id bigserial PRIMARY KEY,
  video_game_id bigint REFERENCES video_games(id) ON DELETE SET NULL,
  provider varchar(64),
  provider_game_id varchar(128),
  provider_slug varchar(128),
  provider_hash varchar(64),
  payload json,
  links json,
  media json,
  synced_at timestamptz,
  created_at timestamptz,
  updated_at timestamptz,
  provider_key varchar(64),
  display_name varchar(255),
  category varchar(64),
  slug varchar(255),
  metadata json
);
CREATE UNIQUE INDEX IF NOT EXISTS video_game_sources_provider_key_uq
  ON video_game_sources(provider_key);
CREATE UNIQUE INDEX IF NOT EXISTS video_game_sources_provider_game_uq
  ON video_game_sources(provider, provider_game_id);
CREATE INDEX IF NOT EXISTS video_game_sources_slug_idx ON video_game_sources(slug);
CREATE INDEX IF NOT EXISTS video_game_sources_video_game_provider_idx
  ON video_game_sources(video_game_id, provider);

CREATE TABLE IF NOT EXISTS game_providers (
  id bigserial PRIMARY KEY,
  provider_key varchar(64) NOT NULL,
  name varchar(255),
  slug varchar(255),
  website_url varchar(255),
  providable_type varchar(255),
  providable_id bigint,
  credentials json,
  metadata json,
  last_synced_at timestamptz,
  refreshed_at timestamptz,
  created_at timestamptz,
  updated_at timestamptz,
  video_game_source_id bigint REFERENCES video_game_sources(id) ON DELETE SET NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS game_providers_providable_provider_key_uq
  ON game_providers(providable_type, providable_id, provider_key);
CREATE INDEX IF NOT EXISTS game_providers_providable_idx
  ON game_providers(providable_type, providable_id);
CREATE INDEX IF NOT EXISTS game_providers_provider_key_idx ON game_providers(provider_key);
CREATE INDEX IF NOT EXISTS game_providers_slug_idx ON game_providers(slug);
CREATE INDEX IF NOT EXISTS game_providers_vgs_idx ON game_providers(video_game_source_id);

-- =============================
-- MEDIA TABLES
-- =============================
CREATE TABLE IF NOT EXISTS game_images (
  id bigserial PRIMARY KEY,
  game_provider_id bigint NOT NULL REFERENCES game_providers(id) ON DELETE CASCADE,
  image_key varchar(255),
  url varchar(255) NOT NULL,
  mime_type varchar(255),
  width integer,
  height integer,
  rank integer NOT NULL DEFAULT 0,
  caption varchar(255),
  variants json,
  metadata json,
  created_at timestamptz,
  updated_at timestamptz,
  media_id bigint,
  storage_disk varchar(255),
  storage_path varchar(255),
  provider_item_id varchar(255),
  video_game_source_id bigint REFERENCES video_game_sources(id) ON DELETE SET NULL,
  provider_payload json,
  small_url text,
  super_url text,
  platforms text[]
);
CREATE UNIQUE INDEX IF NOT EXISTS game_images_provider_image_key_uq
  ON game_images(game_provider_id, image_key);
CREATE INDEX IF NOT EXISTS game_images_provider_rank_idx
  ON game_images(game_provider_id, rank);
CREATE INDEX IF NOT EXISTS game_images_media_id_idx ON game_images(media_id);
CREATE INDEX IF NOT EXISTS game_images_platforms_gin ON game_images USING gin (platforms);

CREATE TABLE IF NOT EXISTS game_videos (
  id bigserial PRIMARY KEY,
  game_provider_id bigint NOT NULL REFERENCES game_providers(id) ON DELETE CASCADE,
  video_key varchar(255),
  name varchar(255),
  description text,
  site_detail_url varchar(255),
  embed_url varchar(255),
  stream_url varchar(255),
  duration_seconds integer,
  published_at timestamptz,
  thumbnails json,
  metadata json,
  created_at timestamptz,
  updated_at timestamptz,
  media_id bigint,
  storage_disk varchar(255),
  storage_path varchar(255),
  provider_item_id varchar(255),
  video_game_source_id bigint REFERENCES video_game_sources(id) ON DELETE SET NULL,
  provider_payload json
);
CREATE UNIQUE INDEX IF NOT EXISTS game_videos_provider_video_key_uq
  ON game_videos(game_provider_id, video_key);
CREATE INDEX IF NOT EXISTS game_videos_provider_published_idx
  ON game_videos(game_provider_id, published_at);
CREATE INDEX IF NOT EXISTS game_videos_media_id_idx ON game_videos(media_id);

CREATE TABLE IF NOT EXISTS provider_media_links (
  id bigserial PRIMARY KEY,
  provider_item_id bigint NOT NULL REFERENCES provider_items(id) ON DELETE CASCADE,
  media_id bigint,
  url text,
  video_game_id bigint REFERENCES video_games(id) ON DELETE SET NULL,
  media_type text,
  title text,
  role text,
  source text,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (media_id IS NOT NULL OR url IS NOT NULL)
);
CREATE INDEX IF NOT EXISTS provider_media_links_item_idx ON provider_media_links(provider_item_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_provider_media_links_item_url ON provider_media_links(provider_item_id,url);
CREATE INDEX IF NOT EXISTS provider_media_links_video_game_idx ON provider_media_links(video_game_id);
CREATE INDEX IF NOT EXISTS provider_media_links_type_role_idx ON provider_media_links(media_type,role);
CREATE INDEX IF NOT EXISTS provider_media_links_source_idx ON provider_media_links(source);
CREATE INDEX IF NOT EXISTS idx_provider_media_links_vg_type ON provider_media_links(video_game_id,media_type);

-- =============================
-- RATINGS & TITLE SOURCES
-- =============================
CREATE TABLE IF NOT EXISTS video_game_ratings_by_locale (
  id bigserial PRIMARY KEY,
  video_game_id bigint NOT NULL REFERENCES video_games(id) ON DELETE CASCADE,
  locale text NOT NULL,
  average_rating numeric,
  rating_count bigint,
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (video_game_id, locale)
);
CREATE TABLE IF NOT EXISTS video_game_title_sources (
  id bigserial PRIMARY KEY,
  video_game_title_id bigint NOT NULL REFERENCES video_game_titles(id) ON DELETE CASCADE,
  source text NOT NULL,
  source_id text,
  metadata jsonb,
  fetched_at timestamptz
);
DO $$ BEGIN IF NOT EXISTS (
  SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
  WHERE c.relkind='i' AND c.relname='uq_vg_title_sources_vid_source_sid' AND n.nspname='public'
) THEN CREATE UNIQUE INDEX uq_vg_title_sources_vid_source_sid ON video_game_title_sources (video_game_title_id, source, coalesce(source_id,'')); END IF; END $$;

CREATE OR REPLACE VIEW video_games_enriched AS
SELECT
  vg.id,
  vgt.product_id AS product_id,
  vgt.id AS title_id,
  vgt.title AS canonical_title,
  COALESCE(vg.display_title, vgt.title) AS source_title,
  vg.slug,
  vgt.normalized_title AS normalized_title,
  vgt.normalized_title AS title_normalized,
  NULL::text AS locale,
  NULL::text AS genre,
  vg.genres,
  NULL::text[] AS platform_codes,
  vg.region_codes,
  NULL::jsonb AS external_ids,
  NULL::jsonb AS external_links,
  vg.release_date,
  vg.developer,
  vg.metadata,
  vg.created_at,
  vg.updated_at,
  NULL::timestamptz AS last_synced_at
FROM video_games vg
LEFT JOIN video_game_titles vgt ON vgt.id = vg.title_id;

-- =============================
-- EXCHANGE RATES (for FX enrichment)
-- =============================
CREATE TABLE IF NOT EXISTS exchange_rates (
  id bigserial PRIMARY KEY,
  base_currency text NOT NULL,
  quote_currency text NOT NULL,
  rate double precision NOT NULL,
  provider text NOT NULL,
  fetched_at timestamptz NOT NULL,
  metadata jsonb,
  UNIQUE (base_currency, quote_currency, provider)
);
CREATE INDEX IF NOT EXISTS idx_exchange_rates_lookup ON exchange_rates (base_currency, quote_currency, provider, fetched_at DESC);

-- Ensure providers.kind column exists if migration applied before addition
DO $$ BEGIN
IF NOT EXISTS (
  SELECT 1 FROM information_schema.columns
  WHERE table_schema='public' AND table_name='providers' AND column_name='kind'
) THEN
  ALTER TABLE providers ADD COLUMN kind text;
END IF; END $$;
