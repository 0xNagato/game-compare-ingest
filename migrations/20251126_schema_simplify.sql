-- GameCompare schema simplification migration (phase 0/1)
-- Generated 2025-11-26

-- Ensure enum exists with desired labels
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'product_kind') THEN
    CREATE TYPE product_kind AS ENUM ('software', 'hardware');
  ELSE
    -- backfill enum values if missing
    IF NOT EXISTS (
      SELECT 1 FROM pg_type t
      JOIN pg_enum e ON t.oid = e.enumtypid
      WHERE t.typname = 'product_kind' AND e.enumlabel = 'software'
    ) THEN
      ALTER TYPE product_kind ADD VALUE 'software';
    END IF;
    IF NOT EXISTS (
      SELECT 1 FROM pg_type t
      JOIN pg_enum e ON t.oid = e.enumtypid
      WHERE t.typname = 'product_kind' AND e.enumlabel = 'hardware'
    ) THEN
      ALTER TYPE product_kind ADD VALUE 'hardware';
    END IF;
  END IF;
END
$$;

-- products.kind column (nullable during backfill)
ALTER TABLE IF EXISTS products
  ADD COLUMN IF NOT EXISTS kind product_kind;

-- product_versions table
DO $$
BEGIN
  CREATE TABLE product_versions (
    id              BIGSERIAL PRIMARY KEY,
    product_id      BIGINT NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    platform_id     BIGINT REFERENCES platforms(id) ON DELETE SET NULL,
    edition         TEXT,
    form_factor     TEXT,
    release_date    DATE,
    metadata        JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
  );
EXCEPTION
  WHEN duplicate_table THEN NULL;
END
$$;

CREATE UNIQUE INDEX IF NOT EXISTS product_versions_unique_idx
  ON product_versions (product_id, COALESCE(platform_id, 0), COALESCE(edition, ''));

-- retailers table adjustments (ensure slug uniqueness exists)
ALTER TABLE IF EXISTS retailers
  ADD CONSTRAINT retailers_slug_unique UNIQUE (slug);

-- offers table alignment
ALTER TABLE IF EXISTS offers
  ADD COLUMN IF NOT EXISTS product_version_id BIGINT,
  ADD COLUMN IF NOT EXISTS metadata JSONB,
  ADD CONSTRAINT offers_product_version_fk
    FOREIGN KEY (product_version_id) REFERENCES product_versions(id) ON DELETE CASCADE;

CREATE UNIQUE INDEX IF NOT EXISTS offers_version_retailer_unique_idx
  ON offers (product_version_id, retailer_id, COALESCE(sku, ''));

-- offer_regions table (new)
DO $$
BEGIN
  CREATE TABLE offer_regions (
    id               BIGSERIAL PRIMARY KEY,
    offer_id         BIGINT NOT NULL REFERENCES offers(id) ON DELETE CASCADE,
    jurisdiction_id  BIGINT NOT NULL REFERENCES jurisdictions(id) ON DELETE CASCADE,
    currency_id      BIGINT NOT NULL REFERENCES currencies(id),
    tax_rule_id      BIGINT REFERENCES tax_rules(id) ON DELETE SET NULL,
    metadata         JSONB,
    UNIQUE (offer_id, jurisdiction_id)
  );
EXCEPTION
  WHEN duplicate_table THEN NULL;
END
$$;

-- prices: ensure new foreign key column for offer_regions
ALTER TABLE IF EXISTS prices
  ADD COLUMN IF NOT EXISTS offer_region_id BIGINT;

ALTER TABLE IF EXISTS prices
  ADD CONSTRAINT prices_offer_region_fk
    FOREIGN KEY (offer_region_id) REFERENCES offer_regions(id) ON DELETE CASCADE;

-- partition guard function retained, but ensure offer_region_id is populated in downstream migrations

-- current_prices table (new) replacing current_price view
DO $$
BEGIN
  CREATE TABLE current_prices (
    offer_region_id BIGINT PRIMARY KEY REFERENCES offer_regions(id) ON DELETE CASCADE,
    amount_minor    BIGINT NOT NULL,
    recorded_at     TIMESTAMPTZ NOT NULL
  );
EXCEPTION
  WHEN duplicate_table THEN NULL;
END
$$;

-- providers table
DO $$
BEGIN
  CREATE TABLE providers (
    id         BIGSERIAL PRIMARY KEY,
    code       CITEXT NOT NULL UNIQUE,
    name       TEXT NOT NULL,
    kind       TEXT NOT NULL CHECK (kind IN ('retailer_api','catalog','media','pricing_api')),
    metadata   JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
  );
EXCEPTION
  WHEN duplicate_table THEN NULL;
END
$$;

-- provider_items table
DO $$
BEGIN
  CREATE TABLE provider_items (
    id                   BIGSERIAL PRIMARY KEY,
    provider_id          BIGINT NOT NULL REFERENCES providers(id) ON DELETE CASCADE,
    external_id          TEXT NOT NULL,
    product_version_id   BIGINT REFERENCES product_versions(id) ON DELETE SET NULL,
    offer_id             BIGINT REFERENCES offers(id) ON DELETE SET NULL,
    last_synced_at       TIMESTAMPTZ,
    payload_hash         TEXT,
    metadata             JSONB,
    UNIQUE (provider_id, external_id)
  );
EXCEPTION
  WHEN duplicate_table THEN NULL;
END
$$;

CREATE INDEX IF NOT EXISTS provider_items_provider_idx ON provider_items (provider_id);
CREATE INDEX IF NOT EXISTS provider_items_offer_idx ON provider_items (offer_id);

-- provider_runs table
DO $$
BEGIN
  CREATE TABLE provider_runs (
    id            BIGSERIAL PRIMARY KEY,
    provider_id   BIGINT NOT NULL REFERENCES providers(id) ON DELETE CASCADE,
    started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at   TIMESTAMPTZ,
    status        TEXT NOT NULL CHECK (status IN ('queued','running','succeeded','failed','partial')),
    item_count    INTEGER,
    error_summary JSONB
  );
EXCEPTION
  WHEN duplicate_table THEN NULL;
END
$$;

CREATE INDEX IF NOT EXISTS provider_runs_provider_idx ON provider_runs (provider_id, started_at DESC);

-- alerts adjustment: precise FK to offer_regions
ALTER TABLE IF EXISTS alerts
  ADD COLUMN IF NOT EXISTS offer_region_id BIGINT;

ALTER TABLE IF EXISTS alerts
  ADD CONSTRAINT alerts_offer_region_fk
    FOREIGN KEY (offer_region_id) REFERENCES offer_regions(id) ON DELETE CASCADE;

-- helper materialized view for later validation (optional)
DO $$
BEGIN
  CREATE MATERIALIZED VIEW IF NOT EXISTS offer_region_latest_prices AS
  SELECT orr.id AS offer_region_id,
         cp.amount_minor,
         cp.recorded_at
  FROM offer_regions orr
  LEFT JOIN current_prices cp ON cp.offer_region_id = orr.id;
END
$$;