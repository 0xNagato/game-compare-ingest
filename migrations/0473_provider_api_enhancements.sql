-- 0473_provider_api_enhancements.sql
-- =============================
-- providers: add descriptive + operational fields
-- =============================
ALTER TABLE IF EXISTS public.providers
  ADD COLUMN IF NOT EXISTS base_url text,
  ADD COLUMN IF NOT EXISTS website_url text,
  ADD COLUMN IF NOT EXISTS docs_url text,
  ADD COLUMN IF NOT EXISTS auth_mode text,
  ADD COLUMN IF NOT EXISTS auth_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS is_active boolean NOT NULL DEFAULT true,
  ADD COLUMN IF NOT EXISTS rate_limit_per_minute integer,
  ADD COLUMN IF NOT EXISTS rate_limit_burst integer,
  ADD COLUMN IF NOT EXISTS metadata jsonb NOT NULL DEFAULT '{}'::jsonb;

-- Backfill NULL flags to sane defaults
UPDATE public.providers
   SET is_active = true,
       auth_metadata = coalesce(auth_metadata, '{}'::jsonb),
       metadata = coalesce(metadata, '{}'::jsonb)
 WHERE is_active IS NULL OR auth_metadata IS NULL OR metadata IS NULL;

CREATE INDEX IF NOT EXISTS providers_kind_idx ON public.providers (kind);
CREATE INDEX IF NOT EXISTS providers_active_idx ON public.providers (is_active);

-- =============================
-- retailer_providers: credentials + sync metadata per retailer binding
-- =============================
ALTER TABLE IF EXISTS public.retailer_providers
  ADD COLUMN IF NOT EXISTS credentials jsonb NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS settings jsonb NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS jurisdiction_scope text[] DEFAULT '{}',
  ADD COLUMN IF NOT EXISTS last_synced_at timestamptz,
  ADD COLUMN IF NOT EXISTS next_sync_at timestamptz,
  ADD COLUMN IF NOT EXISTS sync_status text,
  ADD COLUMN IF NOT EXISTS sync_cursor text,
  ADD COLUMN IF NOT EXISTS sync_error jsonb,
  ADD COLUMN IF NOT EXISTS rate_limit_per_minute integer,
  ADD COLUMN IF NOT EXISTS rate_limit_burst integer,
  ADD COLUMN IF NOT EXISTS metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS is_enabled boolean NOT NULL DEFAULT true,
  ADD COLUMN IF NOT EXISTS priority smallint NOT NULL DEFAULT 100;

UPDATE public.retailer_providers
   SET credentials = coalesce(credentials, '{}'::jsonb),
       settings = coalesce(settings, '{}'::jsonb),
       metadata = coalesce(metadata, '{}'::jsonb),
       jurisdiction_scope = coalesce(jurisdiction_scope, '{}'),
       is_enabled = coalesce(is_enabled, true),
       priority = coalesce(priority, 100)
 WHERE credentials IS NULL OR settings IS NULL OR metadata IS NULL OR jurisdiction_scope IS NULL OR is_enabled IS NULL OR priority IS NULL;

CREATE INDEX IF NOT EXISTS retailer_providers_provider_idx ON public.retailer_providers (provider_id);
CREATE INDEX IF NOT EXISTS retailer_providers_sync_status_idx ON public.retailer_providers (sync_status);
CREATE INDEX IF NOT EXISTS retailer_providers_next_sync_idx ON public.retailer_providers (next_sync_at);

-- =============================
-- provider_items: richer external identifiers & lifecycle tracking
-- =============================
ALTER TABLE IF EXISTS public.provider_items
  ADD COLUMN IF NOT EXISTS external_sku text,
  ADD COLUMN IF NOT EXISTS region_code text,
  ADD COLUMN IF NOT EXISTS last_seen_at timestamptz,
  ADD COLUMN IF NOT EXISTS checksum bytea,
  ADD COLUMN IF NOT EXISTS attributes jsonb NOT NULL DEFAULT '{}'::jsonb;

UPDATE public.provider_items
   SET attributes = coalesce(attributes, '{}'::jsonb)
 WHERE attributes IS NULL;

CREATE INDEX IF NOT EXISTS provider_items_region_idx ON public.provider_items (provider_id, region_code);
CREATE INDEX IF NOT EXISTS provider_items_last_seen_idx ON public.provider_items (last_seen_at DESC);

-- =============================
-- provider_offers: matching confidence + lifecycle metadata
-- =============================
ALTER TABLE IF EXISTS public.provider_offers
  ADD COLUMN IF NOT EXISTS confidence numeric(5,4) CHECK (confidence IS NULL OR (confidence >= 0 AND confidence <= 1)),
  ADD COLUMN IF NOT EXISTS strategy text,
  ADD COLUMN IF NOT EXISTS last_matched_at timestamptz,
  ADD COLUMN IF NOT EXISTS is_active boolean NOT NULL DEFAULT true,
  ADD COLUMN IF NOT EXISTS notes text;

UPDATE public.provider_offers
   SET is_active = coalesce(is_active, true)
 WHERE is_active IS NULL;

CREATE INDEX IF NOT EXISTS provider_offers_active_idx ON public.provider_offers (is_active) WHERE is_active;
CREATE INDEX IF NOT EXISTS provider_offers_confidence_idx ON public.provider_offers (confidence);

-- =============================
-- provider_ingest_runs: align with operational telemetry
-- =============================
ALTER TABLE IF EXISTS public.provider_ingest_runs
  ADD COLUMN IF NOT EXISTS run_kind text,
  ADD COLUMN IF NOT EXISTS job_id text,
  ADD COLUMN IF NOT EXISTS ended_at timestamptz,
  ADD COLUMN IF NOT EXISTS errors jsonb,
  ADD COLUMN IF NOT EXISTS meta jsonb,
  ADD COLUMN IF NOT EXISTS stats jsonb,
  ADD COLUMN IF NOT EXISTS items_processed bigint,
  ADD COLUMN IF NOT EXISTS prices_written bigint;

UPDATE public.provider_ingest_runs
   SET meta = coalesce(meta, '{}'::jsonb),
       stats = coalesce(stats, '{}'::jsonb),
       errors = coalesce(errors, '{}'::jsonb),
       items_processed = coalesce(items_processed, 0),
       prices_written = coalesce(prices_written, 0)
 WHERE meta IS NULL OR stats IS NULL OR errors IS NULL OR items_processed IS NULL OR prices_written IS NULL;

CREATE INDEX IF NOT EXISTS provider_ingest_runs_provider_idx ON public.provider_ingest_runs (provider_id, started_at DESC);
CREATE INDEX IF NOT EXISTS provider_ingest_runs_status_idx ON public.provider_ingest_runs (status);

DO $$
BEGIN
  CREATE TABLE IF NOT EXISTS public.provider_sync_states (
    id bigserial PRIMARY KEY,
    provider_id bigint NOT NULL REFERENCES public.providers(id) ON DELETE CASCADE,
    retailer_provider_id bigint REFERENCES public.retailer_providers(id) ON DELETE CASCADE,
    sync_kind text NOT NULL,
    last_synced_at timestamptz,
    next_sync_at timestamptz,
    sync_status text NOT NULL DEFAULT 'pending',
    sync_details jsonb NOT NULL DEFAULT '{}'::jsonb,
    error_details jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
  );
EXCEPTION
  WHEN duplicate_table THEN NULL;
END $$;

CREATE INDEX IF NOT EXISTS provider_sync_states_provider_idx
  ON public.provider_sync_states (provider_id, sync_kind);
CREATE INDEX IF NOT EXISTS provider_sync_states_next_idx
  ON public.provider_sync_states (next_sync_at);
CREATE INDEX IF NOT EXISTS provider_sync_states_status_idx
  ON public.provider_sync_states (sync_status);

CREATE UNIQUE INDEX IF NOT EXISTS uq_provider_sync_states_kind
  ON public.provider_sync_states (provider_id, sync_kind, coalesce(retailer_provider_id, 0::bigint));

-- Ensure RLS is disabled for administrative tables when running inside Supabase backend role
COMMENT ON TABLE public.provider_sync_states IS 'Tracks per-provider sync windows, cursors, and status for API integrations.';
COMMENT ON COLUMN public.provider_sync_states.sync_kind IS 'Describes dataset being synchronized (e.g., catalogue, offers, prices).';
