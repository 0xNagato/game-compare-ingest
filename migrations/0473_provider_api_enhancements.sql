-- 0473_video_game_source_api_enhancements.sql
-- =============================
-- video_game_sources: add descriptive + operational fields
-- =============================
ALTER TABLE IF EXISTS public.video_game_sources
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
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='video_game_sources' AND column_name='is_active') THEN
    UPDATE public.video_game_sources SET is_active = true WHERE is_active IS NULL;
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='video_game_sources' AND column_name='auth_metadata') THEN
    UPDATE public.video_game_sources SET auth_metadata = '{}' WHERE auth_metadata IS NULL;
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='video_game_sources' AND column_name='metadata') THEN
    UPDATE public.video_game_sources SET metadata = '{}' WHERE metadata IS NULL;
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS video_game_sources_kind_idx ON public.video_game_sources (kind);
CREATE INDEX IF NOT EXISTS video_game_sources_active_idx ON public.video_game_sources (is_active);

-- =============================
-- retailer_video_game_sources: credentials + sync metadata per retailer binding
-- =============================
ALTER TABLE IF EXISTS public.retailer_video_game_sources
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

DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='retailer_video_game_sources')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='retailer_video_game_sources' AND column_name='credentials')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='retailer_video_game_sources' AND column_name='settings')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='retailer_video_game_sources' AND column_name='metadata')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='retailer_video_game_sources' AND column_name='jurisdiction_scope')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='retailer_video_game_sources' AND column_name='is_enabled')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='retailer_video_game_sources' AND column_name='priority') THEN
    UPDATE public.retailer_video_game_sources
       SET credentials = coalesce(credentials, '{}'::jsonb),
           settings = coalesce(settings, '{}'::jsonb),
           metadata = coalesce(metadata, '{}'::jsonb),
           jurisdiction_scope = coalesce(jurisdiction_scope, '{}'),
           is_enabled = coalesce(is_enabled, true),
           priority = coalesce(priority, 100)
     WHERE credentials IS NULL OR settings IS NULL OR metadata IS NULL OR jurisdiction_scope IS NULL OR is_enabled IS NULL OR priority IS NULL;
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS retailer_video_game_sources_video_game_source_idx ON public.retailer_video_game_sources (video_game_source_id);
CREATE INDEX IF NOT EXISTS retailer_video_game_sources_sync_status_idx ON public.retailer_video_game_sources (sync_status);
CREATE INDEX IF NOT EXISTS retailer_video_game_sources_next_sync_idx ON public.retailer_video_game_sources (next_sync_at);

-- =============================
-- video_game_source_items: richer external identifiers & lifecycle tracking
-- =============================
ALTER TABLE IF EXISTS public.video_game_source_items
  ADD COLUMN IF NOT EXISTS external_sku text,
  ADD COLUMN IF NOT EXISTS region_code text,
  ADD COLUMN IF NOT EXISTS last_seen_at timestamptz,
  ADD COLUMN IF NOT EXISTS checksum bytea,
  ADD COLUMN IF NOT EXISTS attributes jsonb NOT NULL DEFAULT '{}'::jsonb;

DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='video_game_source_items') THEN
    UPDATE public.video_game_source_items
       SET attributes = coalesce(attributes, '{}'::jsonb)
     WHERE attributes IS NULL;
  END IF;
END $$;

DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='video_game_source_items') THEN
    CREATE INDEX IF NOT EXISTS video_game_source_items_region_idx ON public.video_game_source_items (video_game_source_id, region_code);
    CREATE INDEX IF NOT EXISTS video_game_source_items_last_seen_idx ON public.video_game_source_items (last_seen_at DESC);
  END IF;
END $$;

-- =============================
-- video_game_source_offers: matching confidence + lifecycle metadata
-- =============================
ALTER TABLE IF EXISTS public.video_game_source_offers
  ADD COLUMN IF NOT EXISTS confidence numeric(5,4) CHECK (confidence IS NULL OR (confidence >= 0 AND confidence <= 1)),
  ADD COLUMN IF NOT EXISTS strategy text,
  ADD COLUMN IF NOT EXISTS last_matched_at timestamptz,
  ADD COLUMN IF NOT EXISTS is_active boolean NOT NULL DEFAULT true,
  ADD COLUMN IF NOT EXISTS notes text;

DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='video_game_source_offers') THEN
    UPDATE public.video_game_source_offers
       SET is_active = coalesce(is_active, true)
     WHERE is_active IS NULL;
  END IF;
END $$;

DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='video_game_source_offers') THEN
    CREATE INDEX IF NOT EXISTS video_game_source_offers_active_idx ON public.video_game_source_offers (is_active) WHERE is_active;
    CREATE INDEX IF NOT EXISTS video_game_source_offers_confidence_idx ON public.video_game_source_offers (confidence);
  END IF;
END $$;

-- =============================
-- video_game_source_ingest_runs: align with operational telemetry
-- =============================
ALTER TABLE IF EXISTS public.video_game_source_ingest_runs
  ADD COLUMN IF NOT EXISTS run_kind text,
  ADD COLUMN IF NOT EXISTS job_id text,
  ADD COLUMN IF NOT EXISTS ended_at timestamptz,
  ADD COLUMN IF NOT EXISTS errors jsonb,
  ADD COLUMN IF NOT EXISTS meta jsonb,
  ADD COLUMN IF NOT EXISTS stats jsonb,
  ADD COLUMN IF NOT EXISTS items_processed bigint,
  ADD COLUMN IF NOT EXISTS prices_written bigint;

-- Skip UPDATE for ingest_runs table as it will be removed in migration 0535

DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='video_game_source_ingest_runs') THEN
    CREATE INDEX IF NOT EXISTS video_game_source_ingest_runs_video_game_source_idx ON public.video_game_source_ingest_runs (video_game_source_id, started_at DESC);
    CREATE INDEX IF NOT EXISTS video_game_source_ingest_runs_status_idx ON public.video_game_source_ingest_runs (status);
  END IF;
END $$;

DO $$
BEGIN
  CREATE TABLE IF NOT EXISTS public.video_game_source_sync_states (
    id bigserial PRIMARY KEY,
    video_game_source_id bigint NOT NULL REFERENCES public.video_game_sources(id) ON DELETE CASCADE,
    retailer_video_game_source_id bigint REFERENCES public.retailer_video_game_sources(id) ON DELETE CASCADE,
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

CREATE INDEX IF NOT EXISTS video_game_source_sync_states_video_game_source_idx
  ON public.video_game_source_sync_states (video_game_source_id, sync_kind);
CREATE INDEX IF NOT EXISTS video_game_source_sync_states_next_idx
  ON public.video_game_source_sync_states (next_sync_at);
CREATE INDEX IF NOT EXISTS video_game_source_sync_states_status_idx
  ON public.video_game_source_sync_states (sync_status);

CREATE UNIQUE INDEX IF NOT EXISTS uq_video_game_source_sync_states_kind
  ON public.video_game_source_sync_states (video_game_source_id, sync_kind, coalesce(retailer_video_game_source_id, 0::bigint));

-- Ensure RLS is disabled for administrative tables when running inside Supabase backend role
COMMENT ON TABLE public.video_game_source_sync_states IS 'Tracks per-video_game_source sync windows, cursors, and status for API integrations.';
COMMENT ON COLUMN public.video_game_source_sync_states.sync_kind IS 'Describes dataset being synchronized (e.g., catalogue, offers, prices).';
