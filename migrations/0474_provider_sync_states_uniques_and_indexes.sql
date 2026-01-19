-- 0474_provider_sync_states_uniques_and_indexes.sql
-- Purpose: Harden provider_sync_states uniqueness and indexing; add security comment on credentials
-- Idempotent: guarded with IF NOT EXISTS / DROP IF EXISTS so it can be re-run safely.

-- Ensure FK lookup/join coverage on retailer_provider_id
-- NOTE: provider_sync_states table is created by migration 0473. Skip if table doesn't exist yet.
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='provider_sync_states') THEN
    CREATE INDEX IF NOT EXISTS provider_sync_states_retailer_provider_idx
      ON public.provider_sync_states (retailer_provider_id);

    -- Replace sentinel-COALESCE unique with explicit partial uniques for clarity
    DROP INDEX IF EXISTS uq_provider_sync_states_kind;

    -- Unique when retailer_provider_id is NULL (provider-level sync window per kind)
    CREATE UNIQUE INDEX IF NOT EXISTS uq_provider_sync_states_kind_null
      ON public.provider_sync_states (provider_id, sync_kind)
      WHERE retailer_provider_id IS NULL;

    -- Unique when retailer_provider_id is NOT NULL (binding-level sync window per kind)
    CREATE UNIQUE INDEX IF NOT EXISTS uq_provider_sync_states_kind_rp
      ON public.provider_sync_states (provider_id, sync_kind, retailer_provider_id)
      WHERE retailer_provider_id IS NOT NULL;
  END IF;
END $$;

-- Security note for credentials storage
DO $$ BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='retailer_providers' AND column_name='credentials'
  ) THEN
    COMMENT ON COLUMN public.retailer_providers.credentials IS
      'Sensitive provider credentials; restrict access via RLS and use application-layer encryption where appropriate.';
  END IF;
END $$;
