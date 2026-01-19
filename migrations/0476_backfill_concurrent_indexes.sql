-- 0476_backfill_concurrent_indexes.sql
-- Purpose: Add helper expression indexes to accelerate NULL-targeted backfill scans.
-- Notes:
-- - Uses CREATE INDEX IF NOT EXISTS (executed within transaction blocks).
-- - These indexes are optional and primarily benefit parallel SKIP LOCKED workers.
-- - All indexes are conditional on table and column existence.

DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='retailer_providers')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='retailer_providers' AND column_name='credentials') THEN
    CREATE INDEX IF NOT EXISTS idx_retailer_providers_credentials_null
      ON public.retailer_providers ((credentials IS NULL));
  END IF;
END $$;

DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='provider_items')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='provider_items' AND column_name='attributes') THEN
    CREATE INDEX IF NOT EXISTS idx_provider_items_attributes_null
      ON public.provider_items ((attributes IS NULL));
  END IF;
END $$;

DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='provider_offers')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='provider_offers' AND column_name='is_active') THEN
    CREATE INDEX IF NOT EXISTS idx_provider_offers_is_active_null
      ON public.provider_offers ((is_active IS NULL));
  END IF;
END $$;

DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='providers')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='providers' AND column_name='auth_metadata')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='providers' AND column_name='metadata')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='providers' AND column_name='is_active') THEN
    CREATE INDEX IF NOT EXISTS idx_providers_meta_null
      ON public.providers ((auth_metadata IS NULL OR metadata IS NULL OR is_active IS NULL));
  END IF;
END $$;

DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='provider_ingest_runs')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='provider_ingest_runs' AND column_name='meta')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='provider_ingest_runs' AND column_name='stats')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='provider_ingest_runs' AND column_name='errors')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='provider_ingest_runs' AND column_name='items_processed')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='provider_ingest_runs' AND column_name='prices_written') THEN
    CREATE INDEX IF NOT EXISTS idx_provider_ingest_runs_meta_null
      ON public.provider_ingest_runs ((meta IS NULL OR stats IS NULL OR errors IS NULL OR items_processed IS NULL OR prices_written IS NULL));
  END IF;
END $$;

DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='provider_sync_states')
     AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='provider_sync_states' AND column_name='sync_details') THEN
    CREATE INDEX IF NOT EXISTS idx_provider_sync_states_sync_details_null
      ON public.provider_sync_states ((sync_details IS NULL));
  END IF;
END $$;
