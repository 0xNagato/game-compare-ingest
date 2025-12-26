-- 0476_backfill_concurrent_indexes.sql
-- Purpose: Add helper expression indexes to accelerate NULL-targeted backfill scans.
-- Notes:
-- - Uses CREATE INDEX CONCURRENTLY IF NOT EXISTS (our custom runner executes CIC outside a transaction).
-- - These indexes are optional and primarily benefit parallel SKIP LOCKED workers.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_retailer_providers_credentials_null
  ON public.retailer_providers ((credentials IS NULL));

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_provider_items_attributes_null
  ON public.provider_items ((attributes IS NULL));

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_provider_offers_is_active_null
  ON public.provider_offers ((is_active IS NULL));

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_providers_meta_null
  ON public.providers ((auth_metadata IS NULL OR metadata IS NULL OR is_active IS NULL));

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_provider_ingest_runs_meta_null
  ON public.provider_ingest_runs ((meta IS NULL OR stats IS NULL OR errors IS NULL OR items_processed IS NULL OR prices_written IS NULL));

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_provider_sync_states_sync_details_null
  ON public.provider_sync_states ((sync_details IS NULL));
