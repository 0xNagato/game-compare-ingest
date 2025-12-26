-- 0004_search_indexes.sql
-- Enable pg_trgm + similarity based search indexes (idempotent)
DO $$ BEGIN
  CREATE EXTENSION IF NOT EXISTS pg_trgm;
EXCEPTION WHEN insufficient_privilege THEN
  RAISE NOTICE 'pg_trgm extension creation skipped (permissions)';
END $$;

-- Search path
-- Public-only schema (legacy gamecompare removed)
SET search_path TO public;

-- Guarded: only attempt index on sellables.title if the column exists (schema drift safe)
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'sellables' AND column_name = 'title'
  ) THEN
  EXECUTE 'CREATE INDEX IF NOT EXISTS idx_sellables_title_trgm ON sellables USING gin (title ext.gin_trgm_ops)';
  ELSE
    RAISE NOTICE 'sellables.title does not exist; skipping idx_sellables_title_trgm';
  END IF;
END $$;

-- Provider items trigram indexes (idempotent); retained for safety even if created earlier
DO $$ BEGIN
  EXECUTE 'CREATE INDEX IF NOT EXISTS idx_provider_items_external_trgm ON provider_items USING gin (external_item_id ext.gin_trgm_ops)';
  EXECUTE 'CREATE INDEX IF NOT EXISTS idx_provider_items_payload_name_trgm ON provider_items USING gin ((payload->>''name'') ext.gin_trgm_ops)';
EXCEPTION WHEN duplicate_table THEN
  NULL;
END $$;
