-- 0022_move_pg_trgm_to_ext.sql
-- Purpose: Relocate pg_trgm extension out of public into dedicated schema 'ext'.
-- Notes:
--  - Existing GIN trigram indexes remain valid (operator class OID unchanged).
--  - Future indexes should schema-qualify operator class as ext.gin_trgm_ops.

CREATE SCHEMA IF NOT EXISTS ext;
DO $$ BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_extension e
    JOIN pg_namespace n ON n.oid = e.extnamespace
    WHERE e.extname = 'pg_trgm' AND n.nspname = 'public'
  ) THEN
    ALTER EXTENSION pg_trgm SET SCHEMA ext;
  END IF;
END $$;
