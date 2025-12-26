-- 0200_extensions_enums.sql (squashed, idempotent)
-- Ensure required extensions are present. Supabase installs many under the `ext` schema.
DO $$ BEGIN
  CREATE EXTENSION IF NOT EXISTS citext WITH SCHEMA public;
EXCEPTION WHEN OTHERS THEN NULL; END $$;
DO $$ BEGIN
  CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA ext;
EXCEPTION WHEN OTHERS THEN NULL; END $$;

-- Enums
DO $$ BEGIN
  CREATE TYPE cmp_op AS ENUM ('above','below');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- NOTE: products.kind is already text with CHECK in this db; keep enum definition out to avoid churn.
