-- Ensure sqlx migrations table is compatible with newer sqlx CLI expecting a `checksum` column.
-- Idempotent: adds the column if missing; if table is missing entirely, creates it.

DO $$
BEGIN
  -- Case 1: table exists; add column if missing
  IF EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'public' AND table_name = '_sqlx_migrations'
  ) THEN
    IF NOT EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = '_sqlx_migrations'
        AND column_name = 'checksum'
    ) THEN
      ALTER TABLE public._sqlx_migrations
        ADD COLUMN checksum bytea; -- nullable to avoid backfilling requirement
    END IF;
  ELSE
    -- Case 2: table missing; create with modern shape
    CREATE TABLE IF NOT EXISTS public._sqlx_migrations (
      version BIGINT PRIMARY KEY,
      description TEXT NOT NULL,
      installed_on TIMESTAMPTZ NOT NULL DEFAULT now(),
      success BOOLEAN NOT NULL,
      checksum BYTEA
    );
  END IF;
END$$;
