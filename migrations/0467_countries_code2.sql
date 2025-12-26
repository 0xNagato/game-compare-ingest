-- 0467_countries_code2.sql
-- Add code2 column expected by ingestion helpers (alias of iso2) and backfill.
-- Idempotent: guarded and safe on re-run.
ALTER TABLE IF EXISTS countries ADD COLUMN IF NOT EXISTS code2 char(2);
UPDATE countries SET code2 = iso2 WHERE code2 IS NULL;
-- Optional unique constraint if absent (iso2 already unique, keep separate for clarity)
DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname='countries_code2_uq' AND conrelid='public.countries'::regclass
  ) THEN
    ALTER TABLE countries ADD CONSTRAINT countries_code2_uq UNIQUE (code2);
  END IF;
END $$;
