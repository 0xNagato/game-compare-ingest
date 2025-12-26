-- 0466_currencies_minor_unit.sql
-- Add minor_unit column to currencies for price scaling (expected by ingestion ensure_currency).
-- Idempotent and safe on re-run.
ALTER TABLE IF EXISTS currencies ADD COLUMN IF NOT EXISTS minor_unit smallint NOT NULL DEFAULT 2;
-- Backfill / normalize known zero or three-decimal currencies
UPDATE currencies SET minor_unit = 0 WHERE code IN ('JPY','KRW','VND','CLP','ISK','HUF');
UPDATE currencies SET minor_unit = 3 WHERE code IN ('BHD','IQD','KWD','JOD','OMR','TND');
-- Ensure all others default to 2
UPDATE currencies SET minor_unit = 2 WHERE minor_unit IS NULL;
