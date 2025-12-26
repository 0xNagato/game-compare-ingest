-- 0465_providers_kind.sql
-- Purpose: add missing 'kind' column to providers table expected by ingestion code.
-- Idempotent: guarded with IF NOT EXISTS.
ALTER TABLE IF EXISTS providers ADD COLUMN IF NOT EXISTS kind text;
-- Optional backfill: set kind where null based on simple heuristic (slug contains 'steam' => storefront)
UPDATE providers SET kind = 'storefront' WHERE kind IS NULL AND (slug ILIKE '%steam%' OR slug ILIKE '%playstation%' OR slug ILIKE '%xbox%' OR slug ILIKE '%nexarda%');
