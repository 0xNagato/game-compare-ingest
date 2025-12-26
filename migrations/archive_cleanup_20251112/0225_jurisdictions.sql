-- 0225_jurisdictions.sql (squashed)
-- Countries assumed present (code2, name, currency_id)
CREATE TABLE IF NOT EXISTS public.jurisdictions (
  id          bigserial PRIMARY KEY,
  country_id  bigint NOT NULL REFERENCES public.countries(id) ON DELETE CASCADE,
  region_code text,
  region_key  text GENERATED ALWAYS AS (coalesce(region_code,'')) STORED
);
-- Uniqueness: one row per country + optional region (guarded for idempotency)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'jurisdictions_country_region_key_uq'
      AND conrelid = 'public.jurisdictions'::regclass
  ) THEN
    ALTER TABLE public.jurisdictions
      ADD CONSTRAINT jurisdictions_country_region_key_uq
      UNIQUE (country_id, region_key);
  END IF;
END $$;
