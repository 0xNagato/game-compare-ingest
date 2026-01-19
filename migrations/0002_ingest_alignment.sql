-- 0002_ingest_alignment.sql
-- Align consolidated schema with ingestion pipeline expectations
-- Idempotent: safe to re-run

-- currencies.minor_unit (default 2)
ALTER TABLE IF EXISTS public.currencies
  ADD COLUMN IF NOT EXISTS minor_unit smallint;
-- Backfill default where NULL
UPDATE public.currencies SET minor_unit = COALESCE(minor_unit, 2);

-- providers.kind column (text)
DO $$ BEGIN
IF NOT EXISTS (
  SELECT 1 FROM information_schema.columns
  WHERE table_schema='public' AND table_name='video_game_sources' AND column_name='kind'
) THEN
  ALTER TABLE public.video_game_sources ADD COLUMN kind text;
END IF; END $$;

-- exchange_rates table (for FX)
CREATE TABLE IF NOT EXISTS public.exchange_rates (
  id bigserial PRIMARY KEY,
  base_currency text NOT NULL,
  quote_currency text NOT NULL,
  rate double precision NOT NULL,
  provider text NOT NULL,
  fetched_at timestamptz NOT NULL,
  metadata jsonb,
  UNIQUE (base_currency, quote_currency, provider)
);
CREATE INDEX IF NOT EXISTS idx_exchange_rates_lookup
  ON public.exchange_rates (base_currency, quote_currency, provider, fetched_at DESC);
