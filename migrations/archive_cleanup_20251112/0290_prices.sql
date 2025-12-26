-- 0290_prices.sql (squashed)
-- Parent partitioned table (monthly partitions on recorded_at)
CREATE TABLE IF NOT EXISTS public.prices (
  id                    bigserial,
  offer_jurisdiction_id bigint NOT NULL REFERENCES public.offer_jurisdictions(id) ON DELETE CASCADE,
  provider_item_id      bigint REFERENCES public.provider_items(id) ON DELETE SET NULL,
  recorded_at           timestamptz NOT NULL,
  amount_minor          bigint NOT NULL CHECK (amount_minor >= 0),
  tax_inclusive         boolean NOT NULL,
  fx_minor_per_unit     bigint,
  btc_sats_per_unit     bigint,
  meta                  jsonb,
  PRIMARY KEY (id, recorded_at)
) PARTITION BY RANGE(recorded_at);

-- Parent-only indexes (each partition clones these)
CREATE INDEX IF NOT EXISTS prices_series_idx ON ONLY public.prices (offer_jurisdiction_id, recorded_at);
CREATE INDEX IF NOT EXISTS idx_prices_parent_oj_recorded_at ON ONLY public.prices (offer_jurisdiction_id, recorded_at);
CREATE INDEX IF NOT EXISTS idx_prices_parent_recorded_at ON ONLY public.prices (recorded_at);
CREATE INDEX IF NOT EXISTS prices_recorded_at_brin ON ONLY public.prices USING brin (recorded_at);

-- Helper function: create a single month partition, clone minimal indexes
CREATE OR REPLACE FUNCTION public.ensure_price_partition(ts timestamptz)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  start_month date := date_trunc('month', ts)::date;
  next_month  date := (date_trunc('month', ts) + interval '1 month')::date;
  part_name   text := format('prices_%s', to_char(start_month, 'YYYY_MM'));
BEGIN
  IF to_regclass(part_name) IS NULL THEN
    EXECUTE format('CREATE TABLE %I PARTITION OF public.prices FOR VALUES FROM (%L) TO (%L);', part_name, start_month, next_month);
    -- Essential indexes (avoid duplicates if function re-run)
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I_oj_recorded_at ON public.%I (offer_jurisdiction_id, recorded_at);', part_name, part_name);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I_recorded_at ON public.%I (recorded_at);', part_name, part_name);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I_recorded_at_brin ON public.%I USING brin (recorded_at);', part_name, part_name);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I_oj_recorded_at_desc ON public.%I (offer_jurisdiction_id, recorded_at DESC) INCLUDE (amount_minor);', part_name, part_name);
  END IF;
END$$;

-- Backward compatibility wrapper name (legacy callers)
CREATE OR REPLACE FUNCTION public.ensure_prices_partition_for(ts timestamptz)
RETURNS void LANGUAGE plpgsql AS $$ BEGIN PERFORM public.ensure_price_partition(ts); END $$;

-- Horizon precreation (12 past months + 6 future) idempotent
DO $$
DECLARE
  i int;
  base date := date_trunc('month', now())::date - interval '12 months';
  future int := 18; -- 12 past + current + 6 future
BEGIN
  FOR i IN 0..future LOOP
    PERFORM public.ensure_price_partition((base + (i || ' months')::interval)::timestamptz);
  END LOOP;
END$$;
