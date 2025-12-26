-- 0040_canonical_partition_helper.sql
-- Canonical monthly partition helper with no INSERT trigger (per Ops notes).
-- Consolidates older variants and ensures a single, stable entry point:
--   ensure_price_partition(ts timestamptz)
-- Also provides a compatibility wrapper:
--   ensure_prices_partition_for(ts timestamptz) → ensure_price_partition(ts)
-- Idempotent and safe to re-run.

SET search_path TO public;

-- 1) Drop legacy INSERT trigger and function (avoid DDL during writes)
DO $$ BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_trigger t
    JOIN pg_class c ON t.tgrelid = c.oid
    WHERE c.relname = 'prices' AND t.tgname = 'prices_partition') THEN
      EXECUTE 'DROP TRIGGER prices_partition ON public.prices';
  END IF;
EXCEPTION WHEN undefined_table THEN NULL; END $$;

DO $$ BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_proc p
    JOIN pg_namespace n ON p.pronamespace = n.oid
    WHERE p.proname = 'prices_partition_insert_trigger' AND n.nspname = 'public') THEN
      EXECUTE 'DROP FUNCTION public.prices_partition_insert_trigger()';
  END IF;
EXCEPTION WHEN undefined_function THEN NULL; END $$;

-- 2) Canonical helper (create partition + essential indexes)
CREATE OR REPLACE FUNCTION ensure_price_partition(ts timestamptz)
RETURNS void LANGUAGE plpgsql AS $fn$
DECLARE
  start_ts timestamptz := date_trunc('month', ts);
  end_ts   timestamptz := (start_ts + interval '1 month');
  part_name text := 'prices_' || to_char(start_ts, 'YYYY_MM');
BEGIN
  -- Create partition
  EXECUTE format('CREATE TABLE IF NOT EXISTS %I PARTITION OF prices FOR VALUES FROM (%L) TO (%L)', part_name, start_ts, end_ts);

  -- Baseline indexes (per partition)
  EXECUTE format('CREATE INDEX IF NOT EXISTS %I_oj_recorded_at ON %I (offer_jurisdiction_id, recorded_at)', part_name, part_name);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %I_oj_recorded_at_desc ON %I (offer_jurisdiction_id, recorded_at DESC) INCLUDE (amount_minor)', part_name, part_name);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %I_recorded_at ON %I (recorded_at)', part_name, part_name);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %I_recorded_at_brin ON %I USING BRIN (recorded_at)', part_name, part_name);
END; $fn$;

-- 3) Compatibility wrapper (older code may call this)
CREATE OR REPLACE FUNCTION ensure_prices_partition_for(ts timestamptz)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  PERFORM ensure_price_partition(ts);
END; $$;

-- 4) Pre-create a safe horizon (last 12 months + next 6 months)
DO $$
DECLARE
  start_month date := (date_trunc('month', now()) - interval '12 months')::date;
  end_month   date := (date_trunc('month', now()) + interval '6 months')::date;
  cur date := start_month;
BEGIN
  WHILE cur < end_month LOOP
    PERFORM ensure_price_partition(cur);
    cur := (cur + interval '1 month')::date;
  END LOOP;
END $$;

COMMENT ON FUNCTION ensure_price_partition(timestamptz) IS 'Create prices monthly partition and essential indexes; no insert trigger used.';
COMMENT ON FUNCTION ensure_prices_partition_for(timestamptz) IS 'Compatibility alias calling ensure_price_partition.';
