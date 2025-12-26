-- 0033_partition_helper_compat.sql
-- Purpose: Normalize partition helper naming and provide backward-compatible wrapper.
-- Canonical helper: ensure_prices_partition_for(ts timestamptz)
-- Legacy alias:    ensure_price_partition(p_ts timestamptz) -> delegates to canonical

SET search_path TO public;

-- Drop conflicting definition to avoid parameter name mismatch issues
DROP FUNCTION IF EXISTS ensure_prices_partition_for(timestamptz);

-- Canonical function: creates monthly partition and essential indexes
CREATE FUNCTION ensure_prices_partition_for(ts timestamptz)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  start_ts timestamptz := date_trunc('month', ts);
  end_ts   timestamptz := (start_ts + interval '1 month');
  part_name text := 'prices_' || to_char(start_ts, 'YYYY_MM');
BEGIN
  -- Create partition table if missing
  EXECUTE format(
    'CREATE TABLE IF NOT EXISTS %I PARTITION OF prices FOR VALUES FROM (%L) TO (%L)',
    part_name, start_ts, end_ts
  );

  -- Standard indexes replicated per partition
  EXECUTE format('CREATE INDEX IF NOT EXISTS %I_oj_recorded_at ON %I (offer_jurisdiction_id, recorded_at)', part_name, part_name);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %I_recorded_at       ON %I (recorded_at)', part_name, part_name);
END;
$$;

-- Backward-compatible wrapper used by earlier migrations/code
CREATE OR REPLACE FUNCTION ensure_price_partition(p_ts timestamptz)
RETURNS void LANGUAGE sql AS $$
  SELECT ensure_prices_partition_for(p_ts)
$$;

-- Optional: small smoke check to ensure current and next-month partitions are provisioned
DO $$
DECLARE
  cur_month  timestamptz := date_trunc('month', now());
  next_month timestamptz := (date_trunc('month', now()) + interval '1 month');
BEGIN
  PERFORM ensure_prices_partition_for(cur_month);
  PERFORM ensure_prices_partition_for(next_month);
END $$;
