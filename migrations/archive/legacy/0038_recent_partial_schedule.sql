-- 0004_recent_partial_schedule.sql
-- Purpose: Rotate static partial index cutoffs on a predictable schedule: every 7 days from the 10th.
-- Strategy:
--   - Redefine ensure_price_partition() to compute an anchor date: latest (10th + n*7 days) <= now().
--   - From that anchor (midnight), compute static cutoff timestamps for 7d and 30d windows.
--   - Embed those literal timestamps in partial index predicates (immutable) via %L.
--   - Backfill by dropping/recreating static partial indexes on all existing partitions.

SET search_path TO public;

CREATE OR REPLACE FUNCTION ensure_price_partition(ts timestamptz)
RETURNS void LANGUAGE plpgsql AS $fn$
DECLARE
  start_ts timestamptz := date_trunc('month', ts);
  end_ts timestamptz := (start_ts + interval '1 month');
  part_name text := 'prices_' || to_char(start_ts, 'YYYY_MM');
  -- schedule: every 7 days from the 10th of some month
  tenth_current_month timestamptz := date_trunc('month', now()) + interval '9 days';
  anchor_base timestamptz := CASE WHEN now() >= tenth_current_month THEN tenth_current_month ELSE (date_trunc('month', now()) - interval '1 month') + interval '9 days' END;
  days_since integer := GREATEST(0, FLOOR(EXTRACT(EPOCH FROM (now() - anchor_base)) / 86400)::int);
  periods integer := (days_since / 7);
  anchor_ts timestamptz := date_trunc('day', anchor_base + (periods || ' days')::interval);
  cutoff_7d timestamptz := anchor_ts - interval '7 days';
  cutoff_30d timestamptz := anchor_ts - interval '30 days';
BEGIN
  -- Partition and baseline indexes
  EXECUTE format('CREATE TABLE IF NOT EXISTS %I PARTITION OF prices FOR VALUES FROM (%L) TO (%L)', part_name, start_ts, end_ts);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %I_oj_recorded_at ON %I (offer_jurisdiction_id, recorded_at)', part_name, part_name);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %I_recorded_at ON %I (recorded_at)', part_name, part_name);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %I_oj_recorded_at_desc ON %I (offer_jurisdiction_id, recorded_at DESC) INCLUDE (amount_minor)', part_name, part_name);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %I_recorded_at_brin ON %I USING BRIN (recorded_at)', part_name, part_name);
  -- Static partial indexes (immutable predicate timestamps) based on schedule anchor
  EXECUTE format('CREATE INDEX IF NOT EXISTS %I_recent_7d_static_idx ON %I (offer_jurisdiction_id, recorded_at) WHERE recorded_at >= %L', part_name, part_name, cutoff_7d);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %I_recent_30d_static_idx ON %I (offer_jurisdiction_id, recorded_at) WHERE recorded_at >= %L', part_name, part_name, cutoff_30d);
END; $fn$;

-- Backfill: drop and recreate static partial indexes with scheduled cutoffs
DO $do$
DECLARE
  tenth_current_month timestamptz := date_trunc('month', now()) + interval '9 days';
  anchor_base timestamptz := CASE WHEN now() >= tenth_current_month THEN tenth_current_month ELSE (date_trunc('month', now()) - interval '1 month') + interval '9 days' END;
  days_since integer := GREATEST(0, FLOOR(EXTRACT(EPOCH FROM (now() - anchor_base)) / 86400)::int);
  periods integer := (days_since / 7);
  anchor_ts timestamptz := date_trunc('day', anchor_base + (periods || ' days')::interval);
  cutoff_7d timestamptz := anchor_ts - interval '7 days';
  cutoff_30d timestamptz := anchor_ts - interval '30 days';
  part record;
BEGIN
  FOR part IN
    SELECT child.relname AS partition_name
    FROM pg_inherits
    JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
    JOIN pg_class child  ON pg_inherits.inhrelid  = child.oid
    JOIN pg_namespace n  ON n.oid = parent.relnamespace
    WHERE parent.relname = 'prices' AND n.nspname = 'public'
  LOOP
    -- Drop old static indexes if present (so predicates update)
    EXECUTE format('DROP INDEX IF EXISTS %I_recent_7d_static_idx', part.partition_name);
    EXECUTE format('DROP INDEX IF EXISTS %I_recent_30d_static_idx', part.partition_name);
    -- Recreate with new scheduled cutoffs
    EXECUTE format('CREATE INDEX %I_recent_7d_static_idx ON %I (offer_jurisdiction_id, recorded_at) WHERE recorded_at >= %L', part.partition_name, part.partition_name, cutoff_7d);
    EXECUTE format('CREATE INDEX %I_recent_30d_static_idx ON %I (offer_jurisdiction_id, recorded_at) WHERE recorded_at >= %L', part.partition_name, part.partition_name, cutoff_30d);
  END LOOP;
END
$do$;
