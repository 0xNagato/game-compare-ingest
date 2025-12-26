-- 0003_recent_partial_static.sql
-- Purpose: Provide static (immutable predicate) partial indexes for recent (7d, 30d) price lookups
--          without relying on volatile functions like now() inside index predicates.
-- Approach:
--   - Capture reference timestamp "anchor_ts" = date_trunc('day', now()) - interval '1 day';
--     This ensures we are always indexing up to end-of-yesterday, making predicates immutable.
--   - Create partial indexes on each existing partition using literal timestamps (anchor_ts - 7d, anchor_ts - 30d).
--   - Update ensure_price_partition to ALSO create these static partial indexes at creation time.
--   - These static indexes can be rotated daily via a scheduled job re-running this migration logic
--     (or a specialized maintenance function) if strict rolling windows are required.
-- Rationale:
--   - Immutable predicates allow PostgreSQL to use them reliably and avoid rejection.
--   - Using end-of-yesterday reduces churn while still serving "recent" queries with high selectivity.
--   - Combines with existing DESC and BRIN indexes for layered performance.

SET search_path TO public;

-- Replace ensure_price_partition directly (compute cutoffs inside function and embed them as literals)
CREATE OR REPLACE FUNCTION ensure_price_partition(ts timestamptz)
RETURNS void LANGUAGE plpgsql AS $fn$
DECLARE
  start_ts timestamptz := date_trunc('month', ts);
  end_ts timestamptz := (start_ts + interval '1 month');
  part_name text := 'prices_' || to_char(start_ts, 'YYYY_MM');
  anchor_ts timestamptz := date_trunc('day', now()) - interval '1 day'; -- end of yesterday
  cutoff_7d timestamptz := anchor_ts - interval '7 days';
  cutoff_30d timestamptz := anchor_ts - interval '30 days';
BEGIN
  -- Partition and baseline indexes
  EXECUTE format('CREATE TABLE IF NOT EXISTS %I PARTITION OF prices FOR VALUES FROM (%L) TO (%L)', part_name, start_ts, end_ts);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %I_oj_recorded_at ON %I (offer_jurisdiction_id, recorded_at)', part_name, part_name);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %I_recorded_at ON %I (recorded_at)', part_name, part_name);
  -- Recent/friendly indexes
  EXECUTE format('CREATE INDEX IF NOT EXISTS %I_oj_recorded_at_desc ON %I (offer_jurisdiction_id, recorded_at DESC) INCLUDE (amount_minor)', part_name, part_name);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %I_recorded_at_brin ON %I USING BRIN (recorded_at)', part_name, part_name);
  -- Static partial indexes (immutable predicate timestamps)
  EXECUTE format('CREATE INDEX IF NOT EXISTS %I_recent_7d_static_idx ON %I (offer_jurisdiction_id, recorded_at) WHERE recorded_at >= %L', part_name, part_name, cutoff_7d);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %I_recent_30d_static_idx ON %I (offer_jurisdiction_id, recorded_at) WHERE recorded_at >= %L', part_name, part_name, cutoff_30d);
END; $fn$;

-- Backfill static partial indexes across existing partitions now
DO $do$
DECLARE
  anchor_ts timestamptz := date_trunc('day', now()) - interval '1 day';
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
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I_recent_7d_static_idx ON %I (offer_jurisdiction_id, recorded_at) WHERE recorded_at >= %L', part.partition_name, part.partition_name, cutoff_7d);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I_recent_30d_static_idx ON %I (offer_jurisdiction_id, recorded_at) WHERE recorded_at >= %L', part.partition_name, part.partition_name, cutoff_30d);
  END LOOP;
END
$do$;
