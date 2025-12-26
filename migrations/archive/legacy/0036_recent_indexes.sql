-- 0036_recent_indexes.sql (renamed from 0002_recent_indexes.sql)
-- Enhanced per-partition indexes (DESC + BRIN) integrated into ensure_price_partition

SET search_path TO public;

DO $do$
BEGIN
  CREATE OR REPLACE FUNCTION ensure_price_partition(ts timestamptz)
  RETURNS void LANGUAGE plpgsql AS $fn$
  DECLARE
    start_ts timestamptz := date_trunc('month', ts);
    end_ts timestamptz := (start_ts + interval '1 month');
    part_name text := 'prices_' || to_char(start_ts, 'YYYY_MM');
  BEGIN
    -- Create partition and baseline indexes
    EXECUTE format('CREATE TABLE IF NOT EXISTS %I PARTITION OF prices FOR VALUES FROM (%L) TO (%L)', part_name, start_ts, end_ts);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I_oj_recorded_at ON %I (offer_jurisdiction_id, recorded_at)', part_name, part_name);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I_recorded_at ON %I (recorded_at)', part_name, part_name);

    -- Additional helpful indexes for recent queries:
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I_oj_recorded_at_desc ON %I (offer_jurisdiction_id, recorded_at DESC) INCLUDE (amount_minor)', part_name, part_name);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I_recorded_at_brin ON %I USING BRIN (recorded_at)', part_name, part_name);
  END;
  $fn$;
END
$do$;

-- Backfill indexes on existing partitions
DO $do$
DECLARE
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
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I_oj_recorded_at_desc ON %I (offer_jurisdiction_id, recorded_at DESC) INCLUDE (amount_minor)', part.partition_name, part.partition_name);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I_recorded_at_brin ON %I USING BRIN (recorded_at)', part.partition_name, part.partition_name);
  END LOOP;
END
$do$;
