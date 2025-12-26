-- 0002_prices_partitions.sql
-- Partition helper function, auto-provision trigger, and horizon pre-create (idempotent)

-- Use public schema (project migrated away from dedicated schema)
SET search_path TO public;

CREATE OR REPLACE FUNCTION ensure_price_partition(ts timestamptz)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  start_ts timestamptz := date_trunc('month', ts);
  end_ts timestamptz := (start_ts + interval '1 month');
  part_name text := 'prices_' || to_char(start_ts, 'YYYY_MM');
BEGIN
  EXECUTE format('CREATE TABLE IF NOT EXISTS %I PARTITION OF prices FOR VALUES FROM (%L) TO (%L)', part_name, start_ts, end_ts);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %I_oj_recorded_at ON %I (offer_jurisdiction_id, recorded_at)', part_name, part_name);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %I_recorded_at ON %I (recorded_at)', part_name, part_name);
END;
$$;

CREATE OR REPLACE FUNCTION prices_partition_insert_trigger()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  PERFORM ensure_price_partition(NEW.recorded_at);
  RETURN NEW;
END; $$;

DROP TRIGGER IF EXISTS prices_partition ON prices;
CREATE TRIGGER prices_partition BEFORE INSERT ON prices FOR EACH ROW EXECUTE FUNCTION prices_partition_insert_trigger();

-- Pre-create last 12 months and next 6 months to avoid cold path on first write
DO $$
DECLARE
  start_month date := (date_trunc('month', now()) - interval '12 months')::date;
  end_month date := (date_trunc('month', now()) + interval '6 months')::date;
  cur date := start_month;
BEGIN
  WHILE cur < end_month LOOP
  PERFORM ensure_price_partition(cur);
    cur := (cur + interval '1 month')::date;
  END LOOP;
END $$;
