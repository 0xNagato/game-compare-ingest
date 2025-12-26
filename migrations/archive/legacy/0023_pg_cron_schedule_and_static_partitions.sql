-- 0023_pg_cron_schedule_and_static_partitions.sql
-- Purpose: Ensure pg_cron exists, schedule the partition index worker, and pre-create a static set of partitions.
-- Static range based on reference date 2025-11: last 12 months + next 6 months.

DO $$ BEGIN
  PERFORM 1 FROM pg_extension WHERE extname = 'pg_cron';
  IF NOT FOUND THEN
    BEGIN
      CREATE EXTENSION IF NOT EXISTS pg_cron WITH SCHEMA extensions;
    EXCEPTION WHEN OTHERS THEN
      CREATE EXTENSION IF NOT EXISTS pg_cron;
    END;
  END IF;
END $$;

-- Schedule worker every 10 minutes (idempotent)
DO $$ BEGIN
  PERFORM cron.schedule('process_partition_indexes', '*/10 * * * *', 'SELECT partition_util.process_partition_index_jobs();');
EXCEPTION WHEN OTHERS THEN
  NULL;
END $$;

-- Create partitions explicitly and enqueue deterministic async index jobs
DO $$ DECLARE
  suffix text;
  months text[] := ARRAY[
    '202412','202501','202502','202503','202504','202505','202506','202507','202508','202509','202510','202511',
    '202512','202601','202602','202603','202604','202605'
  ];
  part_name text;
  start_month date;
  next_month date;
BEGIN
  FOREACH suffix IN ARRAY months LOOP
    part_name := 'prices_' || suffix;
    IF to_regclass(part_name) IS NULL THEN
      start_month := to_date(suffix, 'YYYYMM');
      next_month := start_month + INTERVAL '1 month';
      EXECUTE format('CREATE TABLE %I PARTITION OF prices FOR VALUES FROM (%L) TO (%L);', part_name, start_month, next_month);
    END IF;

    INSERT INTO partition_util.partition_index_jobs(partition_name, index_type)
    VALUES (part_name, 'brin_recorded'), (part_name, 'btree_series')
    ON CONFLICT (partition_name, index_type) DO NOTHING;
  END LOOP;
END $$;
