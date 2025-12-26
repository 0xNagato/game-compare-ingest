-- 0025_monthly_precreate_next_partition_cron.sql
-- Purpose: Add a pg_cron job that runs monthly to pre-create the next month partition
-- using the existing ensure_prices_partition_and_enqueue('YYYYMM').

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

-- Schedule on the 1st day of each month at 00:10 UTC
-- Cron: minute hour dom mon dow -> '10 0 1 * *'
DO $$ BEGIN
  PERFORM cron.schedule(
    'precreate_next_month_partition',
    '10 0 1 * *',
    $$SELECT public.ensure_prices_partition_and_enqueue(to_char(date_trunc('month', now() + interval '1 month'), 'YYYYMM'));$$
  );
EXCEPTION WHEN OTHERS THEN
  -- Ignore if already scheduled or permissions limited
  NULL;
END $$;
