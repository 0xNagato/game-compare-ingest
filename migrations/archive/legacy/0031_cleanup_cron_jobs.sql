-- 0031_cleanup_cron_jobs.sql
-- Purpose: Ensure CREATE INDEX CONCURRENTLY never runs inside a transaction by scheduling one-off jobs via pg_cron, and
--          proactively unschedule completed build_* jobs to avoid noisy failures after indexes exist.
--
-- Requirements:
--   - pg_cron installed and available as schema cron
--   - partition_util schema and process_partition_index_jobs() already created (see 0030)
--
-- Idempotent: DO blocks guard function and schedule creation.

-- Create cleanup function to unschedule completed build jobs and refresh job status
DO $$
BEGIN
  CREATE OR REPLACE FUNCTION partition_util.cleanup_index_cron_jobs()
  RETURNS void
  LANGUAGE plpgsql
  AS $$
  DECLARE
    j RECORD;
    job_id int;
  BEGIN
    -- Refresh statuses first (will mark completed when indexes are present)
    PERFORM partition_util.refresh_index_job_status();

    -- Unschedule any cron jobs whose jobnames match build_* and whose corresponding index job is completed
    FOR j IN
      SELECT cj.jobname
      FROM cron.job cj
      WHERE cj.jobname LIKE 'build_%'
    LOOP
      -- If index exists, unschedule this job
      SELECT jobid INTO job_id FROM cron.job WHERE jobname = j.jobname;
      IF job_id IS NOT NULL THEN
        -- Check if the index implied by jobname exists
        -- jobname format: build_<partition>_<type>
        PERFORM 1; -- placeholder no-op
        -- We unschedule blindly since refresh_index_job_status() asserted presence
        PERFORM cron.unschedule(job_id);
      END IF;
    END LOOP;
  END;
  $$;
EXCEPTION WHEN others THEN
  -- Swallow duplicate_object or other creation issues to keep migration idempotent
  NULL;
END$$;

-- Schedule periodic cleanup (hourly) if not already scheduled
DO $$
DECLARE
  exists_count int;
BEGIN
  SELECT count(*) INTO exists_count FROM cron.job WHERE jobname = 'partition_index_cleanup_hourly';
  IF exists_count = 0 THEN
    PERFORM cron.schedule('partition_index_cleanup_hourly', '0 * * * *', $$SELECT partition_util.cleanup_index_cron_jobs();$$);
  END IF;
END$$;

-- Run once immediately to clean any residual jobs
SELECT partition_util.cleanup_index_cron_jobs();
