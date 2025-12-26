-- Create a safe, idempotent procedure used by pg_cron to refresh the latest game price MV.
-- Supabase-lite deployments may not have the materialized view; in that case the procedure is a no-op.

DO $$ BEGIN
  -- Create procedure if it does not exist
  IF NOT EXISTS (
    SELECT 1 FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname = 'public' AND p.proname = 'refresh_mv_latest_game_price'
  ) THEN
    CREATE PROCEDURE public.refresh_mv_latest_game_price()
    LANGUAGE plpgsql
    AS $$
    BEGIN
      -- Only refresh when the materialized view is present; otherwise, no-op.
      IF to_regclass('public.mv_latest_game_price') IS NOT NULL THEN
        -- Use non-concurrent refresh for broad compatibility.
        EXECUTE 'REFRESH MATERIALIZED VIEW public.mv_latest_game_price';
      ELSE
        RAISE NOTICE 'mv_latest_game_price does not exist; skipping refresh';
      END IF;
    END;
    $$;
  END IF;
END $$;
