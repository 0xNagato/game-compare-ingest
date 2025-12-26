-- 0021_security_rls_fixes.sql
-- Address Supabase linter findings:
-- - Ensure views are SECURITY INVOKER (not SECURITY DEFINER)
-- - Enable RLS on flagged public tables
-- Idempotent and safe to re-run.

SET search_path TO public;

-- Helper: set security_invoker=true on a view if it exists
DO $$ BEGIN
  IF to_regclass('public.partition_index_jobs') IS NOT NULL THEN
    EXECUTE 'ALTER VIEW public.partition_index_jobs SET (security_invoker = true)';
  END IF;
  IF to_regclass('public.sku_regions_vw') IS NOT NULL THEN
    EXECUTE 'ALTER VIEW public.sku_regions_vw SET (security_invoker = true)';
  END IF;
  IF to_regclass('public.latest_price_per_sku_region_vw') IS NOT NULL THEN
    EXECUTE 'ALTER VIEW public.latest_price_per_sku_region_vw SET (security_invoker = true)';
  END IF;
  IF to_regclass('public.latest_region_price_vw') IS NOT NULL THEN
    EXECUTE 'ALTER VIEW public.latest_region_price_vw SET (security_invoker = true)';
  END IF;
  IF to_regclass('public.vw_video_game_latest_price') IS NOT NULL THEN
    EXECUTE 'ALTER VIEW public.vw_video_game_latest_price SET (security_invoker = true)';
  END IF;
  IF to_regclass('public.region_prices_vw') IS NOT NULL THEN
    EXECUTE 'ALTER VIEW public.region_prices_vw SET (security_invoker = true)';
  END IF;
END $$;

-- Enable RLS on flagged tables if they exist
ALTER TABLE IF EXISTS public._mv_price_refresh_state ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.video_game_title ENABLE ROW LEVEL SECURITY;

-- Optional: tighten views by setting security_barrier (does not affect invoker/definer semantics)
DO $$ BEGIN
  IF to_regclass('public.region_prices_vw') IS NOT NULL THEN
    EXECUTE 'ALTER VIEW public.region_prices_vw SET (security_barrier = true)';
  END IF;
END $$;
