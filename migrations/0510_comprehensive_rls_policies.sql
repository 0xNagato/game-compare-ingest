-- 0510_comprehensive_rls_policies.sql
-- Purpose: Apply Row-Level Security (RLS) policies to core tables for service_role access.
-- Idempotent: Safe to re-run; uses IF NOT EXISTS and DO blocks with error handling.
-- This enables read/write access for service_role on data tables while maintaining audit trail.

BEGIN;

-- Helper function to safely enable RLS and grant service_role access
DO $$
DECLARE
  tables_to_secure TEXT[] := ARRAY[
    'products',
    'software',
    'hardware',
    'video_game_titles',
    'video_games',
    'game_consoles',
    'platforms',
    'sellables',
    'retailers',
    'offers',
    'offer_jurisdictions',
    'providers',
    'retailer_providers',
    'provider_items',
    'provider_offers',
    'provider_media_links',
    'provider_ingest_runs',
    'countries',
    'currencies',
    'jurisdictions',
    'tax_rules',
    'prices',
    'current_price',
    'alerts',
    'users',
    'game_media'
  ];
  tbl TEXT;
BEGIN
  FOREACH tbl IN ARRAY tables_to_secure LOOP
    BEGIN
      -- Check if table exists before enabling RLS
      IF to_regclass('public.' || tbl) IS NOT NULL THEN
        EXECUTE 'ALTER TABLE public.' || quote_ident(tbl) || ' ENABLE ROW LEVEL SECURITY';
        
        -- Grant all permissions to service_role via permissive policy
        IF NOT EXISTS (
          SELECT 1 FROM pg_policies
          WHERE schemaname = 'public'
            AND tablename = tbl
            AND policyname = tbl || '_service_all'
        ) THEN
          EXECUTE 'CREATE POLICY ' || quote_ident(tbl || '_service_all') 
            || ' ON public.' || quote_ident(tbl) 
            || ' FOR ALL TO service_role USING (true) WITH CHECK (true)';
        END IF;
      END IF;
    EXCEPTION WHEN OTHERS THEN
      RAISE NOTICE 'Skipped RLS for %: %', tbl, SQLERRM;
    END;
  END LOOP;
END $$;

-- Explicitly handle provider_api_credentials (sensitive table - restricted to service_role)
DO $$
BEGIN
  IF to_regclass('public.provider_api_credentials') IS NOT NULL THEN
    EXECUTE 'ALTER TABLE public.provider_api_credentials ENABLE ROW LEVEL SECURITY';
    
    IF NOT EXISTS (
      SELECT 1 FROM pg_policies
      WHERE schemaname = 'public'
        AND tablename = 'provider_api_credentials'
        AND policyname = 'provider_api_credentials_service_only'
    ) THEN
      -- Only allow service_role to access credentials
      EXECUTE 'CREATE POLICY provider_api_credentials_service_only ON public.provider_api_credentials FOR ALL TO service_role USING (true) WITH CHECK (true)';
    END IF;
  END IF;
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'Skipped RLS for provider_api_credentials: %', SQLERRM;
END $$;

-- Grant necessary permissions to service_role on public schema objects
DO $$
BEGIN
  EXECUTE 'GRANT USAGE ON SCHEMA public TO service_role';
  EXECUTE 'GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO service_role';
  EXECUTE 'GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO service_role';
  EXECUTE 'GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO service_role';
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'Grant permissions failed (may be OK if already granted): %', SQLERRM;
END $$;

-- Audit tables: allow full access to service_role
DO $$
BEGIN
  -- video_game_title_dedupe_audit
  IF to_regclass('public.video_game_title_dedupe_audit') IS NOT NULL THEN
    EXECUTE 'ALTER TABLE public.video_game_title_dedupe_audit ENABLE ROW LEVEL SECURITY';
    IF NOT EXISTS (
      SELECT 1 FROM pg_policies
      WHERE schemaname = 'public'
        AND tablename = 'video_game_title_dedupe_audit'
        AND policyname = 'video_game_title_dedupe_audit_service_all'
    ) THEN
      EXECUTE 'CREATE POLICY video_game_title_dedupe_audit_service_all ON public.video_game_title_dedupe_audit FOR ALL TO service_role USING (true) WITH CHECK (true)';
    END IF;
  END IF;

  -- video_games_dedupe_audit
  IF to_regclass('public.video_games_dedupe_audit') IS NOT NULL THEN
    EXECUTE 'ALTER TABLE public.video_games_dedupe_audit ENABLE ROW LEVEL SECURITY';
    IF NOT EXISTS (
      SELECT 1 FROM pg_policies
      WHERE schemaname = 'public'
        AND tablename = 'video_games_dedupe_audit'
        AND policyname = 'video_games_dedupe_audit_service_all'
    ) THEN
      EXECUTE 'CREATE POLICY video_games_dedupe_audit_service_all ON public.video_games_dedupe_audit FOR ALL TO service_role USING (true) WITH CHECK (true)';
    END IF;
  END IF;
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'Audit table RLS setup failed: %', SQLERRM;
END $$;

COMMIT;
