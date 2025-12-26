-- 0015_enable_rls_and_policies.sql
-- Enable RLS on sensitive tables and create owner-based policies.
-- Idempotent and safe to run multiple times.

SET search_path TO gamecompare, public;

-- Enable RLS on selected tables (skip missing tables)
DO $$
DECLARE
  t TEXT;
  tbls TEXT[] := ARRAY[
    'users', 'alerts', 'offers', 'offer_jurisdictions', 'current_price', 'provider_items'
  ];
BEGIN
  FOREACH t IN ARRAY tbls LOOP
    BEGIN
      EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', t);
    EXCEPTION WHEN undefined_table THEN
      RAISE NOTICE 'rls: table % not found, skipping', t;
    END;
  END LOOP;
END
$$;

-- Users: allow an authenticated user to SELECT/UPDATE/DELETE their own row (matching jwt.claims.user_id)
DO $$
BEGIN
  -- SELECT
  BEGIN
    EXECUTE 'DROP POLICY IF EXISTS users_select_self ON users';
    EXECUTE $$
      CREATE POLICY users_select_self ON users
      FOR SELECT TO authenticated
      USING (
        current_setting(''jwt.claims.user_id'', true) IS NOT NULL
        AND current_setting(''jwt.claims.user_id'', true)::bigint = id
      )
    $$;
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'users_select_self policy create skipped: %', SQLERRM;
  END;

  -- UPDATE/DELETE (manage own row)
  BEGIN
    EXECUTE 'DROP POLICY IF EXISTS users_manage_self ON users';
    EXECUTE $$
      CREATE POLICY users_manage_self ON users
      FOR UPDATE, DELETE TO authenticated
      USING (
        current_setting(''jwt.claims.user_id'', true) IS NOT NULL
        AND current_setting(''jwt.claims.user_id'', true)::bigint = id
      )
      WITH CHECK (
        current_setting(''jwt.claims.user_id'', true) IS NOT NULL
        AND current_setting(''jwt.claims.user_id'', true)::bigint = id
      )
    $$;
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'users_manage_self policy create skipped: %', SQLERRM;
  END;

  -- Service role full access (server processes using service role should still bypass RLS, but keep explicit policy)
  BEGIN
    EXECUTE 'DROP POLICY IF EXISTS service_role_full_users ON users';
    EXECUTE $$
      CREATE POLICY service_role_full_users ON users
      FOR ALL TO service_role
      USING (true)
      WITH CHECK (true)
    $$;
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'service_role policy on users skipped: %', SQLERRM;
  END;
END$$;

-- Alerts: only owner may manage (create/update/delete/select) their alerts
DO $$
BEGIN
  BEGIN
    EXECUTE 'DROP POLICY IF EXISTS alerts_manage_self ON alerts';
    EXECUTE $$
      CREATE POLICY alerts_manage_self ON alerts
      FOR ALL TO authenticated
      USING (
        current_setting(''jwt.claims.user_id'', true) IS NOT NULL
        AND current_setting(''jwt.claims.user_id'', true)::bigint = user_id
      )
      WITH CHECK (
        current_setting(''jwt.claims.user_id'', true) IS NOT NULL
        AND current_setting(''jwt.claims.user_id'', true)::bigint = user_id
      )
    $$;
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'alerts_manage_self policy create skipped: %', SQLERRM;
  END;

  BEGIN
    EXECUTE 'DROP POLICY IF EXISTS service_role_full_alerts ON alerts';
    EXECUTE $$
      CREATE POLICY service_role_full_alerts ON alerts
      FOR ALL TO service_role
      USING (true)
      WITH CHECK (true)
    $$;
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'service_role policy on alerts skipped: %', SQLERRM;
  END;
END$$;

-- Current_price: allow public SELECT (read-only) and service role full access
DO $$
BEGIN
  BEGIN
    EXECUTE 'ALTER TABLE current_price ENABLE ROW LEVEL SECURITY';
  EXCEPTION WHEN undefined_table THEN
    RAISE NOTICE 'current_price not present, skipping rls enable';
  END;

  BEGIN
    EXECUTE 'DROP POLICY IF EXISTS current_price_read_public ON current_price';
    EXECUTE $$
      CREATE POLICY current_price_read_public ON current_price
      FOR SELECT TO anon, authenticated
      USING (true)
    $$;
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'current_price_read_public creation skipped: %', SQLERRM;
  END;

  BEGIN
    EXECUTE 'DROP POLICY IF EXISTS service_role_full_current_price ON current_price';
    EXECUTE $$
      CREATE POLICY service_role_full_current_price ON current_price
      FOR ALL TO service_role
      USING (true)
      WITH CHECK (true)
    $$;
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'service_role policy on current_price skipped: %', SQLERRM;
  END;
END$$;

-- Offer read: ensure offers & offer_jurisdictions are readable
DO $$
BEGIN
  BEGIN
    EXECUTE 'ALTER TABLE offers ENABLE ROW LEVEL SECURITY';
  EXCEPTION WHEN undefined_table THEN NULL; END;
  BEGIN
    EXECUTE 'DROP POLICY IF EXISTS offers_read_public ON offers';
    EXECUTE $$
      CREATE POLICY offers_read_public ON offers
      FOR SELECT TO anon, authenticated
      USING (true)
    $$;
  EXCEPTION WHEN OTHERS THEN RAISE NOTICE 'offers_read_public skipped: %', SQLERRM; END;

  BEGIN
    EXECUTE 'ALTER TABLE offer_jurisdictions ENABLE ROW LEVEL SECURITY';
  EXCEPTION WHEN undefined_table THEN NULL; END;
  BEGIN
    EXECUTE 'DROP POLICY IF EXISTS offer_jurisdictions_read_public ON offer_jurisdictions';
    EXECUTE $$
      CREATE POLICY offer_jurisdictions_read_public ON offer_jurisdictions
      FOR SELECT TO anon, authenticated
      USING (true)
    $$;
  EXCEPTION WHEN OTHERS THEN RAISE NOTICE 'offer_jurisdictions_read_public skipped: %', SQLERRM; END;
END$$;

-- Final notice
RAISE NOTICE '0015_enable_rls_and_policies.sql applied (idempotent).';
