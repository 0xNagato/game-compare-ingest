-- 0486_rls_and_cleanup.sql
-- Purpose: Enable Row Level Security (RLS) across core tables, create baseline policies,
--          propagate policies to future price partitions, and drop obsolete backup table.
-- Idempotent & Supabase-friendly.
-- NOTE: Adjust role names if your deployment differs (Supabase uses 'authenticated', 'anon', 'service_role').

-- 0. Drop obsolete backup table (if confirmed no longer needed).
DROP TABLE IF EXISTS public.platforms_backup CASCADE;

-- 1. Enable RLS on core reference & fact tables.
DO $$ DECLARE r record; BEGIN
  FOR r IN SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename IN (
    'currencies','countries','jurisdictions','tax_rules','products','software','video_game_titles',
    'platforms','video_games','hardware','game_consoles','sellables','retailers','offers','offer_jurisdictions',
    'prices','current_price','users','alerts','providers','retailer_providers','provider_items','provider_offers',
    'provider_ingest_runs','provider_media_links','platform_merge_audit'
  ) LOOP
    EXECUTE format('ALTER TABLE public.%I ENABLE ROW LEVEL SECURITY', r.tablename);
  END LOOP;
END $$;

-- 2. Create generic SELECT policies for read-only tables (reference data) allowing authenticated users.
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE schemaname='public' AND tablename='currencies' AND policyname='currencies_select') THEN
    CREATE POLICY currencies_select ON public.currencies FOR SELECT TO authenticated USING (true);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE schemaname='public' AND tablename='countries' AND policyname='countries_select') THEN
    CREATE POLICY countries_select ON public.countries FOR SELECT TO authenticated USING (true);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE schemaname='public' AND tablename='jurisdictions' AND policyname='jurisdictions_select') THEN
    CREATE POLICY jurisdictions_select ON public.jurisdictions FOR SELECT TO authenticated USING (true);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE schemaname='public' AND tablename='tax_rules' AND policyname='tax_rules_select') THEN
    CREATE POLICY tax_rules_select ON public.tax_rules FOR SELECT TO authenticated USING (true);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE schemaname='public' AND tablename='platforms' AND policyname='platforms_select') THEN
    CREATE POLICY platforms_select ON public.platforms FOR SELECT TO authenticated USING (true);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE schemaname='public' AND tablename='products' AND policyname='products_select') THEN
    CREATE POLICY products_select ON public.products FOR SELECT TO authenticated USING (true);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE schemaname='public' AND tablename='video_game_titles' AND policyname='video_game_titles_select') THEN
    CREATE POLICY video_game_titles_select ON public.video_game_titles FOR SELECT TO authenticated USING (true);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE schemaname='public' AND tablename='video_games' AND policyname='video_games_select') THEN
    CREATE POLICY video_games_select ON public.video_games FOR SELECT TO authenticated USING (true);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE schemaname='public' AND tablename='game_consoles' AND policyname='game_consoles_select') THEN
    CREATE POLICY game_consoles_select ON public.game_consoles FOR SELECT TO authenticated USING (true);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE schemaname='public' AND tablename='sellables' AND policyname='sellables_select') THEN
    CREATE POLICY sellables_select ON public.sellables FOR SELECT TO authenticated USING (true);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE schemaname='public' AND tablename='retailers' AND policyname='retailers_select') THEN
    CREATE POLICY retailers_select ON public.retailers FOR SELECT TO authenticated USING (true);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE schemaname='public' AND tablename='offers' AND policyname='offers_select') THEN
    CREATE POLICY offers_select ON public.offers FOR SELECT TO authenticated USING (true);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE schemaname='public' AND tablename='offer_jurisdictions' AND policyname='offer_jurisdictions_select') THEN
    CREATE POLICY offer_jurisdictions_select ON public.offer_jurisdictions FOR SELECT TO authenticated USING (true);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE schemaname='public' AND tablename='current_price' AND policyname='current_price_select') THEN
    CREATE POLICY current_price_select ON public.current_price FOR SELECT TO authenticated USING (true);
  END IF;
END $$;

-- 3. Service role full write policies (covers INSERT/UPDATE/DELETE). Fact tables + ingestion.
DO $$ BEGIN
  PERFORM 1 FROM pg_policies WHERE schemaname='public' AND tablename='providers' AND policyname='providers_service_write';
  IF NOT FOUND THEN CREATE POLICY providers_service_write ON public.providers FOR ALL TO service_role USING (true) WITH CHECK (true); END IF;
  PERFORM 1 FROM pg_policies WHERE schemaname='public' AND tablename='retailer_providers' AND policyname='retailer_providers_service_write';
  IF NOT FOUND THEN CREATE POLICY retailer_providers_service_write ON public.retailer_providers FOR ALL TO service_role USING (true) WITH CHECK (true); END IF;
  PERFORM 1 FROM pg_policies WHERE schemaname='public' AND tablename='provider_items' AND policyname='provider_items_service_write';
  IF NOT FOUND THEN CREATE POLICY provider_items_service_write ON public.provider_items FOR ALL TO service_role USING (true) WITH CHECK (true); END IF;
  PERFORM 1 FROM pg_policies WHERE schemaname='public' AND tablename='provider_offers' AND policyname='provider_offers_service_write';
  IF NOT FOUND THEN CREATE POLICY provider_offers_service_write ON public.provider_offers FOR ALL TO service_role USING (true) WITH CHECK (true); END IF;
  PERFORM 1 FROM pg_policies WHERE schemaname='public' AND tablename='provider_ingest_runs' AND policyname='provider_ingest_runs_service_write';
  IF NOT FOUND THEN CREATE POLICY provider_ingest_runs_service_write ON public.provider_ingest_runs FOR ALL TO service_role USING (true) WITH CHECK (true); END IF;
  PERFORM 1 FROM pg_policies WHERE schemaname='public' AND tablename='provider_media_links' AND policyname='provider_media_links_service_write';
  IF NOT FOUND THEN CREATE POLICY provider_media_links_service_write ON public.provider_media_links FOR ALL TO service_role USING (true) WITH CHECK (true); END IF;
  PERFORM 1 FROM pg_policies WHERE schemaname='public' AND tablename='prices' AND policyname='prices_service_write';
  IF NOT FOUND THEN CREATE POLICY prices_service_write ON public.prices FOR ALL TO service_role USING (true) WITH CHECK (true); END IF;
END $$;

-- 4. User-centric tables (users, alerts) – restrict to authenticated user context; fallback wide open if mapping unavailable.
-- NOTE: Since users.id is bigint (not auth.uid()), we cannot directly match Supabase JWT subject; allow SELECT for authenticated pending mapping.
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE schemaname='public' AND tablename='users' AND policyname='users_select_auth') THEN
    CREATE POLICY users_select_auth ON public.users FOR SELECT TO authenticated USING (true);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE schemaname='public' AND tablename='alerts' AND policyname='alerts_select_auth') THEN
    CREATE POLICY alerts_select_auth ON public.alerts FOR SELECT TO authenticated USING (true);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE schemaname='public' AND tablename='alerts' AND policyname='alerts_service_write') THEN
    CREATE POLICY alerts_service_write ON public.alerts FOR ALL TO service_role USING (true) WITH CHECK (true);
  END IF;
END $$;

-- 5. Partition policy propagation: redefine ensure_prices_partition_for to apply RLS & policies to new partitions.
CREATE OR REPLACE FUNCTION public.ensure_prices_partition_for(ts timestamptz)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  start_month date := date_trunc('month', ts)::date;
  next_month  date := (date_trunc('month', ts) + interval '1 month')::date;
  part_name   text := format('prices_%s', to_char(start_month, 'YYYY_MM'));
  sql text;
BEGIN
  IF to_regclass(part_name) IS NULL THEN
    sql := format('CREATE TABLE %I PARTITION OF public.prices FOR VALUES FROM (%L) TO (%L);', part_name, start_month, next_month);
    EXECUTE sql;
    -- Basic indexes (inheritance-safe)
    EXECUTE format('CREATE INDEX ON %I (offer_jurisdiction_id, recorded_at);', part_name);
    EXECUTE format('CREATE INDEX %I_recorded_at_idx ON %I (recorded_at);', part_name, part_name);
    -- Enable RLS and clone policies (simplified: service_role writes, authenticated reads)
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY;', part_name);
    -- Guard each policy creation via pg_policies check
    IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE tablename=part_name AND policyname='prices_partition_select') THEN
      EXECUTE format('CREATE POLICY prices_partition_select ON %I FOR SELECT TO authenticated USING (true);', part_name);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE tablename=part_name AND policyname='prices_partition_service_write') THEN
      EXECUTE format('CREATE POLICY prices_partition_service_write ON %I FOR ALL TO service_role USING (true) WITH CHECK (true);', part_name);
    END IF;
  END IF;
END$$;

-- 6. ANALYZE after structural changes.
ANALYZE public.prices;
ANALYZE public.current_price;
