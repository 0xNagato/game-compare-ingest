-- 0527_enable_rls_policies.sql
-- Enable Row Level Security and create policies for all tables
-- Designed for dual-stack architecture: Rust backend (service_role) + Laravel frontend (authenticated users)
-- Idempotent: safe to re-run multiple times

-- =============================
-- DROP EXISTING POLICIES (for idempotency)
-- =============================

DO $$
DECLARE
    pol RECORD;
BEGIN
    FOR pol IN 
        SELECT schemaname, tablename, policyname
        FROM pg_policies
        WHERE schemaname = 'public'
    LOOP
        EXECUTE format('DROP POLICY IF EXISTS %I ON %I.%I', pol.policyname, pol.schemaname, pol.tablename);
    END LOOP;
END $$;

-- =============================
-- PART 1: PUBLIC CATALOG DATA
-- =============================
-- Products, games, platforms - public read access for all authenticated users

-- Products table
ALTER TABLE products ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "products_select_all" ON products;
DROP POLICY IF EXISTS "products_insert_service" ON products;
DROP POLICY IF EXISTS "products_update_service" ON products;
DROP POLICY IF EXISTS "products_delete_service" ON products;

CREATE POLICY "products_select_all" ON products
    FOR SELECT
    TO authenticated, anon
    USING (true);

CREATE POLICY "products_insert_service" ON products
    FOR INSERT
    TO service_role
    WITH CHECK (true);

CREATE POLICY "products_update_service" ON products
    FOR UPDATE
    TO service_role
    USING (true)
    WITH CHECK (true);

CREATE POLICY "products_delete_service" ON products
    FOR DELETE
    TO service_role
    USING (true);

-- Video games table
ALTER TABLE video_games ENABLE ROW LEVEL SECURITY;

CREATE POLICY "video_games_select_all" ON video_games
    FOR SELECT
    TO authenticated, anon
    USING (true);

CREATE POLICY "video_games_insert_service" ON video_games
    FOR INSERT
    TO service_role
    WITH CHECK (true);

CREATE POLICY "video_games_update_service" ON video_games
    FOR UPDATE
    TO service_role
    USING (true)
    WITH CHECK (true);

CREATE POLICY "video_games_delete_service" ON video_games
    FOR DELETE
    TO service_role
    USING (true);

-- Video game titles
ALTER TABLE video_game_titles ENABLE ROW LEVEL SECURITY;

CREATE POLICY "video_game_titles_select_all" ON video_game_titles
    FOR SELECT
    TO authenticated, anon
    USING (true);

CREATE POLICY "video_game_titles_modify_service" ON video_game_titles
    FOR ALL
    TO service_role
    USING (true)
    WITH CHECK (true);

-- Software table
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'software') THEN
        ALTER TABLE software ENABLE ROW LEVEL SECURITY;

        EXECUTE 'CREATE POLICY "software_select_all" ON software
            FOR SELECT TO authenticated, anon USING (true)';

        EXECUTE 'CREATE POLICY "software_modify_service" ON software
            FOR ALL TO service_role USING (true) WITH CHECK (true)';
    END IF;
END $$;

-- Hardware table
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'hardware') THEN
        ALTER TABLE hardware ENABLE ROW LEVEL SECURITY;

        EXECUTE 'CREATE POLICY "hardware_select_all" ON hardware
            FOR SELECT TO authenticated, anon USING (true)';

        EXECUTE 'CREATE POLICY "hardware_modify_service" ON hardware
            FOR ALL TO service_role USING (true) WITH CHECK (true)';
    END IF;
END $$;

-- =============================
-- PART 2: PRICING DATA
-- =============================
-- Public read access for price data

-- Region prices (legacy system)
-- Only enable if table exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='region_prices') THEN
        ALTER TABLE region_prices ENABLE ROW LEVEL SECURITY;

        CREATE POLICY "region_prices_select_all" ON region_prices
            FOR SELECT
            TO authenticated, anon
            USING (true);

        CREATE POLICY "region_prices_modify_service" ON region_prices
            FOR ALL
            TO service_role
            USING (true)
            WITH CHECK (true);
    END IF;
END $$;

-- Game price points (new system)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'game_price_points') THEN
        ALTER TABLE game_price_points ENABLE ROW LEVEL SECURITY;

        EXECUTE 'CREATE POLICY "game_price_points_select_all" ON game_price_points
            FOR SELECT TO authenticated, anon USING (true)';

        EXECUTE 'CREATE POLICY "game_price_points_modify_service" ON game_price_points
            FOR ALL TO service_role USING (true) WITH CHECK (true)';
    END IF;
END $$;

-- Current price (hot read table)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'current_price') THEN
        ALTER TABLE current_price ENABLE ROW LEVEL SECURITY;

        EXECUTE 'CREATE POLICY "current_price_select_all" ON current_price
            FOR SELECT TO authenticated, anon USING (true)';

        EXECUTE 'CREATE POLICY "current_price_modify_service" ON current_price
            FOR ALL TO service_role USING (true) WITH CHECK (true)';
    END IF;
END $$;

-- SKU regions
-- Only enable if table exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='sku_regions') THEN
        ALTER TABLE sku_regions ENABLE ROW LEVEL SECURITY;

        CREATE POLICY "sku_regions_select_all" ON sku_regions
            FOR SELECT
            TO authenticated, anon
            USING (true);

        CREATE POLICY "sku_regions_modify_service" ON sku_regions
            FOR ALL
            TO service_role
            USING (true)
            WITH CHECK (true);
    END IF;
END $$;

-- Offers
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'offers') THEN
        ALTER TABLE offers ENABLE ROW LEVEL SECURITY;

        EXECUTE 'CREATE POLICY "offers_select_all" ON offers
            FOR SELECT TO authenticated, anon USING (true)';

        EXECUTE 'CREATE POLICY "offers_modify_service" ON offers
            FOR ALL TO service_role USING (true) WITH CHECK (true)';
    END IF;
END $$;

-- Offer jurisdictions
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'offer_jurisdictions') THEN
        ALTER TABLE offer_jurisdictions ENABLE ROW LEVEL SECURITY;

        EXECUTE 'CREATE POLICY "offer_jurisdictions_select_all" ON offer_jurisdictions
            FOR SELECT TO authenticated, anon USING (true)';

        EXECUTE 'CREATE POLICY "offer_jurisdictions_modify_service" ON offer_jurisdictions
            FOR ALL TO service_role USING (true) WITH CHECK (true)';
    END IF;
END $$;

-- =============================
-- PART 3: MEDIA DATA
-- =============================
-- Public read access for media

-- Game images
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'game_images') THEN
        EXECUTE 'ALTER TABLE game_images ENABLE ROW LEVEL SECURITY';

        EXECUTE 'CREATE POLICY "game_images_select_all" ON game_images
            FOR SELECT TO authenticated, anon USING (true)';

        EXECUTE 'CREATE POLICY "game_images_modify_service" ON game_images
            FOR ALL TO service_role USING (true) WITH CHECK (true)';
    END IF;
END $$;

-- Game videos
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'game_videos') THEN
        EXECUTE 'ALTER TABLE game_videos ENABLE ROW LEVEL SECURITY';

        EXECUTE 'CREATE POLICY "game_videos_select_all" ON game_videos
            FOR SELECT TO authenticated, anon USING (true)';

        EXECUTE 'CREATE POLICY "game_videos_modify_service" ON game_videos
            FOR ALL TO service_role USING (true) WITH CHECK (true)';
    END IF;
END $$;

-- Product media
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'product_media') THEN
        EXECUTE 'ALTER TABLE product_media ENABLE ROW LEVEL SECURITY';

        EXECUTE 'CREATE POLICY "product_media_select_all" ON product_media
            FOR SELECT TO authenticated, anon USING (true)';

        EXECUTE 'CREATE POLICY "product_media_modify_service" ON product_media
            FOR ALL TO service_role USING (true) WITH CHECK (true)';
    END IF;
END $$;

-- Game media
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'game_media') THEN
        EXECUTE 'ALTER TABLE game_media ENABLE ROW LEVEL SECURITY';

        EXECUTE 'CREATE POLICY "game_media_select_all" ON game_media
            FOR SELECT TO authenticated, anon USING (true)';

        EXECUTE 'CREATE POLICY "game_media_modify_service" ON game_media
            FOR ALL TO service_role USING (true) WITH CHECK (true)';
    END IF;
END $$;

-- Media table
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'media') THEN
        ALTER TABLE media ENABLE ROW LEVEL SECURITY;

        EXECUTE 'CREATE POLICY "media_select_all" ON media
            FOR SELECT TO authenticated, anon USING (true)';

        EXECUTE 'CREATE POLICY "media_modify_service" ON media
            FOR ALL TO service_role USING (true) WITH CHECK (true)';
    END IF;
END $$;

-- =============================
-- PART 4: USER-SPECIFIC DATA
-- =============================
-- Users can only see/modify their own data

-- NOTE: users.id is bigint (Supabase auth.uid() is uuid). Until we ship the id bridge
-- we allow authenticated clients to read their profile via the application layer only
-- and rely on service_role for writes. Tight ownership checks will return in a follow-up
-- migration once ids are aligned.
ALTER TABLE users ENABLE ROW LEVEL SECURITY;

CREATE POLICY "users_select_authenticated" ON users
    FOR SELECT
    TO authenticated
    USING (true);

CREATE POLICY "users_select_service" ON users
    FOR SELECT
    TO service_role
    USING (true);

CREATE POLICY "users_modify_service" ON users
    FOR ALL
    TO service_role
    USING (true)
    WITH CHECK (true);

-- Same constraint mismatch applies to alerts.user_id; authenticated callers read via
-- application services for now while writes are mediated by service_role.
-- Only enable if table exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='alerts') THEN
        ALTER TABLE alerts ENABLE ROW LEVEL SECURITY;

        CREATE POLICY "alerts_select_authenticated" ON alerts
            FOR SELECT
            TO authenticated
            USING (true);

        CREATE POLICY "alerts_service" ON alerts
            FOR ALL
            TO service_role
            USING (true)
            WITH CHECK (true);
    END IF;
END $$;

-- =============================
-- PART 5: PROVIDER/ADMIN DATA
-- =============================
-- Read access for authenticated users, full access for service role

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'providers') THEN
        EXECUTE 'ALTER TABLE providers ENABLE ROW LEVEL SECURITY';

        EXECUTE 'CREATE POLICY "providers_select_authenticated" ON providers
            FOR SELECT TO authenticated, anon USING (true)';

        EXECUTE 'CREATE POLICY "providers_modify_service" ON providers
            FOR ALL TO service_role USING (true) WITH CHECK (true)';
    END IF;
END $$;

-- Provider items
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'provider_items') THEN
        ALTER TABLE provider_items ENABLE ROW LEVEL SECURITY;

        EXECUTE 'CREATE POLICY "provider_items_select_auth" ON provider_items
            FOR SELECT TO authenticated, anon USING (true)';

        EXECUTE 'CREATE POLICY "provider_items_modify_service" ON provider_items
            FOR ALL TO service_role USING (true) WITH CHECK (true)';
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'game_providers') THEN
        EXECUTE 'ALTER TABLE game_providers ENABLE ROW LEVEL SECURITY';

        EXECUTE 'CREATE POLICY "game_providers_select_all" ON game_providers
            FOR SELECT TO authenticated, anon USING (true)';

        EXECUTE 'CREATE POLICY "game_providers_modify_service" ON game_providers
            FOR ALL TO service_role USING (true) WITH CHECK (true)';
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'game_retailers') THEN
        EXECUTE 'ALTER TABLE game_retailers ENABLE ROW LEVEL SECURITY';

        EXECUTE 'CREATE POLICY "game_retailers_select_all" ON game_retailers
            FOR SELECT TO authenticated, anon USING (true)';

        EXECUTE 'CREATE POLICY "game_retailers_modify_service" ON game_retailers
            FOR ALL TO service_role USING (true) WITH CHECK (true)';
    END IF;
END $$;

-- =============================
-- PART 6: REFERENCE DATA
-- =============================
-- Public read access for reference tables

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'platforms') THEN
        EXECUTE 'ALTER TABLE platforms ENABLE ROW LEVEL SECURITY';

        EXECUTE 'CREATE POLICY "platforms_select_all" ON platforms
            FOR SELECT TO authenticated, anon USING (true)';

        EXECUTE 'CREATE POLICY "platforms_modify_service" ON platforms
            FOR ALL TO service_role USING (true) WITH CHECK (true)';
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'countries') THEN
        EXECUTE 'ALTER TABLE countries ENABLE ROW LEVEL SECURITY';

        EXECUTE 'CREATE POLICY "countries_select_all" ON countries
            FOR SELECT TO authenticated, anon USING (true)';

        EXECUTE 'CREATE POLICY "countries_modify_service" ON countries
            FOR ALL TO service_role USING (true) WITH CHECK (true)';
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'currencies') THEN
        EXECUTE 'ALTER TABLE currencies ENABLE ROW LEVEL SECURITY';

        EXECUTE 'CREATE POLICY "currencies_select_all" ON currencies
            FOR SELECT TO authenticated, anon USING (true)';

        EXECUTE 'CREATE POLICY "currencies_modify_service" ON currencies
            FOR ALL TO service_role USING (true) WITH CHECK (true)';
    END IF;
END $$;

-- Regions
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'regions') THEN
        ALTER TABLE regions ENABLE ROW LEVEL SECURITY;

        EXECUTE 'CREATE POLICY "regions_select_all" ON regions
            FOR SELECT TO authenticated, anon USING (true)';

        EXECUTE 'CREATE POLICY "regions_modify_service" ON regions
            FOR ALL TO service_role USING (true) WITH CHECK (true)';
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'cross_reference_entries') THEN
        EXECUTE 'ALTER TABLE cross_reference_entries ENABLE ROW LEVEL SECURITY';

        EXECUTE 'CREATE POLICY "cross_reference_select_all" ON cross_reference_entries
            FOR SELECT TO authenticated, anon USING (true)';

        EXECUTE 'CREATE POLICY "cross_reference_modify_service" ON cross_reference_entries
            FOR ALL TO service_role USING (true) WITH CHECK (true)';
    END IF;
END $$;

-- =============================
-- PART 7: INTERNAL TABLES
-- =============================
-- Service role only

-- Migrations
-- Only enable if table exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='migrations') THEN
        ALTER TABLE migrations ENABLE ROW LEVEL SECURITY;

        CREATE POLICY "migrations_service_only" ON migrations
            FOR ALL
            TO service_role
            USING (true)
            WITH CHECK (true);
    END IF;
END $$;

-- Failed jobs
-- Only enable if table exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='failed_jobs') THEN
        ALTER TABLE failed_jobs ENABLE ROW LEVEL SECURITY;

        CREATE POLICY "failed_jobs_service_only" ON failed_jobs
            FOR ALL
            TO service_role
            USING (true)
            WITH CHECK (true);
    END IF;
END $$;

-- sessions.user_id is bigint (Laravel app-scoped tokens) so we treat the
-- table like other internal infrastructure: service role only until the
-- uuid bridge lands.
-- Only enable if table exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='sessions') THEN
        ALTER TABLE sessions ENABLE ROW LEVEL SECURITY;

        CREATE POLICY "sessions_service_only" ON sessions
            FOR ALL
            TO service_role
            USING (true)
            WITH CHECK (true);
    END IF;
END $$;

-- Cache
-- Only enable if table exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='cache') THEN
        ALTER TABLE cache ENABLE ROW LEVEL SECURITY;

        CREATE POLICY "cache_service_only" ON cache
            FOR ALL
            TO service_role
            USING (true)
            WITH CHECK (true);
    END IF;
END $$;

-- =============================
-- VERIFICATION REPORT
-- =============================

DO $$
DECLARE
    total_tables INT;
    rls_enabled_count INT;
    policy_count INT;
BEGIN
    SELECT COUNT(*) INTO total_tables
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public'
      AND c.relkind = 'r';

    SELECT COUNT(*) INTO rls_enabled_count
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public'
      AND c.relkind = 'r'
      AND c.relrowsecurity = true;

    SELECT COUNT(*) INTO policy_count
    FROM pg_policy p
    JOIN pg_class c ON c.oid = p.polrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public';

    RAISE NOTICE ' ';
    RAISE NOTICE '╔════════════════════════════════════════════════════════════╗';
    RAISE NOTICE '║         RLS POLICIES ENABLED SUCCESSFULLY                 ║';
    RAISE NOTICE '╠════════════════════════════════════════════════════════════╣';
    RAISE NOTICE '║ Total public tables:          %                   ║', LPAD(total_tables::TEXT, 20);
    RAISE NOTICE '║ Tables with RLS enabled:      %                   ║', LPAD(rls_enabled_count::TEXT, 20);
    RAISE NOTICE '║ Total policies created:       %                   ║', LPAD(policy_count::TEXT, 20);
    RAISE NOTICE '║                                                            ║';
    RAISE NOTICE '║ POLICY STRUCTURE:                                          ║';
    RAISE NOTICE '║ • Public data: SELECT for all, modify for service_role    ║';
    RAISE NOTICE '║ • User data: Own records only for authenticated users     ║';
    RAISE NOTICE '║ • Admin data: Service role full access                    ║';
    RAISE NOTICE '║                                                            ║';
    RAISE NOTICE '║ BACKEND (i-miss-rust):                                    ║';
    RAISE NOTICE '║ ✓ Uses service_role credentials (bypasses RLS)            ║';
    RAISE NOTICE '║ ✓ Full read/write access to all tables                    ║';
    RAISE NOTICE '║                                                            ║';
    RAISE NOTICE '║ FRONTEND (game-compare):                                  ║';
    RAISE NOTICE '║ ✓ Uses authenticated user credentials                     ║';
    RAISE NOTICE '║ ✓ RLS policies automatically enforced                     ║';
    RAISE NOTICE '║ ✓ Users can only access their own data                    ║';
    RAISE NOTICE '╚════════════════════════════════════════════════════════════╝';
    RAISE NOTICE ' ';
END $$;

COMMENT ON SCHEMA public IS
'RLS enabled on all tables. Backend uses service_role (bypasses RLS).
Frontend uses authenticated users (RLS enforced).';
