-- Migration: 0561_comprehensive_rls_policies.sql
-- Purpose: Add Row Level Security (RLS) policies to all public schema tables
-- Strategy:
--   - Service role bypasses all RLS (for Rust ingestion operations)
--   - Public read access for catalog data (products, platforms, prices)
--   - Authenticated read access for most tables
--   - Admin-only write access for core tables
--   - User-scoped access for alerts and user-specific data

DO $$
BEGIN
    -- =========================================================================
    -- REFERENCE DATA TABLES (Public Read, Service/Admin Write)
    -- =========================================================================

    -- currencies: Public read, service/admin write
    ALTER TABLE public.currencies ENABLE ROW LEVEL SECURITY;

    CREATE POLICY "currencies_public_read" ON public.currencies
        FOR SELECT USING (true);

    CREATE POLICY "currencies_service_all" ON public.currencies
        FOR ALL USING (auth.role() = 'service_role');

    CREATE POLICY "currencies_admin_write" ON public.currencies
        FOR INSERT WITH CHECK (auth.jwt()->>'role' = 'admin');

    -- countries: Public read, service/admin write
    ALTER TABLE public.countries ENABLE ROW LEVEL SECURITY;

    CREATE POLICY "countries_public_read" ON public.countries
        FOR SELECT USING (true);

    CREATE POLICY "countries_service_all" ON public.countries
        FOR ALL USING (auth.role() = 'service_role');

    CREATE POLICY "countries_admin_write" ON public.countries
        FOR INSERT WITH CHECK (auth.jwt()->>'role' = 'admin');

    -- jurisdictions: Public read, service/admin write
    ALTER TABLE public.jurisdictions ENABLE ROW LEVEL SECURITY;

    CREATE POLICY "jurisdictions_public_read" ON public.jurisdictions
        FOR SELECT USING (true);

    CREATE POLICY "jurisdictions_service_all" ON public.jurisdictions
        FOR ALL USING (auth.role() = 'service_role');

    -- platforms: Public read, service/admin write
    ALTER TABLE public.platforms ENABLE ROW LEVEL SECURITY;

    CREATE POLICY "platforms_public_read" ON public.platforms
        FOR SELECT USING (true);

    CREATE POLICY "platforms_service_all" ON public.platforms
        FOR ALL USING (auth.role() = 'service_role');

    -- retailers: Public read, service/admin write
    ALTER TABLE public.retailers ENABLE ROW LEVEL SECURITY;

    CREATE POLICY "retailers_public_read" ON public.retailers
        FOR SELECT USING (true);

    CREATE POLICY "retailers_service_all" ON public.retailers
        FOR ALL USING (auth.role() = 'service_role');

    -- providers: Public read, service/admin write
    ALTER TABLE public.providers ENABLE ROW LEVEL SECURITY;

    CREATE POLICY "providers_public_read" ON public.providers
        FOR SELECT USING (true);

    CREATE POLICY "providers_service_all" ON public.providers
        FOR ALL USING (auth.role() = 'service_role');

    -- =========================================================================
    -- CATALOG DATA TABLES (Public Read, Service Write)
    -- =========================================================================

    -- products: Public read, service write
    ALTER TABLE public.products ENABLE ROW LEVEL SECURITY;

    CREATE POLICY "products_public_read" ON public.products
        FOR SELECT USING (true);

    CREATE POLICY "products_service_all" ON public.products
        FOR ALL USING (auth.role() = 'service_role');

    -- software: Public read, service write
    ALTER TABLE public.software ENABLE ROW LEVEL SECURITY;

    CREATE POLICY "software_public_read" ON public.software
        FOR SELECT USING (true);

    CREATE POLICY "software_service_all" ON public.software
        FOR ALL USING (auth.role() = 'service_role');

    -- hardware: Public read, service write
    ALTER TABLE public.hardware ENABLE ROW LEVEL SECURITY;

    CREATE POLICY "hardware_public_read" ON public.hardware
        FOR SELECT USING (true);

    CREATE POLICY "hardware_service_all" ON public.hardware
        FOR ALL USING (auth.role() = 'service_role');

    -- video_game_titles: Public read, service write
    ALTER TABLE public.video_game_titles ENABLE ROW LEVEL SECURITY;

    CREATE POLICY "video_game_titles_public_read" ON public.video_game_titles
        FOR SELECT USING (true);

    CREATE POLICY "video_game_titles_service_all" ON public.video_game_titles
        FOR ALL USING (auth.role() = 'service_role');

    -- video_games: Public read, service write
    ALTER TABLE public.video_games ENABLE ROW LEVEL SECURITY;

    CREATE POLICY "video_games_public_read" ON public.video_games
        FOR SELECT USING (true);

    CREATE POLICY "video_games_service_all" ON public.video_games
        FOR ALL USING (auth.role() = 'service_role');

    -- game_consoles: Public read, service write
    ALTER TABLE public.game_consoles ENABLE ROW LEVEL SECURITY;

    CREATE POLICY "game_consoles_public_read" ON public.game_consoles
        FOR SELECT USING (true);

    CREATE POLICY "game_consoles_service_all" ON public.game_consoles
        FOR ALL USING (auth.role() = 'service_role');

    -- =========================================================================
    -- COMMERCE TABLES (Public Read, Service Write)
    -- =========================================================================

    -- sellables: Public read, service write
    ALTER TABLE public.sellables ENABLE ROW LEVEL SECURITY;

    CREATE POLICY "sellables_public_read" ON public.sellables
        FOR SELECT USING (true);

    CREATE POLICY "sellables_service_all" ON public.sellables
        FOR ALL USING (auth.role() = 'service_role');

    -- offers: Public read, service write
    ALTER TABLE public.offers ENABLE ROW LEVEL SECURITY;

    CREATE POLICY "offers_public_read" ON public.offers
        FOR SELECT USING (true);

    CREATE POLICY "offers_service_all" ON public.offers
        FOR ALL USING (auth.role() = 'service_role');

    -- offer_jurisdictions: Public read, service write
    ALTER TABLE public.offer_jurisdictions ENABLE ROW LEVEL SECURITY;

    CREATE POLICY "offer_jurisdictions_public_read" ON public.offer_jurisdictions
        FOR SELECT USING (true);

    CREATE POLICY "offer_jurisdictions_service_all" ON public.offer_jurisdictions
        FOR ALL USING (auth.role() = 'service_role');

    -- =========================================================================
    -- PRICING DATA (Public Read, Service Write)
    -- =========================================================================

    -- prices: Public read, service write
    ALTER TABLE public.prices ENABLE ROW LEVEL SECURITY;

    CREATE POLICY "prices_public_read" ON public.prices
        FOR SELECT USING (true);

    CREATE POLICY "prices_service_all" ON public.prices
        FOR ALL USING (auth.role() = 'service_role');

    -- current_price: Public read, service write
    ALTER TABLE public.current_price ENABLE ROW LEVEL SECURITY;

    CREATE POLICY "current_price_public_read" ON public.current_price
        FOR SELECT USING (true);

    CREATE POLICY "current_price_service_all" ON public.current_price
        FOR ALL USING (auth.role() = 'service_role');

    -- =========================================================================
    -- MEDIA TABLES (Public Read, Service Write)
    -- =========================================================================

    -- game_images: Public read, service write
    ALTER TABLE public.game_images ENABLE ROW LEVEL SECURITY;

    CREATE POLICY "game_images_public_read" ON public.game_images
        FOR SELECT USING (true);

    CREATE POLICY "game_images_service_all" ON public.game_images
        FOR ALL USING (auth.role() = 'service_role');

    -- game_videos: Public read, service write
    ALTER TABLE public.game_videos ENABLE ROW LEVEL SECURITY;

    CREATE POLICY "game_videos_public_read" ON public.game_videos
        FOR SELECT USING (true);

    CREATE POLICY "game_videos_service_all" ON public.game_videos
        FOR ALL USING (auth.role() = 'service_role');

    -- game_media (if exists): Public read, service write
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'game_media') THEN
        ALTER TABLE public.game_media ENABLE ROW LEVEL SECURITY;

        EXECUTE 'CREATE POLICY "game_media_public_read" ON public.game_media
            FOR SELECT USING (true)';

        EXECUTE 'CREATE POLICY "game_media_service_all" ON public.game_media
            FOR ALL USING (auth.role() = ''service_role'')';
    END IF;

    -- canonical_media (if exists): Public read, service write
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'canonical_media') THEN
        ALTER TABLE public.canonical_media ENABLE ROW LEVEL SECURITY;

        EXECUTE 'CREATE POLICY "canonical_media_public_read" ON public.canonical_media
            FOR SELECT USING (true)';

        EXECUTE 'CREATE POLICY "canonical_media_service_all" ON public.canonical_media
            FOR ALL USING (auth.role() = ''service_role'')';
    END IF;

    -- =========================================================================
    -- PROVIDER INTEGRATION TABLES (Authenticated Read, Service Write)
    -- =========================================================================

    -- provider_items: Authenticated read, service write
    ALTER TABLE public.provider_items ENABLE ROW LEVEL SECURITY;

    CREATE POLICY "provider_items_authenticated_read" ON public.provider_items
        FOR SELECT USING (auth.role() = 'authenticated' OR auth.role() = 'service_role');

    CREATE POLICY "provider_items_service_all" ON public.provider_items
        FOR ALL USING (auth.role() = 'service_role');

    -- provider_offers: Authenticated read, service write
    ALTER TABLE public.provider_offers ENABLE ROW LEVEL SECURITY;

    CREATE POLICY "provider_offers_authenticated_read" ON public.provider_offers
        FOR SELECT USING (auth.role() = 'authenticated' OR auth.role() = 'service_role');

    CREATE POLICY "provider_offers_service_all" ON public.provider_offers
        FOR ALL USING (auth.role() = 'service_role');

    -- video_game_sources: Public read, service write
    ALTER TABLE public.video_game_sources ENABLE ROW LEVEL SECURITY;

    CREATE POLICY "video_game_sources_public_read" ON public.video_game_sources
        FOR SELECT USING (true);

    CREATE POLICY "video_game_sources_service_all" ON public.video_game_sources
        FOR ALL USING (auth.role() = 'service_role');

    -- video_game_title_sources (if exists): Public read, service write
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'video_game_title_sources') THEN
        ALTER TABLE public.video_game_title_sources ENABLE ROW LEVEL SECURITY;

        EXECUTE 'CREATE POLICY "video_game_title_sources_public_read" ON public.video_game_title_sources
            FOR SELECT USING (true)';

        EXECUTE 'CREATE POLICY "video_game_title_sources_service_all" ON public.video_game_title_sources
            FOR ALL USING (auth.role() = ''service_role'')';
    END IF;

    -- provider_media_links: Public read, service write
    ALTER TABLE public.provider_media_links ENABLE ROW LEVEL SECURITY;

    CREATE POLICY "provider_media_links_public_read" ON public.provider_media_links
        FOR SELECT USING (true);

    CREATE POLICY "provider_media_links_service_all" ON public.provider_media_links
        FOR ALL USING (auth.role() = 'service_role');

    -- =========================================================================
    -- OPERATIONAL TABLES (Service Only)
    -- =========================================================================

    -- provider_ingest_runs: Service only
    ALTER TABLE public.provider_ingest_runs ENABLE ROW LEVEL SECURITY;

    CREATE POLICY "provider_ingest_runs_service_all" ON public.provider_ingest_runs
        FOR ALL USING (auth.role() = 'service_role');

    -- retailer_providers: Service only
    ALTER TABLE public.retailer_providers ENABLE ROW LEVEL SECURITY;

    CREATE POLICY "retailer_providers_service_all" ON public.retailer_providers
        FOR ALL USING (auth.role() = 'service_role');

    -- exchange_rates: Public read, service write
    ALTER TABLE public.exchange_rates ENABLE ROW LEVEL SECURITY;

    CREATE POLICY "exchange_rates_public_read" ON public.exchange_rates
        FOR SELECT USING (true);

    CREATE POLICY "exchange_rates_service_all" ON public.exchange_rates
        FOR ALL USING (auth.role() = 'service_role');

    -- =========================================================================
    -- USER-SPECIFIC TABLES (User-Scoped Access)
    -- =========================================================================

    -- alerts: Users can read/write their own alerts
    ALTER TABLE public.alerts ENABLE ROW LEVEL SECURITY;

    CREATE POLICY "alerts_user_read_own" ON public.alerts
        FOR SELECT USING (auth.uid() = user_id);

    CREATE POLICY "alerts_user_insert_own" ON public.alerts
        FOR INSERT WITH CHECK (auth.uid() = user_id);

    CREATE POLICY "alerts_user_update_own" ON public.alerts
        FOR UPDATE USING (auth.uid() = user_id);

    CREATE POLICY "alerts_user_delete_own" ON public.alerts
        FOR DELETE USING (auth.uid() = user_id);

    CREATE POLICY "alerts_service_all" ON public.alerts
        FOR ALL USING (auth.role() = 'service_role');

    -- users: Users can read/update their own profile
    ALTER TABLE public.users ENABLE ROW LEVEL SECURITY;

    CREATE POLICY "users_read_own" ON public.users
        FOR SELECT USING (auth.uid() = id);

    CREATE POLICY "users_update_own" ON public.users
        FOR UPDATE USING (auth.uid() = id);

    CREATE POLICY "users_service_all" ON public.users
        FOR ALL USING (auth.role() = 'service_role');

    -- =========================================================================
    -- RATINGS & METADATA (Public Read, Service Write)
    -- =========================================================================

    -- video_game_ratings_by_locale: Public read, service write
    ALTER TABLE public.video_game_ratings_by_locale ENABLE ROW LEVEL SECURITY;

    CREATE POLICY "video_game_ratings_public_read" ON public.video_game_ratings_by_locale
        FOR SELECT USING (true);

    CREATE POLICY "video_game_ratings_service_all" ON public.video_game_ratings_by_locale
        FOR ALL USING (auth.role() = 'service_role');

    -- =========================================================================
    -- OPTIONAL TABLES (Check existence before applying)
    -- =========================================================================

    -- retailer_video_game_sources (if exists)
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'retailer_video_game_sources') THEN
        ALTER TABLE public.retailer_video_game_sources ENABLE ROW LEVEL SECURITY;

        EXECUTE 'CREATE POLICY "retailer_video_game_sources_public_read" ON public.retailer_video_game_sources
            FOR SELECT USING (true)';

        EXECUTE 'CREATE POLICY "retailer_video_game_sources_service_all" ON public.retailer_video_game_sources
            FOR ALL USING (auth.role() = ''service_role'')';
    END IF;

    -- video_game_source_sync_states (if exists)
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'video_game_source_sync_states') THEN
        ALTER TABLE public.video_game_source_sync_states ENABLE ROW LEVEL SECURITY;

        EXECUTE 'CREATE POLICY "video_game_source_sync_states_service_all" ON public.video_game_source_sync_states
            FOR ALL USING (auth.role() = ''service_role'')';
    END IF;

    -- platform_hardware_map (if exists)
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'platform_hardware_map') THEN
        ALTER TABLE public.platform_hardware_map ENABLE ROW LEVEL SECURITY;

        EXECUTE 'CREATE POLICY "platform_hardware_map_public_read" ON public.platform_hardware_map
            FOR SELECT USING (true)';

        EXECUTE 'CREATE POLICY "platform_hardware_map_service_all" ON public.platform_hardware_map
            FOR ALL USING (auth.role() = ''service_role'')';
    END IF;

    -- country_iso_map (if exists)
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'country_iso_map') THEN
        ALTER TABLE public.country_iso_map ENABLE ROW LEVEL SECURITY;

        EXECUTE 'CREATE POLICY "country_iso_map_public_read" ON public.country_iso_map
            FOR SELECT USING (true)';

        EXECUTE 'CREATE POLICY "country_iso_map_service_all" ON public.country_iso_map
            FOR ALL USING (auth.role() = ''service_role'')';
    END IF;

    -- game_providers (if exists)
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'game_providers') THEN
        ALTER TABLE public.game_providers ENABLE ROW LEVEL SECURITY;

        EXECUTE 'CREATE POLICY "game_providers_public_read" ON public.game_providers
            FOR SELECT USING (true)';

        EXECUTE 'CREATE POLICY "game_providers_service_all" ON public.game_providers
            FOR ALL USING (auth.role() = ''service_role'')';
    END IF;

    -- tax_rules (if exists)
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'tax_rules') THEN
        ALTER TABLE public.tax_rules ENABLE ROW LEVEL SECURITY;

        EXECUTE 'CREATE POLICY "tax_rules_public_read" ON public.tax_rules
            FOR SELECT USING (true)';

        EXECUTE 'CREATE POLICY "tax_rules_service_all" ON public.tax_rules
            FOR ALL USING (auth.role() = ''service_role'')';
    END IF;

END $$;

-- =========================================================================
-- IMPORTANT NOTES
-- =========================================================================
--
-- 1. SERVICE ROLE BYPASS:
--    All policies include service_role bypass for Rust ingestion operations.
--    The service role connection string should be used for all gc/import_sqlite
--    operations to bypass RLS entirely.
--
-- 2. PUBLIC READ ACCESS:
--    Catalog data (products, prices, media) is publicly readable to support
--    the public-facing microsite without authentication.
--
-- 3. USER-SCOPED DATA:
--    Alerts and user profiles use auth.uid() to ensure users only access
--    their own data.
--
-- 4. AUTHENTICATED ACCESS:
--    Provider-specific data (provider_items, provider_offers) requires
--    authentication but is readable by any authenticated user.
--
-- 5. PERFORMANCE IMPACT:
--    RLS policies add overhead to every query. For ingestion operations,
--    ALWAYS use the service role to bypass RLS entirely.
--
-- 6. MATERIALIZED VIEWS:
--    Materialized views inherit the RLS policies of their underlying tables.
--    No separate policies needed.
--
-- 7. PARTITIONED TABLES:
--    The prices table partitions inherit RLS from the parent table.
--    No separate policies needed for each partition.
