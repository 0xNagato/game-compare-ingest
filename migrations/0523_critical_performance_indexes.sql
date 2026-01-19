-- 0523_critical_performance_indexes.sql
-- Performance optimization indexes based on query pattern analysis
-- Addresses N+1 queries, table scans, and missing composite indexes

-- =============================
-- PRODUCT CATALOG INDEXES
-- =============================

-- ProductIndexController + GameDetailController: Sorting by popularity/rating
-- Only create if popularity_score column exists
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='public' AND table_name='products' AND column_name='popularity_score'
    ) THEN
        CREATE INDEX IF NOT EXISTS idx_products_popularity_rating
            ON products(popularity_score DESC, rating DESC, freshness_score DESC)
            WHERE popularity_score > 0;
    END IF;
END $$;

-- Filament searchable columns: Full-text search on product names
CREATE INDEX IF NOT EXISTS idx_products_name_trgm
    ON products USING gin(name gin_trgm_ops);

-- Platform filtering (common in API queries)
-- Only create if platform column exists
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='public' AND table_name='products' AND column_name='platform'
    ) THEN
        CREATE INDEX IF NOT EXISTS idx_products_platform
            ON products(platform);
    END IF;
END $$;

-- Slug pattern matching for autocomplete
CREATE INDEX IF NOT EXISTS idx_products_slug_pattern
    ON products(slug text_pattern_ops);

-- =============================
-- PRICING & OFFER INDEXES
-- =============================

-- ensure_offer_jurisdiction: Composite lookup (product_id, region_code, retailer)
-- Used extensively in Rust ingestion: src/database_ops/ingest_providers.rs:3348
-- Only create if sku_regions table exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='sku_regions') THEN
        CREATE INDEX IF NOT EXISTS idx_sku_regions_lookup
            ON sku_regions(product_id, region_code, retailer);

        -- Additional sku_regions index for region-based queries
        CREATE INDEX IF NOT EXISTS idx_sku_regions_region
            ON sku_regions(region_code, is_active)
            WHERE is_active = true;
    END IF;
END $$;

-- Provider offer linking: link_provider_offer lookups
-- Only create if provider_offers table exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='provider_offers') THEN
        CREATE INDEX IF NOT EXISTS idx_provider_offers_lookup
            ON provider_offers(provider_item_id, offer_id);

        -- Reverse lookup: find provider offers by canonical offer_id
        CREATE INDEX IF NOT EXISTS idx_provider_offers_offer_id
            ON provider_offers(offer_id);
    END IF;
END $$;

-- Price series aggregation queries
-- Only create if price_series_aggregates table exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='price_series_aggregates') THEN
        CREATE INDEX IF NOT EXISTS idx_price_aggregates_product_bucket
            ON price_series_aggregates(product_id, bucket, window_start DESC);

        -- Region-specific price aggregations
        CREATE INDEX IF NOT EXISTS idx_price_aggregates_region_window
            ON price_series_aggregates(region_code, bucket, window_start DESC);
    END IF;
END $$;

-- =============================
-- VIDEO GAME & TITLE INDEXES
-- =============================

-- Only create if video_game_titles table exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='video_game_titles') THEN
        -- Video game title normalization lookups
        -- Used in: ensure_video_game_title and title matching
        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='public' AND table_name='video_game_titles'
            AND column_name='normalized_title'
        ) THEN
            CREATE INDEX IF NOT EXISTS idx_video_game_titles_normalized
                ON video_game_titles(video_game_id, normalized_title);
        END IF;

        -- Provider item lookups for title resolution
        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='public' AND table_name='video_game_titles'
            AND column_name='video_game_source_id'
        ) THEN
            CREATE INDEX IF NOT EXISTS idx_video_game_titles_source_item
                ON video_game_titles(video_game_source_id, provider_item_id);
        END IF;
    END IF;
END $$;

-- Only create if video_games table exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='video_games') THEN
        -- Product-based title lookups (used in deduplication)
        -- Only create if product_id column exists
        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='public' AND table_name='video_games'
            AND column_name='product_id'
        ) THEN
            CREATE INDEX IF NOT EXISTS idx_video_games_product
                ON video_games(product_id);
        END IF;

        -- Normalized title searches across video games
        -- Only create if normalized_title column exists
        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='public' AND table_name='video_games'
            AND column_name='normalized_title'
        ) THEN
            CREATE INDEX IF NOT EXISTS idx_video_games_normalized_title
                ON video_games(normalized_title);
        END IF;

        -- Full-text search on video game titles
        -- Only create if title column exists
        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='public' AND table_name='video_games'
            AND column_name='title'
        ) THEN
            CREATE INDEX IF NOT EXISTS idx_video_games_title_trgm
                ON video_games USING gin(title gin_trgm_ops);
        END IF;
    END IF;
END $$;

-- =============================
-- MEDIA INDEXES
-- =============================

-- Only create if product_media table exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='product_media') THEN
        -- ProductMediaResolver: Quality-scored media selection
        CREATE INDEX IF NOT EXISTS idx_product_media_quality
            ON product_media(product_id, is_primary DESC, quality_score DESC, fetched_at DESC);
    END IF;
END $$;

-- Only create if game_images table exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='game_images') THEN
        -- Game images provider lookups
        CREATE INDEX IF NOT EXISTS idx_game_images_provider_item
            ON game_images(video_game_source_id, provider_item_id);

        -- Game images by game provider (BuildComparePageDataAction)
        CREATE INDEX IF NOT EXISTS idx_game_images_game_provider
            ON game_images(game_provider_id, rank);
    END IF;
END $$;

-- Only create if game_videos table exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='game_videos') THEN
        -- Game videos provider lookups
        CREATE INDEX IF NOT EXISTS idx_game_videos_provider_item
            ON game_videos(video_game_source_id, provider_item_id);

        -- Game videos by game provider
        CREATE INDEX IF NOT EXISTS idx_game_videos_game_provider
            ON game_videos(game_provider_id);
    END IF;
END $$;

-- Only create if media table exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='media') THEN
        -- Media library polymorphic lookups (Spatie)
        CREATE INDEX IF NOT EXISTS idx_media_model
            ON media(model_type, model_id, collection_name);
    END IF;
END $$;

-- =============================
-- PROVIDER SYSTEM INDEXES
-- =============================

-- Only create if game_providers table exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='game_providers') THEN
        -- Game providers polymorphic lookups
        CREATE INDEX IF NOT EXISTS idx_game_providers_providable
            ON game_providers(providable_type, providable_id);

        -- Provider key lookups
        CREATE INDEX IF NOT EXISTS idx_game_providers_key
            ON game_providers(provider_key);

        -- Video game source lookups
        CREATE INDEX IF NOT EXISTS idx_game_providers_source
            ON game_providers(video_game_source_id);
    END IF;
END $$;

-- Only create if provider_media_links table exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='provider_media_links') THEN
        -- Provider media links for backfill operations
        CREATE INDEX IF NOT EXISTS idx_provider_media_links_provider
            ON provider_media_links(provider_item_id);
    END IF;
END $$;

-- =============================
-- REGION PRICES INDEXES
-- (Conditional - only if NOT partitioned)
-- =============================

-- Check if region_prices exists and is not partitioned before creating indexes
DO $$
BEGIN
    -- Only create index if region_prices table exists AND is NOT a partitioned table
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='region_prices') THEN
        -- Now that we know the table exists, check if it's partitioned
        IF NOT EXISTS (
            SELECT 1 FROM pg_partitioned_table
            WHERE partrelid = 'region_prices'::regclass
        ) THEN
            -- Latest price lookups per SKU region
            CREATE INDEX IF NOT EXISTS idx_region_prices_sku_latest
                ON region_prices(sku_region_id, recorded_at DESC);

            -- Time-range queries for price history
            CREATE INDEX IF NOT EXISTS idx_region_prices_recorded_at
                ON region_prices(recorded_at DESC)
                WHERE recorded_at >= NOW() - INTERVAL '90 days';
        END IF;
    END IF;
END $$;

-- =============================
-- GAME PRICE POINTS INDEXES
-- (New pricing system)
-- =============================

-- Only create if game_price_points table exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='game_price_points') THEN
        -- Latest price points per retailer
        CREATE INDEX IF NOT EXISTS idx_game_price_points_retailer_collected
            ON game_price_points(game_retailer_id, collected_at DESC);

        -- Sale price filtering
        CREATE INDEX IF NOT EXISTS idx_game_price_points_sale
            ON game_price_points(game_retailer_id, is_sale)
            WHERE is_sale = true;
    END IF;
END $$;

-- Only create if game_retailers table exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='game_retailers') THEN
        -- Game retailers by provider
        CREATE INDEX IF NOT EXISTS idx_game_retailers_provider
            ON game_retailers(game_provider_id);

        -- Game retailers by video game source
        CREATE INDEX IF NOT EXISTS idx_game_retailers_source
            ON game_retailers(video_game_source_id);
    END IF;
END $$;

-- =============================
-- CROSS REFERENCE INDEXES
-- =============================

-- Only create if cross_reference_entries table exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='cross_reference_entries') THEN
        -- Cross-reference normalized key lookups (BuildComparePageDataAction)
        -- Already has UNIQUE constraint, but explicit index helps
        CREATE INDEX IF NOT EXISTS idx_cross_ref_entries_key
            ON cross_reference_entries(normalized_key);

        -- Digital/physical availability filtering
        CREATE INDEX IF NOT EXISTS idx_cross_ref_entries_availability
            ON cross_reference_entries(has_digital, has_physical);
    END IF;
END $$;

-- =============================
-- ALERT & NOTIFICATION INDEXES
-- =============================

-- Only create if alerts table exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='alerts') THEN
        -- Active alerts by user
        CREATE INDEX IF NOT EXISTS idx_alerts_user_active
            ON alerts(user_id, is_active)
            WHERE is_active = true;

        -- Active alerts by product and region
        CREATE INDEX IF NOT EXISTS idx_alerts_product_region
            ON alerts(product_id, region_code, is_active)
            WHERE is_active = true;
    END IF;
END $$;

-- Only create if notification_logs table exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='notification_logs') THEN
        -- Notification logs by alert
        CREATE INDEX IF NOT EXISTS idx_notification_logs_alert
            ON notification_logs(alert_id, status, sent_at DESC);
    END IF;
END $$;

-- =============================
-- PLATFORM & GENRE INDEXES
-- =============================

-- Only create if platforms table exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='platforms') THEN
        -- Platform code lookups (already UNIQUE, but explicit helps)
        CREATE INDEX IF NOT EXISTS idx_platforms_code
            ON platforms(code);

        -- Platform family grouping
        CREATE INDEX IF NOT EXISTS idx_platforms_family
            ON platforms(family);
    END IF;
END $$;

-- Only create if genres table exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='genres') THEN
        -- Genre slug lookups (already UNIQUE)
        CREATE INDEX IF NOT EXISTS idx_genres_slug
            ON genres(slug);

        -- Genre hierarchy traversal
        CREATE INDEX IF NOT EXISTS idx_genres_parent
            ON genres(parent_id)
            WHERE parent_id IS NOT NULL;
    END IF;
END $$;

-- =============================
-- PROVIDER TOPLISTS INDEXES
-- =============================

-- Only create if provider_toplists table exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='provider_toplists') THEN
        -- Toplist slug lookups (already UNIQUE)
        CREATE INDEX IF NOT EXISTS idx_provider_toplists_slug
            ON provider_toplists(slug);

        -- Recent toplists by provider
        CREATE INDEX IF NOT EXISTS idx_provider_toplists_provider_snapshot
            ON provider_toplists(provider_key, snapshot_at DESC);
    END IF;
END $$;

-- Only create if provider_toplist_items table exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='provider_toplist_items') THEN
        -- Toplist items ranking
        CREATE INDEX IF NOT EXISTS idx_provider_toplist_items_rank
            ON provider_toplist_items(provider_toplist_id, rank);
    END IF;
END $$;

-- =============================
-- VENDOR CACHE & SYNC INDEXES
-- =============================

-- Only create if vendor_sync_states table exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='vendor_sync_states') THEN
        -- Vendor sync state by provider (already UNIQUE)
        CREATE INDEX IF NOT EXISTS idx_vendor_sync_states_provider
            ON vendor_sync_states(provider);

        -- Recent syncs
        CREATE INDEX IF NOT EXISTS idx_vendor_sync_states_last_sync
            ON vendor_sync_states(last_full_sync_at DESC NULLS LAST);
    END IF;
END $$;

-- Only create if vendor_http_caches table exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='vendor_http_caches') THEN
        -- HTTP cache lookups
        CREATE INDEX IF NOT EXISTS idx_vendor_http_caches_provider_endpoint
            ON vendor_http_caches(provider, endpoint);
    END IF;
END $$;

-- =============================
-- INDEX STATISTICS
-- =============================

-- Update statistics after index creation (only for tables that exist)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='products') THEN
        EXECUTE 'ANALYZE products';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='video_games') THEN
        EXECUTE 'ANALYZE video_games';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='video_game_titles') THEN
        EXECUTE 'ANALYZE video_game_titles';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='sku_regions') THEN
        EXECUTE 'ANALYZE sku_regions';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='region_prices') THEN
        EXECUTE 'ANALYZE region_prices';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='game_price_points') THEN
        EXECUTE 'ANALYZE game_price_points';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='game_retailers') THEN
        EXECUTE 'ANALYZE game_retailers';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='product_media') THEN
        EXECUTE 'ANALYZE product_media';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='game_images') THEN
        EXECUTE 'ANALYZE game_images';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='game_videos') THEN
        EXECUTE 'ANALYZE game_videos';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='provider_offers') THEN
        EXECUTE 'ANALYZE provider_offers';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='game_providers') THEN
        EXECUTE 'ANALYZE game_providers';
    END IF;
END $$;

-- Log completion
DO $$
BEGIN
    RAISE NOTICE 'Critical performance indexes created successfully';
    RAISE NOTICE 'Run EXPLAIN ANALYZE on slow queries to verify index usage';
END $$;
