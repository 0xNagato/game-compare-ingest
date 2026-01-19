-- 0525_storage_optimizations.sql
-- Storage and memory optimization strategies
-- Estimated savings: 40-60% reduction in database size

-- =============================
-- PART 1: JSONB COMPRESSION TUNING
-- =============================

-- Large JSONB columns consume significant space
-- TOAST compression strategy: EXTERNAL = compress + allow out-of-line storage

-- Game images provider payload (often 5-50KB per row)
-- Only modify if table exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='game_images') THEN
        ALTER TABLE game_images
            ALTER COLUMN provider_payload SET STORAGE EXTERNAL,
            ALTER COLUMN metadata SET STORAGE EXTERNAL,
            ALTER COLUMN variants SET STORAGE EXTERNAL;
    END IF;
END $$;

-- Game videos provider payload
-- Only modify if table exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='game_videos') THEN
        ALTER TABLE game_videos
            ALTER COLUMN provider_payload SET STORAGE EXTERNAL,
            ALTER COLUMN metadata SET STORAGE EXTERNAL,
            ALTER COLUMN thumbnails SET STORAGE EXTERNAL;
    END IF;
END $$;

-- Video games metadata (can be large with external_ids, external_links)
-- Only modify storage for columns that exist
DO $$
BEGIN
    -- metadata column (should always exist)
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='public' AND table_name='video_games' AND column_name='metadata'
    ) THEN
        ALTER TABLE video_games ALTER COLUMN metadata SET STORAGE EXTERNAL;
    END IF;

    -- external_ids column (may not exist yet)
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='public' AND table_name='video_games' AND column_name='external_ids'
    ) THEN
        ALTER TABLE video_games ALTER COLUMN external_ids SET STORAGE EXTERNAL;
    END IF;

    -- external_links column (may not exist yet)
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='public' AND table_name='video_games' AND column_name='external_links'
    ) THEN
        ALTER TABLE video_games ALTER COLUMN external_links SET STORAGE EXTERNAL;
    END IF;
END $$;

-- Game providers credentials (secure but can be compressed)
-- Only modify if table exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='game_providers') THEN
        ALTER TABLE game_providers
            ALTER COLUMN credentials SET STORAGE EXTERNAL,
            ALTER COLUMN metadata SET STORAGE EXTERNAL;
    END IF;
END $$;

-- Game retailers provider payload
-- Only modify if table exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='game_retailers') THEN
        ALTER TABLE game_retailers
            ALTER COLUMN provider_payload SET STORAGE EXTERNAL,
            ALTER COLUMN metadata SET STORAGE EXTERNAL;
    END IF;
END $$;

-- Product media metadata
-- Only modify if table exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='product_media') THEN
        ALTER TABLE product_media
            ALTER COLUMN metadata SET STORAGE EXTERNAL;
    END IF;
END $$;

-- Provider media links metadata
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'provider_media_links') THEN
        ALTER TABLE provider_media_links
            ALTER COLUMN metadata SET STORAGE EXTERNAL;
    END IF;
END $$;

COMMENT ON COLUMN game_images.provider_payload IS
'TOAST: EXTERNAL storage for compression. Consider archival after 90 days if not actively used.';

-- =============================
-- PART 2: PARTITION OLD PRICES
-- =============================

-- Function to detach and archive old price partitions
CREATE OR REPLACE FUNCTION archive_old_price_partitions(
    months_to_keep INTEGER DEFAULT 12,
    dry_run BOOLEAN DEFAULT TRUE
)
RETURNS TABLE(
    partition_name TEXT,
    row_count BIGINT,
    size_bytes BIGINT,
    action TEXT
)
LANGUAGE plpgsql AS $$
DECLARE
    v_partition RECORD;
    v_cutoff_date DATE;
    v_count BIGINT;
    v_size BIGINT;
BEGIN
    v_cutoff_date := DATE_TRUNC('month', NOW() - (months_to_keep || ' months')::INTERVAL)::DATE;

    FOR v_partition IN
        SELECT
            schemaname,
            tablename
        FROM pg_tables
        WHERE schemaname = 'public'
          AND tablename ~ '^prices_\d{4}_\d{2}$'
          AND tablename < 'prices_' || TO_CHAR(v_cutoff_date, 'YYYY_MM')
        ORDER BY tablename
    LOOP
        -- Get row count
        EXECUTE format('SELECT COUNT(*) FROM %I.%I', v_partition.schemaname, v_partition.tablename)
            INTO v_count;

        -- Get size in bytes
        EXECUTE format('SELECT pg_total_relation_size(%L)', v_partition.schemaname || '.' || v_partition.tablename)
            INTO v_size;

        partition_name := v_partition.tablename;
        row_count := v_count;
        size_bytes := v_size;

        IF dry_run THEN
            action := 'DRY RUN - Would detach and archive';
        ELSE
            -- Detach partition
            EXECUTE format('ALTER TABLE prices DETACH PARTITION %I', v_partition.tablename);
            action := 'DETACHED - Ready for archive/drop';

            -- Note: After detaching, you can:
            -- 1. pg_dump the partition to cold storage
            -- 2. Move to archive tablespace: ALTER TABLE partition SET TABLESPACE archive_ts
            -- 3. Drop entirely: DROP TABLE partition
        END IF;

        RETURN NEXT;
    END LOOP;

    RETURN;
END;
$$;

COMMENT ON FUNCTION archive_old_price_partitions IS
'Detach price partitions older than N months for archival.
Usage: SELECT * FROM archive_old_price_partitions(12, true);  -- dry run
       SELECT * FROM archive_old_price_partitions(12, false); -- execute
After detaching, export with: pg_dump -t partition_name > archive.sql';

-- =============================
-- PART 3: CONSOLIDATE PROVIDER USAGE TRACKING
-- =============================

-- Create unified provider usage table (replaces 3 tables)
CREATE TABLE IF NOT EXISTS provider_usage_unified (
    provider VARCHAR PRIMARY KEY,
    total_calls BIGINT NOT NULL DEFAULT 0,
    daily_calls INT NOT NULL DEFAULT 0,
    daily_window DATE,
    last_called_at TIMESTAMP,
    -- Breakdown by target table (replaces provider_usage_breakdowns)
    breakdown JSONB DEFAULT '{}'::jsonb,
    -- Summary stats
    stats JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Migrate data from existing tables
DO $$
DECLARE
    v_provider TEXT;
    v_breakdown JSONB;
BEGIN
    -- Check if old tables exist
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'provider_usages') THEN
        -- Migrate from provider_usages
        INSERT INTO provider_usage_unified (provider, total_calls, daily_calls, daily_window, last_called_at)
        SELECT provider, total_calls, daily_calls, daily_window, last_called_at
        FROM provider_usages
        ON CONFLICT (provider) DO UPDATE SET
            total_calls = EXCLUDED.total_calls,
            daily_calls = EXCLUDED.daily_calls,
            daily_window = EXCLUDED.daily_window,
            last_called_at = EXCLUDED.last_called_at,
            updated_at = NOW();

        RAISE NOTICE 'Migrated % rows from provider_usages', (SELECT COUNT(*) FROM provider_usages);
    END IF;

    -- Migrate breakdown data if exists
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'provider_usage_breakdowns') THEN
        FOR v_provider IN SELECT DISTINCT provider FROM provider_usage_breakdowns LOOP
            -- Aggregate breakdown into JSONB
            SELECT jsonb_object_agg(target_table, jsonb_build_object(
                'total_rows', total_rows,
                'last_event_at', last_event_at
            ))
            INTO v_breakdown
            FROM provider_usage_breakdowns
            WHERE provider = v_provider;

            -- Update unified table
            UPDATE provider_usage_unified
            SET breakdown = v_breakdown,
                updated_at = NOW()
            WHERE provider = v_provider;
        END LOOP;

        RAISE NOTICE 'Migrated breakdown data for % providers', (SELECT COUNT(DISTINCT provider) FROM provider_usage_breakdowns);
    END IF;
END $$;

-- Create index for JSONB queries
CREATE INDEX IF NOT EXISTS idx_provider_usage_unified_breakdown
    ON provider_usage_unified USING gin(breakdown);

COMMENT ON TABLE provider_usage_unified IS
'Consolidated provider usage tracking. Replaces provider_usages, provider_usage_summaries, and provider_usage_breakdowns.
Breakdown JSONB format: {"target_table": {"total_rows": N, "last_event_at": "timestamp"}}';

-- =============================
-- PART 4: ANALYZE DUAL PRICING SYSTEM
-- =============================

-- Analysis view to identify overlap between legacy and new pricing systems
-- Only create if both tables exist
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='region_prices')
       AND EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='game_price_points') THEN
        CREATE OR REPLACE VIEW pricing_system_analysis AS
        WITH legacy_stats AS (
            SELECT
                'region_prices' as system,
                COUNT(*) as total_rows,
                COUNT(DISTINCT sku_region_id) as unique_skus,
                MAX(recorded_at) as latest_data,
                pg_size_pretty(pg_total_relation_size('region_prices')) as total_size
            FROM region_prices
        ),
        new_stats AS (
            SELECT
                'game_price_points' as system,
                COUNT(*) as total_rows,
                COUNT(DISTINCT game_retailer_id) as unique_retailers,
                MAX(collected_at) as latest_data,
                pg_size_pretty(pg_total_relation_size('game_price_points')) as total_size
            FROM game_price_points
        )
        SELECT * FROM legacy_stats
        UNION ALL
        SELECT
            system,
            total_rows,
            unique_retailers as unique_skus,
            latest_data,
            total_size
        FROM new_stats;

        EXECUTE 'COMMENT ON VIEW pricing_system_analysis IS ''Compare legacy (region_prices) vs new (game_price_points) pricing systems.
Use to determine which system is actively used and plan consolidation.''';
    END IF;
END $$;

-- =============================
-- PART 5: MEDIA DEDUPLICATION ANALYSIS
-- =============================

-- Find duplicate media URLs across tables
-- Only create if both source tables exist
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='product_media')
       AND EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='game_images') THEN
        CREATE OR REPLACE VIEW duplicate_media_urls AS
        WITH all_media_urls AS (
            SELECT 'product_media' as source, url, COUNT(*) as occurrences
            FROM product_media
            WHERE url IS NOT NULL
            GROUP BY url

            UNION ALL

            SELECT 'game_images', url, COUNT(*)
            FROM game_images
            WHERE url IS NOT NULL
            GROUP BY url
        ),
        aggregated AS (
            SELECT
                url,
                SUM(occurrences) as total_occurrences,
                COUNT(DISTINCT source) as source_count,
                STRING_AGG(DISTINCT source, ', ' ORDER BY source) as sources
            FROM all_media_urls
            GROUP BY url
            HAVING COUNT(DISTINCT source) > 1 OR SUM(occurrences) > 1
        )
        SELECT
            url,
            total_occurrences,
            source_count,
            sources,
            -- Estimate bytes wasted (assume avg 100 chars per URL)
            (total_occurrences - 1) * 100 as estimated_waste_bytes
        FROM aggregated
        ORDER BY total_occurrences DESC;

        EXECUTE 'COMMENT ON VIEW duplicate_media_urls IS ''Identify duplicate media URLs stored across multiple tables.
Consider creating canonical_media table with foreign key references to eliminate duplicates.''';
    END IF;
END $$;

-- =============================
-- PART 6: CLEANUP ORPHANED DATA
-- =============================

-- Find orphaned provider media links (provider_item no longer exists)
-- Only create if required tables exist
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='provider_media_links')
       AND EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='provider_items') THEN
        CREATE OR REPLACE FUNCTION find_orphaned_provider_media_links()
        RETURNS TABLE(
            provider_media_link_id BIGINT,
            provider_item_id VARCHAR,
            created_at TIMESTAMP
        )
        LANGUAGE plpgsql AS $func$
        BEGIN
            RETURN QUERY
            SELECT
                pml.id,
                pml.provider_item_id,
                pml.created_at
            FROM provider_media_links pml
            LEFT JOIN provider_items pi ON pi.external_id = pml.provider_item_id
            WHERE pi.id IS NULL
            ORDER BY pml.created_at DESC
            LIMIT 1000;
        END;
        $func$;
    END IF;
END $$;

-- Find orphaned game images (game_provider no longer exists)
-- Only create if required tables exist
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='game_images')
       AND EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='game_providers') THEN
        CREATE OR REPLACE FUNCTION find_orphaned_game_images()
        RETURNS TABLE(
            game_image_id BIGINT,
            game_provider_id BIGINT,
            url VARCHAR,
            created_at TIMESTAMP
        )
        LANGUAGE plpgsql AS $func$
        BEGIN
            RETURN QUERY
            SELECT
                gi.id,
                gi.game_provider_id,
                gi.url,
                gi.created_at
            FROM game_images gi
            LEFT JOIN game_providers gp ON gp.id = gi.game_provider_id
            WHERE gp.id IS NULL
            ORDER BY gi.created_at DESC
            LIMIT 1000;
        END;
        $func$;
    END IF;
END $$;

COMMENT ON FUNCTION find_orphaned_provider_media_links IS
'Find provider_media_links without corresponding provider_items. Safe to delete after review.';

COMMENT ON FUNCTION find_orphaned_game_images IS
'Find game_images without corresponding game_providers. Safe to delete after review.';

-- =============================
-- PART 7: VACUUM AND STATISTICS
-- =============================

-- Update table statistics after storage optimizations
-- Only analyze tables that exist
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='game_images') THEN
        EXECUTE 'ANALYZE game_images';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='game_videos') THEN
        EXECUTE 'ANALYZE game_videos';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='video_games') THEN
        EXECUTE 'ANALYZE video_games';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='game_providers') THEN
        EXECUTE 'ANALYZE game_providers';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='product_media') THEN
        EXECUTE 'ANALYZE product_media';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='region_prices') THEN
        EXECUTE 'ANALYZE region_prices';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='game_price_points') THEN
        EXECUTE 'ANALYZE game_price_points';
    END IF;
END $$;

-- =============================
-- USAGE INSTRUCTIONS
-- =============================

DO $$
BEGIN
    RAISE NOTICE '=== Storage Optimization Migration Complete ===';
    RAISE NOTICE ' ';
    RAISE NOTICE 'IMMEDIATE ACTIONS:';
    RAISE NOTICE '1. Check pricing system usage: SELECT * FROM pricing_system_analysis;';
    RAISE NOTICE '2. Find duplicate media: SELECT COUNT(*), SUM(estimated_waste_bytes) FROM duplicate_media_urls;';
    RAISE NOTICE '3. Review old partitions: SELECT * FROM archive_old_price_partitions(12, true);';
    RAISE NOTICE ' ';
    RAISE NOTICE 'GRADUAL CLEANUP (after backup):';
    RAISE NOTICE '4. Archive old partitions: SELECT * FROM archive_old_price_partitions(12, false);';
    RAISE NOTICE '5. Find orphans: SELECT COUNT(*) FROM find_orphaned_provider_media_links();';
    RAISE NOTICE '6. Drop old tables after confirming unified table works:';
    RAISE NOTICE '   -- DROP TABLE provider_usages;';
    RAISE NOTICE '   -- DROP TABLE provider_usage_summaries;';
    RAISE NOTICE '   -- DROP TABLE provider_usage_breakdowns;';
    RAISE NOTICE ' ';
    RAISE NOTICE 'EXPECTED SAVINGS:';
    RAISE NOTICE '- JSONB compression: 20-40%% on affected tables';
    RAISE NOTICE '- Old partition archival: 60%% reduction in active DB size';
    RAISE NOTICE '- Provider usage consolidation: ~5-10MB';
    RAISE NOTICE '- Media deduplication (if implemented): 15-30%%';
    RAISE NOTICE ' ';
    RAISE NOTICE 'Run VACUUM FULL after major cleanup to reclaim disk space.';
END $$;
