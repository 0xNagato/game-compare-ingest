-- 0524_materialized_views.sql
-- Materialized views for read-heavy query optimization
-- Replaces expensive GROUP BY MAX and JSON expansion patterns

-- =============================
-- LATEST PRICES MATERIALIZED VIEW
-- =============================

-- Replaces manual GROUP BY MAX pattern in RegionPrice::scopeLatestForVideoGame()
-- Laravel location: app/Models/RegionPrice.php:65-84
-- Only create if region_prices table exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='region_prices') THEN
        CREATE MATERIALIZED VIEW IF NOT EXISTS mv_latest_prices AS
        SELECT DISTINCT ON (sku_region_id)
            sku_region_id,
            id as region_price_id,
            recorded_at,
            fiat_amount,
            btc_value,
            tax_inclusive,
            currency_id,
            country_id,
            local_amount
        FROM region_prices
        ORDER BY sku_region_id, recorded_at DESC;

        -- Unique index required for CONCURRENTLY refresh
        CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_latest_prices_sku
            ON mv_latest_prices(sku_region_id);

        -- Additional indexes for common query patterns
        CREATE INDEX IF NOT EXISTS idx_mv_latest_prices_recorded_at
            ON mv_latest_prices(recorded_at DESC);

        CREATE INDEX IF NOT EXISTS idx_mv_latest_prices_currency
            ON mv_latest_prices(currency_id);

        EXECUTE 'COMMENT ON MATERIALIZED VIEW mv_latest_prices IS ''Latest price snapshot per SKU region. Refresh after each price ingestion run.
Eliminates expensive window functions and GROUP BY MAX patterns.
Refresh command: REFRESH MATERIALIZED VIEW CONCURRENTLY mv_latest_prices;''';
    END IF;
END $$;

-- =============================
-- CROSS REFERENCE PLATFORMS VIEW
-- =============================

-- Replaces expensive LATERAL JSON expansion in BuildComparePageDataAction
-- Laravel location: app/Actions/Compare/BuildComparePageDataAction.php:1304-1329
-- Only create if cross_reference_entries table exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='cross_reference_entries') THEN
        CREATE MATERIALIZED VIEW IF NOT EXISTS mv_cross_ref_platforms AS
        SELECT DISTINCT UPPER(value) as platform
        FROM cross_reference_entries
        CROSS JOIN LATERAL jsonb_array_elements_text(
            CASE
                WHEN platforms IS NULL THEN '[]'::jsonb
                WHEN platforms::text ~ '^\s*\[' THEN platforms::jsonb
                ELSE '[]'::jsonb
            END
        ) AS value
        WHERE value IS NOT NULL AND value != ''
        ORDER BY platform;

        CREATE INDEX IF NOT EXISTS idx_mv_cross_ref_platforms_platform
            ON mv_cross_ref_platforms(platform);

        EXECUTE 'COMMENT ON MATERIALIZED VIEW mv_cross_ref_platforms IS ''Distinct platform codes from cross_reference_entries JSON arrays.
Refresh hourly (6-hour cache TTL in Laravel allows stale data).
Refresh command: REFRESH MATERIALIZED VIEW CONCURRENTLY mv_cross_ref_platforms;''';
    END IF;
END $$;

-- =============================
-- CROSS REFERENCE CURRENCIES VIEW
-- =============================

-- Same pattern as platforms, for currency codes
-- Only create if cross_reference_entries table exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='cross_reference_entries') THEN
        CREATE MATERIALIZED VIEW IF NOT EXISTS mv_cross_ref_currencies AS
        SELECT DISTINCT UPPER(value) as currency
        FROM cross_reference_entries
        CROSS JOIN LATERAL jsonb_array_elements_text(
            CASE
                WHEN currencies IS NULL THEN '[]'::jsonb
                WHEN currencies::text ~ '^\s*\[' THEN currencies::jsonb
                ELSE '[]'::jsonb
            END
        ) AS value
        WHERE value IS NOT NULL AND value != ''
        ORDER BY currency;

        CREATE INDEX IF NOT EXISTS idx_mv_cross_ref_currencies_currency
            ON mv_cross_ref_currencies(currency);

        EXECUTE 'COMMENT ON MATERIALIZED VIEW mv_cross_ref_currencies IS ''Distinct currency codes from cross_reference_entries JSON arrays.
Refresh hourly.
Refresh command: REFRESH MATERIALIZED VIEW CONCURRENTLY mv_cross_ref_currencies;''';
    END IF;
END $$;

-- =============================
-- GAME PRICE LATEST VIEW
-- =============================

-- Latest price points per game retailer (new pricing system)
-- Only create if game_price_points table exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='game_price_points') THEN
        CREATE MATERIALIZED VIEW IF NOT EXISTS mv_latest_game_prices AS
        SELECT DISTINCT ON (game_retailer_id)
            game_retailer_id,
            id as price_point_id,
            collected_at,
            effective_at,
            currency_code,
            amount_minor,
            btc_value_sats,
            is_sale
        FROM game_price_points
        ORDER BY game_retailer_id, collected_at DESC;

        CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_latest_game_prices_retailer
            ON mv_latest_game_prices(game_retailer_id);

        CREATE INDEX IF NOT EXISTS idx_mv_latest_game_prices_collected
            ON mv_latest_game_prices(collected_at DESC);

        CREATE INDEX IF NOT EXISTS idx_mv_latest_game_prices_currency
            ON mv_latest_game_prices(currency_code);

        EXECUTE 'COMMENT ON MATERIALIZED VIEW mv_latest_game_prices IS ''Latest price point per game retailer (new pricing system).
Refresh after price ingestion runs.
Refresh command: REFRESH MATERIALIZED VIEW CONCURRENTLY mv_latest_game_prices;''';
    END IF;
END $$;

-- =============================
-- PRODUCT MEDIA BEST QUALITY VIEW
-- =============================

-- Pre-computed best quality media per product
-- Used in ProductMediaResolver and BuildComparePageDataAction
-- Only create if product_media table exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='product_media') THEN
        CREATE MATERIALIZED VIEW IF NOT EXISTS mv_product_best_media AS
        SELECT DISTINCT ON (product_id)
            product_id,
            id as product_media_id,
            source,
            external_id,
            media_type,
            title,
            caption,
            url,
            thumbnail_url,
            width,
            height,
            quality_score,
            is_primary,
            fetched_at
        FROM product_media
        ORDER BY product_id, is_primary DESC, quality_score DESC, fetched_at DESC;

        CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_product_best_media_product
            ON mv_product_best_media(product_id);

        CREATE INDEX IF NOT EXISTS idx_mv_product_best_media_quality
            ON mv_product_best_media(quality_score DESC);

        EXECUTE 'COMMENT ON MATERIALIZED VIEW mv_product_best_media IS ''Best quality media item per product based on is_primary, quality_score, and recency.
Refresh after media ingestion or updates.
Refresh command: REFRESH MATERIALIZED VIEW CONCURRENTLY mv_product_best_media;''';
    END IF;
END $$;

-- =============================
-- REFRESH STRATEGY FUNCTION
-- =============================

-- Helper function to refresh all materialized views concurrently
CREATE OR REPLACE FUNCTION refresh_all_materialized_views()
RETURNS TABLE(view_name TEXT, refresh_duration INTERVAL, success BOOLEAN)
LANGUAGE plpgsql AS $$
DECLARE
    v_start TIMESTAMP;
    v_end TIMESTAMP;
    v_view TEXT;
BEGIN
    FOR v_view IN
        SELECT matviewname::text
        FROM pg_matviews
        WHERE schemaname = 'public'
          AND matviewname LIKE 'mv_%'
        ORDER BY matviewname
    LOOP
        BEGIN
            v_start := clock_timestamp();
            EXECUTE format('REFRESH MATERIALIZED VIEW CONCURRENTLY %I', v_view);
            v_end := clock_timestamp();

            view_name := v_view;
            refresh_duration := v_end - v_start;
            success := true;
            RETURN NEXT;

            RAISE NOTICE 'Refreshed % in %', v_view, refresh_duration;
        EXCEPTION WHEN OTHERS THEN
            view_name := v_view;
            refresh_duration := '0 seconds'::interval;
            success := false;
            RETURN NEXT;

            RAISE WARNING 'Failed to refresh %: %', v_view, SQLERRM;
        END;
    END LOOP;
END;
$$;

COMMENT ON FUNCTION refresh_all_materialized_views() IS
'Refresh all mv_* materialized views concurrently.
Usage: SELECT * FROM refresh_all_materialized_views();
Returns status and duration for each view.
Safe to run during active queries (CONCURRENTLY mode).';

-- =============================
-- INITIAL REFRESH
-- =============================

-- Populate materialized views with initial data
DO $$
BEGIN
    -- Only refresh if views exist and have data sources
    IF EXISTS (SELECT 1 FROM pg_matviews WHERE schemaname='public' AND matviewname='mv_latest_prices')
       AND EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='region_prices')
       AND EXISTS (SELECT 1 FROM region_prices LIMIT 1) THEN
        REFRESH MATERIALIZED VIEW mv_latest_prices;
        RAISE NOTICE 'Initial refresh: mv_latest_prices completed';
    END IF;

    IF EXISTS (SELECT 1 FROM pg_matviews WHERE schemaname='public' AND matviewname='mv_cross_ref_platforms')
       AND EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='cross_reference_entries')
       AND EXISTS (SELECT 1 FROM cross_reference_entries LIMIT 1) THEN
        REFRESH MATERIALIZED VIEW mv_cross_ref_platforms;
        RAISE NOTICE 'Initial refresh: mv_cross_ref_platforms completed';
    END IF;

    IF EXISTS (SELECT 1 FROM pg_matviews WHERE schemaname='public' AND matviewname='mv_cross_ref_currencies')
       AND EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='cross_reference_entries')
       AND EXISTS (SELECT 1 FROM cross_reference_entries LIMIT 1) THEN
        REFRESH MATERIALIZED VIEW mv_cross_ref_currencies;
        RAISE NOTICE 'Initial refresh: mv_cross_ref_currencies completed';
    END IF;

    IF EXISTS (SELECT 1 FROM pg_matviews WHERE schemaname='public' AND matviewname='mv_latest_game_prices')
       AND EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='game_price_points')
       AND EXISTS (SELECT 1 FROM game_price_points LIMIT 1) THEN
        REFRESH MATERIALIZED VIEW mv_latest_game_prices;
        RAISE NOTICE 'Initial refresh: mv_latest_game_prices completed';
    END IF;

    IF EXISTS (SELECT 1 FROM pg_matviews WHERE schemaname='public' AND matviewname='mv_product_best_media')
       AND EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='product_media')
       AND EXISTS (SELECT 1 FROM product_media LIMIT 1) THEN
        REFRESH MATERIALIZED VIEW mv_product_best_media;
        RAISE NOTICE 'Initial refresh: mv_product_best_media completed';
    END IF;

    RAISE NOTICE 'All materialized views initialized';
EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'Initial refresh failed: %. Views will be empty until first manual refresh.', SQLERRM;
END $$;

-- =============================
-- USAGE EXAMPLES
-- =============================

-- Example 1: Query latest prices (replaces expensive subquery)
-- Old: SELECT * FROM region_prices WHERE ... GROUP BY sku_region_id MAX(recorded_at)
-- New: SELECT * FROM mv_latest_prices WHERE ...
/*
SELECT
    mv.sku_region_id,
    mv.fiat_amount,
    mv.btc_value,
    c.code as currency_code
FROM mv_latest_prices mv
JOIN currencies c ON c.id = mv.currency_id
WHERE mv.recorded_at >= NOW() - INTERVAL '7 days'
ORDER BY mv.fiat_amount DESC
LIMIT 10;
*/

-- Example 2: Get distinct platforms (replaces LATERAL JSON expansion)
-- Old: SELECT DISTINCT ... FROM cross_reference_entries CROSS JOIN LATERAL jsonb_array_elements_text(...)
-- New: SELECT * FROM mv_cross_ref_platforms
/*
SELECT platform FROM mv_cross_ref_platforms;
*/

-- Example 3: Best media per product (replaces complex ORDER BY with LIMIT)
-- Old: SELECT * FROM product_media WHERE product_id = ? ORDER BY is_primary DESC, quality_score DESC, fetched_at DESC LIMIT 1
-- New: SELECT * FROM mv_product_best_media WHERE product_id = ?
/*
SELECT
    p.name as product_name,
    m.url as best_image_url,
    m.quality_score
FROM products p
LEFT JOIN mv_product_best_media m ON m.product_id = p.id
WHERE p.popularity_score > 50
ORDER BY p.popularity_score DESC
LIMIT 20;
*/

-- =============================
-- MAINTENANCE SCHEDULE
-- =============================

-- Recommended refresh schedule (set up via cron or Laravel scheduler):

-- After each price ingestion run:
--   REFRESH MATERIALIZED VIEW CONCURRENTLY mv_latest_prices;
--   REFRESH MATERIALIZED VIEW CONCURRENTLY mv_latest_game_prices;

-- After media ingestion/updates:
--   REFRESH MATERIALIZED VIEW CONCURRENTLY mv_product_best_media;

-- Hourly (cross-reference data changes infrequently):
--   REFRESH MATERIALIZED VIEW CONCURRENTLY mv_cross_ref_platforms;
--   REFRESH MATERIALIZED VIEW CONCURRENTLY mv_cross_ref_currencies;

-- Or use the helper function to refresh all:
--   SELECT * FROM refresh_all_materialized_views();
