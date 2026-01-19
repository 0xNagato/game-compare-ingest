-- 0528_media_deduplication.sql
-- Media URL deduplication strategy
-- Based on duplicate_media_urls view showing 446 duplicate URLs
--
-- Strategy: Create canonical_media table to store unique URLs
-- All media tables reference canonical_media_id instead of storing URLs inline
--
-- Benefits:
-- - Reduces database size by 15-30% (estimated)
-- - Single source of truth for media URLs
-- - Easier URL validation and CDN migration
-- - Improved cache hit rates

-- =============================
-- PART 1: CREATE CANONICAL MEDIA TABLE
-- =============================

CREATE TABLE IF NOT EXISTS canonical_media (
    id bigserial PRIMARY KEY,
    url text NOT NULL,
    url_hash text NOT NULL, -- SHA256 hash for fast duplicate detection
    cdn_url text, -- Optional CDN/optimized URL
    mime_type text,
    width integer CHECK (width IS NULL OR width > 0),
    height integer CHECK (height IS NULL OR height > 0),
    size_bytes bigint CHECK (size_bytes IS NULL OR size_bytes > 0),
    hash text, -- Content hash (for true dedup beyond URL)
    storage_provider text, -- 'external', 's3', 'cloudinary', etc.
    metadata jsonb DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    last_verified_at timestamptz, -- Track dead links
    access_count bigint DEFAULT 0, -- Usage tracking for cleanup
    UNIQUE (url_hash)
);

-- Indexes for fast lookups
CREATE INDEX IF NOT EXISTS idx_canonical_media_url_hash ON canonical_media (url_hash);
CREATE INDEX IF NOT EXISTS idx_canonical_media_hash ON canonical_media (hash) WHERE hash IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_canonical_media_created_at ON canonical_media USING brin(created_at);
CREATE INDEX IF NOT EXISTS idx_canonical_media_storage_provider ON canonical_media (storage_provider) WHERE storage_provider IS NOT NULL;

-- Function to generate URL hash
CREATE OR REPLACE FUNCTION canonical_media_url_hash(url_text text)
RETURNS text
LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS $$
    SELECT encode(digest(url_text, 'sha256'), 'hex');
$$;

COMMENT ON TABLE canonical_media IS
'Canonical media URL storage. All media tables reference this to deduplicate URLs.
url_hash is SHA256(url) for O(1) duplicate detection.
Access count tracks usage for eventual cleanup of unused media.';

COMMENT ON FUNCTION canonical_media_url_hash IS
'Generate SHA256 hash of URL for deduplication. Use in INSERT ... ON CONFLICT (url_hash).';

-- =============================
-- PART 2: ADD CANONICAL_MEDIA_ID TO EXISTING TABLES
-- =============================

-- Add canonical_media_id to game_media (modern table)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'game_media') THEN
        -- Add column if doesn't exist
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'game_media' AND column_name = 'canonical_media_id'
        ) THEN
            ALTER TABLE game_media ADD COLUMN canonical_media_id bigint REFERENCES canonical_media(id) ON DELETE SET NULL;
            CREATE INDEX IF NOT EXISTS idx_game_media_canonical_media_id ON game_media (canonical_media_id);

            RAISE NOTICE 'Added canonical_media_id to game_media';
        END IF;
    END IF;
END $$;

-- Add canonical_media_id to game_images (legacy table)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'game_images') THEN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'game_images' AND column_name = 'canonical_media_id'
        ) THEN
            ALTER TABLE game_images ADD COLUMN canonical_media_id bigint REFERENCES canonical_media(id) ON DELETE SET NULL;
            CREATE INDEX IF NOT EXISTS idx_game_images_canonical_media_id ON game_images (canonical_media_id);

            RAISE NOTICE 'Added canonical_media_id to game_images';
        END IF;
    END IF;
END $$;

-- Add canonical_media_id to game_videos (legacy table)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'game_videos') THEN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'game_videos' AND column_name = 'canonical_media_id'
        ) THEN
            ALTER TABLE game_videos ADD COLUMN canonical_media_id bigint REFERENCES canonical_media(id) ON DELETE SET NULL;
            CREATE INDEX IF NOT EXISTS idx_game_videos_canonical_media_id ON game_videos (canonical_media_id);

            RAISE NOTICE 'Added canonical_media_id to game_videos';
        END IF;
    END IF;
END $$;

-- =============================
-- PART 3: BACKFILL CANONICAL MEDIA FROM EXISTING DATA
-- =============================

-- Function to safely backfill canonical_media from existing tables
CREATE OR REPLACE FUNCTION backfill_canonical_media(
    dry_run boolean DEFAULT true,
    batch_size integer DEFAULT 1000
)
RETURNS TABLE(
    source_table text,
    urls_processed bigint,
    canonical_created bigint,
    canonical_reused bigint,
    rows_updated bigint
)
LANGUAGE plpgsql AS $$
DECLARE
    v_urls_processed bigint := 0;
    v_canonical_created bigint := 0;
    v_canonical_reused bigint := 0;
    v_rows_updated bigint := 0;
    v_batch record;
BEGIN
    -- Process game_media table
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'game_media') THEN
        FOR v_batch IN
            SELECT DISTINCT
                gm.url,
                canonical_media_url_hash(gm.url) as url_hash,
                gm.width,
                gm.height,
                gm.size_bytes,
                gm.mime_type,
                gm.hash
            FROM game_media gm
            WHERE gm.canonical_media_id IS NULL
              AND gm.url IS NOT NULL
              AND length(gm.url) > 0
            LIMIT batch_size
        LOOP
            v_urls_processed := v_urls_processed + 1;

            IF NOT dry_run THEN
                -- Insert or get existing canonical_media
                INSERT INTO canonical_media (url, url_hash, width, height, size_bytes, mime_type, hash)
                VALUES (v_batch.url, v_batch.url_hash, v_batch.width, v_batch.height, v_batch.size_bytes, v_batch.mime_type, v_batch.hash)
                ON CONFLICT (url_hash) DO UPDATE
                SET access_count = canonical_media.access_count + 1,
                    updated_at = now()
                RETURNING (xmax = 0) INTO STRICT v_batch.was_inserted;

                IF v_batch.was_inserted THEN
                    v_canonical_created := v_canonical_created + 1;
                ELSE
                    v_canonical_reused := v_canonical_reused + 1;
                END IF;

                -- Update game_media to reference canonical_media
                WITH updated AS (
                    UPDATE game_media gm
                    SET canonical_media_id = cm.id
                    FROM canonical_media cm
                    WHERE gm.url = v_batch.url
                      AND cm.url_hash = v_batch.url_hash
                      AND gm.canonical_media_id IS NULL
                    RETURNING 1
                )
                SELECT count(*) INTO v_rows_updated FROM updated;
            END IF;
        END LOOP;

        source_table := 'game_media';
        urls_processed := v_urls_processed;
        canonical_created := v_canonical_created;
        canonical_reused := v_canonical_reused;
        rows_updated := v_rows_updated;
        RETURN NEXT;

        -- Reset counters for next table
        v_urls_processed := 0;
        v_canonical_created := 0;
        v_canonical_reused := 0;
        v_rows_updated := 0;
    END IF;

    -- Process game_images table
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'game_images') THEN
        FOR v_batch IN
            SELECT DISTINCT
                gi.url,
                canonical_media_url_hash(gi.url) as url_hash,
                gi.width,
                gi.height,
                NULL::bigint as size_bytes,
                gi.mime_type,
                NULL::text as hash
            FROM game_images gi
            WHERE gi.canonical_media_id IS NULL
              AND gi.url IS NOT NULL
              AND length(gi.url) > 0
            LIMIT batch_size
        LOOP
            v_urls_processed := v_urls_processed + 1;

            IF NOT dry_run THEN
                INSERT INTO canonical_media (url, url_hash, width, height, mime_type)
                VALUES (v_batch.url, v_batch.url_hash, v_batch.width, v_batch.height, v_batch.mime_type)
                ON CONFLICT (url_hash) DO UPDATE
                SET access_count = canonical_media.access_count + 1,
                    updated_at = now()
                RETURNING (xmax = 0) INTO STRICT v_batch.was_inserted;

                IF v_batch.was_inserted THEN
                    v_canonical_created := v_canonical_created + 1;
                ELSE
                    v_canonical_reused := v_canonical_reused + 1;
                END IF;

                WITH updated AS (
                    UPDATE game_images gi
                    SET canonical_media_id = cm.id
                    FROM canonical_media cm
                    WHERE gi.url = v_batch.url
                      AND cm.url_hash = v_batch.url_hash
                      AND gi.canonical_media_id IS NULL
                    RETURNING 1
                )
                SELECT count(*) INTO v_rows_updated FROM updated;
            END IF;
        END LOOP;

        source_table := 'game_images';
        urls_processed := v_urls_processed;
        canonical_created := v_canonical_created;
        canonical_reused := v_canonical_reused;
        rows_updated := v_rows_updated;
        RETURN NEXT;

        v_urls_processed := 0;
        v_canonical_created := 0;
        v_canonical_reused := 0;
        v_rows_updated := 0;
    END IF;

    -- Process game_videos table (stream_url)
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'game_videos') THEN
        FOR v_batch IN
            SELECT DISTINCT
                gv.stream_url as url,
                canonical_media_url_hash(gv.stream_url) as url_hash,
                NULL::integer as width,
                NULL::integer as height,
                NULL::bigint as size_bytes,
                'video/mp4'::text as mime_type,
                NULL::text as hash
            FROM game_videos gv
            WHERE gv.canonical_media_id IS NULL
              AND gv.stream_url IS NOT NULL
              AND length(gv.stream_url) > 0
            LIMIT batch_size
        LOOP
            v_urls_processed := v_urls_processed + 1;

            IF NOT dry_run THEN
                INSERT INTO canonical_media (url, url_hash, mime_type)
                VALUES (v_batch.url, v_batch.url_hash, v_batch.mime_type)
                ON CONFLICT (url_hash) DO UPDATE
                SET access_count = canonical_media.access_count + 1,
                    updated_at = now()
                RETURNING (xmax = 0) INTO STRICT v_batch.was_inserted;

                IF v_batch.was_inserted THEN
                    v_canonical_created := v_canonical_created + 1;
                ELSE
                    v_canonical_reused := v_canonical_reused + 1;
                END IF;

                WITH updated AS (
                    UPDATE game_videos gv
                    SET canonical_media_id = cm.id
                    FROM canonical_media cm
                    WHERE gv.stream_url = v_batch.url
                      AND cm.url_hash = v_batch.url_hash
                      AND gv.canonical_media_id IS NULL
                    RETURNING 1
                )
                SELECT count(*) INTO v_rows_updated FROM updated;
            END IF;
        END LOOP;

        source_table := 'game_videos';
        urls_processed := v_urls_processed;
        canonical_created := v_canonical_created;
        canonical_reused := v_canonical_reused;
        rows_updated := v_rows_updated;
        RETURN NEXT;
    END IF;

    RETURN;
END;
$$;

COMMENT ON FUNCTION backfill_canonical_media IS
'Backfill canonical_media table from existing media tables.
Usage:
  SELECT * FROM backfill_canonical_media(true, 1000);  -- dry run
  SELECT * FROM backfill_canonical_media(false, 5000); -- execute in batches
Run multiple times until urls_processed = 0.';

-- =============================
-- PART 4: HELPER VIEW FOR MONITORING
-- =============================

CREATE OR REPLACE VIEW canonical_media_stats AS
SELECT
    COUNT(*) as total_urls,
    COUNT(DISTINCT url_hash) as unique_url_hashes,
    SUM(access_count) as total_references,
    AVG(access_count) as avg_references_per_url,
    COUNT(*) FILTER (WHERE access_count = 0) as unused_urls,
    COUNT(*) FILTER (WHERE access_count >= 10) as heavily_used_urls,
    pg_size_pretty(pg_total_relation_size('canonical_media')) as table_size,
    MAX(created_at) as latest_media_added,
    COUNT(*) FILTER (WHERE last_verified_at IS NOT NULL) as verified_urls,
    COUNT(*) FILTER (WHERE last_verified_at IS NULL) as unverified_urls
FROM canonical_media;

COMMENT ON VIEW canonical_media_stats IS
'Statistics about canonical_media table. Use to monitor deduplication effectiveness.';

-- =============================
-- PART 5: CLEANUP FUNCTION FOR UNUSED MEDIA
-- =============================

CREATE OR REPLACE FUNCTION cleanup_unused_canonical_media(
    min_age_days integer DEFAULT 90,
    dry_run boolean DEFAULT true
)
RETURNS TABLE(
    canonical_media_id bigint,
    url text,
    created_at timestamptz,
    access_count bigint,
    action text
)
LANGUAGE plpgsql AS $$
DECLARE
    v_deleted bigint := 0;
BEGIN
    IF dry_run THEN
        RETURN QUERY
        SELECT
            cm.id,
            cm.url,
            cm.created_at,
            cm.access_count,
            'DRY RUN - Would delete'::text
        FROM canonical_media cm
        LEFT JOIN game_media gm ON gm.canonical_media_id = cm.id
        LEFT JOIN game_images gi ON gi.canonical_media_id = cm.id
        LEFT JOIN game_videos gv ON gv.canonical_media_id = cm.id
        WHERE cm.created_at < now() - (min_age_days || ' days')::interval
          AND gm.id IS NULL
          AND gi.id IS NULL
          AND gv.id IS NULL
        ORDER BY cm.created_at
        LIMIT 100;
    ELSE
        WITH deleted AS (
            DELETE FROM canonical_media cm
            USING canonical_media cm2
            LEFT JOIN game_media gm ON gm.canonical_media_id = cm2.id
            LEFT JOIN game_images gi ON gi.canonical_media_id = cm2.id
            LEFT JOIN game_videos gv ON gv.canonical_media_id = cm2.id
            WHERE cm.id = cm2.id
              AND cm2.created_at < now() - (min_age_days || ' days')::interval
              AND gm.id IS NULL
              AND gi.id IS NULL
              AND gv.id IS NULL
            RETURNING cm.id, cm.url, cm.created_at, cm.access_count
        )
        SELECT
            d.id,
            d.url,
            d.created_at,
            d.access_count,
            'DELETED'::text
        FROM deleted d
        INTO canonical_media_id, url, created_at, access_count, action;

        RETURN NEXT;
    END IF;
END;
$$;

COMMENT ON FUNCTION cleanup_unused_canonical_media IS
'Delete canonical_media entries not referenced by any media table.
Useful for cleanup after media pruning or provider removal.
Usage: SELECT * FROM cleanup_unused_canonical_media(90, true);  -- dry run';

-- =============================
-- PART 6: ROLLBACK STRATEGY
-- =============================

-- Create rollback function (does not execute automatically)
CREATE OR REPLACE FUNCTION rollback_canonical_media_migration()
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    RAISE NOTICE 'Starting rollback of canonical_media migration...';

    -- Drop foreign key constraints first
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'game_media') THEN
        ALTER TABLE game_media DROP COLUMN IF EXISTS canonical_media_id CASCADE;
        RAISE NOTICE 'Dropped canonical_media_id from game_media';
    END IF;

    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'game_images') THEN
        ALTER TABLE game_images DROP COLUMN IF EXISTS canonical_media_id CASCADE;
        RAISE NOTICE 'Dropped canonical_media_id from game_images';
    END IF;

    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'game_videos') THEN
        ALTER TABLE game_videos DROP COLUMN IF EXISTS canonical_media_id CASCADE;
        RAISE NOTICE 'Dropped canonical_media_id from game_videos';
    END IF;

    -- Drop views
    DROP VIEW IF EXISTS canonical_media_stats CASCADE;
    RAISE NOTICE 'Dropped canonical_media_stats view';

    -- Drop functions
    DROP FUNCTION IF EXISTS backfill_canonical_media(boolean, integer) CASCADE;
    DROP FUNCTION IF EXISTS cleanup_unused_canonical_media(integer, boolean) CASCADE;
    DROP FUNCTION IF EXISTS canonical_media_url_hash(text) CASCADE;
    RAISE NOTICE 'Dropped canonical_media functions';

    -- Drop table last
    DROP TABLE IF EXISTS canonical_media CASCADE;
    RAISE NOTICE 'Dropped canonical_media table';

    RAISE NOTICE 'Rollback complete. All canonical_media structures removed.';
END;
$$;

COMMENT ON FUNCTION rollback_canonical_media_migration IS
'ROLLBACK FUNCTION - Removes all canonical_media structures.
WARNING: This will orphan all canonical_media_id references!
Only use if migration needs to be completely reverted.
Usage: SELECT rollback_canonical_media_migration();';

-- =============================
-- USAGE INSTRUCTIONS
-- =============================

DO $$
BEGIN
    RAISE NOTICE '=== Media Deduplication Migration Complete ===';
    RAISE NOTICE ' ';
    RAISE NOTICE 'NEXT STEPS:';
    RAISE NOTICE '1. Review duplicate URLs: SELECT COUNT(*), SUM(estimated_waste_bytes) FROM duplicate_media_urls;';
    RAISE NOTICE '2. Dry run backfill: SELECT * FROM backfill_canonical_media(true, 1000);';
    RAISE NOTICE '3. Execute backfill in batches: SELECT * FROM backfill_canonical_media(false, 5000);';
    RAISE NOTICE '4. Monitor stats: SELECT * FROM canonical_media_stats;';
    RAISE NOTICE '5. Update Rust code to use ensure_canonical_media() helper';
    RAISE NOTICE ' ';
    RAISE NOTICE 'ROLLBACK (if needed):';
    RAISE NOTICE '  SELECT rollback_canonical_media_migration();';
    RAISE NOTICE ' ';
    RAISE NOTICE 'EXPECTED BENEFITS:';
    RAISE NOTICE '- 15-30%% reduction in media table sizes';
    RAISE NOTICE '- Single source of truth for media URLs';
    RAISE NOTICE '- Faster duplicate detection via url_hash index';
    RAISE NOTICE '- Easier CDN migration and URL validation';
END $$;
