-- Migration: Enforce unique slugs on video_game_sources
-- Purpose: Populate any missing/duplicate slugs and add a UNIQUE index.

DO $$
BEGIN
    -- Ensure slug column exists before attempting updates.
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'video_game_sources'
          AND column_name = 'slug'
    ) THEN
        RAISE NOTICE 'video_game_sources.slug is missing; skipping slug normalization';
        RETURN;
    END IF;

    -- Backfill NULL/blank slugs from provider_key when available.
    UPDATE video_game_sources
    SET slug = provider_key
    WHERE (slug IS NULL OR slug = '')
      AND provider_key IS NOT NULL;

    -- As a last resort, synthesize a slug using the record id.
    UPDATE video_game_sources
    SET slug = LEFT(CONCAT('video-game-source-', id::text), 255)
    WHERE slug IS NULL OR slug = '';

    -- Deduplicate any remaining collisions by suffixing the primary key.
    WITH ranked AS (
        SELECT id,
               slug,
               ROW_NUMBER() OVER (PARTITION BY slug ORDER BY id) AS rn
        FROM video_game_sources
    )
    UPDATE video_game_sources AS vgs
    SET slug = LEFT(CONCAT(vgs.slug, '-', vgs.id::text), 255)
    FROM ranked r
    WHERE vgs.id = r.id
      AND r.rn > 1;
END $$;

-- Create the UNIQUE index (non-concurrent for migration compatibility).
CREATE UNIQUE INDEX IF NOT EXISTS video_game_sources_slug_unique
    ON video_game_sources (slug);
