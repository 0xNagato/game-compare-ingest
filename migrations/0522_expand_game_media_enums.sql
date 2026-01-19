-- Expand canonical enums for `game_media`
--
-- Why:
-- - We want PlayStation Store media to persist with a stable canonical source (`psstore`).
-- - We want to preserve background images as a first-class semantic media type.
--
-- Notes:
-- - PostgreSQL enum values are additive only; they cannot be removed safely in a down migration.
-- - These statements are safe to re-run on PostgreSQL 12+.

-- Only add enum values if the enum types exist
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_type WHERE typname = 'media_source') THEN
        BEGIN
            ALTER TYPE media_source ADD VALUE IF NOT EXISTS 'psstore';
        EXCEPTION
            WHEN duplicate_object THEN NULL;
        END;
    END IF;
END$$;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_type WHERE typname = 'media_type') THEN
        BEGIN
            ALTER TYPE media_type ADD VALUE IF NOT EXISTS 'background';
        EXCEPTION
            WHEN duplicate_object THEN NULL;
        END;
    END IF;
END$$;
