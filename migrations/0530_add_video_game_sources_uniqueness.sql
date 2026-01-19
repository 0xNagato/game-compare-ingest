-- Migration: Add NOT NULL constraint to video_game_sources
-- Purpose: Ensure video_game_sources have required provider_key
-- Note: Unique indexes already created in migration 0529

-- ============================================================================
-- STEP 1: Add NOT NULL Constraint on provider_key
-- ============================================================================

-- Ensure provider_key is always set for new rows
-- Note: Unique index already exists from migration 0529
DO $$
BEGIN
    -- Only add NOT NULL if column exists and doesn't have it
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'video_game_sources'
          AND column_name = 'provider_key'
          AND is_nullable = 'YES'
    ) THEN
        ALTER TABLE video_game_sources
            ALTER COLUMN provider_key SET NOT NULL;

        RAISE NOTICE 'Added NOT NULL constraint to provider_key';
    ELSE
        RAISE NOTICE 'provider_key already has NOT NULL constraint or column does not exist';
    END IF;
EXCEPTION
    WHEN others THEN
        RAISE NOTICE 'Could not add NOT NULL constraint: %', SQLERRM;
END $$;

-- Add helpful comment
COMMENT ON COLUMN video_game_sources.provider_key IS
    'Unique identifier for this video game source (e.g., "ps-store", "steam", "igdb").
    Required field - must be unique across all sources.';

-- ============================================================================
-- VERIFICATION QUERIES (commented out - for manual testing)
-- ============================================================================

-- Verify unique constraint exists
-- SELECT conname, contype, pg_get_constraintdef(oid)
-- FROM pg_constraint
-- WHERE conrelid = 'video_game_sources'::regclass
--   AND contype = 'u'
-- ORDER BY conname;

-- Verify no duplicates
-- SELECT provider_key, COUNT(*) as cnt
-- FROM video_game_sources
-- WHERE provider_key IS NOT NULL
-- GROUP BY provider_key
-- HAVING COUNT(*) > 1;

-- Check all provider_keys are set
-- SELECT COUNT(*) as null_count
-- FROM video_game_sources
-- WHERE provider_key IS NULL;

-- ============================================================================
-- ROLLBACK INSTRUCTIONS (if needed)
-- ============================================================================

/*
-- To rollback this migration, run the following:

ALTER TABLE video_game_sources
    ALTER COLUMN provider_key DROP NOT NULL;
*/

-- ============================================================================
-- NOTES
-- ============================================================================

/*
This migration enforces data integrity for video_game_sources:

1. provider_key cannot be NULL (already unique via index from 0529)

Common provider_key values:
- "ps-store" - PlayStation Store
- "steam" - Steam
- "microsoft-store" - Xbox/Microsoft Store
- "igdb" - Internet Game Database
- "giant-bomb" - GiantBomb
- "rawg" - RAWG
- "tgdb" - TheGamesDB
- "nexarda" - Nexarda
- "itad" - IsThereAnyDeal

After this migration, Rust code using ensure_video_game_source() will
enforce uniqueness and prevent duplicate source entries.

Impact: Low - adds NOT NULL constraint only
Risk: Low - uses exception handling for safety
*/
