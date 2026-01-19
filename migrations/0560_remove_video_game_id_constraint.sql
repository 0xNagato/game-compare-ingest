-- Migration: 0560_remove_video_game_id_constraint.sql
-- Purpose: Remove the strict unique constraint on video_game_titles.video_game_id
--          to allow the application to fully transition to video_game_ids (array).
--          This prevents "duplicate key value violates unique constraint" errors
--          when multiple titles might historically link to the same game ID during
--          deduplication or ingestion updates.

DO $$
BEGIN
    -- 1. Drop the unique constraint if it exists
    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'video_game_titles_video_game_id_key'
        AND conrelid = 'public.video_game_titles'::regclass
    ) THEN
        ALTER TABLE public.video_game_titles
        DROP CONSTRAINT video_game_titles_video_game_id_key;
    END IF;

    -- 2. Make the column nullable if it isn't already
    --    (This allows us to eventually stop populating it entirely)
    ALTER TABLE public.video_game_titles
    ALTER COLUMN video_game_id DROP NOT NULL;

END $$;
