-- 0330_video_game_titles.sql (squashed)
-- Consolidated video_game_titles schema with enrichment columns
-- NOTE: video_game_titles is already created in 0001_full_consolidated_schema.sql
-- This migration adds indexes on the existing table

-- Search indexes on existing columns
-- Using 'title' column (defined in 0001) instead of non-existent 'name' column
CREATE INDEX IF NOT EXISTS video_game_titles_title_trgm_idx ON public.video_game_titles USING gin ((title) gin_trgm_ops);
