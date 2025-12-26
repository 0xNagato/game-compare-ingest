-- 0479_title_sources_uniques.sql
-- Purpose: Enforce one-to-one mapping between provider source IDs and canonical video_game_titles,
-- and add lookup index to speed joins.
-- Safe, idempotent, Postgres 15+ compatible.

-- Ensure helper table exists (defined in 0001_full_consolidated_schema.sql)
-- CREATE TABLE IF NOT EXISTS video_game_title_sources (...);

-- 1) Each (source, source_id) must map to exactly one title
CREATE UNIQUE INDEX IF NOT EXISTS uq_vg_title_sources_source_sid
  ON public.video_game_title_sources (source, coalesce(source_id, ''));

-- 2) Lookup index for title_id joins
CREATE INDEX IF NOT EXISTS idx_vg_title_sources_title
  ON public.video_game_title_sources (video_game_title_id);
