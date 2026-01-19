-- 0536_add_edition_to_video_games.sql
-- Add the edition column to video_games table if it doesn't exist
-- This allows tracking different editions of the same game on the same platform

ALTER TABLE IF EXISTS public.video_games
  ADD COLUMN IF NOT EXISTS edition text;

-- Create partial unique indexes to enforce uniqueness rules:
-- 1. Only one entry per (title_id, platform_id) when edition is NULL
-- 2. Unique (title_id, platform_id, edition) when edition is NOT NULL

-- Note: Indexes commented out because the current video_games schema
-- uses product_id instead of title_id. These indexes can be created
-- after migration to the new schema with title_id and platform_id.

-- DO $$ BEGIN
--   IF NOT EXISTS (
--     SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
--     WHERE c.relkind='i' AND c.relname='uq_video_games_product_edition_null' AND n.nspname='public'
--   ) THEN
--     CREATE UNIQUE INDEX uq_video_games_product_edition_null
--       ON public.video_games (product_id, COALESCE(edition, ''))
--       WHERE edition IS NULL;
--   END IF;
-- END $$;
--
-- DO $$ BEGIN
--   IF NOT EXISTS (
--     SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
--     WHERE c.relkind='i' AND c.relname='uq_video_games_product_edition' AND n.nspname='public'
--   ) THEN
--     CREATE UNIQUE INDEX uq_video_games_product_edition
--       ON public.video_games (product_id, edition)
--       WHERE edition IS NOT NULL;
--   END IF;
-- END $$;
