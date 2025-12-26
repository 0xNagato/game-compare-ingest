-- 0006_add_video_game_genres.sql
-- Add genres text[] column to video_games in an idempotent way

SET search_path TO gamecompare, public;

ALTER TABLE IF EXISTS video_games
    ADD COLUMN IF NOT EXISTS genres text[];
