-- 0551_rename_product_columns.sql
-- Make the original column renames idempotent so the migration can safely rerun.

DO $$
BEGIN
	IF EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_name='software' AND column_name='product_id'
	) AND NOT EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_name='software' AND column_name='video_game_id'
	) THEN
		ALTER TABLE public.software RENAME COLUMN product_id TO video_game_id;
	END IF;
END$$;

DO $$
BEGIN
	IF EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_name='video_game_titles' AND column_name='product_id'
	) AND NOT EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_name='video_game_titles' AND column_name='video_game_id'
	) THEN
		ALTER TABLE public.video_game_titles RENAME COLUMN product_id TO video_game_id;
	END IF;
END$$;