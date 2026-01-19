-- 0480_title_normalization.sql
-- Purpose: Normalize video_game_titles.normalized_title and keep it in sync; prepare for future uniqueness enforcement.
-- Idempotent: guards existence of function, trigger, and backfill only missing rows.

-- Avoid DO/EXECUTE to play nice with custom splitters
CREATE OR REPLACE FUNCTION normalize_game_title(input text)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
  cleaned text;
BEGIN
  IF input IS NULL THEN RETURN NULL; END IF;
  -- replace non-alphanumeric with space
  cleaned := regexp_replace(lower(input), '[^a-z0-9]+', ' ', 'g');
  cleaned := regexp_replace(cleaned, '\s+', ' ', 'g');
  cleaned := trim(cleaned);
  RETURN cleaned;
END;
$$;

-- 2. Backfill normalized_title where NULL
UPDATE video_game_titles
SET normalized_title = normalize_game_title(title)
WHERE normalized_title IS NULL;

-- Recreate trigger function and trigger idempotently
DROP TRIGGER IF EXISTS video_game_titles_normalize_trg ON video_game_titles;

CREATE OR REPLACE FUNCTION video_game_titles_normalize_fn()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.normalized_title := normalize_game_title(NEW.title);
  RETURN NEW;
END;
$$;

CREATE TRIGGER video_game_titles_normalize_trg
BEFORE INSERT OR UPDATE ON video_game_titles
FOR EACH ROW EXECUTE FUNCTION video_game_titles_normalize_fn();

-- 4. Supporting index for future uniqueness checks (non-unique now to avoid failures if duplicates exist)
CREATE INDEX IF NOT EXISTS idx_video_game_titles_normalized ON video_game_titles (normalized_title);

-- 5. Optional diagnostic view (drop/create idempotent)
CREATE OR REPLACE VIEW video_game_title_duplicates AS
SELECT normalized_title, COUNT(*) AS cnt, array_agg(id) AS title_ids
FROM video_game_titles
GROUP BY normalized_title
HAVING COUNT(*) > 1;

-- Run a sample duplicate count (commented; for manual execution)
-- SELECT * FROM video_game_title_duplicates ORDER BY cnt DESC LIMIT 20;
