-- 0493_video_game_titles_media_columns.sql
-- Purpose: Add canonical media URL columns to video_game_titles for quick access to a game's
--          primary trailer and gameplay videos (first imported occurrence). This supplements
--          row-based game_media for fast read paths.
-- Idempotent: Uses ADD COLUMN IF NOT EXISTS.

ALTER TABLE IF EXISTS public.video_game_titles
  ADD COLUMN IF NOT EXISTS trailer_url text,
  ADD COLUMN IF NOT EXISTS gameplay_url text;

-- Backfill trailer_url (first trailer media) and gameplay_url (first gameplay media) where missing.
-- We select the earliest created_at matching media_type for any video_game attached to the title.
WITH first_trailer AS (
  SELECT vgt.id AS title_id,
         gm.url,
         ROW_NUMBER() OVER (PARTITION BY vgt.id ORDER BY gm.created_at, gm.url) AS rn
  FROM public.video_game_titles vgt
  JOIN public.video_games vg ON vg.title_id = vgt.id
  JOIN public.game_media gm ON gm.video_game_id = vg.id AND gm.media_type = 'trailer'
), first_gameplay AS (
  SELECT vgt.id AS title_id,
         gm.url,
         ROW_NUMBER() OVER (PARTITION BY vgt.id ORDER BY gm.created_at, gm.url) AS rn
  FROM public.video_game_titles vgt
  JOIN public.video_games vg ON vg.title_id = vgt.id
  JOIN public.game_media gm ON gm.video_game_id = vg.id AND gm.media_type = 'gameplay'
), combined AS (
  SELECT COALESCE(ft.title_id, fg.title_id) AS title_id,
         ft.url AS trailer_url,
         fg.url AS gameplay_url
  FROM (SELECT title_id, url FROM first_trailer WHERE rn = 1) ft
  FULL OUTER JOIN (SELECT title_id, url FROM first_gameplay WHERE rn = 1) fg
    ON ft.title_id = fg.title_id
)
UPDATE public.video_game_titles vgt
SET trailer_url = COALESCE(vgt.trailer_url, combined.trailer_url),
    gameplay_url = COALESCE(vgt.gameplay_url, combined.gameplay_url)
FROM combined
WHERE combined.title_id = vgt.id
  AND ((combined.trailer_url IS NOT NULL AND vgt.trailer_url IS NULL)
       OR (combined.gameplay_url IS NOT NULL AND vgt.gameplay_url IS NULL));

-- Optional simple check (NOTICE) of how many titles received backfill
DO $$ DECLARE n_trailer int; n_gameplay int; BEGIN
  SELECT COUNT(*) INTO n_trailer FROM public.video_game_titles WHERE trailer_url IS NOT NULL;
  SELECT COUNT(*) INTO n_gameplay FROM public.video_game_titles WHERE gameplay_url IS NOT NULL;
  RAISE NOTICE 'video_game_titles media columns backfilled: trailer_url=% gameplay_url=%', n_trailer, n_gameplay;
END $$;

COMMENT ON COLUMN public.video_game_titles.trailer_url IS 'Canonical trailer video URL (first imported trailer)';
COMMENT ON COLUMN public.video_game_titles.gameplay_url IS 'Canonical gameplay video URL (first imported gameplay clip)';
