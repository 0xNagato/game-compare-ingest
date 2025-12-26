-- 0410_video_games_enriched_view.sql (squashed)
-- Enriched view projecting platform name alongside video_games fields
CREATE OR REPLACE VIEW public.video_games_enriched_vw AS
SELECT
  vg.id,
  vg.title_id,
  vg.platform_id,
  vg.display_title,
  p.name AS platform,
  vg.edition,
  vg.average_rating,
  vg.rating_count,
  vg.rating_updated_at,
  vg.genres,
  vg.popularity_score,
  vg.rating,
  vg.synopsis,
  vg.release_date,
  vg.developer,
  vg.region_codes,
  vg.metadata
FROM public.video_games vg
JOIN public.platforms p ON p.id = vg.platform_id;

COMMENT ON VIEW public.video_games_enriched_vw IS 'Video games with platform name projected for compatibility consumers.';
