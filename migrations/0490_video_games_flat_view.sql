-- 0490_video_games_flat_view.sql
-- Purpose: Provide a convenient read view where the canonical title appears near IDs,
-- and edition is moved to the end without altering base table column order.
-- Idempotent.
CREATE
OR REPLACE VIEW public.video_games_flat AS
SELECT
    vg.id,
    vg.title_id,
    vg.sellable_id,
    vgt.title AS title, -- canonical title from video_game_titles
    vg.display_title, -- per-row display title (if present)
    vg.platform_id,
    vg.slug,
    vg.release_date,
    vg.developer,
    vg.popularity_score,
    vg.rating,
    vg.average_rating,
    vg.rating_count,
    vg.rating_updated_at,
    vg.region_codes,
    vg.genres,
    vg.metadata,
    vg.created_at,
    vg.updated_at,
    vg.edition -- keep edition but at the end
FROM
    public.video_games vg
    JOIN public.video_game_titles vgt ON vgt.id = vg.title_id;

-- Optional helper index via materialized view can be added in the future if needed.