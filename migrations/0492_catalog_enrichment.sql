-- 0492_catalog_enrichment.sql
-- Purpose: backfill region codes, hydrate catalog display surfaces, and project metadata into typed columns.
-- Idempotent and safe to re-run.
BEGIN;

-- Link video games to their sellable row when missing (software only).
UPDATE public.video_games vg
SET sellable_id = s.id
FROM public.sellables s
WHERE s.kind = 'software'
  AND s.software_title_id = vg.title_id
  AND (vg.sellable_id IS DISTINCT FROM s.id);

-- Genres array
WITH genre_data AS (
    SELECT
        vg.id,
        ARRAY(
            SELECT DISTINCT value
            FROM jsonb_array_elements_text(vg.metadata -> 'genres_union') AS value
        ) AS genres_arr
    FROM public.video_games vg
    WHERE vg.metadata ? 'genres_union'
)
UPDATE public.video_games vg
SET genres = genre_data.genres_arr
FROM genre_data
WHERE vg.id = genre_data.id
  AND genre_data.genres_arr IS NOT NULL
  AND array_length(genre_data.genres_arr, 1) > 0
  AND (vg.genres IS NULL OR array_length(vg.genres, 1) = 0);

-- Ratings
WITH rating_data AS (
    SELECT
        vg.id,
        NULLIF((vg.metadata ->> 'rating_global')::double precision, 0.0) AS avg_rating,
        NULLIF((vg.metadata ->> 'rating_count_global')::bigint, 0) AS rating_count
    FROM public.video_games vg
    WHERE vg.metadata ? 'rating_global' OR vg.metadata ? 'rating_count_global'
)
UPDATE public.video_games vg
SET
    average_rating = COALESCE(vg.average_rating, rating_data.avg_rating),
    rating_count = COALESCE(vg.rating_count, rating_data.rating_count),
    rating_updated_at = CASE
        WHEN vg.rating_updated_at IS NULL AND (rating_data.avg_rating IS NOT NULL OR rating_data.rating_count IS NOT NULL)
        THEN now()
        ELSE vg.rating_updated_at
    END
FROM rating_data
WHERE vg.id = rating_data.id
  AND ((rating_data.avg_rating IS NOT NULL AND vg.average_rating IS NULL)
       OR (rating_data.rating_count IS NOT NULL AND vg.rating_count IS NULL));

-- Developer
WITH developer_data AS (
    SELECT vg.id, NULLIF(vg.metadata ->> 'developer', '') AS developer
    FROM public.video_games vg
    WHERE vg.metadata ? 'developer'
)
UPDATE public.video_games vg
SET developer = developer_data.developer
FROM developer_data
WHERE vg.id = developer_data.id
  AND developer_data.developer IS NOT NULL
  AND (vg.developer IS NULL OR vg.developer = '');

-- Clean up platforms family column using simple heuristics (only where unset).
WITH family_guess AS (
    SELECT
        id,
        CASE
            WHEN code ILIKE 'ps%' OR name ILIKE 'PlayStation%' THEN 'playstation'
            WHEN code ILIKE 'xs%' OR code ILIKE 'xb%' OR name ILIKE 'Xbox%' THEN 'xbox'
            WHEN code ILIKE 'switch' OR name ILIKE 'Switch%' OR name ILIKE 'Nintendo%' THEN 'nintendo'
            WHEN code ILIKE 'wii%' OR code ILIKE '3ds%' OR code ILIKE 'ds%' OR name ILIKE 'Nintendo%' THEN 'nintendo'
            WHEN code ILIKE 'pc' OR code ILIKE 'steam%' OR name ILIKE 'Windows%' OR name ILIKE 'PC%' THEN 'pc'
            WHEN code ILIKE 'ios%' OR code ILIKE 'android%' OR name ILIKE '%Mobile%' THEN 'mobile'
            WHEN code ILIKE 'mac%' OR name ILIKE 'Mac%' THEN 'pc'
            ELSE NULL
        END AS family
    FROM public.platforms
)
UPDATE public.platforms p
SET family = f.family
FROM family_guess f
WHERE p.id = f.id
  AND f.family IS NOT NULL
  AND (p.family IS NULL OR p.family = '');

-- Hydrate provider_media_links typed columns from persisted metadata.
UPDATE public.provider_media_links pml
SET
    media_type = COALESCE(pml.media_type, NULLIF(pml.metadata ->> 'type', '')),
    role = COALESCE(pml.role, NULLIF(pml.metadata ->> 'role', '')),
    title = COALESCE(pml.title, NULLIF(pml.metadata ->> 'title', ''))
WHERE (pml.media_type IS NULL OR pml.role IS NULL OR pml.title IS NULL)
  AND pml.metadata IS NOT NULL
  AND pml.metadata <> '{}'::jsonb;

COMMIT;
