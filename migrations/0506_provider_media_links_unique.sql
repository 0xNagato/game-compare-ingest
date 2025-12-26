-- 0506_provider_media_links_unique.sql
-- Ensure provider_media_links supports ON CONFLICT (provider_item_id, video_game_id, url, source)
BEGIN;

-- 1) Remove duplicate rows so the unique index can be created safely.
WITH
    ranked AS (
        SELECT
            id,
            ROW_NUMBER() OVER (
                PARTITION BY
                    provider_item_id,
                    COALESCE(video_game_id, -1),
                    COALESCE(url, ''),
                    COALESCE(source, '')
                ORDER BY
                    id
            ) AS rn
        FROM
            public.provider_media_links
    )
DELETE FROM public.provider_media_links pml USING ranked r
WHERE
    pml.id=r.id
    AND r.rn>1;

-- 2) Create the unique index backing the ON CONFLICT target used by ensure_provider_media_links_with_meta.
CREATE UNIQUE INDEX IF NOT EXISTS provider_media_links_item_vg_url_source_idx ON public.provider_media_links (provider_item_id, video_game_id, url, source);

COMMIT;