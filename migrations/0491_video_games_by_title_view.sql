-- 0491_video_games_by_title_view.sql
-- Purpose: Provide a title-level rollup showing multi-platform association and price availability,
-- without altering base tables. Idempotent.
CREATE
OR REPLACE VIEW public.video_games_by_title AS
WITH
    offers_for_title AS (
        SELECT
            vgt.id AS title_id,
            o.id AS offer_id
        FROM
            public.video_game_titles vgt
            JOIN public.sellables s ON s.software_title_id = vgt.id
            JOIN public.offers o ON o.sellable_id = s.id
    )
SELECT
    vgt.id AS title_id,
    vgt.title AS title,
    vgt.normalized_title,
    MIN(s.id) AS sellable_id,
    array_agg (
        DISTINCT vg.platform_id
        ORDER BY
            vg.platform_id
    ) AS platform_ids,
    array_agg (
        DISTINCT p.code
        ORDER BY
            p.code
    ) AS platform_codes,
    COUNT(DISTINCT o.offer_id) AS offer_count,
    COALESCE(
        (
            SELECT
                COUNT(*)
            FROM
                public.current_price cp
                JOIN public.offer_jurisdictions oj ON oj.id = cp.offer_jurisdiction_id
            WHERE
                oj.offer_id = ANY (
                    SELECT
                        o2.offer_id
                    FROM
                        offers_for_title o2
                    WHERE
                        o2.title_id = vgt.id
                )
        ),
        0
    ) AS markets_with_current_price
FROM
    public.video_game_titles vgt
    LEFT JOIN public.video_games vg ON vg.title_id = vgt.id
    LEFT JOIN public.platforms p ON p.id = vg.platform_id
    LEFT JOIN public.sellables s ON s.software_title_id = vgt.id
    LEFT JOIN offers_for_title o ON o.title_id = vgt.id
GROUP BY
    vgt.id,
    vgt.title,
    vgt.normalized_title;