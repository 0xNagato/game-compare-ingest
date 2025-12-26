-- 0508_video_game_regional_prices.sql
-- Adds regional price payloads to video_games and exposes a software summary view.

BEGIN;

ALTER TABLE public.video_games
    ADD COLUMN IF NOT EXISTS regional_prices jsonb NOT NULL DEFAULT '[]'::jsonb;

CREATE OR REPLACE VIEW public.video_games_enriched AS
SELECT
    vg.id,
    vgt.product_id,
    vgt.id AS title_id,
    vgt.title AS canonical_title,
    COALESCE(vg.display_title, vgt.title) AS source_title,
    vg.slug,
    vgt.normalized_title,
    vgt.normalized_title AS title_normalized,
    NULL::text AS locale,
    NULL::text AS genre,
    vg.genres,
    NULL::text[] AS platform_codes,
    vg.region_codes,
    vg.regional_prices,
    NULL::jsonb AS external_ids,
    NULL::jsonb AS external_links,
    vg.release_date,
    vg.developer,
    vg.metadata,
    vg.created_at,
    vg.updated_at,
    NULL::timestamp with time zone AS last_synced_at
FROM public.video_games vg
LEFT JOIN public.video_game_titles vgt ON vgt.id = vg.title_id;

CREATE OR REPLACE VIEW public.software_titles_prices AS
SELECT
    s.product_id,
    vgt.id AS title_id,
    COALESCE(vg.display_title, vgt.title) AS display_title,
    vgt.title AS canonical_title,
    vg.id AS video_game_id,
    vg.platform_id,
    p.code AS platform_code,
    vg.regional_prices,
    vg.release_date,
    vg.updated_at
FROM public.software s
LEFT JOIN public.video_game_titles vgt ON vgt.product_id = s.product_id
LEFT JOIN public.video_games vg ON vg.title_id = vgt.id
LEFT JOIN public.platforms p ON p.id = vg.platform_id;

WITH price_data AS (
    SELECT
        vg.id AS video_game_id,
        COALESCE(
            jsonb_agg(
                jsonb_build_object(
                    'offer_jurisdiction_id', oj.id,
                    'region_code', COALESCE(j.region_code, co.iso2),
                    'region_label', CASE
                        WHEN j.region_code IS NULL OR j.region_code = '' THEN co.name
                        ELSE co.name || ' - ' || j.region_code
                    END,
                    'country_iso2', co.iso2,
                    'currency_code', curr.code,
                    'amount_minor', cp.amount_minor,
                    'recorded_at', cp.recorded_at,
                    'title', COALESCE(vg.display_title, vgt.title)
                )
                ORDER BY COALESCE(j.region_code, co.iso2), curr.code
            ),
            '[]'::jsonb
        ) AS prices
    FROM public.video_games vg
    JOIN public.video_game_titles vgt ON vgt.id = vg.title_id
    JOIN public.sellables s ON s.software_title_id = vgt.id
    JOIN public.offers o ON o.sellable_id = s.id
    JOIN public.offer_jurisdictions oj ON oj.offer_id = o.id
    JOIN public.current_price cp ON cp.offer_jurisdiction_id = oj.id
    JOIN public.jurisdictions j ON j.id = oj.jurisdiction_id
    JOIN public.countries co ON co.id = j.country_id
    JOIN public.currencies curr ON curr.id = oj.currency_id
    GROUP BY vg.id
)
UPDATE public.video_games vg
SET regional_prices = price_data.prices
FROM price_data
WHERE vg.id = price_data.video_game_id;

COMMIT;
