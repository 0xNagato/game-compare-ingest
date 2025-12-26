-- 9997_provider_media_links_backfill.sql
-- Re-run provider media link backfill with duplicate-safety guards.

DO $$
DECLARE has_title boolean;
BEGIN
  SELECT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='game_media' AND column_name='title'
  ) INTO has_title;

  WITH media_rows AS (
    SELECT
      gm.video_game_id,
      lower(gm.source::text) AS slug,
      'media:' || COALESCE(gm.external_id, gm.url) AS external_id,
      gm.url,
      gm.media_type::text AS media_type,
      gm.title,
      gm.provider_data
    FROM public.game_media gm
    WHERE gm.source IS NOT NULL
  ),
  inserted_items AS (
    INSERT INTO public.provider_items (provider_id, external_id)
    SELECT DISTINCT p.id AS provider_id,
           mr.external_id
    FROM media_rows mr
    JOIN public.providers p ON p.slug = mr.slug
    ON CONFLICT (provider_id, external_id) DO NOTHING
    RETURNING id, provider_id, external_id
  )
  INSERT INTO public.provider_media_links (
    provider_item_id, video_game_id, url, media_type, role, title, source, metadata
  )
  SELECT
    pi.id AS provider_item_id,
    mr.video_game_id,
    mr.url,
    mr.media_type,
    NULL::text AS role,
    CASE WHEN has_title THEN mr.title ELSE NULL::text END AS title,
    mr.slug AS source,
    mr.provider_data AS metadata
  FROM media_rows mr
  JOIN public.providers p ON p.slug = mr.slug
  JOIN public.provider_items pi
    ON pi.provider_id = p.id
   AND pi.external_id = mr.external_id
  LEFT JOIN public.provider_media_links pml
    ON pml.provider_item_id = pi.id
   AND pml.url = mr.url
  WHERE pml.id IS NULL;
END $$;
