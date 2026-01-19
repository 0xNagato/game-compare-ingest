-- Re-run vg_source media link backfill with duplicate-safety guards.

DO $$
DECLARE
  has_title boolean;
  has_game_media boolean;
  has_video_game_sources boolean;
  has_vg_source_media_links boolean;
BEGIN
  -- Schema-drift tolerant: skip safely if required tables are missing.
  SELECT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema='public' AND table_name='game_media'
  ) INTO has_game_media;

  SELECT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema='public' AND table_name='video_game_sources'
  ) INTO has_video_game_sources;

  SELECT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema='public' AND table_name='vg_source_media_links'
  ) INTO has_vg_source_media_links;

  IF NOT has_game_media OR NOT has_video_game_sources OR NOT has_vg_source_media_links THEN
    RAISE NOTICE 'Skipping 9997_provider_media_links_backfill: missing required tables (game_media=% video_game_sources=% vg_source_media_links=%)',
      has_game_media, has_video_game_sources, has_vg_source_media_links;
    RETURN;
  END IF;

  SELECT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='game_media' AND column_name='title'
  ) INTO has_title;

  IF has_title THEN
    EXECUTE $$
      CREATE TEMP TABLE tmp_media_rows ON COMMIT DROP AS
      SELECT
        gm.video_game_id,
        lower(trim(gm.source::text)) AS slug,
        COALESCE(
          NULLIF(trim(gm.external_id::text), ''),
          NULLIF(trim(gm.url), ''),
          md5(
            COALESCE(gm.url, '') || ':' ||
            COALESCE(gm.media_type::text, '') || ':' ||
            COALESCE(gm.video_game_id::text, '')
          )
        ) AS provider_game_id,
        gm.url,
        gm.media_type::text AS media_type,
        gm.title,
        gm.provider_data::jsonb AS provider_data
      FROM public.game_media gm
      WHERE gm.source IS NOT NULL AND trim(gm.source::text) <> ''
    $$;
  ELSE
    EXECUTE $$
      CREATE TEMP TABLE tmp_media_rows ON COMMIT DROP AS
      SELECT
        gm.video_game_id,
        lower(trim(gm.source::text)) AS slug,
        COALESCE(
          NULLIF(trim(gm.external_id::text), ''),
          NULLIF(trim(gm.url), ''),
          md5(
            COALESCE(gm.url, '') || ':' ||
            COALESCE(gm.media_type::text, '') || ':' ||
            COALESCE(gm.video_game_id::text, '')
          )
        ) AS provider_game_id,
        gm.url,
        gm.media_type::text AS media_type,
        NULL::text AS title,
        gm.provider_data::jsonb AS provider_data
      FROM public.game_media gm
      WHERE gm.source IS NOT NULL AND trim(gm.source::text) <> ''
    $$;
  END IF;

  -- Drop unusable rows to keep downstream inserts clean.
  DELETE FROM tmp_media_rows
  WHERE slug IS NULL OR slug = '' OR provider_game_id IS NULL;

  -- Ensure one video_game_sources row per (provider, provider_game_id).
  INSERT INTO public.video_game_sources (
    provider_key,
    provider,
    provider_game_id,
    provider_slug,
    video_game_id,
    display_name,
    metadata,
    slug,
    created_at,
    updated_at
  )
  SELECT DISTINCT
    concat_ws(':', mr.slug, mr.provider_game_id) AS provider_key,
    mr.slug AS provider,
    mr.provider_game_id,
    mr.slug AS provider_slug,
    mr.video_game_id,
    COALESCE(mr.title, initcap(mr.slug)) AS display_name,
    COALESCE(mr.provider_data::json, '{}'::json) AS metadata,
    mr.slug AS slug,
    now(),
    now()
  FROM tmp_media_rows mr
  ON CONFLICT (provider, provider_game_id) DO UPDATE SET
    video_game_id = COALESCE(EXCLUDED.video_game_id, public.video_game_sources.video_game_id),
    metadata = COALESCE(EXCLUDED.metadata, public.video_game_sources.metadata),
    updated_at = now();

  -- Backfill vg_source_media_links using the freshly ensured sources.
  INSERT INTO public.vg_source_media_links (
    video_game_source_id,
    video_game_id,
    url,
    media_type,
    role,
    title,
    source,
    metadata
  )
  SELECT
    vgs.id,
    mr.video_game_id,
    mr.url,
    mr.media_type,
    NULL::text AS role,
    mr.title,
    mr.slug,
    COALESCE(mr.provider_data, '{}'::jsonb)
  FROM tmp_media_rows mr
  JOIN public.video_game_sources vgs
    ON vgs.provider = mr.slug
   AND vgs.provider_game_id = mr.provider_game_id
  WHERE mr.url IS NOT NULL AND mr.url <> ''
  ON CONFLICT (video_game_source_id, url) DO UPDATE SET
    title = COALESCE(EXCLUDED.title, public.vg_source_media_links.title),
    media_type = COALESCE(EXCLUDED.media_type, public.vg_source_media_links.media_type),
    metadata = COALESCE(public.vg_source_media_links.metadata, '{}'::jsonb) || COALESCE(EXCLUDED.metadata, '{}'::jsonb),
    updated_at = now();
END $$;
