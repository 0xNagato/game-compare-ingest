-- 0521_product_media_bridge_view.sql
-- Read-only bridge view that presents provider media in a Laravel-friendly "product_media" shape.
--
-- Motivation:
-- - Our canonical table is public.game_media (unified across providers).
-- - Legacy tables public.game_images/public.game_videos still exist in older schema snapshots.
-- - Laravel apps often want a single denormalized surface with url/thumbnail_url and a simple media_type (image|video).
--
-- Notes:
-- - This is a VIEW (no writes). If you want Eloquent to use it, prefer treating it as read-only.
-- - The `id` is a deterministic 64-bit hash (NOT a sequence). Collisions are extremely unlikely.
-- - Only creates view if game_media table exists

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='game_media') THEN
    CREATE OR REPLACE VIEW public.product_media_bridge AS
    WITH gm AS (
      SELECT
        hashtextextended(
          concat_ws('|', 'gm', gm.video_game_id::text, gm.source::text, gm.external_id, gm.media_type::text),
          0
        ) AS id,
        vgt.product_id AS product_id,
        gm.source::text AS source,
        gm.external_id AS external_id,
        CASE
          WHEN gm.media_type::text IN ('trailer','gameplay','preview','teaser','clip')
            OR NULLIF(gm.stream_url, '') IS NOT NULL
          THEN 'video'
          ELSE 'image'
        END AS media_type,
        NULLIF(gm.title, '') AS title,
        NULL::text AS caption,
        COALESCE(NULLIF(gm.original_url, ''), gm.url) AS url,
        COALESCE(NULLIF(gm.thumbnail_url, ''), NULLIF(gm.poster_url, ''), NULLIF(gm.cdn_url, '')) AS thumbnail_url,
        NULLIF(gm.provider_data->>'attribution', '') AS attribution,
        NULLIF(gm.provider_data->>'license', '') AS license,
        NULLIF(gm.provider_data->>'license_url', '') AS license_url,
        gm.created_at AS fetched_at,
        gm.provider_data AS metadata,
        gm.created_at AS created_at,
        gm.created_at AS updated_at,
        (
          gm.media_type IN ('cover','hero')
          OR (
            gm.media_type::text IN ('trailer','preview')
            AND (
              NULLIF(gm.stream_url, '') IS NOT NULL
              OR COALESCE(NULLIF(gm.original_url, ''), gm.url) ILIKE '%youtube%'
              OR COALESCE(NULLIF(gm.original_url, ''), gm.url) ILIKE '%youtu.be%'
            )
          )
        ) AS is_primary,
        gm.width::integer AS width,
        gm.height::integer AS height,
        0.0::numeric AS quality_score
      FROM public.game_media gm
      LEFT JOIN public.video_games vg ON vg.id = gm.video_game_id
      LEFT JOIN public.video_game_titles vgt ON vgt.id = vg.title_id
    ),
    legacy_images AS (
      SELECT
        hashtextextended(
          concat_ws('|', 'gi', gi.id::text, gi.game_provider_id::text, COALESCE(gi.image_key, ''), gi.url),
          0
        ) AS id,
        vgt.product_id AS product_id,
        COALESCE(gp.provider_key, vgs.provider, 'legacy')::text AS source,
        gi.image_key AS external_id,
        'image'::text AS media_type,
        NULLIF(gi.caption, '') AS title,
        NULL::text AS caption,
        gi.url::text AS url,
        COALESCE(NULLIF(gi.small_url, ''), gi.url::text) AS thumbnail_url,
        NULL::text AS attribution,
        NULL::text AS license,
        NULL::text AS license_url,
        gi.created_at AS fetched_at,
        COALESCE(gi.provider_payload::jsonb, gi.metadata::jsonb, '{}'::jsonb) AS metadata,
        COALESCE(gi.created_at, now()) AS created_at,
        COALESCE(gi.updated_at, gi.created_at, now()) AS updated_at,
        false AS is_primary,
        gi.width::integer AS width,
        gi.height::integer AS height,
        0.0::numeric AS quality_score
      FROM public.game_images gi
      LEFT JOIN public.game_providers gp ON gp.id = gi.game_provider_id
      LEFT JOIN public.video_game_sources vgs ON vgs.id = COALESCE(gi.video_game_source_id, gp.video_game_source_id)
      LEFT JOIN public.video_games vg ON vg.id = vgs.video_game_id
      LEFT JOIN public.video_game_titles vgt ON vgt.id = vg.title_id
      WHERE FALSE  -- Skip if game_images doesn't exist
    ),
    legacy_videos AS (
      SELECT
        hashtextextended(
          concat_ws('|', 'gv', gv.id::text, gv.game_provider_id::text, COALESCE(gv.video_key, ''), COALESCE(gv.stream_url, gv.embed_url, gv.site_detail_url, '')),
          0
        ) AS id,
        vgt.product_id AS product_id,
        COALESCE(gp.provider_key, vgs.provider, 'legacy')::text AS source,
        gv.video_key AS external_id,
        'video'::text AS media_type,
        NULLIF(gv.name, '') AS title,
        NULLIF(gv.description, '') AS caption,
        COALESCE(NULLIF(gv.stream_url, ''), NULLIF(gv.embed_url, ''), NULLIF(gv.site_detail_url, '')) AS url,
        NULLIF((gv.thumbnails::jsonb->>'small'), '') AS thumbnail_url,
        NULL::text AS attribution,
        NULL::text AS license,
        NULL::text AS license_url,
        gv.created_at AS fetched_at,
        jsonb_strip_nulls(
          jsonb_build_object(
            'site_detail_url', gv.site_detail_url,
            'embed_url', gv.embed_url,
            'stream_url', gv.stream_url,
            'duration_seconds', gv.duration_seconds,
            'published_at', gv.published_at,
            'thumbnails', gv.thumbnails,
            'provider_payload', gv.provider_payload
          )
        ) AS metadata,
        COALESCE(gv.created_at, now()) AS created_at,
        COALESCE(gv.updated_at, gv.created_at, now()) AS updated_at,
        false AS is_primary,
        NULL::integer AS width,
        NULL::integer AS height,
        0.0::numeric AS quality_score
      FROM public.game_videos gv
      LEFT JOIN public.game_providers gp ON gp.id = gv.game_provider_id
      LEFT JOIN public.video_game_sources vgs ON vgs.id = COALESCE(gv.video_game_source_id, gp.video_game_source_id)
      LEFT JOIN public.video_games vg ON vg.id = vgs.video_game_id
      LEFT JOIN public.video_game_titles vgt ON vgt.id = vg.title_id
      WHERE FALSE  -- Skip if game_videos doesn't exist
    )
    SELECT * FROM gm
    UNION ALL
    SELECT * FROM legacy_images
    UNION ALL
    SELECT * FROM legacy_videos;
  END IF;
END $$;
