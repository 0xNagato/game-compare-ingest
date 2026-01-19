-- 0538_products_titles_video_games_bridge.sql
-- Enforce the canonical traversal: products → video_game_titles → video_games.
-- Also ensure video_game_sources maintain explicit child video_game references.

SET statement_timeout = 0;

/* ============================================================
 * 1. video_game_titles must own product_id (FK → products)
 * ============================================================ */
ALTER TABLE IF EXISTS public.video_game_titles
  ADD COLUMN IF NOT EXISTS product_id bigint;

ALTER TABLE IF EXISTS public.video_game_titles
  ADD COLUMN IF NOT EXISTS video_game_ids jsonb;

UPDATE public.video_game_titles
SET video_game_ids = COALESCE(video_game_ids, '[]'::jsonb)
WHERE video_game_ids IS NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'video_game_titles_video_game_ids_is_array'
  ) THEN
    ALTER TABLE public.video_game_titles
      ADD CONSTRAINT video_game_titles_video_game_ids_is_array
      CHECK (video_game_ids IS NULL OR jsonb_typeof(video_game_ids) = 'array');
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='video_game_titles' AND column_name='product_id'
  ) THEN
    RETURN;
  END IF;

  -- Only attempt backfill from video_games.product_id if that column exists (legacy schema)
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='video_games' AND column_name='product_id'
  ) THEN
    -- Direct link via legacy video_game_id
    UPDATE public.video_game_titles vgt
    SET product_id = vg.product_id
    FROM public.video_games vg
    WHERE vgt.product_id IS NULL
      AND vgt.video_game_id IS NOT NULL
      AND vg.id = vgt.video_game_id
      AND vg.product_id IS NOT NULL;

    -- Link via JSON array of video_game_ids (if column exists)
    IF EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema='public' AND table_name='video_game_titles' AND column_name='video_game_ids'
    ) THEN
      WITH arr AS (
        SELECT vgt.id AS title_id, MIN(vg.product_id) AS product_id
        FROM public.video_game_titles vgt
        JOIN LATERAL (
          SELECT (elem.value)::bigint AS vg_id
          FROM jsonb_array_elements_text(COALESCE(vgt.video_game_ids, '[]'::jsonb)) elem
          WHERE elem.value ~ '^[0-9]+$'
        ) j ON TRUE
        JOIN public.video_games vg ON vg.id = j.vg_id AND vg.product_id IS NOT NULL
        WHERE vgt.product_id IS NULL
        GROUP BY vgt.id
      )
      UPDATE public.video_game_titles vgt
      SET product_id = arr.product_id
      FROM arr
      WHERE vgt.id = arr.title_id
        AND vgt.product_id IS NULL;
    END IF;
  END IF;

  -- Fallback: mint placeholder products for any remaining titles.
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='products' AND column_name='platform'
  ) THEN
    -- Products table has platform column
    WITH missing AS (
      SELECT vgt.id,
             COALESCE(NULLIF(trim(vgt.title), ''), format('Untitled #%s', vgt.id)) AS name
      FROM public.video_game_titles vgt
      WHERE vgt.product_id IS NULL
    )
    INSERT INTO public.products (name, platform, slug, category, created_at, updated_at)
    SELECT name, 'unknown', format('auto-vgt-title-%s', id), 'software', now(), now()
    FROM missing
    ON CONFLICT (slug) DO NOTHING;
  ELSE
    -- Products table does NOT have platform column
    WITH missing AS (
      SELECT vgt.id,
             COALESCE(NULLIF(trim(vgt.title), ''), format('Untitled #%s', vgt.id)) AS name
      FROM public.video_game_titles vgt
      WHERE vgt.product_id IS NULL
    )
    INSERT INTO public.products (name, slug, category, created_at, updated_at)
    SELECT name, format('auto-vgt-title-%s', id), 'software', now(), now()
    FROM missing
    ON CONFLICT (slug) DO NOTHING;
  END IF;

  UPDATE public.video_game_titles vgt
  SET product_id = p.id
  FROM public.products p
  WHERE vgt.product_id IS NULL
    AND p.slug = format('auto-vgt-title-%s', vgt.id);
END $$;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='video_game_titles' AND column_name='product_id'
  ) THEN
    EXECUTE 'ALTER TABLE public.video_game_titles ALTER COLUMN product_id SET NOT NULL';
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname='video_game_titles_product_id_fk'
  ) THEN
    ALTER TABLE public.video_game_titles
      ADD CONSTRAINT video_game_titles_product_id_fk
      FOREIGN KEY (product_id) REFERENCES public.products(id)
      ON DELETE CASCADE;
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_video_game_titles_product_id
  ON public.video_game_titles (product_id);

COMMENT ON COLUMN public.video_game_titles.product_id IS
  'Non-negotiable: ONLY video_game_titles carry product_id. Products relate to video_games strictly via titles.';

/* ============================================================
 * 2. video_games must reference titles (title_id) instead of products
 * ============================================================ */
ALTER TABLE IF EXISTS public.video_games
  ADD COLUMN IF NOT EXISTS title_id bigint;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='video_games' AND column_name='title_id'
  ) THEN
    RETURN;
  END IF;

  -- Direct mapping via legacy video_game_id reference on titles
  UPDATE public.video_games vg
  SET title_id = vgt.id
  FROM public.video_game_titles vgt
  WHERE vg.title_id IS NULL
    AND vgt.video_game_id = vg.id;

  -- Mapping via JSON arrays on titles (if column exists)
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='video_game_titles' AND column_name='video_game_ids'
  ) THEN
    WITH arr AS (
      SELECT DISTINCT ON (vg.id) vg.id AS video_game_id, vgt.id AS title_id
      FROM public.video_games vg
      JOIN public.video_game_titles vgt
        ON EXISTS (
          SELECT 1
          FROM jsonb_array_elements_text(COALESCE(vgt.video_game_ids, '[]'::jsonb)) elem
          WHERE elem.value ~ '^[0-9]+$' AND elem.value::bigint = vg.id
        )
      WHERE vg.title_id IS NULL
      ORDER BY vg.id, vgt.id
    )
    UPDATE public.video_games vg
    SET title_id = arr.title_id
    FROM arr
    WHERE vg.id = arr.video_game_id
      AND vg.title_id IS NULL;
  END IF;

  -- Fallback: match by shared product_id before we drop it
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='video_games' AND column_name='product_id'
  ) THEN
    WITH prod_match AS (
      SELECT vg.id AS video_game_id, MIN(vgt.id) AS title_id
      FROM public.video_games vg
      JOIN public.video_game_titles vgt
        ON vgt.product_id = vg.product_id
      WHERE vg.title_id IS NULL AND vgt.product_id IS NOT NULL
      GROUP BY vg.id
    )
    UPDATE public.video_games vg
    SET title_id = prod_match.title_id
    FROM prod_match
    WHERE vg.id = prod_match.video_game_id
      AND vg.title_id IS NULL;
  END IF;

  -- Create placeholder titles for any remaining orphaned games
  WITH missing AS (
    SELECT vg.id AS video_game_id,
           COALESCE(
             NULLIF(trim(vgt.title), ''),
             format('Video Game #%s', vg.id)
           ) AS title,
           vgt.product_id AS product_id
    FROM public.video_games vg
    LEFT JOIN public.video_game_titles vgt ON vgt.video_game_id = vg.id
    WHERE vg.title_id IS NULL
  ),
  seeded AS (
    INSERT INTO public.video_game_titles (
      title,
      normalized_title,
      product_id,
      video_game_id,
      video_game_ids,
      created_at,
      updated_at
    )
    SELECT title,
           LOWER(REGEXP_REPLACE(title, '[^a-z0-9]+', '-', 'gi')),
           COALESCE(product_id, p.id),
           video_game_id,
           jsonb_build_array(video_game_id),
           now(),
           now()
    FROM missing m
    LEFT JOIN LATERAL (
      SELECT id
      FROM public.products
      WHERE id = m.product_id
      LIMIT 1
    ) p ON TRUE
    ON CONFLICT DO NOTHING
    RETURNING id, video_game_id
  )
  UPDATE public.video_games vg
  SET title_id = seeded.id
  FROM seeded
  WHERE vg.id = seeded.video_game_id
    AND vg.title_id IS NULL;
END $$;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='video_games' AND column_name='title_id'
  ) THEN
    EXECUTE 'ALTER TABLE public.video_games ALTER COLUMN title_id SET NOT NULL';
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname='video_games_title_id_fk'
  ) THEN
    ALTER TABLE public.video_games
      ADD CONSTRAINT video_games_title_id_fk
      FOREIGN KEY (title_id) REFERENCES public.video_game_titles(id)
      ON DELETE CASCADE;
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS video_games_title_id_idx
  ON public.video_games (title_id);

CREATE UNIQUE INDEX IF NOT EXISTS video_games_title_platform_edition_unique
  ON public.video_games (title_id, platform_id, COALESCE(edition, ''));

COMMENT ON COLUMN public.video_games.title_id IS
  'Every playable SKU must reference a canonical video_game_title. Products are never referenced directly from video_games.';

-- Remove legacy product_id from video_games now that title_id is canonical
ALTER TABLE IF EXISTS public.video_games
  DROP CONSTRAINT IF EXISTS video_games_product_id_foreign;

ALTER TABLE IF EXISTS public.video_games
  DROP COLUMN IF EXISTS product_id;

/* ============================================================
 * 3. video_game_sources must expose child video_game identifiers
 * ============================================================ */
ALTER TABLE IF EXISTS public.video_game_sources
  ADD COLUMN IF NOT EXISTS video_game_ids jsonb NOT NULL DEFAULT '[]'::jsonb;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'video_game_sources_video_game_ids_is_array'
  ) THEN
    ALTER TABLE public.video_game_sources
      ADD CONSTRAINT video_game_sources_video_game_ids_is_array
      CHECK (jsonb_typeof(video_game_ids) = 'array');
  END IF;
END $$;

UPDATE public.video_game_sources
SET video_game_ids = jsonb_build_array(video_game_id)
WHERE (video_game_ids IS NULL OR jsonb_array_length(video_game_ids) = 0)
  AND video_game_id IS NOT NULL;

CREATE OR REPLACE FUNCTION public.video_game_sources_sync_video_game_ids()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  merged jsonb;
BEGIN
  IF NEW.video_game_ids IS NULL OR jsonb_typeof(NEW.video_game_ids) <> 'array' THEN
    NEW.video_game_ids := '[]'::jsonb;
  END IF;

  IF NEW.video_game_id IS NOT NULL THEN
    SELECT COALESCE(jsonb_agg(DISTINCT v ORDER BY v), '[]'::jsonb)
    INTO merged
    FROM (
      SELECT NEW.video_game_id::bigint AS v
      UNION ALL
      SELECT (elem.value)::bigint AS v
      FROM jsonb_array_elements_text(NEW.video_game_ids) elem
      WHERE elem.value ~ '^[0-9]+$'
    ) s;

    NEW.video_game_ids := merged;
  END IF;

  IF NEW.video_game_id IS NULL AND jsonb_array_length(NEW.video_game_ids) > 0 THEN
    BEGIN
      NEW.video_game_id := (NEW.video_game_ids ->> 0)::bigint;
    EXCEPTION WHEN OTHERS THEN
      NEW.video_game_id := NULL;
    END;
  END IF;

  RETURN NEW;
END;
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_trigger
    WHERE tgname = 'video_game_sources_sync_video_game_ids_trg'
  ) THEN
    CREATE TRIGGER video_game_sources_sync_video_game_ids_trg
    BEFORE INSERT OR UPDATE ON public.video_game_sources
    FOR EACH ROW
    EXECUTE FUNCTION public.video_game_sources_sync_video_game_ids();
  END IF;
END $$;

COMMENT ON COLUMN public.video_game_sources.video_game_ids IS
  'Canonical list of child video_games (per platform/edition) represented by this provider record.';

/* ============================================================
 * 4. Final documentation touch points
 * ============================================================ */
COMMENT ON TABLE public.video_game_titles IS
  'Canonical titles. ONLY this table links products to per-platform video_games via product_id.';

COMMENT ON TABLE public.video_games IS
  'Per-platform (and optional edition) records. Must reference video_game_titles via title_id; never link to products directly.';
