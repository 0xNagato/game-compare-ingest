-- 0490_restore_software_title_link.sql
-- Purpose: restore a strict 1-to-1 mapping between video_game_titles and software
--          and ensure callers can rely on video_game_id everywhere.

SET statement_timeout = 0;

-- Drop dependent views (they are recreated in later migrations).
DROP VIEW IF EXISTS public.video_games_enriched;
DROP VIEW IF EXISTS public.software_titles_prices;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'software'
      AND column_name = 'video_game_id'
  )
  AND NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'software'
      AND column_name = 'product_id'
  ) THEN
    ALTER TABLE public.software RENAME COLUMN video_game_id TO product_id;
  END IF;
END$$;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'video_game_titles'
      AND column_name = 'product_id'
  )
  AND NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'video_game_titles'
      AND column_name = 'video_game_id'
  ) THEN
    ALTER TABLE public.video_game_titles RENAME COLUMN product_id TO video_game_id;
  END IF;
END$$;

-- Drop legacy constraints so we can rebuild them consistently.
ALTER TABLE IF EXISTS public.video_game_titles
  DROP CONSTRAINT IF EXISTS video_game_titles_product_id_fkey;
ALTER TABLE IF EXISTS public.video_game_titles
  DROP CONSTRAINT IF EXISTS video_game_titles_product_id_key;
ALTER TABLE IF EXISTS public.video_game_titles
  DROP CONSTRAINT IF EXISTS video_game_titles_video_game_id_fkey;
ALTER TABLE IF EXISTS public.video_game_titles
  DROP CONSTRAINT IF EXISTS video_game_titles_video_game_id_key;

-- 1) Backfill products for titles that already referenced a product id.
WITH legacy_titles AS (
  SELECT vgt.video_game_id,
         MAX(vgt.title) AS title
  FROM public.video_game_titles vgt
  WHERE vgt.video_game_id IS NOT NULL
  GROUP BY vgt.video_game_id
)
INSERT INTO public.products (id, slug, name, category)
SELECT legacy_titles.video_game_id,
       format('legacy-vgt-%s', legacy_titles.video_game_id),
       legacy_titles.title,
       'software'
FROM legacy_titles
ON CONFLICT (id) DO UPDATE
  SET name = EXCLUDED.name,
      category = 'software';

-- 2) Mint products (and ids) for titles still missing a link.
WITH missing AS (
  SELECT id AS title_id,
         title,
         format('auto-vgt-%s', id) AS slug
  FROM public.video_game_titles
  WHERE video_game_id IS NULL
),
upserted AS (
  INSERT INTO public.products (slug, name, category)
  SELECT slug, title, 'software'
  FROM missing
  ON CONFLICT (slug) DO UPDATE
    SET name = EXCLUDED.name,
        category = 'software'
  RETURNING slug, id AS product_id
)
UPDATE public.video_game_titles vgt
SET video_game_id = up.product_id
FROM upserted up
JOIN missing m ON m.slug = up.slug
WHERE vgt.id = m.title_id
  AND vgt.video_game_id IS NULL;

-- 3) Ensure the software table carries every software product id.
DO $$
DECLARE
  col text;
BEGIN
  SELECT column_name
  INTO col
  FROM information_schema.columns
  WHERE table_schema = 'public'
    AND table_name = 'software'
    AND column_name IN ('video_game_id', 'product_id')
  ORDER BY CASE column_name WHEN 'product_id' THEN 1 ELSE 2 END
  LIMIT 1;

  IF col IS NULL THEN
    RAISE EXCEPTION 'software table missing expected video_game_id/product_id column';
  END IF;

  EXECUTE format(
    'INSERT INTO public.software (%1$I)
       SELECT p.id
       FROM public.products p
       WHERE p.category = ''software''
     ON CONFLICT (%1$I) DO NOTHING',
    col
  );
END$$;

-- 4) Repair any lingering NULL links after inserts (covers legacy rows).
DO $$
DECLARE
  col text;
BEGIN
  SELECT column_name
  INTO col
  FROM information_schema.columns
  WHERE table_schema = 'public'
    AND table_name = 'video_game_titles'
    AND column_name IN ('video_game_id', 'product_id')
  ORDER BY CASE column_name WHEN 'product_id' THEN 1 ELSE 2 END
  LIMIT 1;

  IF col IS NULL THEN
    RAISE EXCEPTION 'video_game_titles missing expected video_game_id/product_id column';
  END IF;

  EXECUTE format(
    'UPDATE public.video_game_titles vgt
        SET %1$I = p.id
      FROM public.products p
      WHERE vgt.%1$I IS NULL
        AND p.category = ''software''
        AND p.slug = format(''auto-vgt-%%s'', vgt.id)',
    col
  );
END$$;

-- 5) Guard against duplicates before re-enabling constraints.
DO $$
DECLARE
  dup boolean;
BEGIN
  EXECUTE
    'SELECT EXISTS (
       SELECT 1
       FROM public.video_game_titles
       GROUP BY video_game_id
       HAVING COUNT(*) > 1
     )'
  INTO dup;

  IF dup THEN
    RAISE EXCEPTION 'Duplicate video_game_id values remain in video_game_titles; resolve before enforcing 1-1';
  END IF;
END$$;

-- 6) Align the products sequence with the newly inserted ids.
-- Use GREATEST to ensure value is never less than 1 (sequences must be >= 1)
SELECT setval('products_id_seq', GREATEST(COALESCE((SELECT MAX(id) FROM public.products), 0), 1));

-- 7) Rebuild the strict constraints.
ALTER TABLE public.video_game_titles
  ALTER COLUMN video_game_id SET NOT NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'video_game_titles_video_game_id_key'
  ) THEN
    ALTER TABLE public.video_game_titles
      ADD CONSTRAINT video_game_titles_video_game_id_key UNIQUE (video_game_id);
  END IF;
END$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'video_game_titles_video_game_id_fkey'
  ) THEN
    ALTER TABLE public.video_game_titles
      ADD CONSTRAINT video_game_titles_video_game_id_fkey
      FOREIGN KEY (video_game_id)
      REFERENCES public.software(product_id)
      ON DELETE CASCADE
      DEFERRABLE INITIALLY IMMEDIATE;
  END IF;
END$$;
