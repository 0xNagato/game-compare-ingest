-- 0489_decouple_titles_products.sql
-- Purpose: decouple video_game_titles from products; link video_games to sellables via non-FK constraints;
-- add search and performance indexes (GIN/BRIN), and backfill sellable_id.
-- Idempotent, safe to re-run.

-- Extensions (Supabase-friendly)
create extension if not exists pg_trgm;
-- Create bloom only if available and permitted (Supabase often restricts it)
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_available_extensions WHERE name='bloom') THEN
    BEGIN
      EXECUTE 'CREATE EXTENSION IF NOT EXISTS bloom';
    EXCEPTION WHEN insufficient_privilege THEN
      -- skip silently on restricted environments
      NULL;
    END;
  END IF;
END$$;

-- 1) Drop hard link between video_game_titles and software(products)
--    Remove FK and uniqueness on (product_id). Keep column for legacy traceability.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'video_game_titles_product_id_fkey'
  ) THEN
    ALTER TABLE public.video_game_titles DROP CONSTRAINT video_game_titles_product_id_fkey;
  END IF;
  IF EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'video_game_titles_product_id_key'
  ) THEN
    ALTER TABLE public.video_game_titles DROP CONSTRAINT video_game_titles_product_id_key;
  END IF;
EXCEPTION WHEN undefined_table THEN
  -- table missing in some environments; ignore
  NULL;
END$$;

-- Ensure product_id is nullable (if previously NOT NULL)
ALTER TABLE public.video_game_titles
  ALTER COLUMN product_id DROP NOT NULL;

-- 2) Ensure robust text-search and fast title lookups (non-FK linkage)
--    Use trigram on title/normalized_title and GIN on search_vector
DO $$
BEGIN
  -- Try GIN trigram first; if not available, fall back to GiST trigram; final fallback to simple btree
  BEGIN
    EXECUTE 'CREATE INDEX IF NOT EXISTS vgt_title_trgm_idx ON public.video_game_titles USING gin (title gin_trgm_ops)';
  EXCEPTION WHEN undefined_object OR invalid_object_definition THEN
    BEGIN
      EXECUTE 'CREATE INDEX IF NOT EXISTS vgt_title_trgm_idx ON public.video_game_titles USING gist (title gist_trgm_ops)';
    EXCEPTION WHEN undefined_object OR invalid_object_definition THEN
      EXECUTE 'CREATE INDEX IF NOT EXISTS vgt_title_btree_idx ON public.video_game_titles (title)';
    END;
  END;
END$$;

DO $$
BEGIN
  BEGIN
    EXECUTE 'CREATE INDEX IF NOT EXISTS vgt_norm_title_trgm_idx ON public.video_game_titles USING gin (normalized_title gin_trgm_ops)';
  EXCEPTION WHEN undefined_object OR invalid_object_definition THEN
    BEGIN
      EXECUTE 'CREATE INDEX IF NOT EXISTS vgt_norm_title_trgm_idx ON public.video_game_titles USING gist (normalized_title gist_trgm_ops)';
    EXCEPTION WHEN undefined_object OR invalid_object_definition THEN
      EXECUTE 'CREATE INDEX IF NOT EXISTS vgt_norm_title_btree_idx ON public.video_game_titles (normalized_title)';
    END;
  END;
END$$;
CREATE INDEX IF NOT EXISTS vgt_search_gin_idx
  ON public.video_game_titles USING gin (search_vector);

-- 3) Link video_games to sellables without FK
--    Add sellable_id and a uniqueness guard on (sellable_id, platform_id, COALESCE(edition,''))
ALTER TABLE public.video_games
  ADD COLUMN IF NOT EXISTS sellable_id bigint;

-- Unique guard only when sellable_id is present
CREATE UNIQUE INDEX IF NOT EXISTS uq_vg_sellable_platform_edition
  ON public.video_games (sellable_id, platform_id, COALESCE(edition,'') )
  WHERE sellable_id IS NOT NULL;

-- Optional: drop FK from video_games.title_id to operate FK-free
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'video_games_title_id_fkey'
  ) THEN
    ALTER TABLE public.video_games DROP CONSTRAINT video_games_title_id_fkey;
  END IF;
EXCEPTION WHEN undefined_table THEN NULL; END$$;

-- Add helper btree index for title_id lookups (FK-free but fast)
CREATE INDEX IF NOT EXISTS idx_video_games_title_id
  ON public.video_games (title_id);

-- 4) Backfill sellable_id for existing rows via title → sellables mapping
--    This is safe and idempotent.
UPDATE public.video_games vg
SET sellable_id = s.id
FROM public.sellables s
WHERE s.software_title_id = vg.title_id
  AND vg.sellable_id IS NULL;

-- 5) Performance helpers on video_games
--    BRIN on release_date (range scans), Bloom on (slug, platform_id) for sparse uniqueness/probes
CREATE INDEX IF NOT EXISTS idx_vg_release_date_brin
  ON public.video_games USING brin (release_date) WITH (pages_per_range=64);

-- Bloom index can help multi-column lookups on large tables; harmless if unused
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_extension WHERE extname='bloom') THEN
    BEGIN
      -- Cast citext slug to text for Bloom; if opclasses are unavailable, skip silently
      EXECUTE 'CREATE INDEX IF NOT EXISTS blm_vg_slug_platform ON public.video_games USING bloom ((slug::text), platform_id) WITH (col1=4, col2=4)';
    EXCEPTION WHEN undefined_object OR invalid_object_definition THEN
      NULL;
    END;
  END IF;
END$$;

-- Done
