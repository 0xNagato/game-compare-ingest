
-- 0529_enforce_products_titles_video_games.sql
--
-- Portfolio-grade schema guarantees + documentation.
--
-- Non-negotiable invariant (must remain true):
--   products → video_game_titles → video_games
--   - ONLY video_game_titles has product_id
--   - video_games MUST NOT reference products directly
--
-- This migration is idempotent and safe to re-run.

-- ============================================================
-- 1) Ensure orphan-safe FK: video_games.title_id → video_game_titles.id
-- ============================================================

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'public' AND table_name = 'video_games'
  ) OR NOT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'public' AND table_name = 'video_game_titles'
  ) THEN
    RETURN;
  END IF;

  -- If we have orphaned video_games rows (title_id points to a missing title),
  -- create placeholder titles using the missing id values so we can enforce
  -- the FK without dropping data.
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='video_games' AND column_name='display_title'
  ) THEN
    EXECUTE $SQL$
      INSERT INTO public.video_game_titles (id, title, normalized_title, created_at, updated_at)
      SELECT DISTINCT
        vg.title_id,
        COALESCE(NULLIF(trim(vg.display_title), ''), 'Unknown Title') AS title,
        NULL,
        now(),
        now()
      FROM public.video_games vg
      WHERE vg.title_id IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM public.video_game_titles vgt WHERE vgt.id = vg.title_id)
    $SQL$;
  ELSE
    EXECUTE $SQL$
      INSERT INTO public.video_game_titles (id, title, normalized_title, created_at, updated_at)
      SELECT DISTINCT
        vg.title_id,
        'Unknown Title' AS title,
        NULL,
        now(),
        now()
      FROM public.video_games vg
      WHERE vg.title_id IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM public.video_game_titles vgt WHERE vgt.id = vg.title_id)
    $SQL$;
  END IF;

  -- Align serial sequence to max(id) after explicit id inserts.
  PERFORM
    CASE
      WHEN pg_get_serial_sequence('public.video_game_titles','id') IS NULL THEN NULL
      ELSE setval(
        pg_get_serial_sequence('public.video_game_titles','id'),
        GREATEST((SELECT COALESCE(MAX(id), 0) FROM public.video_game_titles), 1)
      )
    END;
END$$;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='video_games' AND column_name='title_id'
  ) AND EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema='public' AND table_name='video_game_titles'
  ) THEN
    IF NOT EXISTS (
      SELECT 1 FROM pg_constraint
      WHERE conname = 'video_games_title_id_fk'
        AND conrelid = 'public.video_games'::regclass
    ) THEN
      -- NOT VALID allows the constraint to be created even if some legacy rows are broken.
      -- It still enforces correctness for ALL new writes immediately.
      ALTER TABLE public.video_games
        ADD CONSTRAINT video_games_title_id_fk
        FOREIGN KEY (title_id) REFERENCES public.video_game_titles(id)
        ON DELETE CASCADE
        NOT VALID;

      -- Try to validate immediately; if it fails, keep NOT VALID so new writes are still enforced.
      BEGIN
        ALTER TABLE public.video_games VALIDATE CONSTRAINT video_games_title_id_fk;
      EXCEPTION WHEN OTHERS THEN
        -- Leave constraint NOT VALID; a follow-up data cleanup can validate it later.
        NULL;
      END;
    END IF;
  END IF;
END$$;

-- ============================================================
-- 2) Explicit schema documentation (unambiguous for developers)
-- ============================================================

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='video_game_titles' AND column_name='product_id'
  ) THEN
    COMMENT ON COLUMN public.video_game_titles.product_id IS
      'IMPORTANT: ONLY video_game_titles has product_id. This is the ONLY allowed link from products → video_games. Traversal is products → video_game_titles → video_games.';
  END IF;

  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='video_games' AND column_name='title_id'
  ) THEN
    COMMENT ON COLUMN public.video_games.title_id IS
      'FK to video_game_titles. Do not add product_id to video_games. Products relate to video_games ONLY via video_game_titles.';
  END IF;

  IF EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema='public' AND table_name='video_games'
  ) THEN
    COMMENT ON TABLE public.video_games IS
      'Per-platform (and optionally per-edition) video game rows. Must reference video_game_titles via title_id. MUST NOT reference products directly.';
  END IF;
END $$;

-- ============================================================
-- 3) video_game_sources: maintain unique child video_game IDs
-- ============================================================

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema='public' AND table_name='video_game_sources'
  ) THEN
    -- A source (provider + external id) can represent a title that maps to multiple
    -- internal video_games (e.g., PS5 + PS4 editions).
    ALTER TABLE public.video_game_sources
      ADD COLUMN IF NOT EXISTS video_game_ids jsonb NOT NULL DEFAULT '[]'::jsonb;

    IF NOT EXISTS (
      SELECT 1 FROM pg_constraint
      WHERE conname='video_game_sources_video_game_ids_is_array'
        AND conrelid='public.video_game_sources'::regclass
    ) THEN
      ALTER TABLE public.video_game_sources
        ADD CONSTRAINT video_game_sources_video_game_ids_is_array
        CHECK (jsonb_typeof(video_game_ids) = 'array');
    END IF;

    -- Backfill from legacy single video_game_id when present.
    UPDATE public.video_game_sources
      SET video_game_ids = jsonb_build_array(video_game_id)
      WHERE video_game_id IS NOT NULL
        AND (video_game_ids IS NULL OR jsonb_array_length(video_game_ids) = 0);

    -- Keep legacy video_game_id and new video_game_ids synced + de-duped.
    CREATE OR REPLACE FUNCTION public.video_game_sources_sync_video_game_ids()
    RETURNS trigger
    LANGUAGE plpgsql
    AS $func$
    DECLARE
      merged jsonb;
    BEGIN
      IF NEW.video_game_ids IS NULL OR jsonb_typeof(NEW.video_game_ids) <> 'array' THEN
        NEW.video_game_ids := '[]'::jsonb;
      END IF;

      -- If the legacy single FK is set, ensure it's included in the array.
      IF NEW.video_game_id IS NOT NULL THEN
        SELECT COALESCE(
          jsonb_agg(DISTINCT v ORDER BY v),
          '[]'::jsonb
        )
        INTO merged
        FROM (
          SELECT NEW.video_game_id::bigint AS v
          UNION ALL
          SELECT (e.value)::bigint AS v
          FROM jsonb_array_elements_text(NEW.video_game_ids) e
          WHERE e.value ~ '^[0-9]+$'
        ) s;

        NEW.video_game_ids := merged;
      END IF;

      -- If the legacy FK is NULL but the array has values, pick the first.
      IF NEW.video_game_id IS NULL AND jsonb_array_length(NEW.video_game_ids) > 0 THEN
        BEGIN
          NEW.video_game_id := (NEW.video_game_ids ->> 0)::bigint;
        EXCEPTION WHEN OTHERS THEN
          NEW.video_game_id := NULL;
        END;
      END IF;

      RETURN NEW;
    END;
    $func$;

    IF NOT EXISTS (
      SELECT 1 FROM pg_trigger
      WHERE tgname = 'video_game_sources_sync_video_game_ids_trg'
    ) THEN
      CREATE TRIGGER video_game_sources_sync_video_game_ids_trg
      BEFORE INSERT OR UPDATE ON public.video_game_sources
      FOR EACH ROW
      EXECUTE FUNCTION public.video_game_sources_sync_video_game_ids();
    END IF;
  END IF;
END $$;
