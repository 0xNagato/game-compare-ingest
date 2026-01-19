-- 0480_game_images_enhancements.sql
-- Add small_url, super_url, platforms[]; drop obsolete license; add performance indexes

-- Schema changes
ALTER TABLE public.game_images
  ADD COLUMN IF NOT EXISTS small_url text,
  ADD COLUMN IF NOT EXISTS super_url text,
  ADD COLUMN IF NOT EXISTS platforms text[],
  ADD COLUMN IF NOT EXISTS video_game_id bigint;

-- Backfill video_game_id wherever we can so legacy media rows stay connected
DO $$
DECLARE
  has_vg_id boolean;
  has_vgs boolean;
BEGIN
  SELECT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='game_images' AND column_name='video_game_id'
  ) INTO has_vg_id;

  IF NOT has_vg_id THEN
    RETURN;
  END IF;

  SELECT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='game_images' AND column_name='video_game_source_id'
  ) INTO has_vgs;

  IF has_vgs THEN
    UPDATE public.game_images gi
    SET video_game_id = vgs.video_game_id
    FROM public.video_game_sources vgs
    WHERE gi.video_game_source_id = vgs.id
      AND vgs.video_game_id IS NOT NULL
      AND (gi.video_game_id IS DISTINCT FROM vgs.video_game_id);
  END IF;

END$$;

-- FK enforcement once column exists
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='game_images' AND column_name='video_game_id'
  ) AND NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname='game_images_video_game_id_fk'
  ) THEN
    ALTER TABLE public.game_images
      ADD CONSTRAINT game_images_video_game_id_fk
      FOREIGN KEY (video_game_id) REFERENCES public.video_games(id)
      ON DELETE SET NULL;
  END IF;
END$$;

-- Drop legacy license column if present (keep license_url)
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='game_images' AND column_name='license'
  ) THEN
    EXECUTE 'ALTER TABLE public.game_images DROP COLUMN license';
  END IF;
END$$;

-- Indexes
-- Create GIN index for array membership queries on platforms
CREATE INDEX IF NOT EXISTS idx_game_images_platforms_gin ON public.game_images USING gin (platforms);

-- BRIN index for append-heavy time-series scans by created_at
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = 'idx_game_images_created_brin' AND n.nspname='public') THEN
    EXECUTE 'CREATE INDEX idx_game_images_created_brin ON public.game_images USING brin (created_at)';
  END IF;
END$$;

-- Bloom extension and composite bloom index (if available)
DO $$
BEGIN
  BEGIN
    EXECUTE 'CREATE EXTENSION IF NOT EXISTS bloom';
  EXCEPTION WHEN OTHERS THEN
    -- ignore if extension not available
    NULL;
  END;
  BEGIN
    EXECUTE 'CREATE INDEX IF NOT EXISTS idx_game_images_bloom_source_kind_url ON public.game_images USING bloom (source, kind, url)';
  EXCEPTION WHEN OTHERS THEN
    -- ignore if bloom index creation fails
    NULL;
  END;
END$$;

-- Ensure idempotent uniqueness to allow ON CONFLICT DO NOTHING on (video_game_id, url)
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='game_images' AND column_name='video_game_id'
  ) THEN
    IF NOT EXISTS (
      SELECT 1 FROM pg_indexes WHERE schemaname='public' AND indexname='uq_game_images_vg_url'
    ) THEN
      EXECUTE 'CREATE UNIQUE INDEX uq_game_images_vg_url ON public.game_images (video_game_id, url) WHERE video_game_id IS NOT NULL';
    END IF;
  END IF;
END$$;
