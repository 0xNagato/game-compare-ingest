-- 0480_game_images_enhancements.sql
-- Add small_url, super_url, platforms[]; drop obsolete license; add performance indexes

-- Schema changes
ALTER TABLE public.game_images
  ADD COLUMN IF NOT EXISTS small_url text,
  ADD COLUMN IF NOT EXISTS super_url text,
  ADD COLUMN IF NOT EXISTS platforms text[];

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
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes WHERE schemaname='public' AND indexname='uq_game_images_vg_url'
  ) THEN
    EXECUTE 'CREATE UNIQUE INDEX uq_game_images_vg_url ON public.game_images (video_game_id, url)';
  END IF;
END$$;
