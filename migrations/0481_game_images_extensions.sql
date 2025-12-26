-- 0481_game_images_extensions.sql
-- Add small_url, super_url, platforms text[]; drop obsolete license column
-- Idempotent and safe to re-run

ALTER TABLE IF EXISTS public.game_images
  ADD COLUMN IF NOT EXISTS small_url text,
  ADD COLUMN IF NOT EXISTS super_url text;

-- Drop legacy license column if present
DO $$ BEGIN
  ALTER TABLE public.game_images DROP COLUMN IF EXISTS license;
EXCEPTION WHEN undefined_column THEN
  -- ignore
  NULL;
END $$;

-- Add platforms (array of text)
ALTER TABLE IF EXISTS public.game_images
  ADD COLUMN IF NOT EXISTS platforms text[];

-- Optional: GIN index for platforms array membership queries
CREATE INDEX IF NOT EXISTS idx_game_images_platforms_gin ON public.game_images USING gin (platforms);
