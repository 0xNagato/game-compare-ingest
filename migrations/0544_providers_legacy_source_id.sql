-- Add legacy_source_id to providers to retain stable mapping from sqlite video_game_sources
DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='providers' AND column_name='legacy_source_id'
  ) THEN
    ALTER TABLE public.providers ADD COLUMN legacy_source_id bigint;
  END IF;
END $$;

-- Ensure uniqueness if the column exists
DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes WHERE schemaname='public' AND indexname='uq_providers_legacy_source_id'
  ) THEN
    CREATE UNIQUE INDEX uq_providers_legacy_source_id ON public.providers(legacy_source_id) WHERE legacy_source_id IS NOT NULL;
  END IF;
END $$;
