-- 0495_game_media_legacy_refs.sql
-- Add optional references from game_media rows back to legacy game_images/game_videos tables.
-- This lets us keep a 1:1 mapping for compatibility without forcing every media row
-- to materialize in both places. Columns remain nullable and are guarded by kind-aware
-- check constraints plus partial unique indexes for fast lookups.

DO $$ BEGIN
  ALTER TABLE public.game_media
    ADD COLUMN IF NOT EXISTS legacy_image_id bigint;
  ALTER TABLE public.game_media
    ADD COLUMN IF NOT EXISTS legacy_video_id bigint;
EXCEPTION WHEN undefined_table THEN
  RAISE NOTICE 'game_media table missing; run 0472_game_media.sql first.';
END $$;

-- Ensure we never set both columns at once and that the column used matches kind
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='game_media') THEN
    ALTER TABLE public.game_media
      ADD CONSTRAINT game_media_kind_legacy_chk
      CHECK (
        (legacy_image_id IS NULL OR kind = 'image') AND
        (legacy_video_id IS NULL OR kind = 'video') AND
        NOT (legacy_image_id IS NOT NULL AND legacy_video_id IS NOT NULL)
      );
  END IF;
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- Optional FK pointers back to legacy tables. Keep them DEFERRABLE so bulk loads can
-- defer validation until commit (and they only trigger when the column is set).
DO $$ BEGIN
  ALTER TABLE public.game_media
    ADD CONSTRAINT game_media_legacy_image_fk
    FOREIGN KEY (legacy_image_id)
    REFERENCES public.game_images(id)
    ON DELETE SET NULL
    DEFERRABLE INITIALLY DEFERRED;
EXCEPTION WHEN undefined_table THEN
  RAISE NOTICE 'game_images table missing; legacy image FK skipped.';
WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
  ALTER TABLE public.game_media
    ADD CONSTRAINT game_media_legacy_video_fk
    FOREIGN KEY (legacy_video_id)
    REFERENCES public.game_videos(id)
    ON DELETE SET NULL
    DEFERRABLE INITIALLY DEFERRED;
EXCEPTION WHEN undefined_table THEN
  RAISE NOTICE 'game_videos table missing; legacy video FK skipped.';
WHEN duplicate_object THEN NULL;
END $$;

-- Keep lookups + uniqueness inexpensive with partial uniques.
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='game_media') THEN
    CREATE UNIQUE INDEX IF NOT EXISTS game_media_legacy_image_uidx
      ON public.game_media(legacy_image_id)
      WHERE legacy_image_id IS NOT NULL;
  END IF;
END $$;

DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='game_media') THEN
    CREATE UNIQUE INDEX IF NOT EXISTS game_media_legacy_video_uidx
      ON public.game_media(legacy_video_id)
      WHERE legacy_video_id IS NOT NULL;
  END IF;
END $$;
