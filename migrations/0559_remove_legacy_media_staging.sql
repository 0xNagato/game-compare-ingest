-- 0506_remove_legacy_media_staging.sql
-- Drop the deprecated legacy media staging tables and helper view now that
-- media ingestion writes directly to canonical tables.
DO $$
BEGIN
  IF to_regclass('public.vw_legacy_unprocessed_media') IS NOT NULL THEN
    EXECUTE 'DROP VIEW IF EXISTS public.vw_legacy_unprocessed_media';
  END IF;
  IF to_regclass('public.legacy_image_media_raw') IS NOT NULL THEN
    EXECUTE 'DROP TABLE IF EXISTS public.legacy_image_media_raw';
  END IF;
  IF to_regclass('public.legacy_video_media_raw') IS NOT NULL THEN
    EXECUTE 'DROP TABLE IF EXISTS public.legacy_video_media_raw';
  END IF;
END $$;
