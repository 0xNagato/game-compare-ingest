-- 0010_rls_extend.sql
-- Purpose: Extend Row Level Security to new tables and add read policies for ratings/media.
-- Idempotent: safe to re-run.

SET search_path TO gamecompare, public;

-- Enable RLS on new / missed tables
DO $do$
DECLARE
  t TEXT;
  tbls TEXT[] := ARRAY[
    'video_game_ratings_by_locale',
    'game_videos',
    'game_images',
    'provider_media_links'
  ];
BEGIN
  FOREACH t IN ARRAY tbls LOOP
    BEGIN
      EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', t);
    EXCEPTION WHEN undefined_table THEN
      RAISE NOTICE 'Skipping RLS enable for %, table does not exist (maybe earlier migration not applied)', t;
    END;
  END LOOP;
END
$do$;

-- Read policies (anon + authenticated) for ratings and media
DO $do$
DECLARE
  t TEXT;
  read_tbls TEXT[] := ARRAY[
    'video_game_ratings_by_locale',
    'game_videos',
    'game_images',
    'provider_media_links'
  ];
BEGIN
  FOREACH t IN ARRAY read_tbls LOOP
    BEGIN
      EXECUTE format('DROP POLICY IF EXISTS read_all_public ON %I', t);
  EXECUTE format('CREATE POLICY read_all_public ON %I FOR SELECT TO anon, authenticated USING (true)', t);
    EXCEPTION WHEN undefined_table THEN
      RAISE NOTICE 'Skipping policy creation for %, table missing', t;
    END;
  END LOOP;
END
$do$;

-- Optional (commented): If you later want public read on provider_media_links or prices, uncomment.
-- ALTER TABLE public.provider_media_links ENABLE ROW LEVEL SECURITY;
-- DROP POLICY IF EXISTS read_all_public ON public.provider_media_links;
-- CREATE POLICY read_all_public ON public.provider_media_links FOR SELECT TO anon, authenticated USING (true) WITH CHECK (true);
