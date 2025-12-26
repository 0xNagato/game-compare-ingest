-- 0494_media_type_preview.sql
-- Extend media_type enum to support preview videos.

DO $$ BEGIN
  ALTER TYPE media_type ADD VALUE IF NOT EXISTS 'preview';
EXCEPTION WHEN duplicate_object THEN NULL; END $$;
