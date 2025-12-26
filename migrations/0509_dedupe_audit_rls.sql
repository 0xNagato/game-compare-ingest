-- 0509_dedupe_audit_rls.sql
-- Purpose: ensure the dedupe audit tables participate in RLS and grant service_role access.
-- Idempotent and safe to run multiple times.

DO $$ BEGIN
  IF to_regclass('public.video_game_title_dedupe_audit') IS NOT NULL THEN
    EXECUTE 'ALTER TABLE public.video_game_title_dedupe_audit ENABLE ROW LEVEL SECURITY';
    IF NOT EXISTS (
      SELECT 1 FROM pg_policies
      WHERE schemaname = 'public'
        AND tablename = 'video_game_title_dedupe_audit'
        AND policyname = 'video_game_title_dedupe_audit_service_write'
    ) THEN
      EXECUTE 'CREATE POLICY video_game_title_dedupe_audit_service_write ON public.video_game_title_dedupe_audit FOR ALL TO service_role USING (true) WITH CHECK (true)';
    END IF;
  END IF;

  IF to_regclass('public.video_games_dedupe_audit') IS NOT NULL THEN
    EXECUTE 'ALTER TABLE public.video_games_dedupe_audit ENABLE ROW LEVEL SECURITY';
    IF NOT EXISTS (
      SELECT 1 FROM pg_policies
      WHERE schemaname = 'public'
        AND tablename = 'video_games_dedupe_audit'
        AND policyname = 'video_games_dedupe_audit_service_write'
    ) THEN
      EXECUTE 'CREATE POLICY video_games_dedupe_audit_service_write ON public.video_games_dedupe_audit FOR ALL TO service_role USING (true) WITH CHECK (true)';
    END IF;
  END IF;
END $$;
