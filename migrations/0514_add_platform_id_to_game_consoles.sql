-- 0514_add_platform_id_to_game_consoles.sql
-- Add platform_id column to game_consoles to support platform->hardware mapping backfill.
-- This is a minimal, focused migration that adds only what's needed for the 0510 backfill logic.

DO $$
BEGIN
  IF to_regclass('public.game_consoles') IS NULL THEN
    RAISE NOTICE 'game_consoles table missing; skipping column addition';
    RETURN;
  END IF;

  -- Add platform_id column if not present
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns 
    WHERE table_schema = 'public' 
      AND table_name = 'game_consoles' 
      AND column_name = 'platform_id'
  ) THEN
    ALTER TABLE public.game_consoles 
      ADD COLUMN platform_id bigint REFERENCES public.platforms(id) ON DELETE SET NULL;
    
    CREATE INDEX IF NOT EXISTS idx_game_consoles_platform_id 
      ON public.game_consoles(platform_id);
    
    RAISE NOTICE 'Added platform_id column to game_consoles';
  ELSE
    RAISE NOTICE 'platform_id column already exists on game_consoles';
  END IF;
END$$;
