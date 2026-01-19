-- The hardware table + trigger chain is obsolete. Consoles now flow through the
-- canonical products → video_game_titles → video_games relationship, so this
-- migration simply tears down the legacy helpers (if they still exist) while
-- remaining idempotent for destructive resets.

DO LANGUAGE plpgsql $$
DECLARE
  has_game_consoles boolean;
  has_hardware boolean;
BEGIN
  SELECT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'public' AND table_name = 'game_consoles'
  ) INTO has_game_consoles;

  SELECT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'public' AND table_name = 'hardware'
  ) INTO has_hardware;

  -- Drop trigger first (only if the table still exists).
  IF has_game_consoles THEN
    EXECUTE 'DROP TRIGGER IF EXISTS game_consoles_hardware_guard ON public.game_consoles';
  END IF;

  -- Drop the helper function regardless of table state.
  IF EXISTS (
    SELECT 1
    FROM pg_proc
    WHERE proname = 'ensure_hardware_for_console'
      AND pg_function_is_visible(oid)
  ) THEN
    EXECUTE 'DROP FUNCTION IF EXISTS public.ensure_hardware_for_console()';
  END IF;

  -- Remove the legacy hardware table entirely (if it survived earlier cleanups).
  IF has_hardware THEN
    -- Drop with CASCADE because legacy RLS policies/views may still reference the table.
    EXECUTE 'DROP TABLE IF EXISTS public.hardware CASCADE';
    RAISE NOTICE 'Dropped legacy hardware table (and dependent helpers); consoles now rely on products → titles → games.';
  ELSE
    RAISE NOTICE 'Hardware table already absent; 0550 is a no-op.';
  END IF;
END;
$$;
