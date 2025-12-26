-- 0512_platform_hardware_triggers.sql
-- Trigger functions to keep platform_hardware_map in sync from game_consoles

-- Wrap in DO block to guard against missing tables when running in partial environments
DO $outer$
BEGIN
  IF to_regclass('public.game_consoles') IS NULL OR to_regclass('public.platform_hardware_map') IS NULL THEN
    RAISE NOTICE 'game_consoles or platform_hardware_map missing; skipping trigger creation';
    RETURN;
  END IF;

  -- Create function to sync mapping on insert/update/delete
  IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'tg_sync_platform_hardware_map') THEN
    CREATE OR REPLACE FUNCTION public.tg_sync_platform_hardware_map()
    RETURNS trigger LANGUAGE plpgsql AS $inner$
    BEGIN
      -- INSERT / UPDATE: ensure mapping exists for the platform -> product
      IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN
        -- Only create mapping when platform_id and product_id present
        IF NEW.platform_id IS NOT NULL AND NEW.product_id IS NOT NULL THEN
          INSERT INTO public.platform_hardware_map (platform_id, hardware_product_id, created_at)
          VALUES (NEW.platform_id, NEW.product_id, now())
          ON CONFLICT (platform_id) DO UPDATE SET hardware_product_id = EXCLUDED.hardware_product_id, created_at = EXCLUDED.created_at;
        END IF;
        RETURN NEW;
      ELSIF TG_OP = 'DELETE' THEN
        -- On delete, remove mapping when it points to the deleted product
        IF OLD.platform_id IS NOT NULL THEN
          DELETE FROM public.platform_hardware_map WHERE platform_id = OLD.platform_id;
        END IF;
        RETURN OLD;
      END IF;
      RETURN NULL;
    END;
    $inner$;
  END IF;

  -- Create trigger to call the function after changes on game_consoles
  IF NOT EXISTS (
    SELECT 1 FROM pg_trigger t JOIN pg_class c ON t.tgrelid = c.oid
    WHERE t.tgname = 'tr_game_consoles_sync_platform_hardware_map' AND c.relname = 'game_consoles'
  ) THEN
    CREATE TRIGGER tr_game_consoles_sync_platform_hardware_map
      AFTER INSERT OR UPDATE OR DELETE ON public.game_consoles
      FOR EACH ROW EXECUTE FUNCTION public.tg_sync_platform_hardware_map();
  END IF;
END$outer$;
