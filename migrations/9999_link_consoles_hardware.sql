-- Link game_consoles with hardware by ensuring each console's platform_id
-- has a corresponding hardware row. Idempotent and safe to re-run.

-- Backfill any missing hardware rows for existing consoles.
INSERT INTO public.hardware (product_id)
SELECT DISTINCT gc.product_id
FROM public.game_consoles gc
LEFT JOIN public.hardware h ON h.product_id = gc.product_id
WHERE gc.product_id IS NOT NULL
  AND h.product_id IS NULL
ON CONFLICT DO NOTHING;

-- Function to ensure a hardware row exists before inserting a console entry.
CREATE OR REPLACE FUNCTION public.ensure_hardware_for_console()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF NEW.product_id IS NULL THEN
    RETURN NEW;
  END IF;

  INSERT INTO public.hardware (product_id)
  VALUES (NEW.product_id)
  ON CONFLICT DO NOTHING;
  RETURN NEW;
END;
$$;

-- Attach trigger so future console inserts automatically seed hardware entries.
DROP TRIGGER IF EXISTS game_consoles_hardware_guard ON public.game_consoles;
CREATE TRIGGER game_consoles_hardware_guard
BEFORE INSERT ON public.game_consoles
FOR EACH ROW EXECUTE FUNCTION public.ensure_hardware_for_console();
