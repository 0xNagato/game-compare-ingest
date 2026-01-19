-- 0510_platforms_hardware_one_to_one.sql
-- Purpose: Enforce a one-to-one relationship between `platforms` and `hardware` via
-- a small mapping table. This is non-destructive and idempotent: it creates
-- `platform_hardware_map`, backfills from existing `game_consoles` rows using a
-- deterministic rule, and avoids failing if duplicates exist by picking a single
-- canonical mapping per hardware product.

BEGIN;

-- Guard: only proceed if both source tables exist. Some environments may not
-- have `platforms` or `hardware` yet (older migrations not applied); in that
-- case we skip cleanly.
DO $$
DECLARE
  cnt_total int;
  cnt_conflicts int;
BEGIN
  IF to_regclass('public.platforms') IS NULL OR to_regclass('public.hardware') IS NULL THEN
    RAISE NOTICE 'platforms or hardware table missing; skipping platform_hardware_map creation/backfill.';
    RETURN;
  END IF;

  -- Create the mapping table if not present. Use EXECUTE so the DO block
  -- remains idempotent across Postgres versions.
  IF to_regclass('public.platform_hardware_map') IS NULL THEN
    EXECUTE $create$
      CREATE TABLE public.platform_hardware_map (
        platform_id bigint PRIMARY KEY REFERENCES public.platforms(id) ON DELETE CASCADE,
        hardware_product_id bigint UNIQUE REFERENCES public.hardware(product_id) ON DELETE CASCADE,
        created_at timestamptz NOT NULL DEFAULT now()
      );
    $create$;
  END IF;

  -- Backfill mapping from game_consoles. Choose a deterministic single mapping
  -- per hardware product when multiple candidate platforms exist.
  WITH pairs AS (
    SELECT DISTINCT gc.platform_id, gc.product_id
    FROM public.game_consoles gc
    WHERE gc.platform_id IS NOT NULL AND gc.product_id IS NOT NULL
  ), chosen AS (
    SELECT platform_id, product_id AS hardware_product_id,
           ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY platform_id) AS rn
    FROM pairs
  )
  INSERT INTO public.platform_hardware_map (platform_id, hardware_product_id)
  SELECT platform_id, hardware_product_id FROM chosen WHERE rn = 1
  ON CONFLICT (platform_id) DO NOTHING;

  -- Validation / notice
  SELECT COUNT(*) INTO cnt_total FROM public.platform_hardware_map;
  SELECT COUNT(*) INTO cnt_conflicts FROM (
    SELECT hardware_product_id, COUNT(*) AS c FROM public.platform_hardware_map GROUP BY hardware_product_id HAVING COUNT(*) > 1
  ) t;
  RAISE NOTICE 'platform_hardware_map backfilled: total=% conflicts=%', cnt_total, cnt_conflicts;
END$$;

COMMIT;
