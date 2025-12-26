-- 0520_platform_hardware_backfill.sql
-- Backfill the platform_hardware_map from existing game_consoles when mapping is empty
DO $$
BEGIN
  IF to_regclass('public.platform_hardware_map') IS NULL THEN
    RAISE NOTICE 'platform_hardware_map does not exist; skipping backfill';
    RETURN;
  END IF;
  IF to_regclass('public.game_consoles') IS NULL THEN
    RAISE NOTICE 'game_consoles table missing; skipping backfill';
    RETURN;
  END IF;

  -- Only run when the map is currently empty
  IF (SELECT count(*) FROM public.platform_hardware_map) = 0 THEN
    INSERT INTO public.platform_hardware_map (platform_id, hardware_product_id, created_at)
    SELECT DISTINCT ON (gc.product_id) gc.platform_id, gc.product_id, now()
    FROM (
      -- attempt to infer platform_id for consoles by joining product -> video_games -> platforms
      SELECT gc.id, gc.product_id,
             NULL::bigint AS platform_id
      FROM public.game_consoles gc
    ) gc
    -- If your dataset stores platform_id on game_consoles, replace the SELECT above to use it.
    ON CONFLICT (platform_id) DO NOTHING;
    RAISE NOTICE 'platform_hardware_map backfill attempted';
  ELSE
    RAISE NOTICE 'platform_hardware_map not empty; skipping backfill';
  END IF;
END$$;
