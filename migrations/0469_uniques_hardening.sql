-- 0469_uniques_hardening.sql
-- Purpose: Add/ensure uniqueness & exclusion constraints to prevent duplicate drift
-- Idempotent: Uses conditional checks via pg_catalog where needed.

-- video_game_source ITEMS uniqueness (video_game_source_id, external_id)
-- NOTE: This table is created by migration 0529. Skip if table doesn't exist yet.
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='video_game_source_items') THEN
    IF NOT EXISTS (
      SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE c.relkind='i' AND c.relname='uq_video_game_source_items_video_game_sourceexternal' AND n.nspname='public'
    ) THEN
      CREATE UNIQUE INDEX uq_video_game_source_items_video_game_sourceexternal ON public.video_game_source_items (video_game_source_id, external_id);
    END IF;
  END IF;
END $$;

-- video_game_sourceOFFERS uniqueness (video_game_source_item_id, offer_id)
-- NOTE: This table is created by migration 0529. Skip if table doesn't exist yet.
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='video_game_source_offers') THEN
    IF NOT EXISTS (
      SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE c.relkind='i' AND c.relname='uq_video_game_source_offers_item_offer' AND n.nspname='public'
    ) THEN
      CREATE UNIQUE INDEX uq_video_game_source_offers_item_offer ON public.video_game_source_offers (video_game_source_item_id, offer_id);
    END IF;
  END IF;
END $$;

-- SELLABLES exclusivity already enforced via CHECK; add defensive partial uniques for clarity:
DO $$ BEGIN
  -- software side uniqueness: one sellable per software_title_id
  IF NOT EXISTS (
    SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
      WHERE c.relkind='i' AND c.relname='uq_sellables_software_title' AND n.nspname='public'
  ) THEN
    CREATE UNIQUE INDEX uq_sellables_software_title ON public.sellables (software_title_id) WHERE software_title_id IS NOT NULL;
  END IF;
END $$;
DO $$ BEGIN
  -- hardware side uniqueness: one sellable per console_id
  IF NOT EXISTS (
    SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
      WHERE c.relkind='i' AND c.relname='uq_sellables_console' AND n.nspname='public'
  ) THEN
    CREATE UNIQUE INDEX uq_sellables_console ON public.sellables (console_id) WHERE console_id IS NOT NULL;
  END IF;
END $$;

-- Report existing duplicate counts prior to constraint creation (non-blocking informational). Use NOTICE.
-- NOTE: video_game_source_items tables created in migration 0529. Skip duplicate checks if tables don't exist yet.
DO $$ DECLARE dup_offers int; dup_oj int; dup_pi int; dup_pof int; dup_sell_sw int; dup_sell_hw int; BEGIN
  SELECT COUNT(*) INTO dup_offers FROM (
    SELECT sellable_id, retailer_id, coalesce(sku,'') FROM public.offers GROUP BY 1,2,3 HAVING COUNT(*)>1
  ) t;
  SELECT COUNT(*) INTO dup_oj FROM (
    SELECT offer_id, jurisdiction_id FROM public.offer_jurisdictions GROUP BY 1,2 HAVING COUNT(*)>1
  ) t;

  -- Only check video_game_source tables if they exist
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='video_game_source_items') THEN
    SELECT COUNT(*) INTO dup_pi FROM (
      SELECT video_game_source_id, external_id FROM public.video_game_source_items GROUP BY 1,2 HAVING COUNT(*)>1
    ) t;
  ELSE
    dup_pi := 0;
  END IF;

  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='video_game_source_offers') THEN
    SELECT COUNT(*) INTO dup_pof FROM (
      SELECT video_game_source_item_id, offer_id FROM public.video_game_source_offers GROUP BY 1,2 HAVING COUNT(*)>1
    ) t;
  ELSE
    dup_pof := 0;
  END IF;

  SELECT COUNT(*) INTO dup_sell_sw FROM (
    SELECT software_title_id FROM public.sellables WHERE software_title_id IS NOT NULL GROUP BY 1 HAVING COUNT(*)>1
  ) t;
  SELECT COUNT(*) INTO dup_sell_hw FROM (
    SELECT console_id FROM public.sellables WHERE console_id IS NOT NULL GROUP BY 1 HAVING COUNT(*)>1
  ) t;
  RAISE NOTICE 'duplicate summary: video_game_source_items=% video_game_source_offers=% sellables_sw=% sellables_hw=%', dup_pi, dup_pof, dup_sell_sw, dup_sell_hw;
END $$;

-- Optional remediation guidance comments (only if indexes exist)
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_class WHERE relname='uq_video_game_source_offers_item_offer') THEN
    COMMENT ON INDEX uq_video_game_source_offers_item_offer IS 'Prevents multiple mappings of same video_game_source_item to same offer.';
  END IF;
  IF EXISTS (SELECT 1 FROM pg_class WHERE relname='uq_video_game_source_items_video_game_sourceexternal') THEN
    COMMENT ON INDEX uq_video_game_source_items_video_game_sourceexternal IS 'Ensures one video_game_source_items row per provider/external pair.';
  END IF;
END $$;