-- 0515_remove_gamecompare_demo_retailer.sql
-- Remove the legacy "GameCompare Demo" placeholder retailer that was used during early bootstrap.
-- This retailer is no longer used; all PlayStation Store ingestion now uses the real "PlayStation" retailer.

DO $$
DECLARE
  demo_retailer_id bigint;
  offer_count bigint;
  oj_count bigint;
BEGIN
  -- Find the demo retailer
  SELECT id INTO demo_retailer_id
  FROM public.retailers
  WHERE slug = 'gamecompare-demo';

  IF demo_retailer_id IS NULL THEN
    RAISE NOTICE 'gamecompare-demo retailer not found; nothing to remove';
    RETURN;
  END IF;

  -- Check for any offers (should be 0 based on current state)
  SELECT COUNT(*) INTO offer_count
  FROM public.offers
  WHERE retailer_id = demo_retailer_id;

  IF offer_count > 0 THEN
    -- Clean up offer_jurisdictions first
    DELETE FROM public.offer_jurisdictions
    WHERE offer_id IN (
      SELECT id FROM public.offers WHERE retailer_id = demo_retailer_id
    );
    GET DIAGNOSTICS oj_count = ROW_COUNT;

    -- Clean up offers
    DELETE FROM public.offers WHERE retailer_id = demo_retailer_id;

    RAISE NOTICE 'Removed % offers and % offer_jurisdictions for gamecompare-demo retailer', offer_count, oj_count;
  END IF;

  -- Remove the retailer itself
  DELETE FROM public.retailers WHERE id = demo_retailer_id;

  RAISE NOTICE 'Removed gamecompare-demo retailer (id: %)', demo_retailer_id;
END$$;
