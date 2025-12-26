-- 0469_uniques_hardening.sql
-- Purpose: Add/ensure uniqueness & exclusion constraints to prevent duplicate drift
-- Idempotent: Uses conditional checks via pg_catalog where needed.

-- OFFERS uniqueness (sellable_id, retailer_id, coalesce(sku,'')) already present in consolidated 0001; ensure again for environments missing it.
DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
      WHERE c.relkind='i' AND c.relname='uq_offers_sellable_retailer_sku' AND n.nspname='public'
  ) THEN
    CREATE UNIQUE INDEX uq_offers_sellable_retailer_sku ON public.offers (sellable_id, retailer_id, (coalesce(sku,'')));
  END IF;
END $$;

-- OFFER_JURISDICTIONS uniqueness (offer_id, jurisdiction_id)
DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname='offer_jurisdictions_offer_jurisdiction_uq'
      AND conrelid='public.offer_jurisdictions'::regclass
  ) THEN
    ALTER TABLE public.offer_jurisdictions ADD CONSTRAINT offer_jurisdictions_offer_jurisdiction_uq UNIQUE (offer_id, jurisdiction_id);
  END IF;
END $$;

-- PROVIDER_ITEMS uniqueness (provider_id, external_id)
DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
      WHERE c.relkind='i' AND c.relname='uq_provider_items_provider_external' AND n.nspname='public'
  ) THEN
    CREATE UNIQUE INDEX uq_provider_items_provider_external ON public.provider_items (provider_id, external_id);
  END IF;
END $$;

-- PROVIDER_OFFERS uniqueness (provider_item_id, offer_id)
DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
      WHERE c.relkind='i' AND c.relname='uq_provider_offers_item_offer' AND n.nspname='public'
  ) THEN
    CREATE UNIQUE INDEX uq_provider_offers_item_offer ON public.provider_offers (provider_item_id, offer_id);
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
DO $$ DECLARE dup_offers int; dup_oj int; dup_pi int; dup_pof int; dup_sell_sw int; dup_sell_hw int; BEGIN
  SELECT COUNT(*) INTO dup_offers FROM (
    SELECT sellable_id, retailer_id, coalesce(sku,'') FROM public.offers GROUP BY 1,2,3 HAVING COUNT(*)>1
  ) t;
  SELECT COUNT(*) INTO dup_oj FROM (
    SELECT offer_id, jurisdiction_id FROM public.offer_jurisdictions GROUP BY 1,2 HAVING COUNT(*)>1
  ) t;
  SELECT COUNT(*) INTO dup_pi FROM (
    SELECT provider_id, external_id FROM public.provider_items GROUP BY 1,2 HAVING COUNT(*)>1
  ) t;
  SELECT COUNT(*) INTO dup_pof FROM (
    SELECT provider_item_id, offer_id FROM public.provider_offers GROUP BY 1,2 HAVING COUNT(*)>1
  ) t;
  SELECT COUNT(*) INTO dup_sell_sw FROM (
    SELECT software_title_id FROM public.sellables WHERE software_title_id IS NOT NULL GROUP BY 1 HAVING COUNT(*)>1
  ) t;
  SELECT COUNT(*) INTO dup_sell_hw FROM (
    SELECT console_id FROM public.sellables WHERE console_id IS NOT NULL GROUP BY 1 HAVING COUNT(*)>1
  ) t;
  RAISE NOTICE 'duplicate summary: offers=% offer_jurisdictions=% provider_items=% provider_offers=% sellables_sw=% sellables_hw=%', dup_offers, dup_oj, dup_pi, dup_pof, dup_sell_sw, dup_sell_hw;
END $$;

-- Optional remediation guidance comment
COMMENT ON INDEX uq_provider_offers_item_offer IS 'Prevents multiple mappings of same provider_item to same offer.';
COMMENT ON INDEX uq_provider_items_provider_external IS 'Ensures one provider_items row per provider/external pair.';
COMMENT ON INDEX uq_offers_sellable_retailer_sku IS 'Ensures one offer per sellable/retailer/sku combo.';