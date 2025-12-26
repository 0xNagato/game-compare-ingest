-- 0493_dedupe_core_uniques.sql
-- Purpose: Proactively remove duplicate rows across core tables based on their business-unique keys
--           and repoint dependents to a canonical row. Safe and idempotent.
-- Notes:
-- - Uses GROUP BY keys matching enforced UNIQUE indexes to find duplicates (>1).
-- - Keeps the smallest id per group (deterministic), updates FKs in dependent tables, then deletes extras.
-- - Skips work when no duplicates are present. Designed to be re-runnable.

BEGIN;

-- A) provider_items duplicates by (provider_id, external_id)
WITH d AS (
  SELECT provider_id,
         external_id,
         MIN(id) AS keep_id,
         ARRAY_AGG(id ORDER BY id) AS all_ids,
         COUNT(*) AS cnt
  FROM public.provider_items
  GROUP BY provider_id, external_id
  HAVING COUNT(*) > 1
), upd_pof AS (
  UPDATE public.provider_offers pof
  SET provider_item_id = d.keep_id
  FROM d
  WHERE pof.provider_item_id = ANY(d.all_ids)
    AND pof.provider_item_id <> d.keep_id
  RETURNING 1
), upd_pml AS (
  UPDATE public.provider_media_links pml
  SET provider_item_id = d.keep_id
  FROM d
  WHERE pml.provider_item_id = ANY(d.all_ids)
    AND pml.provider_item_id <> d.keep_id
  RETURNING 1
), upd_prices AS (
  UPDATE public.prices p
  SET provider_item_id = d.keep_id
  FROM d
  WHERE p.provider_item_id = ANY(d.all_ids)
    AND p.provider_item_id <> d.keep_id
  RETURNING 1
)
DELETE FROM public.provider_items pi
USING d
WHERE pi.id = ANY(d.all_ids)
  AND pi.id <> d.keep_id;

-- B) offers duplicates by (sellable_id, retailer_id, COALESCE(sku,''))
WITH d AS (
  SELECT sellable_id,
         retailer_id,
         COALESCE(sku,'') AS sku_norm,
         MIN(id) AS keep_id,
         ARRAY_AGG(id ORDER BY id) AS all_ids,
         COUNT(*) AS cnt
  FROM public.offers
  GROUP BY sellable_id, retailer_id, COALESCE(sku,'')
  HAVING COUNT(*) > 1
), upd_oj AS (
  UPDATE public.offer_jurisdictions oj
  SET offer_id = d.keep_id
  FROM d
  WHERE oj.offer_id = ANY(d.all_ids)
    AND oj.offer_id <> d.keep_id
  RETURNING 1
), upd_pof AS (
  UPDATE public.provider_offers pof
  SET offer_id = d.keep_id
  FROM d
  WHERE pof.offer_id = ANY(d.all_ids)
    AND pof.offer_id <> d.keep_id
  RETURNING 1
)
DELETE FROM public.offers o
USING d
WHERE o.id = ANY(d.all_ids)
  AND o.id <> d.keep_id;

-- C) offer_jurisdictions duplicates by (offer_id, jurisdiction_id)
WITH d AS (
  SELECT offer_id,
         jurisdiction_id,
         MIN(id) AS keep_id,
         ARRAY_AGG(id ORDER BY id) AS all_ids,
         COUNT(*) AS cnt
  FROM public.offer_jurisdictions
  GROUP BY offer_id, jurisdiction_id
  HAVING COUNT(*) > 1
), upd_prices AS (
  UPDATE public.prices p
  SET offer_jurisdiction_id = d.keep_id
  FROM d
  WHERE p.offer_jurisdiction_id = ANY(d.all_ids)
    AND p.offer_jurisdiction_id <> d.keep_id
  RETURNING 1
), upd_current AS (
  UPDATE public.current_price cp
  SET offer_jurisdiction_id = d.keep_id
  FROM d
  WHERE cp.offer_jurisdiction_id = ANY(d.all_ids)
    AND cp.offer_jurisdiction_id <> d.keep_id
  RETURNING 1
)
DELETE FROM public.offer_jurisdictions oj
USING d
WHERE oj.id = ANY(d.all_ids)
  AND oj.id <> d.keep_id;

-- D) provider_offers duplicates by (provider_item_id, offer_id)
WITH d AS (
  SELECT provider_item_id,
         offer_id,
         MIN(id) AS keep_id,
         ARRAY_AGG(id ORDER BY id) AS all_ids,
         COUNT(*) AS cnt
  FROM public.provider_offers
  GROUP BY provider_item_id, offer_id
  HAVING COUNT(*) > 1
)
DELETE FROM public.provider_offers pof
USING d
WHERE pof.id = ANY(d.all_ids)
  AND pof.id <> d.keep_id;

-- E) sellables duplicates: software side (software_title_id), hardware side (console_id)
-- Software side
WITH d AS (
  SELECT software_title_id AS key_id,
         MIN(id) AS keep_id,
         ARRAY_AGG(id ORDER BY id) AS all_ids,
         COUNT(*) AS cnt
  FROM public.sellables
  WHERE software_title_id IS NOT NULL
  GROUP BY software_title_id
  HAVING COUNT(*) > 1
), upd_offers AS (
  UPDATE public.offers o
  SET sellable_id = d.keep_id
  FROM d
  WHERE o.sellable_id = ANY(d.all_ids)
    AND o.sellable_id <> d.keep_id
  RETURNING 1
), upd_vg AS (
  UPDATE public.video_games vg
  SET sellable_id = d.keep_id
  FROM d
  WHERE vg.sellable_id = ANY(d.all_ids)
    AND vg.sellable_id <> d.keep_id
  RETURNING 1
)
DELETE FROM public.sellables s
USING d
WHERE s.id = ANY(d.all_ids)
  AND s.id <> d.keep_id;

-- Hardware side
WITH d AS (
  SELECT console_id AS key_id,
         MIN(id) AS keep_id,
         ARRAY_AGG(id ORDER BY id) AS all_ids,
         COUNT(*) AS cnt
  FROM public.sellables
  WHERE console_id IS NOT NULL
  GROUP BY console_id
  HAVING COUNT(*) > 1
), upd_offers AS (
  UPDATE public.offers o
  SET sellable_id = d.keep_id
  FROM d
  WHERE o.sellable_id = ANY(d.all_ids)
    AND o.sellable_id <> d.keep_id
  RETURNING 1
)
DELETE FROM public.sellables s
USING d
WHERE s.id = ANY(d.all_ids)
  AND s.id <> d.keep_id;

COMMIT;

-- Idempotent guards (indexes likely already exist from 0469, but we reiterate safely)
DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes WHERE schemaname='public' AND indexname='uq_provider_items_provider_external'
  ) THEN
    EXECUTE 'CREATE UNIQUE INDEX uq_provider_items_provider_external ON public.provider_items (provider_id, external_id)';
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes WHERE schemaname='public' AND indexname='uq_provider_offers_item_offer'
  ) THEN
    EXECUTE 'CREATE UNIQUE INDEX uq_provider_offers_item_offer ON public.provider_offers (provider_item_id, offer_id)';
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes WHERE schemaname='public' AND indexname='uq_offers_sellable_retailer_sku'
  ) THEN
    EXECUTE 'CREATE UNIQUE INDEX uq_offers_sellable_retailer_sku ON public.offers (sellable_id, retailer_id, COALESCE(sku, ''''))';
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes WHERE schemaname='public' AND indexname='offer_jurisdictions_offer_jurisdiction_uq'
  ) THEN
    EXECUTE 'CREATE UNIQUE INDEX offer_jurisdictions_offer_jurisdiction_uq ON public.offer_jurisdictions (offer_id, jurisdiction_id)';
  END IF;
END $$;
