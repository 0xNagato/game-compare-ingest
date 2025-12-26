-- docs/verification/integrity_checks.sql
-- Purpose: Reproducible integrity checks and helper objects for media and pricing data.
-- Date: 2025-11-11

-- 1) Helper view: orphan_provider_media_links_vw (idempotent via OR REPLACE)
CREATE OR REPLACE VIEW public.orphan_provider_media_links_vw AS
SELECT pml.*
FROM public.provider_media_links pml
LEFT JOIN public.provider_items pi ON pml.provider_item_id = pi.id
LEFT JOIN public.video_games vg ON pml.video_game_id = vg.id
WHERE (pml.provider_item_id IS NOT NULL AND pi.id IS NULL)
   OR (pml.video_game_id IS NOT NULL AND vg.id IS NULL);

COMMENT ON VIEW public.orphan_provider_media_links_vw IS 'Rows in provider_media_links referencing missing provider_items or video_games (should be 0 if FKs are enforced).';

-- 2) Helper function: media_integrity_snapshot() → jsonb (idempotent)
CREATE OR REPLACE FUNCTION public.media_integrity_snapshot()
RETURNS jsonb
LANGUAGE sql
AS $$
  WITH dups AS (
    SELECT 1
    FROM public.provider_media_links
    GROUP BY provider_item_id, url
    HAVING COUNT(*) > 1
  ),
  orphans AS (
    SELECT 1
    FROM public.orphan_provider_media_links_vw
  )
  SELECT jsonb_build_object(
    'provider_media_links_total', (SELECT COUNT(*) FROM public.provider_media_links),
    'distinct_media_urls', (SELECT COUNT(DISTINCT url) FROM public.provider_media_links),
    'duplicates_by_item_url', (SELECT COUNT(*) FROM dups),
    'orphan_provider_media_links', (SELECT COUNT(*) FROM orphans)
  );
$$;

COMMENT ON FUNCTION public.media_integrity_snapshot() IS 'Returns counts for media link integrity: totals, distinct URLs, duplicate (provider_item_id,url) pairs, orphan rows.';

-- 3) Quick invariants (read-only). Expect all counts = 0.
--    Run these SELECTs to verify integrity after migrations/ingests.

-- 3.1 Media integrity snapshot
SELECT public.media_integrity_snapshot();

-- 3.2 Duplicates by (provider_item_id, url) (should be 0 groups)
SELECT COUNT(*) AS duplicate_groups
FROM (
  SELECT 1
  FROM public.provider_media_links
  GROUP BY provider_item_id, url
  HAVING COUNT(*) > 1
) AS g;

-- 3.3 Orphan links (should be 0 rows)
SELECT COUNT(*) AS orphan_links
FROM public.orphan_provider_media_links_vw;

-- 3.4 Offer jurisdiction uniqueness (should be 0 groups)
SELECT COUNT(*) AS dup_offer_jurisdictions
FROM (
  SELECT 1
  FROM public.offer_jurisdictions
  GROUP BY offer_id, jurisdiction_id
  HAVING COUNT(*) > 1
) AS g;

-- 3.5 Current price rows with missing OJ (should be 0)
SELECT COUNT(*) AS current_price_orphans
FROM public.current_price cp
LEFT JOIN public.offer_jurisdictions oj ON oj.id = cp.offer_jurisdiction_id
WHERE oj.id IS NULL;

-- 3.6 Prices rows with missing OJ (should be 0)
--     Note: This can be slow if table is very large.
SELECT COUNT(*) AS prices_orphans
FROM public.prices p
LEFT JOIN public.offer_jurisdictions oj ON oj.id = p.offer_jurisdiction_id
WHERE oj.id IS NULL;

-- 4) Partition helper smoke test (no-op if function already provisions)
-- Ensure current and next month partitions exist; harmless if already present.
SELECT public.ensure_prices_partition_for(date_trunc('month', now()));
SELECT public.ensure_prices_partition_for(date_trunc('month', now()) + interval '1 month');
