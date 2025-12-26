-- 0484_platforms_family_backfill_and_metrics.sql
-- Purpose: Backfill platform.family for legacy rows, remove/repurpose 'generic',
--          and create analytics view aggregating counts by family.
-- Idempotent: Safe to re-run. Uses conditional UPDATE/DELETE and CREATE OR REPLACE VIEW.
-- Notes:
--  * New ingestion path now sets family + canonical_code. This migration retrofits existing data.
--  * 'generic' platform row is deleted only if unreferenced; otherwise it is tagged with family NULL.
--  * Family classification mirrors Rust classify_family(): playstation/xbox/nintendo/pc.

-- 1. Backfill family where missing, using canonical_code or code/name heuristics.
UPDATE public.platforms p SET family = CASE
  WHEN p.family IS NOT NULL THEN p.family
  WHEN p.canonical_code LIKE 'playstation%' OR p.canonical_code LIKE 'ps%' THEN 'playstation'
  WHEN p.canonical_code LIKE 'xbox%' THEN 'xbox'
  WHEN p.canonical_code LIKE 'nintendo%' OR p.canonical_code LIKE 'supernintendo%' THEN 'nintendo'
  WHEN p.canonical_code = 'pc' THEN 'pc'
  WHEN lower(p.code) LIKE 'pc%' OR lower(p.name) LIKE 'pc%' THEN 'pc'
  WHEN lower(p.code) LIKE 'windows%' OR lower(p.name) LIKE 'windows%' THEN 'pc'
  ELSE p.family
END
WHERE p.family IS NULL;

-- 2. Attempt cleanup of placeholder 'generic' if it has no referencing video_games.
DELETE FROM public.platforms pl
WHERE pl.code = 'generic'
  AND NOT EXISTS (
    SELECT 1 FROM public.video_games vg WHERE vg.platform_id = pl.id
  );

-- If 'generic' persists (referenced), leave it; optional future step could map it to a synthetic family.

-- 3. Optional: ensure canonical_code has only [a-z0-9] (defense-in-depth for older rows).
UPDATE public.platforms p SET canonical_code = lower(regexp_replace(coalesce(p.code,p.name),'[^a-z0-9]','','g'))
WHERE p.canonical_code IS NULL OR p.canonical_code <> lower(regexp_replace(coalesce(p.code,p.name),'[^a-z0-9]','','g'));

-- 4. (Re)create analytics view aggregating by family.
CREATE OR REPLACE VIEW public.platform_family_metrics AS
SELECT
  p.family,
  COUNT(*) AS platform_count,
  COUNT(DISTINCT vg.id) AS video_game_count,
  COUNT(DISTINCT vgt.id) AS title_count
FROM public.platforms p
LEFT JOIN public.video_games vg ON vg.platform_id = p.id
LEFT JOIN public.video_game_titles vgt ON vgt.id = vg.title_id
GROUP BY p.family
ORDER BY p.family;

-- 5. Helpful index (idempotent) to accelerate family-based lookups if not present.
DO $$ BEGIN
  CREATE INDEX IF NOT EXISTS platforms_family_idx ON public.platforms(family);
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- 6. Analyze affected tables for planner stats freshness.
ANALYZE public.platforms;
ANALYZE public.video_games;