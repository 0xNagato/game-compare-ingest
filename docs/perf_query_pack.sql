-- perf_query_pack.sql
-- Baseline performance queries (use EXPLAIN (COSTS OFF, ANALYZE, BUFFERS) in staging)

-- 1) Single-month series (should prune to one partition and use composite index)
EXPLAIN (COSTS OFF)
SELECT p.offer_jurisdiction_id, p.recorded_at, p.amount_minor
FROM prices p
WHERE p.offer_jurisdiction_id = (
  SELECT oj.id FROM offer_jurisdictions oj
  JOIN offers o ON o.id = oj.offer_id
  JOIN retailers r ON r.id = o.retailer_id
  JOIN sellables s ON s.id = o.sellable_id
  JOIN video_game_titles vgt ON vgt.id = s.software_title_id
  JOIN products pr ON pr.id = vgt.video_game_id
  WHERE pr.slug='half-life' AND r.slug='steam'
  LIMIT 1)
  AND p.recorded_at >= '2025-11-01'::timestamptz
  AND p.recorded_at <  '2025-12-01'::timestamptz;

-- 2) Multi-month series (should hit only partitions in range and use per-partition index)
EXPLAIN (COSTS OFF)
SELECT offer_jurisdiction_id, recorded_at, amount_minor
FROM prices
WHERE offer_jurisdiction_id IN (
  SELECT oj.id FROM offer_jurisdictions oj
  JOIN offers o ON o.id = oj.offer_id
  JOIN sellables s ON s.id = o.sellable_id
  JOIN video_game_titles vgt ON vgt.id = s.software_title_id
  JOIN products p ON p.id = vgt.video_game_id
  WHERE p.slug IN ('portal','quake','doom')
)
AND recorded_at >= '2025-09-01'::timestamptz
AND recorded_at <  '2025-12-01'::timestamptz;

-- 3) Latest price read path (must only touch current_price)
EXPLAIN (COSTS OFF)
SELECT cp.offer_jurisdiction_id, cp.amount_minor, cp.recorded_at
FROM current_price cp
JOIN offer_jurisdictions oj ON oj.id = cp.offer_jurisdiction_id
JOIN offers o ON o.id = oj.offer_id
JOIN sellables s ON s.id = o.sellable_id
JOIN video_game_titles vgt ON vgt.id = s.software_title_id
JOIN products p ON p.id = vgt.video_game_id
WHERE p.slug IN ('portal','quake','doom');

-- 4) Alerts lookup (should use partial index alerts_active_oj_idx)
EXPLAIN (COSTS OFF)
SELECT id FROM alerts
WHERE active AND offer_jurisdiction_id = (
  SELECT oj.id FROM offer_jurisdictions oj
  JOIN offers o ON o.id = oj.offer_id
  JOIN retailers r ON r.id = o.retailer_id
  JOIN sellables s ON s.id = o.sellable_id
  JOIN video_game_titles vgt ON vgt.id = s.software_title_id
  JOIN products p ON p.id = vgt.video_game_id
  WHERE p.slug='half-life' AND r.slug='steam'
  LIMIT 1
)
LIMIT 1;

-- 5) Optional: BRIN-only scan sanity (force broad time range)
EXPLAIN (COSTS OFF)
SELECT count(*)
FROM prices
WHERE recorded_at >= '2025-01-01'::timestamptz
  AND recorded_at <  '2026-01-01'::timestamptz;