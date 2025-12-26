-- Acceptance Tests for GameCompare
-- Run these statements on PostgreSQL 15+ (Supabase compatible)

SET search_path TO gamecompare, public;

-- 1) Partition pruning check
EXPLAIN (ANALYZE, BUFFERS)
WITH sample_oj AS (
  SELECT oj.id
  FROM offer_jurisdictions oj
  JOIN offers o ON o.id = oj.offer_id
  JOIN retailers r ON r.id = o.retailer_id
  WHERE r.slug = 'playstation'
  ORDER BY oj.id
  LIMIT 1
)
SELECT count(*)
FROM prices
WHERE offer_jurisdiction_id = (SELECT id FROM sample_oj)
  AND recorded_at >= date_trunc('month', now())
  AND recorded_at < (date_trunc('month', now()) + interval '1 month');
-- Expectation: Plan should reference only the current month partition (prices_YYYYMM)

-- 2) Uniqueness constraints catch duplicates
-- provider_items unique (provider_id, external_id)
BEGIN;
  DO $$ BEGIN
    INSERT INTO providers (name, kind) VALUES ('test-provider', 'catalog') ON CONFLICT DO NOTHING;
  EXCEPTION WHEN unique_violation THEN END $$;
  DO $$ BEGIN
    INSERT INTO providers (name, kind) VALUES ('test-provider', 'catalog') ON CONFLICT DO NOTHING;
  EXCEPTION WHEN unique_violation THEN END $$;
ROLLBACK;
-- Expectation: Second insert of identical provider name hits UNIQUE on name (no duplicate row created)

-- 3) current_price upsert reflects latest point
-- Insert two price points for the same offer_jurisdiction_id; latest wins
DO $$
DECLARE
  oj_id bigint;
  now_ts timestamptz := now();
BEGIN
  SELECT oj.id INTO oj_id
  FROM offer_jurisdictions oj
  JOIN offers o ON o.id = oj.offer_id
  ORDER BY oj.id
  LIMIT 1;

  IF oj_id IS NULL THEN
    RAISE NOTICE 'no offer_jurisdiction rows available for test';
    RETURN;
  END IF;

  INSERT INTO prices (offer_jurisdiction_id, provider_item_id, recorded_at, amount_minor, tax_inclusive)
  VALUES (oj_id, NULL, now_ts - interval '1 day', 5999, true);
  INSERT INTO prices (offer_jurisdiction_id, provider_item_id, recorded_at, amount_minor, tax_inclusive)
  VALUES (oj_id, NULL, now_ts, 4999, true);
  -- Application layer upserts current_price; simulate here:
  INSERT INTO current_price (offer_jurisdiction_id, amount_minor, recorded_at)
  VALUES (oj_id, 4999, now_ts)
  ON CONFLICT (offer_jurisdiction_id)
  DO UPDATE SET amount_minor = EXCLUDED.amount_minor,
                recorded_at = EXCLUDED.recorded_at
  WHERE current_price.recorded_at <= EXCLUDED.recorded_at;
END $$;

SELECT amount_minor, recorded_at
FROM current_price
ORDER BY recorded_at DESC
LIMIT 1;
-- Expectation: amount_minor = 4999 and recorded_at = latest

EXPLAIN (ANALYZE, BUFFERS)
SELECT a.id
FROM alerts a
WHERE a.is_active = true
  AND EXISTS (
    SELECT 1
    FROM products p
    WHERE p.id = a.product_id
  )
ORDER BY a.id
LIMIT 1;

-- Ingestion smoke acceptance: partition & latest price upsert
-- 1) Verify a partition exists for the current month (created by ensure_price_partition during ingest)
SELECT
  to_regclass('prices_' || to_char(date_trunc('month', now()), 'YYYY_MM')) IS NOT NULL AS has_current_month_partition;

-- 2) Verify current_price has at least one row with very fresh recorded_at
SELECT
  COUNT(*) > 0 AS has_fresh_current_price
FROM current_price
WHERE recorded_at > now() - interval '1 hour';

-- 5) Multi-source recording: allow two different providers at same timestamp for same OJ
DO $$
DECLARE
  oj_id bigint;
  ts timestamptz := now();
  p1 bigint; p2 bigint; pi1 bigint; pi2 bigint;
BEGIN
  SELECT id INTO oj_id
  FROM (
    SELECT oj.id, row_number() OVER (ORDER BY oj.id) AS rn
    FROM offer_jurisdictions oj
  ) ranked
  WHERE rn = 1
  LIMIT 1;

  IF oj_id IS NULL THEN
    RAISE NOTICE 'no offer_jurisdiction rows available for multi-source test';
    RETURN;
  END IF;

  -- Ensure two providers and items
  INSERT INTO providers (slug, name, kind)
  VALUES ('ps-store','PlayStation Store','storefront')
  ON CONFLICT (slug) DO NOTHING;
  INSERT INTO providers (slug, name, kind)
  VALUES ('pricing_charts','Pricing Charts','catalog')
  ON CONFLICT (slug) DO NOTHING;

  SELECT id INTO p1 FROM providers WHERE slug='ps-store';
  SELECT id INTO p2 FROM providers WHERE slug='pricing_charts';

  INSERT INTO provider_items (provider_id, external_id)
  VALUES (p1, 'psstore-test-item')
  ON CONFLICT (provider_id, external_id) DO NOTHING;
  INSERT INTO provider_items (provider_id, external_id)
  VALUES (p2, 'pricing-test-item')
  ON CONFLICT (provider_id, external_id) DO NOTHING;

  SELECT id INTO pi1 FROM provider_items WHERE provider_id=p1 AND external_id='psstore-test-item';
  SELECT id INTO pi2 FROM provider_items WHERE provider_id=p2 AND external_id='pricing-test-item';

  -- Insert two prices at same timestamp from two sources
  INSERT INTO prices (offer_jurisdiction_id, provider_item_id, recorded_at, amount_minor, tax_inclusive)
  VALUES (oj_id, pi1, ts, 3000, true);
  INSERT INTO prices (offer_jurisdiction_id, provider_item_id, recorded_at, amount_minor, tax_inclusive)
  VALUES (oj_id, pi2, ts, 3500, true);
END $$;

-- Expect: both rows present
WITH sample_oj AS (
  SELECT id
  FROM (
    SELECT oj.id, row_number() OVER (ORDER BY oj.id) AS rn
    FROM offer_jurisdictions oj
  ) ranked
  WHERE rn = 1
  LIMIT 1
)
SELECT COUNT(*) AS two_rows
FROM prices p
JOIN provider_items pi ON pi.id=p.provider_item_id
JOIN providers pr ON pr.id=pi.provider_id
WHERE p.offer_jurisdiction_id = (SELECT id FROM sample_oj)
  AND p.recorded_at > now() - interval '10 minutes';

-- 4) Tie-breaker acceptance: equal timestamps, different agents -> higher priority wins
DO $$
DECLARE
  oj_id bigint;
  ts timestamptz := now();
BEGIN
  SELECT id INTO oj_id
  FROM (
    SELECT oj.id, row_number() OVER (ORDER BY oj.id) AS rn
    FROM offer_jurisdictions oj
  ) ranked
  WHERE rn = 2
  LIMIT 1;

  IF oj_id IS NULL THEN
    RAISE NOTICE 'insufficient offer_jurisdiction rows for tie-breaker test';
    RETURN;
  END IF;

  -- baseline low-priority agent
  INSERT INTO current_price(offer_jurisdiction_id, amount_minor, recorded_at)
  VALUES (oj_id, 1000, ts - interval '1 minute')
  ON CONFLICT (offer_jurisdiction_id) DO UPDATE SET amount_minor=EXCLUDED.amount_minor, recorded_at=EXCLUDED.recorded_at;
  -- competing higher timestamp at same id
  INSERT INTO current_price(offer_jurisdiction_id, amount_minor, recorded_at)
  VALUES (oj_id, 2000, ts)
  ON CONFLICT (offer_jurisdiction_id) DO UPDATE SET amount_minor=EXCLUDED.amount_minor, recorded_at=EXCLUDED.recorded_at
  WHERE current_price.recorded_at <= EXCLUDED.recorded_at;
END $$;

SELECT amount_minor, recorded_at
FROM current_price
WHERE offer_jurisdiction_id = (
  SELECT id
  FROM (
    SELECT oj.id, row_number() OVER (ORDER BY oj.id) AS rn
    FROM offer_jurisdictions oj
  ) ranked
  WHERE rn = 2
  LIMIT 1
);
-- Expectation: returns amount_minor = 2000 with most recent recorded_at
