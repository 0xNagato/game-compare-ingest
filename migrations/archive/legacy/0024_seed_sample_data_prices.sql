-- 0024_seed_sample_data_prices.sql
-- Purpose: Seed minimal reference entities and sample price time-series across two partitions (2025-10, 2025-11).
-- Idempotent inserts using ON CONFLICT DO NOTHING.

SET search_path TO public;

-- currencies
INSERT INTO currencies (code, name, minor_unit)
VALUES ('USD','US Dollar',2)
ON CONFLICT (code) DO NOTHING;

-- countries
INSERT INTO countries (code2, name, currency_id)
SELECT 'US','United States', c.id FROM currencies c WHERE c.code = 'USD'
ON CONFLICT (code2) DO NOTHING;

-- jurisdiction (national)
INSERT INTO jurisdictions (country_id, region_code)
SELECT ct.id, NULL FROM countries ct WHERE ct.code2 = 'US'
ON CONFLICT DO NOTHING;

-- retailer
INSERT INTO retailers (name, slug)
VALUES ('Steam','steam')
ON CONFLICT (name) DO NOTHING;

-- product + software + title
WITH p AS (
  INSERT INTO products (slug, kind)
  VALUES ('half-life', 'software')
  ON CONFLICT (slug) DO NOTHING
  RETURNING id
), p2 AS (
  SELECT id FROM products WHERE slug = 'half-life'
), s AS (
  INSERT INTO software (product_id)
  SELECT id FROM p UNION SELECT id FROM p2
  ON CONFLICT (product_id) DO NOTHING
  RETURNING product_id
)
INSERT INTO video_game_titles (product_id, name, slug)
SELECT product_id, 'Half-Life', 'half-life'
FROM s
ON CONFLICT (slug) DO NOTHING;

-- platform (PC)
INSERT INTO platforms (name, slug) VALUES ('PC','pc')
ON CONFLICT (name) DO NOTHING;

-- video_game variant (title + platform)
INSERT INTO video_games (title_id, platform_id, edition)
SELECT t.id, pl.id, NULL
FROM video_game_titles t, platforms pl
WHERE t.slug = 'half-life' AND pl.slug = 'pc'
ON CONFLICT DO NOTHING;

-- sellable (software)
WITH prod AS (
  SELECT id FROM products WHERE slug = 'half-life'
)
INSERT INTO sellables (kind, product_id)
SELECT 'software', id FROM prod
ON CONFLICT DO NOTHING;

-- offer (retailer)
INSERT INTO offers (sellable_id, retailer_id, sku)
SELECT s.id, r.id, 'HL-001'
FROM sellables s, retailers r
WHERE s.product_id = (SELECT id FROM products WHERE slug = 'half-life')
  AND r.slug = 'steam'
ON CONFLICT DO NOTHING;

-- offer_jurisdiction
INSERT INTO offer_jurisdictions (offer_id, jurisdiction_id, currency_id)
SELECT o.id, j.id, c.id
FROM offers o, jurisdictions j, currencies c
WHERE o.sku = 'HL-001' AND c.code='USD'
ON CONFLICT DO NOTHING;

-- prices 2025-10
INSERT INTO prices (offer_jurisdiction_id, recorded_at, amount_minor, tax_inclusive, meta)
SELECT oj.id, ts, amt, true, jsonb_build_object('note','seed')
FROM offer_jurisdictions oj,
LATERAL (
  VALUES 
    (TIMESTAMPTZ '2025-10-05 12:00:00+00', 999),
    (TIMESTAMPTZ '2025-10-12 12:00:00+00', 899),
    (TIMESTAMPTZ '2025-10-20 12:00:00+00', 799)
) v(ts, amt)
ON CONFLICT DO NOTHING;

-- prices 2025-11
INSERT INTO prices (offer_jurisdiction_id, recorded_at, amount_minor, tax_inclusive, meta)
SELECT oj.id, ts, amt, true, jsonb_build_object('note','seed')
FROM offer_jurisdictions oj,
LATERAL (
  VALUES 
    (TIMESTAMPTZ '2025-11-02 12:00:00+00', 1099),
    (TIMESTAMPTZ '2025-11-09 12:00:00+00', 999),
    (TIMESTAMPTZ '2025-11-16 12:00:00+00', 899),
    (TIMESTAMPTZ '2025-11-23 12:00:00+00', 999)
) v(ts, amt)
ON CONFLICT DO NOTHING;

-- current_price latest snapshot
INSERT INTO current_price (offer_jurisdiction_id, amount_minor, recorded_at)
SELECT oj.id, 999, TIMESTAMPTZ '2025-11-23 12:00:00+00'
FROM offer_jurisdictions oj
ON CONFLICT (offer_jurisdiction_id) DO UPDATE
  SET amount_minor = EXCLUDED.amount_minor,
      recorded_at = EXCLUDED.recorded_at;
