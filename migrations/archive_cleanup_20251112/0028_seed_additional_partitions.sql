-- 0028_seed_additional_partitions.sql
-- Seed additional products and prices across 2025-09..2025-12, and refresh current_price

SET search_path TO public;

-- Ensure partitions exist for the target months (idempotent)
SELECT ensure_prices_partition_for('2025-09-01'::timestamptz);
SELECT ensure_prices_partition_for('2025-10-01'::timestamptz);
SELECT ensure_prices_partition_for('2025-11-01'::timestamptz);
SELECT ensure_prices_partition_for('2025-12-01'::timestamptz);

-- Seed titles and offers (PC/Steam) if missing
WITH cur AS (
  SELECT id FROM currencies WHERE code='USD'
), base_prod AS (
  INSERT INTO products(slug, kind) VALUES
    ('portal','software'),
    ('quake','software'),
    ('doom','software')
  ON CONFLICT (slug) DO NOTHING
  RETURNING id, slug
), prod_rows AS (
  SELECT id, slug FROM base_prod UNION ALL SELECT id, slug FROM products WHERE slug IN ('portal','quake','doom')
), ensure_sw AS (
  INSERT INTO software(product_id)
  SELECT id FROM prod_rows
  ON CONFLICT DO NOTHING RETURNING product_id
), titles AS (
  INSERT INTO video_game_titles(product_id, name, slug)
  SELECT id, initcap(slug), slug FROM prod_rows
  ON CONFLICT DO NOTHING RETURNING id, slug
), plat AS (
  INSERT INTO platforms(slug, name) VALUES ('pc','PC')
  ON CONFLICT (slug) DO UPDATE SET name=EXCLUDED.name RETURNING id
), sellables_ins AS (
  INSERT INTO sellables(kind, product_id)
  SELECT 'software', id FROM prod_rows
  ON CONFLICT DO NOTHING RETURNING id, product_id
), sellables_all AS (
  SELECT id, product_id FROM sellables_ins UNION ALL SELECT id, product_id FROM sellables WHERE product_id IN (SELECT id FROM prod_rows)
), retailer AS (
  INSERT INTO retailers(slug, name) VALUES ('steam','Steam')
  ON CONFLICT (slug) DO UPDATE SET name=EXCLUDED.name RETURNING id
), offers_ins AS (
  INSERT INTO offers(sellable_id, retailer_id, sku)
  SELECT s.id, (SELECT id FROM retailer), s.product_id::text || '-sku'
  FROM sellables_all s
  ON CONFLICT DO NOTHING RETURNING id, sellable_id
), offers_all AS (
  SELECT id, sellable_id FROM offers_ins UNION ALL SELECT id, sellable_id FROM offers WHERE sellable_id IN (SELECT id FROM sellables_all)
), us_jur_final AS (
  SELECT j.id FROM jurisdictions j LIMIT 1
), ojs AS (
  INSERT INTO offer_jurisdictions(offer_id, jurisdiction_id, currency_id)
  SELECT o.id, (SELECT id FROM us_jur_final), (SELECT id FROM cur) FROM offers_all o
  ON CONFLICT DO NOTHING RETURNING id, offer_id
), final_ojs AS (
  SELECT id FROM ojs UNION ALL SELECT id FROM offer_jurisdictions WHERE offer_id IN (SELECT id FROM offers_all)
)
-- Insert prices across months (4 rows per title)
INSERT INTO prices(offer_jurisdiction_id, recorded_at, amount_minor, tax_inclusive)
SELECT fo.id,
       series.recorded_at,
       base_amount + (ROW_NUMBER() OVER (PARTITION BY fo.id ORDER BY series.recorded_at) * 50) AS amount_minor,
       true
FROM final_ojs fo
CROSS JOIN LATERAL (
  VALUES ('2025-09-10 12:00:00+00'::timestamptz),
         ('2025-10-10 12:00:00+00'::timestamptz),
         ('2025-11-10 12:00:00+00'::timestamptz),
         ('2025-12-10 12:00:00+00'::timestamptz)
) AS series(recorded_at)
CROSS JOIN LATERAL (
  SELECT 1500 AS base_amount
) base
ON CONFLICT DO NOTHING;

-- Refresh current_price to latest per offer_jurisdiction
INSERT INTO current_price(offer_jurisdiction_id, amount_minor, recorded_at)
SELECT oj.id, p.amount_minor, p.recorded_at
FROM offer_jurisdictions oj
JOIN LATERAL (
  SELECT amount_minor, recorded_at
  FROM prices p
  WHERE p.offer_jurisdiction_id = oj.id
  ORDER BY recorded_at DESC
  LIMIT 1
) p ON true
ON CONFLICT (offer_jurisdiction_id) DO UPDATE
  SET amount_minor = EXCLUDED.amount_minor,
      recorded_at  = EXCLUDED.recorded_at;
