-- 0018_compat_views.sql
-- Read-only compatibility views for external shapes: sku_regions and region_prices
-- Idempotent via CREATE OR REPLACE VIEW. Public-only schema.

SET search_path TO public;

-- sku_regions compatibility view
CREATE OR REPLACE VIEW public.sku_regions_vw AS
SELECT
  oj.id::bigint                        AS sku_region_id,
  j.region_code::text                  AS region_code,
  COALESCE(r.slug::text, r.name::text) AS retailer,
  cur.code::text                       AS currency,
  o.sku::text                          AS sku,
  true                                 AS is_active,   -- offers table has no is_active; default to true
  NULL::text                           AS metadata,    -- offers table has no metadata; expose NULL
  NULL::timestamptz                    AS created_at,  -- not tracked on offer_jurisdictions/offers in base schema
  NULL::timestamptz                    AS updated_at,  -- not tracked on offer_jurisdictions/offers in base schema
  j.country_id::bigint                 AS country_id,
  cur.id::bigint                       AS currency_id
FROM public.offer_jurisdictions oj
JOIN public.offers o        ON o.id = oj.offer_id
JOIN public.retailers r     ON r.id = o.retailer_id
JOIN public.jurisdictions j ON j.id = oj.jurisdiction_id
JOIN public.currencies cur  ON cur.id = oj.currency_id;

-- region_prices compatibility view
CREATE OR REPLACE VIEW public.region_prices_vw AS
SELECT
  oj.id::bigint                                                                           AS sku_region_id,
  p.recorded_at                                                                           AS recorded_at,
  (p.amount_minor::numeric / power(10::numeric, cur.minor_unit::int))                     AS fiat_amount,
  NULL::numeric                                                                           AS btc_value,         -- not reliably derivable; expose NULL
  p.tax_inclusive                                                                         AS tax_inclusive,
  CASE WHEN p.fx_minor_per_unit IS NOT NULL THEN
    (p.fx_minor_per_unit::numeric / power(10::numeric, cur.minor_unit::int))
  ELSE NULL END                                                                            AS fx_rate_snapshot,
  CASE WHEN p.btc_sats_per_unit IS NOT NULL THEN p.btc_sats_per_unit::numeric ELSE NULL END AS btc_rate_snapshot,
  p.meta::text                                                                            AS raw_payload,
  p.recorded_at                                                                           AS created_at,
  p.recorded_at                                                                           AS updated_at,
  oj.currency_id                                                                          AS currency_id,
  j.country_id                                                                            AS country_id,
  (p.amount_minor::numeric / power(10::numeric, cur.minor_unit::int))                     AS local_amount
FROM public.prices p
JOIN public.offer_jurisdictions oj ON oj.id = p.offer_jurisdiction_id
JOIN public.jurisdictions j        ON j.id = oj.jurisdiction_id
JOIN public.currencies cur         ON cur.id = oj.currency_id;
