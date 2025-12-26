-- 0032_seed_ps_locales.sql
-- Seed PlayStation Store locales: currencies, countries, jurisdictions, attach to existing offers, insert one recent price per offer_jurisdiction,
-- and refresh current_price. Idempotent and safe to re-run.

SET search_path TO public;

-- Locale country codes derived from PS_STORE_REGIONS (language-country): we use country component
-- Map: country_code -> (iso3, currency_code, currency_name)
-- EUR group: gb uses GBP separately; de fr es it nl fi use EUR; ca CAD; au AUD; nz NZD; us USD; br BRL; pl PLN; ru RUB; tr TRY; jp JPY; kr KRW; hk HKD; tw TWD; se SEK; dk DKK; no NOK

WITH locale_data AS (
  SELECT * FROM (VALUES
    ('us','USA','United States','USD','US Dollar'),
    ('gb','GBR','United Kingdom','GBP','Pound Sterling'),
    ('de','DEU','Germany','EUR','Euro'),
    ('fr','FRA','France','EUR','Euro'),
    ('es','ESP','Spain','EUR','Euro'),
    ('it','ITA','Italy','EUR','Euro'),
    ('nl','NLD','Netherlands','EUR','Euro'),
    ('fi','FIN','Finland','EUR','Euro'),
    ('br','BRA','Brazil','BRL','Brazilian Real'),
    ('pl','POL','Poland','PLN','Polish Zloty'),
    ('ru','RUS','Russia','RUB','Russian Ruble'),
    ('tr','TUR','Turkey','TRY','Turkish Lira'),
    ('jp','JPN','Japan','JPY','Japanese Yen'),
    ('kr','KOR','South Korea','KRW','South Korean Won'),
    ('hk','HKG','Hong Kong','HKD','Hong Kong Dollar'),
    ('tw','TWN','Taiwan','TWD','New Taiwan Dollar'),
    ('ca','CAN','Canada','CAD','Canadian Dollar'),
    ('au','AUS','Australia','AUD','Australian Dollar'),
    ('nz','NZL','New Zealand','NZD','New Zealand Dollar'),
    ('se','SWE','Sweden','SEK','Swedish Krona'),
    ('dk','DNK','Denmark','DKK','Danish Krone'),
    ('no','NOR','Norway','NOK','Norwegian Krone')
  ) AS t(iso2, iso3, country_name, cur_code, cur_name)
), ins_currencies AS (
  INSERT INTO currencies(code, name)
  SELECT cur_code, cur_name FROM locale_data
  ON CONFLICT (code) DO NOTHING
  RETURNING id, code
), cur_all AS (
  SELECT id, code FROM ins_currencies UNION ALL SELECT id, code FROM currencies WHERE code IN (SELECT cur_code FROM locale_data)
), ins_countries AS (
  INSERT INTO countries(iso2, iso3, name, currency_id)
  SELECT ld.iso2, ld.iso3, ld.country_name, c.id
  FROM locale_data ld
  JOIN cur_all c ON c.code = ld.cur_code
  ON CONFLICT (iso2) DO UPDATE SET name = EXCLUDED.name, iso3 = EXCLUDED.iso3, currency_id = EXCLUDED.currency_id
  RETURNING id, iso2
), country_all AS (
  SELECT id, iso2 FROM ins_countries UNION ALL SELECT id, iso2 FROM countries WHERE iso2 IN (SELECT iso2 FROM locale_data)
), ins_jurisdictions AS (
  INSERT INTO jurisdictions(country_id, region_code)
  SELECT ca.id, NULL FROM country_all ca
  ON CONFLICT (country_id, COALESCE(region_code,'')) DO NOTHING
  RETURNING id, country_id
), juris_all AS (
  SELECT id, country_id FROM ins_jurisdictions UNION ALL SELECT id, country_id FROM jurisdictions j WHERE j.country_id IN (SELECT id FROM country_all)
), existing_offers AS (
  SELECT id FROM offers
), offer_juris AS (
  INSERT INTO offer_jurisdictions(offer_id, jurisdiction_id, currency_id)
  SELECT o.id, j.id, c.id
  FROM existing_offers o
  CROSS JOIN juris_all j
  JOIN countries co ON co.id = j.country_id
  JOIN currencies c ON c.id = co.currency_id
  ON CONFLICT (offer_id, jurisdiction_id) DO NOTHING
  RETURNING id, offer_id, jurisdiction_id
), oj_all AS (
  SELECT id, offer_id, jurisdiction_id FROM offer_jurisdictions WHERE offer_id IN (SELECT id FROM existing_offers)
)
INSERT INTO prices(offer_jurisdiction_id, recorded_at, amount_minor, tax_inclusive)
SELECT oj.id,
       date_trunc('minute', now()) - interval '5 minutes' AS recorded_at,
       2999 + ((oj.id % 7) * 100) AS amount_minor,
       true
FROM oj_all oj
ON CONFLICT DO NOTHING;

-- Refresh current_price for all new/updated offer_jurisdictions
INSERT INTO current_price(offer_jurisdiction_id, amount_minor, recorded_at)
SELECT oj.id, p.amount_minor, p.recorded_at
FROM offer_jurisdictions oj
JOIN LATERAL (
  SELECT amount_minor, recorded_at
  FROM prices p
  WHERE p.offer_jurisdiction_id = oj.id
  ORDER BY recorded_at DESC
  LIMIT 1
) p ON TRUE
ON CONFLICT (offer_jurisdiction_id) DO UPDATE SET amount_minor = EXCLUDED.amount_minor, recorded_at = EXCLUDED.recorded_at;

-- Sanity counts (NOTICE output)
DO $$
DECLARE
  offer_count INT;
  juris_count INT;
  price_count INT;
BEGIN
  SELECT COUNT(*) INTO offer_count FROM offers;
  SELECT COUNT(*) INTO juris_count FROM offer_jurisdictions;
  SELECT COUNT(*) INTO price_count FROM prices WHERE recorded_at > now() - interval '1 day';
  RAISE NOTICE 'Offers: %, Offer Jurisdictions: %, Recent Prices (24h): %', offer_count, juris_count, price_count;
END$$;
