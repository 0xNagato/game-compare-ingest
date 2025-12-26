-- Minimal seed dataset for GameCompare (dev/demo)
SET search_path TO gamecompare, public;

-- Currencies
INSERT INTO currencies (code, name, minor_unit) VALUES
  ('USD','US Dollar',2)
ON CONFLICT (code) DO NOTHING;
INSERT INTO currencies (code, name, minor_unit) VALUES
  ('EUR','Euro',2)
ON CONFLICT (code) DO NOTHING;
INSERT INTO currencies (code, name, minor_unit) VALUES
  ('GBP','British Pound',2)
ON CONFLICT (code) DO NOTHING;

-- Countries
INSERT INTO countries (code2, name, currency_id)
SELECT 'US','United States', (SELECT id FROM currencies WHERE code='USD')
ON CONFLICT (code2) DO NOTHING;
INSERT INTO countries (code2, name, currency_id)
SELECT 'GB','United Kingdom', (SELECT id FROM currencies WHERE code='GBP')
ON CONFLICT (code2) DO NOTHING;
INSERT INTO countries (code2, name, currency_id)
SELECT 'DE','Germany', (SELECT id FROM currencies WHERE code='EUR')
ON CONFLICT (code2) DO NOTHING;

-- Jurisdictions (5 total: 3 national + 2 regional)
-- National
INSERT INTO jurisdictions (country_id, region_code)
SELECT (SELECT id FROM countries WHERE code2='US'), NULL
WHERE NOT EXISTS (SELECT 1 FROM jurisdictions WHERE country_id=(SELECT id FROM countries WHERE code2='US') AND region_code IS NULL);
INSERT INTO jurisdictions (country_id, region_code)
SELECT (SELECT id FROM countries WHERE code2='GB'), NULL
WHERE NOT EXISTS (SELECT 1 FROM jurisdictions WHERE country_id=(SELECT id FROM countries WHERE code2='GB') AND region_code IS NULL);
INSERT INTO jurisdictions (country_id, region_code)
SELECT (SELECT id FROM countries WHERE code2='DE'), NULL
WHERE NOT EXISTS (SELECT 1 FROM jurisdictions WHERE country_id=(SELECT id FROM countries WHERE code2='DE') AND region_code IS NULL);
-- Regional (US-CA, DE-BY)
INSERT INTO jurisdictions (country_id, region_code)
SELECT (SELECT id FROM countries WHERE code2='US'), 'US-CA'
WHERE NOT EXISTS (SELECT 1 FROM jurisdictions WHERE country_id=(SELECT id FROM countries WHERE code2='US') AND region_code='US-CA');
INSERT INTO jurisdictions (country_id, region_code)
SELECT (SELECT id FROM countries WHERE code2='DE'), 'DE-BY'
WHERE NOT EXISTS (SELECT 1 FROM jurisdictions WHERE country_id=(SELECT id FROM countries WHERE code2='DE') AND region_code='DE-BY');

-- Retailers
INSERT INTO retailers (name, slug) VALUES ('Steam','steam') ON CONFLICT (name) DO NOTHING;
INSERT INTO retailers (name, slug) VALUES ('PlayStation','playstation') ON CONFLICT (name) DO NOTHING;

-- Providers
INSERT INTO providers (name, kind, slug) VALUES ('steam','retailer_api','steam') ON CONFLICT (name) DO NOTHING;
INSERT INTO providers (name, kind, slug) VALUES ('gb','catalog','gb') ON CONFLICT (name) DO NOTHING;

-- Products
INSERT INTO products (slug, kind) VALUES ('half-life-3','software') ON CONFLICT (slug) DO NOTHING;
INSERT INTO software (video_game_id)
SELECT id FROM products WHERE slug='half-life-3'
ON CONFLICT DO NOTHING;

INSERT INTO products (slug, kind) VALUES ('playbox-one','hardware') ON CONFLICT (slug) DO NOTHING;
INSERT INTO hardware (platform_id)
SELECT id FROM products WHERE slug='playbox-one'
ON CONFLICT DO NOTHING;

-- Titles / Platforms / Video games
INSERT INTO video_game_titles (video_game_id, name, slug)
SELECT (SELECT id FROM products WHERE slug='half-life-3'), 'Half-Life 3', 'half-life-3'
ON CONFLICT DO NOTHING;

INSERT INTO platforms (name, slug) VALUES ('PC','pc') ON CONFLICT (name) DO NOTHING;
INSERT INTO platforms (name, slug) VALUES ('PS5','ps5') ON CONFLICT (name) DO NOTHING;

INSERT INTO video_games (title_id, platform_id, edition)
SELECT (SELECT id FROM video_game_titles WHERE slug='half-life-3'), (SELECT id FROM platforms WHERE slug='pc'), NULL
ON CONFLICT DO NOTHING;
INSERT INTO video_games (title_id, platform_id, edition)
SELECT (SELECT id FROM video_game_titles WHERE slug='half-life-3'), (SELECT id FROM platforms WHERE slug='ps5'), NULL
ON CONFLICT DO NOTHING;

-- Game console model
INSERT INTO game_consoles (platform_id, model, variant)
SELECT (SELECT id FROM products WHERE slug='playbox-one'), 'PlayBox One', NULL
ON CONFLICT DO NOTHING;

-- Sellables
INSERT INTO sellables (kind, software_title_id)
SELECT 'software', (SELECT id FROM video_game_titles WHERE slug='half-life-3')
WHERE NOT EXISTS (SELECT 1 FROM sellables WHERE software_title_id=(SELECT id FROM video_game_titles WHERE slug='half-life-3'));
INSERT INTO sellables (kind, console_id)
SELECT 'hardware', (SELECT id FROM game_consoles WHERE model='PlayBox One')
WHERE NOT EXISTS (SELECT 1 FROM sellables WHERE console_id=(SELECT id FROM game_consoles WHERE model='PlayBox One'));

-- Offers
INSERT INTO offers (sellable_id, retailer_id, sku)
SELECT (SELECT id FROM sellables WHERE software_title_id=(SELECT id FROM video_game_titles WHERE slug='half-life-3')),
       (SELECT id FROM retailers WHERE slug='steam'), NULL
WHERE NOT EXISTS (
  SELECT 1 FROM offers WHERE sellable_id=(SELECT id FROM sellables WHERE software_title_id=(SELECT id FROM video_game_titles WHERE slug='half-life-3')) AND retailer_id=(SELECT id FROM retailers WHERE slug='steam') AND sku IS NULL
);

-- Offer jurisdictions (bind to national US/EU etc.)
INSERT INTO offer_jurisdictions (offer_id, jurisdiction_id, currency_id)
SELECT (SELECT id FROM offers WHERE sellable_id=(SELECT id FROM sellables WHERE software_title_id=(SELECT id FROM video_game_titles WHERE slug='half-life-3')) AND retailer_id=(SELECT id FROM retailers WHERE slug='steam') AND sku IS NULL),
       (SELECT id FROM jurisdictions WHERE country_id=(SELECT id FROM countries WHERE code2='US') AND region_code IS NULL),
       (SELECT id FROM currencies WHERE code='USD')
ON CONFLICT DO NOTHING;

-- Provider items / mappings
INSERT INTO provider_items (provider_id, external_item_id, payload)
SELECT (SELECT id FROM providers WHERE slug='steam'), 'steam:hl3:sku', '{"seed":true}'::jsonb
ON CONFLICT DO NOTHING;
INSERT INTO provider_offers (provider_item_id, offer_id, confidence)
SELECT (SELECT id FROM provider_items WHERE provider_id=(SELECT id FROM providers WHERE slug='steam') AND external_item_id='steam:hl3:sku'),
       (SELECT id FROM offers WHERE sellable_id=(SELECT id FROM sellables WHERE software_title_id=(SELECT id FROM video_game_titles WHERE slug='half-life-3')) AND retailer_id=(SELECT id FROM retailers WHERE slug='steam') AND sku IS NULL),
       0.95
ON CONFLICT DO NOTHING;

-- Prices seed (optional small set)
DO $$
DECLARE
  oj_id bigint := (SELECT id FROM offer_jurisdictions LIMIT 1);
BEGIN
  INSERT INTO prices (offer_jurisdiction_id, provider_item_id, recorded_at, amount_minor, tax_inclusive)
  VALUES (oj_id, (SELECT id FROM provider_items LIMIT 1), now() - interval '2 days', 5999, true)
  ON CONFLICT DO NOTHING;
  INSERT INTO prices (offer_jurisdiction_id, provider_item_id, recorded_at, amount_minor, tax_inclusive)
  VALUES (oj_id, (SELECT id FROM provider_items LIMIT 1), now() - interval '1 day', 4999, true)
  ON CONFLICT DO NOTHING;
END $$;

-- current_price seed
INSERT INTO current_price (offer_jurisdiction_id, amount_minor, recorded_at)
SELECT id, 4999, now() FROM offer_jurisdictions
ON CONFLICT (offer_jurisdiction_id) DO UPDATE SET amount_minor = EXCLUDED.amount_minor, recorded_at = EXCLUDED.recorded_at;
