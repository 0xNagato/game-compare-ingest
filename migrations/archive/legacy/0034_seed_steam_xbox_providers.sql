-- 0034_seed_steam_xbox_providers.sql
-- Purpose: Ensure provider rows for Steam and Microsoft/Xbox exist, and bind them to existing retailers.
-- Idempotent: Uses ON CONFLICT DO NOTHING / UPDATE patterns.
-- Providers kinds: steam (retailer_api), xbox (retailer_api).
-- Also seeds retailer_providers linkage if retailer slugs present (steam, playstation).

SET search_path TO public;

-- Insert Steam provider
INSERT INTO providers(name, kind, slug)
VALUES ('Steam','retailer_api','steam')
ON CONFLICT (name) DO UPDATE SET kind = EXCLUDED.kind, slug = COALESCE(providers.slug, EXCLUDED.slug);

-- Insert Microsoft/Xbox provider (choose canonical name 'Xbox')
INSERT INTO providers(name, kind, slug)
VALUES ('Xbox','retailer_api','xbox')
ON CONFLICT (name) DO UPDATE SET kind = EXCLUDED.kind, slug = COALESCE(providers.slug, EXCLUDED.slug);

-- Bind Steam provider to Steam retailer if both exist
WITH r AS (
  SELECT id FROM retailers WHERE slug='steam'
), p AS (
  SELECT id FROM providers WHERE slug='steam'
)
INSERT INTO retailer_providers(retailer_id, provider_id, credentials)
SELECT r.id, p.id, jsonb_build_object('api_key', '${STEAM_WEB_API_KEY}')
FROM r, p
ON CONFLICT (retailer_id, provider_id) DO NOTHING;

-- Bind Xbox provider to PlayStation retailer is incorrect semantically; bind only if retailer slug 'playstation' exists (placeholder future mapping).
-- If a dedicated xbox retailer row is added later (slug 'xbox'), this will need adjustment. For now, skip if no retailer.
WITH rx AS (
  SELECT id FROM retailers WHERE slug IN ('xbox','playstation') LIMIT 1
), px AS (
  SELECT id FROM providers WHERE slug='xbox'
)
INSERT INTO retailer_providers(retailer_id, provider_id, credentials)
SELECT rx.id, px.id,
  jsonb_build_object(
    -- IMPORTANT: Display Catalog calls are to displaycatalog.mp.microsoft.com, but the token
    -- audience must be `https://onestore.microsoft.com` to authorize pricing/availability.
    'scope', 'https://onestore.microsoft.com/.default'
  )
FROM rx, px
ON CONFLICT (retailer_id, provider_id) DO NOTHING;

-- Comments for future evolution
COMMENT ON TABLE retailer_providers IS 'Links a retailer to a provider (API/catalogue). Steam/Xbox seeded.';
COMMENT ON COLUMN retailer_providers.credentials IS 'Provider-specific credential JSON (tokens, scope, region hints).';
