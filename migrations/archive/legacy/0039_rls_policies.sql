-- 0005_rls_policies.sql
-- Purpose: Enable Row Level Security and define read-only public policies for common read tables.
-- Notes:
--   - Service role bypasses RLS; ingestion/write paths remain unaffected.
--   - Public (anon) and authenticated users can SELECT; no INSERT/UPDATE/DELETE policies defined.
--   - Adjust as needed for your app’s access model.

SET search_path TO gamecompare, public;

-- Helper DO block to enable RLS on a list of tables idempotently
DO $do$
DECLARE
  t TEXT;
  tbls TEXT[] := ARRAY[
    'currencies', 'countries', 'jurisdictions', 'tax_rules',
    'products', 'software', 'hardware', 'video_game_titles', 'video_games', 'platforms', 'game_consoles',
    'sellables', 'retailers', 'offers', 'offer_jurisdictions',
    'providers', 'retailer_providers', 'provider_items', 'provider_offers', 'provider_ingest_runs', 'provider_media_links',
    'current_price'
    -- prices excluded intentionally for heavy writes; add if you want public read
  ];
BEGIN
  FOREACH t IN ARRAY tbls LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', t);
  END LOOP;
END
$do$;

-- Read-only public policies for selected tables
DO $do$
DECLARE
  t TEXT;
  read_tbls TEXT[] := ARRAY[
    'currencies', 'countries', 'jurisdictions',
    'video_game_titles', 'video_games', 'platforms', 'game_consoles',
    'retailers', 'offers', 'offer_jurisdictions',
    'providers', 'provider_items', 'provider_offers',
    'current_price'
  ];
BEGIN
  FOREACH t IN ARRAY read_tbls LOOP
    EXECUTE format('DROP POLICY IF EXISTS read_all_public ON %I', t);
    -- WITH CHECK is not valid on SELECT policies; remove it to avoid ERROR: WITH CHECK cannot be applied to SELECT or DELETE
    EXECUTE format('CREATE POLICY read_all_public ON %I FOR SELECT TO anon, authenticated USING (true)', t);
  END LOOP;
END
$do$;

-- Optional: allow reading prices; comment out if not desired
-- ALTER TABLE public.prices ENABLE ROW LEVEL SECURITY;
-- DROP POLICY IF EXISTS read_all_public ON public.prices;
-- CREATE POLICY read_all_public ON public.prices FOR SELECT TO anon, authenticated USING (true) WITH CHECK (true);
