-- 0478_rls_sweep.sql (simplified)
-- Purpose: Enable RLS and establish baseline policies without creating helper functions.
-- Strategy: Single DO block to avoid statement splitter issues.
-- Idempotent: Uses DROP POLICY IF EXISTS before CREATE POLICY.
-- Notes: Skips partitioned prices table intentionally.
-- Explicit RLS enabling and policies per table (no DO block, idempotent via DROP POLICY IF EXISTS)
-- Predicate reused per line
-- Trusted predicate
-- NOTE: Using ALTER TABLE IF EXISTS to avoid errors if table missing
-- Catalog / lookup tables
ALTER TABLE IF EXISTS public.currencies ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS trusted_app_all ON public.currencies;

CREATE POLICY trusted_app_all ON public.currencies FOR ALL TO PUBLIC USING (
  current_user = 'postgres'
  OR current_user LIKE 'postgres.%'
);

DROP POLICY IF EXISTS trusted_app_insert_check ON public.currencies;

CREATE POLICY trusted_app_insert_check ON public.currencies FOR INSERT TO PUBLIC
WITH
  CHECK (
    current_user = 'postgres'
    OR current_user LIKE 'postgres.%'
  );

DROP POLICY IF EXISTS trusted_app_update_check ON public.currencies;

CREATE POLICY trusted_app_update_check ON public.currencies FOR
UPDATE TO PUBLIC
WITH
  CHECK (
    current_user = 'postgres'
    OR current_user LIKE 'postgres.%'
  );

DROP POLICY IF EXISTS public_read_select ON public.currencies;

CREATE POLICY public_read_select ON public.currencies FOR
SELECT
  TO PUBLIC USING (true);

DROP POLICY IF EXISTS public_no_insert ON public.currencies;

CREATE POLICY public_no_insert ON public.currencies FOR INSERT TO PUBLIC
WITH
  CHECK (false);

DROP POLICY IF EXISTS public_no_update ON public.currencies;

CREATE POLICY public_no_update ON public.currencies FOR
UPDATE TO PUBLIC USING (false)
WITH
  CHECK (false);

DROP POLICY IF EXISTS public_no_delete ON public.currencies;

CREATE POLICY public_no_delete ON public.currencies FOR DELETE TO PUBLIC USING (false);

ALTER TABLE IF EXISTS public.countries ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS trusted_app_all ON public.countries;

CREATE POLICY trusted_app_all ON public.countries FOR ALL TO PUBLIC USING (
  current_user = 'postgres'
  OR current_user LIKE 'postgres.%'
);

DROP POLICY IF EXISTS trusted_app_insert_check ON public.countries;

CREATE POLICY trusted_app_insert_check ON public.countries FOR INSERT TO PUBLIC
WITH
  CHECK (
    current_user = 'postgres'
    OR current_user LIKE 'postgres.%'
  );

DROP POLICY IF EXISTS trusted_app_update_check ON public.countries;

CREATE POLICY trusted_app_update_check ON public.countries FOR
UPDATE TO PUBLIC
WITH
  CHECK (
    current_user = 'postgres'
    OR current_user LIKE 'postgres.%'
  );

DROP POLICY IF EXISTS public_read_select ON public.countries;

CREATE POLICY public_read_select ON public.countries FOR
SELECT
  TO PUBLIC USING (true);

DROP POLICY IF EXISTS public_no_insert ON public.countries;

CREATE POLICY public_no_insert ON public.countries FOR INSERT TO PUBLIC
WITH
  CHECK (false);

DROP POLICY IF EXISTS public_no_update ON public.countries;

CREATE POLICY public_no_update ON public.countries FOR
UPDATE TO PUBLIC USING (false)
WITH
  CHECK (false);

DROP POLICY IF EXISTS public_no_delete ON public.countries;

CREATE POLICY public_no_delete ON public.countries FOR DELETE TO PUBLIC USING (false);

ALTER TABLE IF EXISTS public.jurisdictions ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS trusted_app_all ON public.jurisdictions;

CREATE POLICY trusted_app_all ON public.jurisdictions FOR ALL TO PUBLIC USING (
  current_user = 'postgres'
  OR current_user LIKE 'postgres.%'
);

DROP POLICY IF EXISTS trusted_app_insert_check ON public.jurisdictions;

CREATE POLICY trusted_app_insert_check ON public.jurisdictions FOR INSERT TO PUBLIC
WITH
  CHECK (
    current_user = 'postgres'
    OR current_user LIKE 'postgres.%'
  );

DROP POLICY IF EXISTS trusted_app_update_check ON public.jurisdictions;

CREATE POLICY trusted_app_update_check ON public.jurisdictions FOR
UPDATE TO PUBLIC
WITH
  CHECK (
    current_user = 'postgres'
    OR current_user LIKE 'postgres.%'
  );

DROP POLICY IF EXISTS public_read_select ON public.jurisdictions;

CREATE POLICY public_read_select ON public.jurisdictions FOR
SELECT
  TO PUBLIC USING (true);

DROP POLICY IF EXISTS public_no_insert ON public.jurisdictions;

CREATE POLICY public_no_insert ON public.jurisdictions FOR INSERT TO PUBLIC
WITH
  CHECK (false);

DROP POLICY IF EXISTS public_no_update ON public.jurisdictions;

CREATE POLICY public_no_update ON public.jurisdictions FOR
UPDATE TO PUBLIC USING (false)
WITH
  CHECK (false);

DROP POLICY IF EXISTS public_no_delete ON public.jurisdictions;

CREATE POLICY public_no_delete ON public.jurisdictions FOR DELETE TO PUBLIC USING (false);

ALTER TABLE IF EXISTS public.tax_rules ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS trusted_app_all ON public.tax_rules;

CREATE POLICY trusted_app_all ON public.tax_rules FOR ALL TO PUBLIC USING (
  current_user = 'postgres'
  OR current_user LIKE 'postgres.%'
);

DROP POLICY IF EXISTS trusted_app_insert_check ON public.tax_rules;

CREATE POLICY trusted_app_insert_check ON public.tax_rules FOR INSERT TO PUBLIC
WITH
  CHECK (
    current_user = 'postgres'
    OR current_user LIKE 'postgres.%'
  );

DROP POLICY IF EXISTS trusted_app_update_check ON public.tax_rules;

CREATE POLICY trusted_app_update_check ON public.tax_rules FOR
UPDATE TO PUBLIC
WITH
  CHECK (
    current_user = 'postgres'
    OR current_user LIKE 'postgres.%'
  );

DROP POLICY IF EXISTS public_read_select ON public.tax_rules;

CREATE POLICY public_read_select ON public.tax_rules FOR
SELECT
  TO PUBLIC USING (true);

DROP POLICY IF EXISTS public_no_insert ON public.tax_rules;

CREATE POLICY public_no_insert ON public.tax_rules FOR INSERT TO PUBLIC
WITH
  CHECK (false);

DROP POLICY IF EXISTS public_no_update ON public.tax_rules;

CREATE POLICY public_no_update ON public.tax_rules FOR
UPDATE TO PUBLIC USING (false)
WITH
  CHECK (false);

DROP POLICY IF EXISTS public_no_delete ON public.tax_rules;

CREATE POLICY public_no_delete ON public.tax_rules FOR DELETE TO PUBLIC USING (false);

ALTER TABLE IF EXISTS public.platforms ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS trusted_app_all ON public.platforms;

CREATE POLICY trusted_app_all ON public.platforms FOR ALL TO PUBLIC USING (
  current_user = 'postgres'
  OR current_user LIKE 'postgres.%'
);

DROP POLICY IF EXISTS trusted_app_insert_check ON public.platforms;

CREATE POLICY trusted_app_insert_check ON public.platforms FOR INSERT TO PUBLIC
WITH
  CHECK (
    current_user = 'postgres'
    OR current_user LIKE 'postgres.%'
  );

DROP POLICY IF EXISTS trusted_app_update_check ON public.platforms;

CREATE POLICY trusted_app_update_check ON public.platforms FOR
UPDATE TO PUBLIC
WITH
  CHECK (
    current_user = 'postgres'
    OR current_user LIKE 'postgres.%'
  );

DROP POLICY IF EXISTS public_read_select ON public.platforms;

CREATE POLICY public_read_select ON public.platforms FOR
SELECT
  TO PUBLIC USING (true);

DROP POLICY IF EXISTS public_no_insert ON public.platforms;

CREATE POLICY public_no_insert ON public.platforms FOR INSERT TO PUBLIC
WITH
  CHECK (false);

DROP POLICY IF EXISTS public_no_update ON public.platforms;

CREATE POLICY public_no_update ON public.platforms FOR
UPDATE TO PUBLIC USING (false)
WITH
  CHECK (false);

DROP POLICY IF EXISTS public_no_delete ON public.platforms;

CREATE POLICY public_no_delete ON public.platforms FOR DELETE TO PUBLIC USING (false);

ALTER TABLE IF EXISTS public.products ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS trusted_app_all ON public.products;

CREATE POLICY trusted_app_all ON public.products FOR ALL TO PUBLIC USING (
  current_user = 'postgres'
  OR current_user LIKE 'postgres.%'
);

DROP POLICY IF EXISTS trusted_app_insert_check ON public.products;

CREATE POLICY trusted_app_insert_check ON public.products FOR INSERT TO PUBLIC
WITH
  CHECK (
    current_user = 'postgres'
    OR current_user LIKE 'postgres.%'
  );

DROP POLICY IF EXISTS trusted_app_update_check ON public.products;

CREATE POLICY trusted_app_update_check ON public.products FOR
UPDATE TO PUBLIC
WITH
  CHECK (
    current_user = 'postgres'
    OR current_user LIKE 'postgres.%'
  );

DROP POLICY IF EXISTS public_read_select ON public.products;

CREATE POLICY public_read_select ON public.products FOR
SELECT
  TO PUBLIC USING (true);

DROP POLICY IF EXISTS public_no_insert ON public.products;

CREATE POLICY public_no_insert ON public.products FOR INSERT TO PUBLIC
WITH
  CHECK (false);

DROP POLICY IF EXISTS public_no_update ON public.products;

CREATE POLICY public_no_update ON public.products FOR
UPDATE TO PUBLIC USING (false)
WITH
  CHECK (false);

DROP POLICY IF EXISTS public_no_delete ON public.products;

CREATE POLICY public_no_delete ON public.products FOR DELETE TO PUBLIC USING (false);

ALTER TABLE IF EXISTS public.software ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS trusted_app_all ON public.software;

CREATE POLICY trusted_app_all ON public.software FOR ALL TO PUBLIC USING (
  current_user = 'postgres'
  OR current_user LIKE 'postgres.%'
);

DROP POLICY IF EXISTS trusted_app_insert_check ON public.software;

CREATE POLICY trusted_app_insert_check ON public.software FOR INSERT TO PUBLIC
WITH
  CHECK (
    current_user = 'postgres'
    OR current_user LIKE 'postgres.%'
  );

DROP POLICY IF EXISTS trusted_app_update_check ON public.software;

CREATE POLICY trusted_app_update_check ON public.software FOR
UPDATE TO PUBLIC
WITH
  CHECK (
    current_user = 'postgres'
    OR current_user LIKE 'postgres.%'
  );

ALTER TABLE IF EXISTS public.hardware ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS trusted_app_all ON public.hardware;

CREATE POLICY trusted_app_all ON public.hardware FOR ALL TO PUBLIC USING (
  current_user = 'postgres'
  OR current_user LIKE 'postgres.%'
);

DROP POLICY IF EXISTS trusted_app_insert_check ON public.hardware;

CREATE POLICY trusted_app_insert_check ON public.hardware FOR INSERT TO PUBLIC
WITH
  CHECK (
    current_user = 'postgres'
    OR current_user LIKE 'postgres.%'
  );

DROP POLICY IF EXISTS trusted_app_update_check ON public.hardware;

CREATE POLICY trusted_app_update_check ON public.hardware FOR
UPDATE TO PUBLIC
WITH
  CHECK (
    current_user = 'postgres'
    OR current_user LIKE 'postgres.%'
  );

ALTER TABLE IF EXISTS public.video_game_titles ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS trusted_app_all ON public.video_game_titles;

CREATE POLICY trusted_app_all ON public.video_game_titles FOR ALL TO PUBLIC USING (
  current_user = 'postgres'
  OR current_user LIKE 'postgres.%'
);

DROP POLICY IF EXISTS trusted_app_insert_check ON public.video_game_titles;

CREATE POLICY trusted_app_insert_check ON public.video_game_titles FOR INSERT TO PUBLIC
WITH
  CHECK (
    current_user = 'postgres'
    OR current_user LIKE 'postgres.%'
  );

DROP POLICY IF EXISTS trusted_app_update_check ON public.video_game_titles;

CREATE POLICY trusted_app_update_check ON public.video_game_titles FOR
UPDATE TO PUBLIC
WITH
  CHECK (
    current_user = 'postgres'
    OR current_user LIKE 'postgres.%'
  );

ALTER TABLE IF EXISTS public.video_games ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS trusted_app_all ON public.video_games;

CREATE POLICY trusted_app_all ON public.video_games FOR ALL TO PUBLIC USING (
  current_user = 'postgres'
  OR current_user LIKE 'postgres.%'
);

DROP POLICY IF EXISTS trusted_app_insert_check ON public.video_games;

CREATE POLICY trusted_app_insert_check ON public.video_games FOR INSERT TO PUBLIC
WITH
  CHECK (
    current_user = 'postgres'
    OR current_user LIKE 'postgres.%'
  );

DROP POLICY IF EXISTS trusted_app_update_check ON public.video_games;

CREATE POLICY trusted_app_update_check ON public.video_games FOR
UPDATE TO PUBLIC
WITH
  CHECK (
    current_user = 'postgres'
    OR current_user LIKE 'postgres.%'
  );

ALTER TABLE IF EXISTS public.game_consoles ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS trusted_app_all ON public.game_consoles;

CREATE POLICY trusted_app_all ON public.game_consoles FOR ALL TO PUBLIC USING (
  current_user = 'postgres'
  OR current_user LIKE 'postgres.%'
);

DROP POLICY IF EXISTS trusted_app_insert_check ON public.game_consoles;

CREATE POLICY trusted_app_insert_check ON public.game_consoles FOR INSERT TO PUBLIC
WITH
  CHECK (
    current_user = 'postgres'
    OR current_user LIKE 'postgres.%'
  );

DROP POLICY IF EXISTS trusted_app_update_check ON public.game_consoles;

CREATE POLICY trusted_app_update_check ON public.game_consoles FOR
UPDATE TO PUBLIC
WITH
  CHECK (
    current_user = 'postgres'
    OR current_user LIKE 'postgres.%'
  );

ALTER TABLE IF EXISTS public.game_media ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS trusted_app_all ON public.game_media;

CREATE POLICY trusted_app_all ON public.game_media FOR ALL TO PUBLIC USING (
  current_user = 'postgres'
  OR current_user LIKE 'postgres.%'
);

DROP POLICY IF EXISTS trusted_app_insert_check ON public.game_media;

CREATE POLICY trusted_app_insert_check ON public.game_media FOR INSERT TO PUBLIC
WITH
  CHECK (
    current_user = 'postgres'
    OR current_user LIKE 'postgres.%'
  );

DROP POLICY IF EXISTS trusted_app_update_check ON public.game_media;

CREATE POLICY trusted_app_update_check ON public.game_media FOR
UPDATE TO PUBLIC
WITH
  CHECK (
    current_user = 'postgres'
    OR current_user LIKE 'postgres.%'
  );

-- Trusted-only tables
ALTER TABLE IF EXISTS public.sellables ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS trusted_app_all ON public.sellables;

CREATE POLICY trusted_app_all ON public.sellables FOR ALL TO PUBLIC USING (
  current_user = 'postgres'
  OR current_user LIKE 'postgres.%'
);

DROP POLICY IF EXISTS trusted_app_insert_check ON public.sellables;

CREATE POLICY trusted_app_insert_check ON public.sellables FOR INSERT TO PUBLIC
WITH
  CHECK (
    current_user = 'postgres'
    OR current_user LIKE 'postgres.%'
  );

DROP POLICY IF EXISTS trusted_app_update_check ON public.sellables;

CREATE POLICY trusted_app_update_check ON public.sellables FOR
UPDATE TO PUBLIC
WITH
  CHECK (
    current_user = 'postgres'
    OR current_user LIKE 'postgres.%'
  );

ALTER TABLE IF EXISTS public.retailers ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS trusted_app_all ON public.retailers;

CREATE POLICY trusted_app_all ON public.retailers FOR ALL TO PUBLIC USING (
  current_user = 'postgres'
  OR current_user LIKE 'postgres.%'
);

DROP POLICY IF EXISTS trusted_app_insert_check ON public.retailers;

CREATE POLICY trusted_app_insert_check ON public.retailers FOR INSERT TO PUBLIC
WITH
  CHECK (
    current_user = 'postgres'
    OR current_user LIKE 'postgres.%'
  );

DROP POLICY IF EXISTS trusted_app_update_check ON public.retailers;

CREATE POLICY trusted_app_update_check ON public.retailers FOR
UPDATE TO PUBLIC
WITH
  CHECK (
    current_user = 'postgres'
    OR current_user LIKE 'postgres.%'
  );

ALTER TABLE IF EXISTS public.offers ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS trusted_app_all ON public.offers;

CREATE POLICY trusted_app_all ON public.offers FOR ALL TO PUBLIC USING (
  current_user = 'postgres'
  OR current_user LIKE 'postgres.%'
);

DROP POLICY IF EXISTS trusted_app_insert_check ON public.offers;

CREATE POLICY trusted_app_insert_check ON public.offers FOR INSERT TO PUBLIC
WITH
  CHECK (
    current_user = 'postgres'
    OR current_user LIKE 'postgres.%'
  );

DROP POLICY IF EXISTS trusted_app_update_check ON public.offers;

CREATE POLICY trusted_app_update_check ON public.offers FOR
UPDATE TO PUBLIC
WITH
  CHECK (
    current_user = 'postgres'
    OR current_user LIKE 'postgres.%'
  );

ALTER TABLE IF EXISTS public.offer_jurisdictions ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS trusted_app_all ON public.offer_jurisdictions;

CREATE POLICY trusted_app_all ON public.offer_jurisdictions FOR ALL TO PUBLIC USING (
  current_user = 'postgres'
  OR current_user LIKE 'postgres.%'
);

DROP POLICY IF EXISTS trusted_app_insert_check ON public.offer_jurisdictions;

CREATE POLICY trusted_app_insert_check ON public.offer_jurisdictions FOR INSERT TO PUBLIC
WITH
  CHECK (
    current_user = 'postgres'
    OR current_user LIKE 'postgres.%'
  );

DROP POLICY IF EXISTS trusted_app_update_check ON public.offer_jurisdictions;

CREATE POLICY trusted_app_update_check ON public.offer_jurisdictions FOR
UPDATE TO PUBLIC
WITH
  CHECK (
    current_user = 'postgres'
    OR current_user LIKE 'postgres.%'
  );

ALTER TABLE IF EXISTS public.current_price ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS trusted_app_all ON public.current_price;

CREATE POLICY trusted_app_all ON public.current_price FOR ALL TO PUBLIC USING (
  current_user = 'postgres'
  OR current_user LIKE 'postgres.%'
);

DROP POLICY IF EXISTS trusted_app_insert_check ON public.current_price;

CREATE POLICY trusted_app_insert_check ON public.current_price FOR INSERT TO PUBLIC
WITH
  CHECK (
    current_user = 'postgres'
    OR current_user LIKE 'postgres.%'
  );

DROP POLICY IF EXISTS trusted_app_update_check ON public.current_price;

CREATE POLICY trusted_app_update_check ON public.current_price FOR
UPDATE TO PUBLIC
WITH
  CHECK (
    current_user = 'postgres'
    OR current_user LIKE 'postgres.%'
  );

ALTER TABLE IF EXISTS public.providers ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS trusted_app_all ON public.providers;

CREATE POLICY trusted_app_all ON public.providers FOR ALL TO PUBLIC USING (
  current_user = 'postgres'
  OR current_user LIKE 'postgres.%'
);

DROP POLICY IF EXISTS trusted_app_insert_check ON public.providers;

CREATE POLICY trusted_app_insert_check ON public.providers FOR INSERT TO PUBLIC
WITH
  CHECK (
    current_user = 'postgres'
    OR current_user LIKE 'postgres.%'
  );

DROP POLICY IF EXISTS trusted_app_update_check ON public.providers;

CREATE POLICY trusted_app_update_check ON public.providers FOR
UPDATE TO PUBLIC
WITH
  CHECK (
    current_user = 'postgres'
    OR current_user LIKE 'postgres.%'
  );

ALTER TABLE IF EXISTS public.provider_items ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS trusted_app_all ON public.provider_items;

CREATE POLICY trusted_app_all ON public.provider_items FOR ALL TO PUBLIC USING (
  current_user = 'postgres'
  OR current_user LIKE 'postgres.%'
);

DROP POLICY IF EXISTS trusted_app_insert_check ON public.provider_items;

CREATE POLICY trusted_app_insert_check ON public.provider_items FOR INSERT TO PUBLIC
WITH
  CHECK (
    current_user = 'postgres'
    OR current_user LIKE 'postgres.%'
  );

DROP POLICY IF EXISTS trusted_app_update_check ON public.provider_items;

CREATE POLICY trusted_app_update_check ON public.provider_items FOR
UPDATE TO PUBLIC
WITH
  CHECK (
    current_user = 'postgres'
    OR current_user LIKE 'postgres.%'
  );

ALTER TABLE IF EXISTS public.provider_offers ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS trusted_app_all ON public.provider_offers;

CREATE POLICY trusted_app_all ON public.provider_offers FOR ALL TO PUBLIC USING (
  current_user = 'postgres'
  OR current_user LIKE 'postgres.%'
);

DROP POLICY IF EXISTS trusted_app_insert_check ON public.provider_offers;

CREATE POLICY trusted_app_insert_check ON public.provider_offers FOR INSERT TO PUBLIC
WITH
  CHECK (
    current_user = 'postgres'
    OR current_user LIKE 'postgres.%'
  );

DROP POLICY IF EXISTS trusted_app_update_check ON public.provider_offers;

CREATE POLICY trusted_app_update_check ON public.provider_offers FOR
UPDATE TO PUBLIC
WITH
  CHECK (
    current_user = 'postgres'
    OR current_user LIKE 'postgres.%'
  );

ALTER TABLE IF EXISTS public.provider_ingest_runs ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS trusted_app_all ON public.provider_ingest_runs;

CREATE POLICY trusted_app_all ON public.provider_ingest_runs FOR ALL TO PUBLIC USING (
  current_user = 'postgres'
  OR current_user LIKE 'postgres.%'
);

DROP POLICY IF EXISTS trusted_app_insert_check ON public.provider_ingest_runs;

CREATE POLICY trusted_app_insert_check ON public.provider_ingest_runs FOR INSERT TO PUBLIC
WITH
  CHECK (
    current_user = 'postgres'
    OR current_user LIKE 'postgres.%'
  );

DROP POLICY IF EXISTS trusted_app_update_check ON public.provider_ingest_runs;

CREATE POLICY trusted_app_update_check ON public.provider_ingest_runs FOR
UPDATE TO PUBLIC
WITH
  CHECK (
    current_user = 'postgres'
    OR current_user LIKE 'postgres.%'
  );

ALTER TABLE IF EXISTS public.provider_sync_states ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS trusted_app_all ON public.provider_sync_states;

CREATE POLICY trusted_app_all ON public.provider_sync_states FOR ALL TO PUBLIC USING (
  current_user = 'postgres'
  OR current_user LIKE 'postgres.%'
);

DROP POLICY IF EXISTS trusted_app_insert_check ON public.provider_sync_states;

CREATE POLICY trusted_app_insert_check ON public.provider_sync_states FOR INSERT TO PUBLIC
WITH
  CHECK (
    current_user = 'postgres'
    OR current_user LIKE 'postgres.%'
  );

DROP POLICY IF EXISTS trusted_app_update_check ON public.provider_sync_states;

CREATE POLICY trusted_app_update_check ON public.provider_sync_states FOR
UPDATE TO PUBLIC
WITH
  CHECK (
    current_user = 'postgres'
    OR current_user LIKE 'postgres.%'
  );

ALTER TABLE IF EXISTS public.provider_media_links ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS trusted_app_all ON public.provider_media_links;

CREATE POLICY trusted_app_all ON public.provider_media_links FOR ALL TO PUBLIC USING (
  current_user = 'postgres'
  OR current_user LIKE 'postgres.%'
);

DROP POLICY IF EXISTS trusted_app_insert_check ON public.provider_media_links;

CREATE POLICY trusted_app_insert_check ON public.provider_media_links FOR INSERT TO PUBLIC
WITH
  CHECK (
    current_user = 'postgres'
    OR current_user LIKE 'postgres.%'
  );

DROP POLICY IF EXISTS trusted_app_update_check ON public.provider_media_links;

CREATE POLICY trusted_app_update_check ON public.provider_media_links FOR
UPDATE TO PUBLIC
WITH
  CHECK (
    current_user = 'postgres'
    OR current_user LIKE 'postgres.%'
  );

ALTER TABLE IF EXISTS public.users ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS trusted_app_all ON public.users;

CREATE POLICY trusted_app_all ON public.users FOR ALL TO PUBLIC USING (
  current_user = 'postgres'
  OR current_user LIKE 'postgres.%'
);

DROP POLICY IF EXISTS trusted_app_insert_check ON public.users;

CREATE POLICY trusted_app_insert_check ON public.users FOR INSERT TO PUBLIC
WITH
  CHECK (
    current_user = 'postgres'
    OR current_user LIKE 'postgres.%'
  );

DROP POLICY IF EXISTS trusted_app_update_check ON public.users;

CREATE POLICY trusted_app_update_check ON public.users FOR
UPDATE TO PUBLIC
WITH
  CHECK (
    current_user = 'postgres'
    OR current_user LIKE 'postgres.%'
  );

ALTER TABLE IF EXISTS public.alerts ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS trusted_app_all ON public.alerts;

CREATE POLICY trusted_app_all ON public.alerts FOR ALL TO PUBLIC USING (
  current_user = 'postgres'
  OR current_user LIKE 'postgres.%'
);

DROP POLICY IF EXISTS trusted_app_insert_check ON public.alerts;

CREATE POLICY trusted_app_insert_check ON public.alerts FOR INSERT TO PUBLIC
WITH
  CHECK (
    current_user = 'postgres'
    OR current_user LIKE 'postgres.%'
  );

DROP POLICY IF EXISTS trusted_app_update_check ON public.alerts;

CREATE POLICY trusted_app_update_check ON public.alerts FOR
UPDATE TO PUBLIC
WITH
  CHECK (
    current_user = 'postgres'
    OR current_user LIKE 'postgres.%'
  );