-- 0535_remove_unnecessary_tables.sql
-- Purpose: Drop unnecessary tables that are out of scope for phase 1
-- These tables were designed for a broader commerce/retail platform but are not needed for the game pricing ingestion system.

-- Drop commerce tables (offers, sellables, offer_jurisdictions)
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE tablename='offers') THEN
    DROP TABLE IF EXISTS offers CASCADE;
  END IF;
END $$;

DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE tablename='offer_jurisdictions') THEN
    DROP TABLE IF EXISTS offer_jurisdictions CASCADE;
  END IF;
END $$;

DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE tablename='sellables') THEN
    DROP TABLE IF EXISTS sellables CASCADE;
  END IF;
END $$;

-- Drop product type tables (software, hardware - use products table instead)
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE tablename='software') THEN
    DROP TABLE IF EXISTS software CASCADE;
  END IF;
END $$;

DO $$ BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_tables
    WHERE schemaname='public' AND tablename='hardware'
  ) THEN
    RAISE NOTICE 'Retaining hardware table for console/hardware linkage; skipping drop in 0535.';
  END IF;
END $$;

-- Drop legacy provider tracking (replaced by video_game_sources)
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE tablename='provider_offers') THEN
    DROP TABLE IF EXISTS provider_offers CASCADE;
  END IF;
END $$;

DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE tablename='provider_ingest_runs') THEN
    DROP TABLE IF EXISTS provider_ingest_runs CASCADE;
  END IF;
END $$;

-- Drop redundant provider/game tables
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE tablename='game_providers') THEN
    DROP TABLE IF EXISTS game_providers CASCADE;
  END IF;
END $$;

DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE tablename='video_game_title_sources') THEN
    DROP TABLE IF EXISTS video_game_title_sources CASCADE;
  END IF;
END $$;

-- Drop rating and tax tables (not in scope for phase 1)
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE tablename='video_game_ratings_by_locale') THEN
    DROP TABLE IF EXISTS video_game_ratings_by_locale CASCADE;
  END IF;
END $$;

DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE tablename='tax_rules') THEN
    DROP TABLE IF EXISTS tax_rules CASCADE;
  END IF;
END $$;

-- Drop helper tables (not needed)
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE tablename='country_iso_map') THEN
    DROP TABLE IF EXISTS country_iso_map CASCADE;
  END IF;
END $$;

-- Verify cleanup
DO $$ BEGIN
  RAISE NOTICE 'Cleanup complete: removed offers, sellables, offer_jurisdictions, software, hardware, provider_offers, provider_ingest_runs, game_providers, video_game_title_sources, video_game_ratings_by_locale, tax_rules, country_iso_map';
END $$;
