-- Migration: Create vg_source_media_links and video_game_sources tables
-- Purpose: Create core Rust tables with "video_game_source" naming from scratch
-- Note: Uses video_game_sources directly, no intermediate provider tables

-- ============================================================================
-- STEP 1: Create video_game_sources table
-- ============================================================================

CREATE TABLE IF NOT EXISTS video_game_sources (
  id bigserial PRIMARY KEY,
  video_game_id bigint REFERENCES video_games(id) ON DELETE SET NULL,
  provider varchar(64),
  provider_game_id varchar(128),
  provider_slug varchar(128),
  provider_hash varchar(64),
  payload json,
  links json,
  media json,
  synced_at timestamptz,
  created_at timestamptz,
  updated_at timestamptz,
  provider_key varchar(64),
  display_name varchar(255),
  category varchar(64),
  slug varchar(255),
  metadata json
);

CREATE UNIQUE INDEX IF NOT EXISTS video_game_sources_provider_key_uq
  ON video_game_sources(provider_key);
CREATE UNIQUE INDEX IF NOT EXISTS video_game_sources_provider_game_uq
  ON video_game_sources(provider, provider_game_id);
CREATE INDEX IF NOT EXISTS video_game_sources_slug_idx
  ON video_game_sources(slug);
CREATE INDEX IF NOT EXISTS video_game_sources_video_game_provider_idx
  ON video_game_sources(video_game_id, provider);

-- ============================================================================
-- STEP 2: Create vg_source_media_links table
-- ============================================================================

CREATE TABLE IF NOT EXISTS vg_source_media_links (
  id bigserial PRIMARY KEY,
  video_game_source_id bigint NOT NULL REFERENCES video_game_sources(id) ON DELETE CASCADE,
  media_id bigint,
  url text,
  video_game_id bigint REFERENCES video_games(id) ON DELETE SET NULL,
  media_type text,
  title text,
  role text,
  source text,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (media_id IS NOT NULL OR url IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS vg_source_media_links_source_idx
  ON vg_source_media_links(video_game_source_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_vg_source_media_links_source_url
  ON vg_source_media_links(video_game_source_id, url);
CREATE INDEX IF NOT EXISTS vg_source_media_links_video_game_idx
  ON vg_source_media_links(video_game_id);
CREATE INDEX IF NOT EXISTS vg_source_media_links_type_role_idx
  ON vg_source_media_links(media_type, role);
CREATE INDEX IF NOT EXISTS idx_vg_source_media_links_vg_type
  ON vg_source_media_links(video_game_id, media_type);

COMMENT ON TABLE vg_source_media_links IS
    'Links between video_game_sources and media URLs (images/videos).
    References video_game_sources directly without intermediate provider tables.';

-- ============================================================================
-- ROLLBACK INSTRUCTIONS (if needed)
-- ============================================================================

/*
-- To rollback this migration, run the following:

DROP TABLE IF EXISTS vg_source_media_links CASCADE;
DROP TABLE IF EXISTS video_game_sources CASCADE;
*/

-- ============================================================================
-- NOTES
-- ============================================================================

/*
This migration creates the core Rust tables from scratch with proper naming.

Tables created:
1. video_game_sources - Primary source metadata table (replaces old providers system)
2. vg_source_media_links - Media URLs linked directly to video_game_sources

Key design decisions:
- NO intermediate provider_items table - links directly to video_game_sources
- Uses video_game_source_id instead of provider_item_id
- Clean "vg_source" naming throughout

After this migration:
- Rust code uses 'vg_source_media_links' table name
- Functions use ensure_vg_source_media_links() naming
- References video_game_sources.id directly

Related work:
- Phase 2: Add uniqueness constraints (migration 0530)
- Phase 3: Function renaming in src/database_ops/ingest_providers.rs
- Phase 4: Update all provider implementations to use video_game_sources

Impact: Medium - creates new tables from scratch
Risk: Low - all operations use IF NOT EXISTS
*/
