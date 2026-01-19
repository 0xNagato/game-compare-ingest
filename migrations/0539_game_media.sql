-- 0472_game_media.sql
-- Minimal unified media table and enums required by code/tests (idempotent)

-- Enums
DO $$ BEGIN
  CREATE TYPE media_type AS ENUM (
    'cover', 'hero', 'screenshot', 'artwork', 'trailer', 'gameplay', 'logo', 'icon'
  );
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE TYPE media_source AS ENUM (
    'igdb','giant_bomb','rawg','tgdb','psn','steam','youtube','wikimedia','nexarda','manual'
  );
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- Table
CREATE TABLE IF NOT EXISTS game_media (
  -- composite PK, id not required
  video_game_id    bigint NOT NULL,
  source           media_source NOT NULL,
  external_id      text NOT NULL,
  media_type       media_type NOT NULL,
  url              text NOT NULL CHECK (length(url) > 0),
  cdn_url          text,
  width            integer CHECK (width IS NULL OR width > 0),
  height           integer CHECK (height IS NULL OR height > 0),
  size_bytes       bigint CHECK (size_bytes IS NULL OR size_bytes > 0),
  duration_seconds integer CHECK (duration_seconds IS NULL OR duration_seconds > 0),
  mime_type        text,
  hash             text,
  provider_data    jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at       timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (video_game_id, source, external_id)
);

CREATE INDEX IF NOT EXISTS game_media_created_brin_idx ON game_media USING brin(created_at);
-- Avoid volatile function in predicate; support recent-first queries via included created_at ordering
CREATE INDEX IF NOT EXISTS game_media_video_game_type_created_idx ON game_media (video_game_id, media_type, created_at DESC);
CREATE INDEX IF NOT EXISTS game_media_hash_idx ON game_media (hash) WHERE hash IS NOT NULL;
CREATE INDEX IF NOT EXISTS game_media_provider_data_idx ON game_media USING gin(provider_data);
