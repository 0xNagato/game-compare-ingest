-- 0492_game_media_url_columns.sql
-- Add lifted URL columns to game_media (original/thumbnail/stream/poster) and backfill from provider_data.
-- Idempotent: each ALTER guarded; safe to re-run.

-- Add columns (nullable). Use IF NOT EXISTS for idempotency.
DO $$ BEGIN
  ALTER TABLE game_media ADD COLUMN IF NOT EXISTS original_url   text;
  ALTER TABLE game_media ADD COLUMN IF NOT EXISTS thumbnail_url  text;
  ALTER TABLE game_media ADD COLUMN IF NOT EXISTS stream_url     text;
  ALTER TABLE game_media ADD COLUMN IF NOT EXISTS poster_url     text; -- e.g. preview/poster frame
EXCEPTION WHEN undefined_table THEN
  RAISE NOTICE 'game_media table missing; run prior migrations (0472_game_media.sql) first.';
END $$;

-- Backfill lifted columns from provider_data JSON where available and column is NULL.
-- We coalesce multiple possible legacy keys.
UPDATE game_media SET original_url = COALESCE(
  provider_data->>'original_url',
  provider_data->>'url',
  original_url
) WHERE original_url IS NULL AND (provider_data ? 'original_url' OR provider_data ? 'url');

UPDATE game_media SET thumbnail_url = COALESCE(
  provider_data->>'thumbnail_url',
  provider_data->>'thumb',
  provider_data->>'thumbnail',
  thumbnail_url
) WHERE thumbnail_url IS NULL AND (
  provider_data ? 'thumbnail_url' OR provider_data ? 'thumb' OR provider_data ? 'thumbnail'
);

UPDATE game_media SET stream_url = COALESCE(
  provider_data->>'stream_url',
  provider_data->>'video_url',
  provider_data->>'m3u8',
  stream_url
) WHERE stream_url IS NULL AND (
  provider_data ? 'stream_url' OR provider_data ? 'video_url' OR provider_data ? 'm3u8'
);

UPDATE game_media SET poster_url = COALESCE(
  provider_data->>'poster_url',
  provider_data->>'preview_url',
  provider_data->>'image',
  poster_url
) WHERE poster_url IS NULL AND (
  provider_data ? 'poster_url' OR provider_data ? 'preview_url' OR provider_data ? 'image'
);

-- Indexes for non-null lifted columns (skip if already present). Simple btree sufficient for equality lookups.
CREATE INDEX IF NOT EXISTS game_media_original_url_idx ON game_media (original_url) WHERE original_url IS NOT NULL;
CREATE INDEX IF NOT EXISTS game_media_thumbnail_url_idx ON game_media (thumbnail_url) WHERE thumbnail_url IS NOT NULL;
CREATE INDEX IF NOT EXISTS game_media_stream_url_idx    ON game_media (stream_url) WHERE stream_url IS NOT NULL;
CREATE INDEX IF NOT EXISTS game_media_poster_url_idx    ON game_media (poster_url) WHERE poster_url IS NOT NULL;

-- Optional: unify provider_data by removing lifted keys (commented out; enable after consumer code updated)
-- UPDATE game_media SET provider_data = provider_data - 'original_url' - 'url' - 'thumbnail_url' - 'thumb' - 'thumbnail' - 'stream_url' - 'video_url' - 'm3u8' - 'poster_url' - 'preview_url' - 'image';

-- Verification (non-blocking): count rows with any lifted URL.
DO $$ BEGIN
  PERFORM 1;
  RAISE NOTICE 'game_media lifted URL rows: %', (
    SELECT count(*) FROM game_media WHERE original_url IS NOT NULL OR thumbnail_url IS NOT NULL OR stream_url IS NOT NULL OR poster_url IS NOT NULL
  );
END $$;
