-- 0360_game_images.sql (squashed)
-- Consolidated game_images with extended columns and indexes
CREATE TABLE IF NOT EXISTS public.game_images (
  id               bigserial PRIMARY KEY,
  video_game_id    bigint REFERENCES public.video_games(id) ON DELETE CASCADE,
  provider_item_id bigint REFERENCES public.provider_items(id) ON DELETE SET NULL,
  kind             text NOT NULL DEFAULT 'screenshot',
  mime_type        text NOT NULL,
  width            integer NOT NULL CHECK (width > 0),
  height           integer NOT NULL CHECK (height > 0),
  url              text NOT NULL,
  -- extensions
  ordinal          smallint NOT NULL DEFAULT 0,
  is_primary       boolean NOT NULL DEFAULT false,
  original_url     text,
  thumbnail_url    text,
  attribution      text,
  license          text,
  license_url      text,
  quality_score    real,
  fetched_at       timestamptz,
  metadata         jsonb,
  source           text,
  title            text,
  caption          text,
  created_at       timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_game_images_videogame ON public.game_images(video_game_id);
CREATE INDEX IF NOT EXISTS idx_game_images_provider_item ON public.game_images(provider_item_id);
CREATE INDEX IF NOT EXISTS idx_game_images_kind ON public.game_images(kind);
CREATE INDEX IF NOT EXISTS idx_game_images_primary ON public.game_images (video_game_id, is_primary);
CREATE INDEX IF NOT EXISTS idx_game_images_ordinal ON public.game_images (video_game_id, ordinal);
CREATE INDEX IF NOT EXISTS idx_game_images_metadata_gin ON public.game_images USING gin (metadata);
