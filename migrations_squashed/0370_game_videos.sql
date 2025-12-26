-- 0370_game_videos.sql (squashed)
-- Consolidated game_videos with extended columns and indexes
CREATE TABLE IF NOT EXISTS public.game_videos (
  id               bigserial PRIMARY KEY,
  video_game_id    bigint REFERENCES public.video_games(id) ON DELETE CASCADE,
  provider_item_id bigint REFERENCES public.provider_items(id) ON DELETE SET NULL,
  video_game_title text REFERENCES public.video_game_titles(title) ON DELETE CASCADE,
  kind             text NOT NULL DEFAULT 'trailer',
  mime_type        text NOT NULL,
  duration_seconds integer CHECK (duration_seconds IS NULL OR duration_seconds > 0),
  url              text NOT NULL,
  -- extensions
  ordinal          smallint NOT NULL DEFAULT 0,
  is_primary       boolean NOT NULL DEFAULT false,
  hd_url           text,
  high_url         text,
  low_url          text,
  poster_url       text,
  publish_date     timestamptz,
  source           text,
  metadata         jsonb,
  video_type       text,
  video_show       text,
  guid             text,
  video_id         text,
  embed_player     text,
  playable_url     text,
  created_at       timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_game_videos_videogame ON public.game_videos(video_game_id);
CREATE INDEX IF NOT EXISTS idx_game_videos_provider_item ON public.game_videos(provider_item_id);
CREATE INDEX IF NOT EXISTS idx_game_videos_title ON public.game_videos(video_game_title);
CREATE INDEX IF NOT EXISTS idx_game_videos_kind ON public.game_videos(kind);
CREATE INDEX IF NOT EXISTS idx_game_videos_primary ON public.game_videos (video_game_id, is_primary);
CREATE INDEX IF NOT EXISTS idx_game_videos_ordinal ON public.game_videos (video_game_id, ordinal);
CREATE INDEX IF NOT EXISTS idx_game_videos_guid ON public.game_videos (guid);
CREATE INDEX IF NOT EXISTS idx_game_videos_video_id ON public.game_videos (video_id);
CREATE INDEX IF NOT EXISTS idx_game_videos_metadata_gin ON public.game_videos USING gin (metadata);
