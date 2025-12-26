-- 0400_video_game_title_sources.sql (squashed)
-- Mapping provider_items to canonical video_game_titles
CREATE TABLE IF NOT EXISTS public.video_game_title_sources (
  id              bigserial PRIMARY KEY,
  title_id        bigint NOT NULL REFERENCES public.video_game_titles(id) ON DELETE CASCADE,
  provider_item_id bigint NOT NULL REFERENCES public.provider_items(id) ON DELETE CASCADE,
  source_priority smallint NOT NULL DEFAULT 0,
  created_at      timestamptz NOT NULL DEFAULT now(),
  UNIQUE (provider_item_id),
  UNIQUE (title_id, provider_item_id)
);

CREATE INDEX IF NOT EXISTS idx_title_sources_title ON public.video_game_title_sources (title_id);
CREATE INDEX IF NOT EXISTS idx_title_sources_provider_item ON public.video_game_title_sources (provider_item_id);
