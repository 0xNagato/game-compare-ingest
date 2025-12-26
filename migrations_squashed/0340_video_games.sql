-- 0340_video_games.sql (squashed)
-- Consolidated video_games schema including ratings, metadata, popularity, developer/display fields
CREATE TABLE IF NOT EXISTS public.video_games (
  id                 bigserial PRIMARY KEY,
  title_id           bigint NOT NULL REFERENCES public.video_game_titles(id) ON DELETE CASCADE,
  platform_id        bigint NOT NULL REFERENCES public.platforms(id),
  edition            text,
  -- ratings (legacy aggregate fields)
  average_rating     real,
  rating_count       bigint,
  rating_updated_at  timestamptz,
  genres             text[],
  -- enrichment
  release_date       date,
  display_title      text,
  developer          text,
  region_codes       text[],
  popularity_score   numeric NOT NULL DEFAULT 0,
  rating             numeric NOT NULL DEFAULT 0,
  synopsis           text,
  metadata           jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at         timestamptz NOT NULL DEFAULT now(),
  updated_at         timestamptz NOT NULL DEFAULT now()
);

-- Conditional uniqueness on (title_id, platform_id[, edition])
CREATE UNIQUE INDEX IF NOT EXISTS uq_video_games_title_platform_null
  ON public.video_games(title_id, platform_id) WHERE edition IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_video_games_title_platform_edition
  ON public.video_games(title_id, platform_id, edition) WHERE edition IS NOT NULL;
