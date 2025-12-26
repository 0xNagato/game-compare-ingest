-- 0390_video_game_ratings_by_locale.sql (squashed)
-- Per-locale ratings for video games
CREATE TABLE IF NOT EXISTS public.video_game_ratings_by_locale (
  id                 bigserial PRIMARY KEY,
  video_game_id      bigint NOT NULL REFERENCES public.video_games(id) ON DELETE CASCADE,
  locale             text NOT NULL,
  average_rating     real NOT NULL,
  rating_count       bigint NOT NULL,
  rating_updated_at  timestamptz NOT NULL DEFAULT now(),
  UNIQUE (video_game_id, locale)
);

CREATE INDEX IF NOT EXISTS idx_vgrl_game ON public.video_game_ratings_by_locale (video_game_id);
CREATE INDEX IF NOT EXISTS idx_vgrl_locale ON public.video_game_ratings_by_locale (locale);
