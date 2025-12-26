-- 0330_video_game_titles.sql (squashed)
-- Consolidated video_game_titles schema with enrichment columns
CREATE TABLE IF NOT EXISTS public.video_game_titles (
  id               bigserial PRIMARY KEY,
  product_id       bigint NOT NULL UNIQUE REFERENCES public.software(product_id) ON DELETE CASCADE,
  name             text NOT NULL,
  slug             citext UNIQUE,
  summary          text,
  primary_image_url text,
  metadata         jsonb,
  created_at       timestamptz NOT NULL DEFAULT now(),
  updated_at       timestamptz NOT NULL DEFAULT now()
);

-- Search indexes
CREATE INDEX IF NOT EXISTS video_game_titles_name_trgm_idx ON public.video_game_titles USING gin ((name) ext.gin_trgm_ops);
CREATE INDEX IF NOT EXISTS video_game_titles_slug_trgm_idx ON public.video_game_titles USING gin ((slug::text) ext.gin_trgm_ops);
