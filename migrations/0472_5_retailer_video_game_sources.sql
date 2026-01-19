-- 0472_5_retailer_video_game_sources.sql
-- Create junction table: retailers × video_game_sources
-- Allows retailers to use multiple APIs/sources with independent credentials and sync metadata

CREATE TABLE IF NOT EXISTS public.retailer_video_game_sources (
  id bigserial PRIMARY KEY,
  retailer_id bigint NOT NULL REFERENCES public.retailers(id) ON DELETE CASCADE,
  video_game_source_id bigint NOT NULL REFERENCES public.video_game_sources(id) ON DELETE CASCADE,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  
  UNIQUE (retailer_id, video_game_source_id)
);

-- Index for common queries
CREATE INDEX IF NOT EXISTS retailer_video_game_sources_retailer_idx 
  ON public.retailer_video_game_sources (retailer_id);
CREATE INDEX IF NOT EXISTS retailer_video_game_sources_source_idx 
  ON public.retailer_video_game_sources (video_game_source_id);
