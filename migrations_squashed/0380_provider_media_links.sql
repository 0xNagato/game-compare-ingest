-- 0380_provider_media_links.sql (squashed)
-- Consolidated provider_media_links with enrichment, uniqueness, and trigger
CREATE TABLE IF NOT EXISTS public.provider_media_links (
  id               bigserial PRIMARY KEY,
  provider_item_id bigint NOT NULL REFERENCES public.provider_items(id) ON DELETE CASCADE,
  media_id         bigint,
  url              text,
  -- enrichment
  video_game_id    bigint REFERENCES public.video_games(id) ON DELETE SET NULL,
  media_type       text,
  title            text,
  role             text,
  source           text,
  metadata         jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at       timestamptz NOT NULL DEFAULT now(),
  updated_at       timestamptz NOT NULL DEFAULT now(),
  CHECK (media_id IS NOT NULL OR url IS NOT NULL)
);

-- Indexes and constraints
CREATE INDEX IF NOT EXISTS provider_media_links_item_idx ON public.provider_media_links (provider_item_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_provider_media_links_item_url ON public.provider_media_links (provider_item_id, url);
CREATE INDEX IF NOT EXISTS provider_media_links_video_game_idx ON public.provider_media_links (video_game_id);
CREATE INDEX IF NOT EXISTS provider_media_links_type_role_idx ON public.provider_media_links (media_type, role);
CREATE INDEX IF NOT EXISTS provider_media_links_source_idx ON public.provider_media_links (source);
CREATE INDEX IF NOT EXISTS idx_provider_media_links_vg_type ON public.provider_media_links (video_game_id, media_type);

-- Touch updated_at on update
CREATE OR REPLACE FUNCTION public.provider_media_links_touch_updated_at()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END $$;

DO $$ BEGIN
  CREATE TRIGGER provider_media_links_touch_updated_at_trg
    BEFORE UPDATE ON public.provider_media_links
    FOR EACH ROW EXECUTE FUNCTION public.provider_media_links_touch_updated_at();
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

COMMENT ON TABLE public.provider_media_links IS 'Raw provider media linkage with optional direct video_game_id + typed metadata';
COMMENT ON INDEX uq_provider_media_links_item_url IS 'Deduplicate media links per provider item by URL';
COMMENT ON INDEX idx_provider_media_links_vg_type IS 'Accelerates lookups for game media by type';
COMMENT ON COLUMN public.provider_media_links.media_type IS 'Type of media: image|video|other';
COMMENT ON COLUMN public.provider_media_links.role IS 'Logical role: hero|gallery|trailer|thumbnail';
COMMENT ON COLUMN public.provider_media_links.metadata IS 'Structured metadata: JSON (locale, platform, dimensions, duration, etc.)';
COMMENT ON COLUMN public.provider_media_links.video_game_id IS 'Optional linkage to internal video_games row';
