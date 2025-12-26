-- IMPORTANT:
-- 0001_full_consolidated_schema.sql already defines public.video_game_title_sources
-- with columns (video_game_title_id, source, source_id, ...). To avoid conflicting
-- definitions while preserving intent, this migration creates a separate link table
-- that maps provider_items directly to video_game_titles.

CREATE TABLE IF NOT EXISTS public.video_game_title_provider_items (
  id               bigserial PRIMARY KEY,
  title_id         bigint NOT NULL REFERENCES public.video_game_titles(id) ON DELETE CASCADE,
  provider_item_id bigint NOT NULL REFERENCES public.provider_items(id) ON DELETE CASCADE,
  provider_id      bigint NOT NULL REFERENCES public.providers(id) ON DELETE CASCADE,
  source_priority  smallint NOT NULL DEFAULT 0,
  created_at       timestamptz NOT NULL DEFAULT now(),
  -- Invariants: a provider item maps to exactly one canonical title
  UNIQUE (provider_item_id),
  UNIQUE (title_id, provider_item_id)
);

CREATE INDEX IF NOT EXISTS idx_vgtpi_title ON public.video_game_title_provider_items (title_id);
CREATE INDEX IF NOT EXISTS idx_vgtpi_provider_item ON public.video_game_title_provider_items (provider_item_id);
CREATE INDEX IF NOT EXISTS idx_vgtpi_provider ON public.video_game_title_provider_items (provider_id);

COMMENT ON TABLE public.video_game_title_provider_items IS 'Canonical mapping: provider_items → video_game_titles (one-to-one per provider_item)';
COMMENT ON COLUMN public.video_game_title_provider_items.source_priority IS 'Lower is higher priority when reconciling conflicting sources';
