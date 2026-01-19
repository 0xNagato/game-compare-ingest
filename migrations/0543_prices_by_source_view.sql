-- 0481_prices_by_source_view.sql
-- Purpose: Support front-end discrepancy graphs by exposing time series per source (agent)
-- Also adds indexes to speed source-joins.

-- Parent-only index to speed joins/filtering by provider_item_id and time
CREATE INDEX IF NOT EXISTS prices_provider_item_time_idx ON ONLY public.prices (provider_item_id, recorded_at);

-- View: prices_series_by_source
-- Includes provider slug (agent) derived from providers.slug via provider_items
CREATE OR REPLACE VIEW public.prices_series_by_source AS
SELECT
  p.offer_jurisdiction_id,
  p.recorded_at,
  p.amount_minor,
  p.tax_inclusive,
  pi.provider_id,
  pr.slug AS provider_slug,
  pr.kind AS provider_kind,
  p.provider_item_id
FROM public.prices p
LEFT JOIN public.provider_items pi ON pi.id = p.provider_item_id
LEFT JOIN public.providers pr ON pr.id = pi.provider_id;

-- Helper view: latest_per_source (optional) - latest point per (oj, provider)
CREATE OR REPLACE VIEW public.latest_price_per_source AS
SELECT DISTINCT ON (p.offer_jurisdiction_id, pr.id)
  p.offer_jurisdiction_id,
  pr.slug AS provider_slug,
  p.recorded_at,
  p.amount_minor,
  p.provider_item_id
FROM public.prices p
LEFT JOIN public.provider_items pi ON pi.id = p.provider_item_id
LEFT JOIN public.providers pr ON pr.id = pi.provider_id
ORDER BY p.offer_jurisdiction_id, pr.id, p.recorded_at DESC;
