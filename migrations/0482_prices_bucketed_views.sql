-- Idempotent hourly/daily bucketed series per source for charting
-- Buckets select the latest price within each hour/day per offer_jurisdiction and provider

-- Hourly: latest per hour
DROP VIEW IF EXISTS prices_hourly_last_by_source CASCADE;
CREATE VIEW prices_hourly_last_by_source AS
SELECT DISTINCT ON (
  p.offer_jurisdiction_id,
  pi.provider_id,
  date_trunc('hour', p.recorded_at)
)
  p.offer_jurisdiction_id,
  pi.provider_id,
  prv.slug AS provider_slug,
  date_trunc('hour', p.recorded_at) AS bucket,
  p.amount_minor,
  p.recorded_at
FROM public.prices p
LEFT JOIN public.provider_items pi ON pi.id = p.provider_item_id
LEFT JOIN public.providers prv ON prv.id = pi.provider_id
ORDER BY
  p.offer_jurisdiction_id,
  pi.provider_id,
  date_trunc('hour', p.recorded_at),
  p.recorded_at DESC;

-- Daily: latest per day
DROP VIEW IF EXISTS prices_daily_last_by_source CASCADE;
CREATE VIEW prices_daily_last_by_source AS
SELECT DISTINCT ON (
  p.offer_jurisdiction_id,
  pi.provider_id,
  date_trunc('day', p.recorded_at)
)
  p.offer_jurisdiction_id,
  pi.provider_id,
  prv.slug AS provider_slug,
  date_trunc('day', p.recorded_at) AS bucket,
  p.amount_minor,
  p.recorded_at
FROM public.prices p
LEFT JOIN public.provider_items pi ON pi.id = p.provider_item_id
LEFT JOIN public.providers prv ON prv.id = pi.provider_id
ORDER BY
  p.offer_jurisdiction_id,
  pi.provider_id,
  date_trunc('day', p.recorded_at),
  p.recorded_at DESC;

-- Notes:
-- * These views scan partitions via the parent table.
-- * For performance, always filter by bucket range AND offer_jurisdiction_id.
--   Example:
--   SELECT * FROM prices_hourly_last_by_source
--   WHERE offer_jurisdiction_id = $1
--     AND bucket >= now() - interval '30 days'
--   ORDER BY bucket;
