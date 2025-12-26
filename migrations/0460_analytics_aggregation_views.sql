-- 0460_analytics_aggregation_views.sql
-- Purpose: Add read-fast analytics views / materialized views for dashboards, with cron refresh.
-- Re-runnable and safe on Postgres 15+ (Supabase compatible).

-- =============================
-- DAILY PRICE AGGREGATES PER OFFER_JURISDICTION
-- =============================
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_price_daily AS
SELECT 
  date_trunc('day', p.recorded_at) AS day,
  p.offer_jurisdiction_id,
  min(p.amount_minor) AS min_amount_minor,
  max(p.amount_minor) AS max_amount_minor,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY p.amount_minor) AS median_amount_minor,
  count(*) AS samples
FROM prices p
GROUP BY 1,2;

CREATE UNIQUE INDEX IF NOT EXISTS mv_price_daily_pk
  ON mv_price_daily (day, offer_jurisdiction_id);

-- Helper to refresh daily aggregates
CREATE OR REPLACE FUNCTION refresh_mv_price_daily(concurrent boolean DEFAULT true)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  IF concurrent THEN
    BEGIN
      EXECUTE 'REFRESH MATERIALIZED VIEW CONCURRENTLY mv_price_daily';
    EXCEPTION WHEN OTHERS THEN
      EXECUTE 'REFRESH MATERIALIZED VIEW mv_price_daily';
    END;
  ELSE
    EXECUTE 'REFRESH MATERIALIZED VIEW mv_price_daily';
  END IF;
END $$;

-- =============================
-- 7-DAY ROLLING CHANGE PER OFFER_JURISDICTION (VIEW)
-- =============================
CREATE OR REPLACE VIEW vw_price_change_7d AS
WITH latest AS (
  SELECT cp.offer_jurisdiction_id,
         cp.amount_minor AS current_amount,
         cp.recorded_at  AS current_at
  FROM current_price cp
), past AS (
  SELECT p.offer_jurisdiction_id,
         p.amount_minor,
         p.recorded_at,
         row_number() OVER (PARTITION BY p.offer_jurisdiction_id ORDER BY p.recorded_at DESC) AS rn
  FROM prices p
  WHERE p.recorded_at > now() - INTERVAL '7 days'
)
SELECT l.offer_jurisdiction_id,
       l.current_amount,
       p.amount_minor AS past_amount,
       (l.current_amount - p.amount_minor) AS change_abs,
       CASE WHEN p.amount_minor > 0 THEN (l.current_amount - p.amount_minor)::numeric / p.amount_minor ELSE NULL END AS change_ratio
FROM latest l
LEFT JOIN LATERAL (
  SELECT amount_minor
  FROM past
  WHERE past.offer_jurisdiction_id = l.offer_jurisdiction_id AND rn = 1
) p ON TRUE;

-- =============================
-- TOP MOVERS VIEW (joins titles for display)
-- =============================
CREATE OR REPLACE VIEW vw_top_price_movers_7d AS
SELECT 
  vgt.title,
  oj.id AS offer_jurisdiction_id,
  c.code AS currency_code,
  v.change_abs,
  v.change_ratio
FROM vw_price_change_7d v
JOIN offer_jurisdictions oj ON oj.id = v.offer_jurisdiction_id
JOIN offers o ON o.id = oj.offer_id
JOIN sellables s ON s.id = o.sellable_id
LEFT JOIN video_game_titles vgt ON vgt.id = s.software_title_id
JOIN currencies c ON c.id = oj.currency_id
ORDER BY ABS(v.change_abs) DESC NULLS LAST
LIMIT 100;

-- =============================
-- CRON REFRESH SCHEDULES
-- =============================
DO $$ BEGIN
  PERFORM cron.schedule('refresh_mv_price_daily_hourly', '10 * * * *', 'SELECT refresh_mv_price_daily(true);');
EXCEPTION WHEN OTHERS THEN NULL; END $$;
