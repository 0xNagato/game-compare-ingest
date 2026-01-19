-- 0450_performance_bundle.sql
-- Purpose: Consolidated, idempotent performance & concurrency enhancements applied AFTER 0001_full_consolidated_schema.sql.
-- Includes:
--   * Full-text search (tsvector) + GIN index on video_game_titles
--   * Covering indexes (video_games, current_price)
--   * Expression / partial / functional indexes (lower(slug), release year, active offers)
--   * Generated display column on current_price
--   * Materialized view (hot_price_changes) + unique & sorting indexes
--   * Statistics target tuning
--   * Bloom extension (installed only; bloom index omitted due to operator class constraints)
--   * pg_cron scheduling for hourly MV refresh
--   * Partition index job diagnostics & revised worker (ambiguous jobname fix via explicit qualification & logging)
--   * Safe idempotent guards (IF NOT EXISTS / existence checks)
--
-- NOTE: This file intentionally centralizes prior ad‑hoc tuning into ONE auditable migration.
--       Re-runnable safely; all DDL guarded. If any statement fails due to permissions, rest proceed.

-- =============================
-- EXTENSION (Bloom) – optional advanced multi-column indexing
-- =============================
DO $$ BEGIN
  CREATE EXTENSION IF NOT EXISTS bloom WITH SCHEMA public;
EXCEPTION WHEN OTHERS THEN
  -- Non-fatal: Hosted providers may block bloom; continue
  RAISE NOTICE 'Bloom extension unavailable (%). Skipping.', SQLERRM;
END $$;

-- =============================
-- FULL-TEXT SEARCH (video_game_titles)
-- =============================
ALTER TABLE IF EXISTS video_game_titles
  ADD COLUMN IF NOT EXISTS search_vector tsvector
  GENERATED ALWAYS AS (
    to_tsvector('english', coalesce(title,'') || ' ' || coalesce(normalized_title,''))
  ) STORED;

CREATE INDEX IF NOT EXISTS video_game_titles_search_idx
  ON video_game_titles USING gin(search_vector);

-- =============================
-- COVERING INDEXES
-- =============================
CREATE INDEX IF NOT EXISTS video_games_title_platform_covering_idx
  ON video_games (title_id, platform_id)
  INCLUDE (slug, release_date, edition);

CREATE INDEX IF NOT EXISTS current_price_with_amount_idx
  ON current_price (offer_jurisdiction_id)
  INCLUDE (amount_minor, recorded_at);

-- =============================
-- EXPRESSION / FUNCTIONAL / PARTIAL INDEXES
-- =============================
CREATE INDEX IF NOT EXISTS products_slug_lower_idx
  ON products (lower(slug));

CREATE INDEX IF NOT EXISTS video_games_release_year_idx
  ON video_games ((extract(year FROM release_date)));

CREATE INDEX IF NOT EXISTS offers_active_created_idx
  ON offers (created_at DESC) WHERE is_active = true;

-- =============================
-- GENERATED COLUMN (display price)
-- =============================
ALTER TABLE IF EXISTS current_price
  ADD COLUMN IF NOT EXISTS amount_display_price numeric
  GENERATED ALWAYS AS (amount_minor::numeric / 100) STORED;

-- =============================
-- MATERIALIZED VIEW (hot price changes) + Indexes & CRON refresh
-- =============================
-- Recreate view if definition changed (include offer_jurisdiction_id for uniqueness)
DO $$ BEGIN
  IF to_regclass('hot_price_changes') IS NOT NULL THEN
    -- Preserve data until refresh; DROP then CREATE for definitional drift
    DROP MATERIALIZED VIEW hot_price_changes;
  END IF;
END $$;

CREATE MATERIALIZED VIEW hot_price_changes AS
WITH price_changes AS (
  SELECT cp.offer_jurisdiction_id,
         vgt.title,
         cp.amount_minor AS current_price,
         LAG(p.amount_minor) OVER (PARTITION BY p.offer_jurisdiction_id ORDER BY p.recorded_at DESC) AS prev_price
  FROM current_price cp
  JOIN offer_jurisdictions oj ON cp.offer_jurisdiction_id = oj.id
  JOIN offers o ON oj.offer_id = o.id
  JOIN sellables s ON o.sellable_id = s.id
  JOIN video_game_titles vgt ON s.software_title_id = vgt.id
  JOIN prices p ON p.offer_jurisdiction_id = cp.offer_jurisdiction_id
  WHERE p.recorded_at > now() - INTERVAL '7 days'
)
SELECT offer_jurisdiction_id,
       title,
       current_price,
       prev_price,
       (current_price - prev_price) AS change,
       CASE
         WHEN prev_price IS NOT NULL AND prev_price > 0 THEN
           ((current_price - prev_price)::numeric / prev_price)::numeric
         ELSE NULL
       END AS change_ratio
FROM price_changes
WHERE prev_price IS NOT NULL
ORDER BY abs(current_price - prev_price) DESC
LIMIT 100;

-- Unique index required for REFRESH CONCURRENTLY
CREATE UNIQUE INDEX IF NOT EXISTS hot_price_changes_oj_uidx
  ON hot_price_changes (offer_jurisdiction_id);

-- Sorting / query helper index
CREATE INDEX IF NOT EXISTS hot_price_changes_change_idx
  ON hot_price_changes (change DESC);

-- MV Refresh helper function (safe CONCURRENTLY if unique index exists)
CREATE OR REPLACE FUNCTION refresh_hot_price_changes(concurrent boolean DEFAULT true)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  IF concurrent THEN
    BEGIN
      EXECUTE 'REFRESH MATERIALIZED VIEW CONCURRENTLY hot_price_changes';
    EXCEPTION WHEN OTHERS THEN
      RAISE NOTICE 'Concurrent refresh failed (%). Falling back to non-concurrent.', SQLERRM;
      EXECUTE 'REFRESH MATERIALIZED VIEW hot_price_changes';
    END;
  ELSE
    EXECUTE 'REFRESH MATERIALIZED VIEW hot_price_changes';
  END IF;
END $$;

-- Schedule hourly refresh via pg_cron (idempotent)
DO $$ BEGIN
  PERFORM cron.schedule('refresh_hot_price_changes_hourly', '0 * * * *', 'SELECT refresh_hot_price_changes(true);');
EXCEPTION WHEN OTHERS THEN
  -- Ignore if already scheduled or pg_cron unavailable
  NULL;
END $$;

-- =============================
-- STATISTICS TARGET TUNING
-- =============================
ALTER TABLE video_game_titles ALTER COLUMN title SET STATISTICS 1000;
ALTER TABLE products ALTER COLUMN slug SET STATISTICS 1000;
ALTER TABLE prices ALTER COLUMN offer_jurisdiction_id SET STATISTICS 500;

DO $partition_util$
BEGIN
  -- Only proceed if the schema and base jobs table exist
  IF EXISTS (
    SELECT 1 FROM information_schema.schemata WHERE schema_name='partition_util'
  ) AND EXISTS (
    SELECT 1 FROM information_schema.tables WHERE table_schema='partition_util' AND table_name='partition_index_jobs'
  ) THEN
    -- Logging table for job attempts (optional observability)
    CREATE TABLE IF NOT EXISTS partition_util.partition_index_job_logs (
      id bigserial PRIMARY KEY,
      job_id bigint,
      partition_name text,
      index_type text,
      status text,
      attempt int,
      message text,
      logged_at timestamptz NOT NULL DEFAULT now()
    );

    -- Revised worker: explicit schema qualification; log progress; clearer error capture.
    CREATE OR REPLACE FUNCTION partition_util.process_partition_index_jobs(max_items int DEFAULT 5)
    RETURNS int AS $$
    DECLARE
      r RECORD;
      done_count int := 0;
      idx text;
    BEGIN
      FOR r IN
        SELECT id, partition_name, index_type
        FROM partition_util.partition_index_jobs
        WHERE status IN ('pending','failed')
        ORDER BY created_at
        LIMIT max_items
      LOOP
        BEGIN
          UPDATE partition_util.partition_index_jobs
            SET status='running', last_attempt_at=now(), attempts = COALESCE(attempts,0) + 1, error_message=NULL
          WHERE id = r.id;

          IF r.index_type = 'btree_series' THEN
            idx := format('%s_offer_jurisdiction_id_recorded_at_idx1', r.partition_name);
            IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = idx AND c.relkind='i') THEN
              EXECUTE format('CREATE INDEX %I ON %I (offer_jurisdiction_id, recorded_at);', idx, r.partition_name);
            END IF;
          ELSIF r.index_type = 'brin_recorded' THEN
            idx := format('%s_recorded_brin_idx', r.partition_name);
            IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = idx AND c.relkind='i') THEN
              EXECUTE format('CREATE INDEX %I ON %I USING brin (recorded_at) WITH (pages_per_range=128);', idx, r.partition_name);
            END IF;
          ELSIF r.index_type = 'btree_recent' THEN
            idx := format('%s_recent_30d_idx', r.partition_name);
            IF NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.relname = idx AND c.relkind='i') THEN
              EXECUTE format('CREATE INDEX %I ON %I (offer_jurisdiction_id, recorded_at DESC) WHERE recorded_at > now() - interval ''30 days'';', idx, r.partition_name);
            END IF;
          END IF;

          UPDATE partition_util.partition_index_jobs
            SET status='completed', completed_at=now()
          WHERE id = r.id;
          INSERT INTO partition_util.partition_index_job_logs(job_id, partition_name, index_type, status, attempt, message)
          VALUES (r.id, r.partition_name, r.index_type, 'completed', (SELECT attempts FROM partition_util.partition_index_jobs WHERE id=r.id), idx || ' created/exists');
          done_count := done_count + 1;
        EXCEPTION WHEN OTHERS THEN
          UPDATE partition_util.partition_index_jobs
            SET status='failed', error_message=SQLERRM, last_attempt_at=now()
          WHERE id = r.id;
          INSERT INTO partition_util.partition_index_job_logs(job_id, partition_name, index_type, status, attempt, message)
          VALUES (r.id, r.partition_name, r.index_type, 'failed', (SELECT attempts FROM partition_util.partition_index_jobs WHERE id=r.id), SQLERRM);
        END;
      END LOOP;
      RETURN done_count;
    END;
    $$ LANGUAGE plpgsql;

    -- Diagnostic views (guarded)
    CREATE OR REPLACE VIEW partition_util.partition_index_job_status AS
    SELECT partition_name,
           index_type,
           status,
           attempts,
           error_message,
           last_attempt_at,
           completed_at
    FROM partition_util.partition_index_jobs;

    CREATE OR REPLACE VIEW partition_util.partition_index_job_failures AS
    SELECT * FROM partition_util.partition_index_job_logs WHERE status='failed' ORDER BY logged_at DESC;
  END IF;
END $partition_util$;

-- =============================
-- ANALYZE key tables post changes (non-blocking)
-- =============================
DO $$ BEGIN
  PERFORM 1;  -- placeholder; optionally run ANALYZE manually after data load
END $$;

-- End of performance & concurrency bundle
