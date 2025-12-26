oh-- =========================================================
-- PRODUCTION INDEX ADDITIONS (Zero-Downtime)
-- =========================================================
-- Use these commands when adding indexes to live tables
-- Run OUTSIDE of transactions (each statement separately)
-- =========================================================

-- =========================================================
-- PRICES PARTITIONS (Critical - run per partition)
-- =========================================================
-- Replace YYYY_MM with actual partition names
-- Run during low-traffic periods if possible

-- Time-series index (critical for queries)
create index concurrently if not exists prices_YYYY_MM_series_idx 
  on prices_YYYY_MM (offer_jurisdiction_id, recorded_at);

-- Recent prices index (hot queries)
create index concurrently if not exists prices_YYYY_MM_recent_idx 
  on prices_YYYY_MM (offer_jurisdiction_id, recorded_at desc)
  where recorded_at > now() - interval '30 days';

-- =========================================================
-- SEARCH INDEXES (Heavy - run during maintenance)
-- =========================================================
-- GIN indexes are slow to build, use CONCURRENTLY

-- Products search
create index concurrently if not exists products_slug_trgm_idx 
  on products using gin (slug gin_trgm_ops);

create index concurrently if not exists products_name_trgm_idx 
  on products using gin (name gin_trgm_ops);

-- Video game titles search
create index concurrently if not exists titles_title_trgm_idx 
  on video_game_titles using gin (title gin_trgm_ops);

-- Console search
create index concurrently if not exists consoles_model_trgm_idx 
  on game_consoles using gin (model gin_trgm_ops);

-- =========================================================
-- MONITORING
-- =========================================================
-- Check index creation progress:
-- SELECT now() - query_start AS duration,
--        pid, state, query
-- FROM pg_stat_activity
-- WHERE query ILIKE '%create index%';

-- Check if index is valid:
-- SELECT schemaname, tablename, indexname, 
--        indexdef, pg_relation_size(indexrelid) as size_bytes
-- FROM pg_indexes
-- JOIN pg_class ON indexname = relname
-- WHERE tablename IN ('products', 'video_game_titles', 'prices_2025_11')
-- ORDER BY tablename, indexname;

-- =========================================================
-- CLEANUP INVALID INDEXES (if CONCURRENTLY fails)
-- =========================================================
-- Find invalid indexes:
-- SELECT schemaname, tablename, indexname
-- FROM pg_indexes
-- JOIN pg_class ON indexname = relname
-- JOIN pg_index ON indexrelid = pg_class.oid
-- WHERE NOT indisvalid;

-- Drop invalid index:
-- DROP INDEX CONCURRENTLY invalid_index_name;

-- =========================================================
-- SCHEDULING STRATEGY
-- =========================================================
-- 1. Small tables (<10k rows): Normal index creation is fine
-- 2. Medium tables (10k-1M rows): CONCURRENTLY during off-peak
-- 3. Large tables (>1M rows): CONCURRENTLY, monitor duration
-- 4. Prices partitions: Always CONCURRENTLY, per-partition
-- 5. Test on staging first with production data volume
