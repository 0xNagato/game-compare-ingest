-- =========================================================
-- DATABASE-LEVEL PERFORMANCE SETTINGS (REFERENCE ONLY)
-- =========================================================
-- This file was moved out of the migrations directory to avoid
-- sqlx::migrate! compile-time errors (requires numeric prefixes).
-- Keep these ALTER DATABASE statements manual/optional – applying
-- them automatically can fail on hosted Postgres (permission issues)
-- or conflict with provider-managed settings (e.g. Supabase, Neon).
-- =========================================================

-- Suggested parallel + planner tuning (apply manually if allowed):
-- alter database postgres set max_parallel_workers = 8;
-- alter database postgres set max_parallel_workers_per_gather = 4;
-- alter database postgres set parallel_tuple_cost = 0.01;
-- alter database postgres set parallel_setup_cost = 100;
-- alter database postgres set min_parallel_table_scan_size = '8MB';
-- alter database postgres set min_parallel_index_scan_size = '512kB';
-- alter database postgres set random_page_cost = 1.1;  -- SSD tweak
-- alter database postgres set effective_cache_size = '4GB';
-- alter database postgres set constraint_exclusion = 'partition';

-- Table-level maintenance hints (apply after table creation as needed):
/*
alter table prices set (
  autovacuum_vacuum_scale_factor = 0.01,
  autovacuum_analyze_scale_factor = 0.01,
  fillfactor = 90
);

alter table current_price set (
  autovacuum_vacuum_scale_factor = 0.02,
  fillfactor = 100
);

alter table offers set (
  autovacuum_vacuum_scale_factor = 0.05,
  fillfactor = 95
);
*/

-- Verification queries (run manually):
-- show max_parallel_workers_per_gather;
-- explain (analyze, buffers) select count(*) from prices_2025_11 where recorded_at > now() - interval '30 days';
-- select schemaname, tablename, indexname, idx_scan from pg_stat_user_indexes order by idx_scan desc;
-- select schemaname, tablename, pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as total_size from pg_tables where schemaname = 'public' order by pg_total_relation_size(schemaname||'.'||tablename) desc;
