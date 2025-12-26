-- =========================================================
-- DATABASE-LEVEL PERFORMANCE SETTINGS
-- =========================================================
-- Run these ONCE after creating the database
-- These settings enable parallel query processing
-- =========================================================

-- PERFORMANCE TRICK: Parallel Query Tuning (4x faster aggregations)
-- =========================================================
-- Enable parallel workers (adjust based on CPU cores)
-- Recommendation: max_parallel_workers = (CPU cores - 1)
alter database postgres set max_parallel_workers = 8;
alter database postgres set max_parallel_workers_per_gather = 4;
alter database postgres set parallel_tuple_cost = 0.01;
alter database postgres set parallel_setup_cost = 100;

-- Lower thresholds to trigger parallel execution
alter database postgres set min_parallel_table_scan_size = '8MB';
alter database postgres set min_parallel_index_scan_size = '512kB';

-- PERFORMANCE TRICK: Query Planner Tuning
-- =========================================================
-- Bias toward index scans (we have good indexes)
alter database postgres set random_page_cost = 1.1;  -- SSD-optimized (default 4.0)
alter database postgres set effective_cache_size = '4GB';  -- Adjust to your RAM

-- Better statistics for partition pruning
alter database postgres set constraint_exclusion = 'partition';

-- PERFORMANCE TRICK: Connection Pooling
-- =========================================================
-- Already handled by your Rust sqlx::PgPool
-- Keep pool size modest: (CPU cores * 1.5) for mixed workload

-- PERFORMANCE TRICK: Maintenance Settings
-- =========================================================
-- Aggressive autovacuum for hot tables (prevents bloat)
-- Note: These are table-specific, not database-level
-- Run after schema creation:
/*
alter table prices set (
  autovacuum_vacuum_scale_factor = 0.01,
  autovacuum_analyze_scale_factor = 0.01,
  fillfactor = 90
);

alter table current_price set (
  autovacuum_vacuum_scale_factor = 0.02,
  fillfactor = 100  -- Read-heavy, pack tight
);

alter table offers set (
  autovacuum_vacuum_scale_factor = 0.05,
  fillfactor = 95
);
*/

-- =========================================================
-- VERIFICATION QUERIES
-- =========================================================
-- Check if parallel workers are enabled:
-- show max_parallel_workers_per_gather;

-- Test parallel execution on large table:
-- explain (analyze, buffers) 
-- select count(*) from prices_2025_11 
-- where recorded_at > now() - interval '30 days';
-- Look for "Parallel Seq Scan" or "Parallel Index Scan" in output

-- Check index usage:
-- select schemaname, tablename, indexname, idx_scan, idx_tup_read
-- from pg_stat_user_indexes
-- where schemaname = 'public'
-- order by idx_scan desc;

-- Check table sizes:
-- select schemaname, tablename, 
--        pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as total_size,
--        pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) as table_size,
--        pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename) - pg_relation_size(schemaname||'.'||tablename)) as indexes_size
-- from pg_tables
-- where schemaname = 'public'
-- order by pg_total_relation_size(schemaname||'.'||tablename) desc;
