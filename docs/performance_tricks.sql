-- =========================================================
-- PERFORMANCE OPTIMIZATION TRICKS
-- =========================================================
-- High-impact, low-complexity wins for game pricing platform
-- =========================================================

-- =========================================================
-- 1. FULL-TEXT SEARCH (tsvector) - HUGE WIN for natural language
-- =========================================================
-- Problem: User searches "zelda breath wild" → pg_trgm is slow
-- Solution: Postgres full-text search with ranking

-- Add tsvector column to video_game_titles
alter table video_game_titles 
  add column search_vector tsvector 
  generated always as (
    to_tsvector('english', coalesce(title, '') || ' ' || coalesce(normalized_title, ''))
  ) stored;

-- GIN index on tsvector (MUCH faster than trgm for phrases)
create index concurrently video_game_titles_search_idx 
  on video_game_titles using gin(search_vector);

-- Example query (10-100x faster than ILIKE '%zelda%'):
-- SELECT title, ts_rank(search_vector, query) AS rank
-- FROM video_game_titles, websearch_to_tsquery('english', 'zelda breath wild') query
-- WHERE search_vector @@ query
-- ORDER BY rank DESC
-- LIMIT 20;

-- =========================================================
-- 2. COVERING INDEXES - Eliminate table lookups
-- =========================================================
-- Problem: Query needs id + title but index only has title → extra lookup
-- Solution: Include commonly-fetched columns in index

-- Cover the "search games, show title + platform" query
create index concurrently video_games_title_platform_covering_idx 
  on video_games (title_id, platform_id) 
  include (slug, release_date, edition);

-- Cover the "latest price with currency" query
create index concurrently current_price_with_amount_idx 
  on current_price (offer_jurisdiction_id) 
  include (amount_minor, recorded_at);

-- Now these queries never hit the table, only the index:
-- SELECT slug, release_date FROM video_games WHERE title_id = 123;
-- SELECT amount_minor FROM current_price WHERE offer_jurisdiction_id = 456;

-- =========================================================
-- 3. BRIN INDEXES - Compress time-series indexes
-- =========================================================
-- Problem: B-tree indexes on prices.recorded_at are HUGE (gigabytes)
-- Solution: BRIN (Block Range Index) for time-ordered data

-- BRIN on recorded_at (1000x smaller than B-tree, almost as fast for ranges)
create index concurrently prices_2025_11_recorded_brin_idx 
  on prices_2025_11 using brin(recorded_at) 
  with (pages_per_range = 128);

-- Still use B-tree for precise lookups, BRIN for "last 7 days" queries
-- Savings: 500MB B-tree → 500KB BRIN (1000x smaller)

-- =========================================================
-- 4. EXPRESSION INDEXES - Index computed values
-- =========================================================
-- Problem: Queries filter on LOWER(slug) but slug is citext
-- Solution: Index the expression directly

-- Index lowercased slug (if you query with LOWER() a lot)
create index concurrently products_slug_lower_idx 
  on products (lower(slug));

-- Index year from release_date (for "games from 2024" queries)
create index concurrently video_games_release_year_idx 
  on video_games (extract(year from release_date));

-- =========================================================
-- 5. PARTIAL INDEXES (more aggressive)
-- =========================================================
-- Problem: Index entire table but only query recent/active rows
-- Solution: More partial indexes

-- Only index active offers (90% of offers are active)
create index concurrently offers_active_created_idx 
  on offers (created_at desc) 
  where is_active = true;

-- Only index recent prices (queries rarely look at old data)
create index concurrently prices_2025_11_recent_week_idx 
  on prices_2025_11 (offer_jurisdiction_id, recorded_at desc)
  where recorded_at > now() - interval '7 days';

-- =========================================================
-- 6. PARALLEL QUERY TUNING - Use all CPU cores
-- =========================================================
-- Problem: Postgres uses 1 worker for big queries
-- Solution: Tune parallel query settings (per session or database)

-- Enable parallel queries (adjust based on CPU cores)
alter database postgres set max_parallel_workers_per_gather = 4;
alter database postgres set max_parallel_workers = 8;
alter database postgres set parallel_tuple_cost = 0.01;

-- Test with EXPLAIN (ANALYZE, BUFFERS):
-- EXPLAIN (ANALYZE, BUFFERS) 
-- SELECT * FROM prices_2025_11 
-- WHERE recorded_at > now() - interval '30 days';
-- Look for "Parallel Seq Scan" in output

-- =========================================================
-- 7. STATISTICS TARGETS - Better query planning
-- =========================================================
-- Problem: Planner makes bad choices on skewed columns
-- Solution: Increase statistics sample size

-- Increase stats for high-cardinality columns (1000s of unique values)
alter table video_game_titles 
  alter column title set statistics 1000;

alter table products 
  alter column slug set statistics 1000;

alter table prices 
  alter column offer_jurisdiction_id set statistics 500;

-- Then run: ANALYZE video_game_titles;

-- =========================================================
-- 8. MATERIALIZED VIEWS - Pre-compute hot queries
-- =========================================================
-- Problem: "Top 10 games by price change this week" is slow
-- Solution: Materialized view (refresh hourly)

create materialized view if not exists hot_price_changes as
select 
  vgt.title,
  cp.amount_minor as current_price,
  lag(p.amount_minor) over (partition by p.offer_jurisdiction_id order by p.recorded_at desc) as prev_price,
  cp.amount_minor - lag(p.amount_minor) over (partition by p.offer_jurisdiction_id order by p.recorded_at desc) as change
from current_price cp
join offer_jurisdictions oj on cp.offer_jurisdiction_id = oj.id
join offers o on oj.offer_id = o.id
join sellables s on o.sellable_id = s.id
join video_game_titles vgt on s.software_title_id = vgt.id
join prices p on p.offer_jurisdiction_id = cp.offer_jurisdiction_id
where p.recorded_at > now() - interval '7 days'
order by abs(change) desc
limit 100;

-- Index the materialized view
create index on hot_price_changes (change desc);

-- Refresh hourly via cron:
-- REFRESH MATERIALIZED VIEW CONCURRENTLY hot_price_changes;

-- =========================================================
-- 9. GENERATED COLUMNS - Auto-compute derived values
-- =========================================================
-- Problem: App computes normalized_title every time
-- Solution: Let Postgres compute + index it

-- Already have normalized_title, make it generated
-- alter table video_game_titles 
--   alter column normalized_title 
--   set generated always as (lower(regexp_replace(title, '[^a-zA-Z0-9]+', '', 'g'))) stored;

-- Add price change percent (stored, indexed)
alter table current_price 
  add column amount_display_price numeric 
  generated always as (amount_minor::numeric / 100) stored;

-- =========================================================
-- 10. CONNECTION POOLING - Reduce latency
-- =========================================================
-- Problem: Establishing connections is slow (50-100ms)
-- Solution: PgBouncer or connection pooling (you already have this via Supabase!)

-- Your Rust code already uses PgPool (good!)
-- Tune pool size based on workload:
-- - Read-heavy: pool_size = (CPU cores * 2) + spindles
-- - Write-heavy: pool_size = CPU cores
-- - Mixed: pool_size = CPU cores * 1.5

-- =========================================================
-- 11. BLOOM FILTERS - Multi-column lookups
-- =========================================================
-- Extension for efficient multi-column WHERE clauses
create extension if not exists bloom;

-- Use when querying many columns with AND
create index concurrently video_games_multi_filter_idx 
  on video_games using bloom(platform_id, release_date, edition)
  with (length=80, col1=2, col2=2, col3=2);

-- Good for: WHERE platform_id = X AND release_date = Y AND edition = Z
-- Smaller + faster than composite B-tree for this pattern

-- =========================================================
-- 12. TABLE SETTINGS - Optimize for workload
-- =========================================================
-- For write-heavy tables (prices), reduce fillfactor
alter table prices set (fillfactor = 90);  -- Leave 10% free for HOT updates

-- For read-only lookups (platforms, currencies)
alter table platforms set (fillfactor = 100);  -- Pack tight, no updates expected

-- Autovacuum tuning for hot tables
alter table prices set (
  autovacuum_vacuum_scale_factor = 0.01,  -- Vacuum at 1% dead rows (default 20%)
  autovacuum_analyze_scale_factor = 0.01  -- Analyze more frequently
);

-- =========================================================
-- MONITORING QUERIES
-- =========================================================

-- Show index usage (find unused indexes)
-- SELECT schemaname, tablename, indexname, idx_scan, idx_tup_read, idx_tup_fetch
-- FROM pg_stat_user_indexes
-- WHERE idx_scan = 0
-- ORDER BY pg_relation_size(indexrelid) DESC;

-- Show slow queries (enable pg_stat_statements extension)
-- SELECT query, calls, mean_exec_time, total_exec_time
-- FROM pg_stat_statements
-- ORDER BY mean_exec_time DESC
-- LIMIT 20;

-- Show table bloat
-- SELECT schemaname, tablename, 
--        pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
-- FROM pg_tables
-- WHERE schemaname = 'public'
-- ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- =========================================================
-- TESTING STRATEGY
-- =========================================================
-- 1. BEFORE: Run EXPLAIN (ANALYZE, BUFFERS) on slow query
-- 2. Apply ONE trick from above
-- 3. AFTER: Run same EXPLAIN, compare:
--    - Execution time (look for 2-10x improvement)
--    - Buffers (should decrease)
--    - Index usage (should prefer new index)
-- 4. If no improvement, DROP the index (don't waste space)

-- =========================================================
-- RECOMMENDED ORDER OF IMPLEMENTATION
-- =========================================================
-- For your game pricing platform, implement in this order:
-- 
-- 🥇 IMMEDIATE WINS (do today):
-- 1. tsvector full-text search on video_game_titles
-- 2. Covering index on current_price
-- 3. Parallel query tuning (just ALTER DATABASE settings)
--
-- 🥈 HIGH VALUE (do this week):
-- 4. BRIN indexes on prices partitions (save GBs)
-- 5. Statistics targets on title/slug columns
-- 6. Partial indexes on active offers
--
-- 🥉 ADVANCED (do when scaling):
-- 7. Materialized views for dashboards
-- 8. Bloom filters if complex multi-column queries
-- 9. Expression indexes for computed filters
-- 10. Table settings (fillfactor, autovacuum)
