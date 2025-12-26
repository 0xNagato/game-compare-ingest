# Performance, Indexing & Analytics Guide (Concurrent-Safe)

This document summarizes how GameCompare maintains high write throughput and safe concurrent index creation without violating PostgreSQL transaction boundaries.

## Core Patterns

1. **Parent-only indexes** on `prices` ensure per-partition cloning:
   - `(offer_jurisdiction_id, recorded_at)`
   - Optional BRIN on `recorded_at` for large historical partitions (added later via async jobs).
2. **Monthly partition precreation** via pg_cron ensures ingest never blocks on DDL:
   - Job: `partition_monthly_precreate` (see migration `0025_*`).
3. **One-off concurrent index builds** are scheduled as independent cron jobs (not inside plpgsql transaction):
   - Worker enqueues build jobs only when a partition is missing an index.
   - Each scheduled cron command runs `CREATE INDEX CONCURRENTLY` in its own transaction context.
4. **Cleanup job** (`partition_index_cleanup_hourly`) unschedules completed build jobs after verifying index existence.

## Why Not CREATE INDEX CONCURRENTLY in Functions?
PostgreSQL disallows running `CREATE INDEX CONCURRENTLY` inside an open transaction block. Direct execution from a PL/pgSQL function implicitly wraps statements in a transaction. Scheduling build commands via pg_cron isolates each `CREATE INDEX CONCURRENTLY` in its own top-level session.

## Verification Steps

Run the provided EXPLAIN queries in `docs/perf_query_pack.sql` after seeding:

- Single-month pruning (should show only one partition in the plan).
- Multi-month range with partition pruning.
- Current price fast path (no partition access).
- Alerts partial index usage.

## Adding New Partitions

To pre-create a future partition manually:

```sql
SELECT ensure_price_partition(date_trunc('month', now()) + interval '7 months');
```
Indexes will be scheduled automatically if missing.

## Backfills

For historical backfill:

 
1. Precreate partitions.
2. Bulk load via `COPY` into `prices`.
3. Run `ANALYZE` on affected partitions.
4. Confirm index presence; if missing, worker schedules build job.

## Monitoring

- Check build jobs: `SELECT jobname, schedule FROM cron.job WHERE jobname LIKE 'build_%';`
- Refresh statuses: `SELECT partition_util.refresh_index_job_status();`
- Cleanup: automatic hourly or manual trigger: `SELECT partition_util.cleanup_index_cron_jobs();`

## Alert Path

`current_price` powers real-time checks. Partial indexes on `alerts` (`WHERE active`) ensure threshold scans are tight.

## Extensions

Ensure extensions exist (idempotent in migrations):

```sql
CREATE EXTENSION IF NOT EXISTS citext;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
```

## When to Add BRIN

Add BRIN indexes for older partitions (cold historical queries):

```sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS prices_2024_08_recorded_brin ON public.prices_2024_08 USING brin(recorded_at);
```
Scheduled similarly via cron for large ranges.

## Troubleshooting

- Missing index after scheduling: verify job executed via `SELECT * FROM cron.job_run_details ORDER BY start_time DESC LIMIT 10;`
- Partition absent: ensure monthly job ran; run manual `ensure_price_partition(ts)`.
- Slow pruning: confirm statistics are fresh (`ANALYZE partition_name`).

## Next Enhancements

- Add duration telemetry per index job.
- Materialized 30d stats view refreshed nightly.
- Optional compression/archival for very old partitions.

---
## Analytics Layer (Materialized & Derived Views)

Recent additions introduce a lightweight analytical surface optimized for dashboard read paths without touching hot ingest partitions directly.

### Objects

1. `mv_price_daily` (MATERIALIZED VIEW)
   - Columns: `day`, `offer_jurisdiction_id`, `min_amount_minor`, `max_amount_minor`, `median_amount_minor`, `samples`.
   - Sources: Aggregates from `prices` across all partitions (partition pruning engages via `recorded_at` constraint inside the view’s definition using `date_trunc`).
   - Index: `mv_price_daily_pk` unique btree `(day, offer_jurisdiction_id)` enabling concurrent refresh & point lookups.
2. `vw_price_change_7d` (VIEW)
   - Computes absolute and ratio change between latest `current_price.amount_minor` and most recent price within the last 7 days from `prices`.
   - Logic: Uses window row_number() for past sample selection; left lateral join ensures presence even if no past data → NULL change fields.
3. `vw_top_price_movers_7d` (VIEW)
   - Joins `vw_price_change_7d` to `offer_jurisdictions`, `offers`, `sellables`, `video_game_titles`, `currencies` for display attributes & limits top 100 movers by absolute change.

### Refresh Strategy

`mv_price_daily` is refreshed hourly via pg_cron (`refresh_mv_price_daily_hourly` scheduled at minute 10). Function `refresh_mv_price_daily(concurrent boolean)` attempts `REFRESH MATERIALIZED VIEW CONCURRENTLY` first, falling back to non-concurrent if index invalidation / race prevents concurrent usage.

For immediate manual refresh (e.g., after large backfill):

```sql
SELECT refresh_mv_price_daily(true);  -- prefer concurrent
```

If partition bulk loads cause transient conflicts, rerun with `false`:

```sql
SELECT refresh_mv_price_daily(false);
```

### Usage Patterns

**Daily range query (charting):**
```sql
SELECT day, median_amount_minor
FROM mv_price_daily
WHERE offer_jurisdiction_id = $1
  AND day BETWEEN $2::date AND $3::date
ORDER BY day;
```

**Top movers dashboard:**
```sql
SELECT title, currency_code, change_abs, change_ratio
FROM vw_top_price_movers_7d;
```

**7d change for a specific offer jurisdiction:**
```sql
SELECT change_abs, change_ratio
FROM vw_price_change_7d
WHERE offer_jurisdiction_id = $1;
```

### Performance Considerations

* `mv_price_daily` avoids per-request aggregation over large partitions; median via `percentile_cont` executed during refresh, not query time.
* Hourly refresh aligns with pricing cadence; increase frequency if near-real-time charts required.
* `vw_price_change_7d` leverages `current_price` for latest snapshot → avoids scanning newest partition.
* Limit clause in `vw_top_price_movers_7d` prevents runaway sorts; adjust if more entries needed with pagination.

### Extension & Concurrency Notes

Concurrent refresh depends on a valid unique index covering all rows—ensure `mv_price_daily_pk` remains intact. If schema changes require dropping the index, drop the cron job first to avoid refresh failures, then recreate both index and job.

### Future Enhancements

* Nightly 30d rolling stats MV (median, p90, volatility coefficients).
* Materialized distribution snapshots per platform & region.
* Optional compression / partition detach for legacy price data feeding long-horizon analytics MVs.
* JSONB aggregation (counts by discount buckets) integrated into `mv_price_daily` after evaluating storage vs benefit.

### Operational Checks

| Check | Query |
|-------|-------|
| MV exists | `SELECT 1 FROM pg_class WHERE relname='mv_price_daily';` |
| Cron job | `SELECT * FROM cron.job WHERE jobname='refresh_mv_price_daily_hourly';` |
| Last refresh time (approx) | `SELECT max(day) FROM mv_price_daily;` |
| Row count sample | `SELECT count(*) FROM mv_price_daily;` |

---
Last updated: 2025-11-12
