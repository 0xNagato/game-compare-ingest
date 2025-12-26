# i-miss-rust — Global Games Pricing Platform

Fast, write-heavy, read-fast platform for ingesting and comparing game/software/hardware prices across providers and jurisdictions. Rust + PostgreSQL (Supabase compatible) with partitioned time-series, hot current_price table, and idempotent provider ingestors.

This README gives you copy-paste run commands, minimal setup, provider quickstarts, and ingestion/DB performance tips (BRIN, composite/partial indexes). For deeper DB design, see `README_DB.md` and `docs/`.

## Quickstart

### 0) Environment
Use the Supabase session pooler (5432) or any direct Postgres 15+ URL.

```zsh
# Prefer session pooler port 5432 for bulk ingest
export SUPABASE_DB_SESSION_URL="postgres://USER:PASSWORD@aws-1-eu-west-1.pooler.supabase.com:5432/postgres"
export DATABASE_URL="$SUPABASE_DB_SESSION_URL"

# Optional logging
export RUST_LOG="info,sqlx=warn"
```

### 1) Build (release)
```zsh
cargo build -r
```

### 2) Import legacy SQLite → Postgres (no auto-migrations)
The importer never runs migrations on connect. It applies SQLite read-optimized PRAGMAs by default; switch to safe mode if needed.
```zsh
# default (fast pragmas)
RUST_LOG="info" cargo run -r --bin import_sqlite database.sqlite

# safer pragmas if the SQLite DB may be used by another process
RUST_LOG="info" SQLITE_PERF_SAFE=1 cargo run -r --bin import_sqlite database.sqlite
```

### 3) Apply migrations (safe runner)
Run migrations after importing data (or anytime you need to catch up schema changes).
```zsh
RUST_LOG="info" cargo run -r --bin db_migrate
```

### 4) Sanity counts
```zsh
RUST_LOG="info" cargo run -r --bin db_counts
```

## Providers — unified ingest CLI

The new `ingest` binary is the front door for Phase 1 of the provider consolidation effort. It wires the existing PlayStation, Steam, Xbox, GiantBomb, Nexarda, IGDB, and RAWG pipelines without altering their core logic.

```zsh
# Run the default provider set (Nexarda → RAWG)
RUST_LOG="info" cargo run --bin ingest run

# Limit to specific providers (comma-delimited)
RUST_LOG="info" cargo run --bin ingest run --only playstation,steam

# Skip a provider while keeping order
RUST_LOG="info" cargo run --bin ingest run --skip rawg

# Inspect available provider keys
cargo run --bin ingest providers

# Suppress alert evaluation when running a dry ingest
cargo run --bin ingest run --no-alerts
```

Environment variables used by the legacy binaries continue to work because the CLI forwards execution into the same async functions (`psstore_seed_pipeline`, `SteamProvider::run_from_env`, `xbox::provider::run_from_env`, and so on). All retry/backoff behaviour, rate-limit handling, and data writes remain unchanged.

### Legacy command mapping

| Legacy command | Unified CLI equivalent |
| --------------- | ---------------------- |
| `cargo run --bin worker_manager` | `cargo run --bin ingest run` |
| `PS_STORE_REGIONS="…" PS_MAX_PAGES=… cargo run --bin ps_prices_ingest` | `PS_STORE_REGIONS="…" PS_MAX_PAGES=… cargo run --bin ingest run --only playstation` |
| `cargo run --bin steam_ingest_once` | `cargo run --bin ingest run --only steam` |
| `cargo run --bin xbox_ingest_once` | `cargo run --bin ingest run --only xbox` |
| `cargo run --bin nexarda_catalogue_ingest` | `cargo run --bin ingest run --only nexarda` |


> **Heads-up:** the legacy binaries remain in the tree during the migration window. They now print a reminder directing you to the unified CLI and will be converted into thin wrappers (Phase 4) before removal. Binaries not yet wired into the CLI (e.g., `ingest_itad`) will be migrated in subsequent phases.

The ingestion layer is idempotent—unique constraints on `provider_items`, `provider_offers`, `game_media`, and `offers` still guard duplicates. For large backfills, execute the CLI close to the database and keep `RUST_LOG` focused (e.g., `info,sqlx=warn`).

## Performance: ingestion and read-path indexes

Core table `prices` is partitioned by month on `recorded_at` with parent-only B-tree indexes on `(offer_jurisdiction_id, recorded_at)` for series queries. For massive historical backfills and range scans, BRIN can complement B-tree, especially on very large partitions.

### Composite B-tree (hot reads)
Already present in schema (parent-only so partitions inherit):
```sql
-- Efficient time-bounded series lookups by offer_jurisdiction_id
CREATE INDEX IF NOT EXISTS prices_series_idx ON ONLY public.prices (offer_jurisdiction_id, recorded_at);
```

### Optional: BRIN on recorded_at (range scans on large partitions)
Use BRIN per partition for very large monthly tables to accelerate wide time-range scans during backfills or analytics. Tune `pages_per_range` to your storage/page size and data density.
```sql
-- Example for a specific partition
CREATE INDEX IF NOT EXISTS prices_2025_11_brin_rec ON public.prices_2025_11
USING BRIN (recorded_at) WITH (pages_per_range = 64);

-- Summarize new values after big loads (cron or ad-hoc)
SELECT brin_summarize_new_values('public.prices_2025_11_brin_rec');
```

When to use BRIN
- Very large partitions (10M+ rows)
- Broad range scans (e.g., month+ spans) where B-tree may be heavy
- Append-only workloads (fits time-series writes)

### Partition-specific partial indexes (recent window)
Partial indexes with time predicates must use fixed bounds (immutable). Create per-partition partials when you often query a narrow, recent window within that partition.
```sql
-- Example: “recent” index for a single partition month
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_prices_2025_11_recent
ON public.prices_2025_11 (offer_jurisdiction_id, recorded_at)
WHERE recorded_at >= '2025-11-01' AND recorded_at < '2025-12-01';
```

### Current price (read-hot)
Reads for “latest” should join to `current_price` and never hit partitions.
```sql
-- Upserts are handled by ingestion code; read joins avoid scanning partitions
SELECT oj.id, cp.amount_minor, cp.recorded_at
FROM offer_jurisdictions oj
JOIN current_price cp ON cp.offer_jurisdiction_id = oj.id
WHERE oj.offer_id = $1;
```

### Maintenance
- Partition horizon: pre-create the next month before it starts.
- After large backfills: `ANALYZE public.prices_YYYY_MM;`
- Consider lower autovacuum scale factors per partition to keep stats fresh.

## End-to-end demo flow
```zsh
# 1) Build
cargo build -r

# 2) Import legacy data (strictly links media → video_games; idempotent)
RUST_LOG="info" cargo run -r --bin import_sqlite database.sqlite

# 3) Apply migrations safely
RUST_LOG="info" cargo run -r --bin db_migrate

# 4) Run providers (RAWG/TGDB via manager)
export RAWG_API_KEY="YOUR_RAWG_KEY"
export TGDB_API_KEY="YOUR_TGDB_KEY"
RUST_LOG="info" cargo run -r --bin worker_manager

# 5) PlayStation pricing (example region)
PS_STORE_REGIONS="en-us" PS_MAX_PAGES=1 PS_PAGE_SIZE=50 RUST_LOG="info" cargo run --bin ps_prices_ingest

# 6) Sanity counts
RUST_LOG="info" cargo run -r --bin db_counts
```

## References
- DB design and DDL: `README_DB.md`
- Performance notes: `docs/README_PERF.md`, `docs/ops_notes.md`, `docs/perf_query_pack.sql`
- Migrations: `migrations/`
- Schema performance analysis: `docs/SCHEMA_PERFORMANCE_ANALYSIS.md`

If you want a curated set of SQLs to pre-create partitions, tune autovacuum, or apply partition-specific partial indexes, see `docs/ops_notes.md` and `docs/README_PERF.md`.