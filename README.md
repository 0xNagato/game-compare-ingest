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

## Canonical traversal & source array maintenance

> **Non‑negotiable:** all catalog reads and writes must follow `products → video_game_titles → video_games`. Only `video_game_titles` may own a `product_id`, while every `video_game` references its title via `title_id`. The Rust helpers (`ensure_video_game_for_product_enhanced`, `ProviderEntityCache`) already enforce this, but contributors need to keep the invariant front‑of‑mind when crafting SQL, adding migrations, or debugging ingest pipelines.

### What this means in practice

1. **Never join `products` directly to `video_games`.** Route through `video_game_titles` (or the helper services) so the traversal stays explicit.
2. **`video_game_sources` must mirror their child IDs.** Each source row carries the `video_game_ids` JSON array that drives Laravel + Filament lookups. The triggers created in migrations `0529` and `0538` keep this field synchronized, but bad ad‑hoc updates can still desync it.
3. **Importer SQL must target titles.** If you are touching `src/bin/import_sqlite.rs` or provider‑specific statements, confirm that joins now pivot on `video_game_titles` and never rely on removed legacy columns (e.g., `video_games.product_id`).

### Verifying the invariant

```sql
-- Expect 0 rows. Any result means someone reintroduced product links directly on video_games.
SELECT column_name FROM information_schema.columns
WHERE table_name = 'video_games' AND column_name = 'product_id';

-- Sanity-check traversal fan-out counts
SELECT vgt.id, jsonb_array_length(vgt.video_game_ids) AS sku_count
FROM public.video_game_titles vgt
ORDER BY sku_count DESC
LIMIT 10;
```

### Rehydrating `video_game_ids` / `source_ids`

If you ever need to rebuild the materialized arrays after a manual data fix, run:

```sql
-- Rebuild for all titles (safe, idempotent)
SELECT public.video_game_titles_rebuild_video_game_ids(id)
FROM public.video_game_titles;

-- Rerun the trigger-driven sync for sources (only needed if rows were bulk-updated)
NOTIFY refresh_video_game_sources; -- optional app-side hook
```

Or, if you only touched a handful of IDs, call `video_game_titles_rebuild_video_game_ids(<title_id>)` for each. The helper function will also repopulate `video_game_sources.video_game_ids` via the `video_game_sources_sync_title_arrays_trg` trigger.

Keep these checks close when reviewing PRs—schema migrations `0529_enforce_products_titles_video_games.sql` and `0538_products_titles_video_games_bridge.sql` are the source of truth for the invariant and should be updated if new edge cases appear.

## Providers — unified ingest CLI

The new `ingest` binary is the front door for Phase 1 of the provider consolidation effort. It wires the existing PlayStation, Steam, Xbox, GiantBomb, Nexarda, IGDB, and RAWG pipelines without altering their core logic.

```zsh
# Run the default provider set (Nexarda → RAWG)
RUST_LOG="info" cargo run --bin ingest run

# Limit to specific providers (comma-delimited)
RUST_LOG="info" cargo run --bin ingest run --only playstation_store,steam_store

# Skip a provider while keeping order
RUST_LOG="info" cargo run --bin ingest run --skip rawg

# Inspect available provider keys
cargo run --bin ingest providers

# Suppress alert evaluation when running a dry ingest
cargo run --bin ingest run --no-alerts
```

### Provider naming (canonical IDs + compatibility)

This project currently touches **three different “provider identifier” namespaces**:

1. **Canonical provider ID** (recommended): snake_case, stable across Rust + Laravel + docs.
2. **Rust ingest CLI key**: what you pass to `--only/--skip` (now matches canonical IDs; legacy aliases are still accepted).
3. **Database `providers.slug`**: historically mixed (some use hyphens like `ps-store`). We treat this as an internal DB detail.

Canonical provider IDs (snake_case) are what we standardize on going forward.

| Canonical provider ID | Rust ingest CLI accepts                                   | Known DB `providers.slug` (if different) | Laravel pricing key | Laravel media key                                        |
| --------------------- | --------------------------------------------------------- | ---------------------------------------- | ------------------- | -------------------------------------------------------- |
| `nexarda`             | `nexarda`                                                 | (same)                                   | `nexarda`           | `nexarda`                                                |
| `itad`                | `itad`                                                    | (same)                                   | `itad`              | (n/a)                                                    |
| `steam_store`         | `steam_store`, `steam`                                    | `steam-store`                            | `steam_store`       | `steam` _(legacy key; should converge to `steam_store`)_ |
| `playstation_store`   | `playstation_store`, `playstation`, `psstore`, `ps_store` | `ps-store`                               | `playstation_store` | `playstation_store`                                      |
| `microsoft_store`     | `microsoft_store`, `xbox`, `microsoft`                    | `microsoft-store`                        | `microsoft_store`   | (n/a)                                                    |
| `thegamesdb`          | `thegamesdb`, `tgdb`                                      | (tbd)                                    | `thegamesdb`        | `thegamesdb`                                             |
| `igdb`                | `igdb`                                                    | (same)                                   | (n/a)               | (n/a)                                                    |
| `rawg`                | `rawg`                                                    | (same)                                   | (n/a)               | `rawg`                                                   |
| `giantbomb`           | `giantbomb`                                               | (same)                                   | (n/a)               | `giantbomb`                                              |

Laravel-only providers (not yet wired into the Rust ingest CLI):

- Pricing: `nintendo_eshop`, `pricecharting`, `ebay_browse`
- Media: `wikimedia_commons`

Environment variables used by the legacy binaries continue to work because the CLI forwards execution into the same async functions (`psstore_seed_pipeline`, `SteamProvider::run_from_env`, `xbox::provider::run_from_env`, and so on). All retry/backoff behaviour, rate-limit handling, and data writes remain unchanged.

### Legacy command mapping

| Legacy command                                                         | Unified CLI equivalent                                                                    |
| ---------------------------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| `cargo run --bin worker_manager`                                       | `cargo run --bin ingest run`                                                              |
| `PS_STORE_REGIONS="…" PS_MAX_PAGES=… cargo run --bin ps_prices_ingest` | `PS_STORE_REGIONS="…" PS_MAX_PAGES=… cargo run --bin ingest run --only playstation_store` |
| `cargo run --bin steam_ingest_once`                                    | `cargo run --bin ingest run --only steam_store`                                           |
| `cargo run --bin xbox_ingest_once`                                     | `cargo run --bin ingest run --only microsoft_store`                                       |
| `cargo run --bin nexarda_catalogue_ingest`                             | `cargo run --bin ingest run --only nexarda`                                               |

### Microsoft Store / Xbox Live auth (XSTS) runbook

The `microsoft_store` provider uses Xbox Live APIs that require an **XSTS** token.
This repo supports a headless-friendly device-code flow and persists refreshable tokens on disk.

#### Where tokens are stored

- Default: `.xbox_tokens.json` (in your current working directory when you run the binaries)
- Override: set `XBOX_TOKEN_FILE=/path/to/xbox_tokens.json`

#### 1) One-time setup (device code)

Run the setup binary and follow the printed instructions on _any_ device with a browser.
If the token store already exists, the binary exits successfully without prompting.

```zsh
RUST_LOG="info" cargo run --bin xbox_auth_setup
```

Force a re-auth (overwrite/refresh the token store):

```zsh
XBOX_AUTH_SETUP_FORCE=1 RUST_LOG="info" cargo run --bin xbox_auth_setup
```

#### 2) Validate tokens (smoke test)

This obtains an XSTS token (refreshing tokens as needed) and calls an Xbox Live endpoint:

```zsh
RUST_LOG="info" cargo run --bin xbox_xsts_smoke_test
```

#### 3) Run Microsoft Store ingestion

```zsh
RUST_LOG="info" cargo run --bin ingest run --only microsoft_store
```

Notes:

- The auth flow intentionally uses the traditional Xbox Live authorization path (no SISU) because
  SISU response shapes have been unstable.
- Some Xbox endpoints also require extra headers / configuration (see `src/database_ops/xbox/headers.rs`).

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
