# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**i-miss-rust** is the high-performance data ingestion backend for a dual-stack game price comparison platform. It works in tandem with **game-compare** (Laravel/PHP) which provides the admin UI and public-facing application.

### Architecture: Dual-Stack System

This Rust codebase handles the **write-heavy workload**:
- Multi-provider game catalog and price ingestion (PlayStation, Steam, Xbox, Nexarda, IGDB, GiantBomb, RAWG, ITAD, TheGamesDB)
- Time-series price tracking with monthly partitioned tables
- Media link resolution and storage
- Provider state management and idempotent sync

The **game-compare** Laravel application (located at `~/documents/game-compare`) handles the **read workload**:
- Filament admin UI for catalog management
- Public microsite (React/Vite) for price comparison
- JSON REST API endpoints
- Media resolution and caching
- User alerts and notifications

**Shared Contract**: Both applications read/write to the same PostgreSQL/Supabase database. The schema (see `migrations/`) is the source of truth.

## Essential Commands

### Build & Development

```zsh
# Build release binary (optimized)
cargo build -r

# Build debug (faster compile, slower runtime)
cargo build

# Check without building (fast)
cargo check

# Run with logging
RUST_LOG="info,sqlx=warn" cargo run --bin <binary_name>
```

### Database Operations

```zsh
# Apply migrations (safe runner, checks before applying)
RUST_LOG="info" cargo run -r --bin db_migrate

# Import legacy SQLite data (PHP-compat mode, no auto-migrations)
RUST_LOG="info" cargo run -r --bin import_sqlite database.sqlite

# Get row counts across all tables
RUST_LOG="info" cargo run -r --bin db_counts

# Reset database and reapply migrations (DESTRUCTIVE)
cargo run --bin db_reset_migrate
```

### Unified Provider Ingestion (Primary Interface)

The `ingest` binary is the main entry point for all provider pipelines:

```zsh
# Run all providers in default order (Nexarda → RAWG)
RUST_LOG="info" cargo run --bin ingest run

# Run specific providers only
RUST_LOG="info" cargo run --bin ingest run --only playstation_store,steam_store

# Skip specific providers
RUST_LOG="info" cargo run --bin ingest run --skip rawg

# Disable post-run alert evaluation
cargo run --bin ingest run --no-alerts

# List available provider keys
cargo run --bin ingest providers

# Continuous loop mode (runs providers repeatedly)
cargo run --bin ingest run --loop-secs 3600  # Run every hour
```

### Provider-Specific Environment Variables

**PlayStation Store**:
```zsh
export PS_STORE_REGIONS="en-US,en-GB,de-DE,fr-FR,es-ES"
export PS_PAGE_SIZE=100
export PS_TOTAL_PAGES=500
export PS_STORE_RPS=3  # Requests per second
export YEAR_MIN=2020
export YEAR_MAX=2025
```

**Steam**:
```zsh
# Steam provider uses internal rate limiting
# No API key required (public API)
```

**Xbox**:
```zsh
# Xbox authentication handled via xal crate
# Requires Microsoft account credentials in .env
```

**Nexarda**:
```zsh
export NEXARDA_API_KEY="your_key_here"
```

**IGDB**:
```zsh
export IGDB_CLIENT_ID="your_client_id"
export IGDB_CLIENT_SECRET="your_client_secret"
```

**TheGamesDB**:
```zsh
export TGDB_API_KEY="your_api_key"
```

**RAWG**:
```zsh
export RAWG_API_KEY="your_api_key"
```

### Legacy Provider Commands (Being Deprecated)

These still work but are being migrated to the unified `ingest` CLI:

```zsh
# PlayStation pricing (use: ingest run --only playstation_store)
PS_STORE_REGIONS="en-us" PS_MAX_PAGES=1 cargo run --bin ps_prices_ingest

# Steam (use: ingest run --only steam_store)
cargo run --bin steam_ingest_once

# Xbox (use: ingest run --only microsoft_store)
cargo run --bin xbox_ingest_once

# Worker manager (use: ingest run)
cargo run --bin worker_manager
```

## High-Level Architecture

### Module Structure

```
src/
├── lib.rs                          # Public API, PlayStation pipeline
├── main.rs                         # Legacy main entry point
├── orchestrator.rs                 # Provider orchestration
├── bin/                            # Binary entry points
│   ├── ingest.rs                  # 🌟 Unified provider CLI (PRIMARY)
│   ├── db_migrate.rs              # Safe migration runner
│   ├── db_counts.rs               # Table row counts
│   ├── import_sqlite.rs           # Legacy SQLite import
│   └── [70+ other binaries]       # Specialized tools
├── database_ops/                   # Database layer & provider logic
│   ├── db.rs                      # Core Db struct, connection pooling
│   ├── ingest_providers.rs        # 🔥 Shared ingestion helpers (6.5k LOC)
│   ├── alerts.rs                  # Price alert evaluation
│   ├── exchange.rs                # FX rate sync
│   ├── playstation/               # PS Store client & parsing
│   ├── steam/                     # Steam API client
│   ├── xbox/                      # Xbox provider via xal
│   ├── nexarda/                   # Nexarda v3 API client
│   ├── igdb/                      # IGDB client
│   ├── giantbomb/                 # GiantBomb ingestion
│   └── [other providers]
├── cli/                           # CLI utilities
└── util/                          # Env loading, helpers
```

### Critical Files to Understand

1. **`src/database_ops/ingest_providers.rs` (200KB, 6.5k lines)**:
   - Central ingestion helper library
   - Functions like `ensure_video_game`, `ensure_provider_item`, `ensure_offer`, `link_provider_offer`
   - All providers use these idempotent helpers to write data
   - **Read this first** to understand the ingestion contract

2. **`src/lib.rs`**:
   - PlayStation Store pipeline (`psstore_seed_pipeline`)
   - Demonstrates full ingestion flow: fetch → parse → ensure entities → link prices

3. **`src/bin/ingest.rs`**:
   - Unified CLI orchestrator
   - Provider registry with canonical IDs
   - Shows how all providers are wired together

4. **`src/database_ops/db.rs`**:
   - `Db` struct wrapping SQLx connection pool
   - Helper methods for common queries
   - Price row insertion with partitioning

### Database Schema Philosophy

**Core Principles**:
- **Products are cold**: Catalog data (`products`, `video_game_titles`, `video_games`, `game_consoles`) changes infrequently
- **Prices are hot**: Time-series `prices` table is partitioned by month on `recorded_at`
- **Current price is read-hot**: `current_price` table (small, fast) powers latest price queries without touching partitions
- **Provider abstraction**: All external data flows through `providers` → `provider_items` → `provider_offers` before linking to canonical `offers`
- **Idempotent writes**: Unique constraints everywhere; upsert patterns prevent duplicates

**Key Tables**:
```
products (cold catalog anchor)
├── software (1:1)
│   └── video_game_titles (canonical game titles)
│       └── video_games (platform+edition variants)
└── hardware (1:1)
    └── game_consoles (console SKUs)

sellables (unified commerce layer: software OR hardware)
└── offers (retailer SKUs)
    └── offer_jurisdictions (region+currency)
        ├── prices (PARTITIONED, time-series snapshots)
        └── current_price (latest only, read-hot)

providers (external data sources)
├── provider_items (external catalog entries)
└── provider_offers (external pricing SKUs)
    └── links to canonical offers via offer_provider_offers
```

### Partitioning & Performance

**Prices Table**:
- Monthly partitions: `prices_YYYY_MM` (e.g., `prices_2025_11`)
- Parent-level indexes cloned to partitions: `(offer_jurisdiction_id, recorded_at)`
- Optional BRIN indexes for large historical partitions
- Trigger auto-creates partitions on insert: `ensure_prices_partition_for(ts)`

**Performance Patterns**:
- **Writes**: Bulk insert into `prices` + upsert `current_price` in same transaction
- **Reads (latest)**: Query `current_price` only (never scan partitions)
- **Reads (series)**: Query specific month partition(s) via `recorded_at` pruning
- **Indexes**: Composite B-tree for hot reads, optional BRIN for cold ranges

See `docs/README_PERF.md` and `docs/perf_query_pack.sql` for detailed performance analysis.

### Provider Ingestion Flow

All providers follow this pattern:

1. **Fetch external data** (via HTTP client in `database_ops/{provider}/`)
2. **Parse into domain structs** (provider-specific parsing logic)
3. **Ensure entities** (via `ingest_providers.rs` helpers):
   ```rust
   ensure_provider(db, slug, name)
   ensure_product_named(db, slug, name)
   ensure_video_game_title(db, product_id, title)
   ensure_video_game(db, title_id, platform_id, edition, metadata)
   ensure_provider_item(db, provider_id, external_id, item_metadata)
   ensure_sellable(db, software_title_id)
   ensure_retailer(db, slug, name)
   ensure_offer(db, sellable_id, retailer_id, sku)
   ensure_jurisdiction(db, country_id, region_code)
   ensure_offer_jurisdiction(db, offer_id, jurisdiction_id, currency_id)
   link_provider_offer(db, provider_offer_id, offer_id)
   ```
4. **Ingest prices** (batch write to `prices` + upsert `current_price`)
5. **Link media** (via `ensure_provider_media_links_with_meta`)
6. **Update metadata** (genres, ratings, synopsis via `merge_video_game_metadata`)

**Idempotency**: All `ensure_*` functions use `ON CONFLICT` upserts. Running ingestion multiple times is safe.

### Media Architecture

**Hybrid approach** (see `docs/MEDIA_ARCHITECTURE.md`):
- **Hot path** (primary media): Stored in JSONB columns on `video_games.primary_media` for zero-join queries
- **Cold path** (bulk media): Stored in `game_images`, `game_videos` tables without FK constraints (performance)
- **Provider links**: `provider_media_links` maps external media IDs to canonical media entries

**Laravel integration**: The game-compare app queries `game_images`, `game_videos`, `provider_media_links` and uses Spatie Media Library for additional media management.

## Testing

### Running Tests

```zsh
# Run all tests
cargo test

# Run specific test
cargo test test_name

# Run with output
cargo test -- --nocapture

# Run tests for specific binary
cargo test --bin ingest
```

### Test Database

Tests use the same `DATABASE_URL` from `.env`. For isolated testing:
```zsh
export DATABASE_URL="postgres://user:pass@localhost:5432/test_db"
cargo test
```

## Common Development Workflows

### Adding a New Provider

1. Create provider module in `src/database_ops/your_provider/`
2. Implement client with retry/backoff logic
3. Add provider variant to `ProviderKey` enum in `src/bin/ingest.rs`
4. Implement ingestion logic using `ingest_providers.rs` helpers
5. Wire into `run_provider()` match statement
6. Add environment variables to `.env.example`
7. Update `README.md` provider table

### Debugging Ingestion Issues

```zsh
# Enable detailed logging
RUST_LOG="debug" cargo run --bin ingest run --only your_provider

# SQL query logging
RUST_LOG="debug,sqlx::query=trace" cargo run --bin ingest run

# Single provider with no alerts
cargo run --bin ingest run --only playstation_store --no-alerts
```

### Investigating Database State

```zsh
# Row counts
cargo run --bin db_counts

# Check providers table
psql $DATABASE_URL -c "SELECT id, slug, name FROM providers;"

# Recent prices
psql $DATABASE_URL -c "SELECT * FROM current_price LIMIT 10;"

# Partition list
psql $DATABASE_URL -c "SELECT tablename FROM pg_tables WHERE tablename LIKE 'prices_%';"
```

## Integration with game-compare (Laravel)

### Shared Database Contract

Both applications must coordinate on:
- **Schema changes**: Migrations in `i-miss-rust/migrations/` define the canonical schema
- **Provider slugs**: Use consistent provider identifiers (see `README.md` provider table)
- **Media linking**: Rust writes to `provider_media_links`, Laravel reads for media resolution
- **Price data**: Rust writes `prices` + `current_price`, Laravel reads for API/UI

### Data Flow

```
Provider APIs
     ↓
[i-miss-rust] → PostgreSQL/Supabase ← [game-compare]
  (writes)         (shared DB)           (reads)
                                              ↓
                                    Filament Admin + Public API
```

### Deployment Considerations

- **Rust**: Run ingestion via cron or systemd timer (e.g., `ingest run --loop-secs 3600`)
- **Laravel**: Standard web deployment (nginx/Apache + PHP-FPM)
- **Database**: Supabase pooler port 5432 for session pooling (recommended for bulk writes)
- **Monitoring**: Both apps should log to same centralized system (e.g., Laravel Horizon + Rust tracing)

## Environment Configuration

### Required Variables

```zsh
# Database (choose session pooler for bulk writes)
DATABASE_URL="postgres://USER:PASSWORD@HOST:5432/postgres"
# or
SUPABASE_DB_SESSION_URL="postgres://USER:PASSWORD@HOST.pooler.supabase.com:5432/postgres"

# Logging
RUST_LOG="info,sqlx=warn"

# Provider API keys (as needed)
NEXARDA_API_KEY="..."
TGDB_API_KEY="..."
IGDB_CLIENT_ID="..."
IGDB_CLIENT_SECRET="..."
RAWG_API_KEY="..."
```

See `.env.example` for complete list.

### Performance Tuning

```zsh
# SQLite import with fast pragmas (default)
cargo run -r --bin import_sqlite database.sqlite

# SQLite import with safe pragmas (multi-process safe)
SQLITE_PERF_SAFE=1 cargo run -r --bin import_sqlite database.sqlite

# Restrict PlayStation year range
YEAR_MIN=2020 YEAR_MAX=2025 cargo run --bin ingest run --only playstation_store
```

## Key Design Patterns

### Idempotent Helpers

All `ensure_*` functions in `ingest_providers.rs` use this pattern:
```rust
pub async fn ensure_video_game(
    db: &Db,
    title_id: i64,
    platform_id: i64,
    edition: Option<&str>,
    metadata: Option<Value>,
) -> Result<i64> {
    // INSERT ... ON CONFLICT (title_id, platform_id, edition) DO UPDATE ...
    // Returns existing or new ID
}
```

**Why**: Re-running ingestion is safe. No duplicate data.

### Provider Abstraction

External data is never directly inserted into canonical tables:
```
External API → provider_items → link → products/video_games
External SKU → provider_offers → link → offers
```

**Why**: Audit trail, deduplication across providers, schema evolution independence.

### Batch Price Writes

```rust
ingest_prices(db, offer_jurisdiction_id, &[PriceRow { ... }, ...]).await?;
```

Writes to both `prices` (historical) and `current_price` (latest) in one transaction.

**Why**: Atomic consistency, performance (batch insert).

## Troubleshooting

### Common Issues

**"relation does not exist"**:
- Run migrations: `cargo run --bin db_migrate`
- Check `DATABASE_URL` points to correct database

**"duplicate key violation"**:
- Expected behavior for idempotent operations
- If persistent, check unique constraints match data

**Slow ingestion**:
- Use session pooler (port 5432) not transaction pooler (6543)
- Check `RUST_LOG` isn't set to `trace` (very verbose)
- Review partition indexes: `docs/README_PERF.md`

**Provider timeout/rate limit**:
- Check rate limit env vars (e.g., `PS_STORE_RPS`)
- Review backoff settings in provider client
- Use `--skip` to exclude problematic provider temporarily

## Documentation

- **`README.md`**: Project overview, quickstart, provider table
- **`docs/README_PERF.md`**: Performance guide, indexing, partitioning
- **`docs/MEDIA_ARCHITECTURE.md`**: Media storage design (hot/cold paths)
- **`docs/SCHEMA_PERFORMANCE_ANALYSIS.md`**: Query patterns, optimization
- **`docs/ops_notes.md`**: Operational playbook, partition management
- **`migrations/`**: All schema DDL in chronological order
- **`~/documents/game-compare/docs/`**: Laravel-side documentation (media resolver, data flow, API)

## Performance Benchmarks & Targets

- **Ingestion**: 10k price points < 120s
- **API latency** (Laravel side): p95 < 300ms with cache hit
- **Partition pruning**: Single-month queries should scan 1 partition only
- **Current price queries**: < 1ms (no partition access)

Run `docs/perf_query_pack.sql` to verify query plans after schema changes.
