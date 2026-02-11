# game-compare-ingest ⚙️

High-performance **Rust** data ingestion backend for [game-compare](https://github.com/0xNagato/game-compare). Handles multi-provider game catalog sync, real-time price tracking, and media resolution across 12 data sources.

## Why Rust?

The ingestion workload is write-heavy, latency-sensitive, and I/O-bound: concurrent HTTP requests to 12+ providers, batch database inserts, rate limiting, and retry logic. Rust's async ecosystem (Tokio + SQLx) delivers the throughput needed while keeping resource usage minimal — the full pipeline runs on a $10/month server.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                  Data Flow                               │
│                                                          │
│  PlayStation ─┐                                          │
│  Steam ───────┤                                          │
│  Xbox ────────┤     ┌──────────────────┐                │
│  IGDB ────────┼────►│  Unified Ingest  │                │
│  GiantBomb ───┤     │  CLI (ingest.rs) │                │
│  RAWG ────────┤     │                  │    ┌──────────┐│
│  TheGamesDB ──┤     │  • Rate limiting │───►│PostgreSQL││
│  Nexarda ─────┤     │  • Retry/backoff │    │          ││
│  ITAD ────────┤     │  • Idempotent    │    │ Shared   ││
│  PriceCharting┘     │    upserts       │    │ with     ││
│                     │  • Batch writes  │    │ Laravel  ││
│                     └──────────────────┘    └──────────┘│
└─────────────────────────────────────────────────────────┘
```

## Project Scale

| Metric | Count |
|--------|-------|
| Rust Lines of Code | **63,110** |
| CLI Binaries | **76** |
| Provider Integrations | **12** |
| Database Migrations | **196** |
| Schema Tables | **30+** |

## Provider Integrations

| Provider | Data Type | Method |
|----------|-----------|--------|
| PlayStation Store | Catalog + Prices | REST API (multi-region) |
| Steam | Catalog + Prices | Public API |
| Xbox / Microsoft Store | Catalog + Prices | Graph API + Direct |
| IGDB (Twitch) | Metadata + Media | OAuth2 API |
| GiantBomb | Metadata + Media | REST API |
| RAWG | Metadata + Ratings | REST API |
| TheGamesDB | Metadata + Media | REST API |
| Nexarda | Prices | REST API |
| ITAD (IsThereAnyDeal) | Price History | REST API |
| PriceCharting | Retro Prices | Scraping |

## Core Design Patterns

### Idempotent Ingestion

Every write operation uses `ON CONFLICT ... DO UPDATE` patterns. Running the pipeline multiple times produces identical results — no duplicates, no corruption:

```rust
pub async fn ensure_video_game(
    db: &Db,
    title_id: i64,
    platform_id: i64,
    edition: Option<&str>,
    metadata: Option<Value>,
) -> Result<i64> {
    // INSERT ... ON CONFLICT (title_id, platform_id, edition) DO UPDATE
    // Returns existing or new ID
}
```

### Time-Series Price Tracking

Prices are stored in monthly-partitioned tables for efficient range queries:

```
prices (parent)
├── prices_2025_09
├── prices_2025_10
├── prices_2025_11
└── prices_2025_12

current_price (materialized latest — sub-millisecond reads)
```

### Provider Abstraction

External data never goes directly into canonical tables. It flows through a provider abstraction layer that enables audit trails, deduplication, and schema evolution:

```
External API → provider_items → link → products/video_games
External SKU → provider_offers → link → offers
```

### Schema Design

```
products (cold catalog anchor)
├── software (1:1)
│   └── video_game_titles (canonical)
│       └── video_games (platform+edition variants)
└── hardware (1:1)
    └── game_consoles (SKUs)

sellables (unified commerce)
└── offers (retailer SKUs)
    └── offer_jurisdictions (region+currency)
        ├── prices (PARTITIONED, time-series)
        └── current_price (latest, read-hot)
```

## Module Structure

```
src/
├── bin/                         # 76 CLI binaries
│   ├── ingest.rs               # Unified provider CLI (primary entry point)
│   ├── db_migrate.rs           # Safe migration runner
│   ├── db_counts.rs            # Table row counts
│   └── [provider-specific]     # Standalone tools
├── database_ops/               # Database layer
│   ├── db.rs                   # Connection pooling, helpers
│   ├── ingest_providers.rs     # Shared ingestion helpers (6.5K LOC)
│   ├── alerts.rs               # Price alert evaluation
│   ├── exchange.rs             # FX rate sync
│   ├── playstation/            # PS Store client & parsing
│   ├── steam/                  # Steam API client
│   ├── xbox/                   # Xbox provider
│   ├── igdb/                   # IGDB client
│   ├── giantbomb/              # GiantBomb ingestion
│   ├── rawg/                   # RAWG client
│   ├── tgdb/                   # TheGamesDB client
│   ├── nexarda/                # Nexarda v3 API
│   └── itad/                   # IsThereAnyDeal client
├── cli/                        # CLI utilities
└── util/                       # Env loading, helpers
```

## Usage

```bash
# Build release binary
cargo build --release

# Run all providers in default order
RUST_LOG="info" cargo run --bin ingest run

# Run specific providers only
cargo run --bin ingest run --only playstation_store,steam_store

# Skip specific providers
cargo run --bin ingest run --skip rawg

# List available providers
cargo run --bin ingest providers

# Apply database migrations
cargo run --bin db_migrate

# Check table row counts
cargo run --bin db_counts
```

## Environment

```bash
# Database
DATABASE_URL="postgres://user:pass@host:5432/dbname"

# Provider API keys (as needed)
IGDB_CLIENT_ID="..."
IGDB_CLIENT_SECRET="..."
RAWG_API_KEY="..."
TGDB_API_KEY="..."
NEXARDA_API_KEY="..."
```

## Performance

- **Ingestion throughput:** 10K price points in < 120 seconds
- **Current price queries:** < 1ms (no partition scanning)
- **Partition pruning:** Single-month queries touch exactly 1 partition
- **Connection pooling:** SQLx with configurable pool size

## Integration with game-compare

```
Provider APIs
     ↓
[game-compare-ingest] → PostgreSQL ← [game-compare]
    (writes)             (shared DB)      (reads)
                                              ↓
                                   Filament Admin + Public API + React UI
```

Both applications coordinate on:
- **Schema:** Migrations in this repo define the canonical schema
- **Provider slugs:** Consistent identifiers across both codebases
- **Media linking:** Rust writes provider media links, Laravel resolves for display
- **Price data:** Rust writes `prices` + `current_price`, Laravel reads for API/UI

## Related

- **[game-compare](https://github.com/0xNagato/game-compare)** — Laravel + React frontend (companion repo)

## License

MIT
