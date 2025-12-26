# GameCompare Database Migrations (Per‑Table Squash Architecture)

This project now uses a **per‑table squashed migration strategy**: each logical table (or tightly coupled group) has a single idempotent migration file under the `02xx_*.sql` series. Legacy multi-purpose / iterative migrations remain as **no‑op placeholders** to preserve ordering for existing environments but are not needed for fresh installs.

## Requirements

- PostgreSQL 15+ (17 OK)
- Extensions: `citext`, `pg_trgm` (created in `0200_extensions_enums.sql`)

## Migration Ordering (Fresh Install)

1. 0200_extensions_enums.sql (extensions + enums)
2. 0210_currencies.sql
3. 0220_countries.sql
4. 0225_jurisdictions.sql
5. 0230_tax_rules.sql
6. 0240_products.sql
7. 0241_software.sql
8. 0242_hardware.sql
9. 0245_video_game_titles.sql
10. 0246_platforms.sql
11. 0247_video_games.sql
12. 0250_game_consoles.sql
13. 0260_sellables.sql
14. 0270_retailers.sql
15. 0275_offers.sql
16. 0280_offer_jurisdictions.sql
17. 0290_prices.sql
18. 0291_prices_partition_helper.sql
19. 0295_current_price.sql
20. 0300_users.sql
21. 0310_alerts.sql
22. 0320_providers.sql
23. 0321_prices_provider_fk.sql
24. (Optional seed) 0034_seed_steam_xbox_providers.sql

Legacy files `0035_partitions.sql`, `0036_recent_indexes.sql`, `0040_canonical_partition_helper.sql` are retained as **no‑op notices** to avoid breaking existing applied version histories.

## Applying (Fresh Database)

If using the custom Rust migration runner (preferred) it will apply in lexical order. For manual psql application:

```sql
\i migrations/0200_extensions_enums.sql
\i migrations/0210_currencies.sql
-- ... continue in listed order ...
\i migrations/0320_providers.sql
```

All scripts are idempotent: `CREATE TABLE IF NOT EXISTS`, guarded enums, and `CREATE INDEX IF NOT EXISTS`. Re-running is safe.

## Upgrading Existing Databases

Existing deployments that already executed earlier `0035/0036/0040` helper migrations can simply pull and run the new series; the legacy helper migrations now emit a NOTICE and do nothing. The new canonical helper lives in `0291_prices_partition_helper.sql`.

## Partition Strategy

- `prices` partitioned monthly on `recorded_at`.
- Parent-only indexes: `(offer_jurisdiction_id, recorded_at)` and a partial recent 30d index.
- Per-partition helper creates B-tree ascending, descending INCLUDE(amount_minor), recorded_at, and BRIN indexes.
- Horizon pre-created: last 12 months + next 6 months.

## Hot Read Path

`current_price` stores the most recent price per `offer_jurisdiction_id` and is maintained via application upsert logic. Reads for “latest price” avoid scanning partitions.

### Price Guard (optional)

Ingestion can optionally annotate incoming `prices.meta` using a reference provider as a guard (default: ITAD). The app logs a warning when the relative deviation vs. the guard’s latest price for the same `offer_jurisdiction_id` exceeds a threshold. Writes are not blocked.

Environment variables:

- `PRICE_GUARD_ENABLED` = true|false (default: true)
- `PRICE_GUARD_PROVIDER` = provider slug (default: `itad`)
- `PRICE_GUARD_MAX_PCT_DIFF` = maximum relative deviation before warning (default: `0.5` for 50%)

The guard info is embedded under `prices.meta.guard` with fields: `provider`, `amount_minor`, `recorded_at`, `relative_diff`, `status` ("ok" or "warn_large_deviation").

## Text Search / Fuzzy Matching

Trigram GIN indexes on slugs/names (`products`, `video_game_titles`, `game_consoles`). Additional FTS or bloom indexes may be layered later without breaking the per-table strategy (add new migration file with higher prefix, e.g. `0400_search_ftsvectors.sql`).

## Providers Layer

`0320_providers.sql` creates provider lineage tables (providers, retailer_providers, provider_items, provider_offers, provider_ingest_runs, provider_media_links) in one cohesive unit. Each has required uniqueness and supporting lookup indexes.

## Design Rationale


## Adding New Tables / Columns

1. Prefer creating a new dedicated migration file (e.g. `0330_new_feature_table.sql`).
2. Avoid editing existing per-table files once published to keep deterministic upgrade paths.
3. For column additions: create a new migration referencing only the delta (do not rewrite original file).

## Backfills & Large Data Loads

Use `COPY` into `prices` after calling `ensure_price_partition()` for representative timestamps (the helper pre-creates horizon, but always safe to call). Analyze each newly loaded partition: `ANALYZE prices_YYYY_MM;`.

## Verification Checklist

| Concern             | Resolution                                                  |
| ------------------- | ----------------------------------------------------------- |
| Idempotent reruns   | Guarded `IF NOT EXISTS` / DO blocks                         |
| Partition creation  | `ensure_price_partition` helper + horizon loop              |
| Trigger removal     | Legacy trigger migrations replaced by helper-based approach |
| Search indexes      | Per-table creation (trigram)                                |
| Alert performance   | Partial indexes on active flags                             |
| Provider uniqueness | Composite UNIQUE constraints (provider_item ↔ offer)        |

## Future Enhancements



This README reflects the current per-table squashed migration design. Update when structural changes occur or new feature migrations are introduced.

To quickly inspect lifted URL coverage after migration `0492_game_media_url_columns.sql`, use the diagnostic binary:

```bash
export DATABASE_URL="postgresql://postgres:YOURPASSWORD@HOST:5432/postgres"
cargo run --bin media_health
```

Output JSON fields:

| Field | Meaning |
|-------|---------|
| `total_media` | Total rows in `game_media`. |
| `lifted_any` | Rows with at least one lifted URL column populated. |
| `missing_original` | Rows lacking `original_url` (fallback remained unset). |
| `recent_lifted_30d` | Rows created in last 30 days with a lifted original/thumbnail URL. |
| `percent_with_any` | Coverage percentage of lifted URL presence. |
| `percent_missing_original` | Gap percentage needing potential backfill or provider re-sync. |
| `percent_recent_lifted` | Recent ingestion effectiveness. |

Why this matters:

1. **Operational Visibility** – Confirms migration backfill success and ongoing ingestion is populating lifted columns consistently.
2. **Query Performance** – High `percent_with_any` enables index-only scans on lifted columns instead of JSON extraction on `provider_data`.
3. **Data Quality Guardrail** – `missing_original` spikes can surface provider API regressions or upstream parsing failures.
4. **Freshness Tracking** – `recent_lifted_30d` ratio distinguishes legacy gaps from active ingestion issues.

If `percent_missing_original` remains high, schedule a targeted backfill job that replays provider metadata to hydrate missing columns (keeping original JSON for audit). A dry-run path can diff counts before/after.
