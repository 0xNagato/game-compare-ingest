# Operational Notes (GameCompare Pricing Platform)

## Partition Management

Table: `public.prices` RANGE PARTITIONED by month on `recorded_at`.

Pre-create horizon: last 12 months + next 6 months (already in `0002_partitions.sql`).

Ongoing monthly job (cron / k8s CronJob) should run on first day of each month:

```sql
SELECT ensure_price_partition(date_trunc('month', now()) + interval '6 months');
```

This extends future horizon proactively.

Application path now explicitly calls `ensure_price_partition(ts)` before bulk inserts to avoid DDL inside trigger; trigger removed in `0003_drop_insert_trigger.sql`.

## Vacuum / Analyze Strategy

`prices` partitions: set `autovacuum_vacuum_scale_factor` low (e.g. 0.05) and `autovacuum_analyze_scale_factor` low to keep stats fresh for time-series queries.

Consider:

```sql
ALTER TABLE public.prices_202511 SET (
  autovacuum_vacuum_scale_factor=0.05,
  autovacuum_analyze_scale_factor=0.02
);
```

Automate for new partitions. Run manual `ANALYZE` shortly after each large backfill.

## Fillfactor

`prices` is append-only; default fillfactor (100) is fine. Avoid lowering unless frequent HOT updates occur (not expected).

`current_price` experiences upserts; optional:

```sql
ALTER TABLE public.current_price SET (fillfactor=90);
```

### Current price tie-breaker (agent-based)

As of migration `0480_current_price_agent_tiebreaker.sql`, `current_price` records include:

- `agent text NOT NULL DEFAULT 'unknown'`
- `agent_priority smallint NOT NULL DEFAULT 0`

Write path upserts now resolve conflicts deterministically:

1. Newer `recorded_at` wins
2. If timestamps tie, higher `agent_priority` wins (e.g., storefront=100, retailer_api=90, catalog=20, pipeline=0)
3. If still tied and `amount_minor` differs, the incoming row replaces the existing one

Recommended priorities:

- PlayStation/Xbox/Steam storefront collectors: 100
- First-party retailer APIs: 90
- Pricing/catalog aggregators (e.g., Nexarda, ITAD, pricing_charts): 10–20
- Generic pipeline/default: 0

Set agent names consistently (e.g., `ps-store`, `xbox-store`, `steam-store`, `nexarda`, `pricing_charts`) for observability.

## Index Strategy

Parent-only indexes: `(offer_jurisdiction_id, recorded_at)` and `(recorded_at)` defined on ONLY parent for inheritance.

Optional partial recent index per partition (last 30 days) omitted due to immutable predicate constraints; alternative: create partition-specific partial indexes with concrete bounds:

```sql
CREATE INDEX CONCURRENTLY idx_prices_202511_recent ON public.prices_202511 (offer_jurisdiction_id, recorded_at)
WHERE recorded_at >= '2025-11-01' AND recorded_at < '2025-12-01';
```

Automate via monthly maintenance job.

## Backfill Guidance

Use `COPY` for bulk historical inserts:

```sql
COPY public.prices (
  offer_jurisdiction_id, provider_item_id, recorded_at,
  amount_minor, tax_inclusive, fx_minor_per_unit,
  btc_sats_per_unit, meta
) FROM STDIN (FORMAT csv);
```

Ensure partition exists for each month prior to COPY to avoid runtime DDL.

After large backfill:

```sql
ANALYZE public.prices_202511;
```

### High-performance CSV ingestion pattern (Rust)

For very large price/backfill imports (millions of rows), prefer a zero-allocation tight loop using `csv` crate byte records plus fast parsing utilities. Keep minor units as integers and avoid UTF-8 decoding where not needed.

Key points:

- Use `BufReader` with a multi-megabyte buffer to reduce syscalls.
- Read `ByteRecord` instead of `StringRecord` to skip UTF-8 validation for numeric fields.
- Pre-map header names to column indices once.
- Parse integers with `atoi` (zero alloc) and simple validations via `memchr`.
- Reuse a single `ByteRecord` and call `clear()` to retain capacity.
- Accumulate aggregates or build batch insert structures; flush in 1–5k row chunks.

Example:

```rust
use std::fs::File;
use std::io::BufReader;
use csv::{ReaderBuilder, ByteRecord};
use memchr::memchr;
use atoi::atoi;

fn main() -> anyhow::Result<()> {
  let f = File::open("data.csv")?;
  let mut rdr = ReaderBuilder::new()
    .has_headers(true)
    .flexible(false)
    .trim(csv::Trim::None)
    .from_reader(BufReader::with_capacity(8 << 20, f)); // 8MB buffer

  // Map header names → column indexes once
  let headers = rdr.byte_headers()?.clone();
  let idx_amount  = headers.iter().position(|h| h == b"amount_minor").expect("amount_minor");
  let idx_recorded= headers.iter().position(|h| h == b"recorded_at_iso").expect("recorded_at_iso");
  // … add other indices you need

  let mut rec = ByteRecord::new();
  let mut count = 0u64;
  let mut sum   = 0i128;

  while rdr.read_byte_record(&mut rec)? {
    // parse int fast (no alloc, no UTF-8 check)
    let amt: i64 = atoi(&rec[idx_amount]).unwrap_or(0);

    // quick ISO-8601 sanity (optional): find 'T' fast
    let _has_t = memchr(b'T', &rec[idx_recorded]).is_some();

    sum += amt as i128;
    count += 1;
    rec.clear();      // keep capacity, drop contents
  }

  eprintln!("rows: {count}, sum(amount_minor): {sum}");
  Ok(())
}
```

Use this pattern when converting large CSV exports (e.g. provider historical data) before bulk COPY / batch inserts.

### Discrepancy graphs (per-source series)

Front-end charts comparing sources should query `public.prices_series_by_source`, which surfaces each observation with its `provider_slug` and `provider_kind`.

Example: 14-day series per source for an offer_jurisdiction

```sql
SELECT recorded_at, amount_minor, provider_slug
FROM public.prices_series_by_source
WHERE offer_jurisdiction_id = $1
  AND recorded_at >= now() - interval '14 days'
ORDER BY recorded_at ASC;
```

For latest snapshot per source:

```sql
SELECT * FROM public.latest_price_per_source WHERE offer_jurisdiction_id = $1;
```

Indexing: migration 0481 adds a parent-only index `(provider_item_id, recorded_at)` to speed source joins/time filters.

### Bucketed rollups (hour/day)

For fast, even sampling in charts, use the bucketed views created in migration 0482:

- `prices_hourly_last_by_source` — latest point per hour and provider
- `prices_daily_last_by_source` — latest point per day and provider

Examples:

```sql
-- Hourly (last 7 days)
SELECT bucket, provider_slug, amount_minor
FROM public.prices_hourly_last_by_source
WHERE offer_jurisdiction_id = $1
  AND bucket >= now() - interval '7 days'
ORDER BY bucket, provider_slug;

-- Daily (last 60 days)
SELECT bucket, provider_slug, amount_minor
FROM public.prices_daily_last_by_source
WHERE offer_jurisdiction_id = $1
  AND bucket >= now() - interval '60 days'
ORDER BY bucket, provider_slug;
```

These views select the most recent observation within each bucket; always filter by both `offer_jurisdiction_id` and a bounded `bucket` range for partition pruning.

### PlayStation media: stable vs playable URLs

PlayStation storefront often serves transient, signed media URLs. To avoid storage churn while keeping videos playable:

- In `provider_media_links`, the canonical `url` for PS videos is the stable product page URL:
  `https://store.playstation.com/{locale}/product/{product_id}`
- The actual playable URLs (e.g., `.m3u8`/`.mp4`) are preserved in `metadata.play_urls[]` and `metadata.page_url` mirrors the canonical page.
- In `game_media` rows for PS (`source='psn'`), `provider_data` includes `page_url` alongside the playable `url`.

Client guidance:

1. Prefer `game_media` for direct playback (it stores the playable `url`).
2. If you rely on `provider_media_links`, use its `metadata.play_urls[]` to choose a stream, or link out to `url` (stable product page).

### Unified media classification (kind + title)

All stored media now exposes:

| Field        | Scope                                  | Purpose                                                                                                    |
| ------------ | -------------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| `kind`       | `provider_media_links`, `game_media`   | Coarse type: `image` or `video` (enum `media_kind`). Drives component selection (thumbnail vs player).     |
| `title`      | both                                   | Human-readable label e.g. `trailer`, `screenshot`, `gameplay`, `cover`, `hero`. Use for captions/UI chips. |
| `media_type` | `game_media` enum / free-text on links | Fine-grained legacy classification retained for backward compatibility and analytics filters.              |

Examples:

```sql
-- Recent image gallery for a game
SELECT title, url
FROM game_media
WHERE video_game_id = $1 AND kind = 'image'
ORDER BY created_at DESC
LIMIT 30;

-- Latest trailers (videos) with stable page reference
SELECT gm.title, gm.url AS playable_url, gm.provider_data->>'page_url' AS page_url
FROM game_media gm
WHERE gm.video_game_id = $1 AND gm.kind = 'video'
ORDER BY gm.created_at DESC;

-- Provider-linked media diagnostic for a provider item
SELECT kind, title, url
FROM provider_media_links
WHERE provider_item_id = $1
ORDER BY id;
```

Best practice: prefer `kind` for branching logic and `title` for display. Avoid deriving type from file extensions or legacy `media_type` going forward.

### NDJSON streaming for Nexarda one-shot

The `nexarda_catalogue_ingest` binary supports line-delimited JSON (NDJSON) to keep memory constant while importing large catalogs.

- Set `NEXARDA_CATALOGUE_NDJSON_PATH=/path/to/file.ndjson`
- Optional chunking: `NEXARDA_CATALOGUE_OFFSET` and `NEXARDA_CATALOGUE_LIMIT`
- The process sets `SET search_path TO public` so all unqualified writes target `public.*` tables explicitly.

Example:

```bash
export DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres
export AUTO_MIGRATE=0 DB_MAX_CONNS=2
export NEXARDA_CATALOGUE_NDJSON_PATH=./exports/nexarda.ndjson
export NEXARDA_CATALOGUE_OFFSET=1200 NEXARDA_CATALOGUE_LIMIT=600
cargo run --bin nexarda_catalogue_ingest
```

### Parallel backfills with SKIP LOCKED

For large, online backfills, run N workers in parallel using the `FOR UPDATE SKIP LOCKED` pattern. Each worker repeats the following until 0 rows are returned:

```sql
WITH cte AS (
  SELECT id
  FROM public.providers
  WHERE is_active IS NULL OR auth_metadata IS NULL OR metadata IS NULL
  FOR UPDATE SKIP LOCKED
  LIMIT 1000
)
UPDATE public.providers p
SET is_active = coalesce(p.is_active, true),
    auth_metadata = coalesce(p.auth_metadata, '{}'::jsonb),
    metadata = coalesce(p.metadata, '{}'::jsonb),
    updated_at = now()
FROM cte
WHERE p.id = cte.id
RETURNING p.id;
```

Indexes to aid these predicates are created by migration 0476 (expression indexes on NULL conditions). Tune LIMIT per table size, and run off-hours for minimal impact.

## Running individual migrations (large files)

For oversized SQL files under `migrations/` you can apply them one at a time using the per-file runner. This avoids long transactions and makes progress visible.

- Requirements: `DATABASE_URL` set; file path must exist under `./migrations`.
- Runner: `src/bin/migrate_one.rs` splits SQL safely (handles DO $$ .. $$, comments, and strings) and applies statements sequentially.

Example usage (zsh):

```bash
MIG_FILE=migrations/0001_init.sql cargo run --bin migrate_one
MIG_FILE=migrations/0002_prices_partitions.sql cargo run --bin migrate_one
MIG_FILE=migrations/0003_media_tables.sql cargo run --bin migrate_one
```

The runner prints `progress: X/Y` and a final `applied: <file>` when complete.

## PlayStation price sanity and regions

- Ensure `PS_STORE_REGIONS` contains all locales you care about (format: `ll-CC`), e.g. `en-us en-gb de-de fr-fr es-es it-it nl-nl pt-pt fi-fi sv-se no-no da-dk jp-jp ko-kr au-au nz-nz br-br mx-mx ar-ar za-za pl-pl tr-tr`.
- If you suspect zero-valued prices from PS Store, set `PS_LOG_ZERO_PRICES=1` to log any items where parsed base/discount prices equal 0 (diagnostics only).
- The ingestion path now ignores zero amounts when inserting to `prices` to avoid polluting `current_price`.
- Verify your persisted GraphQL hash `PS_HASH` is up-to-date; an outdated hash can yield incomplete payloads without price nodes.
- Tune `PS_STORE_RPS`, `PS_STORE_MAX_RETRIES`, and `PS_STORE_BACKOFF_MS` if you hit rate limits.

### PS price integrity view

- Migration `0507_ps_price_integrity_view.sql` recreates `public.ps_price_integrity`, which is the canonical helper view used by diagnostics (e.g. `db-counts`) to flag bad PlayStation discounts. Keep this view in sync if future ingestion changes add new metadata keys.
- If a deploy or manual cleanup accidentally drops the view, simply re-run the migration or apply the SQL in the migration file; it is idempotent and safe to reapply.

### Legacy "GameCompare Demo" retailer

- Early bootstrap flows created a placeholder retailer named `GameCompare Demo` (slug `gamecompare-demo`). This placeholder is no longer used; the CLI now defaults to the real PlayStation retailer (`PlayStation`, slug `playstation`).
- If you still see the legacy retailer in an environment, run `gc ps-full-refresh --prune-demo` or delete it manually so coverage and dashboards reflect real storefronts only.

### PlayStation GraphQL response cache (disk)

To reduce repeated calls to `categoryGridRetrieve` and `metGetProductById` across runs, the PS client supports an optional on-disk cache for GraphQL GET responses.

Enable by setting:

```bash
export PS_CACHE_DIR=/path/to/.ps_cache   # e.g., ./\.ps_cache in repo
export PS_CACHE_TTL_SECS=86400           # optional; default 7200s
```

Behavior:

- Cache key = (operationName, locale, persisted sha256, variables) → stable filename.
- On cache hit within TTL, responses are served from disk (no network).
- Errors (persisted query not found, 5xx after retries) are not cached.
- Directory structure: `$PS_CACHE_DIR/{operation}/{locale}/{hash}.json`.

Tip: Use a longer TTL in dev to quickly iterate without re-hitting PS APIs. Bust the cache by deleting files or lowering TTL.

## Steam regions and ingestion settings

- The Steam store API supports a `cc` parameter using ISO 3166-1 alpha-2 country codes (e.g., `us`, `gb`, `de`, `jp`). The API response includes `price_overview.currency` which we persist as minor units; currency is derived from the response.
- Configure target regions via `STEAM_REGIONS` (space-delimited ISO codes), and app IDs via `STEAM_APP_IDS` (space/comma-delimited numeric IDs). Optional:
  - `STEAM_FETCH_MEDIA=1` to pull hero/capsule art when available.
  - `STEAM_MAX_CONCURRENCY` to cap in-flight requests.
  - `STEAM_BATCH_FLUSH` to control insert batch size (1–5k recommended).
  - `STEAM_BACKFILL=1` with `STEAM_RECENT_MISSING_DAYS` to re-fetch recent prices/media for items with gaps.

Suggested starter region pack (covers common currencies; use country codes, not a generic "eu"):

```text
us gb ca au nz jp kr cn br mx ar za tr pl se no dk ch cz hu ro sg hk tw th id my ph vn ae sa il
```

Notes:

- For eurozone pricing, specify individual countries like `de`, `fr`, `it`, `es`, `nl`, `be`, `pt`, `ie`, `fi`, `gr`, `at`, etc.
- Some markets may be limited or share pricing with regional bundles; always rely on `currency` from the response when building `jurisdiction` + `currency` pairs.

## Pricing Data Sampling (2025-11-24)

### Method

- Step 2 sampling executed on **2025-11-24** directly against Supabase project `[REDACTED]` using the SQL editor.
- Pulled a random slice of `prices` (940 rows) alongside provider and currency metadata; raw outputs captured in the Composio scratch directory (`sampling_summary.json`, `index_metadata_summary.json`, `column_metadata_summary.json`).
- Supplemental aggregation queries computed provider share, currency distribution, extreme values, and metadata completeness for the linked `provider_items` and `offers` tables.

### Findings

1. **Provider concentration** — `provider_id = 17` (PlayStation Store) accounted for 100% of the sampled 940 price records. Xbox, Steam, and catalogue providers did not appear, indicating our ingestion or sampling window is effectively PlayStation-only right now.
2. **Currency outlier (currency_id = 10 / RUB)** — All high-value rows belonged to currency_id 10, mapping to Russian ruble. The max `amount_minor` reached `712900` (≈7 129 RUB). We need to confirm whether this represents an ultra-premium SKU or a parsing/FX defect.
3. **Missing provider item regions** — `provider_items.region_code` was NULL for all 172 sampled provider items, preventing jurisdiction analytics without joining through offers. This appears systemic rather than sampling noise.
4. **Offers metadata empty** — `offers.metadata` returned NULL for all 5 859 offers evaluated, so storefront payload details are not preserved for downstream diagnostics.

### Follow-up actions

- **Broaden sampling** — schedule recurring stratified samples (per-provider/per-currency) so dashboards can surface non-PlayStation coverage gaps automatically.
- **Populate provider regions** — update ingestion to hydrate `provider_items.region_code` (or retire the column if redundant) and backfill existing NULLs.
- **Persist offer metadata** — ensure source payloads write into `offers.metadata`; design a safe JSON schema (even `{}`) so diagnostics have something to interrogate.
- **Investigate RUB spike** — review the raw PlayStation payload for the high RUB amount and confirm currency/discount maths before alerting product.

Track remediation status in `.todo/tasks.json`; once fixes land, rerun the sampling queries and append the comparative metrics here.

### SQL templates (copy/paste-ready)

**Provider-stratified sample (ad-hoc):**

```sql
WITH price_sample AS (
  SELECT p.*
  FROM public.prices p
  WHERE p.recorded_at >= now() - interval '7 days'
  TABLESAMPLE SYSTEM (2)   -- adjust for desired volume
)
SELECT pr.slug        AS provider_slug,
       pr.name        AS provider_name,
       count(*)       AS rows,
       min(p.recorded_at) AS min_recorded_at,
       max(p.recorded_at) AS max_recorded_at
FROM price_sample p
JOIN offers o            ON o.id = p.offer_id
JOIN provider_offers po  ON po.offer_id = o.id
JOIN providers pr        ON pr.id = po.provider_id
GROUP BY pr.slug, pr.name
ORDER BY rows DESC;
```

**Currency + time-series sanity (per provider):**

```sql
WITH recent AS (
  SELECT p.offer_jurisdiction_id,
         p.recorded_at,
         p.amount_minor,
         oj.currency_id,
         pr.slug AS provider_slug
  FROM public.prices p
  JOIN offer_jurisdictions oj ON oj.id = p.offer_jurisdiction_id
  JOIN offers o               ON o.id = oj.offer_id
  JOIN provider_offers po     ON po.offer_id = o.id
  JOIN providers pr           ON pr.id = po.provider_id
  WHERE p.recorded_at >= now() - interval '30 days'
    AND pr.slug = $1          -- provider slug filter
)
SELECT date_trunc('day', recorded_at) AS bucket,
       currency_id,
       percentile_disc(0.5) WITHIN GROUP (ORDER BY amount_minor) AS p50_minor,
       max(amount_minor) AS max_minor,
       min(amount_minor) AS min_minor,
       count(*) AS rows
FROM recent
GROUP BY bucket, currency_id
ORDER BY bucket DESC, currency_id;
```

Drop these into Supabase SQL or psql to validate remediation work quickly.

### Automation note

Weekly automation can live either as a Supabase scheduled function (e.g., call the queries above and store JSON in `diagnostics.price_sampling_snapshots`) or as a lightweight ingestion job that writes NDJSON snapshots to S3. Reuse the provider-stratified query, parameterize provider sets, and rotate snapshots every Sunday 03:00 UTC to avoid peak ingest windows.

## Monitoring

Track lag between latest `recorded_at` in partition and `current_price.recorded_at` (should be near zero for active offers).

Alert on partition creation failures (missing future partitions).

Capture ingest run metadata in `provider_ingest_runs` (extend columns if needed for row counts, error summary).

## PGMQ Queue & Services

`ps_long_test` now supports queue-backed scheduling and workers through [PGMQ](https://github.com/tembo-io/pgmq). Configure behaviour with the following environment keys:

- `PS_QUEUE_MODE` — `direct` (default), `scheduler`, or `worker`.
- `PS_QUEUE_NAME` — queue identifier (defaults to `psstore_ingest`).
- `PS_QUEUE_VT_SECS` — visibility timeout for workers (default 180s).
- `PS_QUEUE_POLL_SECS` — worker idle sleep when no jobs are ready (default 5s).
- `PS_QUEUE_MAX_RETRIES` — archive threshold after repeated failures (`0` disables auto-archive).
- `SUPABASE_DB_SESSION_URL` — direct session DSN (port 5432) with no `statement_cache_capacity` query params for LISTEN connections.

Bootstrap the queue (idempotent) alongside migrations:

```sql
select pgmq."create"('psstore_ingest');
```

### Systemd units

Place shared secrets and rates in `/etc/gamecompare/ps_long.env` (or another path referenced by `EnvironmentFile`). Example scheduler unit:

```ini
# /etc/systemd/system/ps-long-scheduler.service
[Unit]
Description=GameCompare PlayStation ingest scheduler
After=network-online.target

[Service]
Type=simple
EnvironmentFile=/etc/gamecompare/ps_long.env
Environment=PS_QUEUE_MODE=scheduler
Environment=PS_QUEUE_NAME=psstore_ingest
ExecStart=/opt/gamecompare/bin/ps_long_test
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Workers can run as a template for horizontal scale:

```ini
# /etc/systemd/system/ps-long-worker@.service
[Unit]
Description=GameCompare PlayStation ingest worker %i
After=network-online.target

[Service]
Type=simple
EnvironmentFile=/etc/gamecompare/ps_long.env
Environment=PS_QUEUE_MODE=worker
Environment=PS_QUEUE_NAME=psstore_ingest
Environment=PS_QUEUE_VT_SECS=180
Environment=PS_QUEUE_POLL_SECS=3
ExecStart=/opt/gamecompare/bin/ps_long_test
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
```

Reload systemd after dropping the units (`sudo systemctl daemon-reload`), then enable `ps-long-scheduler.service` and start as many `ps-long-worker@{n}.service` instances as capacity allows.

Queue health probes:

```sql
select * from pgmq.metrics('psstore_ingest');
select * from pgmq.metrics_all();
```

If workers crash repeatedly on the same payload, the scheduler logs the correlation ID; after `PS_QUEUE_MAX_RETRIES` attempts the job is parked in the archive table (`pgmq.a_psstore_ingest`). Review and replay via `pgmq.archive` or `pgmq.send` as needed.

## Generic multi-provider worker (ingest_worker)

A single binary `ingest_worker` handles queue-backed ingestion for multiple providers with VT heartbeat, LISTEN/NOTIFY wake-ups, retries, and an HTTP API to enqueue jobs and export metrics.

Environment keys:

- `SUPABASE_DB_URL` or `DATABASE_URL` — Postgres DSN (PgBouncer transaction pool is OK)
- `SUPABASE_DB_SESSION_URL` — direct session DSN for LISTEN (5432, no statement cache param)
- `INGEST_QUEUE_NAME` — queue identifier (e.g., `steam_catalog`, `ps_prices`)
- `INGEST_QUEUE_VT_SECS` — worker visibility timeout (e.g., 60–180)
- `INGEST_QUEUE_POLL_SECS` — idle sleep when no jobs (e.g., 1–5)
- `INGEST_QUEUE_MAX_RETRIES` — archive after N failures (0 disables auto-archive)
- `INGEST_QUEUE_RETRY_BASE_SECS` — backoff base (default 5)
- `INGEST_QUEUE_RETRY_MAX_SECS` — cap for backoff (default 300)
- `WORKER_HTTP_ADDR` — optional, bind address for control API (e.g., `127.0.0.1:8082`)

Recommended queues (one process per queue for clear isolation and tuning):

- `steam_catalog` — Steam catalogue/media
- `ps_catalog` — PlayStation catalogue
- `ps_prices` — PlayStation prices
- `ps_ratings` — PlayStation ratings
- `igdb_catalog` — IGDB crawler/ingest
- `xbox_catalog` — Microsoft/Xbox store
- `nexarda_catalog` — Nexarda catalogue/pricing

The worker always emits `NOTIFY ingest_queue, '<queue_name>'` after enqueue and listens on `LISTEN ingest_queue` for immediate wake-ups.

HTTP API (if `WORKER_HTTP_ADDR` set):

- POST `/api/enqueue`
  - Body: `{ "provider": "steam", "task": "catalog", "args": {"app_ids": ["1091500"]} }`
  - Response: `{ ok: true, msg_id, correlation }`
- GET `/api/metrics`
  - Response: `{ last_wait_ms, last_run_ms, dequeues, failures, last_error }`

Provider/task matrix supported by the worker:

- `{"provider":"steam","task":"catalog"}` or `{"provider":"steam","task":"run"}`
- `{"provider":"ps","task":"prices"}`
- `{"provider":"ps","task":"ratings"}`
- `{"provider":"ps","task":"catalog"}` or `{"provider":"ps","task":"ingest"}`
- `{"provider":"igdb","task":"catalog"}`
- `{"provider":"xbox","task":"catalog"}`
- `{"provider":"nexarda","task":"catalog"}`

Systemd template example (one per queue):

```ini
# /etc/systemd/system/ingest-worker@.service
[Unit]
Description=GameCompare ingest worker %i
After=network-online.target

[Service]
Type=simple
EnvironmentFile=/etc/gamecompare/ingest.env
Environment=INGEST_QUEUE_NAME=%i
Environment=INGEST_QUEUE_VT_SECS=90
Environment=INGEST_QUEUE_POLL_SECS=2
Environment=WORKER_HTTP_ADDR=127.0.0.1:90%I
ExecStart=/opt/gamecompare/bin/ingest_worker
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
```

Start a fleet:

```bash
sudo systemctl enable --now ingest-worker@steam_catalog ingest-worker@ps_prices ingest-worker@ps_catalog igest-worker@igdb_catalog
```

Ad-hoc enqueue (HTTP):

```bash
curl -sS -XPOST localhost:9080/api/enqueue \
  -H 'content-type: application/json' \
  -d '{"provider":"ps","task":"prices","args":{"regions":["en-us","en-gb"],"skus":["UP4497-PPSA02139_00-ELDENRING0000000"]}}'
```

SQL queue health:

```sql
select * from pgmq.metrics('ps_prices');
select * from pgmq.metrics_all();
```

Notes:

- Tune VT (`INGEST_QUEUE_VT_SECS`) per task duration; the worker sends VT heartbeats during execution.
- For long-running backfills, prefer larger VT and/or chunk work into smaller jobs.
- Keep one queue per provider-domain to avoid head-of-line blocking and simplify SLOs.

### Backfill helper binary (retailer_providers)

For simple online backfills that only normalize NULL/default columns on `retailer_providers`, a lightweight helper binary is available. It runs N parallel workers using `FOR UPDATE SKIP LOCKED` and updates rows to sane defaults.

Environment:

```bash
export DATABASE_URL=postgres://user:pass@host:5432/db
export BACKFILL_WORKERS=16   # optional, default 16
export BACKFILL_BATCH=500    # optional, default 500
```

Run:

```bash
cargo run --bin backfill
```

Behavior:

- Claims up to `$BACKFILL_BATCH` rows where any of `credentials/settings/metadata/jurisdiction_scope/is_enabled/priority` is NULL
- Updates them to defaults in a transaction and repeats until no rows are left
- Safe to run concurrently across multiple processes; each worker uses `SKIP LOCKED`

See migration `0476_backfill_concurrent_indexes.sql` for helper expression indexes that speed up NULL-targeted scans.

### Provider-scoped claim function

For provider-targeted work queues, use the scoped claim helper to only lock rows for a specific provider (or all providers when NULL):

SQL (installed by migration 0477):

```
SELECT id, provider_id
FROM public.claim_provider_items_batch_scoped(
  p_worker_id => 'worker-1',
  p_batch_size => 1000,
  p_provider_id => 42   -- NULL to claim across all providers
);
```

This function is SECURITY DEFINER. Execution is revoked from PUBLIC; grant only to your service role.

### Out-of-transaction index creation helper

When you need to (re)create large partial indexes without transaction wrapping, use the helper script:

```bash
DATABASE_URL=postgres://user:pass@host:5432/db \
  ./scripts/create_indexes.sh
```

It runs CREATE INDEX CONCURRENTLY for the performance-critical provider_items/provider_offers partial indexes and is safe to run repeatedly.

## Supabase Notes

Extensions `citext`, `pg_trgm` required; create once (idempotent in migration).

RLS: Disable or add policies for service role if enabling row-level security later.

PgBouncer transaction mode: avoid server-side prepared statements (disabled in pool connect options).

## Redis (Future Use)

For Laravel horizon / queues, ensure distinct logical DBs if using single instance.

Key names: prefix `game_compare_` configurable with env.

## Docker Compose (Local Dev Example)

```yaml
version: "3.9"
services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_PASSWORD: postgres
      POSTGRES_USER: postgres
      POSTGRES_DB: postgres
    ports:
      - "5432:5432"
    command:
      ["postgres", "-c", "shared_buffers=256MB", "-c", "max_connections=200"]
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
  app:
    build: .
    depends_on:
      - postgres
      - redis
    environment:
      DATABASE_URL: postgres://postgres:postgres@postgres:5432/postgres
      SQLX_DISABLE_STATEMENT_CACHE: "1"
```

## Alert Evaluation Pattern

Evaluate only touched `offer_jurisdiction_ids` after ingestion.

Use partial index `(offer_jurisdiction_id) WHERE active` for fast lookup.

Batch fetch + compute comparisons in memory; update `last_triggered_at` only when threshold passes.

## Security & Maintenance

Rotate provider credentials stored in `retailer_providers.credentials` JSON via external secret manager; store only ephemeral tokens.

Add CHECK constraints or JSON schema validation for critical payload shapes if needed (via application logic, not DB enforced).

Credentials handling and RLS:

- Migration 0475 introduces an optional `credentials_enc` (bytea) column, a safe view `retailer_providers_safe`, and RLS policies restricting row visibility for the `authenticated` role.
- Prefer reading from `public.retailer_providers_safe` in non-privileged contexts; keep direct table access for backend/service role only.
- Backfill encryption from the application layer using pgcrypto (`pgp_sym_encrypt`) with keys managed outside SQL.

## Future Enhancements

Materialized view for 30d rolling price stats per offer_jurisdiction (refresh scheduled).

Partition archiving: detach very old partitions (`ALTER TABLE ... DETACH PARTITION`) to archive storage.

Consider timescaledb for advanced compression (not required currently).

## Importer (SQLite → Postgres) toggles

The legacy catalogue importer (`src/bin/import_sqlite.rs`) supports a few environment knobs for safety and throughput:

- VIDEO_GAME_UPDATE_CHUNK — max rows per batched UPDATE of `video_games` (default 500). Example: `VIDEO_GAME_UPDATE_CHUNK=1000`.
- PROGRESS_INTERVAL — progress log cadence in rows (defaults to the update chunk).
- SQLITE_PERF — enable read-optimized PRAGMAs (default 1/true). Set `SQLITE_PERF=0` to disable.
- SQLITE_PERF_SAFE — use a conservative PRAGMA set (no journal/locking changes). Example: `SQLITE_PERF_SAFE=1`.

Notes:
- The importer now enforces `PRAGMA query_only=ON` when SQLITE_PERF is enabled (both safe and aggressive sets) to harden the SQLite connection as read-only.
- You can resume or constrain work via: `PRODUCT_ID_MIN`, `VIDEO_GAME_ID_MIN`, `VIDEO_GAME_LIMIT_NEW`, and `VIDEO_GAME_SKIP_EXISTING_ONLY=1`.

### Durable checkpoints (resume across runs)

The importer maintains progress in `public.legacy_import_checkpoints` to support safe resumes across restarts.

- Checkpoint keys (source): `products`, `video_games` (media sources to follow if needed)
- Semantics: importer reads legacy rows with `id > last_legacy_id`
- Env override: `PRODUCT_ID_MIN` / `VIDEO_GAME_ID_MIN` are applied as a lower bound over the stored checkpoint
- Save cadence: `CHECKPOINT_EVERY` (default 1000 rows)

Operational examples:

    # Resume products from last saved position (and never go below id 5000)
    PRODUCT_ID_MIN=5000 CHECKPOINT_EVERY=2000 \
      RUST_LOG=info cargo run --bin import_sqlite -- ./database.sqlite

    # Import only 2k new VG combos (idempotent) and persist progress while iterating
    VIDEO_GAME_LIMIT_NEW=2000 CHECKPOINT_EVERY=1000 \
      RUST_LOG=info cargo run --bin import_sqlite -- ./database.sqlite
