use anyhow::bail;
use anyhow::{anyhow, Context, Result};
use chrono::NaiveDate;
use futures::{future::BoxFuture, TryStreamExt};
use i_miss_rust::database_ops::db::Db;
use i_miss_rust::database_ops::ingest_providers::{
    ensure_platform, ensure_product_named_with_platform, ensure_provider,
    ensure_software_row, ensure_video_game_title, ensure_video_game_source,
    ensure_vg_source_media_links_with_meta, ProviderEntityCache,
    table_exists,
};
use i_miss_rust::util::env::{self, db_url_prefer_session};
use rayon::{prelude::*, ThreadPool, ThreadPoolBuilder};
use serde_json::Value;
use sqlx::sqlite::SqlitePoolOptions; // requires sqlx feature "sqlite"
use sqlx::types::Json; // needed for batch video_game metadata updates
use sqlx::{Row, SqlitePool}; // provided by sqlx when "sqlite" feature is enabled
use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::thread::available_parallelism;
use std::time::{Duration, Instant};
use tracing::{info, warn};
use url::Url;
use uuid::Uuid;

const UPDATE_CHUNK: usize = 500;
const LOOKUP_BATCH_DEFAULT: usize = 512;

fn lookup_batch_size() -> usize {
    std::env::var("IMPORT_LOOKUP_BATCH")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&v| v > 0)
        .unwrap_or(LOOKUP_BATCH_DEFAULT)
}

fn build_rayon_pool() -> Result<Option<Arc<ThreadPool>>> {
    let disabled = std::env::var("IMPORT_RAYON_DISABLED")
        .ok()
        .map(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(false);
    if disabled {
        return Ok(None);
    }
    let desired_threads = std::env::var("IMPORT_RAYON_THREADS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok());
    let thread_count = desired_threads.unwrap_or_else(|| {
        available_parallelism()
            .map(|n| n.get().saturating_sub(1).max(1))
            .unwrap_or(1)
    });
    if thread_count <= 1 {
        return Ok(None);
    }
    let pool = ThreadPoolBuilder::new()
        .num_threads(thread_count)
        .thread_name(|idx| format!("import-rayon-{idx}"))
        .build()
        .context("failed to build rayon pool for import_sqlite")?;
    Ok(Some(Arc::new(pool)))
}

// Simple MIME type guessers for Giant Bomb media when source rows lack explicit type.
fn guess_image_mime(url: &str) -> &'static str {
    let lower = url.to_ascii_lowercase();
    if lower.ends_with(".png") {
        "image/png"
    } else if lower.ends_with(".webp") {
        "image/webp"
    } else if lower.ends_with(".gif") {
        "image/gif"
    } else if lower.ends_with(".avif") {
        "image/avif"
    } else if lower.ends_with(".jpg") || lower.ends_with(".jpeg") {
        "image/jpeg"
    } else {
        "image/jpeg"
    }
}

fn guess_video_mime(url: &str) -> &'static str {
    fn host_matches(host: &str, needle: &str) -> bool {
        if host == needle {
            return true;
        }
        if host.len() <= needle.len() {
            return false;
        }
        if !host.ends_with(needle) {
            return false;
        }
        host.as_bytes()[host.len() - needle.len() - 1] == b'.'
    }

    if let Ok(parsed) = Url::parse(url) {
        if let Some(host) = parsed.host_str() {
            let host_lc = host.to_ascii_lowercase();
            if host_matches(&host_lc, "youtube.com")
                || host_lc == "youtu.be"
                || host_matches(&host_lc, "youtube-nocookie.com")
                || host_matches(&host_lc, "googlevideo.com")
            {
                return "video/youtube";
            }
            if host_matches(&host_lc, "vimeo.com") || host_matches(&host_lc, "vimeocdn.com") {
                return "video/vimeo";
            }
            if host_matches(&host_lc, "twitch.tv")
                || host_matches(&host_lc, "twitchcdn.net")
                || host_matches(&host_lc, "ttvnw.net")
            {
                return "video/twitch";
            }
            if host_matches(&host_lc, "dailymotion.com") || host_matches(&host_lc, "dmcdn.net") {
                return "video/dailymotion";
            }
            if host_matches(&host_lc, "brightcove.net") || host_matches(&host_lc, "bcovlive.io") {
                return "video/brightcove";
            }
            if host_matches(&host_lc, "wistia.com") || host_matches(&host_lc, "wistia.net") {
                return "video/wistia";
            }
            if host_matches(&host_lc, "streamable.com") {
                return "video/streamable";
            }
            if host_matches(&host_lc, "akamaihd.net")
                || host_matches(&host_lc, "cloudfront.net")
                || host_matches(&host_lc, "llnwd.net")
            {
                return "application/vnd.apple.mpegurl";
            }
        }
    }

    let lower = url.to_ascii_lowercase();
    let lower_path = lower.split(['?', '#']).next().unwrap_or(lower.as_str());

    if lower_path.ends_with(".m3u8") {
        "application/vnd.apple.mpegurl"
    } else if lower_path.ends_with(".mpd") {
        "application/dash+xml"
    } else if lower_path.contains(".ism/manifest") || lower_path.ends_with(".ism") {
        "application/vnd.ms-sstr+xml"
    } else if lower_path.ends_with(".webm") {
        "video/webm"
    } else if lower_path.ends_with(".mkv") {
        "video/x-matroska"
    } else if lower_path.ends_with(".ts") {
        "video/mp2t"
    } else if lower_path.ends_with(".mov") {
        "video/quicktime"
    } else if lower_path.ends_with(".avi") {
        "video/x-msvideo"
    } else if lower_path.ends_with(".mpg") || lower_path.ends_with(".mpeg") {
        "video/mpeg"
    } else if lower_path.ends_with(".m4v") {
        "video/x-m4v"
    } else if lower_path.ends_with(".flv") {
        "video/x-flv"
    } else {
        "video/mp4"
    }
}

/// Progress logging interval (rows). Override with env PROGRESS_INTERVAL.
fn progress_interval() -> usize {
    std::env::var("PROGRESS_INTERVAL")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(500)
}

/// Video game UPDATE batch size (rows). Override with env VIDEO_GAME_UPDATE_CHUNK.
fn update_chunk_size() -> usize {
    std::env::var("VIDEO_GAME_UPDATE_CHUNK")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(500)
}

#[derive(Clone)]
struct Progress {
    label: String,
    total: Option<usize>,
    every: usize,
    start: Instant,
    last_log: Instant,
    processed: usize,
}
impl Progress {
    fn new<L: Into<String>>(label: L, total: Option<usize>) -> Self {
        let now = Instant::now();
        Self {
            label: label.into(),
            total,
            every: progress_interval(),
            start: now,
            last_log: now,
            processed: 0,
        }
    }
    fn tick(&mut self, n: usize) {
        self.processed += n;
        if self.processed == n || self.processed % self.every == 0 {
            self.log(false);
        }
    }
    fn finish(&mut self) {
        self.log(true);
    }
    fn log(&mut self, done: bool) {
        let now = Instant::now();
        if !done && now.duration_since(self.last_log) < Duration::from_millis(200) {
            // Avoid log-spam if the interval is very small and loop is super fast
            return;
        }
        self.last_log = now;

        let elapsed = now.duration_since(self.start).as_secs_f64().max(0.001);
        let rate = (self.processed as f64) / elapsed;
        let (pct, eta_s, remaining_count) = if let Some(t) = self.total {
            let pct = (100.0 * (self.processed as f64)) / (t as f64);
            let remaining = if self.processed > 0 {
                let per = elapsed / (self.processed as f64);
                ((t.saturating_sub(self.processed) as f64) * per).max(0.0)
            } else {
                0.0
            };
            (
                Some(pct),
                Some(remaining),
                Some(t.saturating_sub(self.processed)),
            )
        } else {
            (None, None, None)
        };

        match (pct, eta_s, remaining_count) {
            (Some(p), Some(_eta), Some(rem)) if done => {
                info!(target: "progress", label=%self.label, processed=self.processed, remaining=rem, total=?self.total, pct=?format!("{:.1}", p), rate=?format!("{:.1}/s", rate), took=?format!("{:.1}s", elapsed), "done");
            }
            (Some(p), Some(eta), Some(rem)) => {
                info!(target: "progress", label=%self.label, processed=self.processed, remaining=rem, total=?self.total, pct=?format!("{:.1}", p), rate=?format!("{:.1}/s", rate), eta=?format!("{:.1}s", eta), "progress");
            }
            _ if done => {
                info!(target: "progress", label=%self.label, processed=self.processed, rate=?format!("{:.1}/s", rate), took=?format!("{:.1}s", elapsed), "done");
            }
            _ => {
                info!(target: "progress", label=%self.label, processed=self.processed, rate=?format!("{:.1}/s", rate), "progress");
            }
        }
    }
}

#[derive(Debug, Clone)]
struct StageTiming {
    name: String,
    elapsed: Duration,
    success: bool,
}

impl StageTiming {
    fn elapsed_ms(&self) -> f64 {
        self.elapsed.as_secs_f64() * 1000.0
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env::bootstrap_cli("import_sqlite");
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        .init();

    let mut args = std::env::args().skip(1);
    let sqlite_path = resolve_sqlite_path(args.next())?;
    let pg_url = resolve_pg_url(args.next())?;

    let sqlite_url = format!("sqlite://{}", sqlite_path.to_string_lossy());
    let sqlite = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&sqlite_url)
        .await
        .with_context(|| format!("failed to connect to sqlite at {}", sqlite_path.display()))?;

    // Apply performance-oriented PRAGMAs for read-heavy one-shot import (NOT schema migration).
    // Controlled via env SQLITE_PERF (default true). Disable with SQLITE_PERF=0.
    if std::env::var("SQLITE_PERF")
        .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
        .unwrap_or(true)
    {
        apply_sqlite_perf_pragmas(&sqlite).await?;
    }

    // Allow higher parallelism; user confirmed up to 80 connections on target host.
    // Default increased to 48 (safe middle) unless DB_MAX_CONNS provided.

    // Allow overriding max pool size via DB_MAX_CONNS; default to 50 for balanced read/write
    let mut max_conns: u32 = std::env::var("DB_MAX_CONNS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);
    // Optional: force single-connection mode so session-level FAST_INGEST settings apply to all queries.
    if std::env::var("FAST_INGEST_ONE_CONN")
        .ok()
        .map(|v| (v == "1" || v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("on")))
        .unwrap_or(false)
    {
        max_conns = 1;
        tracing::info!(
            max_conns,
            "FAST_INGEST_ONE_CONN enabled — using single connection"
        );
    }

    // Use no-migrate variant to guarantee pure data import.
    let db = Db::connect_no_migrate(&pg_url, max_conns).await?;

    // Optional FAST_INGEST session tweaks: enable with FAST_INGEST=1 (default on).
    // These settings trade durability for speed during one-shot bulk imports.
    // They are applied only for the lifetime of this session.
    if std::env::var("FAST_INGEST")
        .ok()
        .map(|v| (v == "1" || v.eq_ignore_ascii_case("true")))
        .unwrap_or(true)
    {
        if let Err(e) = apply_fast_ingest_session(&db).await {
            tracing::warn!(error=?e, "FAST_INGEST session optimization failed; continuing without it");
        } else {
            tracing::info!("FAST_INGEST session optimizations applied");
        }
    }

    let mut ctx = ImportContext::new(db, sqlite).await?;
    ctx.run().await?;
    ctx.emit_stage_summary();

    info!(
        "import complete: {} products, {} video games pushed",
        ctx.stats.products, ctx.stats.video_games
    );
    Ok(())
}

// Apply per-connection session settings to accelerate bulk ingest.
// These are safe when the process is an isolated, restartable importer and callers
// accept potential data loss on crash before commit (we keep transactions small).
async fn apply_fast_ingest_session(db: &Db) -> Result<()> {
    // Lower durability / fsync cost; shorten commit latency.
    // Apply settings at the SESSION level; prefer FAST_INGEST_ONE_CONN=1 to avoid cross-connection drift.
    let work_mem_mb: u32 = std::env::var("FAST_INGEST_WORK_MEM_MB")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(256);
    let sql = format!(
        concat!(
            "SET synchronous_commit = OFF;",
            "SET statement_timeout = 0;",
            "SET jit = OFF;",
            "SET work_mem = '{}MB';",
            "SET temp_buffers = '{}MB';"
        ),
        work_mem_mb, 2 *
        work_mem_mb
    );
    // Execute as a single batch to avoid borrow/lifetime issues with multiple queries.
    sqlx::raw_sql(&sql).execute(&db.pool).await?;
    Ok(())
}

// Apply aggressive read-optimized PRAGMAs. Safe for a dedicated, read-only import process.
// These reduce fsync, enlarge cache/mmap, and favor in-memory temp structures.
async fn apply_sqlite_perf_pragmas(pool: &SqlitePool) -> Result<()> {
    // Sequence chosen to minimize locking transitions.
    // NOTE: If the source DB might still be written by another process, avoid journal_mode=OFF/EXCLUSIVE locking.
    // Provide override via SQLITE_PERF_SAFE=1 for a safer subset.
    let safe_subset = std::env::var("SQLITE_PERF_SAFE")
        .map(|v| (v == "1" || v.eq_ignore_ascii_case("true")))
        .unwrap_or(false);

    let pragmas = if safe_subset {
        vec![
            "PRAGMA synchronous=NORMAL;",
            "PRAGMA temp_store=MEMORY;",
            "PRAGMA cache_size=-80000;", // ~80k pages (~80MB) if page_size=1KB
            "PRAGMA mmap_size=268435456;", // 256MB
            "PRAGMA optimize;",
        ]
    } else {
        vec![
            "PRAGMA journal_mode=OFF;", // no rollback journal overhead
            "PRAGMA locking_mode=EXCLUSIVE;", // reduce lock churn
            "PRAGMA synchronous=OFF;",  // skip fsync (ok for read-only)
            "PRAGMA temp_store=MEMORY;", // temp B-Trees in RAM
            "PRAGMA cache_size=-160000;", // ~160MB cache (negative = KB units)
            "PRAGMA mmap_size=536870912;", // 512MB memory map
            "PRAGMA page_size=4096;",   // larger pages (ignored if DB already created)
            "PRAGMA optimize;",         // run internal optimizer
            "PRAGMA analysis_limit=1000;", // accelerate ANALYZE internals
        ]
    };

    for stmt in pragmas {
        if let Err(e) = sqlx::query(stmt).execute(pool).await {
            tracing::warn!(pragma=%stmt, error=?e, "sqlite pragma apply failed (continuing)");
        } else {
            tracing::debug!(pragma=%stmt, "sqlite pragma applied");
        }
    }
    Ok(())
}

// ----------------------------------------------------------------------------
// Import context: state, caches, and orchestration
// ----------------------------------------------------------------------------

#[derive(Debug, Default, Clone, Copy)]
struct ImportStats {
    products: usize,
    software_products: usize,
    hardware_products: usize,
    video_games: usize,
}

#[derive(Debug, Clone)]
struct ProductInfo {
    pg_id: i64,
    slug: String,
    title: String,
    kind: String,
    release_date: Option<NaiveDate>,
    synopsis: Option<String>,
    popularity_score: f64,
    rating: f64,
    metadata: Option<Value>,
    title_id: Option<i64>,
}
#[derive(Clone)]
struct ImportContext {
    // Connections
    db: Db,
    sqlite: SqlitePool,
    cache: ProviderEntityCache,
    rayon_pool: Option<Arc<ThreadPool>>,
    stage_timings: Vec<StageTiming>,

    // Operational toggles
    skip_titles: bool,
    media_only: bool,

    // Caches / maps (legacy -> pg or lookups)
    stats: ImportStats,
    provider_id_by_slug: HashMap<String, i64>,
    legacy_provider_slug: HashMap<i64, String>,
    title_id_by_norm: HashMap<String, i64>,
    product_map: HashMap<i64, ProductInfo>, // legacy_product_id -> ProductInfo
    product_platforms: HashMap<i64, Vec<i64>>, // legacy_product_id -> [pg platform ids]
    legacy_vg_map: HashMap<i64, i64>,       // legacy_vg_id -> pg video_games.id

    // Slug/id caches
    product_slugs: HashSet<String>, // global set of product slugs (uniqueness)
    product_slug_to_id: HashMap<String, i64>, // existing products slug -> id
    product_slug_by_legacy: HashMap<i64, String>, // legacy_product_id -> slug (stable across runs)
    video_game_slugs: HashSet<String>, // global set of vg slugs
    vg_slug_by_id: HashMap<i64, String>, // vg_id -> slug (stability on reruns)

    // Entity/key caches
    title_by_product: HashMap<i64, i64>, // product_id -> title_id
    vg_combo_map: HashMap<(i64, i64), i64>, // (title_id, platform_id) -> vg_id
    vg_platform_by_vg: HashMap<i64, i64>, // vg_id -> platform_id
    vg_platform_slug: HashMap<i64, String>, // vg_id -> platform.code (slug)
    platform_by_legacy: HashMap<i64, i64>, // legacy_platform_id -> pg platform id
    platform_by_code: HashMap<String, i64>, // platform.code -> id
    platform_slug_by_pg: HashMap<i64, String>, // platform_id -> code
    currency_map: HashMap<i64, i64>,     // legacy_currency_id -> pg currency id
    country_map: HashMap<i64, i64>,      // legacy_country_id -> pg country id

    // Information schema cache
    column_cache: HashMap<String, HashSet<String>>, // table -> columns present
}

impl ImportContext {
    async fn profile_stage<T, F>(&mut self, name: &'static str, func: F) -> Result<T>
    where
        F: for<'ctx> FnOnce(&'ctx mut ImportContext) -> BoxFuture<'ctx, Result<T>>,
    {
        let start = Instant::now();
        let result = func(self).await;
        let elapsed = start.elapsed();
        let success = result.is_ok();
        info!(
            target: "metrics",
            stage = name,
            took_ms = format!("{:.2}", elapsed.as_secs_f64() * 1000.0),
            success,
            "stage timing"
        );
        self.stage_timings.push(StageTiming {
            name: name.to_string(),
            elapsed,
            success,
        });
        result
    }

    fn emit_stage_summary(&self) {
        if self.stage_timings.is_empty() {
            return;
        }
        let mut timings = self.stage_timings.clone();
        timings.sort_by_key(|t| Reverse(t.elapsed));
        let total = timings
            .iter()
            .fold(Duration::ZERO, |acc, timing| acc + timing.elapsed);
        info!(
            target: "metrics",
            stages = timings.len(),
            total_ms = format!("{:.2}", total.as_secs_f64() * 1000.0),
            "import stage timing summary"
        );
        for timing in timings {
            let pct = if total.as_nanos() == 0 {
                0.0
            } else {
                (timing.elapsed.as_secs_f64() / total.as_secs_f64()) * 100.0
            };
            info!(
                target: "metrics",
                stage = %timing.name,
                took_ms = format!("{:.2}", timing.elapsed_ms()),
                pct = format!("{:.1}", pct),
                success = timing.success,
                "stage timing detail"
            );
        }
    }

    fn parallel_filter_map<T, U, F>(&self, rows: Vec<T>, func: F) -> Vec<U>
    where
        T: Send,
        U: Send,
        F: Fn(T) -> Option<U> + Send + Sync,
    {
        if let Some(pool) = &self.rayon_pool {
            pool.install(|| {
                let func_ref = &func;
                rows.into_par_iter()
                    .filter_map(|row| func_ref(row))
                    .collect()
            })
        } else {
            rows.into_iter().filter_map(func).collect()
        }
    }

    async fn process_currency_chunk(
        &mut self,
        rows: Vec<LegacyCurrency>,
        has_minor_unit: bool,
        prog: &mut Progress,
    ) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }
        let sanitized = self.parallel_filter_map(rows, |row| {
            let code = row
                .code
                .as_deref()
                .unwrap_or("")
                .trim()
                .to_ascii_uppercase();
            if code.len() < 3 {
                warn!(legacy_id=?row.id, ?code, "skipping currency with short code");
                return None;
            }
            let name = row
                .name
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or(&code)
                .to_string();
            let decimals = row.decimals.unwrap_or(2).clamp(0, i64::from(i16::MAX)) as i16;
            Some(CurrencyUpsert {
                legacy_id: row.id,
                code,
                name,
                minor_unit: decimals,
            })
        });
        if sanitized.is_empty() {
            return Ok(0);
        }
        self.flush_currency_batch(&sanitized, has_minor_unit)
            .await?;
        for _ in 0..sanitized.len() {
            prog.tick(1);
        }
        Ok(sanitized.len())
    }

    async fn flush_currency_batch(
        &mut self,
        batch: &[CurrencyUpsert],
        has_minor_unit: bool,
    ) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let codes: Vec<String> = batch.iter().map(|r| r.code.clone()).collect();
        let names: Vec<String> = batch.iter().map(|r| r.name.clone()).collect();
        if has_minor_unit {
            let minors: Vec<i16> = batch.iter().map(|r| r.minor_unit).collect();
            let rows = sqlx::query(
                r#"
                WITH input AS (
                    SELECT UNNEST($1::text[]) AS code,
                           UNNEST($2::text[]) AS name,
                           UNNEST($3::int2[])  AS minor_unit
                )
                INSERT INTO public.currencies (code, name, minor_unit)
                SELECT code, name, minor_unit FROM input
                ON CONFLICT (code) DO UPDATE
                SET name = EXCLUDED.name,
                    minor_unit = EXCLUDED.minor_unit
                RETURNING id, code
                "#,
            )
            .persistent(false)
            .bind(&codes)
            .bind(&names)
            .bind(&minors)
            .fetch_all(&self.db.pool)
            .await?;
            let mut map = HashMap::new();
            for row in rows {
                let id: i64 = row.get("id");
                let code: String = row.get("code");
                map.insert(code.to_ascii_uppercase(), id);
            }
            for entry in batch {
                if let Some(id) = map.get(&entry.code) {
                    self.currency_map.insert(entry.legacy_id, *id);
                }
            }
        } else {
            let rows = sqlx::query(
                r#"
                WITH input AS (
                    SELECT UNNEST($1::text[]) AS code,
                           UNNEST($2::text[]) AS name
                )
                INSERT INTO public.currencies (code, name)
                SELECT code, name FROM input
                ON CONFLICT (code) DO UPDATE
                SET name = EXCLUDED.name
                RETURNING id, code
                "#,
            )
            .persistent(false)
            .bind(&codes)
            .bind(&names)
            .fetch_all(&self.db.pool)
            .await?;
            let mut map = HashMap::new();
            for row in rows {
                let id: i64 = row.get("id");
                let code: String = row.get("code");
                map.insert(code.to_ascii_uppercase(), id);
            }
            for entry in batch {
                if let Some(id) = map.get(&entry.code) {
                    self.currency_map.insert(entry.legacy_id, *id);
                }
            }
        }
        Ok(())
    }

    async fn process_country_chunk(
        &mut self,
        rows: Vec<LegacyCountry>,
        iso2_to_iso3: &HashMap<String, String>,
        prog: &mut Progress,
    ) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }
        let currency_map = &self.currency_map;
        let sanitized = self.parallel_filter_map(rows, |row| {
            let iso2 = row.code.trim().to_ascii_uppercase();
            if iso2.len() != 2 {
                warn!(legacy_id=?row.id, iso=?row.code, "skipping country non-iso2");
                return None;
            }
            let Some(iso3) = iso2_to_iso3.get(&iso2).cloned() else {
                warn!(legacy_id=?row.id, ?iso2, "no iso3 mapping; skipping");
                return None;
            };
            let Some(currency_id) = row
                .currency_id
                .and_then(|legacy| currency_map.get(&legacy).copied())
            else {
                warn!(legacy_id=?row.id, "skipping country missing currency mapping");
                return None;
            };
            let name = row.name.trim();
            if name.is_empty() {
                warn!(legacy_id=?row.id, "skipping country with empty name");
                return None;
            }
            Some(CountryUpsert {
                legacy_id: row.id,
                iso2,
                iso3,
                name: name.to_string(),
                currency_id,
            })
        });
        if sanitized.is_empty() {
            return Ok(0);
        }
        self.flush_country_batch(&sanitized).await?;
        for _ in 0..sanitized.len() {
            prog.tick(1);
        }
        Ok(sanitized.len())
    }

    async fn flush_country_batch(&mut self, batch: &[CountryUpsert]) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let iso2_vec: Vec<String> = batch.iter().map(|r| r.iso2.clone()).collect();
        let iso3_vec: Vec<String> = batch.iter().map(|r| r.iso3.clone()).collect();
        let names: Vec<String> = batch.iter().map(|r| r.name.clone()).collect();
        let currency_ids: Vec<i64> = batch.iter().map(|r| r.currency_id).collect();
        let rows = sqlx::query(
            r#"
            WITH input AS (
                SELECT UNNEST($1::text[]) AS iso2,
                       UNNEST($2::text[]) AS iso3,
                       UNNEST($3::text[]) AS name,
                       UNNEST($4::bigint[]) AS currency_id
            )
            INSERT INTO public.countries (iso2, iso3, name, currency_id)
            SELECT iso2, iso3, name, currency_id FROM input
            ON CONFLICT (iso2) DO UPDATE
            SET iso3 = EXCLUDED.iso3,
                name = EXCLUDED.name,
                currency_id = EXCLUDED.currency_id
            RETURNING id, iso2
            "#,
        )
        .persistent(false)
        .bind(&iso2_vec)
        .bind(&iso3_vec)
        .bind(&names)
        .bind(&currency_ids)
        .fetch_all(&self.db.pool)
        .await?;
        let mut iso_map = HashMap::new();
        for row in rows {
            let id: i64 = row.get("id");
            let iso2: String = row.get("iso2");
            iso_map.insert(iso2.to_ascii_uppercase(), id);
        }
        for entry in batch {
            if let Some(id) = iso_map.get(&entry.iso2) {
                self.country_map.insert(entry.legacy_id, *id);
            }
        }
        Ok(())
    }

    async fn new(db: Db, sqlite: SqlitePool) -> Result<Self> {
        let cache = ProviderEntityCache::new(db.clone());
        let rayon_pool = build_rayon_pool()?;
        // Toggle: allow skipping title normalization updates
        let skip_titles = {
            let a = std::env::var("SKIP_TITLES_FORCE")
                .map(|v| (v == "1" || v.eq_ignore_ascii_case("true")))
                .unwrap_or(false);
            let b = std::env::var("SKIP_TITLES")
                .map(|v| (v == "1" || v.eq_ignore_ascii_case("true")))
                .unwrap_or(false);
            a || b
        };
        // Toggle: skip all non-media stages and run media-only importers
        let media_only = {
            let a = std::env::var("MEDIA_ONLY")
                .map(|v| (v == "1" || v.eq_ignore_ascii_case("true")))
                .unwrap_or(false);
            let b = std::env::var("SKIP_UNTIL_MEDIA")
                .map(|v| (v == "1" || v.eq_ignore_ascii_case("true")))
                .unwrap_or(false);
            a || b
        };
        let mut ctx = Self {
            db,
            sqlite,
            cache,
            rayon_pool,
            stage_timings: Vec::new(),
            skip_titles,
            media_only,
            stats: ImportStats::default(),
            provider_id_by_slug: HashMap::new(),
            legacy_provider_slug: HashMap::new(),
            title_id_by_norm: HashMap::new(),
            product_map: HashMap::new(),
            product_platforms: HashMap::new(),
            legacy_vg_map: HashMap::new(),
            product_slugs: HashSet::new(),
            product_slug_to_id: HashMap::new(),
            product_slug_by_legacy: HashMap::new(),
            video_game_slugs: HashSet::new(),
            vg_slug_by_id: HashMap::new(),
            title_by_product: HashMap::new(),
            vg_combo_map: HashMap::new(),
            vg_platform_by_vg: HashMap::new(),
            vg_platform_slug: HashMap::new(),
            platform_by_legacy: HashMap::new(),
            platform_by_code: HashMap::new(),
            platform_slug_by_pg: HashMap::new(),
            currency_map: HashMap::new(),
            country_map: HashMap::new(),
            column_cache: HashMap::new(),
        };

        ctx.ensure_required_columns().await?;
        // Prefill commonly used caches for idempotency and speed
        ctx.prefill_existing_slugs().await?;
        ctx.prefill_title_cache().await?;
        ctx.ensure_checkpoint_table().await?;
        Ok(ctx)
    }

    async fn run(&mut self) -> Result<()> {

        if self.media_only {
            info!(
                "MEDIA_ONLY enabled — skipping non-media stages (lookups, products, video_games)"
            );
        } else {
            // Providers (from legacy sources) first so media can resolve provider slugs
            let _ = self
                .profile_stage("video_game_sources", |ctx| {
                    Box::pin(ctx.import_video_game_sources())
                })
                .await;

            // Lookups
            self.profile_stage("currencies", |ctx| Box::pin(ctx.import_currencies()))
                .await?;
            self.mark_stage_done("currencies").await?;
            self.profile_stage("platforms", |ctx| Box::pin(ctx.import_platforms()))
                .await?;
            self.mark_stage_done("platforms").await?;
            self.profile_stage("countries", |ctx| Box::pin(ctx.import_countries()))
                .await?;
            self.mark_stage_done("countries").await?;

            // Relationships needed for VG derivation
            self.profile_stage("product_platform_links", |ctx| {
                Box::pin(ctx.load_product_platform_links())
            })
            .await?;

            // Core entities
            self.profile_stage("products", |ctx| Box::pin(ctx.import_products()))
                .await?;
            self.mark_stage_done("products").await?;
            self.profile_stage("video_games", |ctx| Box::pin(ctx.import_video_games()))
                .await?;
            self.mark_stage_done("video_games").await?;
        }

        // Legacy media tables → game_media
        let _ = self
            .profile_stage("game_images", |ctx| Box::pin(ctx.import_game_images()))
            .await;
        let _ = self
            .profile_stage("game_videos", |ctx| Box::pin(ctx.import_game_videos()))
            .await;


        // Giant Bomb media specialized path (optional)
        // Run images in chunked loops until the stage marks done. This avoids having to
        // restart the importer for each chunk and leverages the durable checkpoint
        // (gb_images_id) between iterations.
        loop {
            if self.is_stage_done("gb_images").await.unwrap_or(false) {
                break;
            }
            let _ = self
                .profile_stage("gb_images_chunk", |ctx| {
                    Box::pin(ctx.import_giantbomb_images())
                })
                .await;
            // Small yield between chunks to be polite to SQLite on slower disks
            // and to give Postgres a moment to flush indexes.
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        let _ = self
            .profile_stage("gb_videos", |ctx| Box::pin(ctx.import_giantbomb_videos()))
            .await;

        // Global dedupe for safety
        let _ = self
            .profile_stage("dedupe_all", |ctx| Box::pin(ctx.dedupe_all()))
            .await;
        Ok(())
    }
}

// Methods on ImportContext
impl ImportContext {





    // ----- Deduplication stage (post-import) -----
    // Consolidate duplicate titles created during import by picking a stable keeper ID
    // and re-pointing dependents before deleting duplicates.
    async fn dedupe_products(&self) -> Result<()> {
        let pool = &self.db.pool;
        let mut tx = pool.begin().await?;

        // A) Deduplicate video_game_titles by unified key
        //    Strategy:
        //      1. Identify duplicate title groups (pid0,key)
        //      2. Re-point video_games to keeper title_id.
        //      3. Delete duplicate titles (non-keeper ids).

        // Re-point video_games to keeper title_id
        // Pre-merge potential duplicate video_games rows that would collide under the keep title_id
        // to avoid triggering uq_video_games_title_platform_null.
        let premerge_video_games_sql = r#"
            WITH base AS (
                SELECT id,
                       COALESCE(product_id,0) AS pid0,
                       COALESCE(NULLIF(normalized_title,''), lower(title)) AS key
                FROM public.video_game_titles
            ), groups AS (
                SELECT pid0,
                       key,
                       MIN(id) AS keep_id,
                       ARRAY_AGG(id ORDER BY id) AS all_ids
                FROM base
                WHERE key IS NOT NULL AND key <> ''
                GROUP BY pid0, key
                HAVING COUNT(*) > 1
            ), cand AS (
                SELECT g.keep_id,
                       vg.platform_id,
                       COALESCE(vg.edition,'') AS ed,
                       MIN(vg.id) AS keep_vg_id,
                       ARRAY_AGG(vg.id ORDER BY vg.id) AS vg_ids,
                       COUNT(*) AS ct
                FROM groups g
                JOIN public.video_games vg ON vg.title_id = ANY(g.all_ids)
                GROUP BY g.keep_id, vg.platform_id, COALESCE(vg.edition,'')
                HAVING COUNT(*) > 1
            ), moved_media AS (
                UPDATE public.game_media gm
                SET video_game_id = c.keep_vg_id
                FROM cand c
                WHERE gm.video_game_id = ANY(c.vg_ids) AND gm.video_game_id <> c.keep_vg_id
                RETURNING 1
            ), moved_links AS (
                UPDATE public.provider_media_links pml
                SET video_game_id = c.keep_vg_id
                FROM cand c
                WHERE pml.video_game_id IS NOT NULL AND pml.video_game_id = ANY(c.vg_ids) AND pml.video_game_id <> c.keep_vg_id
                RETURNING 1
            )
            DELETE FROM public.video_games vg
            USING cand c
            WHERE vg.id = ANY(c.vg_ids) AND vg.id <> c.keep_vg_id;
        "#;
        let _res_premerge_vg = sqlx::query(premerge_video_games_sql)
            .persistent(false)
            .execute(&mut *tx)
            .await?;

        // Now safely update remaining video_games to point to the keeper title_id
        let moved_video_games_sql = r#"
            WITH base AS (
                SELECT id,
                       COALESCE(product_id,0) AS pid0,
                       COALESCE(NULLIF(normalized_title,''), lower(title)) AS key
                FROM public.video_game_titles
            ), groups AS (
                SELECT pid0,
                       key,
                       MIN(id) AS keep_id,
                       ARRAY_AGG(id ORDER BY id) AS all_ids
                FROM base
                WHERE key IS NOT NULL AND key <> ''
                GROUP BY pid0, key
                HAVING COUNT(*) > 1
            )
            UPDATE public.video_games vg
            SET title_id = g.keep_id
            FROM groups g
            WHERE vg.title_id = ANY(g.all_ids) AND vg.title_id <> g.keep_id;
        "#;
        let _res_moved_vg = sqlx::query(moved_video_games_sql)
            .persistent(false)
            .execute(&mut *tx)
            .await?;

        // Finally delete duplicate titles
        let delete_titles_sql = r#"
            WITH base AS (
                SELECT id,
                       COALESCE(product_id,0) AS pid0,
                       COALESCE(NULLIF(normalized_title,''), lower(title)) AS key
                FROM public.video_game_titles
            ), groups AS (
                SELECT pid0,
                       key,
                       MIN(id) AS keep_id,
                       ARRAY_AGG(id ORDER BY id) AS all_ids
                FROM base
                WHERE key IS NOT NULL AND key <> ''
                GROUP BY pid0, key
                HAVING COUNT(*) > 1
            )
            DELETE FROM public.video_game_titles t
            USING groups g
            WHERE t.id = ANY(g.all_ids) AND t.id <> g.keep_id;
        "#;
        let res_titles = sqlx::query(delete_titles_sql)
            .persistent(false)
            .execute(&mut *tx)
            .await?;
        let deleted_titles = res_titles.rows_affected();

        tx.commit().await?;
        info!(
            deleted_titles, "deduplication complete (titles)"
        );
        Ok(())
    }

    /// Global deduplication after media ingestion. Consolidates duplicate video_games,
    /// game_media, and provider_media_links. Operates conservatively: only deletes rows
    /// where a clear keeper (MIN(id)) can be chosen and re-points all dependents first.
    async fn dedupe_all(&self) -> Result<()> {
        let pool = &self.db.pool;
        let mut tx = pool.begin().await?;

        // 1. video_games duplicates (same title_id, platform_id, edition NULL/empty)
        let vg_sql = r#"
            WITH base AS (
                SELECT id, title_id, platform_id, COALESCE(edition,'') AS ed
                FROM public.video_games
            ), groups AS (
                SELECT title_id, platform_id, ed,
                       MIN(id) AS keep_id,
                       ARRAY_AGG(id ORDER BY id) AS all_ids
                FROM base
                GROUP BY title_id, platform_id, ed
                HAVING COUNT(*) > 1
            ), moved_media AS (
                UPDATE public.game_media gm
                SET video_game_id = g.keep_id
                FROM groups g
                WHERE gm.video_game_id = ANY(g.all_ids) AND gm.video_game_id <> g.keep_id
                RETURNING 1
            ), moved_links AS (
                UPDATE public.provider_media_links pml
                SET video_game_id = g.keep_id
                FROM groups g
                WHERE pml.video_game_id IS NOT NULL AND pml.video_game_id = ANY(g.all_ids) AND pml.video_game_id <> g.keep_id
                RETURNING 1
            )
            DELETE FROM public.video_games vg
            USING groups g
            WHERE vg.id = ANY(g.all_ids) AND vg.id <> g.keep_id;
        "#;
        let res_vg = sqlx::query(vg_sql)
            .persistent(false)
            .execute(&mut *tx)
            .await?;
        let deleted_vg = res_vg.rows_affected();

        // 2. game_media duplicates.
        // game_media has a composite PRIMARY KEY (video_game_id, source, external_id) so duplicate rows
        // by that trio cannot exist. However, legacy imports may have produced multiple rows sharing
        // (video_game_id, source, url) but differing external_id. We collapse those by choosing the
        // lexicographically smallest external_id as keeper and deleting the rest. This avoids referencing
        // a non-existent surrogate id column.
        let media_sql = r#"
            WITH base AS (
                SELECT video_game_id, source, url, external_id
                FROM public.game_media
            ), groups AS (
                SELECT video_game_id, source, url,
                       MIN(external_id) AS keep_external_id,
                       ARRAY_AGG(external_id ORDER BY external_id) AS all_external_ids,
                       COUNT(*) AS ct
                FROM base
                WHERE url <> '' -- avoid invalid enum cast by not comparing source to ''
                GROUP BY video_game_id, source, url
                HAVING COUNT(*) > 1
            )
            DELETE FROM public.game_media gm
            USING groups g
            WHERE gm.video_game_id = g.video_game_id
              AND gm.source = g.source
              AND gm.url = g.url
              AND gm.external_id <> g.keep_external_id;
        "#;
        let res_media = sqlx::query(media_sql)
            .persistent(false)
            .execute(&mut *tx)
            .await?;
        let deleted_media = res_media.rows_affected();

        // 3. provider_media_links duplicates (same video_game_source_id, video_game_id, source, url)
        let links_sql = r#"
            WITH base AS (
                SELECT id, video_game_source_id, COALESCE(video_game_id,0) AS vg_id, source, COALESCE(url,'') AS url
                FROM public.provider_media_links
            ), groups AS (
                SELECT video_game_source_id, vg_id, source, url,
                       MIN(id) AS keep_id,
                       ARRAY_AGG(id ORDER BY id) AS all_ids
                FROM base
                WHERE url <> '' -- drop source <> '' to avoid enum comparison issues
                GROUP BY video_game_source_id, vg_id, source, url
                HAVING COUNT(*) > 1
            )
            DELETE FROM public.provider_media_links l
            USING groups g
            WHERE l.id = ANY(g.all_ids) AND l.id <> g.keep_id;
        "#;
        let res_links = sqlx::query(links_sql)
            .persistent(false)
            .execute(&mut *tx)
            .await?;
        let deleted_links = res_links.rows_affected();

        tx.commit().await?;
        info!(
            deleted_vg,
            deleted_media,
            deleted_links,
            "global dedupe complete (video_games/game_media/provider_media_links)"
        );
        Ok(())
    }

    // ----- Resume checkpoints -----
    async fn ensure_checkpoint_table(&self) -> Result<()> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS public.legacy_import_checkpoints (
                    source text PRIMARY KEY,
                    last_legacy_id bigint NOT NULL,
                    updated_at timestamptz NOT NULL DEFAULT now()
                )",
        )
        .persistent(false)
        .execute(&self.db.pool)
        .await?;
        Ok(())
    }

    // Generic stage completion helpers -------------------------------------------------
    async fn is_stage_done(&self, stage: &str) -> Result<bool> {
        let key = format!("{}:done", stage);
        // Allow override to force reimport of media stages even if done sentinel exists.
        // FORCE_REIMPORT_MEDIA=1 will cause game_images/game_videos stages to return false here.
        if stage == "game_images" || stage == "game_videos" {
            if let Ok(v) = std::env::var("FORCE_REIMPORT_MEDIA") {
                if v == "1" || v.eq_ignore_ascii_case("true") {
                    tracing::info!(stage, "FORCE_REIMPORT_MEDIA=1 — ignoring done sentinel");
                    return Ok(false);
                }
            }
            // Fine‑grained overrides: FORCE_REIMPORT_IMAGES / FORCE_REIMPORT_VIDEOS
            if stage == "game_images" {
                if let Ok(v) = std::env::var("FORCE_REIMPORT_IMAGES") {
                    if v == "1" || v.eq_ignore_ascii_case("true") {
                        tracing::info!(stage, "FORCE_REIMPORT_IMAGES=1 — ignoring done sentinel");
                        return Ok(false);
                    }
                }
            }
            if stage == "game_videos" {
                if let Ok(v) = std::env::var("FORCE_REIMPORT_VIDEOS") {
                    if v == "1" || v.eq_ignore_ascii_case("true") {
                        tracing::info!(stage, "FORCE_REIMPORT_VIDEOS=1 — ignoring done sentinel");
                        return Ok(false);
                    }
                }
            }
        }
        // Generic multi-stage override: IMPORT_RESET=stageA,stageB (without :done suffix)
        if let Ok(reset_list) = std::env::var("IMPORT_RESET") {
            if !reset_list.trim().is_empty() {
                let should_reset = reset_list.split(',').map(|s| s.trim()).any(|s| s == stage);
                if should_reset {
                    tracing::info!(
                        stage,
                        reset_list,
                        "IMPORT_RESET override — ignoring done sentinel"
                    );
                    return Ok(false);
                }
            }
        }
        if let Some(row) = sqlx::query(
            "SELECT last_legacy_id FROM public.legacy_import_checkpoints WHERE source=$1",
        )
        .persistent(false)
        .bind(&key)
        .fetch_optional(&self.db.pool)
        .await?
        {
            let v: i64 = row.get("last_legacy_id");
            Ok(v == 1)
        } else {
            Ok(false)
        }
    }
    async fn mark_stage_done(&self, stage: &str) -> Result<()> {
        let key = format!("{}:done", stage);
        sqlx::query(
            "INSERT INTO public.legacy_import_checkpoints (source,last_legacy_id) VALUES ($1,1)
                 ON CONFLICT (source) DO UPDATE SET last_legacy_id=1, updated_at=now()",
        )
        .persistent(false)
        .bind(&key)
        .execute(&self.db.pool)
        .await?;
        Ok(())
    }

    async fn get_checkpoint(&self, source: &str) -> Result<i64> {
        // Global kill‑switch (default on): set IMPORT_RESUME=0 to ignore durable checkpoints
        let resume = std::env::var("IMPORT_RESUME")
            .map(|v| !(v == "0" || v.eq_ignore_ascii_case("false")))
            .unwrap_or(true);
        if !resume {
            tracing::info!(source, "IMPORT_RESUME=0 set — ignoring durable checkpoint");
            return Ok(0);
        }
        if let Some(row) = sqlx::query(
            "SELECT last_legacy_id FROM public.legacy_import_checkpoints WHERE source=$1",
        )
        .persistent(false)
        .bind(source)
        .fetch_optional(&self.db.pool)
        .await?
        {
            let ckpt = row.get::<i64, _>("last_legacy_id");
            tracing::info!(source, resume_min_id = ckpt, "loaded durable checkpoint");
            Ok(ckpt)
        } else {
            tracing::info!(
                source,
                "no durable checkpoint found — starting from beginning"
            );
            Ok(0)
        }
    }

    async fn save_checkpoint(&self, source: &str, last_id: i64) -> Result<()> {
        sqlx::query(
            "INSERT INTO public.legacy_import_checkpoints (source, last_legacy_id)
                 VALUES ($1,$2)
                 ON CONFLICT (source) DO UPDATE
                 SET last_legacy_id=EXCLUDED.last_legacy_id, updated_at=now()",
        )
        .persistent(false)
        .bind(source)
        .bind(last_id)
        .execute(&self.db.pool)
        .await?;
        Ok(())
    }

    // ----- Title cache -----
    async fn prefill_title_cache(&mut self) -> Result<()> {
        let rows = sqlx
            ::query(
                "SELECT id, COALESCE(normalized_title, lower(title)) AS key FROM public.video_game_titles"
            )
            .persistent(false)
            .fetch_all(&self.db.pool).await?;
        for r in rows {
            let id: i64 = r.get("id");
            let key: Option<String> = r.get("key");
            if let Some(k) = key {
                self.title_id_by_norm.insert(k, id);
            }
        }
        Ok(())
    }

    // Resolve provider id by slug with in-memory cache. Creates a 'media' kind provider if missing.
    async fn provider_id_for_slug(&mut self, slug: &str) -> Result<i64> {
        if let Some(id) = self.provider_id_by_slug.get(slug) {
            return Ok(*id);
        }
        // Fast-path lookup
        if let Some(id) =
            sqlx::query_scalar::<_, i64>("SELECT id FROM public.providers WHERE slug=$1")
                .persistent(false)
                .bind(slug)
                .fetch_optional(&self.cache.db().pool)
                .await?
        {
            self.provider_id_by_slug.insert(slug.to_string(), id);
            return Ok(id);
        }
        // Lazily create
        let id = i_miss_rust::database_ops::ingest_providers::ensure_provider(
            self.cache.db(),
            slug,
            "media",
            Some(slug),
        )
        .await?;
        self.provider_id_by_slug.insert(slug.to_string(), id);
        Ok(id)
    }

    async fn prefill_existing_slugs(&mut self) -> Result<()> {
        // Prefill product slugs + id mapping in one pass.
        let mut prod_stream = sqlx::query(
            "SELECT id, slug::text AS slug FROM public.products WHERE slug IS NOT NULL",
        )
        .persistent(false)
        .fetch(&self.db.pool);
        while let Some(row) = prod_stream.try_next().await? {
            let id: i64 = row.try_get("id")?;
            if let Ok(slug) = row.try_get::<String, _>("slug") {
                self.product_slugs.insert(slug.clone());
                self.product_slug_to_id.insert(slug, id);
            }
        }
        let mut vg_stream =
            sqlx::query("SELECT slug::text AS slug FROM public.video_games WHERE slug IS NOT NULL")
                .persistent(false)
                .fetch(&self.db.pool);
        while let Some(row) = vg_stream.try_next().await? {
            if let Ok(slug) = row.try_get::<String, _>("slug") {
                self.video_game_slugs.insert(slug);
            }
        }
        // Prefill id->slug map for existing video_games to prevent slug churn on re-runs
        let existing_vgs = sqlx::query(
            "SELECT id, slug::text AS slug FROM public.video_games WHERE slug IS NOT NULL",
        )
        .persistent(false)
        .fetch_all(&self.db.pool)
        .await?;
        for r in existing_vgs {
            let id: i64 = r.get("id");
            if let Ok(slug) = r.try_get::<String, _>("slug") {
                self.vg_slug_by_id.insert(id, slug);
            }
        }
        // Prefill existing titles + video_game combos for batching ensures.
        let titles = sqlx
            ::query(
                "SELECT id, video_game_id FROM public.video_game_titles WHERE video_game_id IS NOT NULL"
            )
            .persistent(false)
            .fetch_all(&self.db.pool).await?;
        for r in titles {
            let tid: i64 = r.get("id");
            let pid: i64 = r.get("video_game_id");
            self.title_by_product.insert(pid, tid);
        }
        let vg_rows = sqlx::query("SELECT id, title_id, platform_id FROM public.video_games")
            .persistent(false)
            .fetch_all(&self.db.pool)
            .await?;
        for r in vg_rows {
            let id: i64 = r.get("id");
            let title_id: i64 = r.get("title_id");
            let platform_id: i64 = r.get("platform_id");
            self.vg_combo_map.insert((title_id, platform_id), id);
            self.vg_platform_by_vg.insert(id, platform_id);
        }
        Ok(())
    }

    fn platform_label_for_product(&self, legacy_product_id: i64) -> String {
        if let Some(platform_ids) = self.product_platforms.get(&legacy_product_id) {
            if platform_ids.len() == 1 {
                if let Some(code) = self.platform_slug_by_pg.get(&platform_ids[0]) {
                    return code.clone();
                }
            } else if !platform_ids.is_empty() {
                return "multi".to_string();
            }
        }
        "unknown".to_string()
    }

    // ---------- Imports with progress ----------

    async fn import_currencies(&mut self) -> Result<()> {
        if self.is_stage_done("currencies").await? {
            info!("stage currencies:done — skipping import");
            return Ok(());
        }
        let total = sqlite_count(&self.sqlite, "currencies")
            .await
            .ok()
            .map(|n| n as usize);
        let mut prog = Progress::new("currencies", total);
        info!("importing currencies");

        let has_minor_unit = self.columns_for("currencies").await?.contains("minor_unit");
        let mut stream = sqlx::query_as::<_, LegacyCurrency>(
            r#"SELECT id, code, name, decimals FROM currencies ORDER BY id"#,
        )
        .persistent(false)
        .fetch(&self.sqlite);
        let batch_size = lookup_batch_size();
        let mut buffer: Vec<LegacyCurrency> = Vec::with_capacity(batch_size);
        let mut count = 0usize;
        while let Some(row) = stream.try_next().await? {
            buffer.push(row);
            if buffer.len() >= batch_size {
                let processed = self
                    .clone()
                    .process_currency_chunk(std::mem::take(&mut buffer), has_minor_unit, &mut prog)
                    .await?;
                count += processed;
            }
        }
        if !buffer.is_empty() {
            count += self
                .clone()
                .process_currency_chunk(buffer, has_minor_unit, &mut prog)
                .await?;
        }
        prog.finish();
        info!(
            count,
            batch_size, "imported currencies with batched upserts"
        );
        Ok(())
    }

    async fn import_platforms(&mut self) -> Result<()> {
        if self.is_stage_done("platforms").await? {
            info!("stage platforms:done — skipping import");
            return Ok(());
        }
        let total = sqlite_count(&self.sqlite, "platforms")
            .await
            .ok()
            .map(|n| n as usize);
        let mut prog = Progress::new("platforms", total);
        info!("importing platforms");

        let mut stream = sqlx::query_as::<_, LegacyPlatform>(
            r#"SELECT id, code, name, family FROM platforms ORDER BY id"#,
        )
        .persistent(false)
        .fetch(&self.sqlite);

        let pool = &self.db.pool;
        let mut count = 0usize;

        while let Some(row) = stream.try_next().await? {
            let name = row.name.as_deref().map(str::trim).unwrap_or("").to_string();
            if name.is_empty() {
                warn!(legacy_id=?row.id, "skipping platform with empty name");
                prog.tick(1);
                continue;
            }
            let mut code = row
                .code
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(|s| s.to_ascii_lowercase())
                .unwrap_or_else(|| simple_slug(&name));

            if code.is_empty() {
                code = format!("platform-{}", row.id);
            }

            let pg_id = ensure_platform(self.cache.db(), &name, Some(&code)).await?;
            if let Err(e) = sqlx::query("UPDATE public.platforms SET code=$1 WHERE id=$2")
                .persistent(false)
                .bind(&code)
                .bind(pg_id)
                .execute(pool)
                .await
            {
                match &e {
                    sqlx::Error::Database(db_err)
                        if db_err.code().map(|c| c.to_string()) == Some("23505".into()) =>
                    {
                        tracing::warn!(pg_id, %name, %code, constraint=?db_err.constraint(), "skip code update (UNIQUE)");
                    }
                    _ => {
                        return Err(e.into());
                    }
                }
            }

            if let Some(family) = row
                .family
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
            {
                let _ = sqlx::query("UPDATE public.platforms SET family=$1 WHERE id=$2")
                    .persistent(false)
                    .bind(family)
                    .bind(pg_id)
                    .execute(pool)
                    .await;
            }

            self.platform_by_legacy.insert(row.id, pg_id);
            self.platform_by_code.insert(code.clone(), pg_id);
            self.platform_slug_by_pg.insert(pg_id, code);
            count += 1;
            prog.tick(1);
        }

        prog.finish();
        info!("imported {count} platforms");
        Ok(())
    }

    async fn import_countries(&mut self) -> Result<()> {
        if self.is_stage_done("countries").await? {
            info!("stage countries:done — skipping import");
            return Ok(());
        }
        let total = sqlite_count(&self.sqlite, "countries")
            .await
            .ok()
            .map(|n| n as usize);
        let mut prog = Progress::new("countries", total);
        info!("importing countries");

        let iso2_to_iso3 = load_iso2_to_iso3(&self.db.pool).await?;

        let sqlite_pool = self.sqlite.clone();
        let mut stream = sqlx::query_as::<_, LegacyCountry>(
            r#"SELECT id, code, name, currency_id FROM countries ORDER BY id"#,
        )
        .persistent(false)
        .fetch(&sqlite_pool);

        let batch_size = lookup_batch_size();
        let mut buffer: Vec<LegacyCountry> = Vec::with_capacity(batch_size);
        let mut count = 0usize;
        while let Some(row) = stream.try_next().await? {
            buffer.push(row);
            if buffer.len() >= batch_size {
                let processed = self
                    .process_country_chunk(std::mem::take(&mut buffer), &iso2_to_iso3, &mut prog)
                    .await?;
                count += processed;
            }
        }
        if !buffer.is_empty() {
            count += self
                .process_country_chunk(buffer, &iso2_to_iso3, &mut prog)
                .await?;
        }

        prog.finish();
        info!(count, batch_size, "imported countries with batched upserts");
        Ok(())
    }

    async fn load_product_platform_links(&mut self) -> Result<()> {
        let total = sqlite_count(&self.sqlite, "game_platform")
            .await
            .ok()
            .map(|n| n as usize);
        let mut prog = Progress::new("product↔platform links", total);

        let mut stream = sqlx::query_as::<_, LegacyGamePlatform>(
            r#"SELECT product_id, platform_id FROM game_platform"#,
        )
        .persistent(false)
        .fetch(&self.sqlite);

        while let Some(row) = stream.try_next().await? {
            if let Some(pg_platform) = self.platform_by_legacy.get(&row.platform_id) {
                self.product_platforms
                    .entry(row.product_id)
                    .or_default()
                    .push(*pg_platform);
            }
            prog.tick(1);
        }
        for platforms in self.product_platforms.values_mut() {
            platforms.sort_unstable();
            platforms.dedup();
        }
        prog.finish();
        Ok(())
    }

    async fn bootstrap_product_from_video_game(
        &mut self,
        legacy_product_id: i64,
        row: &LegacyVideoGame,
        mut product: ProductInfo,
    ) -> Result<ProductInfo> {
        if product.pg_id > 0 {
            return Ok(product);
        }

        let slug_hint = row
            .slug
            .as_deref()
            .filter(|s| !s.trim().is_empty())
            .or_else(|| {
                let trimmed = product.slug.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed)
                }
            });
        let slug_base = resolve_product_slug_base(slug_hint, &product.title, legacy_product_id);
        let slug = if let Some(existing) = self.product_slug_by_legacy.get(&legacy_product_id) {
            existing.clone()
        } else {
            let next = if self.product_slugs.contains(&slug_base) {
                unique_slug_into(&slug_base, &mut self.product_slugs)
            } else {
                self.product_slugs.insert(slug_base.clone());
                slug_base.clone()
            };
            self.product_slug_by_legacy
                .insert(legacy_product_id, next.clone());
            next
        };

        let platform_label = self.platform_label_for_product(legacy_product_id);
        let product_id = ensure_product_named_with_platform(
            self.cache.db(),
            &product.kind,
            &slug,
            &product.title,
            platform_label.as_str(),
        )
        .await?;
        ensure_software_row(self.cache.db(), product_id).await?;

        self.product_slug_to_id.insert(slug.clone(), product_id);

        product.pg_id = product_id;
        product.slug = slug;
        self.product_map.insert(legacy_product_id, product.clone());
        Ok(product)
    }

    async fn import_products(&mut self) -> Result<()> {
        if self.is_stage_done("products").await? {
            info!("stage products:done — skipping import");
            return Ok(());
        }
        // Determine resume point
        let mut min_id = self.get_checkpoint("products").await?;
        if let Ok(min_str) = std::env::var("PRODUCT_ID_MIN") {
            if let Ok(v) = min_str.parse::<i64>() {
                min_id = min_id.max(v);
            }
        }
        tracing::info!(resume_min_id = min_id, env_override = %std::env::var("PRODUCT_ID_MIN").unwrap_or_default(), "products stage resume parameters");
        // Build WHERE clause string separately to avoid temporary reference lifetime issues
        let where_clause = if min_id > 0 {
            Some(format!("id > {}", min_id))
        } else {
            None
        };
        let total = sqlite_count_where(&self.sqlite, "products", where_clause.as_deref())
            .await
            .ok()
            .map(|n| n as usize);
        let mut prog = Progress::new("products", total);

        // Ensure provider and source exist for lineage tracking
        let provider_id = self.provider_id_for_slug("sqlite_import").await?;
        let sqlite_source_id = ensure_video_game_source(
            self.cache.db(),
            "sqlite_import",
            "SQLite Import",
        ).await?;

        let mut stream = if min_id > 0 {
            sqlx::query_as::<_, LegacyProduct>(
                "SELECT id, name, slug, category, release_date, synopsis, metadata, CAST(popularity_score AS REAL) AS popularity_score, CAST(rating AS REAL) AS rating FROM products WHERE id > ? ORDER BY id"
            )
                .persistent(false)
                .bind(min_id)
                .fetch(&self.sqlite)
        } else {
            sqlx::query_as::<_, LegacyProduct>(
                "SELECT id, name, slug, category, release_date, synopsis, metadata, CAST(popularity_score AS REAL) AS popularity_score, CAST(rating AS REAL) AS rating FROM products ORDER BY id"
            )
                .persistent(false)
                .fetch(&self.sqlite)
        };

        let mut processed = 0usize;
        let mut legacy_software_total = 0usize;
        let mut legacy_hardware_total = 0usize;
        // Local title cache to avoid DB lookups without mutably borrowing self while stream is alive
        let mut title_cache = self.title_id_by_norm.clone();

        let checkpoint_every: usize = std::env::var("CHECKPOINT_EVERY")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(1000);
        let mut last_id: i64 = min_id;
        while let Some(row) = stream.try_next().await? {
            last_id = row.id;

            let name = row.name.trim();
            if name.is_empty() {
                prog.tick(1);
                continue;
            }

            let kind = normalize_product_kind(row.category.as_deref()).to_string();
            if kind == "software" {
                legacy_software_total += 1;
            } else {
                legacy_hardware_total += 1;
            }

            let base_slug = resolve_product_slug_base(row.slug.as_deref(), name, row.id);
            let slug = if let Some(existing) = self.product_slug_by_legacy.get(&row.id) {
                existing.clone()
            } else if self.product_slug_to_id.contains_key(&base_slug) {
                base_slug.clone()
            } else {
                unique_slug_into(&base_slug, &mut self.product_slugs)
            };
            self.product_slug_by_legacy.insert(row.id, slug.clone());
            self.product_slugs.insert(slug.clone());

            // Ensure a canonical product row exists (create on-demand when slug not already mapped).
            let platform_label = self.platform_label_for_product(row.id);
            let product_id_value = match self.product_slug_to_id.get(&slug).copied() {
                Some(pid) => pid,
                None => {
                    let pid = ensure_product_named_with_platform(
                        self.cache.db(),
                        &kind,
                        &slug,
                        name,
                        platform_label.as_str(),
                    )
                    .await?;
                    self.product_slug_to_id.insert(slug.clone(), pid);
                    pid
                }
            };
            let product_id = Some(product_id_value);

            let mut title_id = None;
            if kind == "software" {
                // 1. Ensure the provider item exists for lineage
                let _vg_source_item_id = self.cache.ensure_provider_item(
                    provider_id,
                    &row.id.to_string(),
                    Some(serde_json::json!({
                        "legacy_id": row.id,
                        "legacy_slug": row.slug,
                        "legacy_name": name,
                        "imported_at": chrono::Utc::now().to_rfc3339()
                    })),
                    false,
                ).await?;

                // 2. Ensure title exists and is linked to the source item
                let tid: i64 = if let Some(pid) = product_id {
                    if let Some(tid) = self.title_by_product.get(&pid) {
                        // Even if we have the title cached, we must ensure the link exists
                        // But ensure_video_game_title_for_source_item does both.
                        // To be safe and ensure lineage, we call it.
                        // It should be fast if already linked.
                        let slug_hint = if slug.trim().is_empty() {
                            None
                        } else {
                            Some(slug.as_str())
                        };
                        
                        let tid = self.cache.ensure_video_game_title_for_source_item(
                            sqlite_source_id,
                            &row.id.to_string(),
                            Some(pid),
                            None,
                            name,
                            slug_hint,
                            None,
                            None,
                        ).await?;
                        
                        *self.title_by_product.entry(pid).or_insert(tid)
                    } else {
                        let slug_hint = if slug.trim().is_empty() {
                            None
                        } else {
                            Some(slug.as_str())
                        };
                        let tid = self
                            .cache
                            .ensure_video_game_title_for_source_item(
                                sqlite_source_id,
                                &row.id.to_string(),
                                Some(pid),
                                None,
                                name,
                                slug_hint,
                                None,
                                None,
                            )
                            .await?;
                        self.title_by_product.insert(pid, tid);
                        tid
                    }
                } else {
                    // No product available: create placeholder product to satisfy video_game_id NOT NULL constraint
                    // (migration 0490 requires video_game_id to be NOT NULL and reference a product)
                    let norm = normalize_title(name);
                    if let Some(id) = title_cache.get(&norm).copied() {
                        id
                    } else {
                        // Create auto-generated product slug to ensure uniqueness
                        let auto_slug = format!("auto-import-{}", Uuid::new_v4().simple());
                        let pid = self
                            .cache
                            .ensure_product_named("software", &auto_slug, name)
                            .await?;

                        // Now create title with proper product linkage
                        let tid = self
                            .cache
                            .ensure_video_game_title_for_source_item(
                                sqlite_source_id,
                                &row.id.to_string(),
                                Some(pid),
                                None,
                                name,
                                Some(&norm),
                                None,
                                None,
                            )
                            .await?;

                        title_cache.insert(norm, tid);
                        self.title_by_product.insert(pid, tid);
                        tid
                    }
                };
                title_id = Some(tid);
            }

            let release_date = row.release_date.as_deref().and_then(parse_date);
            let metadata = row
                .metadata
                .as_deref()
                .and_then(|raw| serde_json::from_str::<Value>(raw).ok());

            self.product_map.insert(
                row.id,
                ProductInfo {
                    pg_id: product_id_value,
                    slug: slug.clone(),
                    title: name.to_string(),
                    kind: kind.clone(),
                    release_date,
                    synopsis: row
                        .synopsis
                        .as_deref()
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .map(|s| s.to_string()),
                    popularity_score: row.popularity_score.unwrap_or(0.0),
                    rating: row.rating.unwrap_or(0.0),
                    metadata,
                    title_id,
                },
            );

            processed += 1;
            prog.tick(1);
            if processed == 1 || processed % 1000 == 0 {
                self.cache.clear();
            }
            if processed % checkpoint_every == 0 {
                self.save_checkpoint("products", last_id).await?;
            }
        }
        // Final checkpoint save
        if last_id > min_id {
            self.save_checkpoint("products", last_id).await?;
        }
        // Merge any new cache entries back into self after stream is dropped
        self.title_id_by_norm.extend(title_cache.into_iter());
        prog.finish();
        self.stats.products = processed;
        self.stats.software_products = legacy_software_total;
        self.stats.hardware_products = legacy_hardware_total;
        info!(
            total = self.stats.products,
            software = self.stats.software_products,
            hardware = self.stats.hardware_products,
            "products processed (no new products created)"
        );
        Ok(())
    }

    async fn import_video_games(&mut self) -> Result<()> {
        if self.is_stage_done("video_games").await? {
            info!("stage video_games:done — skipping import");
            return Ok(());
        }
        info!("importing video game instances");

        // Optional resume / limiting controls:
        //   VIDEO_GAME_ID_MIN=<legacy id>  : skip rows with legacy id lower than this (fast resume)
        //   VIDEO_GAME_LIMIT_NEW=<n>       : stop after importing (creating) n NEW (title,platform) combos (dedup preserved)
        //   VIDEO_GAME_SKIP_EXISTING_ONLY=1 : if set, we short-circuit rows whose (title_id,platform_id) already exist without building update payload (faster resume if enrichment already applied)
        let vg_id_min: Option<i64> = std::env::var("VIDEO_GAME_ID_MIN")
            .ok()
            .and_then(|s| s.parse().ok());
        let vg_limit_new: Option<usize> = std::env::var("VIDEO_GAME_LIMIT_NEW")
            .ok()
            .and_then(|s| s.parse().ok());
        let skip_existing_fast: bool = std::env::var("VIDEO_GAME_SKIP_EXISTING_ONLY")
            .map(|v| (v == "1" || v.eq_ignore_ascii_case("true")))
            .unwrap_or(false);

        // Ensure an 'unknown' catch-all platform so we never drop a game solely due to missing mapping.
        let unknown_platform_id = {
            let key = "unknown".to_string();
            if let Some(pid) = self.platform_by_code.get(&key) {
                *pid
            } else {
                let pg_id = ensure_platform(self.cache.db(), &key, Some(&key)).await?;
                self.platform_by_code.insert(key.clone(), pg_id);
                self.platform_slug_by_pg.insert(pg_id, key);
                pg_id
            }
        };

        let vg_cols = self.columns_for("video_games").await?;
        let vg_have_genres = vg_cols.contains("genres");
        let vg_have_region_codes = vg_cols.contains("region_codes");
        let vg_have_metadata = vg_cols.contains("metadata");
        let vg_have_display = vg_cols.contains("display_title");
        let vg_have_synopsis = vg_cols.contains("synopsis");
        let vg_have_avg_rating = vg_cols.contains("average_rating");
        let vg_have_rating_count = vg_cols.contains("rating_count");
        let vg_have_rating_updated = vg_cols.contains("rating_updated_at");

        // Determine resume point via durable checkpoint, then apply optional env minimum
        let mut min_id = self.get_checkpoint("video_games").await?;
        if let Some(env_min) = vg_id_min {
            min_id = min_id.max(env_min);
        }
        tracing::info!(resume_min_id = min_id, env_override = ?vg_id_min, "video_games stage resume parameters");

        // Compute total (optionally filtered by resume id) for better progress visibility
        let total_filtered = if min_id > 0 {
            sqlite_count_where(
                &self.sqlite,
                "video_games",
                Some(&format!("id > {}", min_id)),
            )
            .await
            .ok()
        } else {
            sqlite_count(&self.sqlite, "video_games").await.ok()
        };
        // Stream rows to avoid large memory spikes on big datasets
        let mut prog = Progress::new("video_games", total_filtered.map(|n| n as usize));
        let pool = &self.db.pool.clone();
        
        // Ensure provider exists for lineage
        let provider_id = self.provider_id_for_slug("sqlite_import").await?;
        let sqlite_source_id = ensure_video_game_source(
            self.cache.db(),
            "sqlite_import",
            "SQLite Import",
        ).await?;
        
        let chunk = update_chunk_size();
        let mut updates: Vec<VgUpd> = Vec::with_capacity(chunk);
        let mut flushed_batches = 0usize;

        // Collect normalized title updates (legacy override) for batching
        let mut vgt_title_updates: Vec<(i64, String)> = Vec::new();
        // Local cache for title ensures to avoid &mut self during active stream borrow
        let mut vgt_title_cache = self.title_id_by_norm.clone();
        let mut vg_flush_durations: Vec<std::time::Duration> = Vec::new();
        let mut skipped_existing = 0usize; // count of (title_id, platform_id) combinations already present
        let sqlite_pool = self.sqlite.clone();
        let mut stream = if min_id > 0 {
            sqlx::query_as::<_, LegacyVideoGame>(
                r#"
                SELECT
                    id, product_id, title, genre, release_date, developer, metadata, slug,
                    normalized_title, external_ids, platform_codes, region_codes
                FROM video_games
                WHERE id > ?
                ORDER BY id
            "#,
            )
            .persistent(false)
            .bind(min_id)
            .fetch(&sqlite_pool)
        } else {
            sqlx::query_as::<_, LegacyVideoGame>(
                r#"
                SELECT
                    id, product_id, title, genre, release_date, developer, metadata, slug,
                    normalized_title, external_ids, platform_codes, region_codes
                FROM video_games
                ORDER BY id
            "#,
            )
            .persistent(false)
            .fetch(&sqlite_pool)
        };

        // Checkpoint cadence
        let checkpoint_every: usize = std::env::var("CHECKPOINT_EVERY")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(1000);
        let mut seen_rows: usize = 0;
        let mut last_id: i64 = min_id;

        while let Some(row) = stream.try_next().await? {
            last_id = row.id;
            seen_rows += 1;
            if seen_rows % checkpoint_every == 0 {
                self.save_checkpoint("video_games", last_id).await?;
            }
            // Prefer mapped product metadata if present, but do NOT require it.
            // If the legacy product isn't mapped, synthesize a minimal product context from the SQLite row
            // so we can still build video_games purely from SQLite data.
            let fallback_product = if let Some(p) = self.product_map.get(&row.product_id).cloned() {
                p
            } else {
                let fallback_title = row
                    .title
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .unwrap_or("untitled")
                    .to_string();
                let fallback_release = row.release_date.as_deref().and_then(parse_date);
                let fallback_meta = row
                    .metadata
                    .as_deref()
                    .and_then(|raw| serde_json::from_str::<Value>(raw).ok());
                ProductInfo {
                    pg_id: 0,
                    slug: String::new(),
                    title: fallback_title,
                    kind: "software".to_string(),
                    release_date: fallback_release,
                    synopsis: None,
                    popularity_score: 0.0,
                    rating: 0.0,
                    metadata: fallback_meta,
                    title_id: None,
                }
            };

            let mut product = self.clone()
                .bootstrap_product_from_video_game(row.product_id, &row, fallback_product)
                .await?;

            // Resolve/ensure a title_id even when product has no mapped title.
            let title_string = row
                .title
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or(&product.title)
                .to_string();
            let slug_hint_owned = row
                .slug
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .or_else(|| {
                    let trimmed = product.slug.trim();
                    if trimmed.is_empty() {
                        None
                    } else {
                        Some(trimmed.to_string())
                    }
                });
            let title_id: i64 = if let Some(tid) = product.title_id {
                tid
            } else {
                // Ensure provider item for this video game row
                let _vg_source_item_id = self.cache.ensure_provider_item(
                    provider_id,
                    &row.id.to_string(),
                    Some(serde_json::json!({
                        "legacy_video_game_id": row.id,
                        "legacy_product_id": row.product_id,
                        "imported_at": chrono::Utc::now().to_rfc3339()
                    })),
                    false,
                ).await?;

                let pid_opt = if product.pg_id > 0 { Some(product.pg_id) } else { None };

                let tid = self
                    .cache
                    .ensure_video_game_title_for_source_item(
                        sqlite_source_id,
                        &row.id.to_string(),
                        pid_opt,
                        None,
                        &title_string,
                        slug_hint_owned.as_deref(),
                        None,
                        None,
                    )
                    .await?;
                let norm_key = normalize_title(&title_string);
                vgt_title_cache.insert(norm_key.clone(), tid);
                self.title_id_by_norm.insert(norm_key, tid);
                self.title_by_product.insert(product.pg_id, tid);
                product.title_id = Some(tid);
                product.title = title_string.clone();
                tid
            };

            self.product_map.insert(row.product_id, product.clone());

            if !self.skip_titles {
                if let Some(norm) = row
                    .normalized_title
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                {
                    vgt_title_updates.push((title_id, norm.to_string()));
                }
            }

            let mut platform_ids = self
                .product_platforms
                .get(&row.product_id)
                .cloned()
                .unwrap_or_default();

            // Always attempt to derive from platform_codes regardless of existing links, to expand coverage.
            if let Some(ref codes_raw) = row.platform_codes {
                // Normal path: try parse_string_array (expected JSON array or delimiter-aware implementation)
                let mut codes: Vec<String> = Vec::new();
                if let Some(parsed) = parse_string_array(codes_raw) {
                    codes.extend(parsed);
                } else {
                    // Fallback: split on commas / semicolons / whitespace
                    for part in
                        codes_raw.split(|c: char| (c == ',' || c == ';' || c.is_whitespace()))
                    {
                        let p = part.trim();
                        if !p.is_empty() {
                            codes.push(p.to_string());
                        }
                    }
                }
                for code in codes {
                    let lc = code.trim().to_ascii_lowercase();
                    if lc.is_empty() {
                        continue;
                    }
                    let pid = if let Some(pid) = self.platform_by_code.get(&lc) {
                        *pid
                    } else {
                        match ensure_platform(self.cache.db(), &lc, Some(&lc)).await {
                            Ok(new_pid) => {
                                self.platform_by_code.insert(lc.clone(), new_pid);
                                self.platform_slug_by_pg.insert(new_pid, lc.clone());
                                new_pid
                            }
                            Err(e) => {
                                warn!(legacy_video_game_id=?row.id, ?lc, error=?e, "failed to ensure platform for code");
                                continue;
                            }
                        }
                    };
                    platform_ids.push(pid);
                }
            }

            if platform_ids.is_empty() {
                // Assign unknown catch-all so we do not drop this game entirely.
                platform_ids.push(unknown_platform_id);
                info!(legacy_video_game_id=?row.id, legacy_product_id=?row.product_id, "assigned unknown platform (no mapping resolved)");
            }
            platform_ids.sort_unstable();
            platform_ids.dedup();

            let release_date = row
                .release_date
                .as_deref()
                .and_then(parse_date)
                .or(product.release_date);
            let mut metadata = row
                .metadata
                .as_deref()
                .and_then(|raw| serde_json::from_str::<Value>(raw).ok());
            if metadata.is_none() {
                metadata = product.metadata.clone();
            }

            // Extract enrichments from metadata JSON if present
            let (
                meta_synopsis,
                meta_developer,
                meta_genres,
                meta_regions,
                meta_popularity,
                meta_rating,
                meta_avg_rating,
                meta_rating_count,
                meta_rating_updated_at,
            ) = if let Some(Value::Object(obj)) = metadata.clone() {
                let synopsis = obj
                    .get("synopsis")
                    .and_then(|v| v.as_str())
                    .map(|s| s.trim().to_string());
                let developer_meta = obj
                    .get("developer")
                    .and_then(|v| v.as_str())
                    .map(|s| s.trim().to_string());
                let genres_meta = obj.get("genres").and_then(|v| v.as_array()).map(|arr| {
                    arr.iter()
                        .filter_map(|x| x.as_str())
                        .map(|s| simple_slug(s))
                        .collect::<Vec<_>>()
                });
                let regions_meta = obj
                    .get("region_codes")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|x| x.as_str())
                            .map(|s| simple_slug(s))
                            .collect::<Vec<_>>()
                    });
                let popularity = obj.get("popularity_score").and_then(|v| v.as_f64());
                let rating_val = obj.get("rating").and_then(|v| v.as_f64());
                let avg_rating = obj
                    .get("average_rating")
                    .and_then(|v| v.as_f64())
                    .map(|f| f as f32);
                let rating_count = obj.get("rating_count").and_then(|v| v.as_i64());
                let rating_updated = obj
                    .get("rating_updated_at")
                    .and_then(|v| v.as_str())
                    .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                    .map(|dt| dt.with_timezone(&chrono::Utc));
                (
                    synopsis,
                    developer_meta,
                    genres_meta,
                    regions_meta,
                    popularity,
                    rating_val,
                    avg_rating,
                    rating_count,
                    rating_updated,
                )
            } else {
                (None, None, None, None, None, None, None, None, None)
            };

            let developer = meta_developer.or_else(|| {
                row.developer
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_string())
            });
            let region_codes =
                meta_regions.or_else(|| row.region_codes.as_deref().and_then(parse_string_array));
            let genres = meta_genres.or_else(|| row.genre.as_deref().and_then(parse_string_array));
            let display_title = row
                .title
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string());

            for platform_id in platform_ids {
                // Batch ensure video_game row for (title_id, platform_id)
                let key = (title_id, platform_id);
                let existing = self.vg_combo_map.get(&key).copied();
                let vg_id = if let Some(id) = existing {
                    // Already present; skip insert; keep slug stable
                    id
                } else {
                    // Insert row; attempt batched insert for single key by multi-row builder (extendable later)
                    let rows = sqlx
                        ::query(
                            "INSERT INTO public.video_games (title_id, platform_id) VALUES ($1,$2) ON CONFLICT (title_id, platform_id) WHERE edition IS NULL DO NOTHING RETURNING id"
                        )
                        .bind(title_id)
                        .bind(platform_id)
                        .persistent(false)
                        .fetch_all(pool).await?;
                    let inserted_id = if let Some(r) = rows.get(0) {
                        r.get::<i64, _>("id")
                    } else {
                        // Conflict: fetch existing id
                        sqlx
                            ::query_scalar::<_, i64>(
                                "SELECT id FROM public.video_games WHERE title_id=$1 AND platform_id=$2 AND edition IS NULL"
                            )
                            .bind(title_id)
                            .bind(platform_id)
                            .persistent(false)
                            .fetch_one(pool).await?
                    };
                    self.vg_combo_map.insert(key, inserted_id);
                    inserted_id
                };

                // Record mapping from legacy sqlite video_games.id -> new PG video_games.id (first seen)
                if !self.legacy_vg_map.contains_key(&row.id) {
                    self.legacy_vg_map.insert(row.id, vg_id);
                }

                let platform_slug = self
                    .platform_slug_by_pg
                    .get(&platform_id)
                    .map(String::as_str)
                    .unwrap_or("");

                // Avoid slug mutation for existing rows (idempotent reruns)
                let slug = if existing.is_some() {
                    // Use previously recorded slug; if missing (e.g., slug was NULL previously),
                    // derive a unique slug and cache it to prevent collisions.
                    if let Some(s) = self.vg_slug_by_id.get(&vg_id).cloned() {
                        s
                    } else {
                        let base = resolve_video_game_slug_base(
                            row.slug.as_deref(),
                            &product,
                            platform_slug,
                            platform_id,
                        );
                        let s = unique_slug_into(&base, &mut self.video_game_slugs);
                        // Track derived slug for this existing row so future passes remain stable
                        self.vg_slug_by_id.insert(vg_id, s.clone());
                        s
                    }
                } else {
                    let base = resolve_video_game_slug_base(
                        row.slug.as_deref(),
                        &product,
                        platform_slug,
                        platform_id,
                    );
                    let s = unique_slug_into(&base, &mut self.video_game_slugs);
                    // Track slug for new vg
                    self.vg_slug_by_id.insert(vg_id, s.clone());
                    s
                };

                // Merge provider codes from external_ids into metadata (dedupe) before persisting
                let metadata = merge_providers_into_metadata(metadata.clone(), &row.external_ids);

                // Fast skip mode: if existing and skip_existing_fast set, we avoid constructing update struct (dedupe only)
                if existing.is_some() && skip_existing_fast {
                    skipped_existing += 1;
                } else {
                    updates.push(VgUpd {
                        vg_id,
                        slug,
                        release_date,
                        developer: developer.clone(),
                        metadata,
                        region_codes: region_codes.clone(),
                        genres: genres.clone(),
                        display_title: if vg_have_display {
                            display_title.clone().or(Some(product.title.clone()))
                        } else {
                            None
                        },
                        // Avoid moving meta_synopsis across loop iterations: use as_ref().cloned()
                        synopsis: if vg_have_synopsis {
                            meta_synopsis
                                .as_ref()
                                .cloned()
                                .or_else(|| product.synopsis.clone())
                        } else {
                            None
                        },
                        popularity_score: meta_popularity.unwrap_or(product.popularity_score),
                        rating: meta_rating.unwrap_or(product.rating),
                        average_rating: if vg_have_avg_rating {
                            meta_avg_rating
                        } else {
                            None
                        },
                        rating_count: if vg_have_rating_count {
                            meta_rating_count
                        } else {
                            None
                        },
                        rating_updated_at: if vg_have_rating_updated {
                            meta_rating_updated_at
                        } else {
                            None
                        },
                    });
                }

                if updates.len() >= chunk {
                    let start_flush = Instant::now();
                    flush_video_game_updates(
                        pool,
                        &mut updates,
                        vg_have_genres,
                        vg_have_region_codes,
                        vg_have_metadata,
                        vg_have_display,
                        vg_have_synopsis,
                        vg_have_avg_rating,
                        vg_have_rating_count,
                        vg_have_rating_updated,
                    )
                    .await?;
                    vg_flush_durations.push(start_flush.elapsed());
                    flushed_batches += 1;
                    info!(target: "progress", label="video_games.flush", batch_size=chunk, flushed_batches, "flushed VG update batch");
                }

                if existing.is_some() {
                    if !skip_existing_fast {
                        skipped_existing += 1;
                    }
                } else {
                    self.stats.video_games += 1;
                    if let Some(limit) = vg_limit_new {
                        if self.stats.video_games >= limit {
                            info!(
                                limit,
                                "VIDEO_GAME_LIMIT_NEW reached; stopping early after flushes"
                            );
                            // Persist checkpoint before breaking early
                            self.save_checkpoint("video_games", last_id).await?;
                            break;
                        }
                    }
                }
            }

            prog.tick(1);
            // Respect limit after inner loop break
            if let Some(limit) = vg_limit_new {
                if self.stats.video_games >= limit {
                    // Persist checkpoint before breaking outer loop
                    self.save_checkpoint("video_games", last_id).await?;
                    break;
                }
            }
        }

        if !updates.is_empty() {
            let remaining = updates.len();
            let start_flush = Instant::now();
            flush_video_game_updates(
                pool,
                &mut updates,
                vg_have_genres,
                vg_have_region_codes,
                vg_have_metadata,
                vg_have_display,
                vg_have_synopsis,
                vg_have_avg_rating,
                vg_have_rating_count,
                vg_have_rating_updated,
            )
            .await?;
            vg_flush_durations.push(start_flush.elapsed());
            flushed_batches += 1;
            info!(target: "progress", label="video_games.flush", batch_size=remaining, flushed_batches, "flushed final VG batch");
        }

        // Batch apply normalized_title overrides if present using a single UPDATE … FROM (VALUES …) for throughput
        if !self.skip_titles && !vgt_title_updates.is_empty() {
            use sqlx::QueryBuilder;
            // Deduplicate by title_id (last value wins)
            let mut latest: std::collections::HashMap<i64, &str> = std::collections::HashMap::new();
            for (id, norm) in &vgt_title_updates {
                latest.insert(*id, norm.as_str());
            }
            let mut qb = QueryBuilder::new("WITH src(id, normalized_title) AS (VALUES ");
            {
                let mut sep = qb.separated(", ");
                for (id, norm) in latest.into_iter() {
                    sep.push("(");
                    sep.push_bind(id);
                    sep.push(", ");
                    sep.push_bind(norm);
                    sep.push(")");
                }
            }
            qb.push(
                ") UPDATE public.video_game_titles t SET normalized_title = src.normalized_title, updated_at = now() FROM src WHERE t.id = src.id"
            );
            qb.build().persistent(false).execute(pool).await?;
            info!(
                target = "progress",
                label = "video_game_titles.normalized_flush",
                updates = vgt_title_updates.len(),
                "flushed normalized_title overrides (batched)"
            );
        }

        if !vg_flush_durations.is_empty() {
            vg_flush_durations.sort();
            let total = vg_flush_durations.len();
            let sum: std::time::Duration = vg_flush_durations
                .iter()
                .copied()
                .reduce(|a, b| a + b)
                .unwrap();
            let avg_ms = (sum.as_secs_f64() * 1000.0) / (total as f64);
            let p50 = vg_flush_durations[total / 2];
            let p95_idx = (((total as f64) * 0.95).floor() as usize).min(total - 1);
            let p95 = vg_flush_durations[p95_idx];
            info!(target="metrics", kind="video_game_update", batches=total, avg_ms=?avg_ms, p50_ms=?p50.as_secs_f64()*1000.0, p95_ms=?p95.as_secs_f64()*1000.0, "video game update batch latency summary");
        }

        // Final checkpoint save if we advanced
        if last_id > min_id {
            self.save_checkpoint("video_games", last_id).await?;
        }

        // Merge any new title cache entries
        self.title_id_by_norm.extend(vgt_title_cache.into_iter());

        prog.finish();
        info!(
            imported_new=self.stats.video_games,
            skipped_existing=skipped_existing,
            resume_from=?min_id,
            limit_new=?vg_limit_new,
            skip_existing_fast,
            "video games import complete (dedupe + resume controls applied)"
        );
        Ok(())
    }

    // ---------- Legacy media imports → game_media (strictly linked to video_games) ----------

    async fn sqlite_table_exists(&self, table: &str) -> Result<bool> {
        let sql = "SELECT name FROM sqlite_master WHERE type='table' AND name=?1";
        let row = sqlx::query(sql)
            .bind(table)
            .fetch_optional(&self.sqlite)
            .await?;
        Ok(row.is_some())
    }

    async fn sqlite_columns(&self, table: &str) -> Result<HashSet<String>> {
        let mut cols: HashSet<String> = HashSet::new();
        let sql = format!("PRAGMA table_info({})", table);
        let rows = sqlx::query(&sql).fetch_all(&self.sqlite).await?;
        for r in rows {
            let name: String = r.get::<String, _>("name");
            cols.insert(name);
        }
        Ok(cols)
    }

    async fn pg_table_exists(&self, relname: &str) -> Result<bool> {
        let exists = sqlx::query_scalar::<_, Option<String>>("SELECT to_regclass($1)")
            .bind(relname)
            .fetch_one(&self.db.pool)
            .await?;
        Ok(exists.is_some())
    }

    async fn import_game_images(&mut self) -> Result<()> {
        if self.is_stage_done("game_images").await? {
            info!("stage game_images:done — skipping import");
            return Ok(());
        }
        info!("importing legacy game_images → game_media (image)");
        let cols = self.sqlite_columns("game_images").await?;
        // Build SELECT with only present columns to avoid SQLite errors
        let mut select_cols: Vec<&str> = Vec::new();
        for c in [
            "id",
            "product_id",
            "video_game_id",
            "platform_id",
            "platform_code",
            "url",
            "kind",
            "mime_type",
            "width",
            "height",
            "is_primary",
            "ordinal",
            "original_url",
            "thumbnail_url",
            "attribution",
            "license",
            "license_url",
            "title",
            "caption",
            "source",
            "external_id",
            "metadata",
            "provider_id",
        ] {
            if cols.contains(c) {
                select_cols.push(c);
            }
        }
        if !select_cols.contains(&"id") {
            select_cols.push("rowid AS id");
        }
        if !select_cols.contains(&"url") {
            anyhow::bail!("legacy game_images without 'url' column");
        }
        let sql = format!(
            "SELECT {} FROM game_images ORDER BY id",
            select_cols.join(", ")
        );
        let mut stream = sqlx::query(&sql).persistent(false).fetch(&self.sqlite);

        const LINKS_BATCH: usize = 1000; // number of links buffered before flushing grouped inserts
        let mut total_rows = 0usize;
        let mut vg_map_cache: HashMap<i64, Vec<i64>> = HashMap::new();
        // Group provider_media_links inserts by (video_game_source_id, vg_id, src) with a shared metadata blob
        #[derive(Default)]
        struct LinkGroup {
            urls: Vec<(String, Option<String>, Option<String>, Option<String>)>,
            meta: Value,
        }
        let mut link_groups: HashMap<(i64, i64, String), LinkGroup> = HashMap::new();
        let mut pending_links: usize = 0;
        async fn flush_link_groups(
            db: &Db,
            groups: &mut HashMap<(i64, i64, String), LinkGroup>,
        ) -> Result<()> {
            for ((video_game_source_id, vg_id, src), grp) in groups.drain() {
                let _ = ensure_vg_source_media_links_with_meta(
                    db,
                    video_game_source_id,
                    Some(vg_id),
                    &grp.urls,
                    &src,
                    Some(grp.meta),
                )
                .await?;
            }
            Ok(())
        }
        let mut prog = Progress::new("game_images", None);
        while let Some(row) = stream.try_next().await? {
            // Resolve target VG list:
            // 1) If legacy row has video_game_id, treat it as legacy VG id and map through legacy_vg_map
            // 2) Else, if legacy row has product_id, treat it as legacy VG id (per directive) and map
            // 3) Fallback: map legacy product_id → platforms (old behavior)
            let mut target_vgs: Vec<i64> = Vec::new();
            if cols.contains("video_game_id") {
                if let Ok(legacy_vg_id) = row.try_get::<i64, _>("video_game_id") {
                    if legacy_vg_id > 0 {
                        if let Some(pg_vg) = self.legacy_vg_map.get(&legacy_vg_id) {
                            target_vgs.push(*pg_vg);
                        }
                    }
                }
            }
            if target_vgs.is_empty() {
                let legacy_pid = if cols.contains("product_id") {
                    row.try_get::<i64, _>("product_id").unwrap_or(0)
                } else {
                    0
                };
                // Treat product_id as legacy VG id first (per directive)
                if legacy_pid > 0 {
                    if let Some(pg_vg) = self.legacy_vg_map.get(&legacy_pid) {
                        target_vgs.push(*pg_vg);
                    }
                }
                // If still empty, fallback to old product→platform expansion
                if legacy_pid > 0 {
                    if target_vgs.is_empty() {
                        // Cache product_id → VG ids mapping
                        if let Some(v) = vg_map_cache.get(&legacy_pid) {
                            target_vgs.extend_from_slice(v);
                        } else {
                            let mut vgs: Vec<i64> = Vec::new();
                            if let Some(info) = self.product_map.get(&legacy_pid) {
                                if let Some(title_id) = info.title_id {
                                    if let Some(platforms) = self.product_platforms.get(&legacy_pid)
                                    {
                                        for pid in platforms {
                                            let vg_id = self
                                                .cache
                                                .ensure_video_game(title_id, *pid, None)
                                                .await?;
                                            vgs.push(vg_id);
                                        }
                                    }
                                }
                            }
                            vgs.sort_unstable();
                            vgs.dedup();
                            vg_map_cache.insert(legacy_pid, vgs.clone());
                            target_vgs.extend_from_slice(&vgs);
                        }
                    }
                }
            }
            if target_vgs.is_empty() {
                // No way to link strictly, skip row entirely
                prog.tick(1);
                continue;
            }
            let url: String = row.try_get::<String, _>("url").unwrap_or_default();
            if url.trim().is_empty() {
                prog.tick(1);
                continue;
            }
            let media_type = if cols.contains("kind") {
                row.try_get::<String, _>("kind")
                    .unwrap_or_else(|_| "cover".into())
            } else {
                "cover".into()
            };
            let guessed_mime = guess_video_mime(&url);
            let mut mime_type: Option<String> = if cols.contains("mime_type") {
                row.try_get("mime_type").ok()
            } else {
                None
            };
            if mime_type
                .as_ref()
                .map(|s| s.trim().is_empty())
                .unwrap_or(true)
            {
                mime_type = Some(guessed_mime.to_string());
            }
            let title: Option<String> = if cols.contains("title") {
                row.try_get("title").ok()
            } else {
                None
            };
            let caption: Option<String> = if cols.contains("caption") {
                row.try_get("caption").ok()
            } else {
                None
            };
            let is_primary: Option<i64> = if cols.contains("is_primary") {
                row.try_get("is_primary").ok()
            } else {
                None
            };
            let width: Option<i64> = if cols.contains("width") {
                row.try_get("width").ok()
            } else {
                None
            };
            let height: Option<i64> = if cols.contains("height") {
                row.try_get("height").ok()
            } else {
                None
            };
            let original_url: Option<String> = if cols.contains("original_url") {
                row.try_get("original_url").ok()
            } else {
                None
            };
            let thumbnail_url: Option<String> = if cols.contains("thumbnail_url") {
                row.try_get("thumbnail_url").ok()
            } else {
                None
            };
            let attribution: Option<String> = if cols.contains("attribution") {
                row.try_get("attribution").ok()
            } else {
                None
            };
            let license: Option<String> = if cols.contains("license") {
                row.try_get("license").ok()
            } else {
                None
            };
            let license_url: Option<String> = if cols.contains("license_url") {
                row.try_get("license_url").ok()
            } else {
                None
            };
            let mut source: Option<String> = if cols.contains("source") {
                row.try_get("source").ok()
            } else {
                None
            };
            // Attempt provider_id → slug mapping if source blank and provider_id column present
            if source.as_ref().map(|s| s.trim().is_empty()).unwrap_or(true)
                && cols.contains("provider_id")
            {
                if let Ok(pid) = row.try_get::<i64, _>("provider_id") {
                    if pid > 0 {
                        if let Some(slug) = self.legacy_provider_slug.get(&pid) {
                            source = Some(slug.clone());
                        }
                    }
                }
            }
            let external_id: Option<String> = if cols.contains("external_id") {
                row.try_get("external_id").ok()
            } else {
                None
            };
            let meta: Option<String> = if cols.contains("metadata") {
                row.try_get("metadata").ok()
            } else {
                None
            };
            let mut pdata = serde_json::Map::new();
            // Only insert mime_type if present; avoid irrefutable pattern that caused compile error.
            if let Some(mt) = mime_type.as_ref() {
                pdata.insert("mime_type".into(), Value::String(mt.clone()));
            }
            if let Some(t) = title.as_ref() {
                pdata.insert("title".into(), Value::String(t.clone()));
            }
            if let Some(c) = caption {
                pdata.insert("caption".into(), Value::String(c));
            }
            if let Some(p) = is_primary {
                pdata.insert("is_primary".into(), Value::Number((p as i64).into()));
            }
            if let Some(w) = width {
                pdata.insert("width".into(), Value::Number((w as i64).into()));
            }
            if let Some(h) = height {
                pdata.insert("height".into(), Value::Number((h as i64).into()));
            }
            if let Some(ou) = original_url {
                pdata.insert("original_url".into(), Value::String(ou));
            }
            if let Some(tu) = thumbnail_url {
                pdata.insert("thumbnail_url".into(), Value::String(tu));
            }
            // Insert attribution if present (previous irrefutable if-let removed).
            if let Some(att) = attribution.as_ref() {
                pdata.insert("attribution".into(), Value::String(att.clone()));
            }
            if let Some(l) = license {
                pdata.insert("license".into(), Value::String(l));
            }
            if let Some(lu) = license_url {
                pdata.insert("license_url".into(), Value::String(lu));
            }
            if let Some(s) = source.clone() {
                pdata.insert("source".into(), Value::String(s));
            }
            if let Some(eid) = external_id.clone() {
                pdata.insert("external_id".into(), Value::String(eid));
            }
            if let Some(m) = meta.and_then(|raw| serde_json::from_str::<Value>(raw.as_str()).ok()) {
                pdata.insert("legacy_metadata".into(), m);
            }
            let provider_data = Value::Object(pdata);

            // Resolve provider source slug: prefer column; optionally derive from URL if allowed
            let allow_url_source = std::env::var("IMPORTER_ALLOW_URL_SOURCE")
                .ok()
                .map(|v| (v == "1" || v.eq_ignore_ascii_case("true")))
                .unwrap_or(false);
            if source.as_ref().map(|s| s.trim().is_empty()).unwrap_or(true) && allow_url_source {
                if let Some(derived) = derive_provider_slug_from_url(&url) {
                    source = Some(derived);
                }
            }
            // Correct matching on Option to avoid passing &Option<String> where &str expected
            let src = match source
                .map(|s| s.trim().to_lowercase())
                .filter(|s| !s.is_empty())
            {
                Some(s) => s,
                None => {
                    warn!(url=%url, "skipping image without a valid media source");
                    prog.tick(1);
                    continue;
                }
            };
            let src = normalize_provider_slug(&src).to_string();
            let ext_id = external_id.unwrap_or_else(|| url.clone());
            // Resolve provider id via cached slug lookup (create if missing)
            let provider_id: i64 = self.provider_id_for_slug(&src).await?; // avoid &String clone
            let pi_external = if ext_id.is_empty() {
                format!("media:{}", url)
            } else {
                format!("media:{}", ext_id)
            };
            let video_game_source_id = self
                .cache
                .ensure_provider_item(provider_id, &pi_external, None, false)
                .await?;
            for vg_id in target_vgs.iter().copied() {
                // Queue provider_media_links for grouped flush
                let key = (video_game_source_id, vg_id, src.clone());
                let entry = link_groups.entry(key).or_insert_with(|| LinkGroup {
                    urls: Vec::new(),
                    meta: provider_data.clone(),
                });
                entry
                    .urls
                    .push((url.clone(), Some(media_type.clone()), None, title.clone()));
                pending_links += 1;
                if pending_links >= LINKS_BATCH {
                    flush_link_groups(self.cache.db(), &mut link_groups).await?;
                    pending_links = 0;
                }
            }
            total_rows += 1;
            prog.tick(1);
        }

        if pending_links > 0 || !link_groups.is_empty() {
            flush_link_groups(self.cache.db(), &mut link_groups).await?;
            pending_links = 0;
        }
        prog.finish();
        info!(
            imported_rows = total_rows,
            "image media imported (as game_media)"
        );
        Ok(())
    }

    async fn import_game_videos(&mut self) -> Result<()> {
        if self.is_stage_done("game_videos").await? {
            info!("stage game_videos:done — skipping import");
            return Ok(());
        }
        info!("importing legacy game_videos → game_media (video)");
        let cols = self.sqlite_columns("game_videos").await?;
        let mut select_cols: Vec<&str> = Vec::new();
        for c in [
            "id",
            "product_id",
            "video_game_id",
            "platform_id",
            "platform_code",
            "url",
            "kind",
            "mime_type",
            "duration_seconds",
            "title",
            "source",
            "external_id",
            "metadata",
            "provider_id",
        ] {
            if cols.contains(c) {
                select_cols.push(c);
            }
        }
        if !select_cols.contains(&"id") {
            select_cols.push("rowid AS id");
        }
        let missing_url = !select_cols.contains(&"url");
        if missing_url {
            info!(
                "legacy game_videos table lacks 'url' column — will derive fallback URLs from external_id/metadata/id"
            );
        }
        // Define global fallback source once (outside row loop) so we can log it after processing
        let source_fallback = std::env::var("VIDEO_SOURCE_FALLBACK")
            .ok()
            .map(|v| v.trim().to_lowercase())
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| "manual".to_string());
        let sql = format!(
            "SELECT {} FROM game_videos ORDER BY id",
            select_cols.join(", ")
        );
        let mut stream = sqlx::query(&sql).persistent(false).fetch(&self.sqlite);

        const LINKS_BATCH: usize = 800;
        let mut total_rows = 0usize;
        let mut vg_map_cache: HashMap<i64, Vec<i64>> = HashMap::new();
        #[derive(Default)]
        struct LinkGroup {
            urls: Vec<(String, Option<String>, Option<String>, Option<String>)>,
            meta: Value,
        }
        let mut link_groups: HashMap<(i64, i64, String), LinkGroup> = HashMap::new();
        let mut pending_links: usize = 0;
        async fn flush_link_groups(
            db: &Db,
            groups: &mut HashMap<(i64, i64, String), LinkGroup>,
        ) -> Result<()> {
            for ((video_game_source_id, vg_id, src), grp) in groups.drain() {
                let _ = ensure_vg_source_media_links_with_meta(
                    db,
                    video_game_source_id,
                    Some(vg_id),
                    &grp.urls,
                    &src,
                    Some(grp.meta),
                )
                .await?;
            }
            Ok(())
        }
        let mut prog = Progress::new("game_videos", None);
        while let Some(row) = stream.try_next().await? {
            // Resolve target VG list:
            // 1) If legacy row has video_game_id, treat it as legacy VG id and map through legacy_vg_map
            // 2) Else, if legacy row has product_id, treat it as legacy VG id (per directive) and map
            // 3) Fallback: map legacy product_id → platforms (old behavior)
            let mut target_vgs: Vec<i64> = Vec::new();
            if cols.contains("video_game_id") {
                if let Ok(legacy_vg_id) = row.try_get::<i64, _>("video_game_id") {
                    if legacy_vg_id > 0 {
                        if let Some(pg_vg) = self.legacy_vg_map.get(&legacy_vg_id) {
                            target_vgs.push(*pg_vg);
                        }
                    }
                }
            }
            if target_vgs.is_empty() {
                let legacy_pid = if cols.contains("product_id") {
                    row.try_get::<i64, _>("product_id").unwrap_or(0)
                } else {
                    0
                };
                // Treat product_id as legacy VG id first (per directive)
                if legacy_pid > 0 {
                    if let Some(pg_vg) = self.legacy_vg_map.get(&legacy_pid) {
                        target_vgs.push(*pg_vg);
                    }
                }
                // If still empty, fallback to old product→platform expansion
                if legacy_pid > 0 {
                    if target_vgs.is_empty() {
                        if let Some(v) = vg_map_cache.get(&legacy_pid) {
                            target_vgs.extend_from_slice(v);
                        } else {
                            let mut vgs: Vec<i64> = Vec::new();
                            if let Some(info) = self.product_map.get(&legacy_pid) {
                                if let Some(title_id) = info.title_id {
                                    if let Some(platforms) = self.product_platforms.get(&legacy_pid) {
                                        for pid in platforms {
                                            let vg_id = self
                                                .cache
                                                .ensure_video_game(title_id, *pid, None)
                                                .await?;
                                            vgs.push(vg_id);
                                        }
                                    }
                                }
                            }
                            vgs.sort_unstable();
                            vgs.dedup();
                            vg_map_cache.insert(legacy_pid, vgs.clone());
                            target_vgs.extend_from_slice(&vgs);
                        }
                    }
                }
            }
            if target_vgs.is_empty() {
                prog.tick(1);
                continue;
            }
            // Fallback URL derivation when 'url' column is absent.
            let mut url: String = if !missing_url {
                row.try_get::<String, _>("url").unwrap_or_default()
            } else {
                let ext_raw: Option<String> = if cols.contains("external_id") {
                    row.try_get("external_id").ok()
                } else {
                    None
                };
                let meta_raw: Option<String> = if cols.contains("metadata") {
                    row.try_get("metadata").ok()
                } else {
                    None
                };
                let mut candidate = ext_raw.unwrap_or_default();
                let mut _picked_from_meta = false;
                if !(candidate.starts_with("http://") || candidate.starts_with("https://")) {
                    if let Some(mr) = meta_raw.as_ref() {
                        if let Ok(val) = serde_json::from_str::<Value>(mr) {
                            if let Some(u) = val.get("url").and_then(|v| v.as_str()) {
                                candidate = u.to_string();
                                _picked_from_meta = true;
                            } else if let Some(u) = val.get("video_url").and_then(|v| v.as_str()) {
                                candidate = u.to_string();
                                _picked_from_meta = true;
                            }
                        }
                    }
                }
                if !(candidate.starts_with("http://") || candidate.starts_with("https://")) {
                    // Synthetic stable identifier if we still lack a usable URL
                    let legacy_id: i64 = if cols.contains("id") {
                        row.try_get("id").unwrap_or(0)
                    } else {
                        0
                    };
                    candidate = format!("legacy-video://{}", legacy_id);
                }
                if candidate.is_empty() {
                    String::new()
                } else {
                    candidate
                }
            };
            if url.trim().is_empty() {
                prog.tick(1);
                continue;
            }
            let kind = if cols.contains("kind") {
                row.try_get::<String, _>("kind")
                    .unwrap_or_else(|_| "trailer".into())
            } else {
                "trailer".into()
            };
            let mime_type: Option<String> = if cols.contains("mime_type") {
                row.try_get("mime_type").ok()
            } else {
                None
            };
            let title: Option<String> = if cols.contains("title") {
                row.try_get("title").ok()
            } else {
                None
            };
            let duration: Option<i64> = if cols.contains("duration_seconds") {
                row.try_get("duration_seconds").ok()
            } else {
                None
            };
            let mut source: Option<String> = if cols.contains("source") {
                row.try_get("source").ok()
            } else {
                None
            };
            // Attempt provider_id → slug mapping if source blank and legacy provider id present.
            if source.as_ref().map(|s| s.trim().is_empty()).unwrap_or(true)
                && cols.contains("provider_id")
            {
                if let Ok(pid) = row.try_get::<i64, _>("provider_id") {
                    if pid > 0 {
                        if let Some(slug) = self.legacy_provider_slug.get(&pid) {
                            source = Some(slug.clone());
                        }
                    }
                }
            }
            let external_id: Option<String> = if cols.contains("external_id") {
                row.try_get("external_id").ok()
            } else {
                None
            };
            let meta: Option<String> = if cols.contains("metadata") {
                row.try_get("metadata").ok()
            } else {
                None
            };

            let mut pdata = serde_json::Map::new();
            // Borrow mime_type so it remains available for later staging bind clone.
            if let Some(ref mt) = mime_type {
                pdata.insert("mime_type".into(), Value::String(mt.clone()));
            }
            if let Some(t) = title.as_ref() {
                pdata.insert("title".into(), Value::String(t.clone()));
            }
            if let Some(d) = duration {
                pdata.insert("duration_seconds".into(), Value::Number((d as i64).into()));
            }
            if let Some(s) = source.clone() {
                pdata.insert("source".into(), Value::String(s));
            }
            if let Some(eid) = external_id.clone() {
                pdata.insert("external_id".into(), Value::String(eid));
            }
            if let Some(m) = meta.and_then(|raw| serde_json::from_str::<Value>(raw.as_str()).ok()) {
                pdata.insert("legacy_metadata".into(), m);
            }
            // Annotate synthetic URL derivation for traceability
            if missing_url || url.starts_with("legacy-video://") {
                pdata.insert("synthetic_url".into(), Value::Bool(true));
            }
            let provider_data = Value::Object(pdata);
            // Require a known, non-empty source matching providers.slug
            // Allow URL-based source derivation prior to consuming `source`.
            let allow_url_source = std::env::var("IMPORTER_ALLOW_URL_SOURCE")
                .ok()
                .map(|v| (v == "1" || v.eq_ignore_ascii_case("true")))
                .unwrap_or(false);
            if source.as_ref().map(|s| s.trim().is_empty()).unwrap_or(true) && allow_url_source {
                if let Some(derived) = derive_provider_slug_from_url(&url) {
                    source = Some(derived);
                }
            }
            // Use previously computed source_fallback if row has no usable source
            let src = source
                .as_ref()
                .map(|s| s.trim().to_lowercase())
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| source_fallback.clone());

            if src.is_empty() {
                warn!(url=%url, "skipping video without a valid media source after fallback");
                prog.tick(1);
                continue;
            }
            let ext_id = external_id.unwrap_or_else(|| url.clone());
            // Resolve provider id via cached slug lookup (create if missing)
            let provider_id: i64 = self.provider_id_for_slug(&src).await?;
            let pi_external = if ext_id.is_empty() {
                format!("media:{}", url)
            } else {
                format!("media:{}", ext_id)
            };
            let video_game_source_id = self
                .cache
                .ensure_provider_item(provider_id, &pi_external, None, false)
                .await?;
            // Classify fine-grained kind for game_media API: 'trailer'|'gameplay' map directly
            let media_type = if kind.eq_ignore_ascii_case("gameplay") {
                "gameplay"
            } else {
                "trailer"
            };
            for vg_id in target_vgs.iter().copied() {
                let key = (video_game_source_id, vg_id, src.clone());
                let entry = link_groups.entry(key).or_insert_with(|| LinkGroup {
                    urls: Vec::new(),
                    meta: provider_data.clone(),
                });
                entry.urls.push((
                    url.clone(),
                    Some(media_type.to_string()),
                    None,
                    title.clone(),
                ));
                pending_links += 1;
                if pending_links >= LINKS_BATCH {
                    flush_link_groups(self.cache.db(), &mut link_groups).await?;
                    pending_links = 0;
                }
            }
            total_rows += 1;
            prog.tick(1);
        }
        if pending_links > 0 || !link_groups.is_empty() {
            flush_link_groups(self.cache.db(), &mut link_groups).await?;
            pending_links = 0;
        }
        prog.finish();
        info!(processed_rows = total_rows, fallback_source=%source_fallback, "video media processed (as game_media) — batch upserts executed");
        Ok(())
    }

    // -------- Giant Bomb media ingestion (images) --------
    async fn import_giantbomb_images(&mut self) -> Result<()> {
        if self.is_stage_done("gb_images").await? {
            info!("stage gb_images:done — skipping import");
            return Ok(());
        }
        // Chunked resume: use durable checkpoint to continue from last processed legacy id
        // - Key: "gb_images_id" stores last processed giant_bomb_game_images.id
        // - LIMIT controlled by GB_IMAGES_LIMIT (default: 2000)
        let mut resume_min_id: i64 = self.get_checkpoint("gb_images_id").await.unwrap_or(0);
        let chunk_limit: i64 = std::env::var("GB_IMAGES_LIMIT")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(2000);
        let direct_specialized = std::env::var("GB_DIRECT_SPECIALIZED")
            .ok()
            .map(|v| (v == "1" || v.eq_ignore_ascii_case("true")))
            .unwrap_or(false);


        info!(
            mode = if direct_specialized {
                "specialized"
            } else {
                "generic"
            },
            "importing giant_bomb_game_images"
        );


        let cols = self.sqlite_columns("giant_bomb_game_images").await?;
        let sql = r#"
                        SELECT
                            gbi.id AS id,
                            gbi.giant_bomb_game_id AS giant_bomb_game_id,
                            gbi.original_url AS original_url,
                            gbi.super_url AS super_url,
                            gbi.screen_url AS screen_url,
                            gbi.medium_url AS medium_url,
                            gbi.small_url AS small_url,
                            gbi.thumb_url AS thumb_url,
                            gbi.tiny_url AS tiny_url,
                            gbi.icon_url AS icon_url,
                            gbi.source AS source,
                            gbi.name AS caption,
                            gbi.metadata AS metadata,
                            gbg.video_game_id AS video_game_id,
                            vgt.product_id AS product_id,
                            vg.normalized_title AS normalized_title,
                            vg.title AS vg_title
                        FROM giant_bomb_game_images gbi
                        LEFT JOIN giant_bomb_games gbg ON gbg.id = gbi.giant_bomb_game_id
                        LEFT JOIN video_games vg ON vg.id = gbg.video_game_id
                        LEFT JOIN video_game_titles vgt ON vgt.id = vg.title_id
                        WHERE gbi.id > ?
                        ORDER BY gbi.id
                        LIMIT ?
                "#;
        tracing::info!(
            resume_min_id,
            chunk_limit,
            "gb_images: starting chunked scan"
        );
        let mut stream = sqlx::query(sql)
            .persistent(false)
            .bind(resume_min_id)
            .bind(chunk_limit)
            .fetch(&self.sqlite);

        const LINKS_BATCH: usize = 800;
        #[derive(Default)]
        struct LinkGroup {
            urls: Vec<(String, Option<String>, Option<String>, Option<String>)>,
            meta: Value,
        }
        let mut link_groups: HashMap<(i64, i64, String), LinkGroup> = HashMap::new();
        let mut pending_links = 0usize;
        let mut total_rows = 0usize;
        let mut last_id_seen: i64 = resume_min_id;
        let mut prog = Progress::new("gb_images", None);

        async fn flush_groups(
            db: &Db,
            groups: &mut HashMap<(i64, i64, String), LinkGroup>,
        ) -> Result<()> {
            for ((pi, vg, src), g) in groups.drain() {
                let _ = ensure_vg_source_media_links_with_meta(
                    db,
                    pi,
                    Some(vg),
                    &g.urls,
                    &src,
                    Some(g.meta.clone()),
                )
                .await?;
            }
            Ok(())
        }

        while let Some(row) = stream.try_next().await? {
            // Track progress id for durable checkpointing
            if let Ok(img_id) = row.try_get::<i64, _>("id") {
                last_id_seen = img_id;
            }
            // Choose best URL
            let mut url: String = row.try_get::<String, _>("original_url").unwrap_or_default();
            for col in [
                "super_url",
                "screen_url",
                "medium_url",
                "small_url",
                "thumb_url",
                "tiny_url",
                "icon_url",
            ] {
                if url.trim().is_empty() {
                    url = row.try_get::<String, _>(col).unwrap_or_default();
                }
            }
            if url.trim().is_empty() {
                prog.tick(1);
                continue;
            }

            // Resolve target video_games
            let mut target_vgs: Vec<i64> = Vec::new();
            if let Ok(id) = row.try_get::<i64, _>("video_game_id") {
                if id > 0 {
                    if let Some(pg) = self.legacy_vg_map.get(&id) {
                        target_vgs.push(*pg);
                    }
                }
            }
            if target_vgs.is_empty() {
                if let Ok(pid) = row.try_get::<i64, _>("product_id") {
                    if pid > 0 {
                        if let Some(info) = self.product_map.get(&pid) {
                            if let Some(tid) = info.title_id {
                                if let Some(platforms) = self.product_platforms.get(&pid) {
                                    for p in platforms {
                                        let vg_id =
                                            self.cache.ensure_video_game(tid, *p, None).await?;
                                        target_vgs.push(vg_id);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            if target_vgs.is_empty() {
                let norm: Option<String> = row.try_get("normalized_title").ok();
                let title_txt: Option<String> = row.try_get("vg_title").ok();
                if let Some(nt) = norm.as_ref().filter(|s| !s.trim().is_empty()) {
                    let norm_key = nt.trim().to_lowercase();
                    let title_id = if let Some(tid) = self.title_id_by_norm.get(&norm_key).copied()
                    {
                        tid
                    } else {
                        let tid = ensure_title_without_product(
                            self.cache.db(),
                            title_txt.as_deref().unwrap_or(nt),
                        )
                        .await?;
                        self.title_id_by_norm.insert(norm_key.clone(), tid);
                        tid
                    };
                    let unknown =
                        ensure_platform(self.cache.db(), "unknown", Some("unknown")).await?;
                    let vg_id = self
                        .cache
                        .ensure_video_game(title_id, unknown, None)
                        .await?;
                    target_vgs.push(vg_id);
                }
                if target_vgs.is_empty() {
                    prog.tick(1);
                    continue;
                }
            }

            // Provider/meta
            let title: Option<String> = row.try_get("caption").ok();
            let mut source: Option<String> = row.try_get("source").ok();
            if source.as_ref().map(|s| s.trim().is_empty()).unwrap_or(true) {
                source = Some("giantbomb".into());
            }
            let src = source.clone().unwrap();
            let meta_raw: Option<String> = row.try_get("metadata").ok();
            let mut pdata = serde_json::Map::new();
            pdata.insert("source".into(), Value::String(src.clone()));
            pdata.insert("kind".into(), Value::String("cover".into()));
            pdata.insert("url".into(), Value::String(url.clone()));
            if let Some(t) = title.as_ref() {
                pdata.insert("title".into(), Value::String(t.clone()));
            }
            if let Some(m) = meta_raw
                .as_ref()
                .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
            {
                pdata.insert("legacy_metadata".into(), m);
            }
            let provider_data = Value::Object(pdata);



            // Provider item linkage
            let ext_id = url.clone();
            let provider_id = self.provider_id_for_slug(&src).await?;
            let pi_external = format!("media:{}", ext_id);
            let video_game_source_id = self
                .cache
                .ensure_provider_item(provider_id, &pi_external, None, false)
                .await?;

            if direct_specialized {
                // Derive optional dims/kind/platforms
                let (mut width_i, mut height_i) = (1i32, 1i32);
                let mut detected_kind: Option<String> = None;
                if let Some(raw) = meta_raw.as_ref() {
                    if let Ok(v) = serde_json::from_str::<Value>(raw) {
                        if let Some(o) = v.as_object() {
                            if let Some(wv) = o.get("width").and_then(|n| n.as_i64()) {
                                width_i = wv as i32;
                            }
                            if let Some(hv) = o.get("height").and_then(|n| n.as_i64()) {
                                height_i = hv as i32;
                            }
                            if let Some(tags) = o.get("image_tags").or_else(|| o.get("tags")) {
                                if let Some(arr) = tags.as_array() {
                                    if arr.iter().filter_map(|v| v.as_str()).any(|t| {
                                        t.to_ascii_lowercase().contains("cover")
                                            || t.to_ascii_lowercase().contains("box")
                                    }) {
                                        detected_kind = Some("cover".into());
                                    } else if arr.iter().filter_map(|v| v.as_str()).any(|t| {
                                        t.to_ascii_lowercase().contains("artwork")
                                            || t.to_ascii_lowercase().contains("key art")
                                            || t.to_ascii_lowercase().contains("promo")
                                    }) {
                                        detected_kind = Some("artwork".into());
                                    }
                                }
                            }
                        }
                    }
                }
                let mime = guess_image_mime(&url);
                for vg in target_vgs.iter().copied() {
                    let platforms_arr: Option<Vec<String>> =
                        self.platform_slug_for_vg(vg).await?.map(|s| vec![s]);
                    sqlx
                        ::query(
                            "INSERT INTO public.game_images (video_game_id, video_game_source_id, kind, mime_type, width, height, url, original_url, thumbnail_url, small_url, super_url, attribution, metadata, source, title, caption, is_primary, platforms) SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18 WHERE NOT EXISTS (SELECT 1 FROM public.game_images WHERE video_game_id=$1 AND url=$7)"
                        )
                        .persistent(false)
                        .bind(vg)
                        .bind(video_game_source_id)
                        .bind(detected_kind.clone().unwrap_or_else(|| "cover".into()))
                        .bind(mime)
                        .bind(width_i)
                        .bind(height_i)
                        .bind(&url)
                        .bind(row.try_get::<Option<String>, _>("original_url").ok())
                        .bind(row.try_get::<Option<String>, _>("thumb_url").ok())
                        .bind(row.try_get::<Option<String>, _>("small_url").ok())
                        .bind(row.try_get::<Option<String>, _>("super_url").ok())
                        .bind(None::<String>)
                        .bind(provider_data.clone())
                        .bind(&src)
                        .bind(title.clone())
                        .bind(title.clone())
                        .bind(false)
                        .bind(platforms_arr.as_deref())
                        .execute(&self.db.pool).await?;
                }
                // link entries
                for vg in target_vgs.iter().copied() {
                    let key = (video_game_source_id, vg, src.clone());
                    link_groups
                        .entry(key)
                        .or_insert_with(|| LinkGroup {
                            urls: Vec::new(),
                            meta: provider_data.clone(),
                        })
                        .urls
                        .push((url.clone(), Some("cover".into()), None, title.clone()));
                    pending_links += 1;
                }
                if pending_links >= LINKS_BATCH {
                    flush_groups(self.cache.db(), &mut link_groups).await?;
                    pending_links = 0;
                    // Opportunistic checkpoint flush
                    let _ = self.save_checkpoint("gb_images_id", last_id_seen).await;
                }
            } else {
                for vg in target_vgs.iter().copied() {
                    let key = (video_game_source_id, vg, src.clone());
                    link_groups
                        .entry(key)
                        .or_insert_with(|| LinkGroup {
                            urls: Vec::new(),
                            meta: provider_data.clone(),
                        })
                        .urls
                        .push((url.clone(), Some("cover".into()), None, title.clone()));
                    pending_links += 1;
                }
                if pending_links >= LINKS_BATCH {
                    flush_groups(self.cache.db(), &mut link_groups).await?;
                    pending_links = 0;
                    // Opportunistic checkpoint flush
                    let _ = self.save_checkpoint("gb_images_id", last_id_seen).await;
                }
            }
            total_rows += 1;
            prog.tick(1);
        }


        if pending_links > 0 || !link_groups.is_empty() {
            flush_groups(self.cache.db(), &mut link_groups).await?;
        }
        // Persist final checkpoint and optionally mark stage done if this chunk was exhausted
        if total_rows == 0 {
            // No rows returned beyond resume_min_id → stage complete
            self.mark_stage_done("gb_images").await?;
            tracing::info!(resume_min_id, "gb_images: no more rows — stage marked done");
        } else {
            self.save_checkpoint("gb_images_id", last_id_seen).await?;
            tracing::info!(
                processed = total_rows,
                last_id_seen,
                "gb_images: checkpoint saved for next chunk"
            );
        }
        info!(
            imported_rows = total_rows,
            mode = if direct_specialized {
                "game_images"
            } else {
                "game_media"
            },
            "giant bomb image media imported"
        );
        Ok(())
    }

    // -------- Giant Bomb media ingestion (videos) --------
    async fn import_giantbomb_videos(&mut self) -> Result<()> {
        if self.is_stage_done("gb_videos").await? {
            info!("stage gb_videos:done — skipping import");
            return Ok(());
        }

        let sql = r#"
            SELECT
              gbv.id AS id,
              gbv.giant_bomb_game_id AS giant_bomb_game_id,
              gbv.url AS url,
              gbv.high_url AS high_url,
              gbv.hd_url AS hd_url,
              gbv.low_url AS low_url,
              gbv.stream_url AS stream_url,
              gbv.playable_url AS playable_url,
              gbv.preview_url AS poster_url,
              gbv.name AS title,
              gbv.length_seconds AS duration_seconds,
              gbv.source AS source,
              gbv.guid AS external_id,
              gbv.metadata AS metadata,
                            gbg.video_game_id AS video_game_id,
                            vgt.product_id AS product_id,
              vg.normalized_title AS normalized_title,
              vg.title AS vg_title
            FROM giant_bomb_game_videos gbv
            LEFT JOIN giant_bomb_games gbg ON gbg.id = gbv.giant_bomb_game_id
                        LEFT JOIN video_games vg ON vg.id = gbg.video_game_id
                        LEFT JOIN video_game_titles vgt ON vgt.id = vg.title_id
            ORDER BY gbv.id
        "#;
        let mut stream = sqlx::query(sql).persistent(false).fetch(&self.sqlite);
        const LINKS_BATCH: usize = 600;
        #[derive(Default)]
        struct LinkGroup {
            urls: Vec<(String, Option<String>, Option<String>, Option<String>)>,
            meta: Value,
        }
        let mut link_groups: HashMap<(i64, i64, String), LinkGroup> = HashMap::new();
        let mut pending_links = 0usize;
        let mut total_rows = 0usize;
        let mut prog = Progress::new("gb_videos", None);

        async fn flush_groups(
            db: &Db,
            groups: &mut HashMap<(i64, i64, String), LinkGroup>,
        ) -> Result<()> {
            for ((pi, vg, src), g) in groups.drain() {
                let _ = ensure_vg_source_media_links_with_meta(
                    db,
                    pi,
                    Some(vg),
                    &g.urls,
                    &src,
                    Some(g.meta.clone()),
                )
                .await?;
            }
            Ok(())
        }

        while let Some(row) = stream.try_next().await? {
            let mut url: String = row.try_get::<String, _>("high_url").unwrap_or_default();
            for col in ["hd_url", "url", "low_url", "stream_url", "playable_url"] {
                if url.trim().is_empty() {
                    url = row.try_get::<String, _>(col).unwrap_or_default();
                }
            }
            if url.trim().is_empty() {
                prog.tick(1);
                continue;
            }

            // Resolve target vgs
            let mut target_vgs: Vec<i64> = Vec::new();
            if let Ok(id) = row.try_get::<i64, _>("video_game_id") {
                if id > 0 {
                    if let Some(pg) = self.legacy_vg_map.get(&id) {
                        target_vgs.push(*pg);
                    }
                }
            }
            if target_vgs.is_empty() {
                if let Ok(pid) = row.try_get::<i64, _>("product_id") {
                    if pid > 0 {
                        if let Some(info) = self.product_map.get(&pid) {
                            if let Some(tid) = info.title_id {
                                if let Some(plat) = self.product_platforms.get(&pid) {
                                    for p in plat {
                                        let vg_id =
                                            self.cache.ensure_video_game(tid, *p, None).await?;
                                        target_vgs.push(vg_id);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            if target_vgs.is_empty() {
                let norm: Option<String> = row.try_get("normalized_title").ok();
                let title_txt: Option<String> = row.try_get("vg_title").ok();
                if let Some(nt) = norm.as_ref().filter(|s| !s.trim().is_empty()) {
                    let norm_key = nt.trim().to_lowercase();
                    let title_id = if let Some(tid) = self.title_id_by_norm.get(&norm_key).copied()
                    {
                        tid
                    } else {
                        let tid = ensure_title_without_product(
                            self.cache.db(),
                            title_txt.as_deref().unwrap_or(nt),
                        )
                        .await?;
                        self.title_id_by_norm.insert(norm_key.clone(), tid);
                        tid
                    };
                    let unknown =
                        ensure_platform(self.cache.db(), "unknown", Some("unknown")).await?;
                    let vg_id = self
                        .cache
                        .ensure_video_game(title_id, unknown, None)
                        .await?;
                    target_vgs.push(vg_id);
                }
                if target_vgs.is_empty() {
                    prog.tick(1);
                    continue;
                }
            }

            let title: Option<String> = row.try_get("title").ok();
            let duration: Option<i64> = row.try_get("duration_seconds").ok();
            let mut source: Option<String> = row.try_get("source").ok();
            if source.as_ref().map(|s| s.trim().is_empty()).unwrap_or(true) {
                source = Some("giantbomb".into());
            }
            let src = source.clone().unwrap();
            let external_id: Option<String> = row.try_get("external_id").ok();
            let meta_raw: Option<String> = row.try_get("metadata").ok();

            let guessed_mime = guess_video_mime(&url).to_string();
            let mut pdata = serde_json::Map::new();
            pdata.insert("source".into(), Value::String(src.clone()));
            pdata.insert("kind".into(), Value::String("trailer".into()));
            pdata.insert("url".into(), Value::String(url.clone()));
            pdata.insert("mime_type".into(), Value::String(guessed_mime.clone()));
            if let Some(t) = title.as_ref() {
                pdata.insert("title".into(), Value::String(t.clone()));
            }
            if let Some(d) = duration {
                pdata.insert("duration_seconds".into(), Value::Number((d as i64).into()));
            }
            if let Some(e) = external_id.clone() {
                pdata.insert("external_id".into(), Value::String(e));
            }
            if let Some(m) = meta_raw
                .as_ref()
                .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
            {
                pdata.insert("legacy_metadata".into(), m);
            }
            let provider_data = Value::Object(pdata);



            let ext_id = external_id.clone().unwrap_or_else(|| url.clone());
            let provider_id = self.provider_id_for_slug(&src).await?;
            let pi_external = format!("media:{}", ext_id);
            let video_game_source_id = self
                .cache
                .ensure_provider_item(provider_id, &pi_external, None, false)
                .await?;

            for vg in target_vgs.iter().copied() {
                let key = (video_game_source_id, vg, src.clone());
                link_groups
                    .entry(key)
                    .or_insert_with(|| LinkGroup {
                        urls: Vec::new(),
                        meta: provider_data.clone(),
                    })
                    .urls
                    .push((url.clone(), Some("video".into()), None, title.clone()));
                pending_links += 1;
            }
            if pending_links >= LINKS_BATCH {
                flush_groups(self.cache.db(), &mut link_groups).await?;
                pending_links = 0;
            }

            total_rows += 1;
            prog.tick(1);
        }



        if pending_links > 0 || !link_groups.is_empty() {
            flush_groups(self.cache.db(), &mut link_groups).await?;
        }
        info!(
            imported_rows = total_rows,
            "giant bomb video media imported (as game_media)"
        );
        Ok(())
    }

    // Return platform slug for a video_game id, caching results
    async fn platform_slug_for_vg(&mut self, vg_id: i64) -> Result<Option<String>> {
        if let Some(s) = self.vg_platform_slug.get(&vg_id) {
            return Ok(Some(s.clone()));
        }
        let rec = sqlx
            ::query(
                "SELECT p.code FROM public.video_games vg JOIN public.platforms p ON p.id = vg.platform_id WHERE vg.id = $1"
            )
            .persistent(false)
            .bind(vg_id)
            .fetch_optional(&self.db.pool).await?;
        if let Some(r) = rec {
            let code: String = r.try_get("code")?;
            self.vg_platform_slug.insert(vg_id, code.clone());
            Ok(Some(code))
        } else {
            Ok(None)
        }
    }

    // ---------- Schema helpers ----------

    async fn columns_for(&mut self, table: &str) -> Result<HashSet<String>> {
        if let Some(cached) = self.column_cache.get(table) {
            return Ok(cached.clone());
        }
        let rows = sqlx::query(
            "SELECT column_name FROM information_schema.columns
             WHERE table_schema='public' AND table_name=$1",
        )
        .persistent(false)
        .bind(table)
        .fetch_all(&self.db.pool)
        .await?;

        let set = rows
            .into_iter()
            .map(|r| r.get::<String, _>("column_name"))
            .collect::<HashSet<_>>();
        self.column_cache.insert(table.to_string(), set.clone());
        Ok(set)
    }

    async fn ensure_required_columns(&mut self) -> Result<()> {
        let vg_cols = self.columns_for("video_games").await?;
        if !vg_cols.contains("title_id") {
            bail!(
                "public.video_games.title_id is missing. Re-run migrations (0001+ latest) before using import_sqlite."
            );
        }
        Ok(())
    }

    async fn import_video_game_sources(&mut self) -> Result<()> {
        if self.is_stage_done("video_game_sources").await? {
            info!("stage video_game_sources:done — skipping import");
            return Ok(());
        }
        info!("importing legacy video_game_sources → providers");
        // Discover available columns
        let cols = self.sqlite_columns("video_game_sources").await?;
        // Build select list conservatively
        let mut select_cols: Vec<&str> = Vec::new();
        for c in ["id", "name", "slug", "kind"] {
            if cols.contains(c) {
                select_cols.push(c);
            }
        }
        // If there's no name but we have a slug, we'll derive name from slug.
        if !select_cols.contains(&"name") && !select_cols.contains(&"slug") {
            info!("video_game_sources lacks both 'name' and 'slug'; skipping providers import");
            return Ok(());
        }
        let sql = format!(
            "SELECT {} FROM video_game_sources ORDER BY id",
            select_cols.join(", ")
        );
        let rows = sqlx::query(&sql)
            .persistent(false)
            .fetch_all(&self.sqlite)
            .await?;
        let mut inserted = 0usize;
        for r in rows {
            let legacy_id: Option<i64> = if cols.contains("id") {
                r.try_get("id").ok()
            } else {
                None
            };
            // Name may be missing; derive from slug if needed
            let mut name: Option<String> = if cols.contains("name") {
                r.try_get::<String, _>("name")
                    .ok()
                    .map(|s| s.trim().to_string())
            } else {
                None
            };
            let mut slug: Option<String> = if cols.contains("slug") {
                r.try_get::<String, _>("slug")
                    .ok()
                    .map(|s| s.trim().to_string())
            } else {
                None
            };
            if name.as_deref().map(|s| s.is_empty()).unwrap_or(true) {
                if let Some(s) = slug.as_ref().map(|s| s.trim()).filter(|s| !s.is_empty()) {
                    name = Some(s.to_string());
                }
            }
            // If still no name, skip this row (avoid shadowing Option<String> with different type confusion)
            let name_val = match name {
                Some(n) if !n.is_empty() => n,
                _ => {
                    continue;
                }
            };
            // Normalize slug: if absent, derive from name
            if slug.as_deref().map(|s| s.is_empty()).unwrap_or(true) {
                let derived = simple_slug(&name_val);
                if !derived.is_empty() {
                    slug = Some(derived);
                }
            }
            let kind: String = if cols.contains("kind") {
                r.try_get::<String, _>("kind")
                    .unwrap_or_else(|_| "media".into())
            } else {
                "media".into()
            };
            // Compute final slug (explicit or derived) for later mapping usage.
            let final_slug = slug
                .as_deref()
                .map(|s| s.to_string())
                .unwrap_or_else(|| simple_slug(&name_val));
            // Slug-first precheck to avoid race window if ensure_provider old binary is in use.
            let prov_id = if let Some(s) = slug.as_deref() {
                if let Some(existing_id) =
                    sqlx::query_scalar::<_, i64>("SELECT id FROM public.providers WHERE slug=$1")
                        .persistent(false)
                        .bind(s)
                        .fetch_optional(&self.cache.db().pool)
                        .await?
                {
                    // Optionally refresh name if changed
                    if existing_id > 0 {
                        let _ = sqlx
                            ::query(
                                "UPDATE public.providers SET name=$2 WHERE id=$1 AND name IS DISTINCT FROM $2"
                            )
                            .persistent(false)
                            .bind(existing_id)
                            .bind(&name_val)
                            .execute(&self.cache.db().pool).await;
                    }
                    existing_id
                } else {
                    ensure_provider(self.cache.db(), &name_val, &kind, Some(s)).await?
                }
            } else {
                ensure_provider(self.cache.db(), &name_val, &kind, None).await?
            };
            // Persist the stable sqlite source id if present
            if let Some(legacy) = legacy_id {
                let _ = sqlx
                    ::query(
                        "UPDATE public.providers SET legacy_source_id=$1 WHERE id=$2 AND legacy_source_id IS NULL"
                    )
                    .persistent(false)
                    .bind(legacy)
                    .bind(prov_id)
                    .execute(&self.cache.db().pool).await;
                // Record mapping legacy provider id -> slug for media source derivation.
                self.legacy_provider_slug.insert(legacy, final_slug.clone());
            }
            inserted += 1;
        }
        info!(inserted, "providers imported from video_game_sources");
        Ok(())
    }
}

// ---------- SQLite helpers ----------

async fn sqlite_count(sqlite: &SqlitePool, table: &str) -> Result<i64> {
    let sql = format!("SELECT COUNT(1) AS n FROM {}", table);
    let row = sqlx::query(&sql).fetch_one(sqlite).await?;
    Ok(row.get::<i64, _>("n"))
}

// Optional WHERE-filtered COUNT helper for better progress estimates on resumed ranges
async fn sqlite_count_where(
    sqlite: &SqlitePool,
    table: &str,
    where_clause: Option<&str>,
) -> Result<i64> {
    let sql = if let Some(w) = where_clause {
        format!("SELECT COUNT(1) AS n FROM {} WHERE {}", table, w)
    } else {
        format!("SELECT COUNT(1) AS n FROM {}", table)
    };
    let row = sqlx::query(&sql).fetch_one(sqlite).await?;
    Ok(row.get::<i64, _>("n"))
}

// ---------- SQLite shapes ----------

#[derive(Debug, sqlx::FromRow)]
struct LegacyCurrency {
    id: i64,
    code: Option<String>,
    name: Option<String>,
    decimals: Option<i64>,
}
#[derive(Debug, sqlx::FromRow)]
struct LegacyPlatform {
    id: i64,
    code: Option<String>,
    name: Option<String>,
    family: Option<String>,
}
#[derive(Debug, sqlx::FromRow)]
struct LegacyCountry {
    id: i64,
    code: String,
    name: String,
    currency_id: Option<i64>,
}

#[derive(Debug, Clone)]
struct CurrencyUpsert {
    legacy_id: i64,
    code: String,
    name: String,
    minor_unit: i16,
}

#[derive(Debug, Clone)]
struct CountryUpsert {
    legacy_id: i64,
    iso2: String,
    iso3: String,
    name: String,
    currency_id: i64,
}
#[derive(Debug, sqlx::FromRow)]
struct LegacyGamePlatform {
    product_id: i64,
    platform_id: i64,
}
#[derive(Debug, sqlx::FromRow)]
struct LegacyProduct {
    id: i64,
    name: String,
    slug: Option<String>,
    category: Option<String>,
    release_date: Option<String>,
    synopsis: Option<String>,
    metadata: Option<String>,
    popularity_score: Option<f64>,
    rating: Option<f64>,
}
#[derive(Debug, sqlx::FromRow)]
struct LegacyVideoGame {
    id: i64,
    product_id: i64,
    title: Option<String>,
    genre: Option<String>,
    release_date: Option<String>,
    developer: Option<String>,
    metadata: Option<String>,
    slug: Option<String>,
    normalized_title: Option<String>,
    external_ids: Option<String>,
    platform_codes: Option<String>,
    region_codes: Option<String>,
}

// ---------- PG-side helpers ----------
#[derive(Debug, Clone)]
struct VgUpd {
    vg_id: i64,
    slug: String,
    release_date: Option<NaiveDate>,
    developer: Option<String>,
    metadata: Option<Value>,
    region_codes: Option<Vec<String>>,
    genres: Option<Vec<String>>,
    display_title: Option<String>,
    synopsis: Option<String>,
    popularity_score: f64,
    rating: f64,
    average_rating: Option<f32>,
    rating_count: Option<i64>,
    rating_updated_at: Option<chrono::DateTime<chrono::Utc>>,
}

async fn ensure_hardware_row(db: &Db, product_id: i64) -> Result<()> {
    if !table_exists(db, "hardware").await.unwrap_or(false) {
        return Ok(());
    }
    sqlx::query("INSERT INTO public.hardware (product_id) VALUES ($1) ON CONFLICT DO NOTHING")
        .persistent(false)
        .bind(product_id)
        .execute(&db.pool)
        .await?;
    Ok(())
}

// Create or find a title without relying on products. Matches by normalized_title or lower(title).
async fn ensure_title_without_product(db: &Db, title: &str) -> Result<i64> {
    let trimmed = title.trim();
    let final_title = if trimmed.is_empty() {
        format!("Untitled {}", Uuid::new_v4().simple())
    } else {
        trimmed.to_string()
    };
    let norm = normalize_title(&final_title);
    let title_slug = if norm.is_empty() {
        format!("manual-title-{}", Uuid::new_v4().simple())
    } else {
        norm.clone()
    };
    let product_slug = format!("manual-product-{}", title_slug);
    let product_id = ensure_product_named_with_platform(
        db,
        "software",
        &product_slug,
        &final_title,
        "unknown",
    )
    .await?;
    ensure_software_row(db, product_id).await?;
    let title_id = ensure_video_game_title(db, product_id, &final_title, Some(&title_slug)).await?;
    Ok(title_id)
}

async fn load_iso2_to_iso3(_pool: &sqlx::Pool<sqlx::Postgres>) -> Result<HashMap<String, String>> {
    // country_iso_map table was removed in migration 0535
    // Use static ISO 3166-1 alpha-2 to alpha-3 mapping
    let mut map = HashMap::new();

    // Common countries from SQLite database
    map.insert("US".to_string(), "USA".to_string());
    map.insert("CA".to_string(), "CAN".to_string());
    map.insert("GB".to_string(), "GBR".to_string());
    map.insert("AU".to_string(), "AUS".to_string());
    map.insert("DK".to_string(), "DNK".to_string());
    map.insert("JP".to_string(), "JPN".to_string());
    map.insert("DE".to_string(), "DEU".to_string());
    map.insert("FR".to_string(), "FRA".to_string());
    map.insert("BR".to_string(), "BRA".to_string());
    map.insert("RU".to_string(), "RUS".to_string());
    map.insert("ZA".to_string(), "ZAF".to_string());
    map.insert("GI".to_string(), "GIB".to_string());
    map.insert("ER".to_string(), "ERI".to_string());
    map.insert("FO".to_string(), "FRO".to_string());
    map.insert("FM".to_string(), "FSM".to_string());
    map.insert("TJ".to_string(), "TJK".to_string());
    map.insert("HT".to_string(), "HTI".to_string());
    map.insert("BT".to_string(), "BTN".to_string());
    map.insert("SB".to_string(), "SLB".to_string());
    map.insert("NI".to_string(), "NIC".to_string());
    map.insert("LK".to_string(), "LKA".to_string());
    map.insert("NE".to_string(), "NER".to_string());
    map.insert("NG".to_string(), "NGA".to_string());
    map.insert("TM".to_string(), "TKM".to_string());
    map.insert("MQ".to_string(), "MTQ".to_string());
    map.insert("PF".to_string(), "PYF".to_string());
    map.insert("DM".to_string(), "DMA".to_string());
    map.insert("TR".to_string(), "TUR".to_string());
    map.insert("GE".to_string(), "GEO".to_string());

    // EU is not a country code in ISO 3166-1, but map it to Eurozone placeholder
    map.insert("EU".to_string(), "EUR".to_string());

    // Extended coverage for common regions
    map.insert("CN".to_string(), "CHN".to_string());
    map.insert("IN".to_string(), "IND".to_string());
    map.insert("IT".to_string(), "ITA".to_string());
    map.insert("ES".to_string(), "ESP".to_string());
    map.insert("MX".to_string(), "MEX".to_string());
    map.insert("KR".to_string(), "KOR".to_string());
    map.insert("NL".to_string(), "NLD".to_string());
    map.insert("SE".to_string(), "SWE".to_string());
    map.insert("NO".to_string(), "NOR".to_string());
    map.insert("FI".to_string(), "FIN".to_string());
    map.insert("PL".to_string(), "POL".to_string());
    map.insert("BE".to_string(), "BEL".to_string());
    map.insert("CH".to_string(), "CHE".to_string());
    map.insert("AT".to_string(), "AUT".to_string());
    map.insert("PT".to_string(), "PRT".to_string());
    map.insert("IE".to_string(), "IRL".to_string());
    map.insert("NZ".to_string(), "NZL".to_string());
    map.insert("SG".to_string(), "SGP".to_string());
    map.insert("HK".to_string(), "HKG".to_string());
    map.insert("TW".to_string(), "TWN".to_string());
    map.insert("AR".to_string(), "ARG".to_string());
    map.insert("CL".to_string(), "CHL".to_string());
    map.insert("CO".to_string(), "COL".to_string());
    map.insert("PE".to_string(), "PER".to_string());
    map.insert("VE".to_string(), "VEN".to_string());
    map.insert("TH".to_string(), "THA".to_string());
    map.insert("ID".to_string(), "IDN".to_string());
    map.insert("MY".to_string(), "MYS".to_string());
    map.insert("PH".to_string(), "PHL".to_string());
    map.insert("VN".to_string(), "VNM".to_string());
    map.insert("AE".to_string(), "ARE".to_string());
    map.insert("SA".to_string(), "SAU".to_string());
    map.insert("IL".to_string(), "ISR".to_string());
    map.insert("EG".to_string(), "EGY".to_string());
    map.insert("GR".to_string(), "GRC".to_string());
    map.insert("CZ".to_string(), "CZE".to_string());
    map.insert("HU".to_string(), "HUN".to_string());
    map.insert("RO".to_string(), "ROU".to_string());
    map.insert("UA".to_string(), "UKR".to_string());

    info!("Loaded {} ISO2→ISO3 country code mappings", map.len());
    Ok(map)
}

// ---------- Batch updater for VG ----------

async fn flush_video_game_updates(
    pool: &sqlx::Pool<sqlx::Postgres>,
    buf: &mut Vec<VgUpd>,
    have_genres: bool,
    have_region_codes: bool,
    have_metadata: bool,
    have_display: bool,
    have_synopsis: bool,
    have_avg_rating: bool,
    have_rating_count: bool,
    have_rating_updated: bool,
) -> Result<()> {
    if buf.is_empty() {
        return Ok(());
    }

    // Build a single UPDATE FROM (VALUES ...) statement to reduce round trips.
    // Columns always included: id, slug, popularity_score, rating.
    // Optional columns: release_date, developer, metadata, region_codes, genres, display_title,
    // synopsis, average_rating, rating_count, rating_updated_at. We emit NULL where absent.

    use sqlx::QueryBuilder;
    // Filter out any obviously invalid rows to avoid malformed tuples
    let rows: Vec<&VgUpd> = buf.iter().filter(|r| r.vg_id > 0).collect();
    if rows.is_empty() {
        buf.clear();
        return Ok(());
    }

    let mut qb = QueryBuilder::new("WITH src(id, slug, popularity_score, rating");
    if have_metadata {
        qb.push(", metadata");
    }
    if have_region_codes {
        qb.push(", region_codes");
    }
    if have_genres {
        qb.push(", genres");
    }
    if have_display {
        qb.push(", display_title");
    }
    if have_synopsis {
        qb.push(", synopsis");
    }
    if have_avg_rating {
        qb.push(", average_rating");
    }
    if have_rating_count {
        qb.push(", rating_count");
    }
    if have_rating_updated {
        qb.push(", rating_updated_at");
    }
    qb.push(", release_date, developer) AS (VALUES ");

    {
        let mut separated = qb.separated(", ");
        for row in rows.iter() {
            separated.push("(");
            separated.push_bind(row.vg_id); // id
            separated.push(", ");
            separated.push_bind(&row.slug); // slug
            separated.push(", ");
            separated.push_bind(row.popularity_score); // popularity_score
            separated.push(", ");
            separated.push_bind(row.rating); // rating
            if have_metadata {
                separated.push(", ");
                if let Some(meta) = row.metadata.as_ref() {
                    separated.push_bind(Json(meta.clone()));
                } else {
                    separated.push("NULL::jsonb");
                }
            }
            if have_region_codes {
                separated.push(", ");
                if let Some(rc) = row.region_codes.as_ref() {
                    separated.push_bind(rc);
                } else {
                    separated.push("NULL::text[]");
                }
            }
            if have_genres {
                separated.push(", ");
                if let Some(g) = row.genres.as_ref() {
                    separated.push_bind(g);
                } else {
                    separated.push("NULL::text[]");
                }
            }
            if have_display {
                separated.push(", ");
                if let Some(d) = row.display_title.as_ref() {
                    separated.push_bind(d);
                } else {
                    separated.push("NULL::text");
                }
            }
            if have_synopsis {
                separated.push(", ");
                if let Some(s) = row.synopsis.as_ref() {
                    separated.push_bind(s);
                } else {
                    separated.push("NULL::text");
                }
            }
            if have_avg_rating {
                separated.push(", ");
                if let Some(ar) = row.average_rating.as_ref() {
                    separated.push_bind(ar);
                } else {
                    separated.push("NULL::real");
                }
            }
            if have_rating_count {
                separated.push(", ");
                if let Some(rc) = row.rating_count.as_ref() {
                    separated.push_bind(rc);
                } else {
                    separated.push("NULL::bigint");
                }
            }
            if have_rating_updated {
                separated.push(", ");
                if let Some(ru) = row.rating_updated_at.as_ref() {
                    separated.push_bind(ru);
                } else {
                    separated.push("NULL::timestamptz");
                }
            }
            // release_date
            separated.push(", ");
            if let Some(rd) = row.release_date {
                separated.push_bind(rd);
            } else {
                separated.push("NULL::date");
            }
            // developer
            separated.push(", ");
            if let Some(dev) = row.developer.as_ref() {
                separated.push_bind(dev);
            } else {
                separated.push("NULL::text");
            }
            separated.push(")");
        }
    }

    qb.push(") UPDATE public.video_games vg SET ");
    qb.push(
        "slug = src.slug, popularity_score = src.popularity_score, rating = src.rating, updated_at=now()"
    );
    if have_metadata {
        qb.push(", metadata = COALESCE(src.metadata, vg.metadata)");
    }
    if have_region_codes {
        qb.push(", region_codes = COALESCE(src.region_codes, vg.region_codes)");
    }
    if have_genres {
        qb.push(", genres = COALESCE(src.genres, vg.genres)");
    }
    if have_display {
        qb.push(", display_title = COALESCE(src.display_title, vg.display_title)");
    }
    if have_synopsis {
        qb.push(", synopsis = COALESCE(src.synopsis, vg.synopsis)");
    }
    if have_avg_rating {
        qb.push(", average_rating = COALESCE(src.average_rating, vg.average_rating)");
    }
    if have_rating_count {
        qb.push(", rating_count = COALESCE(src.rating_count, vg.rating_count)");
    }
    if have_rating_updated {
        qb.push(", rating_updated_at = COALESCE(src.rating_updated_at, vg.rating_updated_at)");
    }
    qb.push(
        ", release_date = COALESCE(src.release_date, vg.release_date), developer = COALESCE(src.developer, vg.developer) "
    );
    qb.push("FROM src WHERE vg.id = src.id");
    // Execute; on failure, fall back to per-row updates to ensure forward progress
    if let Err(_e) = qb.build().persistent(false).execute(pool).await {
        for r in rows.iter() {
            sqlx::query(
                "UPDATE public.video_games SET \
                 slug = $2, popularity_score = $3, rating = $4, \
                 metadata = COALESCE($5, metadata), \
                 region_codes = COALESCE($6, region_codes), \
                 genres = COALESCE($7, genres), \
                 display_title = COALESCE($8, display_title), \
                 synopsis = COALESCE($9, synopsis), \
                 average_rating = COALESCE($10, average_rating), \
                 rating_count = COALESCE($11, rating_count), \
                 rating_updated_at = COALESCE($12, rating_updated_at), \
                 release_date = COALESCE($13, release_date), \
                 developer = COALESCE($14, developer), \
                 updated_at = now() \
                 WHERE id = $1",
            )
            .persistent(false)
            .bind(r.vg_id)
            .bind(&r.slug)
            .bind(r.popularity_score)
            .bind(r.rating)
            .bind(r.metadata.as_ref().map(|m| Json(m.clone())))
            .bind(r.region_codes.as_ref())
            .bind(r.genres.as_ref())
            .bind(r.display_title.as_ref())
            .bind(r.synopsis.as_ref())
            .bind(r.average_rating.as_ref())
            .bind(r.rating_count.as_ref())
            .bind(r.rating_updated_at.as_ref())
            .bind(r.release_date)
            .bind(r.developer.as_ref())
            .execute(pool)
            .await?;
        }
    }
    buf.clear();
    Ok(())
}

async fn handle_update_retry(e: sqlx::Error, attempts: usize, batch: usize) -> Result<()> {
    let retryable = matches!(
        e,
        sqlx::Error::Io(_) | sqlx::Error::PoolTimedOut | sqlx::Error::Database(_)
    );
    if !retryable || attempts >= 3 {
        return Err(e.into());
    }
    let backoff_ms = 200 * (attempts as u64);
    warn!(batch, attempts, error=?e, backoff_ms, "retry batch update");
    tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
    Ok(())
}

// ---------- Pure helpers ----------

fn parse_date(raw: &str) -> Option<NaiveDate> {
    let t = raw.trim();
    if t.is_empty() {
        return None;
    }
    NaiveDate::parse_from_str(t, "%Y-%m-%d")
        .or_else(|_| NaiveDate::parse_from_str(t, "%Y-%m-%d %H:%M:%S"))
        .or_else(|_| NaiveDate::parse_from_str(t, "%Y-%m-%dT%H:%M:%S"))
        .ok()
}

fn parse_string_array(raw: &str) -> Option<Vec<String>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    if trimmed.starts_with('[') {
        if let Ok(values) = serde_json::from_str::<Vec<Value>>(trimmed) {
            let mut out = Vec::new();
            for v in values {
                match v {
                    Value::String(s) => out.push(simple_slug(&s)),
                    Value::Object(obj) => {
                        if let Some(code) = obj.get("code").and_then(|v| v.as_str()) {
                            out.push(simple_slug(code));
                        } else if let Some(slug) = obj.get("slug").and_then(|v| v.as_str()) {
                            out.push(simple_slug(slug));
                        } else if let Some(name) = obj.get("name").and_then(|v| v.as_str()) {
                            out.push(simple_slug(name));
                        }
                    }
                    _ => {}
                }
            }
            if !out.is_empty() {
                return Some(out);
            }
        }
        return None;
    }
    let parts: Vec<String> = trimmed
        .split(|c: char| (c == ',' || c == ';' || c == '|' || c.is_whitespace()))
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(simple_slug)
        .filter(|s| !s.is_empty())
        .collect();
    if parts.is_empty() {
        None
    } else {
        Some(parts)
    }
}

fn simple_slug(value: &str) -> String {
    let mut slug = String::new();
    let mut last_dash = false;
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            slug.push(ch.to_ascii_lowercase());
            last_dash = false;
        } else if matches!(ch, '-' | '_' | ' ' | '/' | ':' | '.') {
            if !last_dash && !slug.is_empty() {
                slug.push('-');
                last_dash = true;
            }
        }
    }
    slug.trim_matches('-').to_string()
}

// Merge provider codes from legacy external_ids list into a metadata JSON object.
// external_ids is expected to be either a JSON array (e.g. ["steam:123","psn:456"]) or a delimited string.
// We extract provider codes (substring before first ':') and maintain a unique, sorted array in metadata.providers.
fn merge_providers_into_metadata(
    mut metadata: Option<Value>,
    external_ids: &Option<String>,
) -> Option<Value> {
    let mut providers: HashSet<String> = HashSet::new();
    // Extract existing providers from metadata if present
    if let Some(Value::Object(obj)) = metadata.as_ref() {
        if let Some(Value::Array(arr)) = obj.get("providers") {
            for v in arr {
                if let Some(s) = v.as_str() {
                    providers.insert(s.to_string());
                }
            }
        }
    }
    if let Some(raw) = external_ids
        .as_ref()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
    {
        // Try JSON first
        let parsed: Option<Vec<String>> = if raw.starts_with('[') {
            serde_json::from_str::<Vec<Value>>(raw).ok().map(|vals| {
                vals.into_iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect::<Vec<_>>()
            })
        } else {
            None
        };
        let list: Vec<String> = if let Some(p) = parsed {
            p
        } else {
            raw.split(|c: char| (c == ',' || c == ';' || c == '|' || c.is_whitespace()))
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .collect()
        };
        for entry in list {
            let code = entry
                .split(':')
                .next()
                .unwrap_or(&entry)
                .trim()
                .to_ascii_lowercase();
            if !code.is_empty() {
                providers.insert(code);
            }
        }
    }
    if providers.is_empty() {
        return metadata;
    }
    // Build/merge object
    let mut obj = match metadata.take() {
        Some(Value::Object(map)) => map,
        _ => serde_json::Map::new(),
    };
    let mut provider_vec: Vec<String> = providers.into_iter().collect();
    provider_vec.sort();
    obj.insert(
        "providers".into(),
        Value::Array(provider_vec.into_iter().map(Value::String).collect()),
    );
    Some(Value::Object(obj))
}

fn normalize_product_kind(raw: Option<&str>) -> &'static str {
    if let Some(s) = raw {
        let lc = s.trim().to_ascii_lowercase();
        if lc.contains("hardware") || lc.contains("console") || lc.contains("device") {
            return "hardware";
        }
    }
    "software"
}

/// Normalize provider slugs to the canonical values used by this repo.
///
/// Important: this is for *provider identity* (providers.slug), not for media enum values.
/// The PlayStation media source stored in `game_media.source` is normalized elsewhere to `psstore`.
fn normalize_provider_slug(slug: &str) -> &str {
    let s = slug.trim();
    if s.eq_ignore_ascii_case("psn")
        || s.eq_ignore_ascii_case("psstore")
        || s.eq_ignore_ascii_case("ps_store")
        || s.eq_ignore_ascii_case("ps")
        || s.eq_ignore_ascii_case("playstation")
        || s.eq_ignore_ascii_case("playstation_store")
        || s.eq_ignore_ascii_case("playstation-store")
    {
        return "ps-store";
    }
    s
}

fn derive_provider_slug_from_url(url: &str) -> Option<String> {
    // Very lightweight host extraction (avoid adding heavy deps):
    let lower = url.trim().to_ascii_lowercase();
    let host = if let Some(pos) = lower.find("://") {
        let rest = &lower[pos + 3..];
        if let Some(end) = rest.find('/') {
            &rest[..end]
        } else {
            rest
        }
    } else {
        // No scheme, try up to first slash
        if let Some(end) = lower.find('/') {
            &lower[..end]
        } else {
            lower.as_str()
        }
    };
    let host = host.trim_start_matches("www.");
    // Known mappings from host patterns → provider slug (must already exist in providers)
    let candidates: &[(&str, &str)] = &[
        ("steamstatic.com", "steam"),
        ("steampowered.com", "steam"),
        ("store.steampowered.com", "steam"),
        ("cdn.cloudflare.steamstatic.com", "steam"),
        ("playstation.net", "ps-store"),
        ("sonyentertainmentnetwork.com", "ps-store"),
        ("pscdn.co", "ps-store"),
        ("image.api.playstation.com", "ps-store"),
        ("media.playstation.com", "ps-store"),
        ("rawg.io", "rawg"),
        ("media.rawg.io", "rawg"),
        ("data.rawg.io", "rawg"),
        ("igdb.com", "igdb"),
        ("images.igdb.com", "igdb"),
        ("thegamesdb.net", "tgdb"),
        ("giantbomb.com", "giantbomb"),
        ("giantbomb1.cbsistatic.com", "giantbomb"),
        ("static.giantbomb.com", "giantbomb"),
        ("store-images.s-microsoft.com", "xbox"),
        ("assets.xbox.com", "xbox"),
        ("xboxlive.com", "xbox"),
        ("gog.com", "gog"),
        ("images.gog.com", "gog"),
        ("ytimg.com", "youtube"),
        ("youtube.com", "youtube"),
    ];
    for (needle, slug) in candidates {
        if host.contains(needle) {
            return Some((*slug).to_string());
        }
    }
    None
}

fn resolve_product_slug_base(preferred: Option<&str>, name: &str, legacy_id: i64) -> String {
    preferred
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(simple_slug)
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| {
            let from_name = simple_slug(name);
            if from_name.is_empty() {
                format!("legacy-product-{legacy_id}")
            } else {
                from_name
            }
        })
}

fn resolve_video_game_slug_base(
    preferred: Option<&str>,
    product: &ProductInfo,
    platform_slug_maybe_empty: &str,
    platform_id: i64,
) -> String {
    if let Some(b) = preferred
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(simple_slug)
        .filter(|s| !s.is_empty())
    {
        return b;
    }
    let plat = if platform_slug_maybe_empty.is_empty() {
        format!("platform-{platform_id}")
    } else {
        platform_slug_maybe_empty.to_string()
    };
    let base = format!("{}-{}", product.slug, plat);
    if base.is_empty() {
        format!("legacy-vg-{}", product.pg_id)
    } else {
        base
    }
}

fn unique_slug_into(base: &str, set: &mut HashSet<String>) -> String {
    let mut candidate = base.to_string();
    let mut index = 2;
    while set.contains(&candidate) {
        candidate = format!("{}-{}", base, index);
        index += 1;
    }
    set.insert(candidate.clone());
    candidate
}

fn normalize_title(s: &str) -> String {
    s.trim().to_lowercase()
}

// ---------- CLI helpers ----------

fn resolve_sqlite_path(arg: Option<String>) -> Result<PathBuf> {
    let candidate = arg.unwrap_or_else(|| "database.sqlite".to_string());
    let mut path = PathBuf::from(&candidate);
    if !path.is_absolute() {
        let cwd = std::env::current_dir().context("failed to resolve current working directory")?;
        path = cwd.join(path);
    }
    if !path.exists() {
        return Err(anyhow!("legacy sqlite file '{}' not found", path.display()));
    }
    Ok(path.canonicalize().unwrap_or(path))
}

fn resolve_pg_url(arg: Option<String>) -> Result<String> {
    if let Some(url) = arg {
        let trimmed = url.trim();
        if trimmed.is_empty() {
            bail!("database URL argument cannot be empty");
        }
        // Prefer session pooler if user passed a Supabase transaction pooler URL.
        return Ok(db_url_prefer_session_url(trimmed));
    }
    // Fall back to env-based resolver (prefers SUPABASE_DB_SESSION_URL > SUPABASE_DB_URL > DATABASE_URL)
    match db_url_prefer_session() {
        Ok(u) => Ok(u),
        Err(e) => Err(anyhow!(
            "provide a Postgres URL as second CLI argument or set SUPABASE_DB_SESSION_URL / SUPABASE_DB_URL / DATABASE_URL: {e}"
        )),
    }
}

fn db_url_prefer_session_url(raw: &str) -> String {
    // Minimal inline mirror of prefer_session_mode to avoid additional imports here.
    // Respect DISABLE_SESSION_SWAP=1 to keep the URL exactly as provided (e.g., for direct IPv6 hosts
    // or when deliberately using the transaction pooler for testing).
    if std::env::var("DISABLE_SESSION_SWAP")
        .map(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false)
    {
        return raw.to_string();
    }
    if raw.contains("pooler.supabase.com:6543") {
        raw.replace("pooler.supabase.com:6543", "pooler.supabase.com:5432")
    } else {
        raw.to_string()
    }
}
