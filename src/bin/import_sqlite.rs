use anyhow::bail;
use anyhow::{Context, Result, anyhow};
use chrono::NaiveDate;
use futures::TryStreamExt;
use i_miss_rust::database_ops::db::Db;
use i_miss_rust::database_ops::ingest_providers::{
    ProviderEntityCache, ensure_currency, ensure_platform, ensure_provider,
    ensure_provider_media_links_with_meta, ensure_sellable_for_title, upsert_game_media_batch,
};
use i_miss_rust::util::env::{self, db_url_prefer_session};
use serde_json::Value;
use sqlx::sqlite::SqlitePoolOptions; // requires sqlx feature "sqlite"
use sqlx::types::Json; // needed for batch video_game metadata updates
use sqlx::{Row, SqlitePool}; // provided by sqlx when "sqlite" feature is enabled
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tracing::{info, warn};

const UPDATE_CHUNK: usize = 500;

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
    let lower = url.to_ascii_lowercase();
    if lower.ends_with(".webm") {
        "video/webm"
    } else if lower.ends_with(".m3u8") {
        "application/vnd.apple.mpegurl"
    } else if lower.ends_with(".mpd") {
        "application/dash+xml"
    } else if lower.ends_with(".mov") {
        "video/quicktime"
    } else if lower.ends_with(".avi") {
        "video/x-msvideo"
    } else if lower.ends_with(".mpg") || lower.ends_with(".mpeg") {
        "video/mpeg"
    } else {
        "video/mp4"
    }
}

/// Progress logging interval (rows). Override with env PROGRESS_INTERVAL.
fn progress_interval() -> usize {
    std::env::var("PROGRESS_INTERVAL")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(UPDATE_CHUNK)
}

/// Video game UPDATE batch size (rows). Override with env VIDEO_GAME_UPDATE_CHUNK.
fn update_chunk_size() -> usize {
    std::env::var("VIDEO_GAME_UPDATE_CHUNK")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(UPDATE_CHUNK)
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
        .unwrap_or(40);
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

    // Optional FAST_INGEST session tweaks: enable with FAST_INGEST=1 (default off).
    // These settings trade durability for speed during one-shot bulk imports.
    // They are applied only for the lifetime of this session.
    if std::env::var("FAST_INGEST")
        .ok()
        .map(|v| (v == "1" || v.eq_ignore_ascii_case("true")))
        .unwrap_or(false)
    {
        if let Err(e) = apply_fast_ingest_session(&db).await {
            tracing::warn!(error=?e, "FAST_INGEST session optimization failed; continuing without it");
        } else {
            tracing::info!("FAST_INGEST session optimizations applied");
        }
    }

    let mut ctx = ImportContext::new(db, sqlite).await?;
    ctx.run().await?;

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
        .unwrap_or(64);
    let sql = format!(
        concat!(
            "SET synchronous_commit = OFF;",
            "SET statement_timeout = 0;",
            "SET jit = OFF;",
            "SET work_mem = {}MB;",
            "SET temp_buffers = '64MB';"
        ),
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
            // Enforce read-only behavior at the connection level
            "PRAGMA query_only=ON;",
            "PRAGMA synchronous=NORMAL;",
            "PRAGMA temp_store=MEMORY;",
            "PRAGMA cache_size=-80000;", // ~80k pages (~80MB) if page_size=1KB
            "PRAGMA mmap_size=268435456;", // 256MB
            "PRAGMA optimize;",
        ]
    } else {
        vec![
            // Enforce read-only behavior at the connection level
            "PRAGMA query_only=ON;",
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
    sellable_id: Option<i64>,
}

struct ImportContext {
    // Connections
    db: Db,
    sqlite: SqlitePool,
    cache: ProviderEntityCache,

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
    sellable_by_title: HashMap<i64, i64>, // title_id -> sellable_id
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
    async fn new(db: Db, sqlite: SqlitePool) -> Result<Self> {
        let cache = ProviderEntityCache::new(db.clone());
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
            sellable_by_title: HashMap::new(),
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
        let media_staging_enabled = std::env::var("MEDIA_STAGING")
            .ok()
            .map(|v| (v == "1" || v.eq_ignore_ascii_case("true")))
            .unwrap_or(false);
        if self.media_only {
            info!(
                "MEDIA_ONLY enabled — skipping non-media stages (lookups, products, video_games)"
            );
        } else {
            // Providers (from legacy sources) first so media can resolve provider slugs
            let _ = self.import_video_game_sources().await;

            // Lookups
            self.import_currencies().await?;
            self.mark_stage_done("currencies").await?;
            self.import_platforms().await?;
            self.mark_stage_done("platforms").await?;
            self.import_countries().await?;
            self.mark_stage_done("countries").await?;

            // Relationships needed for VG derivation
            self.load_product_platform_links().await?;

            // Core entities
            self.import_products().await?;
            self.mark_stage_done("products").await?;
            self.import_video_games().await?;
            self.mark_stage_done("video_games").await?;
        }

        // Legacy media tables → game_media
        let _ = self.import_game_images().await.map(|_| ());
        let _ = self.import_game_videos().await.map(|_| ());
        if media_staging_enabled {
            let _ = self.process_staged_video_media().await.map(|_| ());
        } else {
            info!("MEDIA_STAGING disabled — skipping legacy_video_media_raw drain");
        }

        // Giant Bomb media specialized path (optional)
        // Run images in chunked loops until the stage marks done. This avoids having to
        // restart the importer for each chunk and leverages the durable checkpoint
        // (gb_images_id) between iterations.
        loop {
            if self.is_stage_done("gb_images").await.unwrap_or(false) {
                break;
            }
            let _ = self.import_giantbomb_images().await.map(|_| ());
            // Small yield between chunks to be polite to SQLite on slower disks
            // and to give Postgres a moment to flush indexes.
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        let _ = self.import_giantbomb_videos().await.map(|_| ());

        // Global dedupe for safety
        let _ = self.dedupe_all().await.map(|_| ());
        Ok(())
    }
}

// Methods on ImportContext
impl ImportContext {
    async fn process_staged_video_media(&mut self) -> Result<()> {
        if !self
            .pg_table_exists("public.legacy_video_media_raw")
            .await?
        {
            info!("legacy_video_media_raw table missing — skipping staged video processing");
            return Ok(());
        }
        // Same fallback invocation for video media path (in case only videos staged or image path skipped)
        let need_fallback = self.legacy_vg_map.is_empty() || self.product_map.is_empty();
        let enable_fallback = std::env::var("MEDIA_FALLBACK_MAPPING")
            .ok()
            .map(|v| (v == "1" || v.eq_ignore_ascii_case("true")))
            .unwrap_or(need_fallback);
        if need_fallback && enable_fallback {
            if let Err(e) = self.rebuild_fallback_media_mappings().await {
                warn!(error=?e, "fallback media mapping rebuild failed (video); proceeding without it");
            } else {
                info!(
                    "fallback media mappings rebuilt (video path) product_map={}, legacy_vg_map={}",
                    self.product_map.len(),
                    self.legacy_vg_map.len()
                );
            }
        }
        let limit: i64 = std::env::var("MEDIA_STAGE_PROCESS_LIMIT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(5000);
        let rows = sqlx
            ::query(
                "SELECT id, legacy_id, legacy_product_id, legacy_video_game_id, provider_slug, kind, url, title, mime_type, duration_seconds, external_id, legacy_metadata FROM public.legacy_video_media_raw WHERE processed=false ORDER BY id LIMIT $1"
            )
            .bind(limit)
            .persistent(false)
            .fetch_all(&self.db.pool).await?;
        if rows.is_empty() {
            info!("no staged video media to process");
            return Ok(());
        }
        // --- New optimized path: batch ensure provider_items first to avoid per-row round trips ---
        #[derive(Debug)]
        struct StagedVidRow {
            provider_slug: String,
            provider_item_external: String, // external key used for provider_items
            external_id: String,            // media external id
            url: String,
            media_type: String,
            provider_data: Value,
            video_game_ids: Vec<i64>,
        }
        let mut staged: Vec<StagedVidRow> = Vec::new();
        let mut provider_item_pairs: Vec<(i64, String)> = Vec::new(); // (provider_id, external_item_id)
        let mut row_ids: Vec<i64> = Vec::with_capacity(rows.len());
        for r in rows.iter() {
            let rid: i64 = r.try_get("id")?;
            row_ids.push(rid);
            let url: String = r.try_get("url")?;
            if url.trim().is_empty() {
                continue;
            }
            let kind: String = r
                .try_get::<String, _>("kind")
                .unwrap_or_else(|_| "trailer".into());
            let provider_slug: Option<String> = r
                .try_get::<Option<String>, _>("provider_slug")
                .ok()
                .flatten();
            let provider_slug = provider_slug.unwrap_or_else(|| "manual".into());
            let provider_id = self.provider_id_for_slug(&provider_slug).await?;
            let ext_id: Option<String> = r.try_get("external_id").ok();
            let external_id = ext_id
                .filter(|e| !e.trim().is_empty())
                .unwrap_or_else(|| url.clone());
            let mut target_vgs: Vec<i64> = Vec::new();
            if let Ok(legacy_vg) = r.try_get::<i64, _>("legacy_video_game_id") {
                if legacy_vg > 0 {
                    if let Some(pg_vg) = self.legacy_vg_map.get(&legacy_vg) {
                        target_vgs.push(*pg_vg);
                    }
                }
            }
            if target_vgs.is_empty() {
                if let Ok(legacy_pid) = r.try_get::<i64, _>("legacy_product_id") {
                    if legacy_pid > 0 {
                        if let Some(info) = self.product_map.get(&legacy_pid) {
                            if let Some(title_id) = info.title_id {
                                if let Some(platforms) = self.product_platforms.get(&legacy_pid) {
                                    for pid in platforms {
                                        let vg_id = self
                                            .cache
                                            .ensure_video_game(title_id, *pid, None)
                                            .await?;
                                        target_vgs.push(vg_id);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            if target_vgs.is_empty() {
                continue;
            }
            let mut meta_map = serde_json::Map::new();
            meta_map.insert("kind".into(), Value::String(kind.clone()));
            meta_map.insert("provider_slug".into(), Value::String(provider_slug.clone()));
            meta_map.insert("url".into(), Value::String(url.clone()));
            if let Ok(title) = r.try_get::<Option<String>, _>("title") {
                if let Some(t) = title {
                    meta_map.insert("title".into(), Value::String(t));
                }
            }
            if let Ok(mime) = r.try_get::<Option<String>, _>("mime_type") {
                if let Some(m) = mime {
                    meta_map.insert("mime_type".into(), Value::String(m));
                }
            }
            if let Ok(dur) = r.try_get::<Option<i32>, _>("duration_seconds") {
                if let Some(ds) = dur {
                    meta_map.insert("duration_seconds".into(), Value::Number(ds.into()));
                }
            }
            if let Ok(legacy_meta) = r.try_get::<Option<Value>, _>("legacy_metadata") {
                if let Some(lm) = legacy_meta {
                    meta_map.insert("legacy_metadata".into(), lm);
                }
            }
            let provider_data = Value::Object(meta_map);
            let pi_external = format!("media:{}", external_id);
            provider_item_pairs.push((provider_id, pi_external.clone()));
            let media_type = (if kind.eq_ignore_ascii_case("gameplay") {
                "gameplay"
            } else {
                "trailer"
            })
            .to_string();
            staged.push(StagedVidRow {
                provider_slug,
                provider_item_external: pi_external,
                external_id,
                url,
                media_type,
                provider_data,
                video_game_ids: target_vgs,
            });
        }
        if staged.is_empty() {
            info!("no eligible staged video media rows after filtering");
            return Ok(());
        }
        // Deduplicate provider_item pairs.
        use std::collections::HashMap as StdHashMap;
        let mut uniq_pairs: StdHashMap<(i64, String), ()> = StdHashMap::new();
        for (pid, ext) in provider_item_pairs.into_iter() {
            uniq_pairs.entry((pid, ext)).or_insert(());
        }
        let (pid_vec, ext_vec): (Vec<i64>, Vec<String>) = uniq_pairs.keys().cloned().unzip();
        // Batch ensure provider_items
        let ensured_rows = sqlx::query(
            r#"WITH inp AS (
                SELECT UNNEST($1::bigint[]) AS provider_id,
                       UNNEST($2::text[])   AS external_item_id
            )
            INSERT INTO public.provider_items (provider_id, external_item_id)
            SELECT provider_id, external_item_id FROM inp
            ON CONFLICT (provider_id, external_item_id)
            DO UPDATE SET updated_at = now()
            RETURNING provider_id, external_item_id, id"#,
        )
        .bind(&pid_vec)
        .bind(&ext_vec)
        .persistent(false)
        .fetch_all(&self.db.pool)
        .await?;
        let mut provider_item_map: StdHashMap<(i64, String), i64> = StdHashMap::new();
        for r in ensured_rows {
            let pid: i64 = r.try_get("provider_id")?;
            let ext: String = r.try_get("external_item_id")?;
            let id: i64 = r.try_get("id")?;
            provider_item_map.insert((pid, ext), id);
        }
        // Build media batch and link groups using resolved provider_item ids.
        let mut media_batch: Vec<(i64, String, String, String, String, Value)> = Vec::new();
        let mut link_groups: StdHashMap<
            (i64, i64, String),
            Vec<(String, Option<String>, Option<String>, Option<String>)>,
        > = StdHashMap::new();
        for sv in staged.into_iter() {
            let provider_id = self.provider_id_for_slug(&sv.provider_slug).await?; // cached
            let key = (provider_id, sv.provider_item_external.clone());
            let provider_item_id = *provider_item_map
                .get(&key)
                .expect("provider_item_id missing after batch ensure");
            for vg_id in sv.video_game_ids.iter().copied() {
                media_batch.push((
                    vg_id,
                    sv.provider_slug.clone(),
                    sv.external_id.clone(),
                    sv.media_type.clone(),
                    sv.url.clone(),
                    sv.provider_data.clone(),
                ));
                link_groups
                    .entry((provider_item_id, vg_id, sv.provider_slug.clone()))
                    .or_default()
                    .push((sv.url.clone(), Some(sv.media_type.clone()), None, None));
            }
        }
        if !media_batch.is_empty() {
            let borrowed: Vec<(i64, &str, &str, &str, &str, &Value)> = media_batch
                .iter()
                .map(|(a, b, c, d, e, f)| (*a, b.as_str(), c.as_str(), d.as_str(), e.as_str(), f))
                .collect();
            let _ = upsert_game_media_batch(self.cache.db(), &borrowed).await?;
            for ((provider_item_id, vg_id, src), urls) in link_groups.into_iter() {
                let _ = ensure_provider_media_links_with_meta(
                    self.cache.db(),
                    provider_item_id,
                    Some(vg_id),
                    &urls,
                    &src,
                    None,
                )
                .await?;
            }
            sqlx::query(
                "UPDATE public.legacy_video_media_raw SET processed=true WHERE id = ANY($1)",
            )
            .bind(&row_ids)
            .persistent(false)
            .execute(&self.db.pool)
            .await
            .ok();
        }
        info!(
            processed_rows = media_batch.len(),
            "staged video media processed (batch provider_item ensures)"
        );
        Ok(())
    }

    // ---------------------------------------------------------------------
    // Fallback mapping reconstruction for media staging
    // ---------------------------------------------------------------------
    // Build minimal product_map (legacy_product_id -> ProductInfo) and legacy_vg_map
    // (legacy_video_game_id -> pg video_games.id) for the set of legacy IDs referenced in
    // unprocessed staging rows. This allows media processing to succeed when earlier
    // import stages (products/video_games) were skipped due to sentinels or FORCE flags.
    async fn rebuild_fallback_media_mappings(&mut self) -> Result<()> {
        use sqlx::Row;
        let img_exists = self
            .pg_table_exists("public.legacy_image_media_raw")
            .await?;
        let vid_exists = self
            .pg_table_exists("public.legacy_video_media_raw")
            .await?;
        if !img_exists && !vid_exists {
            info!("legacy media staging tables missing — skipping fallback mapping rebuild");
            return Ok(());
        }
        // Collect distinct legacy product ids from staging tables.
        // Optimized: perform two simple indexed scans (after adding partial indexes) and
        // deduplicate in-memory instead of forcing PostgreSQL to hash/merge DISTINCT + UNION.
        // This substantially reduces planning/execution time for large staging sets.
        use std::collections::HashSet as StdHashSet;
        let mut legacy_product_ids: StdHashSet<i64> = StdHashSet::new();
        // Image staging rows
        let img_prod_rows = sqlx
            ::query_scalar::<_, i64>(
                "SELECT legacy_product_id FROM public.legacy_image_media_raw WHERE processed=false AND legacy_product_id > 0"
            )
            .persistent(false)
            .fetch_all(&self.db.pool).await?;
        for pid in img_prod_rows {
            legacy_product_ids.insert(pid);
        }
        // Video staging rows
        let vid_prod_rows = sqlx
            ::query_scalar::<_, i64>(
                "SELECT legacy_product_id FROM public.legacy_video_media_raw WHERE processed=false AND legacy_product_id > 0"
            )
            .persistent(false)
            .fetch_all(&self.db.pool).await?;
        for pid in vid_prod_rows {
            legacy_product_ids.insert(pid);
        }
        // Only pull SQLite product rows we do not already have mappings for.
        for legacy_pid in legacy_product_ids.into_iter() {
            if self.product_map.contains_key(&legacy_pid) {
                continue;
            }
            // Lookup legacy product in SQLite
            let lp_opt = sqlx
                ::query(
                    "SELECT id, name, slug, category, release_date, synopsis, metadata, CAST(popularity_score AS REAL) AS popularity_score, CAST(rating AS REAL) AS rating FROM products WHERE id = ?"
                )
                .persistent(false)
                .bind(legacy_pid)
                .fetch_optional(&self.sqlite).await?;
            let Some(lp) = lp_opt else {
                continue;
            };
            let name: String = lp.try_get::<String, _>("name").unwrap_or_default();
            let slug: String = lp
                .try_get::<String, _>("slug")
                .unwrap_or_else(|_| simple_slug(&name));
            // Attempt PG product resolution by slug first, then by case-insensitive name.
            let pg_product_id_opt =
                sqlx::query_scalar::<_, i64>("SELECT id FROM public.products WHERE slug = $1")
                    .persistent(false)
                    .bind(&slug)
                    .fetch_optional(&self.db.pool)
                    .await?;
            let pg_product_id = if let Some(pid) = pg_product_id_opt {
                pid
            } else {
                sqlx::query_scalar::<_, i64>(
                    "SELECT id FROM public.products WHERE lower(name) = lower($1) LIMIT 1",
                )
                .persistent(false)
                .bind(&name)
                .fetch_optional(&self.db.pool)
                .await?
                .unwrap_or_else(|| {
                    // Could not resolve; skip mapping for this legacy product (media rows may still fallback via provider-only inserts)
                    0
                })
            };
            if pg_product_id == 0 {
                continue;
            }
            // Title id (choose first matching title for product)
            let title_id = sqlx
                ::query_scalar::<_, i64>(
                    "SELECT id FROM public.video_game_titles WHERE video_game_id = $1 ORDER BY id LIMIT 1"
                )
                .persistent(false)
                .bind(pg_product_id)
                .fetch_optional(&self.db.pool).await?;
            // Platforms -> video_games
            let platform_vgs = if let Some(tid) = title_id {
                let vgs = sqlx::query_scalar::<_, i64>(
                    "SELECT id FROM public.video_games WHERE title_id = $1",
                )
                .persistent(false)
                .bind(tid)
                .fetch_all(&self.db.pool)
                .await?;
                vgs
            } else {
                Vec::new()
            };
            if !platform_vgs.is_empty() {
                self.product_platforms
                    .insert(legacy_pid, platform_vgs.clone());
            }
            let popularity_score: f64 = lp.try_get::<f64, _>("popularity_score").unwrap_or(0.0);
            let rating: f64 = lp.try_get::<f64, _>("rating").unwrap_or(0.0);
            let metadata = lp
                .try_get::<Option<String>, _>("metadata")
                .ok()
                .and_then(|opt| opt.and_then(|s| serde_json::from_str::<Value>(s.as_str()).ok()));
            let release_date = lp
                .try_get::<Option<String>, _>("release_date")
                .ok()
                .and_then(|opt| opt.and_then(|val| parse_date(val.as_str())));
            let synopsis = lp.try_get::<Option<String>, _>("synopsis").ok().flatten();
            self.product_map.insert(
                legacy_pid,
                ProductInfo {
                    pg_id: pg_product_id,
                    slug: slug.clone(),
                    title: name.clone(),
                    kind: normalize_product_kind(
                        lp.try_get::<Option<String>, _>("category")
                            .ok()
                            .flatten()
                            .as_deref(),
                    )
                    .to_string(),
                    release_date,
                    synopsis,
                    popularity_score,
                    rating,
                    metadata,
                    title_id,
                    sellable_id: None,
                },
            );
        }
        // Legacy VG ids referenced in staging rows
        // Video game legacy ids (same optimization pattern as products)
        let mut legacy_vg_ids: StdHashSet<i64> = StdHashSet::new();
        let img_vg_rows = sqlx
            ::query_scalar::<_, i64>(
                "SELECT legacy_video_game_id FROM public.legacy_image_media_raw WHERE processed=false AND legacy_video_game_id > 0"
            )
            .persistent(false)
            .fetch_all(&self.db.pool).await?;
        for id in img_vg_rows {
            legacy_vg_ids.insert(id);
        }
        let vid_vg_rows = sqlx
            ::query_scalar::<_, i64>(
                "SELECT legacy_video_game_id FROM public.legacy_video_media_raw WHERE processed=false AND legacy_video_game_id > 0"
            )
            .persistent(false)
            .fetch_all(&self.db.pool).await?;
        for id in vid_vg_rows {
            legacy_vg_ids.insert(id);
        }
        for legacy_vg_id in legacy_vg_ids.into_iter() {
            if self.legacy_vg_map.contains_key(&legacy_vg_id) {
                continue;
            }
            // Fetch legacy VG row from SQLite (title + normalized_title + product_id).
            let vg_opt = sqlx
                ::query(
                    "SELECT id, product_id, title, normalized_title, slug FROM video_games WHERE id = ?"
                )
                .persistent(false)
                .bind(legacy_vg_id)
                .fetch_optional(&self.sqlite).await?;
            let Some(vgrow) = vg_opt else {
                continue;
            };
            // Determine normalized title for matching.
            let norm = if let Ok(Some(nt)) = vgrow.try_get::<Option<String>, _>("normalized_title")
            {
                if nt.trim().is_empty() {
                    None
                } else {
                    Some(nt.trim().to_string())
                }
            } else {
                None
            };
            let title_txt = if let Ok(Some(t)) = vgrow.try_get::<Option<String>, _>("title") {
                Some(t.trim().to_string())
            } else {
                None
            };
            let norm_val = norm.clone().unwrap_or_else(|| {
                title_txt
                    .clone()
                    .map(|t| normalize_title(t.as_str()))
                    .unwrap_or_default()
            });
            // Match PG video_game via normalized title.
            let pg_vg_opt = sqlx
                ::query_scalar::<_, i64>(
                    "SELECT v.id FROM public.video_games v JOIN public.video_game_titles t ON t.id = v.title_id WHERE t.normalized_title = $1 ORDER BY v.id LIMIT 1"
                )
                .persistent(false)
                .bind(&norm_val)
                .fetch_optional(&self.db.pool).await?;
            if let Some(pg_vg_id) = pg_vg_opt {
                self.legacy_vg_map.insert(legacy_vg_id, pg_vg_id);
                continue;
            }
            // Fallback: if we can resolve product mapping, pick first VG for that product.
            if let Ok(prod_id_opt) = vgrow.try_get::<Option<i64>, _>("product_id") {
                if let Some(legacy_pid) = prod_id_opt {
                    if let Some(info) = self.product_map.get(&legacy_pid) {
                        if let Some(tid) = info.title_id {
                            let pg_vg_opt2 = sqlx
                                ::query_scalar::<_, i64>(
                                    "SELECT id FROM public.video_games WHERE title_id = $1 ORDER BY id LIMIT 1"
                                )
                                .persistent(false)
                                .bind(tid)
                                .fetch_optional(&self.db.pool).await?;
                            if let Some(pg_vg_id2) = pg_vg_opt2 {
                                self.legacy_vg_map.insert(legacy_vg_id, pg_vg_id2);
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    // ----- Deduplication stage (post-import) -----
    // Consolidate duplicate titles/sellables created during import by picking a stable keeper ID
    // and re-pointing dependents before deleting duplicates.
    async fn dedupe_products(&self) -> Result<()> {
        let pool = &self.db.pool;
        let mut tx = pool.begin().await?;

        // A) Deduplicate video_game_titles by unified key while SAFELY merging sellables first to avoid
        //    hitting the partial unique index uq_sellables_software_title. Strategy:
        //      1. Identify duplicate title groups (pid0,key)
        //      2. For each group, collect all sellables referencing ANY title id in the group.
        //         Pick a keeper sellable (MIN(id)); repoint offers to keeper; delete other sellables.
        //      3. Re-point video_games to keeper title_id.
        //      4. Update remaining sellables to keeper title_id.
        //      5. Delete duplicate titles (non-keeper ids).
        let merge_sellables_sql = r#"
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
            ), sellable_groups AS (
                SELECT g.keep_id,
                       MIN(s.id) AS keep_sellable_id,
                       ARRAY_AGG(s.id ORDER BY s.id) AS sellable_ids
                FROM groups g
                JOIN public.sellables s ON s.software_title_id = ANY(g.all_ids)
                GROUP BY g.keep_id
            ), moved_offers AS (
                UPDATE public.offers o
                SET sellable_id = sg.keep_sellable_id
                FROM sellable_groups sg
                WHERE o.sellable_id = ANY(sg.sellable_ids) AND o.sellable_id <> sg.keep_sellable_id
                RETURNING 1
            )
            DELETE FROM public.sellables s
            USING sellable_groups sg
            WHERE s.id = ANY(sg.sellable_ids) AND s.id <> sg.keep_sellable_id;
        "#;
        let _res_merge_sellables = sqlx::query(merge_sellables_sql)
            .persistent(false)
            .execute(&mut *tx)
            .await?;

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

        // Update remaining sellables to keeper title_id (after collapsing duplicates)
        let update_sellables_sql = r#"
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
            UPDATE public.sellables s
            SET software_title_id = g.keep_id
            FROM groups g
            WHERE s.software_title_id = ANY(g.all_ids) AND s.software_title_id <> g.keep_id;
        "#;
        let _res_update_sellables = sqlx::query(update_sellables_sql)
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

        // B) Deduplicate sellables for software titles: ensure one sellable per title
        let sellables_sql = r#"
            WITH dup AS (
                SELECT software_title_id,
                       MIN(id) AS keep_id,
                       ARRAY_AGG(id ORDER BY id) AS all_ids
                FROM public.sellables
                WHERE kind = 'software' AND software_title_id IS NOT NULL
                GROUP BY software_title_id
                HAVING COUNT(*) > 1
            ), moved_offers AS (
                UPDATE public.offers o
                SET sellable_id = d.keep_id
                FROM dup d
                WHERE o.sellable_id = ANY(d.all_ids) AND o.sellable_id <> d.keep_id
                RETURNING 1
            )
            DELETE FROM public.sellables s
            USING dup d
            WHERE s.software_title_id = d.software_title_id AND s.id <> d.keep_id;
        "#;
        let res_sellables = sqlx::query(sellables_sql)
            .persistent(false)
            .execute(&mut *tx)
            .await?;
        let deleted_sellables = res_sellables.rows_affected();

        tx.commit().await?;
        info!(
            deleted_titles,
            deleted_sellables, "deduplication complete (titles/sellables)"
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

        // 3. provider_media_links duplicates (same provider_item_id, video_game_id, source, url)
        let links_sql = r#"
            WITH base AS (
                SELECT id, provider_item_id, COALESCE(video_game_id,0) AS vg_id, source, COALESCE(url,'') AS url
                FROM public.provider_media_links
            ), groups AS (
                SELECT provider_item_id, vg_id, source, url,
                       MIN(id) AS keep_id,
                       ARRAY_AGG(id ORDER BY id) AS all_ids
                FROM base
                WHERE url <> '' -- drop source <> '' to avoid enum comparison issues
                GROUP BY provider_item_id, vg_id, source, url
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

    async fn ensure_title_cached(&mut self, title: &str) -> Result<i64> {
        let norm = normalize_title(title);
        if let Some(id) = self.title_id_by_norm.get(&norm) {
            return Ok(*id);
        }
        // NOTE: We allow creating NEW titles even when skip_titles=true so that
        // subsequent video_game imports are not blocked. skip_titles now only
        // freezes normalized_title update batches (see vgt_title_updates usage).
        let rec = sqlx
            ::query(
                "INSERT INTO public.video_game_titles (title, normalized_title) VALUES ($1,$2) RETURNING id"
            )
            .persistent(false)
            .bind(title)
            .bind(&norm)
            .fetch_one(&self.db.pool).await?;
        let id: i64 = rec.get("id");
        self.title_id_by_norm.insert(norm, id);
        Ok(id)
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
        // Prefill existing titles and sellables + video_game combos for batching ensures.
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
        let sellables =
            sqlx::query("SELECT id, software_title_id FROM public.sellables WHERE kind='software'")
                .persistent(false)
                .fetch_all(&self.db.pool)
                .await?;
        for r in sellables {
            let sid: i64 = r.get("id");
            let tid: i64 = r.get("software_title_id");
            self.sellable_by_title.insert(tid, sid);
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

        let mut stream = sqlx::query_as::<_, LegacyCurrency>(
            r#"SELECT id, code, name, decimals FROM currencies ORDER BY id"#,
        )
        .persistent(false)
        .fetch(&self.sqlite);

        let mut count = 0usize;
        while let Some(row) = stream.try_next().await? {
            let code = row
                .code
                .as_deref()
                .unwrap_or_default()
                .trim()
                .to_ascii_uppercase();
            if code.len() < 3 {
                warn!(legacy_id=?row.id, ?code, "skipping currency with short code");
                prog.tick(1);
                continue;
            }
            let name = row
                .name
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or(&code)
                .to_string();
            let decimals = row.decimals.unwrap_or(2);
            let pg_id = ensure_currency(self.cache.db(), &code, &name, decimals as i16).await?;
            self.currency_map.insert(row.id, pg_id);
            count += 1;
            prog.tick(1);
        }
        prog.finish();
        info!("imported {count} currencies");
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

        let mut stream = sqlx::query_as::<_, LegacyCountry>(
            r#"SELECT id, code, name, currency_id FROM countries ORDER BY id"#,
        )
        .persistent(false)
        .fetch(&self.sqlite);

        let mut count = 0usize;

        while let Some(row) = stream.try_next().await? {
            let iso2 = row.code.trim().to_ascii_uppercase();
            if iso2.len() != 2 {
                warn!(legacy_id=?row.id, iso=?row.code, "skipping country non-iso2");
                prog.tick(1);
                continue;
            }
            let Some(iso3) = iso2_to_iso3.get(&iso2).cloned() else {
                warn!(legacy_id=?row.id, ?iso2, "no iso3 mapping; skipping");
                prog.tick(1);
                continue;
            };
            let Some(currency_id) = row
                .currency_id
                .and_then(|legacy| self.currency_map.get(&legacy).copied())
            else {
                warn!(legacy_id=?row.id, "skipping country missing currency mapping");
                prog.tick(1);
                continue;
            };
            let name = row.name.trim();
            if name.is_empty() {
                warn!(legacy_id=?row.id, "skipping country with empty name");
                prog.tick(1);
                continue;
            }

            sqlx::query(
                "INSERT INTO public.countries (iso2, iso3, name, currency_id)
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT (iso2) DO UPDATE
                 SET name = EXCLUDED.name, currency_id = EXCLUDED.currency_id",
            )
            .persistent(false)
            .bind(&iso2)
            .bind(&iso3)
            .bind(name)
            .bind(currency_id)
            .execute(&self.db.pool)
            .await?;
            let pg_id: (i64,) = sqlx::query_as("SELECT id FROM public.countries WHERE iso2 = $1")
                .persistent(false)
                .bind(&iso2)
                .fetch_one(&self.db.pool)
                .await?;
            self.country_map.insert(row.id, pg_id.0);

            count += 1;
            prog.tick(1);
        }

        prog.finish();
        info!("imported {count} countries");
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

            // Do not create or mutate products here; only look up existing by slug.
            let product_id: Option<i64> = self.product_slug_to_id.get(&slug).copied();

            let mut title_id = None;
            let mut sellable_id = None;
            if kind == "software" {
                let tid: i64 = if let Some(pid) = product_id {
                    if let Some(tid) = self.title_by_product.get(&pid) {
                        *tid
                    } else {
                        let rec = sqlx
                            ::query(
                                "INSERT INTO public.video_game_titles (product_id,title)
                                 VALUES ($1,$2)
                                 ON CONFLICT (product_id) DO UPDATE SET title=EXCLUDED.title, updated_at=now()
                                 RETURNING id"
                            )
                            .persistent(false)
                            .bind(pid)
                            .bind(name)
                            .fetch_one(&self.db.pool).await?;
                        let tid: i64 = rec.get("id");
                        self.title_by_product.insert(pid, tid);
                        tid
                    }
                } else {
                    // No product available: ensure a title without video_game_id link (decoupled; cached locally)
                    let norm = normalize_title(name);
                    if let Some(id) = title_cache.get(&norm).copied() {
                        id
                    } else {
                        let rec = sqlx
                            ::query(
                                "INSERT INTO public.video_game_titles (title, normalized_title) VALUES ($1,$2) RETURNING id"
                            )
                            .persistent(false)
                            .bind(name)
                            .bind(&norm)
                            .fetch_one(&self.db.pool).await?;
                        let id: i64 = rec.get("id");
                        title_cache.insert(norm, id);
                        id
                    }
                };
                title_id = Some(tid);

                let sid = if let Some(sid) = self.sellable_by_title.get(&tid) {
                    *sid
                } else {
                    let sid = ensure_sellable_for_title(&self.db, tid).await?;
                    self.sellable_by_title.insert(tid, sid);
                    sid
                };
                sellable_id = Some(sid);
            }

            let release_date = row.release_date.as_deref().and_then(parse_date);
            let metadata = row
                .metadata
                .as_deref()
                .and_then(|raw| serde_json::from_str::<Value>(raw).ok());

            self.product_map.insert(
                row.id,
                ProductInfo {
                    pg_id: product_id.unwrap_or(0),
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
                    sellable_id,
                },
            );

            processed += 1;
            prog.tick(1);
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
        let pool = &self.db.pool;
        let chunk = update_chunk_size();
        let mut updates: Vec<VgUpd> = Vec::with_capacity(chunk);
        let mut flushed_batches = 0usize;

        // Collect normalized title updates (legacy override) for batching
        let mut vgt_title_updates: Vec<(i64, String)> = Vec::new();
        // Local cache for title ensures to avoid &mut self during active stream borrow
        let mut vgt_title_cache = self.title_id_by_norm.clone();
        let mut vg_flush_durations: Vec<std::time::Duration> = Vec::new();
        let mut skipped_existing = 0usize; // count of (title_id, platform_id) combinations already present
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
            .fetch(&self.sqlite)
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
            .fetch(&self.sqlite)
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
            let product = if let Some(p) = self.product_map.get(&row.product_id).cloned() {
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
                    title_id: None, // derive below from SQLite title
                    sellable_id: None,
                }
            };

            // Resolve/ensure a title_id even when product has no mapped title.
            let title_id: i64 = if let Some(tid) = product.title_id {
                tid
            } else {
                // Prefer SQLite row.title; fallback to synthesized product.title
                let to_title = row
                    .title
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .unwrap_or(&product.title);
                let norm = normalize_title(to_title);
                if let Some(id) = vgt_title_cache.get(&norm).copied() {
                    id
                } else {
                    let rec = sqlx
                        ::query(
                            "INSERT INTO public.video_game_titles (title, normalized_title) VALUES ($1,$2) RETURNING id"
                        )
                        .persistent(false)
                        .bind(to_title)
                        .bind(&norm)
                        .fetch_one(&self.db.pool).await?;
                    let id: i64 = rec.get("id");
                    vgt_title_cache.insert(norm, id);
                    id
                }
            };

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
                            "INSERT INTO public.video_games (title_id, platform_id) VALUES ($1,$2) ON CONFLICT DO NOTHING RETURNING id"
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
        let staging = std::env::var("MEDIA_STAGING")
            .ok()
            .map(|v| (v == "1" || v.eq_ignore_ascii_case("true")))
            .unwrap_or(false);
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

        const BATCH: usize = 500;
        // Staging batch for legacy_image_media_raw inserts
        const STAGE_BATCH: usize = 1000;
        const LINKS_BATCH: usize = 1000; // number of links buffered before flushing grouped inserts
        let mut batch: Vec<(i64, String, String, String, String, Value)> =
            Vec::with_capacity(BATCH);
        let mut total_rows = 0usize;
        // Buffer rows when MEDIA_STAGING=1 to avoid per-row INSERT cost
        #[derive(Clone)]
        struct LegacyImageRawRow {
            legacy_id: i64,
            legacy_product_id: i64,
            legacy_video_game_id: i64,
            provider_slug: Option<String>,
            kind: String,
            url: String,
            title: Option<String>,
            mime_type: Option<String>,
            width: Option<i32>,
            height: Option<i32>,
            is_primary: Option<bool>,
            attribution: Option<String>,
            external_id: Option<String>,
            legacy_metadata: Value,
        }

        async fn insert_legacy_image_media_raw_batch(
            db: &Db,
            rows: &[LegacyImageRawRow],
        ) -> Result<u64> {
            use sqlx::types::Json;
            if rows.is_empty() {
                return Ok(0);
            }
            let legacy_id: Vec<i64> = rows.iter().map(|r| r.legacy_id).collect();
            let legacy_product_id: Vec<i64> = rows.iter().map(|r| r.legacy_product_id).collect();
            let legacy_video_game_id: Vec<i64> =
                rows.iter().map(|r| r.legacy_video_game_id).collect();
            let provider_slug: Vec<Option<String>> =
                rows.iter().map(|r| r.provider_slug.clone()).collect();
            let kind: Vec<String> = rows.iter().map(|r| r.kind.clone()).collect();
            let url: Vec<String> = rows.iter().map(|r| r.url.clone()).collect();
            let title: Vec<Option<String>> = rows.iter().map(|r| r.title.clone()).collect();
            let mime_type: Vec<Option<String>> = rows.iter().map(|r| r.mime_type.clone()).collect();
            let width: Vec<Option<i32>> = rows.iter().map(|r| r.width).collect();
            let height: Vec<Option<i32>> = rows.iter().map(|r| r.height).collect();
            let is_primary: Vec<Option<bool>> = rows.iter().map(|r| r.is_primary).collect();
            let attribution: Vec<Option<String>> =
                rows.iter().map(|r| r.attribution.clone()).collect();
            let external_id: Vec<Option<String>> =
                rows.iter().map(|r| r.external_id.clone()).collect();
            let legacy_metadata: Vec<Json<Value>> = rows
                .iter()
                .map(|r| Json(r.legacy_metadata.clone()))
                .collect();

            let res = sqlx
                ::query(
                    "INSERT INTO public.legacy_image_media_raw \
                 (legacy_id, legacy_product_id, legacy_video_game_id, provider_slug, kind, url, title, mime_type, width, height, is_primary, attribution, external_id, legacy_metadata) \
                 SELECT * FROM UNNEST( \
                   $1::bigint[], $2::bigint[], $3::bigint[], $4::text[], $5::text[], $6::text[], \
                   $7::text[], $8::text[], $9::int4[], $10::int4[], $11::bool[], $12::text[], \
                   $13::text[], $14::jsonb[]) \
                 ON CONFLICT DO NOTHING"
                )
                .persistent(false)
                .bind(legacy_id)
                .bind(legacy_product_id)
                .bind(legacy_video_game_id)
                .bind(provider_slug)
                .bind(kind)
                .bind(url)
                .bind(title)
                .bind(mime_type)
                .bind(width)
                .bind(height)
                .bind(is_primary)
                .bind(attribution)
                .bind(external_id)
                .bind(legacy_metadata)
                .execute(&db.pool).await?;
            Ok(res.rows_affected())
        }
        let mut stage_buf: Vec<LegacyImageRawRow> = Vec::with_capacity(STAGE_BATCH);
        let mut vg_map_cache: HashMap<i64, Vec<i64>> = HashMap::new();
        // Group provider_media_links inserts by (provider_item_id, vg_id, src) with a shared metadata blob
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
            for ((provider_item_id, vg_id, src), grp) in groups.drain() {
                let _ = ensure_provider_media_links_with_meta(
                    db,
                    provider_item_id,
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
            if target_vgs.is_empty() && !staging {
                // No way to link strictly (and not staging), skip row entirely
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
                    .unwrap_or_else(|_| "screenshot".into())
            } else {
                "screenshot".into()
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
            if staging {
                // Stage raw row (one per legacy row, do not expand across target_vgs here)
                let provider_slug_stage = source.clone().and_then(|s| {
                    let trimmed = s.trim();
                    if trimmed.is_empty() {
                        None
                    } else {
                        Some(trimmed.to_lowercase())
                    }
                });
                stage_buf.push(LegacyImageRawRow {
                    legacy_id: row.try_get::<i64, _>("id").unwrap_or(0),
                    legacy_product_id: row.try_get::<i64, _>("product_id").unwrap_or(0),
                    legacy_video_game_id: row.try_get::<i64, _>("video_game_id").unwrap_or(0),
                    provider_slug: provider_slug_stage,
                    kind: media_type.clone(),
                    url: url.clone(),
                    title: title.clone(),
                    mime_type: mime_type.clone(),
                    width: width.map(|w| w as i32),
                    height: height.map(|h| h as i32),
                    is_primary: is_primary.map(|p| p == 1),
                    attribution: attribution.clone(),
                    external_id: external_id.clone(),
                    legacy_metadata: provider_data.clone(),
                });
                if stage_buf.len() >= STAGE_BATCH {
                    let _ =
                        insert_legacy_image_media_raw_batch(self.cache.db(), &stage_buf).await?;
                    stage_buf.clear();
                }
                total_rows += 1;
                prog.tick(1);
                continue;
            }
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
            let ext_id = external_id.unwrap_or_else(|| url.clone());
            // Resolve provider id via cached slug lookup (create if missing)
            let provider_id: i64 = self.provider_id_for_slug(&src).await?; // avoid &String clone
            let pi_external = if ext_id.is_empty() {
                format!("media:{}", url)
            } else {
                format!("media:{}", ext_id)
            };
            let provider_item_id = self
                .cache
                .ensure_provider_item(provider_id, &pi_external, None, false)
                .await?;
            for vg_id in target_vgs.iter().copied() {
                batch.push((
                    vg_id,
                    src.clone(),
                    ext_id.clone(),
                    media_type.clone(),
                    url.clone(),
                    provider_data.clone(),
                ));
                // Queue provider_media_links for grouped flush
                let key = (provider_item_id, vg_id, src.clone());
                let entry = link_groups.entry(key).or_insert_with(|| LinkGroup {
                    urls: Vec::new(),
                    meta: provider_data.clone(),
                });
                entry
                    .urls
                    .push((url.clone(), Some(media_type.clone()), None, title.clone()));
                pending_links += 1;
                if batch.len() >= BATCH {
                    // Convert to borrowed tuples for batch upsert
                    let borrowed: Vec<(i64, &str, &str, &str, &str, &Value)> = batch
                        .iter()
                        .map(|(a, b, c, d, e, f)| {
                            (*a, b.as_str(), c.as_str(), d.as_str(), e.as_str(), f)
                        })
                        .collect();
                    let _ = upsert_game_media_batch(self.cache.db(), &borrowed).await?;
                    batch.clear();
                }
                if pending_links >= LINKS_BATCH {
                    flush_link_groups(self.cache.db(), &mut link_groups).await?;
                    pending_links = 0;
                }
            }
            total_rows += 1;
            prog.tick(1);
        }
        // Flush any remaining staged rows
        if staging && !stage_buf.is_empty() {
            let _ = insert_legacy_image_media_raw_batch(self.cache.db(), &stage_buf).await?;
            stage_buf.clear();
        }
        if !batch.is_empty() {
            let borrowed: Vec<(i64, &str, &str, &str, &str, &Value)> = batch
                .iter()
                .map(|(a, b, c, d, e, f)| (*a, b.as_str(), c.as_str(), d.as_str(), e.as_str(), f))
                .collect();
            let _ = upsert_game_media_batch(self.cache.db(), &borrowed).await?;
            batch.clear();
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
        let staging = std::env::var("MEDIA_STAGING")
            .ok()
            .map(|v| (v == "1" || v.eq_ignore_ascii_case("true")))
            .unwrap_or(false);
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

        const BATCH: usize = 250;
        const LINKS_BATCH: usize = 800;
        // For staging image thumbnails derived during video import
        const STAGE_IMG_BATCH: usize = 1000;
        let mut batch: Vec<(i64, String, String, String, String, Value)> =
            Vec::with_capacity(BATCH);
        let mut total_rows = 0usize;
        // Buffer for legacy_image_media_raw rows when staging is enabled in this video importer
        #[derive(Clone)]
        struct LegacyImageRawRowV {
            legacy_id: i64,
            legacy_product_id: i64,
            legacy_video_game_id: i64,
            provider_slug: Option<String>,
            kind: String,
            url: String,
            title: Option<String>,
            mime_type: Option<String>,
            width: Option<i32>,
            height: Option<i32>,
            is_primary: Option<bool>,
            attribution: Option<String>,
            external_id: Option<String>,
            legacy_metadata: Value,
        }
        async fn insert_legacy_image_media_raw_batch_v(
            db: &Db,
            rows: &[LegacyImageRawRowV],
        ) -> Result<u64> {
            use sqlx::types::Json;
            if rows.is_empty() {
                return Ok(0);
            }
            let legacy_id: Vec<i64> = rows.iter().map(|r| r.legacy_id).collect();
            let legacy_product_id: Vec<i64> = rows.iter().map(|r| r.legacy_product_id).collect();
            let legacy_video_game_id: Vec<i64> =
                rows.iter().map(|r| r.legacy_video_game_id).collect();
            let provider_slug: Vec<Option<String>> =
                rows.iter().map(|r| r.provider_slug.clone()).collect();
            let kind: Vec<String> = rows.iter().map(|r| r.kind.clone()).collect();
            let url: Vec<String> = rows.iter().map(|r| r.url.clone()).collect();
            let title: Vec<Option<String>> = rows.iter().map(|r| r.title.clone()).collect();
            let mime_type: Vec<Option<String>> = rows.iter().map(|r| r.mime_type.clone()).collect();
            let width: Vec<Option<i32>> = rows.iter().map(|r| r.width).collect();
            let height: Vec<Option<i32>> = rows.iter().map(|r| r.height).collect();
            let is_primary: Vec<Option<bool>> = rows.iter().map(|r| r.is_primary).collect();
            let attribution: Vec<Option<String>> =
                rows.iter().map(|r| r.attribution.clone()).collect();
            let external_id: Vec<Option<String>> =
                rows.iter().map(|r| r.external_id.clone()).collect();
            let legacy_metadata: Vec<Json<Value>> = rows
                .iter()
                .map(|r| Json(r.legacy_metadata.clone()))
                .collect();

            let res = sqlx
                ::query(
                    "INSERT INTO public.legacy_image_media_raw \
                 (legacy_id, legacy_product_id, legacy_video_game_id, provider_slug, kind, url, title, mime_type, width, height, is_primary, attribution, external_id, legacy_metadata) \
                 SELECT * FROM UNNEST( \
                   $1::bigint[], $2::bigint[], $3::bigint[], $4::text[], $5::text[], $6::text[], \
                   $7::text[], $8::text[], $9::int4[], $10::int4[], $11::bool[], $12::text[], \
                   $13::text[], $14::jsonb[]) \
                 ON CONFLICT DO NOTHING"
                )
                .persistent(false)
                .bind(legacy_id)
                .bind(legacy_product_id)
                .bind(legacy_video_game_id)
                .bind(provider_slug)
                .bind(kind)
                .bind(url)
                .bind(title)
                .bind(mime_type)
                .bind(width)
                .bind(height)
                .bind(is_primary)
                .bind(attribution)
                .bind(external_id)
                .bind(legacy_metadata)
                .execute(&db.pool).await?;
            Ok(res.rows_affected())
        }
        let mut stage_buf_img_v: Vec<LegacyImageRawRowV> = Vec::with_capacity(STAGE_IMG_BATCH);
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
            for ((provider_item_id, vg_id, src), grp) in groups.drain() {
                let _ = ensure_provider_media_links_with_meta(
                    db,
                    provider_item_id,
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
            let mut target_vgs: Vec<i64> = Vec::new();
            if cols.contains("video_game_id") {
                if let Ok(vg_id) = row.try_get::<i64, _>("video_game_id") {
                    if vg_id > 0 {
                        target_vgs.push(vg_id);
                    }
                }
            }
            if target_vgs.is_empty() {
                let legacy_pid = if cols.contains("product_id") {
                    row.try_get::<i64, _>("product_id").unwrap_or(0)
                } else {
                    0
                };
                if legacy_pid > 0 {
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
            if target_vgs.is_empty() && !staging {
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
                let mut picked_from_meta = false;
                if !(candidate.starts_with("http://") || candidate.starts_with("https://")) {
                    if let Some(mr) = meta_raw.as_ref() {
                        if let Ok(val) = serde_json::from_str::<Value>(mr) {
                            if let Some(u) = val.get("url").and_then(|v| v.as_str()) {
                                candidate = u.to_string();
                                picked_from_meta = true;
                            } else if let Some(u) = val.get("video_url").and_then(|v| v.as_str()) {
                                candidate = u.to_string();
                                picked_from_meta = true;
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
            if staging {
                // Stage raw video row
                let provider_slug_stage = if src.is_empty() {
                    None
                } else {
                    Some(src.clone())
                };
                sqlx
                    ::query(
                        "INSERT INTO public.legacy_video_media_raw (legacy_id, legacy_product_id, legacy_video_game_id, provider_slug, kind, url, title, mime_type, duration_seconds, external_id, legacy_metadata) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT DO NOTHING"
                    )
                    .persistent(false)
                    .bind(row.try_get::<i64, _>("id").unwrap_or(0))
                    .bind(row.try_get::<i64, _>("product_id").unwrap_or(0))
                    .bind(row.try_get::<i64, _>("video_game_id").unwrap_or(0))
                    .bind(provider_slug_stage)
                    .bind(kind.clone())
                    .bind(&url)
                    .bind(title.clone())
                    .bind(mime_type.clone())
                    .bind(duration.map(|d| d as i32))
                    .bind(external_id.clone())
                    .bind(provider_data.clone())
                    .execute(&self.db.pool).await?;
                total_rows += 1;
                prog.tick(1);
                continue;
            }
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
            let provider_item_id = self
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
                batch.push((
                    vg_id,
                    src.clone(),
                    ext_id.clone(),
                    media_type.to_string(),
                    url.clone(),
                    provider_data.clone(),
                ));
                let key = (provider_item_id, vg_id, src.clone());
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
                if batch.len() >= BATCH {
                    let borrowed: Vec<(i64, &str, &str, &str, &str, &Value)> = batch
                        .iter()
                        .map(|(a, b, c, d, e, f)| {
                            (*a, b.as_str(), c.as_str(), d.as_str(), e.as_str(), f)
                        })
                        .collect();
                    let _ = upsert_game_media_batch(self.cache.db(), &borrowed).await?;
                    batch.clear();
                }
                if pending_links >= LINKS_BATCH {
                    flush_link_groups(self.cache.db(), &mut link_groups).await?;
                    pending_links = 0;
                }
            }
            total_rows += 1;
            prog.tick(1);
        }
        if !batch.is_empty() {
            let borrowed: Vec<(i64, &str, &str, &str, &str, &Value)> = batch
                .iter()
                .map(|(a, b, c, d, e, f)| (*a, b.as_str(), c.as_str(), d.as_str(), e.as_str(), f))
                .collect();
            let _ = upsert_game_media_batch(self.cache.db(), &borrowed).await?;
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

        if self.legacy_vg_map.is_empty() || self.product_map.is_empty() {
            if let Err(e) = self.rebuild_fallback_media_mappings().await {
                warn!(error=?e, "gb_images fallback mapping rebuild failed");
            }
        }
        info!(
            mode = if direct_specialized {
                "specialized"
            } else {
                "generic"
            },
            "importing giant_bomb_game_images"
        );

        let staging = std::env::var("MEDIA_STAGING")
            .ok()
            .map(|v| (v == "1" || v.eq_ignore_ascii_case("true")))
            .unwrap_or(false);
        // When staging is enabled, buffer rows and use UNNEST batch insert
        const STAGE_BATCH_GB: usize = 1000;
        #[derive(Clone)]
        struct LegacyImageRawRowGB {
            legacy_id: i64,
            legacy_product_id: i64,
            legacy_video_game_id: i64,
            provider_slug: Option<String>,
            kind: String,
            url: String,
            title: Option<String>,
            mime_type: Option<String>,
            width: Option<i32>,
            height: Option<i32>,
            is_primary: Option<bool>,
            attribution: Option<String>,
            external_id: Option<String>,
            legacy_metadata: Value,
        }
        async fn insert_legacy_image_media_raw_batch_gb(
            db: &Db,
            rows: &[LegacyImageRawRowGB],
        ) -> Result<u64> {
            use sqlx::types::Json;
            if rows.is_empty() {
                return Ok(0);
            }
            let legacy_id: Vec<i64> = rows.iter().map(|r| r.legacy_id).collect();
            let legacy_product_id: Vec<i64> = rows.iter().map(|r| r.legacy_product_id).collect();
            let legacy_video_game_id: Vec<i64> =
                rows.iter().map(|r| r.legacy_video_game_id).collect();
            let provider_slug: Vec<Option<String>> =
                rows.iter().map(|r| r.provider_slug.clone()).collect();
            let kind: Vec<String> = rows.iter().map(|r| r.kind.clone()).collect();
            let url: Vec<String> = rows.iter().map(|r| r.url.clone()).collect();
            let title: Vec<Option<String>> = rows.iter().map(|r| r.title.clone()).collect();
            let mime_type: Vec<Option<String>> = rows.iter().map(|r| r.mime_type.clone()).collect();
            let width: Vec<Option<i32>> = rows.iter().map(|r| r.width).collect();
            let height: Vec<Option<i32>> = rows.iter().map(|r| r.height).collect();
            let is_primary: Vec<Option<bool>> = rows.iter().map(|r| r.is_primary).collect();
            let attribution: Vec<Option<String>> =
                rows.iter().map(|r| r.attribution.clone()).collect();
            let external_id: Vec<Option<String>> =
                rows.iter().map(|r| r.external_id.clone()).collect();
            let legacy_metadata: Vec<Json<Value>> = rows
                .iter()
                .map(|r| Json(r.legacy_metadata.clone()))
                .collect();

            let res = sqlx
                ::query(
                    "INSERT INTO public.legacy_image_media_raw \
                 (legacy_id, legacy_product_id, legacy_video_game_id, provider_slug, kind, url, title, mime_type, width, height, is_primary, attribution, external_id, legacy_metadata) \
                 SELECT * FROM UNNEST( \
                   $1::bigint[], $2::bigint[], $3::bigint[], $4::text[], $5::text[], $6::text[], \
                   $7::text[], $8::text[], $9::int4[], $10::int4[], $11::bool[], $12::text[], \
                   $13::text[], $14::jsonb[]) \
                 ON CONFLICT DO NOTHING"
                )
                .persistent(false)
                .bind(legacy_id)
                .bind(legacy_product_id)
                .bind(legacy_video_game_id)
                .bind(provider_slug)
                .bind(kind)
                .bind(url)
                .bind(title)
                .bind(mime_type)
                .bind(width)
                .bind(height)
                .bind(is_primary)
                .bind(attribution)
                .bind(external_id)
                .bind(legacy_metadata)
                .execute(&db.pool).await?;
            Ok(res.rows_affected())
        }
        let mut stage_buf_gb: Vec<LegacyImageRawRowGB> = Vec::with_capacity(STAGE_BATCH_GB);
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
                            vg.product_id AS product_id,
                            vg.normalized_title AS normalized_title,
                            vg.title AS vg_title
                        FROM giant_bomb_game_images gbi
                        LEFT JOIN giant_bomb_games gbg ON gbg.id = gbi.giant_bomb_game_id
                        LEFT JOIN video_games vg ON vg.id = gbg.video_game_id
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

        const BATCH: usize = 400;
        const LINKS_BATCH: usize = 800;
        let mut batch: Vec<(i64, String, String, String, String, Value)> =
            Vec::with_capacity(BATCH);
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
                let _ = ensure_provider_media_links_with_meta(
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
                        ensure_title_without_product(
                            self.cache.db(),
                            title_txt.as_deref().unwrap_or(nt),
                        )
                        .await?
                    };
                    let unknown =
                        ensure_platform(self.cache.db(), "unknown", Some("unknown")).await?;
                    let vg_id = self
                        .cache
                        .ensure_video_game(title_id, unknown, None)
                        .await?;
                    target_vgs.push(vg_id);
                }
                if target_vgs.is_empty() && !staging {
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
            pdata.insert("kind".into(), Value::String("screenshot".into()));
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

            if staging {
                // Buffer and batch insert into legacy_image_media_raw
                let provider_slug_stage = source.clone().and_then(|s| {
                    let t = s.trim();
                    if t.is_empty() {
                        None
                    } else {
                        Some(t.to_lowercase())
                    }
                });
                stage_buf_gb.push(LegacyImageRawRowGB {
                    legacy_id: row.try_get::<i64, _>("id").unwrap_or(0),
                    legacy_product_id: row.try_get::<i64, _>("product_id").unwrap_or(0),
                    legacy_video_game_id: row.try_get::<i64, _>("video_game_id").unwrap_or(0),
                    provider_slug: provider_slug_stage,
                    kind: "screenshot".to_string(),
                    url: url.clone(),
                    title: title.clone(),
                    mime_type: None,
                    width: None,
                    height: None,
                    is_primary: None,
                    attribution: None,
                    external_id: None,
                    legacy_metadata: provider_data.clone(),
                });
                if stage_buf_gb.len() >= STAGE_BATCH_GB {
                    let _ = insert_legacy_image_media_raw_batch_gb(self.cache.db(), &stage_buf_gb)
                        .await?;
                    stage_buf_gb.clear();
                }
                total_rows += 1;
                continue;
            }

            // Provider item linkage
            let ext_id = url.clone();
            let provider_id = self.provider_id_for_slug(&src).await?;
            let pi_external = format!("media:{}", ext_id);
            let provider_item_id = self
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
                            "INSERT INTO public.game_images (video_game_id, provider_item_id, kind, mime_type, width, height, url, original_url, thumbnail_url, small_url, super_url, attribution, metadata, source, title, caption, is_primary, platforms) SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18 WHERE NOT EXISTS (SELECT 1 FROM public.game_images WHERE video_game_id=$1 AND url=$7)"
                        )
                        .persistent(false)
                        .bind(vg)
                        .bind(provider_item_id)
                        .bind(detected_kind.clone().unwrap_or_else(|| "screenshot".into()))
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
                    let key = (provider_item_id, vg, src.clone());
                    link_groups
                        .entry(key)
                        .or_insert_with(|| LinkGroup {
                            urls: Vec::new(),
                            meta: provider_data.clone(),
                        })
                        .urls
                        .push((url.clone(), Some("screenshot".into()), None, title.clone()));
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
                    batch.push((
                        vg,
                        src.clone(),
                        url.clone(),
                        "screenshot".into(),
                        url.clone(),
                        provider_data.clone(),
                    ));
                    let key = (provider_item_id, vg, src.clone());
                    link_groups
                        .entry(key)
                        .or_insert_with(|| LinkGroup {
                            urls: Vec::new(),
                            meta: provider_data.clone(),
                        })
                        .urls
                        .push((url.clone(), Some("screenshot".into()), None, title.clone()));
                    pending_links += 1;
                }
                if batch.len() >= BATCH {
                    let borrowed: Vec<(i64, &str, &str, &str, &str, &Value)> = batch
                        .iter()
                        .map(|(a, b, c, d, e, f)| {
                            (*a, b.as_str(), c.as_str(), d.as_str(), e.as_str(), f)
                        })
                        .collect();
                    let _ = upsert_game_media_batch(self.cache.db(), &borrowed).await?;
                    batch.clear();
                    // Opportunistic checkpoint flush
                    let _ = self.save_checkpoint("gb_images_id", last_id_seen).await;
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

        // Flush any remaining staged rows (gb images)
        if staging && !stage_buf_gb.is_empty() {
            let _ = insert_legacy_image_media_raw_batch_gb(self.cache.db(), &stage_buf_gb).await?;
        }
        if !batch.is_empty() {
            let borrowed: Vec<(i64, &str, &str, &str, &str, &Value)> = batch
                .iter()
                .map(|(a, b, c, d, e, f)| (*a, b.as_str(), c.as_str(), d.as_str(), e.as_str(), f))
                .collect();
            let _ = upsert_game_media_batch(self.cache.db(), &borrowed).await?;
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
        // For stability keep videos in generic game_media path for now
        let staging = std::env::var("MEDIA_STAGING")
            .ok()
            .map(|v| (v == "1" || v.eq_ignore_ascii_case("true")))
            .unwrap_or(false);
        // When staging is enabled, buffer rows and use UNNEST batch insert into legacy_video_media_raw
        const STAGE_BATCH_GB_VID: usize = 1000;
        #[derive(Clone)]
        struct LegacyVideoRawRowGB {
            legacy_id: i64,
            legacy_product_id: i64,
            legacy_video_game_id: i64,
            provider_slug: Option<String>,
            kind: String,
            url: String,
            title: Option<String>,
            mime_type: Option<String>,
            duration_seconds: Option<i32>,
            external_id: Option<String>,
            legacy_metadata: Value,
        }
        async fn insert_legacy_video_media_raw_batch_gb(
            db: &Db,
            rows: &[LegacyVideoRawRowGB],
        ) -> Result<u64> {
            use sqlx::types::Json;
            if rows.is_empty() {
                return Ok(0);
            }
            let legacy_id: Vec<i64> = rows.iter().map(|r| r.legacy_id).collect();
            let legacy_product_id: Vec<i64> = rows.iter().map(|r| r.legacy_product_id).collect();
            let legacy_video_game_id: Vec<i64> =
                rows.iter().map(|r| r.legacy_video_game_id).collect();
            let provider_slug: Vec<Option<String>> =
                rows.iter().map(|r| r.provider_slug.clone()).collect();
            let kind: Vec<String> = rows.iter().map(|r| r.kind.clone()).collect();
            let url: Vec<String> = rows.iter().map(|r| r.url.clone()).collect();
            let title: Vec<Option<String>> = rows.iter().map(|r| r.title.clone()).collect();
            let mime_type: Vec<Option<String>> = rows.iter().map(|r| r.mime_type.clone()).collect();
            let duration_seconds: Vec<Option<i32>> =
                rows.iter().map(|r| r.duration_seconds).collect();
            let external_id: Vec<Option<String>> =
                rows.iter().map(|r| r.external_id.clone()).collect();
            let legacy_metadata: Vec<Json<Value>> = rows
                .iter()
                .map(|r| Json(r.legacy_metadata.clone()))
                .collect();

            let res = sqlx
                ::query(
                    "INSERT INTO public.legacy_video_media_raw \
                 (legacy_id, legacy_product_id, legacy_video_game_id, provider_slug, kind, url, title, mime_type, duration_seconds, external_id, legacy_metadata) \
                 SELECT * FROM UNNEST( \
                   $1::bigint[], $2::bigint[], $3::bigint[], $4::text[], $5::text[], $6::text[], \
                   $7::text[], $8::text[], $9::int4[], $10::text[], $11::jsonb[]) \
                 ON CONFLICT DO NOTHING"
                )
                .persistent(false)
                .bind(legacy_id)
                .bind(legacy_product_id)
                .bind(legacy_video_game_id)
                .bind(provider_slug)
                .bind(kind)
                .bind(url)
                .bind(title)
                .bind(mime_type)
                .bind(duration_seconds)
                .bind(external_id)
                .bind(legacy_metadata)
                .execute(&db.pool).await?;
            Ok(res.rows_affected())
        }
        let mut stage_buf_gb_vid: Vec<LegacyVideoRawRowGB> = Vec::with_capacity(STAGE_BATCH_GB_VID);
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
              vg.product_id AS product_id,
              vg.normalized_title AS normalized_title,
              vg.title AS vg_title
            FROM giant_bomb_game_videos gbv
            LEFT JOIN giant_bomb_games gbg ON gbg.id = gbv.giant_bomb_game_id
            LEFT JOIN video_games vg ON vg.id = gbg.video_game_id
            ORDER BY gbv.id
        "#;
        let mut stream = sqlx::query(sql).persistent(false).fetch(&self.sqlite);
        const BATCH: usize = 300;
        const LINKS_BATCH: usize = 600;
        let mut batch: Vec<(i64, String, String, String, String, Value)> =
            Vec::with_capacity(BATCH);
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
                let _ = ensure_provider_media_links_with_meta(
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
                        ensure_title_without_product(
                            self.cache.db(),
                            title_txt.as_deref().unwrap_or(nt),
                        )
                        .await?
                    };
                    let unknown =
                        ensure_platform(self.cache.db(), "unknown", Some("unknown")).await?;
                    let vg_id = self
                        .cache
                        .ensure_video_game(title_id, unknown, None)
                        .await?;
                    target_vgs.push(vg_id);
                }
                if target_vgs.is_empty() && !staging {
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

            let mut pdata = serde_json::Map::new();
            pdata.insert("source".into(), Value::String(src.clone()));
            pdata.insert("kind".into(), Value::String("trailer".into()));
            pdata.insert("url".into(), Value::String(url.clone()));
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

            if staging {
                // Buffer and batch insert
                let provider_slug_stage = source.clone().and_then(|s| {
                    let t = s.trim();
                    if t.is_empty() {
                        None
                    } else {
                        Some(t.to_lowercase())
                    }
                });
                stage_buf_gb_vid.push(LegacyVideoRawRowGB {
                    legacy_id: row.try_get::<i64, _>("id").unwrap_or(0),
                    legacy_product_id: row.try_get::<i64, _>("product_id").unwrap_or(0),
                    legacy_video_game_id: row.try_get::<i64, _>("video_game_id").unwrap_or(0),
                    provider_slug: provider_slug_stage,
                    kind: "trailer".to_string(),
                    url: url.clone(),
                    title: title.clone(),
                    mime_type: None,
                    duration_seconds: duration.map(|d| d as i32),
                    external_id: external_id.clone(),
                    legacy_metadata: provider_data.clone(),
                });
                if stage_buf_gb_vid.len() >= STAGE_BATCH_GB_VID {
                    let _ =
                        insert_legacy_video_media_raw_batch_gb(self.cache.db(), &stage_buf_gb_vid)
                            .await?;
                    stage_buf_gb_vid.clear();
                }
                total_rows += 1;
                continue;
            }

            let ext_id = external_id.clone().unwrap_or_else(|| url.clone());
            let provider_id = self.provider_id_for_slug(&src).await?;
            let pi_external = format!("media:{}", ext_id);
            let provider_item_id = self
                .cache
                .ensure_provider_item(provider_id, &pi_external, None, false)
                .await?;

            for vg in target_vgs.iter().copied() {
                batch.push((
                    vg,
                    src.clone(),
                    ext_id.clone(),
                    "trailer".into(),
                    url.clone(),
                    provider_data.clone(),
                ));
                let key = (provider_item_id, vg, src.clone());
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
            if batch.len() >= BATCH {
                let borrowed: Vec<(i64, &str, &str, &str, &str, &Value)> = batch
                    .iter()
                    .map(|(a, b, c, d, e, f)| {
                        (*a, b.as_str(), c.as_str(), d.as_str(), e.as_str(), f)
                    })
                    .collect();
                let _ = upsert_game_media_batch(self.cache.db(), &borrowed).await?;
                batch.clear();
            }
            if pending_links >= LINKS_BATCH {
                flush_groups(self.cache.db(), &mut link_groups).await?;
                pending_links = 0;
            }

            total_rows += 1;
            prog.tick(1);
        }

        // Flush any remaining staged rows (gb videos)
        if staging && !stage_buf_gb_vid.is_empty() {
            let _ =
                insert_legacy_video_media_raw_batch_gb(self.cache.db(), &stage_buf_gb_vid).await?;
        }
        if !batch.is_empty() {
            let borrowed: Vec<(i64, &str, &str, &str, &str, &Value)> = batch
                .iter()
                .map(|(a, b, c, d, e, f)| (*a, b.as_str(), c.as_str(), d.as_str(), e.as_str(), f))
                .collect();
            let _ = upsert_game_media_batch(self.cache.db(), &borrowed).await?;
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
    sqlx::query("INSERT INTO public.hardware (product_id) VALUES ($1) ON CONFLICT DO NOTHING")
        .persistent(false)
        .bind(product_id)
        .execute(&db.pool)
        .await?;
    Ok(())
}

// Create or find a title without relying on products. Matches by normalized_title or lower(title).
async fn ensure_title_without_product(db: &Db, title: &str) -> Result<i64> {
    let norm = normalize_title(title);
    if
        let Some(row) = sqlx
            ::query(
                "SELECT id FROM public.video_game_titles WHERE normalized_title=$1 OR lower(title)=$1 ORDER BY id LIMIT 1"
            )
            .persistent(false)
            .bind(&norm)
            .fetch_optional(&db.pool).await?
    {
        return Ok(row.get::<i64, _>("id"));
    }
    let rec = sqlx
        ::query(
            "INSERT INTO public.video_game_titles (title, normalized_title) VALUES ($1,$2) RETURNING id"
        )
        .persistent(false)
        .bind(title)
        .bind(&norm)
        .fetch_one(&db.pool).await?;
    Ok(rec.get::<i64, _>("id"))
}

async fn load_iso2_to_iso3(pool: &sqlx::Pool<sqlx::Postgres>) -> Result<HashMap<String, String>> {
    let rows = sqlx::query("SELECT iso2, iso3 FROM public.country_iso_map")
        .persistent(false)
        .fetch_all(pool)
        .await?;
    Ok(rows
        .into_iter()
        .map(|r| {
            let iso2: String = r.get("iso2");
            let iso3: String = r.get("iso3");
            (iso2.to_ascii_uppercase(), iso3.to_ascii_uppercase())
        })
        .collect())
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
    if parts.is_empty() { None } else { Some(parts) }
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

// Note: No fallback mapping; media sources must match existing providers.slug.

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
        ("playstation.net", "psn"),
        ("sonyentertainmentnetwork.com", "psn"),
        ("pscdn.co", "psn"),
        ("image.api.playstation.com", "psn"),
        ("media.playstation.com", "psn"),
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
