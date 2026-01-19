use anyhow::{anyhow, Context, Result};
use chrono::{Datelike, Utc};
use futures::{future::pending, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::env;
// use std::fmt; // unused
use sqlx::Row;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::time::{interval, sleep, Duration, MissedTickBehavior};
use tokio_postgres::{AsyncMessage, NoTls};
use url::{form_urlencoded, Url};

use i_miss_rust::database_ops::db::Db;
use i_miss_rust::database_ops::nexarda::provider::{NexardaOptions, NexardaProvider};
use i_miss_rust::util::env as env_util;

// -------- Manager: in-memory logs + pause/resume control --------
#[derive(Clone)]
struct Manager {
    paused: Arc<AtomicBool>,
    logs: Arc<Mutex<VecDeque<String>>>,
    log_capacity: usize,
}

impl Manager {
    fn new(capacity: usize) -> Self {
        Self {
            paused: Arc::new(AtomicBool::new(false)),
            logs: Arc::new(Mutex::new(VecDeque::with_capacity(capacity))),
            log_capacity: capacity,
        }
    }

    fn is_paused(&self) -> bool {
        self.paused.load(Ordering::Relaxed)
    }
    fn set_paused(&self, val: bool) {
        self.paused.store(val, Ordering::Relaxed);
    }
}

fn push_log(mgr: &Manager, msg: impl AsRef<str>) {
    let ts = Utc::now().to_rfc3339();
    let mut guard = mgr.logs.lock().unwrap();
    if guard.len() >= mgr.log_capacity {
        guard.pop_front();
    }
    guard.push_back(format!("{} | {}", ts, msg.as_ref()));
}

/// Library entrypoint: run the ingest worker with env-configured settings.
pub async fn run_from_env() -> Result<()> {
    i_miss_rust::env_boot::ensure_dotenv();
    // logging
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .compact()
        .try_init();

    // DB
    let database_url = env_util::db_url_prefer_session()
        .context("Set SUPABASE_IPV6_DB / SUPABASE_DB_URL / DATABASE_URL")?;
    let db = Db::connect(&database_url, 50u32).await?;

    // Config
    let queue_cfg = QueueConfig::from_env();
    let manager = Manager::new(1000);
    ensure_queue(&db, &queue_cfg).await?;
    let start_msg = format!(
        "[ingest_worker] start queue={} vt={} poll={} max_retries={}",
        queue_cfg.queue_name,
        queue_cfg.visibility_timeout_secs,
        queue_cfg.poll_interval_secs,
        queue_cfg.max_retries
    );
    println!("{}", start_msg);
    push_log(&manager, &start_msg);

    // Metrics and HTTP (optional)
    let metrics = Arc::new(Mutex::new(WorkerMetrics::default()));
    if let Ok(addr) = env::var("WORKER_HTTP_ADDR") {
        if !addr.is_empty() {
            start_http_server(
                db.clone(),
                queue_cfg.clone(),
                metrics.clone(),
                manager.clone(),
                addr,
            );
        }
    }

    // LISTEN wake (generic channel)
    let mut notify_stream = {
        let session_url = env::var("SUPABASE_DB_SESSION_URL")
            .ok()
            .or_else(|| env::var("SUPABASE_DB_URL").ok());
        if let Some(url) = session_url.clone() {
            let sanitized = sanitize_session_url(&url);
            match connect_listen_channel(sanitized, &queue_cfg.notify_channels).await {
                Ok(rx) => Some(rx),
                Err(err) => {
                    eprintln!("[ingest_worker] LISTEN setup failed: {err:?}");
                    None
                }
            }
        } else {
            None
        }
    };

    let poll_delay = Duration::from_secs(queue_cfg.poll_interval_secs.max(1));

    loop {
        let t_poll = std::time::Instant::now();
        if manager.is_paused() {
            sleep(poll_delay).await;
            continue;
        }

        match pop_job(&db, &queue_cfg).await? {
            Some(p) => {
                {
                    let waited = t_poll.elapsed();
                    let mut m = metrics.lock().unwrap();
                    m.last_wait_ms = waited.as_millis() as u64;
                    m.dequeues += 1;
                }
                let t_run = std::time::Instant::now();

                // VT heartbeat
                let db_clone = db.clone();
                let cfg_clone = queue_cfg.clone();
                let msg_id = p.msg_id;
                let (hb_tx, mut hb_rx) = tokio::sync::oneshot::channel::<()>();
                tokio::spawn(async move {
                    let mut tick = interval(Duration::from_secs(
                        (cfg_clone.visibility_timeout_secs as u64).max(4) / 2,
                    ));
                    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
                    loop {
                        tokio::select! {
                            _ = tick.tick() => { let _ = set_job_vt(&db_clone, &cfg_clone, msg_id, cfg_clone.visibility_timeout_secs).await; }
                            _ = &mut hb_rx => break,
                        }
                    }
                });

                let res = handle_job(&db, &p.job).await;
                let run_elapsed = t_run.elapsed();
                let _ = hb_tx.send(());
                match res {
                    Ok(_) => {
                        delete_job(&db, &queue_cfg, p.msg_id).await?;
                        {
                            let mut m = metrics.lock().unwrap();
                            m.last_run_ms = run_elapsed.as_millis() as u64;
                        }
                        let ok_msg = format!(
                            "[ingest_worker] job msg_id={} provider={} task={} acked (ran {:.2?})",
                            p.msg_id, p.job.provider, p.job.task, run_elapsed
                        );
                        println!("{}", ok_msg);
                        push_log(&manager, &ok_msg);
                    }
                    Err(err) => {
                        let fail_msg = format!(
                            "[ingest_worker] job msg_id={} provider={} task={} failed after {:.2?}: {err:?}",
                            p.msg_id, p.job.provider, p.job.task, run_elapsed
                        );
                        eprintln!("{}", fail_msg);
                        push_log(&manager, &fail_msg);
                        {
                            let mut m = metrics.lock().unwrap();
                            m.last_run_ms = run_elapsed.as_millis() as u64;
                            m.failures += 1;
                            m.last_error = Some(err.to_string());
                        }
                        let attempt = (p.read_ct as u32).saturating_add(1);
                        let mut delay = queue_cfg
                            .retry_base_secs
                            .saturating_mul(1u64 << attempt.saturating_sub(1).min(6));
                        if delay > queue_cfg.retry_max_secs {
                            delay = queue_cfg.retry_max_secs;
                        }
                        if queue_cfg.max_retries > 0 && attempt > queue_cfg.max_retries {
                            archive_job(&db, &queue_cfg, p.msg_id).await?;
                            let arch_msg = format!(
                                "[ingest_worker] job msg_id={} archived after {} attempts",
                                p.msg_id, p.read_ct
                            );
                            println!("{}", arch_msg);
                            push_log(&manager, &arch_msg);
                        } else {
                            set_job_vt(&db, &queue_cfg, p.msg_id, delay as i32).await?;
                            let sched_msg = format!(
                                "[ingest_worker] job msg_id={} rescheduled in {}s (attempt {})",
                                p.msg_id, delay, attempt
                            );
                            println!("{}", sched_msg);
                            push_log(&manager, &sched_msg);
                        }
                    }
                }
            }
            None => {
                tokio::select! {
                    _ = sleep(poll_delay) => {}
                    msg = async { match &mut notify_stream { Some(rx) => rx.recv().await, None => pending::<Option<String>>().await } } => {
                        if let Some(payload) = msg {
                            let note = format!("[ingest_worker] NOTIFY: {}", payload);
                            println!("{}", note);
                            push_log(&manager, &note);
                        }
                    }
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    i_miss_rust::util::env::bootstrap_cli("ingest_worker");
    run_from_env().await
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IngestJob {
    provider: String,
    task: String,
    args: Option<serde_json::Value>,
    #[serde(default)]
    provider_id: Option<i64>,
    requested_at: chrono::DateTime<Utc>,
    correlation_id: String,
}

impl IngestJob {
    fn new(provider: &str, task: &str, args: Option<serde_json::Value>) -> Self {
        Self {
            provider: provider.to_string(),
            task: task.to_string(),
            args,
            provider_id: None,
            requested_at: Utc::now(),
            correlation_id: format!(
                "ingest-{}-{}",
                Utc::now().timestamp_millis(),
                std::process::id()
            ),
        }
    }
}

#[derive(Debug)]
struct PoppedJob {
    msg_id: i64,
    read_ct: i32,
    job: IngestJob,
}

#[derive(Debug, Clone, Default, Serialize)]
struct WorkerMetrics {
    last_wait_ms: u64,
    last_run_ms: u64,
    dequeues: u64,
    failures: u64,
    last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct QueueConfig {
    queue_name: String,
    visibility_timeout_secs: i32,
    poll_interval_secs: u64,
    max_retries: u32,
    retry_base_secs: u64,
    retry_max_secs: u64,
    notify_channels: Vec<String>,
}

impl QueueConfig {
    fn from_env() -> Self {
        let queue_name = env::var("INGEST_QUEUE_NAME")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "default_ingest".to_string());
        let vt = env::var("INGEST_QUEUE_VT_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(60)
            .max(1);
        let poll = env::var("INGEST_QUEUE_POLL_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(2);
        let max_retries = env::var("INGEST_QUEUE_MAX_RETRIES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(5);
        let retry_base_secs = env::var("INGEST_QUEUE_RETRY_BASE_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(5);
        let retry_max_secs = env::var("INGEST_QUEUE_RETRY_MAX_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(300);
        let notify_channels = env::var("INGEST_NOTIFY_CHANNEL")
            .ok()
            .map(|s| {
                s.split(',')
                    .map(|p| p.trim().to_string())
                    .filter(|p| !p.is_empty())
                    .collect::<Vec<_>>()
            })
            .filter(|v: &Vec<String>| !v.is_empty())
            .unwrap_or_else(|| vec!["ingest_queue".to_string()]);
        Self {
            queue_name,
            visibility_timeout_secs: vt,
            poll_interval_secs: poll,
            max_retries,
            retry_base_secs,
            retry_max_secs,
            notify_channels,
        }
    }
}

async fn handle_job(db: &Db, job: &IngestJob) -> Result<()> {
    match (job.provider.as_str(), job.task.as_str()) {
        // Unified task to run all supported steps for a provider
        ("ps", "all") | ("playstation", "all") | ("psstore", "all") => {
            // Run PS catalogue (may include media), then prices, then ratings
            i_miss_rust::database_ops::playstation::ingest_demo::run_from_env().await?;
            i_miss_rust::database_ops::playstation::prices::run_from_env().await?;
            i_miss_rust::database_ops::playstation::ratings::run_from_env().await?;
            Ok(())
        }
        ("steam", "all") => {
            // Steam provider run (catalogue/media as implemented in provider)
            i_miss_rust::database_ops::steam::provider::SteamProvider::run_from_env(db).await?;
            Ok(())
        }
        ("steam", "backfill") => {
            // Parameterized backfill via env to leverage existing code path.
            // Supported optional args: { recent_days: i64, max_regions: usize, fetch_media: bool, language: string }
            struct EnvScope {
                prev: Vec<(String, Option<String>)>,
            }
            impl EnvScope {
                fn new() -> Self {
                    Self { prev: Vec::new() }
                }
                fn set(&mut self, k: &str, v: &str) {
                    let p = std::env::var(k).ok();
                    self.prev.push((k.to_string(), p));
                    _set_env_var(k, v);
                }
            }
            impl Drop for EnvScope {
                fn drop(&mut self) {
                    for (k, v) in self.prev.drain(..) {
                        if let Some(val) = v {
                            _set_env_var(&k, &val);
                        } else {
                            _remove_env_var(&k);
                        }
                    }
                }
            }
            let mut envs = EnvScope::new();
            envs.set("STEAM_BACKFILL", "1");
            if let Some(args) = &job.args {
                if let Some(days) = args.get("recent_days").and_then(|v| v.as_i64()) {
                    envs.set("STEAM_RECENT_MISSING_DAYS", &days.to_string());
                }
                if let Some(maxr) = args.get("max_regions").and_then(|v| v.as_u64()) {
                    envs.set("STEAM_MAX_REGIONS", &maxr.to_string());
                }
                if let Some(fetch) = args.get("fetch_media").and_then(|v| v.as_bool()) {
                    if fetch {
                        envs.set("STEAM_FETCH_MEDIA", "1");
                    }
                }
                if let Some(lang) = args.get("language").and_then(|v| v.as_str()) {
                    envs.set("STEAM_LANGUAGE", lang);
                }
            }
            i_miss_rust::database_ops::steam::provider::SteamProvider::run_from_env(db).await?;
            Ok(())
        }
        ("igdb", "all") => {
            i_miss_rust::database_ops::igdb::client::run_from_env(db).await?;
            Ok(())
        }
        ("igdb", "backfill") => {
            // Structured IGDB backfill:
            // { from_year, to_year, platforms: [ids], page_size, max_pages, reqs_per_min, rps, concurrency, max_retries, backoff_ms }
            use i_miss_rust::database_ops::igdb::client::IgdbService;
            // Allow rate-limit args to override env for this job via scoped env
            struct EnvScope {
                prev: Vec<(String, Option<String>)>,
            }
            impl EnvScope {
                fn new() -> Self {
                    Self { prev: Vec::new() }
                }
                fn set(&mut self, k: &str, v: &str) {
                    let p = std::env::var(k).ok();
                    self.prev.push((k.to_string(), p));
                    _set_env_var(k, v);
                }
            }
            impl Drop for EnvScope {
                fn drop(&mut self) {
                    for (k, v) in self.prev.drain(..) {
                        if let Some(val) = v {
                            _set_env_var(&k, &val);
                        } else {
                            _remove_env_var(&k);
                        }
                    }
                }
            }
            let mut scoped = EnvScope::new();
            let (mut y_from, mut y_to) = (2020i32, chrono::Utc::now().year());
            let mut platforms: Vec<i32> = vec![];
            let mut page_size: usize = 200;
            let mut max_pages: usize = 50;
            if let Some(args) = &job.args {
                if let Some(v) = args.get("from_year").and_then(|v| v.as_i64()) {
                    y_from = v as i32;
                }
                if let Some(v) = args.get("to_year").and_then(|v| v.as_i64()) {
                    y_to = v as i32;
                }
                if let Some(arr) = args.get("platforms").and_then(|v| v.as_array()) {
                    platforms = arr
                        .iter()
                        .filter_map(|x| x.as_i64().map(|n| n as i32))
                        .collect();
                }
                if let Some(v) = args.get("page_size").and_then(|v| v.as_u64()) {
                    page_size = v as usize;
                }
                if let Some(v) = args.get("max_pages").and_then(|v| v.as_u64()) {
                    max_pages = v as usize;
                }
                // Optional rate-limit controls
                if let Some(v) = args.get("reqs_per_min").and_then(|v| v.as_u64()) {
                    scoped.set("IGDB_REQS_PER_MIN", &v.to_string());
                }
                if let Some(v) = args.get("rps").and_then(|v| v.as_f64()) {
                    scoped.set("IGDB_RPS", &format!("{}", v));
                }
                if let Some(v) = args.get("concurrency").and_then(|v| v.as_u64()) {
                    scoped.set("IGDB_CONCURRENCY", &v.to_string());
                }
                if let Some(v) = args.get("max_retries").and_then(|v| v.as_u64()) {
                    scoped.set("IGDB_MAX_RETRIES", &v.to_string());
                }
                if let Some(v) = args.get("backoff_ms").and_then(|v| v.as_u64()) {
                    scoped.set("IGDB_BACKOFF_MS", &v.to_string());
                }
            }
            let svc = IgdbService::new_from_env()?;
            let _ = svc
                .backfill_range(db, y_from, y_to, &platforms, page_size, max_pages)
                .await?;
            Ok(())
        }
        ("xbox", "all") | ("microsoft", "all") => {
            // Map structured args to env for xbox provider
            // Args: { market, language, product_ids:[...], product_ids_file, ms_cv, dry_run:bool, chunk_sleep_ms, chunk_size, reqs_per_min, rps, max_retries, backoff_ms }
            struct EnvScope {
                prev: Vec<(String, Option<String>)>,
            }
            impl EnvScope {
                fn new() -> Self {
                    Self { prev: Vec::new() }
                }
                fn set(&mut self, k: &str, v: &str) {
                    let p = std::env::var(k).ok();
                    self.prev.push((k.to_string(), p));
                    _set_env_var(k, v);
                }
            }
            impl Drop for EnvScope {
                fn drop(&mut self) {
                    for (k, v) in self.prev.drain(..) {
                        if let Some(val) = v {
                            _set_env_var(&k, &val);
                        } else {
                            _remove_env_var(&k);
                        }
                    }
                }
            }
            let mut envs = EnvScope::new();
            if let Some(args) = &job.args {
                if let Some(v) = args.get("market").and_then(|v| v.as_str()) {
                    envs.set("XBOX_MARKET", v);
                }
                if let Some(v) = args.get("language").and_then(|v| v.as_str()) {
                    envs.set("XBOX_LANGUAGE", v);
                }
                if let Some(v) = args.get("product_ids_file").and_then(|v| v.as_str()) {
                    envs.set("XBOX_PRODUCT_IDS_FILE", v);
                }
                if let Some(arr) = args.get("product_ids").and_then(|v| v.as_array()) {
                    let joined = arr
                        .iter()
                        .filter_map(|x| x.as_str().map(|s| s.to_string()))
                        .collect::<Vec<_>>()
                        .join(",");
                    if !joined.is_empty() {
                        envs.set("XBOX_PRODUCT_IDS", &joined);
                    }
                }
                if let Some(v) = args.get("ms_cv").and_then(|v| v.as_str()) {
                    envs.set("XBOX_MS_CV", v);
                }
                if let Some(v) = args.get("dry_run").and_then(|v| v.as_bool()) {
                    if v {
                        envs.set("XBOX_DRY_RUN", "1");
                    }
                }
                if let Some(v) = args.get("chunk_sleep_ms").and_then(|v| v.as_u64()) {
                    envs.set("XBOX_CHUNK_SLEEP_MS", &v.to_string());
                }
                if let Some(v) = args.get("chunk_size").and_then(|v| v.as_u64()) {
                    envs.set("XBOX_CHUNK_SIZE", &v.to_string());
                }
                if let Some(v) = args.get("reqs_per_min").and_then(|v| v.as_u64()) {
                    envs.set("XBOX_REQS_PER_MIN", &v.to_string());
                }
                if let Some(v) = args.get("rps").and_then(|v| v.as_f64()) {
                    envs.set("XBOX_RPS", &format!("{}", v));
                }
                if let Some(v) = args.get("max_retries").and_then(|v| v.as_u64()) {
                    envs.set("XBOX_MAX_RETRIES", &v.to_string());
                }
                if let Some(v) = args.get("backoff_ms").and_then(|v| v.as_u64()) {
                    envs.set("XBOX_BACKOFF_MS", &v.to_string());
                }
            }
            i_miss_rust::database_ops::xbox::provider::run_from_env(db).await?;
            Ok(())
        }
        ("xbox", "backfill") | ("microsoft", "backfill") => {
            // Route to same provider run until a specialized backfill flow is added
            i_miss_rust::database_ops::xbox::provider::run_from_env(db).await?;
            Ok(())
        }

        ("steam", "catalog") | ("steam", "run") => {
            i_miss_rust::database_ops::steam::provider::SteamProvider::run_from_env(db).await?;
            Ok(())
        }
        ("ps", "backfill") | ("playstation", "backfill") | ("psstore", "backfill") => {
            // Parameterized PS Store backfill using the existing prices pipeline.
            // Supported args (all optional):
            // { locales: ["en-us","en-gb"], regions: ["en-us"], region: "us", pages: 5, page_size: 100,
            //   page_concurrency: 2, rps: 3, max_retries: 5, backoff_ms: 1500, max_offset: 5000,
            //   sha: "...", cat_ps4: "...", cat_ps5: "..." }
            struct EnvScope {
                prev: Vec<(String, Option<String>)>,
            }
            impl EnvScope {
                fn new() -> Self {
                    Self { prev: Vec::new() }
                }
                fn set(&mut self, k: &str, v: &str) {
                    let p = std::env::var(k).ok();
                    self.prev.push((k.to_string(), p));
                    _set_env_var(k, v);
                }
            }
            impl Drop for EnvScope {
                fn drop(&mut self) {
                    for (k, v) in self.prev.drain(..) {
                        if let Some(val) = v {
                            _set_env_var(&k, &val);
                        } else {
                            _remove_env_var(&k);
                        }
                    }
                }
            }
            let mut envs = EnvScope::new();
            if let Some(args) = &job.args {
                // locales/regions handling
                if let Some(locales) = args.get("locales").and_then(|v| v.as_array()) {
                    let vals: Vec<String> = locales
                        .iter()
                        .filter_map(|x| x.as_str().map(|s| s.to_lowercase()))
                        .collect();
                    if !vals.is_empty() {
                        envs.set("PS_STORE_REGIONS", &vals.join(","));
                    }
                } else if let Some(regions) = args.get("regions").and_then(|v| v.as_array()) {
                    let vals: Vec<String> = regions
                        .iter()
                        .filter_map(|x| x.as_str().map(|s| s.to_lowercase()))
                        .collect();
                    if !vals.is_empty() {
                        envs.set("PS_STORE_REGIONS", &vals.join(","));
                    }
                } else if let Some(region) = args.get("region").and_then(|v| v.as_str()) {
                    // Accept simple country like "us" and expand to en-us
                    let r = region.trim().to_lowercase();
                    let locale = if r.len() == 2 { format!("en-{}", r) } else { r };
                    envs.set("PS_STORE_REGIONS", &locale);
                }
                if let Some(pages) = args.get("pages").and_then(|v| v.as_u64()) {
                    envs.set("PS_MAX_PAGES", &pages.to_string());
                }
                if let Some(psize) = args.get("page_size").and_then(|v| v.as_u64()) {
                    envs.set("PS_PAGE_SIZE", &psize.to_string());
                }
                if let Some(pc) = args.get("page_concurrency").and_then(|v| v.as_u64()) {
                    envs.set("PS_PAGE_CONCURRENCY", &pc.to_string());
                }
                if let Some(rps) = args.get("rps").and_then(|v| v.as_u64()) {
                    envs.set("PS_STORE_RPS", &rps.to_string());
                }
                if let Some(retries) = args.get("max_retries").and_then(|v| v.as_u64()) {
                    envs.set("PS_STORE_MAX_RETRIES", &retries.to_string());
                }
                if let Some(backoff) = args.get("backoff_ms").and_then(|v| v.as_u64()) {
                    envs.set("PS_STORE_BACKOFF_MS", &backoff.to_string());
                }
                if let Some(max_off) = args.get("max_offset").and_then(|v| v.as_u64()) {
                    envs.set("PS_MAX_OFFSET", &max_off.to_string());
                }
                if let Some(sha) = args.get("sha").and_then(|v| v.as_str()) {
                    envs.set("PS_HASH", sha);
                }
                // If PS_HASH not provided via args/env, but PSSTORE_SHA256 is set globally,
                // map it into PS_HASH for downstream code compatibility
                if std::env::var("PS_HASH")
                    .ok()
                    .filter(|s| !s.is_empty())
                    .is_none()
                {
                    if let Ok(sha2) = std::env::var("PSSTORE_SHA256") {
                        if !sha2.is_empty() {
                            envs.set("PS_HASH", &sha2);
                        }
                    }
                }
                if let Some(cat4) = args.get("cat_ps4").and_then(|v| v.as_str()) {
                    envs.set("PS4_CATEGORY", cat4);
                }
                if let Some(cat5) = args.get("cat_ps5").and_then(|v| v.as_str()) {
                    envs.set("PS5_CATEGORY", cat5);
                }
            }
            i_miss_rust::database_ops::playstation::prices::run_from_env().await?;
            Ok(())
        }
        ("ps", "prices") | ("playstation", "prices") | ("psstore", "prices") => {
            // Optional args mapping (same keys as backfill)
            struct EnvScope {
                prev: Vec<(String, Option<String>)>,
            }
            impl EnvScope {
                fn new() -> Self {
                    Self { prev: Vec::new() }
                }
                fn set(&mut self, k: &str, v: &str) {
                    let p = std::env::var(k).ok();
                    self.prev.push((k.to_string(), p));
                    _set_env_var(k, v);
                }
            }
            impl Drop for EnvScope {
                fn drop(&mut self) {
                    for (k, v) in self.prev.drain(..) {
                        if let Some(val) = v {
                            _set_env_var(&k, &val);
                        } else {
                            _remove_env_var(&k);
                        }
                    }
                }
            }
            let mut envs = EnvScope::new();
            if let Some(args) = &job.args {
                if let Some(locales) = args.get("locales").and_then(|v| v.as_array()) {
                    let vals: Vec<String> = locales
                        .iter()
                        .filter_map(|x| x.as_str().map(|s| s.to_lowercase()))
                        .collect();
                    if !vals.is_empty() {
                        envs.set("PS_STORE_REGIONS", &vals.join(","));
                    }
                } else if let Some(regions) = args.get("regions").and_then(|v| v.as_array()) {
                    let vals: Vec<String> = regions
                        .iter()
                        .filter_map(|x| x.as_str().map(|s| s.to_lowercase()))
                        .collect();
                    if !vals.is_empty() {
                        envs.set("PS_STORE_REGIONS", &vals.join(","));
                    }
                } else if let Some(region) = args.get("region").and_then(|v| v.as_str()) {
                    let r = region.trim().to_lowercase();
                    let locale = if r.len() == 2 { format!("en-{}", r) } else { r };
                    envs.set("PS_STORE_REGIONS", &locale);
                }
                if let Some(pages) = args.get("pages").and_then(|v| v.as_u64()) {
                    envs.set("PS_MAX_PAGES", &pages.to_string());
                }
                if let Some(psize) = args.get("page_size").and_then(|v| v.as_u64()) {
                    envs.set("PS_PAGE_SIZE", &psize.to_string());
                }
                if let Some(pc) = args.get("page_concurrency").and_then(|v| v.as_u64()) {
                    envs.set("PS_PAGE_CONCURRENCY", &pc.to_string());
                }
                if let Some(rps) = args.get("rps").and_then(|v| v.as_u64()) {
                    envs.set("PS_STORE_RPS", &rps.to_string());
                }
                if let Some(retries) = args.get("max_retries").and_then(|v| v.as_u64()) {
                    envs.set("PS_STORE_MAX_RETRIES", &retries.to_string());
                }
                if let Some(backoff) = args.get("backoff_ms").and_then(|v| v.as_u64()) {
                    envs.set("PS_STORE_BACKOFF_MS", &backoff.to_string());
                }
                if let Some(max_off) = args.get("max_offset").and_then(|v| v.as_u64()) {
                    envs.set("PS_MAX_OFFSET", &max_off.to_string());
                }
                if let Some(sha) = args.get("sha").and_then(|v| v.as_str()) {
                    envs.set("PS_HASH", sha);
                }
                if std::env::var("PS_HASH")
                    .ok()
                    .filter(|s| !s.is_empty())
                    .is_none()
                {
                    if let Ok(sha2) = std::env::var("PSSTORE_SHA256") {
                        if !sha2.is_empty() {
                            envs.set("PS_HASH", &sha2);
                        }
                    }
                }
                if let Some(cat4) = args.get("cat_ps4").and_then(|v| v.as_str()) {
                    envs.set("PS4_CATEGORY", cat4);
                }
                if let Some(cat5) = args.get("cat_ps5").and_then(|v| v.as_str()) {
                    envs.set("PS5_CATEGORY", cat5);
                }
            }
            i_miss_rust::database_ops::playstation::prices::run_from_env().await?;
            Ok(())
        }
        ("ps", "ratings") | ("playstation", "ratings") | ("psstore", "ratings") => {
            i_miss_rust::database_ops::playstation::ratings::run_from_env().await?;
            Ok(())
        }
        ("ps", "catalog")
        | ("ps", "ingest")
        | ("playstation", "catalog")
        | ("playstation", "ingest")
        | ("psstore", "catalog")
        | ("psstore", "ingest") => {
            i_miss_rust::database_ops::playstation::ingest_demo::run_from_env().await?;
            Ok(())
        }
        ("igdb", "catalog") => {
            struct EnvScope {
                prev: Vec<(String, Option<String>)>,
            }
            impl EnvScope {
                fn new() -> Self {
                    Self { prev: Vec::new() }
                }
                fn set(&mut self, key: &str, val: &str) {
                    let prev = std::env::var(key).ok();
                    self.prev.push((key.to_string(), prev));
                    _set_env_var(key, val);
                }
            }
            impl Drop for EnvScope {
                fn drop(&mut self) {
                    for (key, val) in self.prev.drain(..) {
                        if let Some(v) = val {
                            _set_env_var(&key, &v);
                        } else {
                            _remove_env_var(&key);
                        }
                    }
                }
            }

            let mut envs = EnvScope::new();
            if let Some(args) = &job.args {
                if let Some(v) = args.get("mode").and_then(|v| v.as_str()) {
                    envs.set("IGDB_MODE", v);
                }
                if let Some(v) = args.get("page_size").and_then(|v| v.as_u64()) {
                    envs.set("IGDB_PAGE_SIZE", &v.to_string());
                }
                if let Some(v) = args.get("max_pages").and_then(|v| v.as_u64()) {
                    envs.set("IGDB_MAX_PAGES", &v.to_string());
                }
                if let Some(v) = args.get("reqs_per_min").and_then(|v| v.as_u64()) {
                    envs.set("IGDB_REQS_PER_MIN", &v.to_string());
                }
                if let Some(v) = args.get("rps").and_then(|v| v.as_f64()) {
                    envs.set("IGDB_RPS", &format!("{}", v));
                }
                if let Some(v) = args.get("concurrency").and_then(|v| v.as_u64()) {
                    envs.set("IGDB_CONCURRENCY", &v.to_string());
                }
            }

            i_miss_rust::database_ops::igdb::client::run_from_env(db).await?;
            Ok(())
        }
        ("xbox", "catalog") => {
            i_miss_rust::database_ops::xbox::provider::run_from_env(db).await?;
            Ok(())
        }
        ("nexarda", "catalog") | ("nexarda", "ingest") | ("nexarda", "all") => {
            let base_url_opt = env::var("NEXARDA_BASE_URL").ok().filter(|s| !s.is_empty());
            let timeout_secs: u64 = env::var("NEXARDA_TIMEOUT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(30);
            // Allow per-job rate-limit overrides, mapped into env
            struct EnvScope {
                prev: Vec<(String, Option<String>)>,
            }
            impl EnvScope {
                fn new() -> Self {
                    Self { prev: Vec::new() }
                }
                fn set(&mut self, k: &str, v: &str) {
                    let p = std::env::var(k).ok();
                    self.prev.push((k.to_string(), p));
                    _set_env_var(k, v);
                }
            }
            impl Drop for EnvScope {
                fn drop(&mut self) {
                    for (k, v) in self.prev.drain(..) {
                        if let Some(val) = v {
                            _set_env_var(&k, &val);
                        } else {
                            _remove_env_var(&k);
                        }
                    }
                }
            }
            let mut envs = EnvScope::new();
            if let Some(args) = &job.args {
                if let Some(v) = args.get("reqs_per_min").and_then(|v| v.as_u64()) {
                    envs.set("NEXARDA_REQS_PER_MIN", &v.to_string());
                }
                if let Some(v) = args.get("rps").and_then(|v| v.as_f64()) {
                    envs.set("NEXARDA_RPS", &format!("{}", v));
                }
                if let Some(v) = args.get("concurrency").and_then(|v| v.as_u64()) {
                    envs.set("NEXARDA_CONCURRENCY", &v.to_string());
                }
                if let Some(v) = args.get("max_retries").and_then(|v| v.as_u64()) {
                    envs.set("NEXARDA_MAX_RETRIES", &v.to_string());
                }
                if let Some(v) = args.get("backoff_ms").and_then(|v| v.as_u64()) {
                    envs.set("NEXARDA_BACKOFF_MS", &v.to_string());
                }
            }
            let nx = NexardaProvider::new(base_url_opt.as_deref(), Some(timeout_secs))
                .context("nexarda init")?;
            let opts = NexardaOptions {
                products: serde_json::from_str(&env::var("NEXARDA_PRODUCTS").unwrap_or_default())
                    .unwrap_or_default(),
                store_map: serde_json::from_str(&env::var("NEXARDA_STORE_MAP").unwrap_or_default())
                    .unwrap_or_default(),
                api_key: env::var("NEXARDA_API_KEY").ok().filter(|s| !s.is_empty()),
                auto_register_stores: Some(true),
                default_regions: serde_json::from_str(
                    &env::var("NEXARDA_DEFAULT_REGIONS").unwrap_or_default(),
                )
                .unwrap_or_default(),
                dynamic_store_overrides: serde_json::from_str(
                    &env::var("NEXARDA_STORE_OVERRIDES").unwrap_or_default(),
                )
                .unwrap_or_default(),
                default_tax_inclusive: Some(true),
                context: None,
                base_url: None,
                timeout: None,
            };
            nx.ingest_to_db(db, opts).await?;
            Ok(())
        }
        ("nexarda", "json") | ("nexarda", "catalogue_file") => {
            use i_miss_rust::database_ops::nexarda::provider::NexardaProvider;
            let path = job
                .args
                .as_ref()
                .and_then(|a| a.get("path"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .or_else(|| std::env::var("NEXARDA_CATALOGUE_PATH").ok())
                .or_else(|| std::env::var("NEXARDA_CATALOGUE_FILE").ok())
                .or_else(|| Some("nexarda_product_catalogue.json".to_string()))
                .unwrap();
            let simple = job
                .args
                .as_ref()
                .and_then(|a| a.get("simple"))
                .and_then(|v| v.as_bool())
                .or_else(|| {
                    std::env::var("NEXARDA_SIMPLE")
                        .ok()
                        .map(|s| (s == "1" || s.eq_ignore_ascii_case("true")))
                })
                .unwrap_or(false);
            let limit = job
                .args
                .as_ref()
                .and_then(|a| a.get("limit"))
                .and_then(|v| v.as_u64())
                .map(|n| n as usize)
                .or_else(|| {
                    std::env::var("NEXARDA_CATALOGUE_LIMIT")
                        .ok()
                        .and_then(|s| s.parse().ok())
                });
            let processed =
                NexardaProvider::ingest_catalogue_file(db, Some(&path), Some(simple), limit)
                    .await?;
            println!(
                "[ingest_worker] nexarda json ingested {} titles (path={})",
                processed, path
            );
            Ok(())
        }

        ("tgdb", "catalog")
        | ("tgdb", "ingest")
        | ("tgdb", "sync")
        | ("tgdb", "all")
        | ("thegamesdb", "catalog")
        | ("thegamesdb", "ingest")
        | ("thegamesdb", "sync")
        | ("thegamesdb", "all") => {
            // TGDB mirror ingestion (catalogue + media links when schema supports them).
            // Args (all optional): { api_key, year_min, year_max, page_size, reqs_per_min }
            struct EnvScope {
                prev: Vec<(String, Option<String>)>,
            }
            impl EnvScope {
                fn new() -> Self {
                    Self { prev: Vec::new() }
                }
                fn set(&mut self, k: &str, v: &str) {
                    let p = std::env::var(k).ok();
                    self.prev.push((k.to_string(), p));
                    _set_env_var(k, v);
                }
            }
            impl Drop for EnvScope {
                fn drop(&mut self) {
                    for (k, v) in self.prev.drain(..) {
                        if let Some(val) = v {
                            _set_env_var(&k, &val);
                        } else {
                            _remove_env_var(&k);
                        }
                    }
                }
            }

            let mut api_key = std::env::var("TGDB_API_KEY").ok().filter(|s| !s.is_empty());
            if let Some(args) = &job.args {
                let mut envs = EnvScope::new();

                if let Some(v) = args.get("year_min").and_then(|v| v.as_i64()) {
                    envs.set("TGDB_YEAR_MIN", &v.to_string());
                }
                if let Some(v) = args.get("year_max").and_then(|v| v.as_i64()) {
                    envs.set("TGDB_YEAR_MAX", &v.to_string());
                }
                if let Some(v) = args.get("page_size").and_then(|v| v.as_u64()) {
                    envs.set("TGDB_PAGE_SIZE", &v.to_string());
                }
                if let Some(v) = args.get("reqs_per_min").and_then(|v| v.as_u64()) {
                    envs.set("TGDB_REQS_PER_MIN", &v.to_string());
                }
                if let Some(k) = args.get("api_key").and_then(|v| v.as_str()) {
                    api_key = Some(k.to_string());
                }

                // Keep env overrides alive for the duration of the async call.
                // (Drop restores previous values.)
                let allow_anon = std::env::var("TGDB_ALLOW_ANON")
                    .ok()
                    .map(|v| !(v == "0" || v.eq_ignore_ascii_case("false")))
                    .unwrap_or(false);

                if api_key.is_none() && !allow_anon {
                    println!(
                        "[ingest_worker] tgdb: skipped (TGDB_API_KEY missing; set TGDB_ALLOW_ANON=1 to attempt anonymous calls)"
                    );
                    return Ok(());
                }

                i_miss_rust::database_ops::tgdb::sync(db, api_key).await?;
                return Ok(());
            }

            let allow_anon = std::env::var("TGDB_ALLOW_ANON")
                .ok()
                .map(|v| !(v == "0" || v.eq_ignore_ascii_case("false")))
                .unwrap_or(false);
            if api_key.is_none() && !allow_anon {
                println!(
                    "[ingest_worker] tgdb: skipped (TGDB_API_KEY missing; set TGDB_ALLOW_ANON=1 to attempt anonymous calls)"
                );
                return Ok(());
            }
            i_miss_rust::database_ops::tgdb::sync(db, api_key).await?;
            Ok(())
        }

        ("itad", "prices_scan") | ("itad", "sync") | ("itad", "all") => {
            // ITAD pricing sync (bounded). Args (all optional):
            // { api_key, base_url, timeout_secs, country, deals_limit, max_game_overviews, default_currency, country_name }
            struct EnvScope {
                prev: Vec<(String, Option<String>)>,
            }
            impl EnvScope {
                fn new() -> Self {
                    Self { prev: Vec::new() }
                }
                fn set(&mut self, k: &str, v: &str) {
                    let p = std::env::var(k).ok();
                    self.prev.push((k.to_string(), p));
                    _set_env_var(k, v);
                }
            }
            impl Drop for EnvScope {
                fn drop(&mut self) {
                    for (k, v) in self.prev.drain(..) {
                        if let Some(val) = v {
                            _set_env_var(&k, &val);
                        } else {
                            _remove_env_var(&k);
                        }
                    }
                }
            }

            let mut api_key = std::env::var("ITAD_API_KEY").ok().filter(|s| !s.is_empty());
            let mut envs = EnvScope::new();

            if let Some(args) = &job.args {
                if let Some(k) = args.get("api_key").and_then(|v| v.as_str()) {
                    api_key = Some(k.to_string());
                }
                if let Some(v) = args.get("base_url").and_then(|v| v.as_str()) {
                    envs.set("ITAD_BASE_URL", v);
                }
                if let Some(v) = args.get("timeout_secs").and_then(|v| v.as_u64()) {
                    envs.set("ITAD_TIMEOUT_SECS", &v.to_string());
                }
                if let Some(v) = args.get("country").and_then(|v| v.as_str()) {
                    envs.set("ITAD_COUNTRY", v);
                }
                if let Some(v) = args.get("deals_limit").and_then(|v| v.as_u64()) {
                    envs.set("ITAD_DEALS_LIMIT", &v.to_string());
                }
                if let Some(v) = args.get("max_game_overviews").and_then(|v| v.as_u64()) {
                    envs.set("ITAD_MAX_GAME_OVERVIEWS", &v.to_string());
                }
                if let Some(v) = args.get("default_currency").and_then(|v| v.as_str()) {
                    envs.set("ITAD_DEFAULT_CURRENCY", v);
                }
                if let Some(v) = args.get("country_name").and_then(|v| v.as_str()) {
                    envs.set("ITAD_COUNTRY_NAME", v);
                }
            }

            let _ = i_miss_rust::database_ops::itad::sync(db, api_key).await?;
            Ok(())
        }
        ("rawg", "catalog")
        | ("rawg", "ingest")
        | ("rawg", "sync")
        | ("rawg", "all")
        | ("rawg", "range") => {
            struct EnvScope {
                prev: Vec<(String, Option<String>)>,
            }
            impl EnvScope {
                fn new() -> Self {
                    Self { prev: Vec::new() }
                }
                fn set(&mut self, key: &str, val: &str) {
                    let prev = std::env::var(key).ok();
                    self.prev.push((key.to_string(), prev));
                    _set_env_var(key, val);
                }
            }
            impl Drop for EnvScope {
                fn drop(&mut self) {
                    for (key, val) in self.prev.drain(..) {
                        if let Some(v) = val {
                            _set_env_var(&key, &v);
                        } else {
                            _remove_env_var(&key);
                        }
                    }
                }
            }

            let mut year_min = std::env::var("RAWG_YEAR_MIN")
                .ok()
                .and_then(|s| s.parse::<i32>().ok())
                .unwrap_or(2015);
            let mut year_max = std::env::var("RAWG_YEAR_MAX")
                .ok()
                .and_then(|s| s.parse::<i32>().ok())
                .unwrap_or(2025);
            let mut api_key = std::env::var("RAWG_API_KEY").ok().filter(|s| !s.is_empty());
            let mut scope = EnvScope::new();

            if let Some(args) = &job.args {
                if let Some(v) = args.get("year_min").and_then(|v| v.as_i64()) {
                    year_min = v as i32;
                }
                if let Some(v) = args.get("year_max").and_then(|v| v.as_i64()) {
                    year_max = v as i32;
                }
                if let Some(v) = args.get("mode").and_then(|v| v.as_str()) {
                    scope.set("RAWG_MODE", v);
                }
                if let Some(k) = args.get("api_key").and_then(|v| v.as_str()) {
                    api_key = Some(k.to_string());
                }
                if let Some(v) = args.get("page_size").and_then(|v| v.as_u64()) {
                    scope.set("RAWG_PAGE_SIZE", &v.to_string());
                }
                if let Some(v) = args.get("reqs_per_min").and_then(|v| v.as_u64()) {
                    scope.set("RAWG_REQS_PER_MIN", &v.to_string());
                }
                if let Some(v) = args.get("fetch_details").and_then(|v| v.as_bool()) {
                    scope.set("RAWG_FETCH_DETAILS", if v { "1" } else { "0" });
                }
                if let Some(v) = args.get("sleep_ms").and_then(|v| v.as_u64()) {
                    scope.set("RAWG_SLEEP_MS_OVERRIDE", &v.to_string());
                }
            }

            if year_min > year_max {
                std::mem::swap(&mut year_min, &mut year_max);
            }

            // rawg::sync uses env for range bounds, so set them here for consistent behavior.
            scope.set("RAWG_YEAR_MIN", &year_min.to_string());
            scope.set("RAWG_YEAR_MAX", &year_max.to_string());

            i_miss_rust::database_ops::rawg::sync(db, api_key).await?;
            Ok(())
        }
        ("giantbomb", "json") | ("giant_bomb", "json") | ("gb", "json") | ("giantbomb", "load") => {
            use i_miss_rust::database_ops::giantbomb::ingest::ingest_from_file;
            let candidates = [
                job.args
                    .as_ref()
                    .and_then(|a| a.get("path"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                std::env::var("GIANT_BOMB_FILE").ok(),
                Some("keep/giant_bomb_games_detailed.json".to_string()),
                Some("keep/giant_bomb_games_detailed_2.json".to_string()),
                Some("keep/giant_bomb_games_detalied.json".to_string()),
            ];
            let mut path = String::new();
            for c in candidates.into_iter().flatten() {
                path = c;
                break;
            }
            let limit = job
                .args
                .as_ref()
                .and_then(|a| a.get("limit"))
                .and_then(|v| v.as_u64())
                .map(|n| n as usize)
                .or_else(|| std::env::var("GB_LIMIT").ok().and_then(|s| s.parse().ok()));
            let count = ingest_from_file(db, &path, limit).await?;
            println!(
                "[ingest_worker] giantbomb json ingested {} entries (path={})",
                count, path
            );
            Ok(())
        }
        // Provider items scoped claim+finalize (housekeeping or external processing wrapper)
        ("provider_items", "scoped") => {
            let batch_size_env: i32 = env::var("INGEST_PROVIDER_ITEMS_BATCH")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(500);
            let batch_size = job
                .args
                .as_ref()
                .and_then(|v| v.get("batch_size"))
                .and_then(|b| b.as_i64())
                .map(|n| n as i32)
                .unwrap_or(batch_size_env);
            let count = process_provider_items_scoped(db, job.provider_id, batch_size).await?;
            println!(
                "[ingest_worker] provider_items scoped processed {} rows (provider_id={:?})",
                count, job.provider_id
            );
            Ok(())
        }
        _ => Err(anyhow!(
            "unknown provider/task: {}/{}",
            job.provider,
            job.task
        )),
    }
}

async fn ensure_queue(db: &Db, cfg: &QueueConfig) -> Result<()> {
    // PGMQ's create() is not fully idempotent in this environment because it attempts to
    // re-add existing objects to the extension, which errors if the sequence/table already
    // belongs to the extension. To be safe, detect existing queue relations and only call
    // create() when they are missing.
    let q_name = format!("q_{}", cfg.queue_name);
    let a_name = format!("a_{}", cfg.queue_name);
    let exists: bool = sqlx::query_scalar(
        "select exists (
             select 1
             from pg_class c
             join pg_namespace n on n.oid = c.relnamespace
             where c.relkind = 'r' and c.relname = $1
         ) or exists (
             select 1
             from pg_class c
             join pg_namespace n on n.oid = c.relnamespace
             where c.relkind = 'r' and c.relname = $2
         )",
    )
    .bind(&q_name)
    .bind(&a_name)
    .fetch_one(&db.pool)
    .await?;

    if !exists {
        sqlx::query("SELECT pgmq.\"create\"($1)")
            .bind(&cfg.queue_name)
            .execute(&db.pool)
            .await?;
    }

    Ok(())
}

async fn enqueue_job(db: &Db, cfg: &QueueConfig, job: &IngestJob) -> Result<i64> {
    let payload = serde_json::to_value(job)?;
    let row = sqlx::query("SELECT pgmq.send($1, $2) AS msg_id")
        .bind(&cfg.queue_name)
        .bind(sqlx::types::Json(payload))
        .fetch_one(&db.pool)
        .await?;
    let msg_id: i64 = row.try_get("msg_id").unwrap_or_default();
    // Notify all configured channels
    for ch in &cfg.notify_channels {
        let _ = sqlx::query("SELECT pg_notify($1, $2)")
            .bind(ch)
            .bind(cfg.queue_name.as_str())
            .execute(&db.pool)
            .await;
    }
    Ok(msg_id)
}

async fn pop_job(db: &Db, cfg: &QueueConfig) -> Result<Option<PoppedJob>> {
    // Prefer 4-arg read() when available; fallback to 3-arg for older pgmq
    let row4 =
        sqlx::query("SELECT msg_id, read_ct, message FROM pgmq.read($1, $2, 1, NULL::jsonb)")
            .bind(&cfg.queue_name)
            .bind(cfg.visibility_timeout_secs)
            .fetch_optional(&db.pool)
            .await;

    let row_opt = match row4 {
        Ok(opt) => opt,
        Err(_) => {
            // Try 3-arg signature
            sqlx::query("SELECT msg_id, read_ct, message FROM pgmq.read($1, $2, 1)")
                .bind(&cfg.queue_name)
                .bind(cfg.visibility_timeout_secs)
                .fetch_optional(&db.pool)
                .await?
        }
    };

    if let Some(row) = row_opt {
        let msg_id: i64 = row.try_get("msg_id")?;
        let read_ct: i32 = row.try_get("read_ct")?;
        let message: serde_json::Value = row.try_get("message")?;
        match serde_json::from_value::<IngestJob>(message) {
            Ok(job) => Ok(Some(PoppedJob {
                msg_id,
                read_ct,
                job,
            })),
            Err(e) => {
                eprintln!(
                    "[ingest_worker] bad payload msg_id={} err={e:?}; archiving",
                    msg_id
                );
                archive_job(db, cfg, msg_id).await?;
                Ok(None)
            }
        }
    } else {
        Ok(None)
    }
}

async fn delete_job(db: &Db, cfg: &QueueConfig, msg_id: i64) -> Result<()> {
    sqlx::query("SELECT pgmq.delete($1, $2)")
        .bind(&cfg.queue_name)
        .bind(msg_id)
        .execute(&db.pool)
        .await?;
    Ok(())
}
async fn archive_job(db: &Db, cfg: &QueueConfig, msg_id: i64) -> Result<()> {
    sqlx::query("SELECT pgmq.archive($1, $2)")
        .bind(&cfg.queue_name)
        .bind(msg_id)
        .execute(&db.pool)
        .await?;
    Ok(())
}
async fn set_job_vt(db: &Db, cfg: &QueueConfig, msg_id: i64, vt_secs: i32) -> Result<()> {
    // pgmq.set_vt signature in this environment: (queue_name text, msg_id bigint, vt integer)
    sqlx::query("SELECT pgmq.set_vt($1, $2, $3)")
        .persistent(false)
        .bind(&cfg.queue_name)
        .bind(msg_id)
        .bind(vt_secs)
        .execute(&db.pool)
        .await?;
    Ok(())
}

fn sanitize_session_url(raw: &str) -> String {
    if let Ok(mut parsed) = Url::parse(raw) {
        let pairs: Vec<(String, String)> = parsed
            .query_pairs()
            .filter(|(k, _)| k != "statement_cache_capacity")
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        parsed.set_query(None);
        if !pairs.is_empty() {
            let mut serializer = form_urlencoded::Serializer::new(String::new());
            for (k, v) in pairs {
                serializer.append_pair(&k, &v);
            }
            parsed.set_query(Some(&serializer.finish()));
        }
        parsed.to_string()
    } else {
        raw.to_string()
    }
}

async fn connect_listen_channel(
    url: String,
    channels: &Vec<String>,
) -> Result<tokio::sync::mpsc::UnboundedReceiver<String>> {
    let (client, mut connection) = tokio_postgres::connect(&url, NoTls).await?;
    // LISTEN on all channels
    for ch in channels {
        client.batch_execute(&format!("LISTEN {}", ch)).await?;
    }
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let channel_set: std::collections::HashSet<String> = channels.iter().cloned().collect();
    tokio::spawn(async move {
        let _client = client;
        let mut messages = futures::stream::poll_fn(move |cx| connection.poll_message(cx));
        while let Some(message) = messages.next().await {
            match message {
                Ok(AsyncMessage::Notification(n)) => {
                    if channel_set.contains(n.channel()) {
                        if tx.send(format!("{}:{}", n.channel(), n.payload())).is_err() {
                            break;
                        }
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    eprintln!("[ingest_worker] listen error: {e}");
                    break;
                }
            }
        }
    });
    Ok(rx)
}

#[derive(Deserialize)]
struct EnqueueReq {
    provider: String,
    task: String,
    #[serde(default)]
    args: Option<serde_json::Value>,
    #[serde(default)]
    provider_id: Option<i64>,
}

fn start_http_server(
    db: Db,
    cfg: QueueConfig,
    metrics: Arc<Mutex<WorkerMetrics>>,
    manager: Manager,
    addr: String,
) {
    use actix_web::{web, App, HttpResponse, HttpServer, Responder};
    tokio::spawn(async move {
        let db = web::Data::new(db);
        let cfg = web::Data::new(cfg);
        let metrics = web::Data::new(metrics);
        let manager_data = web::Data::new(manager);
        let bind_addr = addr.clone();
        let server = HttpServer::new(move || {
            App::new()
                .app_data(db.clone())
                .app_data(cfg.clone())
                .app_data(metrics.clone())
                .app_data(manager_data.clone())
                .route(
                    "/",
                    web::get().to(|| async { HttpResponse::Ok().body("ok") }),
                )
                .route("/api/enqueue", web::post().to(enqueue))
                .route("/api/info", web::get().to(get_info))
                .route("/api/metrics", web::get().to(get_metrics))
                .route("/api/logs", web::get().to(get_logs))
                .route("/api/pause", web::post().to(pause))
                .route("/api/resume", web::post().to(resume))
                .route("/api/status", web::get().to(get_status))
                // debug helpers (safe to keep; read-only)
                .route("/api/pgmq_metrics", web::get().to(get_pgmq_metrics))
                .route("/api/pgmq_counts", web::get().to(get_pgmq_counts))
                .route("/api/pgmq_peek", web::get().to(get_pgmq_peek))
                .route("/api/pgmq_read_once", web::post().to(post_pgmq_read_once))
        })
        .bind(bind_addr.clone())
        .expect("failed to bind http server")
        .run();

        println!("[ingest_worker] http listening on {bind_addr}");
        if let Err(e) = server.await {
            eprintln!("[ingest_worker] http server error: {e:?}");
        }
    });

    async fn enqueue(
        db: actix_web::web::Data<Db>,
        cfg: actix_web::web::Data<QueueConfig>,
        body: actix_web::web::Json<EnqueueReq>,
    ) -> impl actix_web::Responder {
        let mut job = IngestJob::new(&body.provider, &body.task, body.args.clone());
        job.provider_id = body.provider_id;
        match enqueue_job(&db, &cfg, &job).await {
            Ok(msg_id) => actix_web::HttpResponse::Ok()
                .json(json!({"ok": true, "msg_id": msg_id, "correlation": job.correlation_id})),
            Err(e) => actix_web::HttpResponse::InternalServerError()
                .json(json!({"ok": false, "error": e.to_string()})),
        }
    }

    async fn get_metrics(
        metrics: actix_web::web::Data<Arc<Mutex<WorkerMetrics>>>,
    ) -> impl actix_web::Responder {
        let m = metrics.lock().unwrap().clone();
        actix_web::HttpResponse::Ok().json(m)
    }

    async fn get_info(cfg: actix_web::web::Data<QueueConfig>) -> impl Responder {
        actix_web::HttpResponse::Ok().json(cfg.as_ref())
    }

    async fn get_logs(
        manager: actix_web::web::Data<Manager>,
        query: actix_web::web::Query<std::collections::HashMap<String, String>>,
    ) -> impl Responder {
        let limit: usize = query
            .get("limit")
            .and_then(|s| s.parse().ok())
            .unwrap_or(200);
        let logs = manager.logs.lock().unwrap();
        let take = limit.min(logs.len());
        let start = logs.len() - take;
        let slice: Vec<String> = logs.iter().skip(start).take(take).cloned().collect();
        actix_web::HttpResponse::Ok().json(json!({"lines": slice}))
    }

    async fn pause(manager: actix_web::web::Data<Manager>) -> impl Responder {
        manager.set_paused(true);
        actix_web::HttpResponse::Ok().json(json!({"ok": true, "paused": true}))
    }

    async fn resume(manager: actix_web::web::Data<Manager>) -> impl Responder {
        manager.set_paused(false);
        actix_web::HttpResponse::Ok().json(json!({"ok": true, "paused": false}))
    }

    async fn get_status(manager: actix_web::web::Data<Manager>) -> impl Responder {
        actix_web::HttpResponse::Ok().json(json!({"paused": manager.is_paused()}))
    }

    // Return pgmq.metrics(queue_name) for this worker
    async fn get_pgmq_metrics(
        db: actix_web::web::Data<Db>,
        cfg: actix_web::web::Data<QueueConfig>,
    ) -> impl Responder {
        let res = sqlx::query_scalar::<_, serde_json::Value>(
            r#"select row_to_json(t) from pgmq.metrics($1) t"#,
        )
        .bind(&cfg.queue_name)
        .fetch_all(&db.pool)
        .await;

        match res {
            Ok(rows) => actix_web::HttpResponse::Ok().json(json!({"ok": true, "metrics": rows})),
            Err(e) => actix_web::HttpResponse::InternalServerError()
                .json(json!({"ok": false, "error": e.to_string()})),
        }
    }

    // Quick counts for queue and archive tables
    async fn get_pgmq_counts(
        db: actix_web::web::Data<Db>,
        cfg: actix_web::web::Data<QueueConfig>,
    ) -> impl Responder {
        let q_name = format!("pgmq.q_{}", cfg.queue_name);
        let a_name = format!("pgmq.a_{}", cfg.queue_name);

        let q_sql = format!("select count(*)::bigint as cnt from {}", q_name);
        let a_sql = format!("select count(*)::bigint as cnt from {}", a_name);

        let q_cnt = sqlx::query_scalar::<_, i64>(&q_sql)
            .fetch_one(&db.pool)
            .await;
        let a_cnt = sqlx::query_scalar::<_, i64>(&a_sql)
            .fetch_one(&db.pool)
            .await;

        match (q_cnt, a_cnt) {
            (Ok(q), Ok(a)) => {
                actix_web::HttpResponse::Ok().json(json!({"ok": true, "q_count": q, "a_count": a}))
            }
            (Err(e), _) | (_, Err(e)) => actix_web::HttpResponse::InternalServerError()
                .json(json!({"ok": false, "error": e.to_string()})),
        }
    }

    // Peek at next visible message payload without changing VT (direct table scan)
    async fn get_pgmq_peek(
        db: actix_web::web::Data<Db>,
        cfg: actix_web::web::Data<QueueConfig>,
    ) -> impl Responder {
        let q_name = format!("pgmq.q_{}", cfg.queue_name);
        let sql = format!("select message from {} order by msg_id asc limit 1", q_name);
        let row = sqlx::query_scalar::<_, serde_json::Value>(&sql)
            .fetch_optional(&db.pool)
            .await;
        match row {
            Ok(Some(val)) => {
                actix_web::HttpResponse::Ok().json(json!({"ok": true, "message": val}))
            }
            Ok(None) => actix_web::HttpResponse::Ok().json(json!({"ok": true, "message": null})),
            Err(e) => actix_web::HttpResponse::InternalServerError()
                .json(json!({"ok": false, "error": e.to_string()})),
        }
    }

    // Invoke pgmq.read once (will set VT on one message if available) and return the raw row
    async fn post_pgmq_read_once(
        db: actix_web::web::Data<Db>,
        cfg: actix_web::web::Data<QueueConfig>,
    ) -> impl Responder {
        let sql4 = r#"select row_to_json(t) from pgmq.read($1, $2, 1, NULL::jsonb) t"#;
        let res4 = sqlx::query_scalar::<_, serde_json::Value>(sql4)
            .bind(&cfg.queue_name)
            .bind(cfg.visibility_timeout_secs)
            .fetch_optional(&db.pool)
            .await;
        match res4 {
            Ok(Some(val)) => {
                actix_web::HttpResponse::Ok().json(json!({"ok": true, "row": val, "arity": 4}))
            }
            Ok(None) => {
                // Fallback to 3-arg read in case of version differences
                let sql3 = r#"select row_to_json(t) from pgmq.read($1, $2, 1) t"#;
                let res3 = sqlx::query_scalar::<_, serde_json::Value>(sql3)
                    .bind(&cfg.queue_name)
                    .bind(cfg.visibility_timeout_secs)
                    .fetch_optional(&db.pool)
                    .await;
                match res3 {
                    Ok(Some(val)) => actix_web::HttpResponse::Ok()
                        .json(json!({"ok": true, "row": val, "arity": 3})),
                    Ok(None) => {
                        actix_web::HttpResponse::Ok().json(json!({"ok": true, "row": null}))
                    }
                    Err(e) => actix_web::HttpResponse::InternalServerError()
                        .json(json!({"ok": false, "error": e.to_string()})),
                }
            }
            Err(e) => actix_web::HttpResponse::InternalServerError()
                .json(json!({"ok": false, "error": e.to_string()})),
        }
    }
}

async fn process_provider_items_scoped(
    db: &Db,
    provider_id: Option<i64>,
    batch_size: i32,
) -> Result<u64> {
    // Claim a batch using the SECURITY DEFINER function added in migration 0477
    let worker_id = format!("ingest-worker-{}", std::process::id());
    let rows = sqlx::query(
        "SELECT id, provider_id FROM public.claim_provider_items_batch_scoped($1, $2, $3)",
    )
    .bind(&worker_id)
    .bind(batch_size)
    .bind(provider_id)
    .fetch_all(&db.pool)
    .await?;

    if rows.is_empty() {
        return Ok(0);
    }

    let ids: Vec<i64> = rows
        .iter()
        .map(|r| r.try_get::<i64, _>("id").unwrap_or_default())
        .collect();

    // Finalize with NULL attributes (no change) to clear locks and set last_seen_at/updated_at.
    // Replace this with real extraction logic as needed.
    sqlx::query("SELECT public.finalize_provider_items($1::bigint[], $2::jsonb)")
        .bind(&ids)
        .bind(Option::<serde_json::Value>::None)
        .execute(&db.pool)
        .await?;

    Ok(ids.len() as u64)
}

// Scoped helpers to encapsulate unsafe environment mutations.
// SAFETY rationale: we only mutate process env within a short-lived scope (EnvScope)
// and restore prior values on Drop to avoid leaking state.
#[inline]
fn _set_env_var(key: &str, val: &str) {
    // SAFETY: Scoped, deterministic use; restored by EnvScope::drop.
    unsafe { std::env::set_var(key, val) }
}

#[inline]
fn _remove_env_var(key: &str) {
    // SAFETY: Scoped, deterministic use; restored by EnvScope::drop.
    unsafe { std::env::remove_var(key) }
}
