use actix_web::{web, App, HttpResponse, HttpServer, Responder};
use anyhow::{anyhow, Context, Result};
use dotenv::dotenv; // kept for local use, but we call through env_boot::ensure_dotenv()
use futures::future::join_all;
use i_miss_rust::util::env as env_util;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::env;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use tokio::process::Command;
use tokio::time::{sleep, Duration};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct WorkerSpec {
    name: String,
    queue: String,
    notify_channel: String,
    addr: String, // host:port for worker HTTP
}

impl WorkerSpec {
    fn from_tuple(name: &str, queue: &str, port: u16) -> Self {
        let addr = format!("127.0.0.1:{}", port);
        Self {
            name: name.to_string(),
            queue: queue.to_string(),
            notify_channel: queue.to_string(),
            addr,
        }
    }
}

#[derive(Default)]
struct ProcHandle {
    child: Option<tokio::process::Child>,
}

struct ManagerState {
    specs: Vec<WorkerSpec>,
    procs: Mutex<HashMap<String, ProcHandle>>, // name -> process handle
    http: Client,
}

impl ManagerState {
    fn new(specs: Vec<WorkerSpec>) -> Self {
        let http = Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();
        Self {
            specs,
            procs: Mutex::new(HashMap::new()),
            http,
        }
    }

    fn find_spec(&self, name: &str) -> Option<WorkerSpec> {
        self.specs.iter().find(|s| s.name == name).cloned()
    }
}

#[derive(Serialize)]
struct WorkerSnapshot {
    name: String,
    queue: String,
    addr: String,
    running: bool,
    info: serde_json::Value,
    metrics: serde_json::Value,
}

/// Library entrypoint: run the worker manager with env-configured settings.
pub async fn run_from_env() -> Result<()> {
    i_miss_rust::env_boot::ensure_dotenv();

    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .compact()
        .try_init();

    // Parse worker set: MANAGER_WORKERS="default_ingest:9025,psstore_ingest:9081,igdb_catalog:9082,gb_catalog:9083,steam_ingest:9084,itad_pricing:9085,nexarda_ingest:9086,xbox_ingest:9087"
    let workers_env = env::var("MANAGER_WORKERS").unwrap_or_else(|_| {
        // Sensible defaults covering our main providers; override via env as needed.
        "default_ingest:9025,psstore_ingest:9081,igdb_catalog:9082,gb_catalog:9083,steam_ingest:9084,itad_pricing:9085,nexarda_ingest:9086,xbox_ingest:9087,rawg_ingest:9088,tgdb_ingest:9089".to_string()
    });
    let mut specs: Vec<WorkerSpec> = Vec::new();
    for part in workers_env
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
    {
        if let Some((queue, port_str)) = part.split_once(':') {
            let port: u16 = port_str.parse().unwrap_or(0);
            if port > 0 {
                let name = queue.to_string();
                specs.push(WorkerSpec::from_tuple(&name, queue, port));
            }
        }
    }
    if specs.is_empty() {
        specs.push(WorkerSpec::from_tuple(
            "default_ingest",
            "default_ingest",
            9025,
        ));
    }

    let state = web::Data::new(Arc::new(ManagerState::new(specs)));

    // Optional: autostart all configured workers when MANAGER_AUTOSTART=1
    if env::var("MANAGER_AUTOSTART").ok().as_deref() == Some("1") {
        let state_clone = state.clone();
        tokio::spawn(async move {
            // small delay to let the HTTP server bind cleanly before starting workers
            sleep(Duration::from_millis(200)).await;
            for spec in &state_clone.specs {
                // best-effort: if already running, skip
                if state_clone
                    .http
                    .get(format!("http://{}/api/info", spec.addr))
                    .send()
                    .await
                    .is_ok()
                {
                    continue;
                }
                // spawn worker process similar to start_worker
                let mut cmd = Command::new("target/debug/ingest_worker");
                cmd.env("INGEST_QUEUE_NAME", &spec.queue)
                    .env("INGEST_NOTIFY_CHANNEL", &spec.notify_channel)
                    .env("WORKER_HTTP_ADDR", &spec.addr)
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .kill_on_drop(true);
                let _ = cmd.spawn();
                // no insertion into procs map here to keep this fire-and-forget
                // clients can still manage via /start which updates the handle map
                sleep(Duration::from_millis(50)).await;
            }

            // Optional auto-seed phase: enqueue initial backlog tasks if queue empty.
            // Enabled when MANAGER_AUTO_SEED=1. Intentionally runs after workers start.
            if env::var("MANAGER_AUTO_SEED").ok().as_deref() == Some("1") {
                // Optional periodic schedule: if interval is set, keep seeding when queues are idle.
                // Default is 0 (seed once only).
                let interval_secs: u64 = env::var("MANAGER_AUTO_SEED_INTERVAL_SECS")
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(0);

                // wait a bit longer for workers to bind their HTTP servers
                sleep(Duration::from_secs(1)).await;

                loop {
                    for spec in &state_clone.specs {
                        let base = format!("http://{}", spec.addr);

                        // Check pgmq metrics to see if queue already has visible/inflight jobs
                        let metrics_url = format!("{}/api/pgmq_metrics", base);
                        let mut should_seed = true;
                        if let Ok(resp) = state_clone.http.get(&metrics_url).send().await {
                            if let Ok(val) = resp.json::<serde_json::Value>().await {
                                // val shape: { ok: true, metrics: [ {"queue_name":"...", ... } ] }
                                let row = val.get("metrics").and_then(|m| m.get(0));
                                let visible = row
                                    .and_then(|r| r.get("visible"))
                                    .and_then(|v| v.as_u64())
                                    .unwrap_or(0);
                                let inflight = row
                                    .and_then(|r| {
                                        r.get("inflight")
                                            .or_else(|| r.get("in_flight"))
                                            .or_else(|| r.get("in_flight_messages"))
                                            .or_else(|| r.get("in_flight_count"))
                                    })
                                    .and_then(|v| v.as_u64())
                                    .unwrap_or(0);

                                if visible > 0 || inflight > 0 {
                                    should_seed = false;
                                }
                            }
                        }
                        if !should_seed {
                            continue;
                        }

                        // Derive provider & task seeds from worker name
                        let (provider, task, args) = match spec.name.as_str() {
                            // PlayStation store catalog (broad product ingestion)
                            "psstore_ingest" => (
                                "psstore",
                                "catalog",
                                json!({
                                    "pages": env::var("SEED_PS_PAGES").ok().and_then(|s| s.parse::<u64>().ok()).unwrap_or(50),
                                    "page_size": env::var("SEED_PS_PAGE_SIZE").ok().and_then(|s| s.parse::<u64>().ok()).unwrap_or(250),
                                    "page_concurrency": env::var("SEED_PS_CONCURRENCY").ok().and_then(|s| s.parse::<u64>().ok()).unwrap_or(3),
                                    "rps": env::var("SEED_PS_RPS").ok().and_then(|s| s.parse::<u64>().ok()).unwrap_or(4)
                                }),
                            ),
                            // IGDB (default: top-monthly unless overridden)
                            "igdb_catalog" => (
                                "igdb",
                                "catalog",
                                json!({
                                    "page_size": env::var("SEED_IGDB_PAGE_SIZE").ok().and_then(|s| s.parse::<u64>().ok()).unwrap_or(500),
                                    "max_pages": env::var("SEED_IGDB_MAX_PAGES").ok().and_then(|s| s.parse::<u64>().ok()).unwrap_or(40),
                                    "mode": env::var("SEED_IGDB_MODE").unwrap_or_else(|_| "top-monthly".to_string())
                                }),
                            ),
                            // GiantBomb JSON file ingest (one-shot); limit controllable
                            "gb_catalog" => (
                                "giantbomb",
                                "json",
                                json!({
                                    "limit": env::var("SEED_GB_LIMIT").ok().and_then(|s| s.parse::<u64>().ok()).unwrap_or(4000)
                                }),
                            ),
                            // Steam catalog (run provider environment logic)
                            "steam_ingest" => ("steam", "catalog", json!({})),
                            // Nexarda catalog ingest
                            "nexarda_ingest" => (
                                "nexarda",
                                "catalog",
                                json!({
                                    "reqs_per_min": env::var("SEED_NEXARDA_REQS_PER_MIN").ok().and_then(|s| s.parse::<u64>().ok()).unwrap_or(30),
                                    "concurrency": env::var("SEED_NEXARDA_CONCURRENCY").ok().and_then(|s| s.parse::<u64>().ok()).unwrap_or(1)
                                }),
                            ),
                            // Xbox catalog (all)
                            "xbox_ingest" => (
                                "xbox",
                                "catalog",
                                json!({
                                    "chunk_size": env::var("SEED_XBOX_CHUNK_SIZE").ok().and_then(|s| s.parse::<u64>().ok()).unwrap_or(100),
                                    "reqs_per_min": env::var("SEED_XBOX_REQS_PER_MIN").ok().and_then(|s| s.parse::<u64>().ok()).unwrap_or(20)
                                }),
                            ),
                            // RAWG catalog/toplists (mode optional; defaults to range)
                            "rawg_ingest" => (
                                "rawg",
                                "catalog",
                                json!({
                                    "year_min": env::var("SEED_RAWG_YEAR_MIN").ok().and_then(|s| s.parse::<i64>().ok()).unwrap_or(2015),
                                    "year_max": env::var("SEED_RAWG_YEAR_MAX").ok().and_then(|s| s.parse::<i64>().ok()).unwrap_or(2025),
                                    "page_size": env::var("SEED_RAWG_PAGE_SIZE").ok().and_then(|s| s.parse::<u64>().ok()).unwrap_or(40),
                                    "mode": env::var("SEED_RAWG_MODE").unwrap_or_else(|_| "range".to_string())
                                }),
                            ),
                            // TheGamesDB mirror
                            "tgdb_ingest" => (
                                "tgdb",
                                "catalog",
                                json!({
                                    "year_min": env::var("SEED_TGDB_YEAR_MIN").ok().and_then(|s| s.parse::<i64>().ok()).unwrap_or(2015),
                                    "year_max": env::var("SEED_TGDB_YEAR_MAX").ok().and_then(|s| s.parse::<i64>().ok()).unwrap_or(2025),
                                    "page_size": env::var("SEED_TGDB_PAGE_SIZE").ok().and_then(|s| s.parse::<u64>().ok()).unwrap_or(50)
                                }),
                            ),
                            // Itad pricing
                            "itad_pricing" => (
                                "itad",
                                "prices_scan",
                                json!({
                                    "deals_limit": env::var("SEED_ITAD_DEALS_LIMIT").ok().and_then(|s| s.parse::<u64>().ok()).unwrap_or(50)
                                }),
                            ),
                            // default fallback
                            _ => (spec.name.as_str(), "catalog", json!({})),
                        };

                        // Enqueue
                        let enqueue_url = format!("{}/api/enqueue", base);
                        let payload = json!({"provider": provider, "task": task, "args": args});
                        if let Err(err) = state_clone
                            .http
                            .post(&enqueue_url)
                            .json(&payload)
                            .send()
                            .await
                        {
                            eprintln!(
                                "[manager] seed enqueue failed worker={} err={err}",
                                spec.name
                            );
                        } else {
                            println!(
                                "[manager] seeded worker={} provider={} task={}",
                                spec.name, provider, task
                            );
                        }
                    }

                    if interval_secs == 0 {
                        break;
                    }
                    sleep(Duration::from_secs(interval_secs)).await;
                }
            }
        });
    }

    // HTTP server
    let addr = env::var("MANAGER_HTTP_ADDR")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "127.0.0.1:9090".to_string());
    println!("[multiworker] manager listening on {}", addr);
    HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            .route(
                "/",
                web::get().to(|| async { HttpResponse::Ok().body("manager-ok") }),
            )
            .route("/manager/workers", web::get().to(list_workers))
            .route(
                "/manager/workers/{name}/start",
                web::post().to(start_worker),
            )
            .route("/manager/workers/{name}/stop", web::post().to(stop_worker))
            .route(
                "/manager/workers/{name}/restart",
                web::post().to(restart_worker),
            )
            .route(
                "/manager/workers/{name}/logs",
                web::get().to(get_worker_logs),
            )
            .route("/manager/logs", web::get().to(get_all_logs))
            .route("/manager/enqueue", web::post().to(enqueue_via_worker))
            .route(
                "/manager/enqueue_by_provider",
                web::post().to(enqueue_by_provider),
            )
            .route(
                "/manager/enqueue_by_provider",
                web::get().to(enqueue_by_provider_get),
            )
    })
    .bind(addr)?
    .run()
    .await
    .context("manager http")
}

#[tokio::main]
async fn main() -> Result<()> {
    env_util::bootstrap_cli("worker_manager");
    run_from_env().await
}

async fn list_workers(state: web::Data<Arc<ManagerState>>) -> impl Responder {
    let mut out: Vec<WorkerSnapshot> = Vec::new();
    for spec in &state.specs {
        let base = format!("http://{}", spec.addr);
        let (running, info, metrics) = query_worker(&state.http, &base).await;
        out.push(WorkerSnapshot {
            name: spec.name.clone(),
            queue: spec.queue.clone(),
            addr: spec.addr.clone(),
            running,
            info,
            metrics,
        });
    }
    HttpResponse::Ok().json(out)
}

async fn query_worker(http: &Client, base: &str) -> (bool, serde_json::Value, serde_json::Value) {
    let info = http.get(format!("{}/api/info", base)).send().await;
    let running = info.is_ok();
    let info_json = match info {
        Ok(r) => r
            .json::<serde_json::Value>()
            .await
            .unwrap_or_else(|_| json!({"ok": false})),
        Err(_) => json!({"ok": false}),
    };
    // replaced: metrics_json parsed below with explicit match
    let metrics_resp = http.get(format!("{}/api/metrics", base)).send().await;
    let metrics_json = match metrics_resp {
        Ok(r) => r
            .json::<serde_json::Value>()
            .await
            .unwrap_or_else(|_| json!({"ok": false})),
        Err(_) => json!({"ok": false}),
    };
    (running, info_json, metrics_json)
}

async fn start_worker(
    path: web::Path<(String,)>,
    state: web::Data<Arc<ManagerState>>,
) -> impl Responder {
    let name = &path.0;
    let Some(spec) = state.find_spec(name) else {
        return HttpResponse::NotFound().json(json!({"ok": false, "error":"unknown worker"}));
    };
    // If already reachable, report success
    if state
        .http
        .get(format!("http://{}/api/info", spec.addr))
        .send()
        .await
        .is_ok()
    {
        return HttpResponse::Ok().json(json!({"ok": true, "already_running": true}));
    }

    // Try to spawn target/debug/ingest_worker
    let bin_path = env::current_dir()
        .ok()
        .map(|p| p.join("target").join("debug").join("ingest_worker"));
    let candidate = bin_path.as_ref().and_then(|p| p.to_str());
    let cmd_path = candidate.unwrap_or("ingest_worker");

    let mut cmd = Command::new(cmd_path);
    cmd.env("INGEST_QUEUE_NAME", &spec.queue)
        .env("INGEST_NOTIFY_CHANNEL", &spec.notify_channel)
        .env("WORKER_HTTP_ADDR", &spec.addr)
        // Propagate DB/session URLs explicitly (inheritance should handle this, but make it robust)
        .envs({
            let mut m = std::collections::HashMap::new();
            if let Ok(v) = env::var("SUPABASE_IPV6_DB") {
                m.insert("SUPABASE_IPV6_DB".to_string(), v);
            }
            if let Ok(v) = env::var("V6_HOST") {
                m.insert("V6_HOST".to_string(), v);
            }
            if let Ok(v) = env::var("V6_USER") {
                m.insert("V6_USER".to_string(), v);
            }
            if let Ok(v) = env::var("V6_PASSWORD") {
                m.insert("V6_PASSWORD".to_string(), v);
            }
            if let Ok(v) = env::var("V6_DATABASE") {
                m.insert("V6_DATABASE".to_string(), v);
            }
            if let Ok(v) = env::var("V6_PORT") {
                m.insert("V6_PORT".to_string(), v);
            }
            if let Ok(v) = env::var("SUPABASE_DB_URL") {
                m.insert("SUPABASE_DB_URL".to_string(), v);
            }
            if let Ok(v) = env::var("DATABASE_URL") {
                m.insert("DATABASE_URL".to_string(), v);
            }
            if let Ok(v) = env::var("SUPABASE_DB_SESSION_URL") {
                m.insert("SUPABASE_DB_SESSION_URL".to_string(), v);
            }
            m
        })
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    let child = match cmd.spawn() {
        Ok(ch) => ch,
        Err(e) => {
            return HttpResponse::InternalServerError()
                .json(json!({"ok": false, "error": format!("spawn failed: {}", e)}));
        }
    };

    {
        let mut procs = state.procs.lock().unwrap();
        procs.insert(spec.name.clone(), ProcHandle { child: Some(child) });
    }

    // Wait a moment and check
    sleep(Duration::from_millis(800)).await;
    let ok = state
        .http
        .get(format!("http://{}/api/info", spec.addr))
        .send()
        .await
        .is_ok();
    HttpResponse::Ok().json(json!({"ok": ok, "addr": spec.addr}))
}

async fn stop_worker(
    path: web::Path<(String,)>,
    state: web::Data<Arc<ManagerState>>,
) -> impl Responder {
    let name = &path.0;
    let Some(spec) = state.find_spec(name) else {
        return HttpResponse::NotFound().json(json!({"ok": false, "error":"unknown worker"}));
    };
    let mut ok = false;
    {
        let mut procs = state.procs.lock().unwrap();
        if let Some(handle) = procs.get_mut(&spec.name) {
            if let Some(child) = handle.child.as_mut() {
                let _ = child.kill().await;
                ok = true;
            }
            handle.child = None;
        }
    }
    // Best-effort pause if still reachable
    let _ = state
        .http
        .post(format!("http://{}/api/pause", spec.addr))
        .send()
        .await;
    HttpResponse::Ok().json(json!({"ok": ok}))
}

#[derive(Deserialize)]
struct EnqueueBody {
    name: String,
    provider: String,
    task: String,
    #[serde(default)]
    args: Option<serde_json::Value>,
    #[serde(default)]
    provider_id: Option<i64>,
}

async fn enqueue_via_worker(
    body: web::Json<EnqueueBody>,
    state: web::Data<Arc<ManagerState>>,
) -> impl Responder {
    let Some(spec) = state.find_spec(&body.name) else {
        return HttpResponse::NotFound().json(json!({"ok": false, "error":"unknown worker"}));
    };
    let base = format!("http://{}", spec.addr);
    let resp = state
        .http
        .post(format!("{}/api/enqueue", base))
        .json(&json!({
            "provider": body.provider,
            "task": body.task,
            "args": body.args,
            "provider_id": body.provider_id,
        }))
        .send()
        .await;
    match resp {
        Ok(r) => match r.json::<serde_json::Value>().await {
            Ok(v) => HttpResponse::Ok().json(v),
            Err(e) => HttpResponse::InternalServerError()
                .json(json!({"ok": false, "error": e.to_string()})),
        },
        Err(e) => {
            HttpResponse::InternalServerError().json(json!({"ok": false, "error": e.to_string()}))
        }
    }
}

#[derive(Deserialize)]
struct EnqueueByProviderBody {
    provider: String,
    task: String,
    #[serde(default)]
    args: Option<serde_json::Value>,
    #[serde(default)]
    provider_id: Option<i64>,
}

fn provider_to_worker_name(p: &str) -> Option<&'static str> {
    // normalize: lowercase, split on non-alnum markers (":", "_", "-") and pick the first token
    let p = p.to_ascii_lowercase();
    let base = p
        .split(|c: char| (c == ':' || c == '_' || c == '-'))
        .next()
        .unwrap_or("");
    match base {
        "ps" | "psn" | "playstation" | "psstore" => Some("psstore_ingest"),
        "igdb" => Some("igdb_catalog"),
        "gb" | "giantbomb" | "giant" | "bomb" => Some("gb_catalog"),
        "steam" => Some("steam_ingest"),
        "itad" | "isthereanydeal" => Some("itad_pricing"),
        "nexarda" => Some("nexarda_ingest"),
        "xbox" | "msstore" | "microsoft" => Some("xbox_ingest"),
        "rawg" => Some("rawg_ingest"),
        "tgdb" | "thegamesdb" => Some("tgdb_ingest"),
        _ => None,
    }
}

async fn enqueue_by_provider(
    body: web::Json<EnqueueByProviderBody>,
    state: web::Data<Arc<ManagerState>>,
) -> impl Responder {
    let Some(worker_name) = provider_to_worker_name(&body.provider) else {
        return HttpResponse::BadRequest()
            .json(json!({"ok": false, "error": "unknown provider -> worker mapping"}));
    };
    let Some(spec) = state.find_spec(worker_name) else {
        return HttpResponse::NotFound()
            .json(json!({"ok": false, "error": "worker not configured for provider"}));
    };
    let base = format!("http://{}", spec.addr);
    let resp = state
        .http
        .post(format!("{}/api/enqueue", base))
        .json(&json!({
            "provider": body.provider,
            "task": body.task,
            "args": body.args,
            "provider_id": body.provider_id,
        }))
        .send()
        .await;
    match resp {
        Ok(r) => match r.json::<serde_json::Value>().await {
            Ok(v) => HttpResponse::Ok().json(v),
            Err(e) => HttpResponse::InternalServerError()
                .json(json!({"ok": false, "error": e.to_string()})),
        },
        Err(e) => {
            HttpResponse::InternalServerError().json(json!({"ok": false, "error": e.to_string()}))
        }
    }
}

// GET version for tooling symmetry: /manager/enqueue_by_provider?provider=itad&task=prices_scan&args=%7B...%7D&provider_id=123
async fn enqueue_by_provider_get(
    query: web::Query<HashMap<String, String>>,
    state: web::Data<Arc<ManagerState>>,
) -> impl Responder {
    let provider = match query.get("provider").cloned() {
        Some(p) => p,
        None => {
            return HttpResponse::BadRequest()
                .json(json!({"ok": false, "error": "missing provider"}));
        }
    };
    let task = match query.get("task").cloned() {
        Some(t) => t,
        None => {
            return HttpResponse::BadRequest().json(json!({"ok": false, "error": "missing task"}));
        }
    };
    let args_val = query
        .get("args")
        .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok());
    let provider_id = query.get("provider_id").and_then(|s| s.parse::<i64>().ok());
    let Some(worker_name) = provider_to_worker_name(&provider) else {
        return HttpResponse::BadRequest()
            .json(json!({"ok": false, "error": "unknown provider -> worker mapping"}));
    };
    let Some(spec) = state.find_spec(worker_name) else {
        return HttpResponse::NotFound()
            .json(json!({"ok": false, "error": "worker not configured for provider"}));
    };
    let base = format!("http://{}", spec.addr);
    let resp = state
        .http
        .post(format!("{}/api/enqueue", base))
        .json(&json!({
            "provider": provider,
            "task": task,
            "args": args_val,
            "provider_id": provider_id,
        }))
        .send()
        .await;
    match resp {
        Ok(r) => match r.json::<serde_json::Value>().await {
            Ok(v) => HttpResponse::Ok().json(v),
            Err(e) => HttpResponse::InternalServerError()
                .json(json!({"ok": false, "error": e.to_string()})),
        },
        Err(e) => {
            HttpResponse::InternalServerError().json(json!({"ok": false, "error": e.to_string()}))
        }
    }
}

async fn get_worker_logs(
    path: web::Path<(String,)>,
    query: web::Query<HashMap<String, String>>,
    state: web::Data<Arc<ManagerState>>,
) -> impl Responder {
    let name = &path.0;
    let Some(spec) = state.find_spec(name) else {
        return HttpResponse::NotFound().json(json!({"ok": false, "error":"unknown worker"}));
    };
    let limit = query
        .get("limit")
        .cloned()
        .unwrap_or_else(|| "200".to_string());
    let url = format!("http://{}/api/logs?limit={}", spec.addr, limit);
    let resp = state.http.get(url).send().await;
    match resp {
        Ok(r) => match r.json::<serde_json::Value>().await {
            Ok(v) => HttpResponse::Ok().json(json!({"name": name, "logs": v})),
            Err(e) => HttpResponse::InternalServerError()
                .json(json!({"ok": false, "error": e.to_string()})),
        },
        Err(e) => {
            HttpResponse::InternalServerError().json(json!({"ok": false, "error": e.to_string()}))
        }
    }
}

// GET /manager/logs?limit=N — aggregate logs from all configured workers
async fn get_all_logs(
    query: web::Query<HashMap<String, String>>,
    state: web::Data<Arc<ManagerState>>,
) -> impl Responder {
    let limit: usize = query
        .get("limit")
        .and_then(|s| s.parse().ok())
        .unwrap_or(200);
    let client = Client::new();
    let tasks = state.specs.iter().map(|spec| {
        let client = client.clone();
        let name = spec.name.clone();
        let url = format!("http://{}/api/logs?limit={}", spec.addr, limit);
        async move {
            let body = match client.get(&url).send().await {
                Ok(r) => r
                    .text()
                    .await
                    .unwrap_or_else(|e| format!("{{\"ok\":false,\"error\":\"{}\"}}", e)),
                Err(e) => format!("{{\"ok\":false,\"error\":\"{}\"}}", e),
            };
            (name, body)
        }
    });
    let results: Vec<(String, String)> = join_all(tasks).await;
    let payload: serde_json::Value = serde_json::json!({
        "ok": true,
        "logs": results.into_iter().map(|(name, body)| serde_json::json!({"worker": name, "logs": body})).collect::<Vec<_>>()
    });
    HttpResponse::Ok().json(payload)
}

// POST /manager/workers/{name}/restart — stop then start the worker
async fn restart_worker(
    path: web::Path<(String,)>,
    state: web::Data<Arc<ManagerState>>,
) -> impl Responder {
    let name = path.0.clone();
    // Best-effort stop
    {
        let mut map = state.procs.lock().unwrap();
        if let Some(mut handle) = map.remove(&name) {
            if let Some(mut child) = handle.child.take() {
                // ignore kill errors; process may already have exited
                let _ = child.kill().await;
            }
        }
    }
    // Start anew if spec exists
    match state.find_spec(&name) {
        None => HttpResponse::NotFound()
            .json(serde_json::json!({"ok": false, "error": "unknown worker"})),
        Some(spec) => {
            let mut cmd = Command::new("target/debug/ingest_worker");
            cmd.env("INGEST_QUEUE_NAME", &spec.queue)
                .env("INGEST_NOTIFY_CHANNEL", &spec.notify_channel)
                .env("WORKER_HTTP_ADDR", &spec.addr);
            // Inherit DB envs from manager process
            if let Ok(url) = std::env::var("SUPABASE_DB_URL") {
                cmd.env("SUPABASE_DB_URL", url);
            }
            if let Ok(url) = std::env::var("DATABASE_URL") {
                cmd.env("DATABASE_URL", url);
            }
            if let Ok(url) = std::env::var("SUPABASE_DB_SESSION_URL") {
                cmd.env("SUPABASE_DB_SESSION_URL", url);
            }
            let child = match cmd.spawn() {
                Ok(c) => c,
                Err(e) => {
                    return HttpResponse::InternalServerError().json(
                        serde_json::json!({"ok": false, "error": format!("restart spawn failed: {}", e)})
                    );
                }
            };
            state
                .procs
                .lock()
                .unwrap()
                .insert(name.clone(), ProcHandle { child: Some(child) });
            HttpResponse::Ok().json(serde_json::json!({"ok": true, "restarted": true}))
        }
    }
}
