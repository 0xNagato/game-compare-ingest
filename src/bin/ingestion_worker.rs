use anyhow::{anyhow, Result};
use serde_json::Value;
use sqlx::Row;
use std::{env, ffi::OsStr, sync::Arc, time::Duration};
use tokio::{
    task::JoinSet,
    time::{interval, MissedTickBehavior},
};
use tracing::{debug, error, info};

use i_miss_rust::database_ops::db::Db;
use i_miss_rust::util::env as env_util;

fn set_env_var<K, V>(key: K, value: V)
where
    K: AsRef<OsStr>,
    V: AsRef<OsStr>,
{
    unsafe { std::env::set_var(key, value) }
}

fn remove_env_var<K>(key: K)
where
    K: AsRef<OsStr>,
{
    unsafe { std::env::remove_var(key) }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    i_miss_rust::util::env::bootstrap_cli("ingestion_worker");
    dotenv::dotenv().ok();
    tracing_subscriber::fmt::init();

    let db_url = env_util::db_url_prefer_session()?;
    let worker_id =
        Arc::new(env::var("WORKER_ID").unwrap_or_else(|_| format!("pid-{}", std::process::id())));
    let kinds_filter: Option<Vec<String>> = env::var("JOB_KINDS").ok().map(|s| {
        s.split(|c: char| (c == ',' || c == ' '))
            .filter(|t| !t.is_empty())
            .map(|t| t.trim().to_string())
            .collect()
    });

    let max_concurrency: usize = env::var("WORKER_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1)
        .max(1);
    let poll_interval = Duration::from_secs(
        env::var("WORKER_POLL_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(2),
    );
    let backoff_on_error = Duration::from_secs(
        env::var("WORKER_ERROR_BACKOFF_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(5),
    );

    let pool = Arc::new(
        sqlx::postgres::PgPoolOptions::new()
            .max_connections(((max_concurrency as u32) * 4).max(5))
            .connect(&db_url)
            .await?,
    );
    let db_url = Arc::new(db_url);

    info!(worker_id=%worker_id.as_ref(), concurrency=%max_concurrency, poll_secs=%poll_interval.as_secs(), "ingestion worker started");

    let mut set: JoinSet<()> = JoinSet::new();
    let mut ticker = interval(poll_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        // Try to fill available slots
        while set.len() < max_concurrency {
            match claim_next_job(pool.as_ref(), worker_id.as_ref(), kinds_filter.as_deref()).await {
                Ok(Some(job)) => {
                    let pool = pool.clone();
                    let db_url = db_url.clone();
                    let worker_id = worker_id.clone();
                    set.spawn(async move {
                        let job_id = job.id;
                        let kind = job.kind.clone();
                        let payload = job.payload.clone();
                        info!(job_id, kind=%kind, worker=%worker_id.as_ref(), "processing job");
                        let outcome = process_job(db_url.as_ref(), &kind, &payload).await;
                        match outcome {
                            Ok(_) => {
                                if let Err(db_err) =
                                    complete_job(pool.as_ref(), job_id, true, None).await
                                {
                                    error!(job_id, error=%db_err, "failed to mark job complete");
                                } else {
                                    info!(job_id, kind=%kind, "job completed");
                                }
                            }
                            Err(err) => {
                                error!(job_id, kind=%kind, error=%err, "job failed");
                                let err_str = err.to_string();
                                if let Err(db_err) =
                                    complete_job(pool.as_ref(), job_id, false, Some(&err_str)).await
                                {
                                    error!(job_id, error=%db_err, "failed to record job failure");
                                }
                            }
                        }
                    });
                }
                Ok(None) => {
                    break;
                }
                Err(e) => {
                    error!(error=%e, "error claiming job; backing off");
                    tokio::time::sleep(backoff_on_error).await;
                    break;
                }
            }
        }

        tokio::select! {
            Some(res) = set.join_next(), if !set.is_empty() => {
                if let Err(join_err) = res {
                    error!(error=%join_err, "worker task panicked");
                }
            }
            _ = ticker.tick() => {
                if set.is_empty() {
                    debug!(worker_id=%worker_id.as_ref(), "idle tick");
                }
            }
        }
    }
}

struct JobRow {
    id: i64,
    kind: String,
    payload: Value,
}

async fn claim_next_job(
    pool: &sqlx::PgPool,
    worker_id: &str,
    kinds: Option<&[String]>,
) -> Result<Option<JobRow>> {
    let mut tx = pool.begin().await?;
    let rec = if let Some(kinds) = kinds {
        sqlx::query(
            "SELECT id, kind, payload FROM ingestion_jobs \
             WHERE status='queued' AND scheduled_at <= now() AND kind = ANY($1) \
             ORDER BY priority ASC, scheduled_at ASC, id ASC \
             FOR UPDATE SKIP LOCKED LIMIT 1",
        )
        .bind(kinds)
        .fetch_optional(&mut *tx)
        .await?
    } else {
        sqlx::query(
            "SELECT id, kind, payload FROM ingestion_jobs \
             WHERE status='queued' AND scheduled_at <= now() \
             ORDER BY priority ASC, scheduled_at ASC, id ASC \
             FOR UPDATE SKIP LOCKED LIMIT 1",
        )
        .fetch_optional(&mut *tx)
        .await?
    };

    let Some(row) = rec else {
        tx.rollback().await?;
        return Ok(None);
    };
    let id: i64 = row.get("id");
    sqlx
        ::query(
            "UPDATE ingestion_jobs \
         SET status='running', locked_at=now(), locked_by=$2, attempts=attempts+1, started_at = COALESCE(started_at, now()), updated_at=now() \
         WHERE id=$1"
        )
        .bind(id)
        .bind(worker_id)
        .execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(Some(JobRow {
        id,
        kind: row.get("kind"),
        payload: row.get("payload"),
    }))
}

async fn complete_job(pool: &sqlx::PgPool, id: i64, ok: bool, err: Option<&str>) -> Result<()> {
    if ok {
        sqlx
            ::query(
                "UPDATE ingestion_jobs SET status='done', finished_at=now(), updated_at=now(), last_error=NULL WHERE id=$1"
            )
            .bind(id)
            .execute(pool).await?;
    } else {
        // Retry with simple backoff: attempts * 60 seconds
        sqlx
            ::query(
                "UPDATE ingestion_jobs \
             SET status=CASE WHEN attempts < max_attempts THEN 'queued' ELSE 'failed' END, \
                 finished_at=CASE WHEN attempts >= max_attempts THEN now() ELSE NULL END, \
                 scheduled_at = CASE WHEN attempts < max_attempts THEN now() + make_interval(secs => (attempts*60)) ELSE scheduled_at END, \
                 last_error=$2, updated_at=now() \
             WHERE id=$1"
            )
            .bind(id)
            .bind(err)
            .execute(pool).await?;
    }
    Ok(())
}

async fn process_job(db_url: &str, kind: &str, payload: &Value) -> Result<()> {
    match kind {
        // PlayStation Store region ingest: payload expects { region: "en-us", pages?: u32, page_size?: u32, cat_ps4?: str, cat_ps5?: str, sha?: str }
        "psstore.region" | "ps.region" => {
            let region = payload
                .get("region")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("payload.region missing"))?
                .to_lowercase();
            let pages = payload
                .get("pages")
                .and_then(|v| v.as_u64())
                .unwrap_or_else(|| {
                    env::var("PS_MAX_PAGES")
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(1)
                }) as u32;
            let page_size = payload
                .get("page_size")
                .and_then(|v| v.as_u64())
                .unwrap_or_else(|| {
                    env::var("PS_PAGE_SIZE")
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(100)
                }) as u32;
            let cat_ps4 = payload
                .get("cat_ps4")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| {
                    env::var("PS4_CATEGORY")
                        .unwrap_or_else(|_| "44d8bb20-653e-431e-8ad0-c0a365f68d2f".into())
                });
            let cat_ps5 = payload
                .get("cat_ps5")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| {
                    env::var("PS5_CATEGORY")
                        .unwrap_or_else(|_| "4cbf39e2-5749-4970-ba81-93a489e4570c".into())
                });
            let sha = payload
                .get("sha")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| {
                    env::var("PS_HASH").unwrap_or_else(|_| {
                        "9845afc0dbaab4965f6563fffc703f588c8e76792000e8610843b8d3ee9c4c09".into()
                    })
                });

            let regions = vec![region];
            let prev_psstore_sha = std::env::var("PSSTORE_SHA256").ok();
            if !sha.is_empty() {
                set_env_var("PSSTORE_SHA256", &sha);
            }
            let ingest_result = i_miss_rust::database_ops::playstation::prices::ingest_prices(
                db_url, &regions, pages, page_size, &cat_ps4, &cat_ps5,
            )
            .await;
            match (&prev_psstore_sha, &ingest_result) {
                (Some(prev), _) => set_env_var("PSSTORE_SHA256", prev),
                (None, _) => remove_env_var("PSSTORE_SHA256"),
            }
            ingest_result?;
            Ok(())
        }
        "xbox.market" | "xbox.region" | "xbox.ingest" | "microsoft.market" | "microsoft.region" => {
            struct EnvScope {
                prev: Vec<(String, Option<String>)>,
            }
            impl EnvScope {
                fn new() -> Self {
                    Self { prev: Vec::new() }
                }
                fn set(&mut self, key: &str, val: &str) {
                    let prior = std::env::var(key).ok();
                    self.prev.push((key.to_string(), prior));
                    set_env_var(key, val)
                }
            }
            impl Drop for EnvScope {
                fn drop(&mut self) {
                    for (key, val) in self.prev.drain(..) {
                        match val {
                            Some(v) => {
                                set_env_var(&key, v);
                            }
                            None => {
                                remove_env_var(&key);
                            }
                        }
                    }
                }
            }

            let mut envs = EnvScope::new();
            if let Some(market) = payload.get("market").and_then(|v| v.as_str()) {
                envs.set("XBOX_MARKET", market);
            }
            if let Some(language) = payload.get("language").and_then(|v| v.as_str()) {
                envs.set("XBOX_LANGUAGE", language);
            }
            if let Some(ms_cv) = payload.get("ms_cv").and_then(|v| v.as_str()) {
                envs.set("XBOX_MS_CV", ms_cv);
            }
            if let Some(csv) = payload.get("product_ids_csv").and_then(|v| v.as_str()) {
                let parsed = i_miss_rust::database_ops::xbox::provider::parse_product_ids(csv);
                if !parsed.is_empty() {
                    envs.set("XBOX_PRODUCT_IDS", &parsed.join(","));
                }
            }
            if let Some(arr) = payload.get("product_ids").and_then(|v| v.as_array()) {
                let list: Vec<String> = arr
                    .iter()
                    .filter_map(|x| x.as_str().map(|s| s.trim().to_string()))
                    .filter(|s| !s.is_empty())
                    .collect();
                if !list.is_empty() {
                    envs.set("XBOX_PRODUCT_IDS", &list.join(","));
                }
            }
            if let Some(path) = payload.get("product_ids_file").and_then(|v| v.as_str()) {
                envs.set("XBOX_PRODUCT_IDS_FILE", path);
            }
            if let Some(v) = payload.get("dry_run").and_then(|v| v.as_bool()) {
                if v {
                    envs.set("XBOX_DRY_RUN", "1");
                }
            }
            if let Some(v) = payload.get("chunk_size").and_then(|v| v.as_u64()) {
                envs.set("XBOX_CHUNK_SIZE", &v.to_string());
            }
            if let Some(v) = payload.get("chunk_sleep_ms").and_then(|v| v.as_u64()) {
                envs.set("XBOX_CHUNK_SLEEP_MS", &v.to_string());
            }
            if let Some(v) = payload.get("reqs_per_min").and_then(|v| v.as_u64()) {
                envs.set("XBOX_REQS_PER_MIN", &v.to_string());
            }
            if let Some(v) = payload.get("rps").and_then(|v| v.as_f64()) {
                envs.set("XBOX_RPS", &format!("{}", v));
            }
            if let Some(v) = payload.get("max_retries").and_then(|v| v.as_u64()) {
                envs.set("XBOX_MAX_RETRIES", &v.to_string());
            }
            if let Some(v) = payload.get("backoff_ms").and_then(|v| v.as_u64()) {
                envs.set("XBOX_BACKOFF_MS", &v.to_string());
            }

            let max_conns = env::var("XBOX_DB_MAX_CONNS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(8);
            let db = Db::connect(db_url, max_conns).await?;
            let _ = sqlx::query("SET search_path TO public")
                .execute(&db.pool)
                .await;
            i_miss_rust::database_ops::xbox::provider::run_from_env(&db).await?;
            Ok(())
        }
        other => Err(anyhow!("unsupported job kind: {}", other)),
    }
}
