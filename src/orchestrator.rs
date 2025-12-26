use anyhow::{Context, Result};
use std::env;
use tokio::process::{Child, Command};

/// Handle for a spawned background binary (worker/manager)
pub struct ProcHandle {
    pub child: Child,
}

fn inherit_db_env(cmd: &mut Command) {
    if let Ok(v) = env::var("SUPABASE_IPV6_DB") {
        cmd.env("SUPABASE_IPV6_DB", v);
    }
    if let Ok(v) = env::var("V6_HOST") {
        cmd.env("V6_HOST", v);
    }
    if let Ok(v) = env::var("V6_USER") {
        cmd.env("V6_USER", v);
    }
    if let Ok(v) = env::var("V6_PASSWORD") {
        cmd.env("V6_PASSWORD", v);
    }
    if let Ok(v) = env::var("V6_DATABASE") {
        cmd.env("V6_DATABASE", v);
    }
    if let Ok(v) = env::var("V6_PORT") {
        cmd.env("V6_PORT", v);
    }
    if let Ok(v) = env::var("SUPABASE_DB_URL") {
        cmd.env("SUPABASE_DB_URL", v);
    }
    if let Ok(v) = env::var("DATABASE_URL") {
        cmd.env("DATABASE_URL", v);
    }
    if let Ok(v) = env::var("SUPABASE_DB_SESSION_URL") {
        cmd.env("SUPABASE_DB_SESSION_URL", v);
    }
}

/// Spawn the multi-worker manager binary with the provided worker set and addr.
/// managers: e.g., "default_ingest:9025,psstore_ingest:9081"
/// addr: e.g., "127.0.0.1:9090"
pub async fn spawn_worker_manager(managers: &str, addr: &str) -> Result<ProcHandle> {
    let bin = env::var("WORKER_MANAGER_BIN")
        .unwrap_or_else(|_| "target/debug/worker_manager".to_string());
    let mut cmd = Command::new(bin);
    cmd.env("MANAGER_WORKERS", managers)
        .env("MANAGER_HTTP_ADDR", addr);
    inherit_db_env(&mut cmd);
    let child = cmd.spawn().context("failed to spawn worker_manager")?;
    Ok(ProcHandle { child })
}

/// Spawn an ingest worker for a given queue and bind address.
/// queue: e.g., "default_ingest"; addr: e.g., "127.0.0.1:9025"; notify: defaults to queue if None.
pub async fn spawn_ingest_worker(
    queue: &str,
    addr: &str,
    notify: Option<&str>,
) -> Result<ProcHandle> {
    let bin =
        env::var("INGEST_WORKER_BIN").unwrap_or_else(|_| "target/debug/ingest_worker".to_string());
    let mut cmd = Command::new(bin);
    let notify = notify.unwrap_or(queue);
    cmd.env("INGEST_QUEUE_NAME", queue)
        .env("INGEST_NOTIFY_CHANNEL", notify)
        .env("WORKER_HTTP_ADDR", addr);
    inherit_db_env(&mut cmd);
    let child = cmd.spawn().context("failed to spawn ingest_worker")?;
    Ok(ProcHandle { child })
}
