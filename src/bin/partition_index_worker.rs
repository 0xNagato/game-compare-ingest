// =========================================================
// PARTITION INDEX WORKER - Background job processor
// =========================================================
// Purpose: Process partition_index_jobs queue and execute
//          CREATE INDEX CONCURRENTLY (cannot run from PL/pgSQL)
// =========================================================

use anyhow::{Context, Result};
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions, PgSslMode};
use sqlx::Row;
use std::env;
use std::str::FromStr;
use std::time::Duration;
use tokio::time::sleep;

/// Job record from partition_index_jobs table
#[derive(Debug, sqlx::FromRow)]
struct IndexJob {
    id: i64,
    partition_name: String,
    index_type: String,
}

/// Poll interval in seconds (default: 30s)
fn get_poll_interval() -> Duration {
    let secs = env::var("POLL_INTERVAL_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(30);
    Duration::from_secs(secs)
}

/// Get database connection pool
async fn get_db_pool() -> Result<PgPool> {
    let database_url = env::var("DATABASE_URL").context("DATABASE_URL must be set")?;

    // Disable statement cache (avoids named prepared stmt collisions under PgBouncer)
    // and keep pool modest unless overridden.
    let max_conns = env::var("WORKER_MAX_CONNS")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(5);

    // Configure connect options to disable statement cache (avoids prepared stmt name collisions)
    let mut connect_options = PgConnectOptions::from_str(&database_url)
        .unwrap()
        .statement_cache_capacity(0);

    // Ensure TLS is enabled when DSN contains sslmode=require
    if database_url.contains("sslmode=require") && !database_url.contains("sslmode=disable") {
        connect_options = connect_options.ssl_mode(PgSslMode::Require);
    }

    PgPoolOptions::new()
        .max_connections(max_conns)
        .connect_with(connect_options)
        .await
        .context("Failed to connect to database")
}

/// Fetch the next pending job (oldest, not in cooldown)
async fn poll_next_job(pool: &PgPool) -> Result<Option<IndexJob>> {
    let job = sqlx::query_as::<_, IndexJob>(
        r#"
                SELECT id, partition_name, index_type
                FROM partition_util.partition_index_jobs
                WHERE status = 'pending'
                    AND (last_attempt_at IS NULL OR last_attempt_at < now() - interval '5 minutes')
                ORDER BY created_at
                LIMIT 1
                "#,
    )
    .persistent(false) // unnamed statement (safe with transaction pooling)
    .fetch_optional(pool)
    .await
    .context("Failed to poll for pending jobs")?;

    Ok(job)
}

/// Mark job as running
async fn mark_job_running(pool: &PgPool, job_id: i64) -> Result<()> {
    sqlx::query(
        r#"
    UPDATE partition_util.partition_index_jobs
        SET status = 'running',
            attempts = attempts + 1,
            last_attempt_at = now()
        WHERE id = $1
        "#,
    )
    .persistent(false)
    .bind(job_id)
    .execute(pool)
    .await
    .context("Failed to mark job as running")?;

    Ok(())
}

/// Build CREATE INDEX CONCURRENTLY SQL for a job
fn build_index_sql(job: &IndexJob) -> String {
    // Stable, lowercase index name to avoid duplicates across retries
    let index_name = format!("{}_{}_idx", job.partition_name, job.index_type).to_lowercase();
    let table_name = format!("public.{}", job.partition_name);

    match job.index_type.as_str() {
        "brin_recorded" => {
            format!(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS {} ON {} USING brin(recorded_at) WITH (pages_per_range = 128)",
                index_name, table_name
            )
        }
        "btree_series" => {
            format!(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS {} ON {} (offer_jurisdiction_id, recorded_at)",
                index_name, table_name
            )
        }
        // NOTE: Avoid volatile predicates in worker-created indexes; use DESC composite for recency scans.
        "btree_recent" => {
            format!(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS {} ON {} (offer_jurisdiction_id, recorded_at DESC)",
                index_name, table_name
            )
        }
        _ => {
            panic!("Unknown index type: {}", job.index_type);
        }
    }
}

/// Execute CREATE INDEX CONCURRENTLY (dynamic SQL - exception to query!() rule)
async fn execute_index_creation(pool: &PgPool, sql: &str) -> Result<()> {
    // EXCEPTION: Using query() here because CREATE INDEX statement is dynamic
    // (partition names and index types vary). This is documented in CONTRIBUTING.md
    // as an acceptable use case for runtime prepared statements.
    sqlx::query(sql)
        .persistent(false)
        .execute(pool)
        .await
        .context("Failed to create index")?;

    Ok(())
}

/// Mark job as completed
async fn mark_job_completed(pool: &PgPool, job_id: i64) -> Result<()> {
    sqlx::query(
        r#"
    UPDATE partition_util.partition_index_jobs
        SET status = 'completed',
            completed_at = now(),
            error_message = NULL
        WHERE id = $1
        "#,
    )
    .persistent(false)
    .bind(job_id)
    .execute(pool)
    .await
    .context("Failed to mark job as completed")?;

    Ok(())
}

/// Mark job as failed or pending (for retry)
async fn mark_job_failed(pool: &PgPool, job_id: i64, error_msg: &str) -> Result<()> {
    // Get current attempts to decide if we should retry
    let row =
        sqlx::query(r#"SELECT attempts FROM partition_util.partition_index_jobs WHERE id = $1"#)
            .persistent(false)
            .bind(job_id)
            .fetch_one(pool)
            .await?;

    let attempts: i32 = row.get::<i32, _>("attempts");
    let new_status = if attempts >= 3 { "failed" } else { "pending" };

    sqlx::query(
        r#"
    UPDATE partition_util.partition_index_jobs
        SET status = $1,
            error_message = $2
        WHERE id = $3
        "#,
    )
    .persistent(false)
    .bind(new_status)
    .bind(error_msg)
    .bind(job_id)
    .execute(pool)
    .await
    .context("Failed to mark job status")?;

    Ok(())
}

/// Process a single job
async fn process_job(pool: &PgPool, job: IndexJob) -> Result<()> {
    let job_id = job.id;
    let partition = job.partition_name.clone();
    let index_type = job.index_type.clone();

    println!(
        "[INFO] Processing job #{}: {} ({})",
        job_id, partition, index_type
    );

    // Mark as running
    mark_job_running(pool, job_id).await?;

    // Build and execute index creation
    let sql = build_index_sql(&job);
    println!("[INFO] Executing: {}", sql);

    match execute_index_creation(pool, &sql).await {
        Ok(_) => {
            mark_job_completed(pool, job_id).await?;
            println!(
                "[SUCCESS] Job #{} completed: {} ({})",
                job_id, partition, index_type
            );
        }
        Err(e) => {
            let error_msg = format!("{:#}", e);
            println!("[ERROR] Job #{} failed: {}", job_id, error_msg);
            mark_job_failed(pool, job_id, &error_msg).await?;
        }
    }

    Ok(())
}

/// Main worker loop
async fn run_worker(pool: PgPool) -> Result<()> {
    // Ensure schema/tables exist for resiliency (idempotent)
    ensure_partition_job_tables(&pool).await?;
    let poll_interval = get_poll_interval();

    println!("[INFO] Partition Index Worker starting...");
    println!("[INFO] Poll interval: {:?}", poll_interval);
    println!("[INFO] Press Ctrl+C to stop");

    loop {
        // Check for shutdown signal
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!("\n[INFO] Shutdown signal received, exiting gracefully...");
                break;
            }
            result = poll_next_job(&pool) => {
                match result? {
                    Some(job) => {
                        if let Err(e) = process_job(&pool, job).await {
                            eprintln!("[ERROR] Failed to process job: {:#}", e);
                        }
                    }
                    None => {
                        // No pending jobs, sleep before next poll
                        sleep(poll_interval).await;
                    }
                }
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    i_miss_rust::util::env::bootstrap_cli("partition_index_worker");
    // Load environment variables from .env if present
    dotenv::dotenv().ok();

    // Get database connection pool
    let pool = get_db_pool().await?;

    // Run worker loop
    run_worker(pool).await?;

    println!("[INFO] Worker stopped");
    Ok(())
}

/// Ensure required schema and tables exist (idempotent safeguards)
async fn ensure_partition_job_tables(pool: &PgPool) -> Result<()> {
    // Create schema and tables if missing; ignore errors after existence checks
    sqlx::query("CREATE SCHEMA IF NOT EXISTS partition_util")
        .persistent(false)
        .execute(pool)
        .await
        .ok();

    // Jobs table
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS partition_util.partition_index_jobs (
          id              bigserial primary key,
          partition_name  text not null,
          index_type      text not null check (index_type in ('brin_recorded','btree_series','btree_recent')),
          status          text not null default 'pending' check (status in ('pending','running','completed','failed')),
          attempts        int not null default 0,
          last_attempt_at timestamptz,
          completed_at    timestamptz,
          error_message   text,
          created_at      timestamptz not null default now(),
          unique (partition_name, index_type)
        )
        "#
    )
        .persistent(false)
        .execute(pool).await
        .ok();

    // Pending index for quick polling
    sqlx::query(
        r#"CREATE INDEX IF NOT EXISTS partition_index_jobs_pending_idx
           ON partition_util.partition_index_jobs (created_at)
           WHERE status='pending'"#,
    )
    .persistent(false)
    .execute(pool)
    .await
    .ok();

    // Logs table (optional)
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS partition_util.partition_index_job_logs (
          id bigserial PRIMARY KEY,
          job_id bigint,
          partition_name text,
          index_type text,
          status text,
          attempt int,
          message text,
          logged_at timestamptz NOT NULL DEFAULT now()
        )
        "#,
    )
    .persistent(false)
    .execute(pool)
    .await
    .ok();

    Ok(())
}
