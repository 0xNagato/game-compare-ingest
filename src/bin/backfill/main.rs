// Concurrent SKIP LOCKED backfill worker
// Usage:
//   DATABASE_URL=postgres://... BACKFILL_WORKERS=16 BACKFILL_BATCH=500 cargo run --bin backfill
//
// This worker claims batches of NULL/default rows across retailer_providers and updates
// them to sane defaults in parallel, using FOR UPDATE SKIP LOCKED to avoid contention.

use anyhow::Result;
use sqlx::PgPool;
use std::{sync::Arc, time::Duration};
use tokio::{task, time};
use tracing::{error, info};

#[derive(Clone)]
struct Config {
    pool: PgPool,
    workers: usize,
    batch_size: i64,
}

#[tokio::main]
async fn main() -> Result<()> {
    i_miss_rust::util::env::bootstrap_cli("backfill");
    // Init tracing subscriber (env-filter honored if set)
    tracing_subscriber::fmt::init();

    let database_url = std::env::var("DATABASE_URL")
        .expect("DATABASE_URL env var must be set to connect to Postgres");
    let pool = PgPool::connect(&database_url).await?;

    // Tune these via env
    let workers = std::env::var("BACKFILL_WORKERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(16);
    let batch_size = std::env::var("BACKFILL_BATCH")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(500);

    let cfg = Arc::new(Config {
        pool,
        workers,
        batch_size,
    });

    let mut handles = Vec::with_capacity(cfg.workers);
    info!(
        workers = cfg.workers,
        batch_size = cfg.batch_size,
        "starting backfill workers"
    );

    for i in 0..cfg.workers {
        let c = cfg.clone();
        let handle = task::spawn(async move {
            if let Err(e) = worker_loop(c, i as i32).await {
                error!(worker = i, error = ?e, "worker failed");
            }
        });
        handles.push(handle);
    }

    for h in handles {
        let _ = h.await;
    }

    Ok(())
}

async fn worker_loop(cfg: Arc<Config>, worker_id: i32) -> Result<()> {
    loop {
        // Choose which table backfill to run. Retailer providers as example.
        let claimed = run_retailer_providers_batch(&cfg.pool, cfg.batch_size).await?;
        if claimed == 0 {
            // No rows claimed; back off briefly
            time::sleep(Duration::from_millis(500)).await;
        } else {
            info!(worker = worker_id, claimed, "batch updated");
            // short yield
            time::sleep(Duration::from_millis(10)).await;
        }
    }
}

// Backfill for retailer_providers: fill NULLs to sane defaults
// Returns number of rows updated in this iteration.
async fn run_retailer_providers_batch(pool: &PgPool, batch_size: i64) -> Result<u64> {
    // Use a single atomic statement with FOR UPDATE SKIP LOCKED inside a CTE.
    // No explicit transaction is required; the lock and update occur within this statement.
    let result = sqlx::query(
        r#"
        WITH cte AS (
          SELECT id
          FROM public.retailer_providers
          WHERE credentials IS NULL
             OR settings IS NULL
             OR metadata IS NULL
             OR jurisdiction_scope IS NULL
             OR is_enabled IS NULL
             OR priority IS NULL
          FOR UPDATE SKIP LOCKED
          LIMIT $1
        )
        UPDATE public.retailer_providers rp
        SET credentials        = coalesce(rp.credentials, '{}'::jsonb),
            settings           = coalesce(rp.settings, '{}'::jsonb),
            metadata           = coalesce(rp.metadata, '{}'::jsonb),
            jurisdiction_scope = coalesce(rp.jurisdiction_scope, '{}'::text[]),
            is_enabled         = coalesce(rp.is_enabled, true),
            priority           = coalesce(rp.priority, 100)
        FROM cte
        WHERE rp.id = cte.id
        RETURNING rp.id
        "#,
    )
    .bind(batch_size)
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}
