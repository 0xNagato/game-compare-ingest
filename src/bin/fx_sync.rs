use anyhow::Result;
use chrono::Utc;
use i_miss_rust::database_ops::{db::Db, exchange::ExchangeService};
use std::time::Duration;
use tracing_subscriber::{fmt::SubscriberBuilder, EnvFilter};

#[tokio::main]
async fn main() -> Result<()> {
    i_miss_rust::util::env::bootstrap_cli("fx_sync");
    dotenv::dotenv().ok();
    init_tracing();
    let db_url = std::env::var("SUPABASE_DB_URL").or_else(|_| std::env::var("DATABASE_URL"))?;
    let db = Db::connect(&db_url, 5).await?;
    let svc = ExchangeService::new(db.clone());

    let interval_secs: u64 = std::env::var("FX_SYNC_INTERVAL_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    if interval_secs == 0 {
        // one-shot
        run_once(&svc).await?;
    } else {
        loop {
            if let Err(e) = run_once(&svc).await {
                eprintln!("[fx_sync] error: {e:?}");
            }
            tokio::time::sleep(Duration::from_secs(interval_secs)).await;
        }
    }
    Ok(())
}

async fn run_once(svc: &ExchangeService) -> Result<()> {
    let start = Utc::now();
    let summary = svc.sync_all().await?;
    println!(
        "[fx_sync] synced rates: fetched={} stored={} elapsed_ms={} ts={}",
        summary.fetched,
        summary.stored,
        (Utc::now() - start).num_milliseconds(),
        summary.timestamp
    );
    Ok(())
}

fn init_tracing() {
    let _ = SubscriberBuilder::default()
        .with_env_filter(
            EnvFilter::from_default_env()
                .add_directive("info".parse().unwrap_or_else(|_| "info".parse().unwrap())),
        )
        .with_target(false)
        .try_init();
}
