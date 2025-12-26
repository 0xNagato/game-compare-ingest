use anyhow::Result;
use i_miss_rust::database_ops::{db::Db, igdb::client};
use i_miss_rust::util::env;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    env::bootstrap_cli("igdb_direct_loop");
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        .init();

    let db_url = std::env::var("DATABASE_URL")
        .or_else(|_| std::env::var("SUPABASE_DB_URL"))
        .expect("DATABASE_URL or SUPABASE_DB_URL must be set");
    let max_conns = std::env::var("DB_MAX_CONNS")
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(8);
    let db = Db::connect(&db_url, max_conns).await?;

    info!(
        target = "igdb",
        "starting direct IGDB ingestion loop (no queue)"
    );
    client::run_from_env(&db).await?;
    info!(target = "igdb", "direct IGDB ingestion finished");

    Ok(())
}
