// HTTP API server binary for i-miss-rust
// Provides RESTful APIs for Laravel (game-compare) integration

use anyhow::Result;
use i_miss_rust::api::ApiServer;
use i_miss_rust::database_ops::db::Db;
use i_miss_rust::util::env as env_util;

#[actix_web::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,sqlx=warn".into()),
        )
        .init();

    tracing::info!("Initializing i-miss-rust API server");

    // Load dotenv/env once (safe to call multiple times)
    env_util::init_env();

    // Load configuration from environment
    let server = ApiServer::from_env()?;

    // Initialize database connection
    let database_url = env_util::db_url_prefer_session()?;
    let max_connections: u32 = env_util::env_parse("DB_MAX_CONNS", 10u32);
    let db = Db::connect_no_migrate(&database_url, max_connections).await?;

    tracing::info!("Database connected successfully");

    // Start HTTP server
    server.run(db).await?;

    Ok(())
}
