use anyhow::Result;
use i_miss_rust::database_ops::db::Db;
use i_miss_rust::database_ops::xbox::provider::run_from_env;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

#[tokio::main]
async fn main() -> Result<()> {
    i_miss_rust::util::env::bootstrap_cli("xbox_ingest_once");
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(EnvFilter::from_default_env())
        .finish();
    let _ = tracing::subscriber::set_global_default(subscriber);

    // Year-range reminder for consistency
    let year_min: i32 = std::env::var("YEAR_MIN")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2020);
    let year_max: i32 = std::env::var("YEAR_MAX")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2025);
    println!("remember: restricting to releases between {year_min}-{year_max} inclusive\n");

    dotenv::dotenv().ok();
    let database_url =
        std::env::var("DATABASE_URL").or_else(|_| std::env::var("SUPABASE_DB_URL"))?;
    let max_conns: u32 = std::env::var("DB_MAX_CONNS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8);
    let db = Db::connect(&database_url, max_conns).await?;
    // Pin schema to public for this session
    let _ = sqlx::query("SET search_path TO public")
        .persistent(false)
        .execute(&db.pool)
        .await;

    // Expect XBOX_PRODUCT_IDS env var (comma list) else skip
    if std::env::var("XBOX_PRODUCT_IDS")
        .map(|v| v.trim().is_empty())
        .unwrap_or(true)
    {
        eprintln!(
            "XBOX_PRODUCT_IDS must be set to comma-separated product bigIds; skipping ingestion"
        );
        return Ok(());
    }

    run_from_env(&db).await?;
    Ok(())
}
