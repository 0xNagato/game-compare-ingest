use anyhow::Result;
use i_miss_rust::database_ops::db::Db;
use i_miss_rust::database_ops::xbox::provider::parse_product_ids;
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

    // Load repo-root .env first. Dotenv is first-value-wins, so later files only fill missing vars.
    dotenv::dotenv().ok();

    // Optional: load Azure auth vars from a dedicated env file used during experimentation.
    // This avoids pasting bearer tokens into the repo-root .env while still enabling AAD auth.
    let _ = dotenv::from_filename("azure_xbox_standalone/.env");
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

    // If toplist harvesting is disabled, require a manual product id list.
    // Otherwise `run_from_env()` can populate IDs from toplists.
    let harvest_toplists = std::env::var("XBOX_HARVEST_TOPLISTS")
        .map(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(true);

    let has_ids_env = std::env::var("XBOX_PRODUCT_IDS")
        .ok()
        .map(|v| !v.trim().is_empty())
        .unwrap_or(false);
    let has_ids_file = std::env::var("XBOX_PRODUCT_IDS_FILE")
        .ok()
        .and_then(|path| std::fs::read_to_string(path).ok())
        .map(|contents| !parse_product_ids(&contents).is_empty())
        .unwrap_or(false);

    if !harvest_toplists && !(has_ids_env || has_ids_file) {
        eprintln!(
            "XBOX_HARVEST_TOPLISTS=0 and no manual IDs were provided; set XBOX_PRODUCT_IDS (comma-separated bigIds) or XBOX_PRODUCT_IDS_FILE, or enable toplist harvesting"
        );
        return Ok(());
    }

    run_from_env(&db).await?;
    Ok(())
}
