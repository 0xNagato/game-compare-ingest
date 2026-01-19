use anyhow::Result;
use i_miss_rust::database_ops::db::Db;
use i_miss_rust::database_ops::steam::provider::print_all_region_prices_for_app;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    i_miss_rust::util::env::bootstrap_cli("steam_once");
    // Minimal one-shot runner for Steam provider (architectural note: avoids orchestrator loops in main.rs)
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let database_url = i_miss_rust::util::env::db_url_prefer_session()
        .expect("Set SUPABASE_IPV6_DB / SUPABASE_DB_URL / DATABASE_URL / DB_URL");
    println!("[steam_once] connecting to DB (env provided)");
    let max_conns: u32 = std::env::var("DB_MAX_CONNS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30);
    // Avoid prepared statement name collisions (PgBouncer txn mode):
    // 1) Disable client statement cache, 2) Force simple protocol (no server prepared statements)
    unsafe {
        std::env::set_var("SQLX_DISABLE_STATEMENT_CACHE", "1");
        std::env::set_var("SQLX_PG_SIMPLE", "1");
    }
    let db = Db::connect(&database_url, max_conns).await?;

    // If STEAM_APP_PRICES_ALL=1 and STEAM_APP_ID set, just print prices for that single app across all regions.
    if std::env::var("STEAM_APP_PRICES_ALL").ok().as_deref() == Some("1") {
        let appid = std::env::var("STEAM_APP_ID")
            .expect("STEAM_APP_ID must be set when STEAM_APP_PRICES_ALL=1");
        print_all_region_prices_for_app(&appid).await?;
        return Ok(());
    }

    if std::env::var("STEAM_ONLY_PAID").ok().as_deref() == Some("1") {
        eprintln!(
            "[steam_once] STEAM_ONLY_PAID=1 active: free / zero-priced apps will be skipped before entity creation"
        );
    }

    i_miss_rust::database_ops::steam::provider::SteamProvider::run_from_env(&db).await?;
    Ok(())
}
