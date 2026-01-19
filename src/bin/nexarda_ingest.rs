use anyhow::Result;
use i_miss_rust::database_ops::db::Db;
use i_miss_rust::database_ops::nexarda::provider::{NexardaOptions, NexardaProvider};
use i_miss_rust::util::env;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    env::bootstrap_cli("nexarda_ingest");
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let database_url = env::db_url_prefer_session()
        .expect("Set SUPABASE_IPV6_DB / SUPABASE_DB_URL / DATABASE_URL / DB_URL");
    let max_conns: u32 = std::env::var("DB_MAX_CONNS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8);
    let db = Db::connect(&database_url, max_conns).await?;

    let nx = NexardaProvider::new(
        std::env::var("NEXARDA_BASE_URL").ok().as_deref(),
        std::env::var("NEXARDA_TIMEOUT")
            .ok()
            .and_then(|s| s.parse().ok()),
    )?;
    let opts = NexardaOptions {
        products: serde_json::from_str(&std::env::var("NEXARDA_PRODUCTS").unwrap_or("[]".into()))
            .unwrap_or_default(),
        store_map: serde_json::from_str(&std::env::var("NEXARDA_STORE_MAP").unwrap_or("{}".into()))
            .unwrap_or_default(),
        api_key: std::env::var("NEXARDA_API_KEY").ok(),
        auto_register_stores: Some(true),
        default_regions: serde_json::from_str(
            &std::env::var("NEXARDA_DEFAULT_REGIONS").unwrap_or("[]".into()),
        )
        .unwrap_or_default(),
        dynamic_store_overrides: serde_json::from_str(
            &std::env::var("NEXARDA_STORE_OVERRIDES").unwrap_or("{}".into()),
        )
        .unwrap_or_default(),
        default_tax_inclusive: Some(true),
        context: None,
        base_url: None,
        timeout: None,
    };

    info!("nexarda: ingest start");
    match nx.ingest_to_db(&db, opts).await {
        Ok(cnt) => info!(count = cnt, "nexarda: ingest complete"),
        Err(e) => error!(error=%e, "nexarda: ingest failed"),
    }
    Ok(())
}
