use anyhow::Result;
use std::time::Instant;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use i_miss_rust::database_ops::db::Db;
use i_miss_rust::database_ops::giantbomb::ingest::ingest_from_file as gb_ingest_from_file;
use i_miss_rust::database_ops::nexarda::provider::{NexardaOptions, NexardaProvider};
use i_miss_rust::psstore_seed_pipeline;
use i_miss_rust::util::env as env_util;

#[tokio::main]
async fn main() -> Result<()> {
    i_miss_rust::util::env::bootstrap_cli("ingest_all");
    // Logging
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    // Year-range reminder visible once for all providers started via this orchestrator
    let year_min: i32 = std::env::var("YEAR_MIN")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2020);
    let year_max: i32 = std::env::var("YEAR_MAX")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2025);
    println!("remember: restricting to releases between {year_min}-{year_max} inclusive\n");

    // DB
    let database_url = env_util::db_url_prefer_session()
        .expect("Set SUPABASE_IPV6_DB / SUPABASE_DB_URL / DATABASE_URL");
    let max_conns: u32 = std::env::var("DB_MAX_CONNS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8);
    let db = Db::connect(&database_url, max_conns).await?;
    // Ensure all provider writes go to public.* when using this orchestrator
    let _ = sqlx::query("SET search_path TO public")
        .persistent(false)
        .execute(&db.pool)
        .await;

    let start = Instant::now();

    // Phase A: JSON-first (Nexarda, GiantBomb) — run sequentially
    // 1) Nexarda
    let nx = NexardaProvider::new(
        std::env::var("NEXARDA_BASE_URL").ok().as_deref(),
        std::env::var("NEXARDA_TIMEOUT")
            .ok()
            .and_then(|s| s.parse().ok()),
    )
    .expect("nexarda client");
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
    info!("provider=nexarda action=start");
    if let Err(e) = nx.ingest_to_db(&db, opts).await {
        error!(error=%e, "nexarda failed");
    }
    info!(
        elapsed_ms = start.elapsed().as_millis() as u64,
        "provider=nexarda action=done"
    );

    // 2) GiantBomb JSON file (optional). If GIANT_BOMB_FILE not set, default path under keep/
    let gb_enabled = std::env::var("GB_ENABLED").ok().as_deref() == Some("1");
    let gb_path = std::env::var("GIANT_BOMB_FILE").unwrap_or_else(|_| {
        if std::path::Path::new("merged_games.json").exists() {
            "merged_games.json".into()
        } else if std::path::Path::new("keep/giant_bomb_games_detailed.json").exists() {
            "keep/giant_bomb_games_detailed.json".into()
        } else {
            "giant_bomb_games_detailed.json".into()
        }
    });
    if gb_enabled {
        info!(path=%gb_path, "provider=giantbomb_json action=start");
        match gb_ingest_from_file(
            &db,
            &gb_path,
            std::env::var("GB_LIMIT").ok().and_then(|s| s.parse().ok()),
        )
        .await
        {
            Ok(count) => info!(
                count,
                elapsed_ms = start.elapsed().as_millis() as u64,
                "provider=giantbomb_json action=done"
            ),
            Err(e) => error!(error=%e, "giantbomb_json failed"),
        }
    }

    // Phase B: live APIs concurrent (psstore, xbox, steam, igdb)
    // Gate each provider with *_ENABLED env = "1"
    let ps_enabled = std::env::var("PSSTORE_ENABLED").ok().as_deref() == Some("1");
    let xbox_enabled = std::env::var("XBOX_ENABLED").ok().as_deref() == Some("1");
    let steam_enabled = std::env::var("STEAM_ENABLED").ok().as_deref() == Some("1");
    let igdb_enabled = std::env::var("IGDB_ENABLED").ok().as_deref() == Some("1");
    let rawg_enabled = std::env::var("RAWG_ENABLED").ok().as_deref() == Some("1");

    // Build futures conditionally
    let mut tasks = Vec::new();
    if ps_enabled {
        let db_ps = db.clone();
        tasks.push(tokio::spawn(async move {
            info!("provider=psstore action=start");
            match psstore_seed_pipeline(&db_ps).await {
                Ok(summary) => {
                    info!(
                        price_rows = summary.total_price_rows_written,
                        offer_jurisdictions = summary.offer_jurisdiction_ids.len(),
                        "provider=psstore action=done"
                    );
                }
                Err(e) => {
                    error!(error=%e, "psstore failed");
                }
            }
        }));
    }
    if xbox_enabled {
        let db_x = db.clone();
        tasks.push(tokio::spawn(async move {
            info!("provider=xbox action=start");
            if let Err(e) = i_miss_rust::database_ops::xbox::provider::run_from_env(&db_x).await {
                error!(error=%e, "xbox failed");
            }
            info!("provider=xbox action=done");
        }));
    }
    if steam_enabled {
        let db_s = db.clone();
        tasks.push(tokio::spawn(async move {
            info!("provider=steam action=start");
            if let Err(e) =
                i_miss_rust::database_ops::steam::provider::SteamProvider::run_from_env(&db_s).await
            {
                error!(error=%e, "steam failed");
            }
            info!("provider=steam action=done");
        }));
    }
    if igdb_enabled {
        let db_i = db.clone();
        tasks.push(tokio::spawn(async move {
            info!("provider=igdb action=start");
            if let Err(e) = i_miss_rust::database_ops::igdb::client::run_from_env(&db_i).await {
                error!(error=%e, "igdb failed");
            }
            info!("provider=igdb action=done");
        }));
    }
    if rawg_enabled {
        let db_r = db.clone();
        let rawg_api_key = std::env::var("RAWG_API_KEY").ok().filter(|s| !s.is_empty());
        tasks.push(tokio::spawn(async move {
            info!("provider=rawg action=start");
            if let Err(e) = i_miss_rust::database_ops::rawg::sync(&db_r, rawg_api_key).await {
                error!(error=%e, "rawg failed");
            }
            info!("provider=rawg action=done");
        }));
    }

    // Join all concurrently started tasks
    for t in tasks {
        let _ = t.await;
    }

    info!(
        total_ms = start.elapsed().as_millis() as u64,
        "ingest_all complete"
    );
    Ok(())
}
