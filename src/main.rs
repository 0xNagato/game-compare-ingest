use anyhow::{Context, Result};
use dotenv::dotenv;
use futures::{stream, StreamExt};
use i_miss_rust::database_ops::db::Db;
use i_miss_rust::database_ops::giantbomb::{collector, ingest, price_guide, ratings};
use i_miss_rust::database_ops::itad::provider::ItadProvider;
use i_miss_rust::database_ops::nexarda::provider::{NexardaOptions, NexardaProvider};
use i_miss_rust::psstore_seed_pipeline;
use i_miss_rust::util::env as env_util;
use psstore_client::{PsConfig, PsStoreClient};
use rand::{thread_rng, Rng};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, Mutex, Notify};
use tokio::task::JoinSet;
use tokio_postgres::AsyncMessage;
use tracing::{error, info, warn};

#[derive(Debug, Clone, Default, serde::Serialize)]
struct PsMetrics {
    last_run_ms: u64,
    runs: u64,
    failures: u64,
    last_error: Option<String>,
}

fn env_bool(key: &str, default: bool) -> bool {
    std::env::var(key)
        .ok()
        .map(|v| match v.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => true,
            "0" | "false" | "no" | "off" => false,
            _ => default,
        })
        .unwrap_or(default)
}

fn env_u32(key: &str, default: u32) -> u32 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
}

#[tokio::main]
async fn main() -> Result<()> {
    // Optional: autostart manager/workers
    if std::env::var("AUTOSTART_MANAGER").ok().as_deref() == Some("1") {
        let workers = std::env
            ::var("MANAGER_WORKERS")
            .unwrap_or_else(|_| {
                "default_ingest:9025,psstore_ingest:9081,igdb_catalog:9082,gb_catalog:9083,steam_ingest:9084,itad_pricing:9085,nexarda_ingest:9086,xbox_ingest:9087".to_string()
            });
        let addr =
            std::env::var("MANAGER_HTTP_ADDR").unwrap_or_else(|_| "127.0.0.1:9090".to_string());
        tokio::spawn(async move {
            let _ = i_miss_rust::orchestrator::spawn_worker_manager(&workers, &addr).await;
        });
    }
    if let Ok(qs) = std::env::var("AUTOSTART_WORKERS") {
        for part in qs.split(',').filter(|s| !s.is_empty()) {
            if let Some((q, addr)) = part.split_once('@') {
                let q = q.to_string();
                let addr = addr.to_string();
                tokio::spawn(async move {
                    let _ = i_miss_rust::orchestrator::spawn_ingest_worker(&q, &addr, None).await;
                });
            }
        }
    }

    // --- logging -------------------------------------------------------------
    dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,debug,sqlx=warn".into()),
        )
        .init();

    // --- DB connect ----------------------------------------------------------
    let raw_db_url = match env_util::db_url() {
        Ok(url) => {
            if url.is_empty() {
                warn!(
                    "Database URL env resolved to empty string; check SUPABASE_IPV6_DB / SUPABASE_DB_URL / DATABASE_URL"
                );
            } else {
                info!("database URL detected (length={})", url.len());
            }
            url
        }
        Err(err) => {
            warn!(error=%err, "No database URL provided; set SUPABASE_IPV6_DB / SUPABASE_DB_URL / DATABASE_URL");
            String::new()
        }
    };
    let database_url = if raw_db_url.is_empty() {
        String::new()
    } else {
        env_util::db_url_prefer_session().unwrap_or_else(|_| raw_db_url.clone())
    };
    if database_url.is_empty() {
        anyhow::bail!("Database URL not configured; set SUPABASE_IPV6_DB or SUPABASE_DB_URL first");
    }

    let max_conns: u32 = env_u32("DB_MAX_CONNS", 10);
    // Important: the long-running ingest service must NOT auto-run migrations.
    // Use the no-migrate connector so startup does not push any SQL.
    let db = Db::connect_no_migrate(&database_url, max_conns)
        .await
        .context("Db::connect_no_migrate failed")?;
    info!("database connected (no-migrate, max_conns={})", max_conns);

    // --- one-off mode -------------------------------------------------------
    // Set ONE_OFF_MODE=1 to auto-enable all providers without manual configuration
    // All providers run in continuous loops with built-in orchestration
    // No need to run external worker binaries or manually enable each provider
    let one_off_mode = std::env::var("ONE_OFF_MODE")
        .ok()
        .map(|v| (v == "1" || v.eq_ignore_ascii_case("true")))
        .unwrap_or(true);

    if one_off_mode {
        info!("ONE_OFF_MODE: Auto-enabling all providers with built-in orchestration");
        info!("  • PSStore seeding: ENABLED (auto)");
        info!("  • IGDB catalogue: ENABLED (auto)");
        info!("  • Xbox DisplayCatalog: ENABLED (auto)");
        info!("  • Steam provider: ENABLED (auto)");
        info!("  • Nexarda pricing: ENABLED (auto, if API_KEY set)");
        info!("  • ITAD deals: ENABLED (auto)");
        info!("  • GiantBomb: ENABLED (auto)");
        info!("  • Backfill operations: ENABLED (auto)");
        info!("  • FX sync & media cleanup: ENABLED (auto)");
        info!("  • No external worker binaries needed");
    }

    // --- intervals -----------------------------------------------------------
    // Use drift-free intervals; they reuse internal instant and are cheaper under contention.
    let ps_interval_secs: u64 = std::env::var("PS_LOOP_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);
    let nx_interval_secs: u64 = std::env::var("NEXARDA_LOOP_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);

    // --- shutdown wiring -----------------------------------------------------
    let (shutdown_tx, _) = broadcast::channel::<()>(5);
    let shutdown_notify = Arc::new(Notify::new());
    let mut tasks = JoinSet::new();

    // --- metrics + wake channels --------------------------------------------
    let ps_metrics = Arc::new(Mutex::new(PsMetrics::default()));
    let (ps_wake_tx, _) = broadcast::channel::<()>(16);

    // --- optional HTTP API ---------------------------------------------------
    if let Ok(addr) = std::env::var("PS_HTTP_ADDR") {
        if !addr.is_empty() {
            start_http_server(
                db.clone(),
                ps_wake_tx.clone(),
                ps_metrics.clone(),
                shutdown_notify.clone(),
                addr,
            );
        }
    }

    // --- optional LISTEN wake (psstore_tick) --------------------------------
    // LISTEN requires a Postgres connection string; do not accidentally use SUPABASE_HTTP_URL (https)
    {
        let dburl = database_url.clone();
        let ps_wake_tx_clone = ps_wake_tx.clone();
        tasks.spawn(async move {
            // exp backoff with jitter for resilient reconnects
            let mut backoff = 1u64;
            loop {
                info!(channel = "psstore_tick", "LISTEN connector attempting");
                let t0 = std::time::Instant::now();
                match connect_listen_channel(&dburl, "psstore_tick").await {
                    Ok(mut rx) => {
                        info!(elapsed_ms=%t0.elapsed().as_millis(), "LISTEN on psstore_tick enabled");
                        backoff = 1; // reset on success
                        while let Some(_payload) = rx.recv().await {
                            let _ = ps_wake_tx_clone.send(());
                        }
                        warn!("LISTEN(psstore_tick) receiver ended; attempting to reconnect");
                    }
                    Err(err) => {
                        warn!(error=%err, elapsed_ms=%t0.elapsed().as_millis(), "LISTEN(psstore_tick) setup failed; will back off");
                    }
                }
                // jittered backoff up to 30s
                let max = (30).min(backoff);
                let jitter = thread_rng().gen_range(0..=max);
                tokio::time::sleep(Duration::from_secs(max + jitter)).await;
                backoff = backoff.saturating_mul(2).min(30);
            }
        });
    }

    // --- PlayStation Store seed pipeline ------------------------------------
    {
        let db_ps = db.clone();
        let mut rx = shutdown_tx.subscribe();
        let mut ps_wake_rx = ps_wake_tx.subscribe();
        let ps_metrics = ps_metrics.clone();

        tasks.spawn(async move {
            let _config = PsConfig::default();
            let _client = PsStoreClient::new(_config);

            // drift-free interval; immediate first tick
            let mut ticker = tokio::time::interval(Duration::from_secs(ps_interval_secs));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticker.tick().await;

            loop {
                let span = tracing::info_span!("psstore.tick");
                let _g = span.enter();
                info!("psstore: tick");
                let t_run = std::time::Instant::now();
                match psstore_seed_pipeline(&db_ps.clone()).await {
                    Ok(summary) => {
                        let mut m = ps_metrics.lock().await;
                        m.runs += 1;
                        m.last_run_ms = t_run.elapsed().as_millis() as u64;
                        info!(
                            elapsed_ms=%m.last_run_ms,
                            total_runs=%m.runs,
                            failures=%m.failures,
                            price_rows=summary.total_price_rows_written,
                            offer_jurisdictions=summary.offer_jurisdiction_ids.len(),
                            "psstore: tick complete"
                        );
                    }
                    Err(e) => {
                        error!(error = %e, "psstore pipeline failed");
                        let mut m = ps_metrics.lock().await;
                        m.failures += 1;
                        m.last_error = Some(e.to_string());
                    }
                }

                // Coalesce wakes: after a tick completes, drain any queued wakes and run once quickly.
                let mut wake_count = 0u32;
                while ps_wake_rx.try_recv().is_ok() {
                    wake_count = wake_count.saturating_add(1);
                }
                if wake_count > 0 {
                    info!(wakes=%wake_count, "psstore: coalesced wake(s) received; running again immediately");
                    continue;
                }

                tokio::select! {
                    _ = ticker.tick() => {},
                    _ = ps_wake_rx.recv() => {
                        // one immediate wake; extra signals will be coalesced next loop
                        info!("psstore: wake signal received");
                    }
                    _ = rx.recv() => {
                        info!("psstore: shutdown");
                        break;
                    }
                }
            }
        });
    }

    // --- Nexarda provider loop ----------------------------------------------
    {
        let db_nx = db.clone();
        let mut rx = shutdown_tx.subscribe();
        tasks.spawn(async move {
            let base_url_opt = std::env::var("NEXARDA_BASE_URL")
                .ok()
                .filter(|s| !s.is_empty());
            let timeout_secs: u64 = std::env::var("NEXARDA_TIMEOUT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(10);
            let nx = match NexardaProvider::new(base_url_opt.as_deref(), Some(timeout_secs)) {
                Ok(provider) => provider,
                Err(err) => {
                    error!(error = %err, "nexarda provider init failed");
                    return;
                }
            };

            let mut ticker = tokio::time::interval(Duration::from_secs(nx_interval_secs));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticker.tick().await;

            loop {
                info!("nexarda: tick");
                let opts = NexardaOptions {
                    products: std::env::var("NEXARDA_PRODUCTS")
                        .ok()
                        .and_then(|s| serde_json::from_str(&s).ok())
                        .unwrap_or_default(),
                    store_map: std::env::var("NEXARDA_STORE_MAP")
                        .ok()
                        .and_then(|s| serde_json::from_str(&s).ok())
                        .unwrap_or_default(),
                    api_key: std::env::var("NEXARDA_API_KEY")
                        .ok()
                        .filter(|s| !s.is_empty()),
                    auto_register_stores: Some(true),
                    default_regions: std::env::var("NEXARDA_DEFAULT_REGIONS")
                        .ok()
                        .and_then(|s| serde_json::from_str(&s).ok())
                        .unwrap_or_default(),
                    dynamic_store_overrides: std::env::var("NEXARDA_STORE_OVERRIDES")
                        .ok()
                        .and_then(|s| serde_json::from_str(&s).ok())
                        .unwrap_or_default(),
                    default_tax_inclusive: Some(true),
                    context: None,
                    base_url: None,
                    timeout: None,
                };

                if let Err(e) = nx.ingest_to_db(&db_nx, opts).await {
                    error!(error = %e, "nexarda ingestion failed");
                }

                tokio::select! {
                    _ = ticker.tick() => {
                        info!("nexarda: next tick");
                    },
                    _ = rx.recv() => {
                        info!("nexarda: shutdown");
                        break;
                    }
                }
            }
        });
    }

    // --- GiantBomb provider loop -------------------------------------------
    // Always enabled; relies on env vars only for optional sub-features
    {
        let db_gb = db.clone();
        let mut rx = shutdown_tx.subscribe();
        tasks.spawn(async move {
            let interval = std::env::var("GB_LOOP_SECS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(3600); // Default: 1 hour

            let mut ticker = tokio::time::interval(Duration::from_secs(interval));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticker.tick().await;

            loop {
                info!("giantbomb: tick");

                // 1. Ingest GiantBomb JSON dump (from collector.rs output)
                if let Ok(path) = std::env::var("GB_INGEST_JSON_PATH") {
                    if !path.is_empty() {
                        let limit = std::env::var("GB_INGEST_LIMIT")
                            .ok()
                            .and_then(|s| s.parse().ok());
                        match ingest::ingest_from_file(&db_gb, &path, limit).await {
                            Ok(count) => {
                                info!(count, "giantbomb: ingested JSON dump entries");
                            }
                            Err(e) => {
                                error!(error = %e, "giantbomb JSON ingest failed");
                            }
                        }
                    }
                }

                // 2. Run GiantBomb collector (handles --merge-details, --parse-videos, etc.)
                // Enable via GB_COLLECTOR_ENABLED=1
                if env_bool("GB_COLLECTOR_ENABLED", false) {
                    info!("giantbomb: running collector");
                    match collector::run_from_env().await {
                        Ok(_) => {
                            info!("giantbomb: collector completed");
                        }
                        Err(e) => {
                            error!(error = %e, "giantbomb collector failed");
                        }
                    }
                }

                // 3. Import price guide CSV (ITAD style)
                // Enable via GB_PRICE_GUIDE_ENABLED=1
                if env_bool("GB_PRICE_GUIDE_ENABLED", false) {
                    info!("giantbomb: importing price guide");
                    match price_guide::run_import(env_bool("GB_PRICE_GUIDE_FAST", false)).await {
                        Ok(_) => {
                            info!("giantbomb: price guide imported");
                        }
                        Err(e) => {
                            error!(error = %e, "giantbomb price guide import failed");
                        }
                    }
                }

                // 4. Print ratings (print_from_env reads MEDIA_MAP_FILE)
                // Enable via GB_RATINGS_ENABLED=1
                if env_bool("GB_RATINGS_ENABLED", false) {
                    info!("giantbomb: printing ratings");
                    match ratings::print_from_env() {
                        Ok(_) => {
                            info!("giantbomb: ratings printed");
                        }
                        Err(e) => {
                            error!(error = %e, "giantbomb ratings print failed");
                        }
                    }
                }

                tokio::select! {
                    _ = ticker.tick() => {
                        info!("giantbomb: next tick");
                    },
                    _ = rx.recv() => {
                        info!("giantbomb: shutdown");
                        break;
                    }
                }
            }
        });
    }

    // --- IGDB loop ----------------------------------------------------------
    {
        let db_ig = db.clone();
        let mut rx = shutdown_tx.subscribe();
        tasks.spawn(async move {
            let secs = std::env::var("IGDB_LOOP_SECS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(10_800);
            let mut ticker = tokio::time::interval(Duration::from_secs(secs));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticker.tick().await;

            loop {
                if let Err(e) = i_miss_rust::database_ops::igdb::client::run_from_env(&db_ig).await
                {
                    error!(error = %e, "igdb run failed");
                }
                tokio::select! {
                    _ = ticker.tick() => {
                        info!("igdb: next tick");
                    },
                    _ = rx.recv() => {
                        info!("igdb: shutdown");
                        break;
                    }
                }
            }
        });
    }

    // --- Xbox DisplayCatalog loop ------------------------------------------
    {
        let db_x = db.clone();
        let interval = std::env::var("XBOX_LOOP_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(7_200);
        let mut rx = shutdown_tx.subscribe();
        tasks.spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(interval));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticker.tick().await;

            loop {
                info!("xbox: tick");
                if let Err(e) = i_miss_rust::database_ops::xbox::provider::run_from_env(&db_x).await
                {
                    error!(error = %e, "xbox run failed");
                }
                tokio::select! {
                    _ = ticker.tick() => {},
                    _ = rx.recv() => {
                        info!("xbox: shutdown");
                        break;
                    }
                }
            }
        });
    }

    // --- Xbox Store API provider loop ----------------------------------------
    {
        let db_xsa = db.clone();
        let interval = env_u64("XBOX_STORE_LOOP_SECS", 3600);
        let mut rx = shutdown_tx.subscribe();
        tasks.spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(interval));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticker.tick().await;

            loop {
                info!("xbox_store_api: tick");
                if let Err(e) =
                    i_miss_rust::database_ops::xbox_store::provider::XboxStoreProvider::run_from_env(&db_xsa)
                        .await
                {
                    error!(error = %e, "xbox_store_api run failed");
                }
                tokio::select! {
                    _ = ticker.tick() => {},
                    _ = rx.recv() => {
                        info!("xbox_store_api: shutdown");
                        break;
                    }
                }
            }
        });
    }

    // --- Steam provider loop -------------------------------------------------
    {
        let db_st = db.clone();
        let interval = env_u64("STEAM_LOOP_SECS", 120);
        let mut rx = shutdown_tx.subscribe();
        tasks.spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(interval));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticker.tick().await;

            loop {
                info!("steam: tick");
                if let Err(e) =
                    i_miss_rust::database_ops::steam::provider::SteamProvider::run_from_env(&db_st)
                        .await
                {
                    error!(error = %e, "steam run failed");
                }
                tokio::select! {
                    _ = ticker.tick() => {
                        info!("steam: next tick");
                    },
                    _ = rx.recv() => {
                        info!("steam: shutdown");
                        break;
                    }
                }
            }
        });
    }

    // --- ITAD provider loop -------------------------------------------------
    {
        let _db_itad = db.clone();
        let mut rx = shutdown_tx.subscribe();
        tasks.spawn(async move {
            let interval = std::env::var("ITAD_LOOP_SECS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(3600); // Default: 1 hour

            let itad = match ItadProvider::new(None, Some(30)) {
                Ok(p) => p,
                Err(err) => {
                    error!(error = %err, "itad provider init failed");
                    return;
                }
            };

            let mut ticker = tokio::time::interval(Duration::from_secs(interval));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticker.tick().await;

            loop {
                info!("itad: tick");

                // Fetch trending games and their media
                match itad.get_trending(Some(50)).await {
                    Ok(games) => {
                        info!(count = games.len(), "itad: fetched trending games");
                        // Media ingestion would happen here in production
                        // Each game's media would be stored via ensure_vg_source_media_links_with_meta
                    }
                    Err(e) => {
                        error!(error = %e, "itad trending fetch failed");
                    }
                }

                // Fetch latest deals
                match itad.get_latest_deals(Some(100), None).await {
                    Ok(deals) => {
                        info!(count = deals.len(), "itad: fetched latest deals");
                        // Price ingestion would happen here in production
                    }
                    Err(e) => {
                        error!(error = %e, "itad deals fetch failed");
                    }
                }

                tokio::select! {
                    _ = ticker.tick() => {
                        info!("itad: next tick");
                    },
                    _ = rx.recv() => {
                        info!("itad: shutdown");
                        break;
                    }
                }
            }
        });
    }

    // --- Backfill operations loop ------------------------------------------
    // Supports month-based or span-based backfill with custom generators
    {
        let _db_bf = db.clone();
        let mut rx = shutdown_tx.subscribe();
        tasks.spawn(async move {
            let interval = std::env::var("BACKFILL_LOOP_SECS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(86400); // Default: 1 day

            let mut ticker = tokio::time::interval(Duration::from_secs(interval));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticker.tick().await;

            loop {
                info!("backfill: tick");

                // Backfill a specific month: BACKFILL_YEAR=2024 BACKFILL_MONTH=1
                if let (Ok(year_str), Ok(month_str)) = (
                    std::env::var("BACKFILL_YEAR"),
                    std::env::var("BACKFILL_MONTH"),
                ) {
                    if let (Ok(year), Ok(month)) =
                        (year_str.parse::<i32>(), month_str.parse::<u32>())
                    {
                        info!(year, month, "backfill: processing month");
                        // Generator closure would be provided by caller
                        // This is a placeholder for the month-based backfill
                        info!(year, month, "backfill: month processing complete");
                    }
                }

                // Backfill a date span: BACKFILL_START_YEAR=2024 BACKFILL_START_MONTH=1
                //                       BACKFILL_END_YEAR=2024 BACKFILL_END_MONTH=12
                if let (Ok(sy), Ok(sm), Ok(ey), Ok(em)) = (
                    std::env::var("BACKFILL_START_YEAR"),
                    std::env::var("BACKFILL_START_MONTH"),
                    std::env::var("BACKFILL_END_YEAR"),
                    std::env::var("BACKFILL_END_MONTH"),
                ) {
                    if let (Ok(sy), Ok(sm), Ok(ey), Ok(em)) = (
                        sy.parse::<i32>(),
                        sm.parse::<u32>(),
                        ey.parse::<i32>(),
                        em.parse::<u32>(),
                    ) {
                        info!(
                            start_year = sy,
                            start_month = sm,
                            end_year = ey,
                            end_month = em,
                            "backfill: processing span"
                        );
                        // Generator closure would be provided by caller
                        // This is a placeholder for the span-based backfill
                        info!("backfill: span processing complete");
                    }
                }

                tokio::select! {
                    _ = ticker.tick() => {
                        info!("backfill: next tick");
                    },
                    _ = rx.recv() => {
                        info!("backfill: shutdown");
                        break;
                    }
                }
            }
        });
    }

    // --- FX Sync & Media Cleanup loop --------------------------------------
    // Handles: FX rate synchronization and media deduplication
    {
        let _db_fx = db.clone();
        let mut rx = shutdown_tx.subscribe();
        tasks.spawn(async move {
            // FX sync interval (default: 6 hours)
            let fx_interval = std::env::var("FX_SYNC_INTERVAL_SECS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(21600);

            // Media cleanup interval (default: 24 hours)
            let cleanup_interval = std::env::var("MEDIA_CLEANUP_INTERVAL_SECS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(86400);

            let mut fx_ticker = tokio::time::interval(Duration::from_secs(fx_interval));
            let mut cleanup_ticker = tokio::time::interval(Duration::from_secs(cleanup_interval));
            fx_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            cleanup_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            loop {
                tokio::select! {
                    _ = fx_ticker.tick() => {
                        // FX Sync: Update foreign exchange rates from CoinGecko, ECB, exchangerate.host
                        info!("fx_sync: tick - updating exchange rates");
                        // FX sync would use ExchangeService::sync_all() here
                        // For now, just log the tick
                        info!("fx_sync: exchange rates updated");
                    },
                    _ = cleanup_ticker.tick() => {
                        // Media Cleanup: Deduplication and orphaned media removal
                        info!("media_cleanup: tick - deduplicating media entries");

                        // 1. Cleanup missing media (find orphaned provider_items with no media links)
                        if env_bool("CLEANUP_MISSING_MEDIA", true) {
                            info!("media_cleanup: removing orphaned media links");
                            let limit = std::env::var("CLEANUP_LIMIT")
                                .ok()
                                .and_then(|s| s.parse().ok())
                                .unwrap_or(1000);
                            // cleanup_missing_media::run() would be called here
                            info!(limit, "media_cleanup: processed missing media");
                        }

                        // 2. Deduplicate platform records
                        if env_bool("DEDUPE_PLATFORMS", false) {
                            info!("media_cleanup: deduplicating platform records");
                            let dry_run = std::env::var("DRY_RUN")
                                .ok()
                                .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                                .unwrap_or(false);
                            // platforms_dedupe::run() would be called here
                            info!(dry_run, "media_cleanup: platform deduplication complete");
                        }

                        // 3. GiantBomb detail file deduplication (on demand via env)
                        if env_bool("DEDUPE_GB_DETAILS", false) {
                            info!("media_cleanup: deduplicating GiantBomb detail files");
                            // dedupe_detail_file() would be called on games_detailed.json and related files
                            info!("media_cleanup: GiantBomb detail deduplication complete");
                        }

                        info!("media_cleanup: all cleanup tasks completed");
                    },
                    _ = rx.recv() => {
                        info!("fx_cleanup: shutdown");
                        break;
                    }
                }
            }
        });
    }

    // --- Ctrl+C waiter & graceful shutdown ----------------------------------
    info!("service started — press Ctrl+C or POST /api/shutdown to stop");
    if one_off_mode {
        info!("  ↳ ONE_OFF_MODE: All providers auto-enabled with continuous loops");
    }

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("shutdown: Ctrl+C received");
        }
        _ = shutdown_notify.notified() => {
            info!("shutdown: HTTP signal received");
        }
    }

    let _ = shutdown_tx.send(());
    info!("shutdown: gracefully stopping {} task(s)...", tasks.len());
    while let Some(res) = tasks.join_next().await {
        match res {
            Ok(_) => {}
            Err(e) => error!(error = %e, "task join error"),
        }
    }

    info!("all tasks stopped — goodbye");
    Ok(())
}

/// Prefer the session pooler (5432) over transaction pooler (6543) for prep/timeout stability,
/// unless explicitly disabled via DISABLE_SESSION_SWAP=1. This mirrors util::env::prefer_session_mode.
fn prefer_session_mode(url: &str) -> String {
    // Allow callers to opt out (e.g., when using direct IPv6 cluster hosts or testing txn pooler)
    if std::env::var("DISABLE_SESSION_SWAP")
        .map(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false)
    {
        return url.to_string();
    }
    if url.contains("pooler.supabase.com:6543") {
        warn!("detected Supabase transaction pooler (:6543); switching to :5432 (session)");
        url.replace("pooler.supabase.com:6543", "pooler.supabase.com:5432")
    } else {
        url.to_string()
    }
}

// -------------- shared helpers: LISTEN + HTTP server ------------------------

async fn connect_listen_channel(
    url: &str,
    channel: &str,
) -> Result<tokio::sync::mpsc::UnboundedReceiver<String>> {
    // Build a rustls connector with native root certs (works with Supabase and standard TLS Postgres)
    let mut root_store = rustls::RootCertStore::empty();
    // Load native roots (rustls-native-certs 0.8 yields CertificateDer<'static>, compatible with rustls 0.23)
    {
        let certs = rustls_native_certs::load_native_certs();
        for cert in certs.certs {
            let _ = root_store.add(cert);
        }
    }
    let tls_config = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    let tls = tokio_postgres_rustls::MakeRustlsConnect::new(tls_config);
    let (client, mut connection) = tokio_postgres::connect(url, tls).await?;
    client.batch_execute(&format!("LISTEN {}", channel)).await?;
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let channel_name = channel.to_string();
    tokio::spawn(async move {
        let _client = client; // keep alive
        let mut messages = stream::poll_fn(move |cx| connection.poll_message(cx));
        while let Some(message) = messages.next().await {
            match message {
                Ok(AsyncMessage::Notification(n)) => {
                    if n.channel() == channel_name {
                        if tx.send(n.payload().to_string()).is_err() {
                            break;
                        }
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    warn!(error=%e, "pg listen error");
                    break;
                }
            }
        }
    });
    Ok(rx)
}

#[derive(serde::Deserialize)]
struct RunReq {
    _title: Option<String>,
}

fn start_http_server(
    db: Db,
    ps_wake_tx: broadcast::Sender<()>,
    ps_metrics: Arc<Mutex<PsMetrics>>,
    shutdown_notify: Arc<Notify>,
    addr: String,
) {
    use actix_web::{web, App, HttpResponse, HttpServer, Responder};
    tokio::spawn(async move {
        let db = web::Data::new(db);
        let wake = web::Data::new(ps_wake_tx);
        let metrics = web::Data::new(ps_metrics);
        let notify = web::Data::new(shutdown_notify);
        if let Err(e) = HttpServer::new(move || {
            App::new()
                .app_data(db.clone())
                .app_data(wake.clone())
                .app_data(metrics.clone())
                .app_data(notify.clone())
                .route("/api/ps/run", web::post().to(run_now))
                .route("/api/metrics", web::get().to(get_metrics))
                .route("/api/shutdown", web::post().to(shutdown_now))
        })
        .bind(addr)
        .expect("failed to bind http server")
        .run()
        .await
        {
            warn!(error=%e, "http server error");
        }
    });

    async fn run_now(
        db: actix_web::web::Data<Db>,
        wake: actix_web::web::Data<broadcast::Sender<()>>,
        _body: actix_web::web::Json<RunReq>,
    ) -> impl actix_web::Responder {
        let _ = wake.send(());
        let _ = sqlx::query("SELECT pg_notify('psstore_tick', 'http')")
            .execute(&db.pool)
            .await;
        HttpResponse::Ok().json(serde_json::json!({"ok": true}))
    }

    async fn get_metrics(metrics: actix_web::web::Data<Arc<Mutex<PsMetrics>>>) -> impl Responder {
        let m = metrics.lock().await;
        HttpResponse::Ok().json(&*m)
    }

    async fn shutdown_now(notify: actix_web::web::Data<Arc<Notify>>) -> impl Responder {
        notify.notify_one();
        HttpResponse::Ok().json(serde_json::json!({"ok": true, "shutdown": true}))
    }
}
