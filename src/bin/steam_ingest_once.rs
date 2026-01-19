use anyhow::Result;
use i_miss_rust::database_ops::db::Db;
use i_miss_rust::database_ops::steam::provider::SteamProvider;
use sqlx::types::chrono::Utc;
use sqlx::Row;
use tracing_subscriber::{EnvFilter, FmtSubscriber};
// (steam_ingest_once) Only uses high-level ensure_* helpers; price ingestion handled inside provider.
use i_miss_rust::database_ops::ingest_providers::*;
use i_miss_rust::util::env::{db_url_prefer_session, preflight_check};
use reqwest::Client;
use serde_json::json;
// NOTE: Local ensure_* overrides removed; using shared helpers from ingest_providers now.

async fn require_ingest_schema(db: &Db) -> Result<()> {
    // Fail fast if the DB isn't migrated yet. This binary must NOT run migrations.
    let required_tables: Vec<String> = vec![
        // dims
        "currencies",
        "countries",
        "jurisdictions",
        // core ingest mapping
        "providers",
        "provider_items",
        "provider_offers",
        "retailers",
        "offers",
        "offer_jurisdictions",
        // price fact + read-hot
        "prices",
        "current_price",
        // observability + media
        "ingest_runs",
        "vg_source_media_links",
    ]
    .into_iter()
    .map(|s| s.to_string())
    .collect();

    let missing: Vec<String> = sqlx::query_scalar(
        "SELECT t FROM unnest($1::text[]) AS t WHERE to_regclass('public.' || t) IS NULL",
    )
    .persistent(false)
    .bind(&required_tables)
    .fetch_all(&db.pool)
    .await?;

    if !missing.is_empty() {
        return Err(anyhow::anyhow!(
            "missing required ingestion tables in target DB: {missing:?}. Run migrations first (e.g., migrations_tool / db_migrate / Supabase migrations)."
        ));
    }

    Ok(())
}

async fn dump_app_html(appid: &str) -> Result<std::path::PathBuf, anyhow::Error> {
    let url = format!(
        "https://store.steampowered.com/app/{}/?cc=US&l=english",
        appid
    );
    let client = Client::builder()
        .user_agent("Mozilla/5.0 (compatible; TheOnlyHolyGhost/1.0; +https://example.com)")
        .build()?;
    let resp = client.get(&url).send().await?;
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    let dir = std::path::Path::new("exports/html");
    if !dir.exists() {
        let _ = std::fs::create_dir_all(dir);
    }
    let path = dir.join(format!("steam_app_{}.html", appid));
    std::fs::write(&path, &body)?;
    // Print a short preview to stdout
    let preview = &body.as_bytes()[..body.len().min(2048)];
    println!(
        "\n--- HTML GET {} (status {}) preview ({} bytes) ---\n{}\n--- saved to: {} ---\n",
        url,
        status.as_u16(),
        body.len(),
        String::from_utf8_lossy(preview),
        path.display()
    );
    Ok(path)
}

#[tokio::main]
async fn main() -> Result<()> {
    i_miss_rust::util::env::bootstrap_cli("steam_ingest_once");
    // Log a consolidated, redacted env snapshot (PSStore-style); do NOT require any one DSN key,
    // because db_url_prefer_session() supports multiple sources and optional IPv6.
    let _ = preflight_check(
        "steam-ingest-once",
        &[],
        &[
            "SUPABASE_IPV6_DB",
            "SUPABASE_DB_SESSION_URL",
            "SUPABASE_DB_URL",
            "DATABASE_URL",
            "DB_URL",
            "DB_MAX_CONNS",
            "STEAM_APP_IDS",
            "STEAM_REGIONS",
            "YEAR_MIN",
            "YEAR_MAX",
        ],
    );
    // Logging init
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

    let database_url = db_url_prefer_session()?;
    println!("[steam_ingest_once] connecting to DB (env provided)");
    let max_conns: u32 = std::env::var("DB_MAX_CONNS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8);
    // Avoid server-side prepared statement cache collisions (esp. with PgBouncer in txn mode).
    // Force simple query protocol to completely bypass prepared statements (no Parse/Bind/Describe lifecycle).
    unsafe {
        std::env::set_var("SQLX_DISABLE_STATEMENT_CACHE", "1");
        std::env::set_var("SQLX_PG_SIMPLE", "1");
    }
    let db = Db::connect(&database_url, max_conns).await?;

    // Ensure DB schema is present before any reads/writes.
    require_ingest_schema(&db).await?;
    // Pin schema to public for all writes in this session
    let _ = sqlx::query("SET search_path TO public")
        .persistent(false)
        .execute(&db.pool)
        .await;

    // Ingest must not bootstrap data. If lookups are empty, fail fast with a clear fix.
    let cur_cnt: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM currencies")
        .persistent(false)
        .fetch_one(&db.pool)
        .await?;
    let cty_cnt: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM countries")
        .persistent(false)
        .fetch_one(&db.pool)
        .await?;
    if cur_cnt == 0 || cty_cnt == 0 {
        return Err(anyhow::anyhow!(
            "missing lookup seed data (currencies={cur_cnt}, countries={cty_cnt}). Seed lookups first (currencies/countries/jurisdictions) using dedicated bootstrap tooling or SQL seed scripts; steam_ingest_once will not insert these automatically."
        ));
    }

    // Require at least some app ids
    if std::env::var("STEAM_APP_IDS")
        .map(|v| v.trim().is_empty())
        .unwrap_or(true)
    {
        println!("STEAM_APP_IDS must be set to a comma-separated list of 30 app IDs");
        std::process::exit(2);
    }
    // Force media and multi-region ingest every time in this binary
    unsafe {
        std::env::set_var("STEAM_FETCH_MEDIA", "1");
    }
    // Remove any artificial region caps so we take all curated regions or STEAM_REGIONS if user set it
    unsafe {
        std::env::remove_var("STEAM_MAX_REGIONS");
    }
    // Ensure no paid-only gating here
    unsafe {
        std::env::remove_var("STEAM_ONLY_PAID");
    }

    // Prepare ingest run observability
    let provider_id = ensure_provider(&db, "steam", "retailer_api", Some("steam")).await?;
    let regions_env = std::env::var("STEAM_REGIONS").unwrap_or_default();
    let region_code = regions_env.split_whitespace().next().map(|s| s.to_string());
    let run_id = ingest_run_start(
        &db,
        provider_id,
        region_code.as_deref(),
        Some(json!({"forced_media": true})),
    )
    .await?;

    // Capture app ids up front for post-run counts
    let app_ids_env = std::env::var("STEAM_APP_IDS").unwrap_or_default();
    let app_ids: Vec<String> = app_ids_env
        .split(',')
        .filter(|s| !s.trim().is_empty())
        .map(|s| s.trim().to_string())
        .collect();

    // Resume controls: either explicit STEAM_RESUME_AFTER_ID or infer last processed from DB
    let mut start_idx: usize = 0;
    if let Ok(after_id) = std::env::var("STEAM_RESUME_AFTER_ID") {
        if !after_id.trim().is_empty() {
            if let Some(pos) = app_ids.iter().position(|s| s == &after_id) {
                start_idx = pos.saturating_add(1);
                println!(
                    "[steam_ingest_once] resume: starting after app_id={after_id} (index {pos})"
                );
            }
        }
    } else if std::env::var("STEAM_RESUME_FROM_LAST").ok().as_deref() == Some("1") {
        // Find the last app id that already has a provider_item row; resume after it.
        let rows = sqlx
            ::query(
                "SELECT pi.external_id FROM provider_items pi JOIN providers p ON p.id=pi.provider_id WHERE p.slug='steam' AND pi.external_id = ANY($1)"
            )
            .persistent(false)
            .bind(&app_ids)
            .fetch_all(&db.pool).await
            .unwrap_or_default();
        use sqlx::Row;
        let existing: std::collections::HashSet<String> = rows
            .into_iter()
            .map(|r| r.get::<String, _>("external_id"))
            .collect();
        let mut last_seen: Option<usize> = None;
        for (i, id) in app_ids.iter().enumerate() {
            if existing.contains(id) {
                last_seen = Some(i);
            }
        }
        if let Some(i) = last_seen {
            start_idx = i.saturating_add(1);
            if start_idx < app_ids.len() {
                println!(
                    "[steam_ingest_once] resume: found last processed index {i} (app_id={}), resuming at index {start_idx}",
                    app_ids[i]
                );
            }
        }
    }

    if start_idx > 0 && start_idx < app_ids.len() {
        let remaining = app_ids[start_idx..].join(",");
        unsafe {
            std::env::set_var("STEAM_APP_IDS", remaining);
        }
    }

    let mut status = "ok";
    if let Err(e) = SteamProvider::run_from_env(&db).await {
        status = "error";
        eprintln!("steam provider run failed: {e}");
    }

    // Post-run quick counts for observability
    let mut items_processed: i64 = app_ids.len() as i64;
    // Map app_ids to provider_item ids for steam
    let pid_rows = sqlx
        ::query(
            "SELECT pi.id FROM provider_items pi JOIN providers p ON p.id=pi.provider_id WHERE p.slug='steam' AND pi.external_id = ANY($1)"
        )
        .persistent(false)
        .bind(&app_ids)
        .fetch_all(&db.pool).await
        .unwrap_or_default();
    let video_game_source_ids: Vec<i64> = pid_rows.iter().map(|r| r.get::<i64, _>("id")).collect();
    let prices_written: i64 = if video_game_source_ids.is_empty() {
        0
    } else {
        sqlx::query_scalar(
            "SELECT COUNT(*)::bigint FROM prices WHERE recorded_at > now() - interval '10 minutes' AND video_game_source_id = ANY($1)"
        )
            .persistent(false)
            .bind(&video_game_source_ids)
            .fetch_one(&db.pool).await
            .unwrap_or(0)
    };

    let _ = ingest_run_finish(&db, run_id, status, items_processed, prices_written, None).await;

    // Post-run diagnostics summary (counts per app id for quick verification)
    let app_ids_env = std::env::var("STEAM_APP_IDS").unwrap_or_default();
    let app_ids: Vec<String> = app_ids_env
        .split(',')
        .filter(|s| !s.trim().is_empty())
        .map(|s| s.trim().to_string())
        .collect();
    if !app_ids.is_empty() {
        for aid in &app_ids {
            // provider item id -> enrich diagnostic with actual records, not counts
            if
                let Some(row) = sqlx
                    ::query(
                        "SELECT pi.id FROM provider_items pi JOIN providers p ON p.id=pi.provider_id WHERE pi.external_id=$1 AND (p.slug='steam' OR p.name ILIKE 'steam%') LIMIT 1"
                    )
                    .persistent(false)
                    .bind(aid)
                    .fetch_optional(&db.pool).await?
            {
                let pid: i64 = row.get("id");

                // Offer jurisdictions (list values)
                let oj_rows = sqlx
                    ::query(
                        "SELECT oj.id, oj.currency_id, oj.jurisdiction_id FROM offer_jurisdictions oj \
                     JOIN provider_offers pof ON pof.offer_id = oj.offer_id \
                     WHERE pof.video_game_source_id=$1 ORDER BY oj.id"
                    )
                    .persistent(false)
                    .bind(pid)
                    .fetch_all(&db.pool).await?;
                let offer_jurisdictions: Vec<serde_json::Value> = oj_rows
                    .iter()
                    .map(
                        |r|
                            json!({
                        "id": r.get::<i64,_>("id"),
                        "currency_id": r.get::<i64,_>("currency_id"),
                        "jurisdiction_id": r.get::<i64,_>("jurisdiction_id")
                    })
                    )
                    .collect();

                // Latest N prices (values)
                let price_rows = sqlx
                    ::query(
                        "SELECT recorded_at, amount_minor, tax_inclusive FROM prices WHERE video_game_source_id=$1 ORDER BY recorded_at DESC LIMIT 5"
                    )
                    .persistent(false)
                    .bind(pid)
                    .fetch_all(&db.pool).await?;
                let prices: Vec<serde_json::Value> = price_rows
                    .iter()
                    .map(
                        |r|
                            json!({
                        "recorded_at": r.get::<chrono::DateTime<chrono::Utc>,_>("recorded_at"),
                        "amount_minor": r.get::<i64,_>("amount_minor"),
                        "tax_inclusive": r.get::<bool,_>("tax_inclusive")
                    })
                    )
                    .collect();

                // Current price (if present) for each offer_jurisdiction mapped
                let current_rows = sqlx
                    ::query(
                        "SELECT cp.offer_jurisdiction_id, cp.amount_minor, cp.recorded_at FROM current_price cp \
                     JOIN offer_jurisdictions oj ON oj.id=cp.offer_jurisdiction_id \
                     JOIN provider_offers pof ON pof.offer_id=oj.offer_id \
                     WHERE pof.video_game_source_id=$1 ORDER BY cp.recorded_at DESC"
                    )
                    .persistent(false)
                    .bind(pid)
                    .fetch_all(&db.pool).await?;
                let current_prices: Vec<serde_json::Value> = current_rows
                    .iter()
                    .map(
                        |r|
                            json!({
                        "offer_jurisdiction_id": r.get::<i64,_>("offer_jurisdiction_id"),
                        "amount_minor": r.get::<i64,_>("amount_minor"),
                        "recorded_at": r.get::<chrono::DateTime<chrono::Utc>,_>("recorded_at")
                    })
                    )
                    .collect();

                // Media links (include coarse kind + title for UI rendering)
                let media_rows = sqlx
                    ::query(
                        "SELECT kind, media_type, url, role, source, title FROM vg_source_media_links WHERE video_game_source_id=$1 ORDER BY id"
                    )
                    .persistent(false)
                    .bind(pid)
                    .fetch_all(&db.pool).await?;
                let media_links: Vec<serde_json::Value> = media_rows
                    .iter()
                    .map(
                        |r|
                            json!({
                        "kind": r.get::<Option<String>,_>("kind"),
                        "media_type": r.get::<Option<String>,_>("media_type"),
                        "url": r.get::<String,_>("url"),
                        "role": r.get::<Option<String>,_>("role"),
                        "source": r.get::<Option<String>,_>("source"),
                        "title": r.get::<Option<String>,_>("title")
                    })
                    )
                    .collect();

                let diagnostic =
                    json!({
                    "app_id": aid,
                    "video_game_source_id": pid,
                    "offer_jurisdictions": offer_jurisdictions,
                    "prices_recent": prices,
                    "current_prices": current_prices,
                    "media_links": media_links
                });
                println!("[steam_ingest_once] diagnostic: {}", diagnostic);
            } else {
                eprintln!(
                    "[steam_ingest_once] app {aid}: no provider_item row created; creating minimal mapping as fallback"
                );
                // Fallback: create a minimal mapping so diagnostics and downstream flows can proceed.
                let provider_id = ensure_provider(
                    &db,
                    "steam",
                    "storefront",
                    Some("steam-store")
                ).await?;
                let retailer_id = ensure_retailer(&db, "Steam", Some("steam")).await?;
                let usd_id = ensure_currency(&db, "USD", "US Dollar", 2).await?;
                let us_id = ensure_country(&db, "US", "United States", usd_id).await?;
                let us_nat_id = ensure_national_jurisdiction(&db, us_id).await?;
                let product_slug = format!("steam-app-{}", aid);
                let product_id = ensure_product(&db, "software", Some(&product_slug)).await?;
                let sellable_id = ensure_sellable(&db, "software", product_id).await?;
                let offer_sku = format!("steam:{}", aid);
                let offer_id = ensure_offer(&db, sellable_id, retailer_id, Some(&offer_sku)).await?;
                let _oj_id = ensure_offer_jurisdiction(&db, offer_id, us_nat_id, usd_id).await?;
                let video_game_source_id = ensure_provider_item(
                    &db,
                    provider_id,
                    aid,
                    Some(json!({"source":"fallback"}))
                ).await?;
                let provider_offer_id = link_provider_offer(
                    &db,
                    video_game_source_id,
                    offer_id,
                    Some(0.5)
                ).await?;

                println!("[steam_ingest_once] app {aid}: created provider_item id {} as fallback", video_game_source_id);
                // Now proceed with diagnostics using the newly created provider_item
            }
            // Fetch and dump the HTML for this app page
            let _ = dump_app_html(aid).await;
        }
    }
    Ok(())
}
