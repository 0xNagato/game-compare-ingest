use anyhow::Result;

use sqlx::Row;
// NOTE: In this environment std::env::set_var is treated as unsafe; wrap in unsafe block.
use psstore_client::get_env;
use tracing_subscriber::EnvFilter;
// Crate modules
use i_miss_rust::database_ops::db::Db;
use i_miss_rust::database_ops::steam::provider::SteamProvider;

// Integration test for multi-region Steam ingest.
// This test exercises a single paid app (Cyberpunk 2077 appid 1091500) across two regions (US, GB)
// and validates that domain entities (provider_item, offers, offer_jurisdictions, optional prices) are created.
// NOTE: Relies on a live DB + Steam API.
// - Skipped unless RUN_DB_TESTS=1 is set.
// - Marked #[ignore] to avoid mandatory CI flakiness; run explicitly when needed.
// To execute:
//   RUN_DB_TESTS=1 cargo test --test steam_multi_region -- --ignored --nocapture

#[tokio::test]
#[ignore]

async fn steam_multi_region_ingest_minimal() -> Result<()> {
    if std::env::var("RUN_DB_TESTS").ok().as_deref() != Some("1") {
        eprintln!("[steam_test] skipping (set RUN_DB_TESTS=1 to enable)");
        return Ok(());
    }

    dotenv::dotenv().ok();

    // Ensure required env vars for Steam ingestion (test isolated, deterministic)
    unsafe {
        std::env::set_var("STEAM_APP_IDS", "1091500"); // Cyberpunk 2077
        std::env::set_var("STEAM_REGIONS", "US:USD,GB:GBP");
        std::env::set_var("STEAM_BATCH_FLUSH", "25");
        std::env::set_var("STEAM_MAX_CONCURRENCY", "2");
        std::env::set_var("STEAM_DEBUG", "1"); // enable debug trace prints for proof
    }

    // Require SUPABASE_DB_URL (matches project convention) or fall back to DATABASE_URL.
    let database_url = if std::env::var("SUPABASE_DB_URL").is_ok() {
        get_env("SUPABASE_DB_URL")
    } else {
        get_env("DATABASE_URL")
    };

    // Init tracing (info level) for local visibility if --nocapture used.
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();

    let db = Db::connect(&database_url, 4).await.unwrap();
    println!("[steam_test] starting multi-region ingest for app 1091500 (Cyberpunk 2077)");

    // Pre-count existing price rows & provider items for the app across target regions.
    let pre_prices = sqlx::query(
        "SELECT COUNT(p.id) AS cnt FROM prices p \n        JOIN provider_items pi ON pi.id = p.provider_item_id \n        WHERE pi.external_id = $1"
    )
        .bind("1091500")
        .persistent(true)
        .fetch_one(&db.pool)
        .await?
        .get::<i64,_>("cnt");
    println!("[steam_test] pre-existing price rows: {pre_prices}");

    SteamProvider::run_from_env(&db).await?;

    // Validate provider_item exists.
    let provider_item = sqlx::query("SELECT id FROM provider_items WHERE external_id=$1")
        .bind("1091500")
        .persistent(true)
        .fetch_optional(&db.pool)
        .await?;
    assert!(
        provider_item.is_some(),
        "provider_item for app 1091500 not created"
    );
    let provider_item_id: i64 = provider_item.unwrap().get("id");
    println!("[steam_test] provider_item id: {provider_item_id}");

    // Validate provider_offers mapping.
    let offer_map_cnt: i64 =
        sqlx::query("SELECT COUNT(id) AS cnt FROM provider_offers WHERE provider_item_id=$1")
            .bind(provider_item_id)
            .persistent(false)
            .fetch_one(&db.pool)
            .await?
            .get("cnt");
    assert!(
        offer_map_cnt >= 1,
        "Expected at least one provider_offers row for provider_item {provider_item_id}"
    );
    println!("[steam_test] provider_offers mappings: {offer_map_cnt}");

    // Validate jurisdictions (should have at least 2 offer_jurisdictions for US & GB).
    let oj_cnt: i64 = sqlx::query(
        "SELECT COUNT(oj.id) AS cnt FROM offer_jurisdictions oj \n         JOIN offers o ON o.id = oj.offer_id \n         JOIN provider_offers pof ON pof.offer_id = o.id \n         WHERE pof.provider_item_id = $1"
    )
        .bind(provider_item_id)
        .persistent(false)
        .fetch_one(&db.pool)
        .await?
        .get("cnt");
    assert!(
        oj_cnt >= 2,
        "Expected >=2 offer_jurisdictions, got {}",
        oj_cnt
    );
    println!("[steam_test] offer_jurisdictions count: {oj_cnt}");

    // Post-ingest prices count; price rows may vary if a region reports no price_overview.
    let post_prices = sqlx::query(
        "SELECT COUNT(p.id) AS cnt FROM prices p \n        JOIN provider_items pi ON pi.id = p.provider_item_id \n        WHERE pi.external_id = $1"
    )
        .bind("1091500")
        .persistent(false)
        .fetch_one(&db.pool)
        .await?
        .get::<i64,_>("cnt");
    println!("[steam_test] post-ingest price rows: {post_prices}");

    if post_prices == pre_prices {
        eprintln!(
            "[steam_test] WARNING: No new price rows ingested (pre={pre_prices}, post={post_prices}). Game may be free or API returned no price_overview."
        );
    }

    // Assert at least one price row overall (from any prior ingestion) to keep invariant meaningful.
    assert!(
        post_prices > 0 || pre_prices > 0,
        "No price rows found for app 1091500 after ingest attempts"
    );

    println!("[steam_test] SUCCESS: steam multi-region ingest invariants satisfied");
    Ok(())
}
