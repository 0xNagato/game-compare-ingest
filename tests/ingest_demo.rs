use anyhow::Result;
use chrono::Utc;
use serde_json::json;
use std::env;
// Removed unused tracing_subscriber imports to silence warnings.

use dotenv::dotenv;
use i_miss_rust::database_ops::db::{Db, PriceRow};
use i_miss_rust::database_ops::ingest_providers::{
    ensure_offer, ensure_offer_jurisdiction, ensure_product, ensure_provider, ensure_provider_item,
    ensure_retailer, ensure_sellable, ingest_prices, link_provider_offer,
};
use sqlx::Row;
use sqlx::query;

#[tokio::test]
async fn ingest_demo_persistent() -> Result<()> {
    dotenv().ok();
    // Disable auto migrations for this test to avoid legacy media_source enum conversion issues
    // that surface as "invalid input value for enum media_source: \"\"" during the migration
    // phase in environments containing pre-existing rows with blank source values. We only need
    // a minimal subset of tables for this price ingest demonstration.
    unsafe {
        std::env::set_var("AUTO_MIGRATE", "0");
    }
    // Enable verbose SQL logging if caller sets TEST_SQL_TRACE=1
    if std::env::var("TEST_SQL_TRACE")
        .map(|v| v == "1")
        .unwrap_or(false)
    {
        unsafe {
            std::env::set_var("RUST_LOG", "sqlx::query=trace");
        }
    }
    let database_url = env::var("SUPABASE_DB_URL").or_else(|_| env::var("DATABASE_URL"))?;
    // Disable prepared statements to avoid "prepared statement sqlx_s_1 already exists" under PgBouncer transaction mode.
    unsafe {
        std::env::set_var("SQLX_DISABLE_STATEMENT_CACHE", "1");
        std::env::set_var("SQLX_PG_SIMPLE", "1");
    }
    let db = Db::connect(&database_url, 5).await?;
    // Defensive: if a legacy game_media table exists with stray blank sources (text column prior to enum refactor),
    // attempt to clean them up. Ignore errors if table/column mismatch.
    let _ = sqlx::query("DELETE FROM game_media WHERE source::text = ''")
        .persistent(false)
        .execute(&db.pool)
        .await;

    // Ensure minimal refs (USD, US country, national jurisdiction)
    let currency_id: i64 = ensure_currency(&db, "USD", "US Dollar", 2).await?;
    let country_id: i64 = ensure_country(&db, "US", "United States", currency_id).await?;
    let jurisdiction_id: i64 = ensure_national_jurisdiction(&db, country_id).await?;

    // Build entities
    let provider_id = ensure_provider(&db, "steam", "retailer_api", Some("steam")).await?;
    let retailer_id = ensure_retailer(&db, "Steam", Some("steam")).await?;
    let product_id = ensure_product(&db, "software", Some("half-life-3")).await?;
    let sellable_id = ensure_sellable(&db, "software", product_id).await?;
    let offer_id = ensure_offer(&db, sellable_id, retailer_id, None).await?;
    let oj_id = ensure_offer_jurisdiction(&db, offer_id, jurisdiction_id, currency_id).await?;
    let item_id = ensure_provider_item(
        &db,
        provider_id,
        "steam:hl3:sku:000",
        Some(json!({"source":"demo"})),
    )
    .await?;
    link_provider_offer(&db, item_id, offer_id, Some(0.6)).await?;

    // Ingest two price points (ensure partition pre-created by ingest helper)
    let now = Utc::now();
    let earlier = now - chrono::Duration::days(1);
    let rows = vec![
        PriceRow {
            offer_jurisdiction_id: oj_id,
            provider_item_id: Some(item_id),
            recorded_at: earlier,
            amount_minor: 5999,
            tax_inclusive: true,
            fx_minor_per_unit: None,
            btc_sats_per_unit: None,
            meta: json!({}),
        },
        PriceRow {
            offer_jurisdiction_id: oj_id,
            provider_item_id: Some(item_id),
            recorded_at: now,
            amount_minor: 4999,
            tax_inclusive: true,
            fx_minor_per_unit: None,
            btc_sats_per_unit: None,
            meta: json!({"sale":true}),
        },
    ];
    let ingest_res = ingest_prices(&db, rows).await?;
    println!(
        "Inserted current for offer_jurisdiction_ids: {:?}",
        ingest_res.offer_jurisdiction_ids
    );

    Ok(())
}

async fn ensure_currency(db: &Db, code: &str, name: &str, minor_unit: i16) -> Result<i64> {
    if let Some(rec) = sqlx::query("SELECT id FROM currencies WHERE code = $1")
        .persistent(false)
        .bind(code)
        .fetch_optional(&db.pool)
        .await?
    {
        return Ok(rec.get::<i64, _>("id"));
    }
    let rec = sqlx::query(
        "INSERT INTO currencies (code, name, minor_unit) VALUES ($1,$2,$3) RETURNING id",
    )
    .persistent(false)
    .bind(code)
    .bind(name)
    .bind(minor_unit)
    .fetch_one(&db.pool)
    .await?;
    Ok(rec.get::<i64, _>("id"))
}

async fn ensure_country(db: &Db, code2: &str, name: &str, currency_id: i64) -> Result<i64> {
    if let Some(rec) = sqlx::query("SELECT id FROM countries WHERE code2 = $1")
        .persistent(false)
        .bind(code2)
        .fetch_optional(&db.pool)
        .await?
    {
        return Ok(rec.get::<i64, _>("id"));
    }
    let rec = sqlx::query(
        "INSERT INTO countries (code2, name, currency_id) VALUES ($1,$2,$3) RETURNING id",
    )
    .persistent(false)
    .bind(code2)
    .bind(name)
    .bind(currency_id)
    .fetch_one(&db.pool)
    .await?;
    Ok(rec.get::<i64, _>("id"))
}

async fn ensure_national_jurisdiction(db: &Db, country_id: i64) -> Result<i64> {
    if let Some(rec) =
        sqlx::query("SELECT id FROM jurisdictions WHERE country_id=$1 AND region_code IS NULL")
            .persistent(false)
            .bind(country_id)
            .fetch_optional(&db.pool)
            .await?
    {
        return Ok(rec.get::<i64, _>("id"));
    }
    let rec = sqlx::query(
        "INSERT INTO jurisdictions (country_id, region_code) VALUES ($1, NULL) RETURNING id",
    )
    .persistent(false)
    .bind(country_id)
    .fetch_one(&db.pool)
    .await?;
    Ok(rec.get::<i64, _>("id"))
}
