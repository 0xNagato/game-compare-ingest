use anyhow::{Context, Result};
use chrono::Utc;
use i_miss_rust::database_ops::db::{CurrentPriceRow, Db, PriceRow};
use i_miss_rust::util::env as env_util;
use serde_json::{Value, json};
use sqlx::Row; // bring Row trait for .get
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    env_util::bootstrap_cli("ingest_demo_write");
    // Use simple protocol by exporting SQLX_PG_SIMPLE=1 when running this binary to avoid prepared statement clashes.
    let database_url = env::var("SUPABASE_DB_URL").or_else(|_| env::var("DATABASE_URL"))?;
    let db = Db::connect(&database_url, 5).await?;

    // Ensure minimal refs (USD, US) if not present
    let currency_id = ensure_currency(&db, "USD", "US Dollar", 2).await?;
    let country_id = ensure_country(&db, "US", "United States", currency_id).await?;
    let jurisdiction_id = ensure_national_jurisdiction(&db, country_id).await?;

    // Resolve identifiers from environment so we never persist fabricated names.
    let provider_name = env::var("INGEST_PROVIDER_NAME")
        .context("set INGEST_PROVIDER_NAME (e.g., playstation_store)")?;
    let provider_kind =
        env::var("INGEST_PROVIDER_KIND").unwrap_or_else(|_| "storefront".to_string());
    let provider_slug = env::var("INGEST_PROVIDER_SLUG").ok();

    let retailer_name =
        env::var("INGEST_RETAILER_NAME").context("set INGEST_RETAILER_NAME (e.g., PlayStation)")?;
    let retailer_slug = env::var("INGEST_RETAILER_SLUG").ok();

    let product_kind = env::var("INGEST_PRODUCT_KIND").unwrap_or_else(|_| "software".to_string());
    let product_slug = env::var("INGEST_PRODUCT_SLUG")
        .context("set INGEST_PRODUCT_SLUG to an existing or desired product slug")?;
    let sellable_kind = env::var("INGEST_SELLABLE_KIND").unwrap_or_else(|_| product_kind.clone());

    let provider_item_id = env::var("INGEST_PROVIDER_ITEM_ID")
        .context("set INGEST_PROVIDER_ITEM_ID (external SKU/id)")?;
    let provider_item_payload: Option<Value> = env::var("INGEST_PROVIDER_ITEM_PAYLOAD")
        .ok()
        .and_then(|raw| serde_json::from_str(&raw).ok())
        .or_else(|| Some(json!({"source":"manual_ingest" })));

    let provider_id = ensure_provider(
        &db,
        &provider_name,
        &provider_kind,
        provider_slug.as_deref(),
    )
    .await?;
    let retailer_id = ensure_retailer(&db, &retailer_name, retailer_slug.as_deref()).await?;
    let product_id = ensure_product(&db, &product_kind, Some(&product_slug)).await?;
    let sellable_id = ensure_sellable(&db, &sellable_kind, product_id).await?;
    let offer_id = ensure_offer(&db, sellable_id, retailer_id, None).await?;
    let oj_id = ensure_offer_jurisdiction(&db, offer_id, jurisdiction_id, currency_id).await?;
    let item_id = ensure_provider_item(
        &db,
        provider_id,
        &provider_item_id,
        provider_item_payload.clone(),
    )
    .await?;
    link_provider_offer(&db, item_id, offer_id, Some(0.9)).await?;

    // Two price points
    let now = Utc::now();
    let earlier = now - chrono::Duration::hours(1);
    let rows = vec![
        PriceRow {
            offer_jurisdiction_id: oj_id,
            provider_item_id: Some(item_id),
            recorded_at: earlier,
            amount_minor: 999,
            tax_inclusive: true,
            fx_minor_per_unit: None,
            btc_sats_per_unit: None,
            meta: json!({"ingest_label":"initial_snapshot"}),
        },
        PriceRow {
            offer_jurisdiction_id: oj_id,
            provider_item_id: Some(item_id),
            recorded_at: now,
            amount_minor: 799,
            tax_inclusive: true,
            fx_minor_per_unit: None,
            btc_sats_per_unit: None,
            meta: json!({"ingest_label":"latest_snapshot","sale":true}),
        },
    ];
    let cp_agent = env::var("INGEST_CURRENT_PRICE_AGENT")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .or_else(|| provider_slug.clone())
        .unwrap_or_else(|| provider_name.clone());
    let cp_priority = env::var("INGEST_CURRENT_PRICE_PRIORITY")
        .ok()
        .and_then(|v| v.parse::<i16>().ok())
        .unwrap_or(1);

    let (affected_oj_ids, upserts) = ingest_prices(&db, rows, &cp_agent, cp_priority).await?;
    println!("WROTE offer_jurisdiction_id(s): {:?}", affected_oj_ids);
    println!("Current_price rows upserted: {}", upserts);
    println!("Proof: run db_counts before and after to see deltas (prices + current_price).");
    Ok(())
}

async fn ensure_currency(db: &Db, code: &str, name: &str, minor_unit: i16) -> Result<i64> {
    if let Some(rec) = sqlx::query("SELECT id FROM publiccurrencies WHERE code = $1")
        .persistent(false)
        .bind(code)
        .fetch_optional(&db.pool)
        .await?
    {
        return Ok(rec.get::<i64, _>("id"));
    }
    let rec = sqlx::query(
        "INSERT INTO publiccurrencies (code, name, minor_unit) VALUES ($1,$2,$3) RETURNING id",
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
    if let Some(rec) = sqlx::query("SELECT id FROM publiccountries WHERE code2 = $1")
        .persistent(false)
        .bind(code2)
        .fetch_optional(&db.pool)
        .await?
    {
        return Ok(rec.get::<i64, _>("id"));
    }
    let rec = sqlx::query(
        "INSERT INTO publiccountries (code2, name, currency_id) VALUES ($1,$2,$3) RETURNING id",
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
    if let Some(rec) = sqlx::query(
        "SELECT id FROM publicjurisdictions WHERE country_id=$1 AND region_code IS NULL",
    )
    .persistent(false)
    .bind(country_id)
    .fetch_optional(&db.pool)
    .await?
    {
        return Ok(rec.get::<i64, _>("id"));
    }
    let rec = sqlx::query(
        "INSERT INTO publicjurisdictions (country_id, region_code) VALUES ($1, NULL) RETURNING id",
    )
    .persistent(false)
    .bind(country_id)
    .fetch_one(&db.pool)
    .await?;
    Ok(rec.get::<i64, _>("id"))
}

async fn ensure_provider(db: &Db, name: &str, kind: &str, slug: Option<&str>) -> Result<i64> {
    if let Some(r) = sqlx::query("SELECT id FROM publicproviders WHERE name = $1")
        .persistent(false)
        .bind(name)
        .fetch_optional(&db.pool)
        .await?
    {
        return Ok(r.get("id"));
    }
    let rec = sqlx::query(
        "INSERT INTO publicproviders (name, kind, slug) VALUES ($1,$2,$3) RETURNING id",
    )
    .persistent(false)
    .bind(name)
    .bind(kind)
    .bind(slug)
    .fetch_one(&db.pool)
    .await?;
    Ok(rec.get("id"))
}

async fn ensure_retailer(db: &Db, name: &str, slug: Option<&str>) -> Result<i64> {
    if let Some(r) = sqlx::query("SELECT id FROM publicretailers WHERE name = $1")
        .persistent(false)
        .bind(name)
        .fetch_optional(&db.pool)
        .await?
    {
        return Ok(r.get("id"));
    }
    let rec = sqlx::query("INSERT INTO publicretailers (name, slug) VALUES ($1,$2) RETURNING id")
        .persistent(false)
        .bind(name)
        .bind(slug)
        .fetch_one(&db.pool)
        .await?;
    Ok(rec.get("id"))
}

async fn ensure_product(db: &Db, kind: &str, slug: Option<&str>) -> Result<i64> {
    if let Some(s) = slug {
        if let Some(rec) = sqlx::query("SELECT id FROM publicproducts WHERE slug = $1")
            .persistent(false)
            .bind(s)
            .fetch_optional(&db.pool)
            .await?
        {
            return Ok(rec.get("id"));
        }
    }
    let rec = sqlx::query("INSERT INTO publicproducts (slug, kind) VALUES ($1,$2) RETURNING id")
        .persistent(false)
        .bind(slug)
        .bind(kind)
        .fetch_one(&db.pool)
        .await?;
    Ok(rec.get("id"))
}

async fn ensure_sellable(db: &Db, kind: &str, product_id: i64) -> Result<i64> {
    if let Some(r) = sqlx::query("SELECT id FROM publicsellables WHERE product_id = $1")
        .persistent(false)
        .bind(product_id)
        .fetch_optional(&db.pool)
        .await?
    {
        return Ok(r.get("id"));
    }
    let rec =
        sqlx::query("INSERT INTO publicsellables (kind, product_id) VALUES ($1,$2) RETURNING id")
            .persistent(false)
            .bind(kind)
            .bind(product_id)
            .fetch_one(&db.pool)
            .await?;
    Ok(rec.get("id"))
}

async fn ensure_offer(
    db: &Db,
    sellable_id: i64,
    retailer_id: i64,
    sku: Option<&str>,
) -> Result<i64> {
    let rec = match sku {
        Some(s) => {
            sqlx::query(
                "SELECT id FROM publicoffers WHERE sellable_id=$1 AND retailer_id=$2 AND sku=$3",
            )
            .persistent(false)
            .bind(sellable_id)
            .bind(retailer_id)
            .bind(s)
            .fetch_optional(&db.pool)
            .await?
        }
        None => sqlx::query(
            "SELECT id FROM publicoffers WHERE sellable_id=$1 AND retailer_id=$2 AND sku IS NULL",
        )
        .persistent(false)
        .bind(sellable_id)
        .bind(retailer_id)
        .fetch_optional(&db.pool)
        .await?,
    };
    if let Some(r) = rec {
        return Ok(r.get("id"));
    }
    let inserted = sqlx::query(
        "INSERT INTO publicoffers (sellable_id, retailer_id, sku) VALUES ($1,$2,$3) RETURNING id",
    )
    .persistent(false)
    .bind(sellable_id)
    .bind(retailer_id)
    .bind(sku)
    .fetch_one(&db.pool)
    .await?;
    Ok(inserted.get("id"))
}

async fn ensure_offer_jurisdiction(
    db: &Db,
    offer_id: i64,
    jurisdiction_id: i64,
    currency_id: i64,
) -> Result<i64> {
    if let Some(rec) = sqlx::query(
        "SELECT id FROM publicoffer_jurisdictions WHERE offer_id=$1 AND jurisdiction_id=$2",
    )
    .persistent(false)
    .bind(offer_id)
    .bind(jurisdiction_id)
    .fetch_optional(&db.pool)
    .await?
    {
        return Ok(rec.get("id"));
    }
    let inserted = sqlx
        ::query(
            "INSERT INTO publicoffer_jurisdictions (offer_id, jurisdiction_id, currency_id) VALUES ($1,$2,$3) RETURNING id"
        )
        .persistent(false)
        .bind(offer_id)
        .bind(jurisdiction_id)
        .bind(currency_id)
        .fetch_one(&db.pool).await?;
    Ok(inserted.get("id"))
}

async fn ensure_provider_item(
    db: &Db,
    provider_id: i64,
    external_item_id: &str,
    payload: Option<Value>,
) -> Result<i64> {
    if let Some(rec) = sqlx::query(
        "SELECT id FROM publicprovider_items WHERE provider_id=$1 AND external_item_id=$2",
    )
    .persistent(false)
    .bind(provider_id)
    .bind(external_item_id)
    .fetch_optional(&db.pool)
    .await?
    {
        return Ok(rec.get("id"));
    }
    let inserted = sqlx
        ::query(
            "INSERT INTO publicprovider_items (provider_id, external_item_id, payload) VALUES ($1,$2,$3) RETURNING id"
        )
        .persistent(false)
        .bind(provider_id)
        .bind(external_item_id)
        .bind(payload)
        .fetch_one(&db.pool).await?;
    Ok(inserted.get("id"))
}

async fn link_provider_offer(
    db: &Db,
    provider_item_id: i64,
    offer_id: i64,
    confidence: Option<f32>,
) -> Result<i64> {
    if let Some(rec) = sqlx::query(
        "SELECT id FROM publicprovider_offers WHERE provider_item_id=$1 AND offer_id=$2",
    )
    .persistent(false)
    .bind(provider_item_id)
    .bind(offer_id)
    .fetch_optional(&db.pool)
    .await?
    {
        return Ok(rec.get("id"));
    }
    let inserted = sqlx
        ::query(
            "INSERT INTO publicprovider_offers (provider_item_id, offer_id, confidence) VALUES ($1,$2,$3) RETURNING id"
        )
        .persistent(false)
        .bind(provider_item_id)
        .bind(offer_id)
        .bind(confidence)
        .fetch_one(&db.pool).await?;
    Ok(inserted.get("id"))
}

async fn ingest_prices(
    db: &Db,
    price_rows: Vec<PriceRow>,
    cp_agent: &str,
    cp_priority: i16,
) -> Result<(Vec<i64>, usize)> {
    db.bulk_insert_prices(&price_rows).await?;
    let mut updates = Vec::with_capacity(price_rows.len());
    for r in &price_rows {
        updates.push(CurrentPriceRow {
            offer_jurisdiction_id: r.offer_jurisdiction_id,
            amount_minor: r.amount_minor,
            recorded_at: r.recorded_at,
            agent: cp_agent.to_string(),
            agent_priority: cp_priority,
        });
    }
    db.upsert_current_prices(&updates).await?;
    let oj_ids = price_rows.iter().map(|r| r.offer_jurisdiction_id).collect();
    Ok((oj_ids, updates.len()))
}
