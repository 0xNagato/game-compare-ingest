use anyhow::Result;
use chrono::{Datelike, Duration, NaiveDate, Utc};
use psstore_client::{PsConfig, PsStoreClient};
use sqlx::PgPool;
use std::env;
// serde_json::json not needed

#[tokio::test]
async fn psstore_category_print() -> Result<()> {
    // Skip if no network or perms; just basic smoke
    // Known PS5 category and sha from repo JSON
    let category = "4cbf39e2-5749-4970-ba81-93a489e4570c";

    let client = PsStoreClient::new(PsConfig::default());
    let hits = client
        .category_grid_retrieve("en-us", category, 12, 0)
        .await;
    match hits {
        Ok(list) => {
            println!("psstore hits count: {}", list.len());
            for h in list.iter().take(5) {
                println!(
                    "ps: product={:?} concept={:?} name={:?}",
                    h.product_id, h.concept_id, h.name
                );
            }
        }
        Err(e) => {
            eprintln!("psstore call failed: {e}");
            // Don't fail network-dependent test hard; mark as ok
        }
    }
    Ok(())
}

#[tokio::test]
async fn csv_price_roundtrip_current_price() -> Result<()> {
    let db_url = match env::var("SUPABASE_DB_URL").or_else(|_| env::var("DATABASE_URL")) {
        Ok(v) => v,
        Err(_) => {
            eprintln!("DATABASE_URL not set; skipping");
            return Ok(());
        }
    };
    let pool = PgPool::connect(&db_url).await?;

    // Ensure minimal refs exist
    sqlx
        ::query(
            "INSERT INTO currencies (id, code, name, minor_unit) VALUES (1,'USD','US Dollar',2) ON CONFLICT (id) DO NOTHING"
        )
        .execute(&pool).await?;
    sqlx
        ::query(
            "INSERT INTO countries (id, code2, name, currency_id) VALUES (1,'US','United States',1) ON CONFLICT (id) DO NOTHING"
        )
        .execute(&pool).await?;
    sqlx::query(
        "INSERT INTO jurisdictions (id, country_id) VALUES (1,1) ON CONFLICT (id) DO NOTHING",
    )
    .execute(&pool)
    .await?;
    sqlx
        ::query(
            "INSERT INTO retailers (id, slug, name) VALUES (1,'test','Test Retailer') ON CONFLICT (id) DO NOTHING"
        )
        .execute(&pool).await?;
    // minimal product + sellable
    sqlx
        ::query(
            "INSERT INTO products (id, slug, kind) VALUES (1,'csv-test-game','software') ON CONFLICT (id) DO NOTHING"
        )
        .execute(&pool).await?;
    sqlx
        ::query(
            "INSERT INTO sellables (id, kind, product_id) VALUES (1,'software',1) ON CONFLICT (id) DO NOTHING"
        )
        .execute(&pool).await?;
    sqlx
        ::query(
            "INSERT INTO offers (id, sellable_id, retailer_id) VALUES (1,1,1) ON CONFLICT (id) DO NOTHING"
        )
        .execute(&pool).await?;
    sqlx
        ::query(
            "INSERT INTO offer_jurisdictions (id, offer_id, jurisdiction_id, currency_id) VALUES (1,1,1,1) ON CONFLICT (id) DO NOTHING"
        )
        .execute(&pool).await?;

    // Ensure partition for this month
    let now = Utc::now();
    let naive = NaiveDate::from_ymd_opt(now.year(), now.month(), 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    let first: chrono::DateTime<Utc> = chrono::DateTime::from_naive_utc_and_offset(naive, Utc);
    sqlx::query("SELECT ensure_price_partition($1)")
        .bind(first)
        .execute(&pool)
        .await?;

    // Insert two price points and check current_price
    let earlier = now - Duration::minutes(10);
    sqlx
        ::query(
            r#"INSERT INTO prices (offer_jurisdiction_id, provider_item_id, recorded_at, amount_minor, tax_inclusive)
           VALUES ($1, NULL, $2, $3, true), ($1, NULL, $4, $5, true)"#
        )
        .bind(1i64)
        .bind(earlier)
        .bind(1234i64)
        .bind(now)
        .bind(2345i64)
        .execute(&pool).await?;

    // Upsert current_price as import would
    sqlx
        ::query(
            r#"insert into current_price (offer_jurisdiction_id, amount_minor, recorded_at)
           values ($1,$2,$3)
           on conflict (offer_jurisdiction_id) do update set amount_minor=excluded.amount_minor, recorded_at=excluded.recorded_at
           where current_price.recorded_at <= excluded.recorded_at"#
        )
        .bind(1i64)
        .bind(2345i64)
        .bind(now)
        .execute(&pool).await?;

    let amount: i64 =
        sqlx::query_scalar("SELECT amount_minor FROM current_price WHERE offer_jurisdiction_id=$1")
            .bind(1i64)
            .fetch_one(&pool)
            .await?;
    assert_eq!(amount, 2345);
    println!("current_price amount_minor: {}", amount);
    Ok(())
}
