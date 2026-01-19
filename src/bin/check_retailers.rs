// src/bin/check_retailers.rs
use anyhow::{Context, Result};
use i_miss_rust::util::env::{self, db_url};
use sqlx::PgPool;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    env::bootstrap_cli("check_retailers");
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let database_url = db_url().context("no database URL env vars set")?;
    let pool = PgPool::connect(&database_url).await?;

    println!("\n=== Current Retailers ===");
    let retailers: Vec<(i64, String, String)> =
        sqlx::query_as("SELECT id, slug, name FROM retailers ORDER BY id")
            .fetch_all(&pool)
            .await?;

    for (id, slug, name) in &retailers {
        let offer_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM offers WHERE retailer_id = $1")
                .bind(id)
                .fetch_one(&pool)
                .await?;

        let oj_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(DISTINCT oj.id) 
             FROM offer_jurisdictions oj
             JOIN offers o ON o.id = oj.offer_id
             WHERE o.retailer_id = $1",
        )
        .bind(id)
        .fetch_one(&pool)
        .await?;

        println!(
            "  {} (id: {}, slug: {}): {} offers, {} jurisdictions",
            name, id, slug, offer_count, oj_count
        );
    }

    println!("\n=== Orphaned Data Check ===");
    let orphaned_offers: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM offers WHERE retailer_id NOT IN (SELECT id FROM retailers)",
    )
    .fetch_one(&pool)
    .await?;
    println!("  Orphaned offers: {}", orphaned_offers);

    // Check providers mapping
    println!("\n=== Provider -> Retailer Mapping ===");
    let provider_retailers: Vec<(String, String, String, String)> = sqlx::query_as(
        "SELECT p.name as provider_name, p.slug as provider_slug, 
                r.name as retailer_name, r.slug as retailer_slug
         FROM retailer_providers rp
         JOIN providers p ON p.id = rp.provider_id
         JOIN retailers r ON r.id = rp.retailer_id
         ORDER BY p.name",
    )
    .fetch_all(&pool)
    .await?;

    for (prov_name, prov_slug, ret_name, ret_slug) in provider_retailers {
        println!(
            "  Provider '{}' ({}) -> Retailer '{}' ({})",
            prov_name, prov_slug, ret_name, ret_slug
        );
    }

    // Check if there are provider_offers without valid retailer mapping
    println!("\n=== Provider Items without Retailer ===");
    let unmapped_provider_items: i64 = sqlx::query_scalar(
        "SELECT COUNT(DISTINCT pi.id)
         FROM provider_items pi
         WHERE NOT EXISTS (
             SELECT 1 FROM provider_offers po
             JOIN offers o ON o.id = po.offer_id
             JOIN retailers r ON r.id = o.retailer_id
             WHERE po.video_game_source_id = pi.id
         ) AND EXISTS (
             SELECT 1 FROM provider_offers po WHERE po.video_game_source_id = pi.id
         )",
    )
    .fetch_one(&pool)
    .await?;

    println!(
        "  Provider items with offers but no valid retailer: {}",
        unmapped_provider_items
    );

    Ok(())
}
