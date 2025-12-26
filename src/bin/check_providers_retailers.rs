use anyhow::Context;
use i_miss_rust::database_ops::db::Db;
use i_miss_rust::util::env::db_url;
use sqlx::Row;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let database_url = db_url().context("no database URL env vars set")?;
    let db = Db::connect(&database_url, 5).await?;

    println!("=== Providers ===");
    let providers = sqlx::query("SELECT id, slug, name, kind FROM providers ORDER BY id")
        .fetch_all(&db.pool)
        .await?;

    for row in providers {
        let id: i64 = row.get("id");
        let slug: String = row.get("slug");
        let name: String = row.get("name");
        let kind: Option<String> = row.get("kind");
        println!("  {} - {} ({}), kind: {:?}", id, name, slug, kind);
    }

    println!("\n=== Retailers ===");
    let retailers = sqlx::query("SELECT id, slug, name FROM retailers ORDER BY id")
        .fetch_all(&db.pool)
        .await?;

    for row in retailers {
        let id: i64 = row.get("id");
        let slug: String = row.get("slug");
        let name: String = row.get("name");
        println!("  {} - {} ({})", id, name, slug);
    }

    println!("\n=== Existing Retailer Provider Mappings ===");
    let mappings = sqlx::query(
        "SELECT rp.id, p.name as provider_name, p.slug as provider_slug, 
                r.name as retailer_name, r.slug as retailer_slug
         FROM retailer_providers rp
         JOIN providers p ON p.id = rp.provider_id
         JOIN retailers r ON r.id = rp.retailer_id
         ORDER BY rp.id",
    )
    .fetch_all(&db.pool)
    .await?;

    if mappings.is_empty() {
        println!("  (none)");
    } else {
        for row in mappings {
            let id: i64 = row.get("id");
            let provider_name: String = row.get("provider_name");
            let provider_slug: String = row.get("provider_slug");
            let retailer_name: String = row.get("retailer_name");
            let retailer_slug: String = row.get("retailer_slug");
            println!(
                "  {} - {} ({}) -> {} ({})",
                id, provider_name, provider_slug, retailer_name, retailer_slug
            );
        }
    }

    Ok(())
}
