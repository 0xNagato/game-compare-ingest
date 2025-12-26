use anyhow::Result;
use i_miss_rust::database_ops::db::Db;
use i_miss_rust::database_ops::ingest_providers::ensure_retailer;
use sqlx::Row;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let db_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let db = Db::connect(&db_url, 2).await?;

    println!("\n=== Testing ensure_retailer with defaults ===");
    println!("Calling: ensure_retailer(db, \"PlayStation\", Some(\"playstation\"))");

    let id = ensure_retailer(&db, "PlayStation", Some("playstation")).await?;
    println!("Result: retailer_id = {}", id);

    // Query to see what we got
    let row = sqlx::query("SELECT id, name, slug FROM retailers WHERE id = $1")
        .bind(id)
        .fetch_one(&db.pool)
        .await?;

    let name: String = row.get("name");
    let slug: Option<String> = row.get("slug");

    println!("Retrieved: id={}, name='{}', slug='{:?}'", id, name, slug);

    Ok(())
}
