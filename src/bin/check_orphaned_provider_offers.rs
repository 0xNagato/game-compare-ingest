use anyhow::Context;
use i_miss_rust::database_ops::db::Db;
use i_miss_rust::util::env::db_url;
use sqlx::Row;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let database_url = db_url().context("no database URL env vars set")?;
    let db = Db::connect(&database_url, 5).await?;

    // Check total provider_offers
    let total_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM provider_offers")
        .fetch_one(&db.pool)
        .await?;
    println!("Total provider_offers: {}", total_count);

    // Check orphaned provider_offers (offer_id no longer exists in offers)
    let orphaned_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(DISTINCT po.id) 
         FROM provider_offers po 
         WHERE NOT EXISTS (SELECT 1 FROM offers o WHERE o.id = po.offer_id)",
    )
    .fetch_one(&db.pool)
    .await?;
    println!(
        "Orphaned provider_offers (deleted offers): {}",
        orphaned_count
    );

    // Check if we have any provider_offers with offer_id = NULL
    let null_offer_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM provider_offers WHERE offer_id IS NULL")
            .fetch_one(&db.pool)
            .await?;
    println!("Provider_offers with NULL offer_id: {}", null_offer_count);

    // Sample orphaned provider_offers to see what got lost
    if orphaned_count > 0 {
        println!("\n=== Sample Orphaned Provider Offers ===");
        let samples = sqlx::query(
            "SELECT po.id, po.video_game_source_id, po.offer_id, po.confidence, 
                    pi.external_item_id, p.name as provider_name
             FROM provider_offers po
             JOIN provider_items pi ON pi.id = po.video_game_source_id
             JOIN providers p ON p.id = pi.provider_id
             WHERE NOT EXISTS (SELECT 1 FROM offers o WHERE o.id = po.offer_id)
             LIMIT 10",
        )
        .fetch_all(&db.pool)
        .await?;

        for row in samples {
            let po_id: i64 = row.get("id");
            let video_game_source_id: i64 = row.get("video_game_source_id");
            let offer_id: i64 = row.get("offer_id");
            let confidence: Option<f32> = row.get("confidence");
            let external_item_id: String = row.get("external_item_id");
            let provider_name: String = row.get("provider_name");

            println!(
                "  provider_offer id={}, video_game_source_id={} (ext_id={}, provider={}), deleted offer_id={}, confidence={:?}",
                po_id, video_game_source_id, external_item_id, provider_name, offer_id, confidence
            );
        }
    }

    Ok(())
}
