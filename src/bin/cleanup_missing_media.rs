use anyhow::Result;
use i_miss_rust::database_ops::db::Db;
use sqlx::Row;

#[tokio::main]
async fn main() -> Result<()> {
    i_miss_rust::util::env::bootstrap_cli("cleanup_missing_media");
    dotenv::dotenv().ok();
    let db_url = std::env::var("SUPABASE_DB_URL").or_else(|_| std::env::var("DATABASE_URL"))?;
    let db = Db::connect(&db_url, 5).await?;

    let limit: i64 = std::env::var("CLEANUP_LIMIT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000);

    let rows = sqlx::query(
        "SELECT pi.id, pi.external_item_id, COUNT(pml.id) AS media_count\
         FROM public.provider_items pi\
         LEFT JOIN public.provider_media_links pml ON pml.video_game_source_id = pi.id\
         GROUP BY pi.id, pi.external_item_id\
         HAVING COUNT(pml.id) = 0\
         ORDER BY pi.id DESC\
         LIMIT $1",
    )
    .bind(limit)
    .fetch_all(&db.pool)
    .await?;

    println!("Found {} provider_items without media", rows.len());
    for r in rows {
        let id: i64 = r.get("id");
        let ext: String = r
            .get::<Option<String>, _>("external_item_id")
            .unwrap_or_default();
        println!("provider_item id={} ext={}", id, ext);
    }

    Ok(())
}
