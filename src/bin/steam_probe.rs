use anyhow::Result;
use i_miss_rust::database_ops::db::Db;
use sqlx::Row;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

#[tokio::main]
async fn main() -> Result<()> {
    i_miss_rust::util::env::bootstrap_cli("steam_probe");
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(EnvFilter::from_default_env())
        .finish();
    let _ = tracing::subscriber::set_global_default(subscriber);

    let database_url =
        std::env::var("DATABASE_URL").or_else(|_| std::env::var("SUPABASE_DB_URL"))?;
    println!("[steam_probe] connecting to DB (env provided)");
    let db = Db::connect(&database_url, 4).await?;

    // Identify provider id (slug set to 'steam-store')
    let provider_id: Option<i64> = sqlx::query("SELECT id FROM providers WHERE slug = $1")
        .persistent(false)
        .bind("steam-store")
        .fetch_optional(&db.pool)
        .await?
        .map(|r| r.get(0));
    let Some(pid) = provider_id else {
        println!("steam provider not found");
        return Ok(());
    };

    // Counts
    let item_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM provider_items WHERE provider_id = $1")
            .persistent(false)
            .bind(pid)
            .fetch_one(&db.pool)
            .await?;
    let media_count: i64 = sqlx
        ::query_scalar(
            "SELECT COUNT(*) FROM vg_source_media_links WHERE video_game_source_id IN (SELECT id FROM provider_items WHERE provider_id = $1)"
        )
        .persistent(false)
        .bind(pid)
        .fetch_one(&db.pool).await?;
    let price_rows: i64 = sqlx
        ::query_scalar(
            "SELECT COUNT(*) FROM prices p JOIN provider_items pi ON p.video_game_source_id = pi.id WHERE pi.provider_id = $1"
        )
        .persistent(false)
        .bind(pid)
        .fetch_one(&db.pool).await?;
    let current_count: i64 = sqlx
        ::query_scalar(
            "SELECT COUNT(*) FROM current_price cp JOIN offer_jurisdictions oj ON cp.offer_jurisdiction_id = oj.id JOIN offers o ON oj.offer_id = o.id JOIN provider_offers pof ON pof.offer_id = o.id JOIN provider_items pi ON pof.video_game_source_id = pi.id WHERE pi.provider_id = $1"
        )
        .persistent(false)
        .bind(pid)
        .fetch_one(&db.pool).await?;

    println!(
        "steam evidence => provider_id={} provider_items={} media_links={} price_rows={} current_price_rows={}",
        pid, item_count, media_count, price_rows, current_count
    );

    // Sample prices
    let sample_prices = sqlx
        ::query(
            r#"SELECT p.recorded_at, p.amount_minor, pi.external_id, (p.meta->>'kind') AS kind FROM prices p JOIN provider_items pi ON p.video_game_source_id = pi.id WHERE pi.provider_id = $1 ORDER BY p.recorded_at DESC LIMIT 5"#
        )
        .persistent(false)
        .bind(pid)
        .fetch_all(&db.pool).await?;
    println!("latest price samples:");
    for r in sample_prices {
        println!(
            "{} | {} | {} | {}",
            r.get::<chrono::DateTime<chrono::Utc>, _>(0),
            r.get::<i64, _>(1),
            r.get::<String, _>(2),
            r.get::<String, _>(3)
        );
    }

    // Sample media (show kind + title for classification)
    let sample_media = sqlx
        ::query(
            "SELECT kind, title, url FROM vg_source_media_links WHERE video_game_source_id IN (SELECT id FROM provider_items WHERE provider_id = $1) LIMIT 5"
        )
        .persistent(false)
        .bind(pid)
        .fetch_all(&db.pool).await?;
    println!("media samples:");
    for r in sample_media {
        println!(
            "{} | {} | {}",
            r.get::<String, _>(0),                             // kind
            r.get::<Option<String>, _>(1).unwrap_or_default(), // title
            r.get::<String, _>(2)                              // url
        );
    }

    Ok(())
}
