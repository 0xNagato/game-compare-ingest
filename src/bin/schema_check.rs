use anyhow::Result;
use i_miss_rust::util::env::{self, db_url};
use sqlx::{PgPool, Row};
use tracing_subscriber::EnvFilter;

async fn print_columns(pool: &PgPool, table: &str) -> Result<()> {
    let rows = sqlx::query(
        "SELECT column_name, is_nullable, data_type \
         FROM information_schema.columns \
         WHERE table_schema = ANY (current_schemas(true)) AND table_name = $1 \
         ORDER BY ordinal_position",
    )
    .persistent(false)
    .bind(table)
    .fetch_all(pool)
    .await?;

    if rows.is_empty() {
        println!("columns {:30} (none)", table);
        return Ok(());
    }

    println!("columns {:30}", table);
    for row in rows {
        let name: String = row.get("column_name");
        let nullable: String = row.get("is_nullable");
        let dtype: String = row.get("data_type");
        println!("  - {:24} {:8} {}", name, nullable, dtype);
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    env::bootstrap_cli("schema_check");
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let db_url_val = db_url()?;
    let pool = PgPool::connect(&db_url_val).await?;

    // Key tables to verify
    let tables = [
        "currencies",
        "countries",
        "jurisdictions",
        // Legacy (Laravel / PHP compat) tables
        "sku_regions",
        "region_prices",
        "products",
        "software",
        "hardware",
        "platforms",
        "video_game_titles",
        "video_games",
        "game_consoles",
        "sellables",
        "offers",
        "offer_jurisdictions",
        "provider_items",
        "prices",
        "current_price",
        "alerts",
        "game_images",
        "game_videos",
        "vg_source_media_links",
        "video_game_ratings_by_locale",
        "video_game_title_sources",
    ];

    for t in tables.iter() {
        let exists: bool = sqlx::query(
            "SELECT EXISTS (\
                    SELECT 1 FROM information_schema.tables \
                    WHERE table_schema = ANY (current_schemas(true)) AND table_name=$1\
                 )",
        )
        .bind(t)
        .fetch_one(&pool)
        .await?
        .get(0);
        println!("table {:30} exists={}", t, exists);
    }

    // Column detail for debugging schema variants (especially for legacy Supabase DBs).
    // Keep this list small to avoid noisy output.
    let _ = print_columns(&pool, "video_game_titles").await;
    let _ = print_columns(&pool, "video_games").await;
    let _ = print_columns(&pool, "video_game_sources").await;
    let _ = print_columns(&pool, "prices").await;
    let _ = print_columns(&pool, "current_price").await;
    let _ = print_columns(&pool, "game_videos").await;

    // Partition sample: count partitions
    let part_count: i64 = sqlx
        ::query(
            "SELECT count(*) FROM pg_class c JOIN pg_inherits i ON c.oid=i.inhrelid JOIN pg_class p ON p.oid=i.inhparent WHERE p.relname='prices'"
        )
        .fetch_one(&pool).await?
        .get(0);
    println!("prices partitions count={}", part_count);

    Ok(())
}
