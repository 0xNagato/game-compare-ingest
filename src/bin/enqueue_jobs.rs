use anyhow::Result;
use serde_json::json;
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    i_miss_rust::util::env::bootstrap_cli("enqueue_jobs");
    dotenv::dotenv().ok();
    let db_url = env::var("SUPABASE_DB_URL").or_else(|_| env::var("DATABASE_URL"))?;
    let regions_raw = env::var("PS_STORE_REGIONS").unwrap_or_else(|_| "en-us".into());
    let regions: Vec<String> = regions_raw
        .split(|c: char| (c == ',' || c == ' '))
        .filter(|s| !s.is_empty())
        .map(|s| s.trim().to_lowercase())
        .collect();
    let pages: u32 = env::var("PS_MAX_PAGES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    let page_size: u32 = env::var("PS_PAGE_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100);

    let cat_ps4 =
        env::var("PS4_CATEGORY").unwrap_or_else(|_| "44d8bb20-653e-431e-8ad0-c0a365f68d2f".into());
    let cat_ps5 =
        env::var("PS5_CATEGORY").unwrap_or_else(|_| "4cbf39e2-5749-4970-ba81-93a489e4570c".into());
    // Prefer PSSTORE_SHA256 override if available
    let sha = env::var("PSSTORE_SHA256")
        .or_else(|_| env::var("PS_HASH"))
        .unwrap_or_else(|_| {
            "9845afc0dbaab4965f6563fffc703f588c8e76792000e8610843b8d3ee9c4c09".into()
        });

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .connect(&db_url)
        .await?;

    for r in regions {
        let payload = json!({
            "region": r,
            "pages": pages,
            "page_size": page_size,
            "cat_ps4": cat_ps4,
            "cat_ps5": cat_ps5,
            "sha": sha,
        });
        // Dedup key: kind + region + month
        let dedupe_key = format!("psstore.region:{}:{}:{}", r, pages, page_size);
        sqlx
            ::query(
                "INSERT INTO ingestion_jobs(kind, dedupe_key, payload) VALUES ($1,$2,$3) \
             ON CONFLICT (dedupe_key) DO UPDATE SET payload=EXCLUDED.payload, scheduled_at=now(), status='queued', updated_at=now()"
            )
            .bind("psstore.region")
            .bind(&dedupe_key)
            .bind(&payload)
            .execute(&pool).await?;
        println!("Enqueued: {}", dedupe_key);
    }
    Ok(())
}
