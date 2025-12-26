use anyhow::Result;
use i_miss_rust::database_ops::db::Db;
use i_miss_rust::util::env;
use sqlx::Row;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    env::bootstrap_cli("verify_platforms");
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        .init();

    let pg_url = std::env::var("DATABASE_URL").or_else(|_| std::env::var("SUPABASE_DB_URL"))?;
    let max_conns = std::env::var("DB_MAX_CONNS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8);
    let db = Db::connect(&pg_url, max_conns).await?;

    // Count duplicates by canonical_code (should be 0 after real dedupe)
    let dupe_rows = sqlx
        ::query(
            "SELECT canonical_code, COUNT(*) AS ct FROM platforms WHERE canonical_code IS NOT NULL GROUP BY canonical_code HAVING COUNT(*) > 1 ORDER BY ct DESC"
        )
        .persistent(false)
        .fetch_all(&db.pool).await?;
    if dupe_rows.is_empty() {
        info!("No duplicate canonical_code groups detected");
    } else {
        for r in dupe_rows.iter() {
            let code: String = r.get("canonical_code");
            let ct: i64 = r.get("ct");
            info!(%code, ct, "duplicate group");
        }
    }

    // Verify all video_games.platform_id exist in platforms
    let missing = sqlx
        ::query(
            "SELECT vg.platform_id FROM video_games vg LEFT JOIN platforms p ON p.id=vg.platform_id WHERE p.id IS NULL LIMIT 10"
        )
        .persistent(false)
        .fetch_all(&db.pool).await?;
    if missing.is_empty() {
        info!("All video_games.platform_id values have matching platforms rows");
    } else {
        info!(
            count = missing.len(),
            "Missing platform references detected (sample logged)"
        );
        for r in missing.iter() {
            info!(missing_platform_id = r.get::<i64, _>("platform_id"));
        }
    }

    // Emit platform counts summary
    let counts = sqlx::query("SELECT COUNT(*) AS n FROM platforms")
        .persistent(false)
        .fetch_one(&db.pool)
        .await?;
    let n: i64 = counts.get("n");
    info!(platform_rows = n, "platform table row count");
    Ok(())
}
