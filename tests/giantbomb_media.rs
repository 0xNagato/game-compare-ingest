use anyhow::Result;
use i_miss_rust::database_ops::db::Db;
use i_miss_rust::database_ops::giantbomb::ingest::ingest_from_file;
use sqlx::Row;
use std::env;

// Integration test: ingest a tiny GiantBomb dump and assert a game_media row is created.
// Requires a prepared JSON fixture path set via GIANTBOMB_FIXTURE (defaults to tests/data/giantbomb_sample.json).
#[tokio::test]
async fn giantbomb_media_upsert() -> Result<()> {
    dotenv::dotenv().ok();

    let run_db_tests = env::var("RUN_DB_TESTS")
        .ok()
        .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false);
    if !run_db_tests {
        eprintln!("skipping: set RUN_DB_TESTS=1 to run giantbomb_media_upsert");
        return Ok(());
    }

    let database_url = match env::var("SUPABASE_DB_URL").or_else(|_| env::var("DATABASE_URL")) {
        Ok(v) => v,
        Err(_) => {
            eprintln!(
                "skipping: set SUPABASE_DB_URL or DATABASE_URL to run giantbomb_media_upsert"
            );
            return Ok(());
        }
    };
    let db = Db::connect(&database_url, 5).await?;

    // Ensure prerequisite currency/country/jurisdiction minimal for video_game creation side-effects.
    ensure_currency(&db, "USD", "US Dollar", 2).await?;
    let country_id = ensure_country(&db, "US", "United States", "USD").await?;
    ensure_national_jurisdiction(&db, country_id).await?;

    let fixture =
        env::var("GIANTBOMB_FIXTURE").unwrap_or_else(|_| "tests/data/giantbomb_sample.json".into());

    // Ingest only 1 item for speed.
    let _created = ingest_from_file(&db, &fixture, Some(1)).await?;

    // Verify at least one game_media row sourced from giantbomb.
    // Provider stored using media_source enum variant 'giant_bomb'
    let row = sqlx::query("SELECT COUNT(*) AS cnt FROM game_media WHERE source='giant_bomb'")
        .persistent(false)
        .fetch_one(&db.pool)
        .await?;
    let cnt: i64 = row.get("cnt");
    assert!(
        cnt >= 1,
        "expected >=1 giantbomb game_media rows, got {}",
        cnt
    );
    Ok(())
}

async fn ensure_currency(db: &Db, code: &str, name: &str, minor_unit: i16) -> Result<i64> {
    if let Some(rec) = sqlx::query("SELECT id FROM currencies WHERE code=$1")
        .persistent(false)
        .bind(code)
        .fetch_optional(&db.pool)
        .await?
    {
        return Ok(rec.get("id"));
    }
    let rec =
        sqlx::query("INSERT INTO currencies (code,name,minor_unit) VALUES ($1,$2,$3) RETURNING id")
            .persistent(false)
            .bind(code)
            .bind(name)
            .bind(minor_unit)
            .fetch_one(&db.pool)
            .await?;
    Ok(rec.get("id"))
}

async fn ensure_country(db: &Db, code2: &str, name: &str, currency_code: &str) -> Result<i64> {
    // Lookup currency id first
    let cur_id = sqlx::query("SELECT id FROM currencies WHERE code=$1")
        .persistent(false)
        .bind(currency_code)
        .fetch_one(&db.pool)
        .await?
        .get::<i64, _>("id");
    if let Some(rec) = sqlx::query("SELECT id FROM countries WHERE code2=$1")
        .persistent(false)
        .bind(code2)
        .fetch_optional(&db.pool)
        .await?
    {
        return Ok(rec.get("id"));
    }
    let rec = sqlx::query(
        "INSERT INTO countries (code2,name,currency_id) VALUES ($1,$2,$3) RETURNING id",
    )
    .persistent(false)
    .bind(code2)
    .bind(name)
    .bind(cur_id)
    .fetch_one(&db.pool)
    .await?;
    Ok(rec.get("id"))
}

async fn ensure_national_jurisdiction(db: &Db, country_id: i64) -> Result<i64> {
    if let Some(rec) =
        sqlx::query("SELECT id FROM jurisdictions WHERE country_id=$1 AND region_code IS NULL")
            .persistent(false)
            .bind(country_id)
            .fetch_optional(&db.pool)
            .await?
    {
        return Ok(rec.get("id"));
    }
    let rec = sqlx::query(
        "INSERT INTO jurisdictions (country_id,region_code) VALUES ($1,NULL) RETURNING id",
    )
    .persistent(false)
    .bind(country_id)
    .fetch_one(&db.pool)
    .await?;
    Ok(rec.get("id"))
}
