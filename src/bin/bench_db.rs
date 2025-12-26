use anyhow::Result;
use i_miss_rust::database_ops::db::Db;
use i_miss_rust::util::env;
use sqlx::Row;
use std::time::Instant;
use tracing::info;

// Simple benchmark harness: counts rows in target tables and times representative queries.
// Configure tables via BENCH_TABLES (comma-separated). Default core tables.
// Set BENCH_ITER= to control repeated timing loops.
#[tokio::main]
async fn main() -> Result<()> {
    env::bootstrap_cli("bench_db");
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        .init();

    let pg_url = env::db_url()?;
    let max_conns = std::env::var("DB_MAX_CONNS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8);
    let db = Db::connect(&pg_url, max_conns).await?;

    let tables_env = std::env::var("BENCH_TABLES").unwrap_or_else(|_| {
        "products,video_game_titles,video_games,platforms,prices,current_price".to_string()
    });
    let tables: Vec<&str> = tables_env
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();
    let iter: usize = std::env::var("BENCH_ITER")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3);

    info!(tables=?tables, iter, "starting benchmark harness");

    // Row counts
    for t in &tables {
        let sql = format!("SELECT COUNT(1) AS n FROM public.{}", t);
        let start = Instant::now();
        let row = sqlx::query(&sql)
            .persistent(false)
            .fetch_one(&db.pool)
            .await?;
        let n: i64 = row.get("n");
        let dur = start.elapsed();
        info!(table = *t, rows = n, ms = dur.as_millis(), "row count");
    }

    // Representative timed query: latest price per offer_jurisdiction (DISTINCT ON)
    if tables.iter().any(|t| *t == "prices") {
        let distinct_sql = "SELECT DISTINCT ON (offer_jurisdiction_id) offer_jurisdiction_id, amount_minor, recorded_at FROM prices ORDER BY offer_jurisdiction_id, recorded_at DESC LIMIT 500";
        for i in 0..iter {
            let start = Instant::now();
            let rows = sqlx::query(distinct_sql)
                .persistent(false)
                .fetch_all(&db.pool)
                .await?;
            let dur = start.elapsed();
            info!(
                iteration = i + 1,
                fetched = rows.len(),
                ms = dur.as_millis(),
                q = "latest_prices_distinct_on",
                "timed query"
            );
        }
    }

    // Representative join query: titles with platform counts
    if tables.iter().any(|t| *t == "video_games") {
        let join_sql = "SELECT t.id, t.title, COUNT(vg.id) AS platform_count FROM video_game_titles t JOIN video_games vg ON vg.title_id=t.id GROUP BY t.id, t.title ORDER BY platform_count DESC LIMIT 100";
        for i in 0..iter {
            let start = Instant::now();
            let rows = sqlx::query(join_sql)
                .persistent(false)
                .fetch_all(&db.pool)
                .await?;
            let dur = start.elapsed();
            info!(
                iteration = i + 1,
                fetched = rows.len(),
                ms = dur.as_millis(),
                q = "title_platform_counts",
                "timed query"
            );
        }
    }

    info!("benchmark complete");
    Ok(())
}
