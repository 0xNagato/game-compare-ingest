//! ingest_smoke.rs
//! Minimal end-to-end ingestion smoke: builds schema (migrations), runs sample_ingest_flow,
//! prints resulting offer_jurisdiction_id + current_price row, and confirms partition existence.

use anyhow::Result;
use i_miss_rust::database_ops::{db::Db, ingest_providers::sample_ingest_flow};
use sqlx::Row;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

#[tokio::main]
async fn main() -> Result<()> {
    i_miss_rust::util::env::bootstrap_cli("ingest_smoke");
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(EnvFilter::from_default_env())
        .finish();
    let _ = tracing::subscriber::set_global_default(subscriber);

    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL required");
    let db = Db::connect(&database_url, 30).await?;

    let ingest_res = sample_ingest_flow(&db).await?;
    println!(
        "offer_jurisdiction_ids={:?}",
        ingest_res.offer_jurisdiction_ids
    );

    // Pull current_price row(s)
    for oj in &ingest_res.offer_jurisdiction_ids {
        if
            let Some(rec) = sqlx
                ::query(
                    "SELECT offer_jurisdiction_id, amount_minor, recorded_at FROM current_price WHERE offer_jurisdiction_id=$1"
                )
                .bind(oj)
                .fetch_optional(&db.pool).await?
        {
            let id: i64 = rec.get("offer_jurisdiction_id");
            let amt: i64 = rec.get("amount_minor");
            let ts: chrono::DateTime<chrono::Utc> = rec.get("recorded_at");
            println!("current_price row: oj_id={} amount_minor={} recorded_at={}", id, amt, ts);
        }
    }

    // Verify partition exists for the recorded month
    let part_names = sqlx
        ::query(
            "SELECT inhrelid::regclass::text AS child FROM pg_inherits WHERE inhparent='prices'::regclass"
        )
        .fetch_all(&db.pool).await?;
    println!(
        "prices partitions count={} sample=[{}]",
        part_names.len(),
        part_names
            .get(0)
            .map(|r| r.get::<String, _>("child"))
            .unwrap_or_default()
    );

    Ok(())
}
