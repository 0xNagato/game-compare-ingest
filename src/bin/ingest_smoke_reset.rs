//! ingest_smoke_reset.rs
//! Destructive variant of ingestion smoke. If env I_UNDERSTAND=drop it will:
//! 1. Drop all tables, views, enums, and helper schemas (similar to db_reset tool logic subset)
//! 2. Re-run migrations (AUTO_MIGRATE path via Db::connect)
//! 3. Execute sample_ingest_flow and print results.
//! If I_UNDERSTAND is not "drop" or "DROP" it aborts safely.

use anyhow::{Context, Result};
use i_miss_rust::database_ops::{db::Db, ingest_providers::sample_ingest_flow};
use sqlx::Row;
use tracing::{info, warn};
use tracing_subscriber::{EnvFilter, FmtSubscriber};

#[tokio::main]
async fn main() -> Result<()> {
    i_miss_rust::util::env::bootstrap_cli("ingest_smoke_reset");
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(EnvFilter::from_default_env())
        .finish();
    let _ = tracing::subscriber::set_global_default(subscriber);

    if std::env::var("I_UNDERSTAND")
        .unwrap_or_default()
        .to_lowercase()
        != "drop"
    {
        warn!(
            "I_UNDERSTAND != drop; aborting destructive reset. Set I_UNDERSTAND=drop to proceed."
        );
        return Ok(());
    }

    let database_url = std::env::var("DATABASE_URL").context("DATABASE_URL required")?;
    // Connect without auto migrate first (we'll drop), then reconnect with migrations.
    // Initial connect; statement cache disabled internally in Db::connect
    let db = Db::connect(&database_url, 5).await?;
    // Proactively deallocate any prepared statements (defensive)
    let _ = sqlx::raw_sql("DEALLOCATE ALL;").execute(&db.pool).await;

    info!("Beginning destructive reset");
    drop_all(&db).await?;
    info!("Schema dropped; reconnecting to apply migrations");

    // Reconnect (fresh pool) to ensure objects recreated cleanly
    let db = Db::connect(&database_url, 5).await?;
    info!("Migrations applied; running sample_ingest_flow");
    let ingest_res = sample_ingest_flow(&db).await?;
    println!(
        "offer_jurisdiction_ids={:?}",
        ingest_res.offer_jurisdiction_ids
    );
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

    Ok(())
}

async fn drop_all(db: &Db) -> Result<()> {
    // Drop view(s)
    sqlx::raw_sql("DROP VIEW IF EXISTS video_games_enriched CASCADE")
        .execute(&db.pool)
        .await?;

    // Collect partition children to drop first
    let price_children = sqlx
        ::query(
            "SELECT inhrelid::regclass::text AS child FROM pg_inherits WHERE inhparent='prices'::regclass"
        )
        .fetch_all(&db.pool).await?;
    for r in price_children {
        let child: String = r.get("child");
        let stmt = format!("DROP TABLE IF EXISTS {} CASCADE", child);
        sqlx::raw_sql(&stmt).execute(&db.pool).await?;
    }

    // Tables to drop (CASCADE for simplicity). Order from leaves upwards reduces work.
    let tables = [
        "prices",
        "current_price",
        "vg_source_media_links",
        "game_images",
        "game_videos",
        "video_game_ratings_by_locale",
        "video_game_title_sources",
        "provider_offers",
        "provider_items",
        "provider_ingest_runs",
        "offers",
        "offer_jurisdictions",
        "sellables",
        "video_games",
        "game_consoles",
        "video_game_titles",
        "software",
        "hardware",
        "platforms",
        "retailer_providers",
        "providers",
        "retailers",
        "alerts",
        "tax_rules",
        "jurisdictions",
        "countries",
        "currencies",
        "users",
        "exchange_rates",
    ];
    for t in tables {
        sqlx::raw_sql(&format!("DROP TABLE IF EXISTS {} CASCADE", t))
            .execute(&db.pool)
            .await?;
    }

    // Drop enums (ignore errors)
    for ty in ["cmp_op", "sellable_kind"] {
        let _ = sqlx::raw_sql(&format!("DROP TYPE IF EXISTS {}", ty))
            .execute(&db.pool)
            .await;
    }
    Ok(())
}
