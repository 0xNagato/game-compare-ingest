//! media_health.rs
//! Lightweight diagnostic binary to report lifted URL coverage and media integrity.
//! Usage:
//!   DATABASE_URL=postgres://... cargo run --bin media_health
//! Output: JSON summary to stdout.

use anyhow::{Context, Result};
use i_miss_rust::util::env as env_util;
use serde::Serialize;
use sqlx::{Pool, Postgres};
use std::env;
use tracing::{info, Level};
use tracing_subscriber::EnvFilter;

#[derive(Serialize)]
struct MediaHealth {
    total_media: i64,
    lifted_any: i64,
    missing_original: i64,
    recent_lifted_30d: i64,
    percent_with_any: f64,
    percent_missing_original: f64,
    percent_recent_lifted: f64,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_util::bootstrap_cli("media_health");
    init_tracing();
    let db_url = env_util::db_url_prefer_session()
        .context("Set SUPABASE_IPV6_DB / SUPABASE_DB_URL / DATABASE_URL / DB_URL")?;
    let pool = Pool::<Postgres>::connect(&db_url).await?;

    // Use simple dynamic queries (avoid compile-time prepare requirement) for portability.
    let total_media: i64 = sqlx::query_scalar("SELECT count(*) FROM game_media")
        .fetch_one(&pool)
        .await?;
    let lifted_any: i64 = sqlx
        ::query_scalar(
            "SELECT count(*) FROM game_media WHERE original_url IS NOT NULL OR thumbnail_url IS NOT NULL OR stream_url IS NOT NULL OR poster_url IS NOT NULL"
        )
        .fetch_one(&pool).await?;
    let missing_original: i64 =
        sqlx::query_scalar("SELECT count(*) FROM game_media WHERE original_url IS NULL")
            .fetch_one(&pool)
            .await?;
    let recent_lifted_30d: i64 = sqlx
        ::query_scalar(
            "SELECT count(*) FROM game_media WHERE created_at > now() - interval '30 days' AND (original_url IS NOT NULL OR thumbnail_url IS NOT NULL)"
        )
        .fetch_one(&pool).await?;

    let percent_with_any = pct(lifted_any, total_media);
    let percent_missing_original = pct(missing_original, total_media);
    let percent_recent_lifted = pct(recent_lifted_30d, total_media);

    let summary = MediaHealth {
        total_media,
        lifted_any,
        missing_original,
        recent_lifted_30d,
        percent_with_any,
        percent_missing_original,
        percent_recent_lifted,
    };
    // Threshold exit codes (for CI):
    // MEDIA_HEALTH_MIN_PERCENT_WITH_ANY (float, default 0.0)
    // MEDIA_HEALTH_MAX_PERCENT_MISSING_ORIGINAL (float, optional)
    let min_with_any: f64 = env::var("MEDIA_HEALTH_MIN_PERCENT_WITH_ANY")
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.0);
    let max_missing_original: Option<f64> = env::var("MEDIA_HEALTH_MAX_PERCENT_MISSING_ORIGINAL")
        .ok()
        .and_then(|s| s.parse::<f64>().ok());

    let json_out = serde_json::to_string_pretty(&summary)?;
    println!("{}", json_out);
    info!(min_with_any, max_missing_original=?max_missing_original, "media_health emitted");

    let mut exit_code: i32 = 0;
    if summary.percent_with_any < min_with_any {
        eprintln!(
            "ERROR: percent_with_any {:.2}% below required minimum {:.2}%",
            summary.percent_with_any, min_with_any
        );
        exit_code = 2; // degraded
    }
    if let Some(max_missing) = max_missing_original {
        if summary.percent_missing_original > max_missing {
            eprintln!(
                "ERROR: percent_missing_original {:.2}% exceeds allowed maximum {:.2}%",
                summary.percent_missing_original, max_missing
            );
            // escalate exit code (pick highest severity)
            exit_code = exit_code.max(3);
        }
    }

    if exit_code != 0 {
        // Use std::process::exit to propagate failure status to CI.
        std::process::exit(exit_code);
    }
    Ok(())
}

fn pct(part: i64, whole: i64) -> f64 {
    if whole == 0 {
        0.0
    } else {
        ((part as f64) * 100.0) / (whole as f64)
    }
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_max_level(Level::INFO)
        .try_init();
}
