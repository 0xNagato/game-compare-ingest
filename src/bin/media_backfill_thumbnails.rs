//! media_backfill_thumbnails.rs
//! Hydrate missing thumbnail_url for game_media rows using provider_data heuristics.
//! Dry-run: BACKFILL_THUMBS_DRY_RUN=1
//! Limit: BACKFILL_THUMBS_LIMIT (default 5000)
//! Concurrency kept simple (sequential) to minimize DB contention.

use anyhow::{Context, Result};
use i_miss_rust::util::env as env_util;
use serde_json::Value;
use sqlx::{Pool, Postgres, Row};
use std::env;
use tracing::{info, warn};
use tracing_subscriber::{fmt, EnvFilter};

#[tokio::main]
async fn main() -> Result<()> {
    env_util::bootstrap_cli("media_backfill_thumbnails");
    init_tracing();
    let db_url = env::var("DATABASE_URL").context("DATABASE_URL required")?;
    let pool = Pool::<Postgres>::connect(&db_url).await?;
    let dry_run = env_flag("BACKFILL_THUMBS_DRY_RUN");
    let limit: i64 = env::var("BACKFILL_THUMBS_LIMIT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5000);

    // Fetch candidate rows missing thumbnail_url
    let rows = sqlx
        ::query(
            "SELECT video_game_id, source, external_id, provider_data, url, original_url FROM game_media WHERE thumbnail_url IS NULL LIMIT $1"
        )
        .bind(limit)
        .fetch_all(&pool).await?;
    let mut updates: Vec<(i64, String, String, String)> = Vec::new();
    for r in rows.iter() {
        let vg_id: i64 = r.get("video_game_id");
        let source: String = r.get("source");
        let external_id: String = r.get("external_id");
        let url: Option<String> = r.try_get("url").ok();
        let original_url: Option<String> = r.try_get("original_url").ok();
        let pdata: Value = r.get("provider_data");
        if let Some(th) = derive_thumb(&pdata, url.as_deref(), original_url.as_deref(), &source) {
            updates.push((vg_id, source.clone(), external_id.clone(), th));
        }
    }
    info!(
        candidates = rows.len(),
        will_update = updates.len(),
        dry_run,
        "thumbnail backfill scan complete"
    );

    if !dry_run && !updates.is_empty() {
        // Batch update using UNNEST arrays to reduce round trips.
        let vg_ids: Vec<i64> = updates.iter().map(|u| u.0).collect();
        let sources: Vec<&str> = updates.iter().map(|u| u.1.as_str()).collect();
        let external_ids: Vec<&str> = updates.iter().map(|u| u.2.as_str()).collect();
        let thumbs: Vec<&str> = updates.iter().map(|u| u.3.as_str()).collect();
        let updated = sqlx::query(
            r#"WITH data AS (
                SELECT UNNEST($1::bigint[]) AS video_game_id,
                       UNNEST($2::text[]) AS source,
                       UNNEST($3::text[]) AS external_id,
                       UNNEST($4::text[]) AS thumbnail_url
            )
            UPDATE game_media gm
            SET thumbnail_url = data.thumbnail_url
            FROM data
            WHERE gm.video_game_id = data.video_game_id
              AND gm.source = data.source::media_source
              AND gm.external_id = data.external_id"#,
        )
        .bind(&vg_ids)
        .bind(&sources)
        .bind(&external_ids)
        .bind(&thumbs)
        .execute(&pool)
        .await?;
        info!(rows = updated.rows_affected(), "thumbnail backfill applied");
    } else if dry_run {
        info!("dry-run mode: no updates applied");
    }
    Ok(())
}

fn env_flag(key: &str) -> bool {
    env::var(key)
        .ok()
        .map(|v| (v == "1" || v.eq_ignore_ascii_case("true")))
        .unwrap_or(false)
}

fn derive_thumb(
    pdata: &Value,
    url: Option<&str>,
    original: Option<&str>,
    source: &str,
) -> Option<String> {
    // Direct JSON keys first
    if let Some(obj) = pdata.as_object() {
        for k in [
            "thumbnail_url",
            "thumb",
            "thumbnail",
            "small_url",
            "image_thumb",
        ] {
            if let Some(v) = obj.get(k).and_then(|x| x.as_str()) {
                if looks_like_url(v) {
                    return Some(v.to_string());
                }
            }
        }
    }
    // Heuristic per source (e.g., giant_bomb might have pattern modifications)
    if let Some(orig) = original.or(url) {
        if source == "giant_bomb" {
            // Example: replace /original/ with /thumb/ if present
            if orig.contains("/original/") {
                return Some(orig.replace("/original/", "/thumb/"));
            }
        }
        if source == "igdb" && orig.contains("_image") {
            // Derive small variant
            return Some(orig.replace("_image", "_thumb"));
        }
    }
    None
}

fn looks_like_url(s: &str) -> bool {
    let ls = s.to_ascii_lowercase();
    (ls.starts_with("http://") || ls.starts_with("https://")) && ls.len() > 8 && ls.contains('.')
}

fn init_tracing() {
    let _ = fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .try_init();
}
