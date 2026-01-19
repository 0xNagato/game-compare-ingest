use anyhow::Result;
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use tracing::{info, warn};

use crate::database_ops::db::Db;

async fn table_exists(db: &Db, name: &str) -> bool {
    sqlx::query_scalar::<_, bool>(
        "SELECT TRUE FROM information_schema.tables WHERE table_schema = ANY (current_schemas(true)) AND table_name = $1 LIMIT 1",
    )
    .persistent(false)
    .bind(name)
    .fetch_optional(&db.pool)
    .await
    .ok()
    .flatten()
    .unwrap_or(false)
}

fn slugify(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('-');
        }
    }
    out.trim_matches('-').to_string()
}
fn parse_year(date: &str) -> Option<i32> {
    date.get(0..4)?.parse().ok()
}

fn classify_tgdb_role(t: &str) -> &'static str {
    match t.to_ascii_lowercase().as_str() {
        "boxart" => "cover",
        "fanart" => "banner",
        "screenshot" => "screenshot",
        _ => "image",
    }
}

// ---------- TGDB API Shapes (minimal) ----------
// Only the fields actually inspected by the ingest logic are modeled; all others are ignored.
// Optional wrappers reflect sporadic omissions in upstream responses.

#[derive(Debug, Deserialize)]
struct TgdbPlatforms {
    data: Option<TgdbPlatformsData>,
}

#[derive(Debug, Deserialize)]
struct TgdbPlatformsData {
    platforms: Option<HashMap<String, TgdbPlatform>>, // keyed by platform id string
}

#[derive(Debug, Deserialize, Clone)]
struct TgdbPlatform {
    id: Option<u32>,
    name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct VideoGame {
    data: Option<VideoGameData>,
    pages: Option<VideoGamePages>,
}

#[derive(Debug, Deserialize)]
struct VideoGameData {
    games: Option<HashMap<String, VideoGameEntry>>, // keyed by game id string
}

#[derive(Debug, Deserialize)]
struct VideoGamePages {
    next: Option<u32>,
}

#[derive(Debug, Deserialize, Clone)]
struct VideoGameEntry {
    id: Option<u64>,
    #[serde(rename = "game_title")]
    game_title: Option<String>,
    release_date: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GameImages {
    base_url: Option<GameImagesBaseUrl>,
    data: Option<GameImagesData>,
}

#[derive(Debug, Deserialize)]
struct GameImagesBaseUrl {
    original: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GameImagesData {
    images: Option<HashMap<String, Vec<GameImageEntry>>>, // keyed by game id
}

#[derive(Debug, Deserialize)]
struct GameImageEntry {
    filename: Option<String>,
    #[serde(rename = "type")]
    r#type: Option<String>, // mapped via classify_tgdb_role
}

/// Ingest TGDB by platform, filtering to [year_min, year_max] descending; mirrors game entries and media.
pub async fn ingest_range(
    db: &Db,
    api_key: Option<String>,
    year_min: i32,
    year_max: i32,
) -> Result<()> {
    use crate::database_ops::ingest_providers::{
        ensure_platform, ensure_product_named, ensure_provider, ensure_provider_item,
        ensure_software_row, ensure_vg_source_media_links_with_meta, ensure_video_game,
        ensure_video_game_title, upsert_game_media_batch,
    };
    let provider_id: Option<i64> = if table_exists(db, "provider_items").await
        && table_exists(db, "video_game_sources").await
    {
        Some(ensure_provider(db, "tgdb", "catalog", Some("tgdb")).await?)
    } else {
        warn!(
            "TGDB: provider_items/video_game_sources missing; skipping provider_items + media linking (core catalogue ingest will continue)"
        );
        None
    };
    let can_write_provider_media_links =
        provider_id.is_some() && table_exists(db, "vg_source_media_links").await;
    let client = reqwest::Client::new();
    let key = api_key
        .or_else(|| std::env::var("TGDB_API_KEY").ok())
        .unwrap_or_default();
    let rpm: u64 = std::env::var("TGDB_REQS_PER_MIN")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(60);
    let sleep_ms = if rpm > 0 { (60_000u64 / rpm).max(1) } else { 0 };
    let page_size: u32 = std::env::var("TGDB_PAGE_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(50);

    // fetch platforms
    let mut purl = "https://api.thegamesdb.net/Platforms".to_string();
    if !key.is_empty() {
        purl.push_str(&format!("?apikey={}", key));
    }
    if sleep_ms > 0 {
        tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;
    }
    let presp = client.get(&purl).send().await?;
    if !presp.status().is_success() {
        warn!(status=?presp.status(), "TGDB platforms failed");
        return Ok(());
    }
    let plats: TgdbPlatforms = presp.json().await?;
    let Some(map) = plats.data.and_then(|d| d.platforms) else {
        return Ok(());
    };
    let mut platforms: Vec<TgdbPlatform> = map.into_values().collect();
    platforms.sort_by_key(|p| p.id.unwrap_or(0));

    for plat in platforms {
        let pid = match plat.id {
            Some(v) => v,
            None => {
                continue;
            }
        };
        let pname = plat.name.unwrap_or_else(|| format!("Platform {}", pid));
        let pslug = slugify(&pname);
        let platform_id = ensure_platform(db, &pname, Some(&pslug)).await?;

        // page through games filtered by platform; then filter by year and process
        let mut page: u64 = 1;
        loop {
            let mut gurl = format!(
                "https://api.thegamesdb.net/Games?filter[platform]={}&page={}&limit={}",
                pid, page, page_size
            );
            if !key.is_empty() {
                gurl = format!(
                    "{}{}apikey={}",
                    gurl,
                    if gurl.contains('?') { "&" } else { "?" },
                    key
                );
            }
            if sleep_ms > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;
            }
            let gresp = client.get(&gurl).send().await?;
            if !gresp.status().is_success() {
                warn!(status=?gresp.status(), pid, page, "TGDB games page failed");
                break;
            }
            let games: VideoGame = gresp.json().await?;
            let mut list: Vec<_> = games
                .data
                .as_ref()
                .and_then(|d| d.games.as_ref())
                .map(|m| m.values().cloned().collect())
                .unwrap_or_default();
            if list.is_empty() {
                break;
            }
            // filter by release year if present
            list.retain(|g| {
                g.release_date
                    .as_ref()
                    .and_then(|s| parse_year(s))
                    .map(|y| y >= year_min && y <= year_max)
                    .unwrap_or(true)
            });
            // sort descending by release_date
            list.sort_by(|a, b| {
                let ya = a
                    .release_date
                    .as_ref()
                    .and_then(|s| parse_year(s))
                    .unwrap_or(0);
                let yb = b
                    .release_date
                    .as_ref()
                    .and_then(|s| parse_year(s))
                    .unwrap_or(0);
                yb.cmp(&ya)
            });
            for g in list {
                let gid = match g.id {
                    Some(v) => v,
                    None => {
                        continue;
                    }
                };
                let title = g
                    .game_title
                    .clone()
                    .unwrap_or_else(|| format!("Game {}", gid));
                let slug = slugify(&title);
                let product_id = ensure_product_named(db, "software", &slug, &title).await?;
                ensure_software_row(db, product_id).await?;
                let title_id = ensure_video_game_title(db, product_id, &title, Some(&slug)).await?;
                let vg_id = ensure_video_game(db, title_id, platform_id, None).await?;
                let pi: Option<i64> = if let Some(pid) = provider_id {
                    match ensure_provider_item(db, pid, &format!("tgdb:{}", gid), None).await {
                        Ok(id) => Some(id),
                        Err(err) => {
                            warn!(game_id = gid, error = %err, "TGDB: ensure_provider_item failed; skipping provider linkage for this game");
                            None
                        }
                    }
                } else {
                    None
                };

                // images
                let mut iurl = format!("https://api.thegamesdb.net/Games/Images?games_id={}", gid);
                if !key.is_empty() {
                    iurl.push_str(&format!("&apikey={}", key));
                }
                if sleep_ms > 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;
                }
                let iresp = client.get(&iurl).send().await?;
                if iresp.status().is_success() {
                    let imgs: GameImages = iresp.json().await?;
                    if let (Some(base), Some(data)) =
                        (imgs.base_url.and_then(|b| b.original), imgs.data)
                    {
                        if let Some(map) = data.images {
                            if let Some(entries) = map.get(&gid.to_string()) {
                                let mut media_links: Vec<(
                                    String,
                                    Option<String>,
                                    Option<String>,
                                    Option<String>,
                                )> = Vec::new();
                                let mut canonical_media_rows: Vec<(
                                    i64,
                                    String,
                                    String,
                                    String,
                                    String,
                                    serde_json::Value,
                                )> = Vec::new();
                                for (idx, im) in entries.iter().enumerate() {
                                    if let Some(fname) = &im.filename {
                                        let url = format!("{}{}", base, fname);
                                        let role = classify_tgdb_role(
                                            im.r#type.as_deref().unwrap_or("image"),
                                        )
                                        .to_string();
                                        media_links.push((
                                            url.clone(),
                                            Some("image".into()),
                                            Some(role.clone()),
                                            Some(title.clone()),
                                        ));
                                        let provider_data = json!({
                                            "source": "tgdb",
                                            "role": role,
                                            "media_class": "image",
                                            "tgdb_game_id": gid,
                                            "filename": fname,
                                            "platform_id": platform_id,
                                            "release_date": g.release_date,
                                            "type": im.r#type,
                                            "position": idx,
                                        });
                                        canonical_media_rows.push((
                                            vg_id,
                                            "tgdb".to_string(),
                                            format!("tgdb:{}:{}", gid, fname),
                                            role.clone(),
                                            url,
                                            provider_data,
                                        ));
                                    }
                                }

                                if !canonical_media_rows.is_empty() {
                                    let borrowed: Vec<_> = canonical_media_rows
                                        .iter()
                                        .map(|(vg, source, external_id, media_type, url, pdata)| {
                                            (
                                                *vg,
                                                source.as_str(),
                                                external_id.as_str(),
                                                media_type.as_str(),
                                                url.as_str(),
                                                pdata,
                                            )
                                        })
                                        .collect();
                                    if let Err(err) = upsert_game_media_batch(db, &borrowed).await {
                                        warn!(
                                            game_id = gid,
                                            error = %err,
                                            "TGDB: failed to upsert canonical media"
                                        );
                                    }
                                }

                                if can_write_provider_media_links {
                                    if let Some(video_game_source_id) = pi {
                                        if !media_links.is_empty() {
                                            let meta = json!({
                                                "platform_id": platform_id,
                                                "year": g.release_date,
                                                "source": "tgdb",
                                            });
                                            let _ = ensure_vg_source_media_links_with_meta(
                                                db,
                                                video_game_source_id,
                                                None,
                                                &media_links,
                                                "tgdb",
                                                Some(meta),
                                            )
                                            .await;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            // continue if next page exists; otherwise break
            match games.pages.and_then(|p| p.next) {
                Some(_) => {
                    page += 1;
                }
                None => {
                    break;
                }
            }
        }
    }
    info!("tgdb ingest_range complete");
    Ok(())
}

/// Minimal sync shim
pub async fn sync(db: &Db, api_key: Option<String>) -> Result<()> {
    let y_min: i32 = std::env::var("TGDB_YEAR_MIN")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2015);
    let y_max: i32 = std::env::var("TGDB_YEAR_MAX")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2025);
    ingest_range(db, api_key, y_min, y_max).await
}
