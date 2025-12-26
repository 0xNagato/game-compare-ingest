use anyhow::Result;
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tracing::{debug, info, warn};

use crate::database_ops::db::Db;

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct RawgListResponse<T> {
    count: Option<u64>,
    next: Option<String>,
    previous: Option<String>,
    results: Vec<T>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct RawgPlatformEntry {
    platform: RawgIdName,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[allow(dead_code)]
struct RawgIdName {
    id: i64,
    name: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct RawgScreenshot {
    image: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct RawgGameRow {
    id: i64,
    name: String,
    slug: Option<String>,
    released: Option<String>,
    background_image: Option<String>,
    background_image_additional: Option<String>,
    platforms: Option<Vec<RawgPlatformEntry>>, // list rows include nested platform ids
    short_screenshots: Option<Vec<RawgScreenshot>>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
struct RawgStore {
    id: Option<i64>,
    name: String,
    slug: Option<String>,
    domain: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct RawgStoreEntry {
    id: Option<i64>,
    url: Option<String>,
    #[serde(default)]
    store: RawgStore,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct RawgClip {
    clip: Option<String>,
    clips: Option<HashMap<String, String>>,
    preview: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct RawgMetacriticPlatform {
    metascore: Option<i32>,
    url: Option<String>,
    platform: Option<RawgIdName>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct RawgGameDetail {
    id: i64,
    name: String,
    slug: Option<String>,
    released: Option<String>,
    metacritic: Option<i32>,
    metacritic_url: Option<String>,
    platforms: Option<Vec<RawgPlatformEntry>>,
    developers: Option<Vec<RawgIdName>>,
    publishers: Option<Vec<RawgIdName>>,
    stores: Option<Vec<RawgStoreEntry>>,
    metacritic_platforms: Option<Vec<RawgMetacriticPlatform>>,
    clip: Option<RawgClip>,
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

fn role_from_rawg_key(k: &str) -> &'static str {
    match k {
        "background_image" => "cover",
        "background_image_additional" => "banner",
        _ => "screenshot",
    }
}

/// Ingest RAWG games within [year_min, year_max] descending, respecting simple RPM pacing.
pub async fn ingest_range(
    db: &Db,
    api_key: Option<String>,
    year_min: i32,
    year_max: i32,
) -> Result<()> {
    use crate::database_ops::ingest_providers::{
        ensure_platform, ensure_product_named, ensure_provider, ensure_provider_item,
        ensure_provider_media_links_with_meta, ensure_retailer, ensure_software_row,
        ensure_video_game, ensure_video_game_title,
    };

    let provider_id = ensure_provider(db, "rawg", "catalog", Some("rawg")).await?;
    let client = reqwest::Client::new();
    let key = api_key
        .or_else(|| std::env::var("RAWG_API_KEY").ok())
        .unwrap_or_default();
    let rpm: u64 = std::env::var("RAWG_REQS_PER_MIN")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(60);
    let page_size: u32 = std::env::var("RAWG_PAGE_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(40);
    let fetch_details: bool = std::env::var("RAWG_FETCH_DETAILS")
        .ok()
        .map(|s| (s == "1" || s.eq_ignore_ascii_case("true")))
        .unwrap_or(false);
    let sleep_ms = if rpm > 0 { (60_000u64 / rpm).max(1) } else { 0 };

    for year in (year_min..=year_max).rev() {
        // descending
        let mut page: u32 = 1;
        loop {
            let mut url = format!(
                "https://api.rawg.io/api/games?dates={}-01-01,{}-12-31&ordering=-released&page={}&page_size={}",
                year, year, page, page_size
            );
            if !key.is_empty() {
                url.push_str(&format!("&key={}", key));
            }
            if sleep_ms > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;
            }
            let resp = client.get(&url).send().await?;
            if !resp.status().is_success() {
                warn!(status=?resp.status(), year, page, "RAWG list request failed");
                break;
            }
            let list: RawgListResponse<RawgGameRow> = resp.json().await?;
            if list.results.is_empty() {
                break;
            }
            debug!(year, page, count = list.results.len(), "RAWG page fetched");

            for row in list.results {
                if let Some(rel) = &row.released {
                    if let Some(y) = parse_year(rel) {
                        if y < year_min || y > year_max {
                            continue;
                        }
                    }
                }

                let name = row.name.clone();
                let base_slug = row.slug.clone().unwrap_or_else(|| slugify(&name));
                let product_id = ensure_product_named(db, "software", &base_slug, &name).await?;
                ensure_software_row(db, product_id).await?;
                let title_id =
                    ensure_video_game_title(db, product_id, &name, Some(&base_slug)).await?;

                let mut detail_data = None;
                if fetch_details {
                    detail_data = fetch_game_detail(&client, row.id, &key, sleep_ms).await?;
                }

                let platform_entries = row
                    .platforms
                    .clone()
                    .or_else(|| detail_data.as_ref().and_then(|d| d.platforms.clone()));

                let mut video_game_ids: Vec<i64> = Vec::new();
                if let Some(plats) = platform_entries {
                    for ent in plats {
                        let plat_name = ent.platform.name.trim();
                        if plat_name.is_empty() {
                            continue;
                        }
                        let plat_slug = slugify(plat_name);
                        let platform_id = ensure_platform(db, plat_name, Some(&plat_slug)).await?;
                        let vg_id = ensure_video_game(db, title_id, platform_id, None).await?;
                        video_game_ids.push(vg_id);
                    }
                }
                if video_game_ids.is_empty() {
                    let platform_id = ensure_platform(db, "RAWG", Some("rawg")).await?;
                    let vg_id = ensure_video_game(db, title_id, platform_id, None).await?;
                    video_game_ids.push(vg_id);
                }
                let primary_vg_id = video_game_ids.first().copied();

                if let Some(date_str) = row.released.as_ref() {
                    if let Ok(date) = NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
                        for vg in &video_game_ids {
                            let _ = sqlx
                                ::query(
                                    "UPDATE public.video_games SET release_date = COALESCE(release_date, $1) WHERE id=$2"
                                )
                                .persistent(false)
                                .bind(date)
                                .bind(vg)
                                .execute(&db.pool).await;
                        }
                    }
                }
                if row.released.is_none() {
                    if let Some(detail_release) = detail_data
                        .as_ref()
                        .and_then(|d| d.released.as_ref())
                        .and_then(|s| NaiveDate::parse_from_str(s, "%Y-%m-%d").ok())
                    {
                        for vg in &video_game_ids {
                            let _ = sqlx
                                ::query(
                                    "UPDATE public.video_games SET release_date = COALESCE(release_date, $1) WHERE id=$2"
                                )
                                .persistent(false)
                                .bind(detail_release)
                                .bind(vg)
                                .execute(&db.pool).await;
                        }
                    }
                }

                let mut provider_meta = Map::new();
                provider_meta.insert("rawg_id".into(), json!(row.id));
                if let Some(slug) = &row.slug {
                    provider_meta.insert("rawg_slug".into(), json!(slug));
                }
                if let Some(released) = &row.released {
                    provider_meta.insert("released".into(), json!(released));
                }
                if let Some(bg) = &row.background_image {
                    provider_meta.insert("background_image".into(), json!(bg));
                }
                if let Some(extra) = &row.background_image_additional {
                    provider_meta.insert("background_image_additional".into(), json!(extra));
                }

                if let Some(detail) = detail_data.as_ref() {
                    if let Some(score) = detail.metacritic {
                        provider_meta.insert("metacritic".into(), json!(score));
                    }
                    if let Some(url) = &detail.metacritic_url {
                        provider_meta.insert("metacritic_url".into(), json!(url));
                    }
                    if let Some(devs) = &detail.developers {
                        let dev_names: Vec<String> = devs.iter().map(|d| d.name.clone()).collect();
                        if !dev_names.is_empty() {
                            provider_meta.insert("developers".into(), json!(dev_names));
                        }
                    }
                    if let Some(pubs) = &detail.publishers {
                        let pub_names: Vec<String> = pubs.iter().map(|d| d.name.clone()).collect();
                        if !pub_names.is_empty() {
                            provider_meta.insert("publishers".into(), json!(pub_names));
                        }
                    }
                    if let Some(mplats) = &detail.metacritic_platforms {
                        provider_meta.insert("metacritic_platforms".into(), json!(mplats));
                    }

                    if let Some(stores) = &detail.stores {
                        let mut store_records = Vec::new();
                        let mut seen_stores: HashSet<String> = HashSet::new();
                        for store in stores {
                            let store_name = store.store.name.trim();
                            if store_name.is_empty() {
                                continue;
                            }
                            let slug = store
                                .store
                                .slug
                                .clone()
                                .unwrap_or_else(|| store_name.to_string());
                            if !seen_stores.insert(slug.clone()) {
                                continue;
                            }
                            if let Ok(retailer_id) =
                                ensure_retailer(db, store_name, store.store.slug.as_deref()).await
                            {
                                store_records.push(json!({
                                    "retailer_id": retailer_id,
                                    "retailer_slug": store.store.slug,
                                    "name": store_name,
                                    "url": store.url,
                                    "domain": store.store.domain.clone(),
                                }));
                            }
                        }
                        if !store_records.is_empty() {
                            provider_meta.insert("stores".into(), Value::Array(store_records));
                        }
                    }
                    if let Some(clip) = &detail.clip {
                        provider_meta.insert("clip".into(), json!(clip));
                    }
                }

                if !video_game_ids.is_empty() {
                    if let Some(detail) = detail_data.as_ref() {
                        if let Some(dev_name) = detail
                            .developers
                            .as_ref()
                            .and_then(|d| d.first())
                            .map(|d| d.name.clone())
                        {
                            for id in &video_game_ids {
                                let _ = sqlx
                                    ::query(
                                        "UPDATE public.video_games SET developer = COALESCE(developer, $1) WHERE id=$2"
                                    )
                                    .persistent(false)
                                    .bind(&dev_name)
                                    .bind(id)
                                    .execute(&db.pool).await;
                            }
                        }
                        if let Some(score) = detail.metacritic {
                            let avg_rating = (score as f32) / 20.0;
                            for id in &video_game_ids {
                                let _ = sqlx
                                    ::query(
                                        "UPDATE public.video_games SET average_rating = COALESCE($1, average_rating), rating_updated_at = CASE WHEN $1 IS NOT NULL THEN now() ELSE rating_updated_at END WHERE id=$2"
                                    )
                                    .persistent(false)
                                    .bind(avg_rating)
                                    .bind(id)
                                    .execute(&db.pool).await;
                            }
                        }
                    }
                    if provider_meta.get("released").is_none() {
                        if let Some(detail_release) =
                            detail_data.as_ref().and_then(|d| d.released.as_ref())
                        {
                            provider_meta.insert("released".into(), json!(detail_release));
                        }
                    }
                }

                if let Some(detail_release) = detail_data.as_ref().and_then(|d| d.released.as_ref())
                {
                    if row.released.is_none() {
                        provider_meta.insert("released".into(), json!(detail_release));
                    }
                }

                if let Some(platforms) = row.platforms.as_ref() {
                    let platform_names: Vec<String> =
                        platforms.iter().map(|p| p.platform.name.clone()).collect();
                    if !platform_names.is_empty() {
                        provider_meta.insert("platforms".into(), json!(platform_names));
                    }
                }

                let provider_item_meta = Value::Object(provider_meta);
                let provider_item_id = ensure_provider_item(
                    db,
                    provider_id,
                    &format!("rawg:{}", row.id),
                    Some(provider_item_meta),
                )
                .await?;

                let mut media: Vec<(String, Option<String>, Option<String>, Option<String>)> =
                    Vec::new();
                if let Some(url) = &row.background_image {
                    media.push((
                        url.clone(),
                        Some("image".into()),
                        Some(role_from_rawg_key("background_image").into()),
                        Some(name.clone()),
                    ));
                }
                if let Some(url) = &row.background_image_additional {
                    media.push((
                        url.clone(),
                        Some("image".into()),
                        Some(role_from_rawg_key("background_image_additional").into()),
                        Some(name.clone()),
                    ));
                }
                if let Some(ss) = &row.short_screenshots {
                    for s in ss {
                        if let Some(u) = &s.image {
                            media.push((
                                u.clone(),
                                Some("image".into()),
                                Some("screenshot".into()),
                                Some(name.clone()),
                            ));
                        }
                    }
                }
                if let Some(detail) = detail_data.as_ref() {
                    if let Some(clip) = &detail.clip {
                        if let Some(url) = &clip.clip {
                            media.push((
                                url.clone(),
                                Some("video".into()),
                                Some("trailer".into()),
                                Some(name.clone()),
                            ));
                        }
                    }
                }
                if !media.is_empty() {
                    let media_meta = json!({
                        "year": row.released,
                        "source": "rawg",
                        "metacritic": detail_data.as_ref().and_then(|d| d.metacritic),
                        "clip_preview": detail_data
                            .as_ref()
                            .and_then(|d| d.clip.as_ref())
                            .and_then(|c| c.preview.clone()),
                    });
                    let _ = ensure_provider_media_links_with_meta(
                        db,
                        provider_item_id,
                        primary_vg_id,
                        &media,
                        "rawg",
                        Some(media_meta),
                    )
                    .await;
                }
            }

            page += 1;
        }
    }
    info!("rawg ingest_range complete");
    Ok(())
}

/// Minimal sync shim
pub async fn sync(db: &Db, api_key: Option<String>) -> Result<()> {
    let y_min: i32 = std::env::var("RAWG_YEAR_MIN")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2015);
    let y_max: i32 = std::env::var("RAWG_YEAR_MAX")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2025);
    ingest_range(db, api_key, y_min, y_max).await
}

async fn fetch_game_detail(
    client: &reqwest::Client,
    game_id: i64,
    key: &str,
    sleep_ms: u64,
) -> Result<Option<RawgGameDetail>> {
    if sleep_ms > 0 {
        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
    }
    let mut durl = format!("https://api.rawg.io/api/games/{}", game_id);
    if !key.is_empty() {
        durl.push_str(&format!("?key={}", key));
    }
    let resp = client.get(&durl).send().await?;
    if resp.status().is_success() {
        let detail: RawgGameDetail = resp.json().await?;
        Ok(Some(detail))
    } else {
        warn!(status = ?resp.status(), game_id, "RAWG detail request failed");
        Ok(None)
    }
}
