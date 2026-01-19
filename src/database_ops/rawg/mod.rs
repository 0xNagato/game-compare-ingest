use anyhow::Result;
use chrono::{Datelike, NaiveDate};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tracing::{debug, info, warn};

use crate::database_ops::db::Db;
use crate::database_ops::ingest_providers::{
    ensure_vg_source_media_links_with_meta, extract_normalized_rating_from_payload,
    upsert_game_media_batch,
};

async fn table_exists(db: &Db, table: &str) -> Result<bool> {
    let exists: Option<bool> = sqlx::query_scalar(
        "SELECT TRUE FROM information_schema.tables WHERE table_schema = ANY (current_schemas(true)) AND table_name = $1 LIMIT 1",
    )
    .persistent(false)
    .bind(table)
    .fetch_optional(&db.pool)
    .await?;
    Ok(exists.unwrap_or(false))
}

async fn column_exists(db: &Db, table: &str, column: &str) -> Result<bool> {
    let exists: Option<bool> = sqlx::query_scalar(
        "SELECT TRUE FROM information_schema.columns WHERE table_schema = ANY (current_schemas(true)) AND table_name = $1 AND column_name = $2 LIMIT 1",
    )
    .persistent(false)
    .bind(table)
    .bind(column)
    .fetch_optional(&db.pool)
    .await?;
    Ok(exists.unwrap_or(false))
}

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

#[derive(Clone)]
struct RawgMediaRecord {
    url: String,
    media_class: String,
    role: String,
    title: Option<String>,
    provider_data: Value,
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
        // RAWG calls this "additional" but it behaves like another background/keyart asset.
        // Using "background" improves downstream prioritization vs lumping into a generic banner.
        "background_image_additional" => "background",
        _ => "screenshot",
    }
}

fn max_screenshots_per_game() -> usize {
    std::env::var("RAWG_MAX_SCREENSHOTS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(8)
}

fn collect_rawg_media_records(
    row: &RawgGameRow,
    detail: Option<&RawgGameDetail>,
    display_name: &str,
    max_screenshots: usize,
) -> Vec<RawgMediaRecord> {
    let mut records: Vec<RawgMediaRecord> = Vec::new();
    let clip_preview = detail
        .and_then(|d| d.clip.as_ref())
        .and_then(|c| c.preview.clone());
    let metacritic = detail.and_then(|d| d.metacritic);

    if let Some(url) = &row.background_image {
        let role = role_from_rawg_key("background_image").to_string();
        records.push(RawgMediaRecord {
            url: url.clone(),
            media_class: "image".into(),
            role: role.clone(),
            title: Some(display_name.to_string()),
            provider_data: json!({
                "source": "rawg",
                "asset_kind": "background_image",
                "media_class": "image",
                "role": role,
                "rawg_id": row.id,
                "released": row.released,
                "rawg_slug": row.slug,
                "metacritic": metacritic,
                "clip_preview": clip_preview,
            }),
        });
    }
    if let Some(url) = &row.background_image_additional {
        let role = role_from_rawg_key("background_image_additional").to_string();
        records.push(RawgMediaRecord {
            url: url.clone(),
            media_class: "image".into(),
            role: role.clone(),
            title: Some(display_name.to_string()),
            provider_data: json!({
                "source": "rawg",
                "asset_kind": "background_image_additional",
                "media_class": "image",
                "role": role,
                "rawg_id": row.id,
                "released": row.released,
                "rawg_slug": row.slug,
                "metacritic": metacritic,
                "clip_preview": clip_preview,
            }),
        });
    }
    if let Some(ss) = &row.short_screenshots {
        for (idx, shot) in ss.iter().enumerate().take(max_screenshots) {
            if let Some(url) = &shot.image {
                let title = Some(format!("{} - Screenshot {}", display_name, idx + 1));
                records.push(RawgMediaRecord {
                    url: url.clone(),
                    media_class: "image".into(),
                    role: "screenshot".into(),
                    title,
                    provider_data: json!({
                        "source": "rawg",
                        "asset_kind": "screenshot",
                        "media_class": "image",
                        "role": "screenshot",
                        "rawg_id": row.id,
                        "released": row.released,
                        "rawg_slug": row.slug,
                        "screenshot_index": idx + 1,
                        "metacritic": metacritic,
                        "clip_preview": clip_preview,
                    }),
                });
            }
        }
    }
    if let Some(detail) = detail {
        if let Some(clip) = &detail.clip {
            if let Some(url) = &clip.clip {
                records.push(RawgMediaRecord {
                    url: url.clone(),
                    media_class: "video".into(),
                    role: "trailer".into(),
                    title: Some(display_name.to_string()),
                    provider_data: json!({
                        "source": "rawg",
                        "asset_kind": "clip",
                        "media_class": "video",
                        "role": "trailer",
                        "rawg_id": row.id,
                        "released": row.released,
                        "rawg_slug": row.slug,
                        "clips": clip.clips,
                        "preview": clip.preview,
                        "metacritic": metacritic,
                        "clip_preview": clip_preview,
                    }),
                });
            }
        }
    }

    records
}

async fn persist_rawg_media(
    db: &Db,
    video_game_ids: &[i64],
    video_game_source_id: Option<i64>,
    primary_vg_id: Option<i64>,
    row: &RawgGameRow,
    detail: Option<&RawgGameDetail>,
    media_records: &[RawgMediaRecord],
) {
    if media_records.is_empty() {
        return;
    }

    if !video_game_ids.is_empty() {
        let mut batch_rows: Vec<(i64, &str, &str, &str, &str, &Value)> =
            Vec::with_capacity(video_game_ids.len() * media_records.len());
        for vg_id in video_game_ids {
            for record in media_records {
                batch_rows.push((
                    *vg_id,
                    "rawg",
                    record.url.as_str(),
                    record.role.as_str(),
                    record.url.as_str(),
                    &record.provider_data,
                ));
            }
        }
        if let Err(err) = upsert_game_media_batch(db, &batch_rows).await {
            warn!(error = %err, rawg_id = row.id, "RAWG: failed to upsert canonical media");
        }
    }

    if let Some(video_game_source_id) = video_game_source_id {
        let tuples: Vec<(String, Option<String>, Option<String>, Option<String>)> = media_records
            .iter()
            .map(|record| {
                (
                    record.url.clone(),
                    Some(record.media_class.clone()),
                    Some(record.role.clone()),
                    record.title.clone(),
                )
            })
            .collect();
        let media_meta = json!({
            "year": row.released,
            "source": "rawg",
            "metacritic": detail.and_then(|d| d.metacritic),
            "clip_preview": detail
                .and_then(|d| d.clip.as_ref())
                .and_then(|c| c.preview.clone()),
        });
        let _ = ensure_vg_source_media_links_with_meta(
            db,
            video_game_source_id,
            primary_vg_id,
            &tuples,
            "rawg",
            Some(media_meta),
        )
        .await;
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
        ensure_retailer, ensure_software_row, ensure_video_game, ensure_video_game_for_product,
        ensure_video_game_source, ensure_video_game_title, ensure_video_game_title_for_source_item,
        merge_video_game_metadata, update_video_game_developer_if_empty,
        update_video_game_global_rating_if_null, update_video_game_release_date_if_null,
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
    let max_pages: u32 = std::env::var("RAWG_MAX_PAGES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let max_items: u64 = std::env::var("RAWG_MAX_ITEMS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let fetch_details: bool = std::env::var("RAWG_FETCH_DETAILS")
        .ok()
        .map(|s| (s == "1" || s.eq_ignore_ascii_case("true")))
        .unwrap_or(false);
    let max_screenshots = max_screenshots_per_game();
    let sleep_ms = if rpm > 0 { (60_000u64 / rpm).max(1) } else { 0 };

    let provider_items_exist = table_exists(db, "provider_items").await.unwrap_or(false);
    let video_games_catalog_schema = column_exists(db, "video_games", "title_id")
        .await
        .unwrap_or(false)
        && column_exists(db, "video_games", "platform_id")
            .await
            .unwrap_or(false);

    // Titles schema detection: prefer the canonical source-registry linkage when available.
    let titles_keyed_by_source_item =
        column_exists(db, "video_game_titles", "video_game_source_id")
            .await
            .unwrap_or(false)
            && column_exists(db, "video_game_titles", "video_game_source_id")
                .await
                .unwrap_or(false);
    let titles_has_video_game_id = if titles_keyed_by_source_item {
        column_exists(db, "video_game_titles", "video_game_id")
            .await
            .unwrap_or(false)
    } else {
        false
    };
    let titles_has_product_id = if titles_keyed_by_source_item {
        column_exists(db, "video_game_titles", "product_id")
            .await
            .unwrap_or(false)
    } else {
        false
    };

    // Only resolve the RAWG video_game_source once, and only when the schema supports it.
    let rawg_source_id = if titles_keyed_by_source_item {
        Some(ensure_video_game_source(db, "rawg", "RAWG").await?)
    } else {
        None
    };

    info!(
        provider_id,
        year_min,
        year_max,
        rpm,
        page_size,
        max_pages,
        max_items,
        fetch_details,
        provider_items_exist,
        video_games_catalog_schema,
        titles_keyed_by_source_item,
        titles_has_video_game_id,
        titles_has_product_id,
        "rawg ingest_range starting"
    );

    let mut processed_total: u64 = 0;

    for year in (year_min..=year_max).rev() {
        // descending
        let mut page: u32 = 1;
        loop {
            if max_pages > 0 && page > max_pages {
                debug!(year, max_pages, "RAWG max_pages reached");
                break;
            }
            if max_items > 0 && processed_total >= max_items {
                debug!(year, processed_total, max_items, "RAWG max_items reached");
                break;
            }
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
                if max_items > 0 && processed_total >= max_items {
                    break;
                }
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
                // Support both legacy title linkage (by product_id/video_game_id) and the newer
                // source+provider_item linkage.
                let mut canonical_vg_for_title: Option<i64> = None;
                let title_id = if titles_keyed_by_source_item {
                    let sid = rawg_source_id
                        .expect("rawg_source_id must exist when titles are keyed by source item");
                    let provider_item_key = row.id.to_string();

                    let (product_link_id, vg_link_id) = if titles_has_video_game_id {
                        // Some deployments require video_game_id for titles keyed by source item.
                        let canonical_vg_id = ensure_video_game_for_product(
                            db,
                            product_id,
                            &name,
                            Some(&base_slug),
                            None,
                        )
                        .await?;
                        canonical_vg_for_title = Some(canonical_vg_id);
                        (
                            if titles_has_product_id {
                                Some(product_id)
                            } else {
                                None
                            },
                            Some(canonical_vg_id),
                        )
                    } else {
                        (
                            if titles_has_product_id {
                                Some(product_id)
                            } else {
                                None
                            },
                            None,
                        )
                    };

                    ensure_video_game_title_for_source_item(
                        db,
                        sid,
                        &provider_item_key,
                        product_link_id,
                        vg_link_id,
                        &name,
                        Some(&base_slug),
                        None,
                        None,
                    )
                    .await?
                } else {
                    ensure_video_game_title(db, product_id, &name, Some(&base_slug)).await?
                };

                let mut detail_data = None;
                if fetch_details {
                    detail_data = fetch_game_detail(&client, row.id, &key, sleep_ms).await?;
                }

                let platform_entries = row
                    .platforms
                    .clone()
                    .or_else(|| detail_data.as_ref().and_then(|d| d.platforms.clone()));

                let mut video_game_ids: Vec<i64> = Vec::new();
                if video_games_catalog_schema {
                    if let Some(plats) = platform_entries {
                        for ent in plats {
                            let plat_name = ent.platform.name.trim();
                            if plat_name.is_empty() {
                                continue;
                            }
                            let plat_slug = slugify(plat_name);
                            let platform_id =
                                ensure_platform(db, plat_name, Some(&plat_slug)).await?;
                            match ensure_video_game(db, title_id, platform_id, None).await {
                                Ok(vg_id) => video_game_ids.push(vg_id),
                                Err(err) => {
                                    warn!(
                                        error = %err,
                                        "RAWG: ensure_video_game failed; will fall back to canonical video game"
                                    );
                                    break;
                                }
                            }
                        }
                    }

                    if video_game_ids.is_empty() {
                        if let Some(vg_id) = canonical_vg_for_title {
                            video_game_ids.push(vg_id);
                        } else {
                            let platform_id = ensure_platform(db, "RAWG", Some("rawg")).await?;
                            let vg_id = ensure_video_game(db, title_id, platform_id, None).await?;
                            video_game_ids.push(vg_id);
                        }
                    }
                } else {
                    let vg_id = if let Some(vg_id) = canonical_vg_for_title {
                        vg_id
                    } else {
                        ensure_video_game_for_product(db, product_id, &name, Some(&base_slug), None)
                            .await?
                    };
                    video_game_ids.push(vg_id);
                }
                let primary_vg_id = video_game_ids.first().copied();

                // Best-effort core fields (no hard dependency on optional columns).
                let release_date_str = row
                    .released
                    .as_deref()
                    .or_else(|| detail_data.as_ref().and_then(|d| d.released.as_deref()));
                if let Some(date_str) = release_date_str {
                    if let Ok(date) = NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
                        for vg in &video_game_ids {
                            let _ = update_video_game_release_date_if_null(db, *vg, date).await;
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
                        let detail_value = serde_json::to_value(detail).ok();
                        if let Some(dev_name) = detail
                            .developers
                            .as_ref()
                            .and_then(|d| d.first())
                            .map(|d| d.name.clone())
                        {
                            for id in &video_game_ids {
                                let _ =
                                    update_video_game_developer_if_empty(db, *id, &dev_name).await;
                            }
                        }
                        if let Some(avg_rating) = detail_value.as_ref().and_then(|payload| {
                            extract_normalized_rating_from_payload(Some("rawg"), payload)
                        }) {
                            for id in &video_game_ids {
                                let _ = update_video_game_global_rating_if_null(
                                    db,
                                    *id,
                                    Some(avg_rating),
                                    None,
                                )
                                .await;
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

                if let Some(vg_id) = primary_vg_id {
                    // Record RAWG facts in video_games.metadata (best-effort; no-op on legacy schemas).
                    let _ = merge_video_game_metadata(
                        db,
                        vg_id,
                        json!({
                            "sources": { "rawg": provider_item_meta.clone() },
                            "rawg_numeric": row.id,
                        }),
                    )
                    .await;
                }
                let video_game_source_id = if provider_items_exist {
                    match ensure_provider_item(
                        db,
                        provider_id,
                        &format!("rawg:{}", row.id),
                        Some(provider_item_meta.clone()),
                    )
                    .await
                    {
                        Ok(id) => Some(id),
                        Err(err) => {
                            warn!(error = %err, "RAWG: ensure_provider_item failed; continuing without provider_items");
                            None
                        }
                    }
                } else {
                    None
                };

                let detail_ref = detail_data.as_ref();
                let media_records =
                    collect_rawg_media_records(&row, detail_ref, &name, max_screenshots);
                persist_rawg_media(
                    db,
                    &video_game_ids,
                    video_game_source_id,
                    primary_vg_id,
                    &row,
                    detail_ref,
                    &media_records,
                )
                .await;

                processed_total += 1;
            }

            page += 1;
        }

        if max_items > 0 && processed_total >= max_items {
            break;
        }
    }
    info!(processed_total, "rawg ingest_range complete");
    Ok(())
}

/// Fetch and ingest top N games from a specific date range with ordering
async fn ingest_top_games(
    db: &Db,
    api_key: &str,
    date_start: &str,
    date_end: &str,
    genre: Option<&str>,
    ordering: &str,
    limit: u32,
) -> Result<u64> {
    use crate::database_ops::ingest_providers::{
        ensure_platform, ensure_product_named, ensure_provider, ensure_provider_item,
        ensure_retailer, ensure_software_row, ensure_video_game, ensure_video_game_for_product,
        ensure_video_game_source, ensure_video_game_title_for_source_item,
        merge_video_game_metadata, replace_provider_toplist_items,
        update_video_game_developer_if_empty, update_video_game_global_rating_if_null,
        update_video_game_release_date_if_null, upsert_provider_toplist,
    };

    let provider_id = ensure_provider(db, "rawg", "catalog", Some("rawg")).await?;
    let client = reqwest::Client::new();
    let rpm: u64 = std::env::var("RAWG_REQS_PER_MIN")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30);
    let sleep_ms = if rpm > 0 { (60_000u64 / rpm).max(1) } else { 0 };
    let max_screenshots = max_screenshots_per_game();

    let provider_items_exist = table_exists(db, "provider_items").await.unwrap_or(false);
    let video_games_catalog_schema = column_exists(db, "video_games", "title_id")
        .await
        .unwrap_or(false)
        && column_exists(db, "video_games", "platform_id")
            .await
            .unwrap_or(false);

    let titles_keyed_by_source_item =
        column_exists(db, "video_game_titles", "video_game_source_id")
            .await
            .unwrap_or(false)
            && column_exists(db, "video_game_titles", "video_game_source_id")
                .await
                .unwrap_or(false);
    let titles_has_video_game_id = if titles_keyed_by_source_item {
        column_exists(db, "video_game_titles", "video_game_id")
            .await
            .unwrap_or(false)
    } else {
        false
    };
    let titles_has_product_id = if titles_keyed_by_source_item {
        column_exists(db, "video_game_titles", "product_id")
            .await
            .unwrap_or(false)
    } else {
        false
    };

    let rawg_source_id = if titles_keyed_by_source_item {
        Some(ensure_video_game_source(db, "rawg", "RAWG").await?)
    } else {
        None
    };

    let mut url = format!(
        "https://api.rawg.io/api/games?dates={},{}&ordering={}&page_size={}&key={}",
        date_start, date_end, ordering, limit, api_key
    );
    if let Some(g) = genre {
        url.push_str(&format!("&genres={}", g));
    }

    if sleep_ms > 0 {
        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
    }

    let resp = client.get(&url).send().await?;
    if !resp.status().is_success() {
        anyhow::bail!("RAWG top games request failed: {}", resp.status());
    }

    let list: RawgListResponse<RawgGameRow> = resp.json().await?;
    let count = list.results.len() as u64;
    info!(
        date_start,
        date_end, genre, ordering, count, "RAWG top games fetched"
    );

    let list_type = if genre.is_some() {
        "top_genre"
    } else {
        "top_monthly"
    };
    let toplist_slug = if let Some(g) = genre {
        format!("rawg:top_genre:{g}:{date_start}:{date_end}")
    } else {
        format!("rawg:top_monthly:{date_start}:{date_end}")
    };
    let mut ranked_products: Vec<(u32, i64)> = Vec::new();
    let mut seen_products: std::collections::HashSet<i64> = std::collections::HashSet::new();

    for (idx, row) in list.results.into_iter().enumerate() {
        let name = row.name.clone();
        let base_slug = row.slug.clone().unwrap_or_else(|| slugify(&name));
        let product_id = ensure_product_named(db, "software", &base_slug, &name).await?;
        ensure_software_row(db, product_id).await?;

        // Persist the ordered toplist membership (best-effort; duplicates are ignored).
        if seen_products.insert(product_id) {
            ranked_products.push(((idx as u32) + 1, product_id));
        }

        let mut canonical_vg_for_title: Option<i64> = None;
        let title_id = if titles_keyed_by_source_item {
            let sid = rawg_source_id.expect("rawg_source_id must exist");
            let provider_item_key = row.id.to_string();

            let (product_link_id, vg_link_id) = if titles_has_video_game_id {
                let canonical_vg_id =
                    ensure_video_game_for_product(db, product_id, &name, Some(&base_slug), None)
                        .await?;
                canonical_vg_for_title = Some(canonical_vg_id);
                (
                    if titles_has_product_id {
                        Some(product_id)
                    } else {
                        None
                    },
                    Some(canonical_vg_id),
                )
            } else {
                (
                    if titles_has_product_id {
                        Some(product_id)
                    } else {
                        None
                    },
                    None,
                )
            };

            ensure_video_game_title_for_source_item(
                db,
                sid,
                &provider_item_key,
                product_link_id,
                vg_link_id,
                &name,
                Some(&base_slug),
                None,
                None,
            )
            .await?
        } else {
            crate::database_ops::ingest_providers::ensure_video_game_title(
                db,
                product_id,
                &name,
                Some(&base_slug),
            )
            .await?
        };

        // Always fetch details for top games to get retailer info
        let detail_data = fetch_game_detail(&client, row.id, api_key, sleep_ms).await?;

        let platform_entries = row
            .platforms
            .clone()
            .or_else(|| detail_data.as_ref().and_then(|d| d.platforms.clone()));

        let mut video_game_ids: Vec<i64> = Vec::new();
        if video_games_catalog_schema {
            if let Some(plats) = platform_entries {
                for ent in plats {
                    let plat_name = ent.platform.name.trim();
                    if plat_name.is_empty() {
                        continue;
                    }
                    let plat_slug = slugify(plat_name);
                    let platform_id = ensure_platform(db, plat_name, Some(&plat_slug)).await?;
                    match ensure_video_game(db, title_id, platform_id, None).await {
                        Ok(vg_id) => video_game_ids.push(vg_id),
                        Err(err) => {
                            warn!(error = %err, "ensure_video_game failed");
                            break;
                        }
                    }
                }
            }

            if video_game_ids.is_empty() {
                if let Some(vg_id) = canonical_vg_for_title {
                    video_game_ids.push(vg_id);
                } else {
                    let platform_id = ensure_platform(db, "RAWG", Some("rawg")).await?;
                    let vg_id = ensure_video_game(db, title_id, platform_id, None).await?;
                    video_game_ids.push(vg_id);
                }
            }
        } else {
            let vg_id = if let Some(vg_id) = canonical_vg_for_title {
                vg_id
            } else {
                ensure_video_game_for_product(db, product_id, &name, Some(&base_slug), None).await?
            };
            video_game_ids.push(vg_id);
        }
        let primary_vg_id = video_game_ids.first().copied();

        // Release date
        let release_date_str = row
            .released
            .as_deref()
            .or_else(|| detail_data.as_ref().and_then(|d| d.released.as_deref()));
        if let Some(date_str) = release_date_str {
            if let Ok(date) = NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
                for vg in &video_game_ids {
                    let _ = update_video_game_release_date_if_null(db, *vg, date).await;
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

        // Store retailer information
        let mut has_retailers = false;
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

            // Handle stores/retailers
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
                        has_retailers = true;
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

            // Mark if we don't have retailer info
            if !has_retailers {
                provider_meta.insert("no_retailers".into(), json!(true));
            }

            if let Some(clip) = &detail.clip {
                provider_meta.insert("clip".into(), json!(clip));
            }

            // Update video game metadata
            if !video_game_ids.is_empty() {
                let detail_value = serde_json::to_value(detail).ok();
                if let Some(dev_name) = detail
                    .developers
                    .as_ref()
                    .and_then(|d| d.first())
                    .map(|d| d.name.clone())
                {
                    for id in &video_game_ids {
                        let _ = update_video_game_developer_if_empty(db, *id, &dev_name).await;
                    }
                }
                if let Some(avg_rating) = detail_value.as_ref().and_then(|payload| {
                    extract_normalized_rating_from_payload(Some("rawg"), payload)
                }) {
                    for id in &video_game_ids {
                        let _ = update_video_game_global_rating_if_null(
                            db,
                            *id,
                            Some(avg_rating),
                            None,
                        )
                        .await;
                    }
                }
            }
        } else {
            // No detail data fetched means no retailers
            provider_meta.insert("no_retailers".into(), json!(true));
        }

        if let Some(platforms) = row.platforms.as_ref() {
            let platform_names: Vec<String> =
                platforms.iter().map(|p| p.platform.name.clone()).collect();
            if !platform_names.is_empty() {
                provider_meta.insert("platforms".into(), json!(platform_names));
            }
        }

        let provider_item_meta = Value::Object(provider_meta);

        if let Some(vg_id) = primary_vg_id {
            let _ = merge_video_game_metadata(
                db,
                vg_id,
                json!({
                    "sources": { "rawg": provider_item_meta.clone() },
                    "rawg_numeric": row.id,
                }),
            )
            .await;
        }

        let video_game_source_id = if provider_items_exist {
            match ensure_provider_item(
                db,
                provider_id,
                &format!("rawg:{}", row.id),
                Some(provider_item_meta.clone()),
            )
            .await
            {
                Ok(id) => Some(id),
                Err(err) => {
                    warn!(error = %err, "ensure_provider_item failed");
                    None
                }
            }
        } else {
            None
        };

        let detail_ref = detail_data.as_ref();
        let media_records = collect_rawg_media_records(&row, detail_ref, &name, max_screenshots);
        persist_rawg_media(
            db,
            &video_game_ids,
            video_game_source_id,
            primary_vg_id,
            &row,
            detail_ref,
            &media_records,
        )
        .await;
    }

    // Record the toplist snapshot for Laravel Spotlight consumption.
    // This is schema-optional: if tables do not exist, these become no-ops.
    let meta = json!({
        "source": "rawg",
        "ordering": ordering,
        "limit": limit,
        "count": count,
        "url": url,
    });
    let toplist_id = upsert_provider_toplist(
        db,
        "rawg",
        &toplist_slug,
        list_type,
        Some(date_start),
        Some(date_end),
        genre,
        Some(meta),
    )
    .await?;
    replace_provider_toplist_items(db, toplist_id, &ranked_products).await?;

    Ok(count)
}

/// Fetch top games of the month
pub async fn ingest_top_monthly(db: &Db, api_key: Option<String>) -> Result<u64> {
    let key = api_key
        .or_else(|| std::env::var("RAWG_API_KEY").ok())
        .unwrap_or_default();

    // Get current month date range
    let now = chrono::Utc::now().naive_utc().date();
    let start_of_month = chrono::NaiveDate::from_ymd_opt(now.year(), now.month(), 1)
        .ok_or_else(|| anyhow::anyhow!("invalid date"))?;
    let end_of_month = if now.month() == 12 {
        chrono::NaiveDate::from_ymd_opt(now.year() + 1, 1, 1)
            .and_then(|d| d.pred_opt())
            .ok_or_else(|| anyhow::anyhow!("invalid date"))?
    } else {
        chrono::NaiveDate::from_ymd_opt(now.year(), now.month() + 1, 1)
            .and_then(|d| d.pred_opt())
            .ok_or_else(|| anyhow::anyhow!("invalid date"))?
    };

    let date_start = start_of_month.format("%Y-%m-%d").to_string();
    let date_end = end_of_month.format("%Y-%m-%d").to_string();

    let limit: u32 = std::env::var("RAWG_TOP_MONTHLY_LIMIT")
        .ok()
        .and_then(|s| s.parse().ok())
        .or_else(|| {
            std::env::var("RAWG_TOP_LIMIT")
                .ok()
                .and_then(|s| s.parse().ok())
        })
        .unwrap_or(50);

    info!(
        date_start,
        date_end, limit, "RAWG ingesting top games of the month"
    );
    ingest_top_games(db, &key, &date_start, &date_end, None, "-rating", limit).await
}

/// Fetch top games by genre
pub async fn ingest_top_by_genre(db: &Db, api_key: Option<String>, genre: &str) -> Result<u64> {
    let key = api_key
        .or_else(|| std::env::var("RAWG_API_KEY").ok())
        .unwrap_or_default();

    // Use 2025 as the year range
    let date_start = "2025-01-01";
    let date_end = "2025-12-31";

    let limit: u32 = std::env::var("RAWG_TOP_GENRE_LIMIT")
        .ok()
        .and_then(|s| s.parse().ok())
        .or_else(|| {
            std::env::var("RAWG_TOP_LIMIT")
                .ok()
                .and_then(|s| s.parse().ok())
        })
        .unwrap_or(50);

    info!(genre, limit, "RAWG ingesting top games by genre");
    ingest_top_games(
        db,
        &key,
        date_start,
        date_end,
        Some(genre),
        "-rating",
        limit,
    )
    .await
}

/// Minimal sync shim
pub async fn sync(db: &Db, api_key: Option<String>) -> Result<()> {
    // Check for required schema tables (legacy-safe)
    let required_tables = [
        "platforms",
        "providers",
        "provider_items",
        "video_game_sources",
        "video_game_titles",
        "video_games",
    ];
    let mut missing: Vec<&str> = Vec::new();
    for t in required_tables {
        if !table_exists(db, t).await.unwrap_or(false) {
            missing.push(t);
        }
    }
    if !missing.is_empty() {
        use crate::database_ops::ingest_providers::php_compat_schema;
        let compat = php_compat_schema(db).await.unwrap_or(false);
        warn!(
            missing_tables = ?missing,
            php_compat = compat,
            "rawg sync: required schema missing; skipping RAWG ingestion to preserve backward compatibility"
        );
        return Ok(());
    }

    let mode = std::env::var("RAWG_MODE").unwrap_or_else(|_| "range".to_string());

    match mode.as_str() {
        "top_monthly" => {
            ingest_top_monthly(db, api_key).await?;
            Ok(())
        }
        "top_genres" => {
            // Ingest top games by genre.
            // Default: action(4), sports(15), shooter(2).
            // Override with RAWG_TOP_GENRES (CSV of genre ids).
            let genre_ids: Vec<String> = std::env::var("RAWG_TOP_GENRES")
                .ok()
                .map(|s| {
                    s.split(',')
                        .map(|p| p.trim().to_string())
                        .filter(|p| !p.is_empty())
                        .collect::<Vec<_>>()
                })
                .filter(|v| !v.is_empty())
                .unwrap_or_else(|| vec!["4".into(), "15".into(), "2".into()]);

            for genre_id in genre_ids {
                match ingest_top_by_genre(db, api_key.clone(), &genre_id).await {
                    Ok(count) => {
                        info!(genre = %genre_id, count, "ingested top games");
                    }
                    Err(e) => {
                        warn!(genre = %genre_id, error = %e, "failed to ingest genre");
                    }
                }
            }
            Ok(())
        }
        "all" => {
            // Run all modes
            ingest_top_monthly(db, api_key.clone()).await?;

            let genre_ids: Vec<String> = std::env::var("RAWG_TOP_GENRES")
                .ok()
                .map(|s| {
                    s.split(',')
                        .map(|p| p.trim().to_string())
                        .filter(|p| !p.is_empty())
                        .collect::<Vec<_>>()
                })
                .filter(|v| !v.is_empty())
                .unwrap_or_else(|| vec!["4".into(), "15".into(), "2".into()]);

            for genre_id in genre_ids {
                match ingest_top_by_genre(db, api_key.clone(), &genre_id).await {
                    Ok(count) => {
                        info!(genre = %genre_id, count, "ingested top games");
                    }
                    Err(e) => {
                        warn!(genre = %genre_id, error = %e, "failed to ingest genre");
                    }
                }
            }
            Ok(())
        }
        _ => {
            // Default range mode
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
    }
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
