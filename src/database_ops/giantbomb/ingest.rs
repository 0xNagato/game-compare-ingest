use anyhow::Result;
use serde_json::Value;
use std::{
    collections::HashSet,
    fs,
    io::{BufRead, BufReader},
    path::Path,
};
use tracing::{info, instrument, warn};

use crate::database_ops::db::Db;
use crate::database_ops::ingest_providers::{
    ensure_platform, ensure_provider, ensure_vg_source_media_links_with_meta,
    ensure_video_game_source, upsert_game_media, ProviderEntityCache,
};
use crate::database_ops::media_map::normalize_title;

const GIANTBOMB_PROVIDER_KEY: &str = "giantbomb";

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

/// Ingest a GiantBomb style JSON dump (top-level object keyed by guid) creating:
/// products/software/video_game_titles/video_games and provider_items + media links.
/// Returns number of titles ingested (created or already existing).
#[instrument(skip(db))]
pub async fn ingest_from_file(db: &Db, path: &str, limit: Option<usize>) -> Result<usize> {
    // Route writes to public.* explicitly
    let _ = sqlx::query("SET search_path TO public")
        .persistent(false)
        .execute(&db.pool)
        .await;
    // Year-range reminder (informational only). Print only when env overrides are provided to reduce log noise.
    let year_min_env = std::env::var("YEAR_MIN").ok();
    let year_max_env = std::env::var("YEAR_MAX").ok();
    if year_min_env.is_some() || year_max_env.is_some() {
        let year_min: i32 = year_min_env.and_then(|s| s.parse().ok()).unwrap_or(2020);
        let year_max: i32 = year_max_env.and_then(|s| s.parse().ok()).unwrap_or(2025);
        println!(
            "remember: restricting to releases between {year_min}-{year_max} inclusive (where applicable)\n"
        );
    }

    // Resume-after control for JSON dumps (best-effort; order may vary)
    let resume_after_guid = std::env::var("GB_RESUME_AFTER_GUID")
        .ok()
        .filter(|s| !s.trim().is_empty());
    let mut resume_passed_obj = resume_after_guid.is_none();
    let mut resume_passed_arr = resume_after_guid.is_none();
    let mut resume_guid_found = resume_after_guid.is_none();
    let p = Path::new(path);
    if !p.exists() {
        return Ok(0);
    }
    let raw = fs::read_to_string(p)?;
    let v: Value = serde_json::from_str(&raw)?;
    let mut count = 0usize;
    let provider_id = ensure_provider(db, "giantbomb", "catalog", Some("giantbomb")).await?;

    // Schema detection: some DBs use a source-registry schema where `video_game_titles` is keyed by
    // (video_game_source_id, vg_source_item_id), and may require video_game_id (NOT NULL) or product_id.
    let titles_has_source_id =
        column_exists(db, "video_game_titles", "video_game_source_id").await?;
    let titles_has_vg_source_item_id =
        column_exists(db, "video_game_titles", "vg_source_item_id").await?;
    let sources_table_exists = table_exists(db, "video_game_sources").await?;
    if titles_has_source_id && titles_has_vg_source_item_id && !sources_table_exists {
        warn!(
            "video_game_titles has (video_game_source_id, vg_source_item_id) but video_game_sources table is missing; falling back to legacy title schema"
        );
    }
    let titles_keyed_by_source_item =
        titles_has_source_id && titles_has_vg_source_item_id && sources_table_exists;
    let titles_has_video_game_id = titles_keyed_by_source_item
        && column_exists(db, "video_game_titles", "video_game_id").await?;
    let titles_has_product_id =
        titles_keyed_by_source_item && column_exists(db, "video_game_titles", "product_id").await?;
    let video_games_is_laravel = if titles_has_video_game_id {
        column_exists(db, "video_games", "product_id").await?
            && column_exists(db, "video_games", "title").await?
    } else {
        false
    };
    let source_id = if titles_keyed_by_source_item {
        Some(ensure_video_game_source(db, "giantbomb", "Giant Bomb").await?)
    } else {
        None
    };

    let provider_items_exists = table_exists(db, "provider_items").await?;
    if !provider_items_exists {
        warn!("provider_items table missing; skipping provider_items/provider_media_links writes for giantbomb ingest");
    }

    // Ensure a generic platform so we can create a video_games row (PC acts as neutral)
    let platform_id = ensure_platform(db, "PC", Some("pc")).await?;
    let mut entity_cache = ProviderEntityCache::new(db.clone());
    match v {
        // Common dump shape: object keyed by guid
        Value::Object(map) => {
            for (k, val) in map.into_iter() {
                if !resume_passed_obj {
                    // Prefer guid field when present, else key
                    let guid_cur = val
                        .get("guid")
                        .and_then(|g| g.as_str())
                        .unwrap_or_else(|| k.as_str());
                    if resume_after_guid.as_deref() == Some(guid_cur) {
                        resume_passed_obj = true;
                        resume_guid_found = true;
                        continue; // skip the resume marker entry itself
                    }
                    continue;
                }
                if let Some(lim) = limit {
                    if count >= lim {
                        break;
                    }
                }
                if let Value::Object(obj) = val {
                    let name = obj
                        .get("name")
                        .and_then(|n| n.as_str())
                        .unwrap_or_else(|| k.as_str());
                    if name.trim().is_empty() {
                        continue;
                    }
                    let slug = normalize_title(name);
                    // Provider item keyed by guid (string id). This must exist before we can
                    // ensure a canonical video_game_titles row in the source-registry schema.
                    let guid = obj
                        .get("guid")
                        .and_then(|g| g.as_str())
                        .unwrap_or_else(|| k.as_str());
                    let product_id = entity_cache
                        .ensure_product_named("software", &slug, name)
                        .await?;
                    entity_cache.ensure_software_row(product_id).await?;
                    let (_title_id, vg_id) = if titles_keyed_by_source_item {
                        let sid = source_id
                            .expect("source_id must exist when titles_keyed_by_source_item");
                        if titles_has_video_game_id {
                            if !video_games_is_laravel {
                                anyhow::bail!(
                                    "video_game_titles.video_game_id is present but video_games is not Laravel-style (missing product_id/title)"
                                );
                            }
                            let canonical_vg_id = entity_cache
                                .ensure_video_game_for_product_laravel(
                                    product_id,
                                    name,
                                    Some(&slug),
                                    None,
                                    GIANTBOMB_PROVIDER_KEY,
                                )
                                .await?;
                            let title_id = entity_cache
                                .ensure_video_game_title_for_source_item(
                                    sid,
                                    guid,
                                    Some(product_id),
                                    Some(canonical_vg_id),
                                    name,
                                    Some(&slug),
                                    None,
                                    None,
                                )
                                .await?;
                            (title_id, canonical_vg_id)
                        } else {
                            let legacy_link_id = if titles_has_product_id {
                                Some(product_id)
                            } else {
                                None
                            };
                            let title_id = entity_cache
                                .ensure_video_game_title_for_source_item(
                                    sid,
                                    guid,
                                    legacy_link_id,
                                    None,
                                    name,
                                    Some(&slug),
                                    None,
                                    None,
                                )
                                .await?;
                            // Create (or ensure) a single platform instance of the game; edition unknown.
                            // Laravel schema: use product_id directly
                            let vg_id = entity_cache
                                .ensure_video_game_for_product_laravel(
                                    product_id,
                                    name,
                                    Some(&slug),
                                    None,
                                    GIANTBOMB_PROVIDER_KEY,
                                )
                                .await?;
                            (title_id, vg_id)
                        }
                    } else {
                        let title_id = entity_cache
                            .ensure_video_game_title(product_id, name, Some(&slug))
                            .await?;
                        // Laravel schema: use product_id directly
                        let vg_id = entity_cache
                            .ensure_video_game_for_product_laravel(
                                product_id,
                                name,
                                Some(&slug),
                                None,
                                GIANTBOMB_PROVIDER_KEY,
                            )
                            .await?;
                        (title_id, vg_id)
                    };
                    let video_game_source_id = if provider_items_exists {
                        Some(
                            entity_cache
                                .ensure_provider_item(provider_id, guid, None, false)
                                .await?,
                        )
                    } else {
                        None
                    };
                    // Optional primary cover image - extract all URL variants
                    if let Some(img) = obj.get("image") {
                        let original = img.get("original_url").and_then(|u| u.as_str());
                        let super_url = img.get("super_url").and_then(|u| u.as_str());
                        let small = img.get("small_url").and_then(|u| u.as_str());

                        let primary = original.or(super_url).or(small);

                        if let Some(image_url) = primary {
                            if !image_url.is_empty() {
                                let tuples = vec![(
                                    image_url.to_string(),
                                    Some("image".into()),
                                    Some("cover".into()),
                                    Some(name.to_string()),
                                )];
                                let meta = serde_json::json!({"source":"gb"});
                                // Align source naming with media_source enum variant 'giant_bomb'
                                if let Some(video_game_source_id) = video_game_source_id {
                                    let _ = ensure_vg_source_media_links_with_meta(
                                        db,
                                        video_game_source_id,
                                        Some(vg_id),
                                        &tuples,
                                        "giant_bomb",
                                        Some(meta),
                                    )
                                    .await?;
                                }
                                // Upsert into game_media with all URL variants
                                let pdata = serde_json::json!({
                                    "original_url": original,
                                    "super_url": super_url,
                                    "small_url": small,
                                    "source": "giant_bomb"
                                });
                                let _ = upsert_game_media(
                                    db,
                                    vg_id,
                                    "giant_bomb",
                                    image_url,
                                    "cover",
                                    image_url,
                                    pdata,
                                )
                                .await;
                            }
                        }
                    }
                    // Additional gallery images under results.images[]
                    if let Some(images) = obj.get("images").and_then(|v| v.as_array()) {
                        let mut tuples: Vec<(
                            String,
                            Option<String>,
                            Option<String>,
                            Option<String>,
                        )> = Vec::new();
                        for (idx, img) in images.iter().enumerate() {
                            // Extract all three URL variants from GiantBomb response
                            let original = img.get("original_url").and_then(|u| u.as_str());
                            let super_url = img.get("super_url").and_then(|u| u.as_str());
                            let small = img.get("small_url").and_then(|u| u.as_str());

                            // Select best available as primary URL
                            let primary = original.or(super_url).or(small);

                            if let Some(url) = primary {
                                if !url.is_empty() {
                                    tuples.push((
                                        url.to_string(),
                                        Some("image".into()),
                                        Some("gallery".into()),
                                        Some(format!("{} screenshot {}", name, idx + 1)),
                                    ));

                                    // Store in game_media with ALL URL variants in provider_data
                                    // Migration 0492 will extract these to dedicated columns:
                                    // - original_url from provider_data->>'original_url'
                                    // - thumbnail_url from provider_data->>'small_url'
                                    let provider_data = serde_json::json!({
                                        "original_url": original,
                                        "super_url": super_url,
                                        "small_url": small,
                                        "source": "giant_bomb"
                                    });

                                    let _ = upsert_game_media(
                                        db,
                                        vg_id,
                                        "giant_bomb",
                                        url,
                                        "screenshot",
                                        url,
                                        provider_data,
                                    )
                                    .await;
                                }
                            }
                        }
                        if !tuples.is_empty() {
                            let meta = serde_json::json!({"source":"gb","kind":"images"});
                            if let Some(video_game_source_id) = video_game_source_id {
                                let _ = ensure_vg_source_media_links_with_meta(
                                    db,
                                    video_game_source_id,
                                    Some(vg_id),
                                    &tuples,
                                    "giant_bomb",
                                    Some(meta),
                                )
                                .await?;
                            }
                        }
                    }
                    // Videos under results.videos[] (prefer hd/high url)
                    {
                        let mut tuples: Vec<(
                            String,
                            Option<String>,
                            Option<String>,
                            Option<String>,
                        )> = Vec::new();
                        let mut seen: HashSet<String> = HashSet::new();

                        // Primary video list: videos[] or raw_results.videos[]
                        let videos = obj.get("videos").and_then(|v| v.as_array()).or_else(|| {
                            obj.get("raw_results")
                                .and_then(|r| r.get("videos"))
                                .and_then(|v| v.as_array())
                        });
                        if let Some(videos) = videos {
                            for vid in videos.iter() {
                                let url = vid
                                    .get("hd_url")
                                    .and_then(|u| u.as_str())
                                    .or_else(|| vid.get("high_url").and_then(|u| u.as_str()))
                                    .or_else(|| vid.get("low_url").and_then(|u| u.as_str()))
                                    .or_else(|| vid.get("playable_url").and_then(|u| u.as_str()))
                                    .or_else(|| vid.get("url").and_then(|u| u.as_str()));
                                let Some(url) = url else { continue };
                                if url.is_empty() || !seen.insert(url.to_string()) {
                                    continue;
                                }
                                let title = vid
                                    .get("name")
                                    .and_then(|t| t.as_str())
                                    .map(|s| s.to_string())
                                    .or_else(|| Some(format!("{} video", name)));
                                tuples.push((
                                    url.to_string(),
                                    Some("video".into()),
                                    Some("video".into()),
                                    title,
                                ));

                                // Classify into trailer/gameplay based on video_type
                                let vtype =
                                    vid.get("video_type").and_then(|t| t.as_str()).unwrap_or("");
                                let mt = if vtype.eq_ignore_ascii_case("trailer") {
                                    "trailer"
                                } else {
                                    "gameplay"
                                };
                                let pdata = serde_json::json!({
                                    "video_type": vtype,
                                    "source": "videos",
                                });
                                let _ =
                                    upsert_game_media(db, vg_id, "giant_bomb", url, mt, url, pdata)
                                        .await;
                            }
                        }

                        // Secondary: video_api_payloads[*].payload.results.{hd_url,high_url,low_url}
                        if let Some(payloads) =
                            obj.get("video_api_payloads").and_then(|v| v.as_array())
                        {
                            for p in payloads.iter() {
                                let res = p.pointer("/payload/results");
                                let Some(res) = res else { continue };
                                let url = res
                                    .get("hd_url")
                                    .and_then(|u| u.as_str())
                                    .or_else(|| res.get("high_url").and_then(|u| u.as_str()))
                                    .or_else(|| res.get("low_url").and_then(|u| u.as_str()));
                                let Some(url) = url else { continue };
                                if url.is_empty() || !seen.insert(url.to_string()) {
                                    continue;
                                }
                                let title = res
                                    .get("name")
                                    .and_then(|t| t.as_str())
                                    .map(|s| s.to_string())
                                    .or_else(|| Some(format!("{} video", name)));
                                tuples.push((
                                    url.to_string(),
                                    Some("video".into()),
                                    Some("video".into()),
                                    title,
                                ));

                                let vtype =
                                    res.get("video_type").and_then(|t| t.as_str()).unwrap_or("");
                                let mt = if vtype.eq_ignore_ascii_case("trailer") {
                                    "trailer"
                                } else {
                                    "gameplay"
                                };
                                let pdata = serde_json::json!({
                                    "video_type": vtype,
                                    "source": "video_api_payloads",
                                });
                                let _ =
                                    upsert_game_media(db, vg_id, "giant_bomb", url, mt, url, pdata)
                                        .await;
                            }
                        }

                        if !tuples.is_empty() {
                            let meta = serde_json::json!({"source":"gb","kind":"videos"});
                            if let Some(video_game_source_id) = video_game_source_id {
                                let _ = ensure_vg_source_media_links_with_meta(
                                    db,
                                    video_game_source_id,
                                    Some(vg_id),
                                    &tuples,
                                    "giant_bomb",
                                    Some(meta),
                                )
                                .await?;
                            }
                        }
                    }
                    count += 1;
                }
            }
        }
        // Alternate dump shape: array of game objects
        Value::Array(items) => {
            for val in items.into_iter() {
                if !resume_passed_arr {
                    if let Some(guid_cur) = val.get("guid").and_then(|g| g.as_str()) {
                        if resume_after_guid.as_deref() == Some(guid_cur) {
                            resume_passed_arr = true;
                            resume_guid_found = true;
                            continue; // skip the resume marker entry itself
                        }
                    }
                    continue;
                }
                if let Some(lim) = limit {
                    if count >= lim {
                        break;
                    }
                }
                if let Value::Object(obj) = val {
                    // name
                    let name = obj.get("name").and_then(|n| n.as_str()).unwrap_or("");
                    if name.trim().is_empty() {
                        continue;
                    }
                    let slug = normalize_title(name);
                    // guid is required for provider item; fall back to slug if missing
                    let guid = obj
                        .get("guid")
                        .and_then(|g| g.as_str())
                        .unwrap_or_else(|| slug.as_str());
                    let product_id = entity_cache
                        .ensure_product_named("software", &slug, name)
                        .await?;
                    entity_cache.ensure_software_row(product_id).await?;
                    let (_title_id, vg_id) = if titles_keyed_by_source_item {
                        let sid = source_id
                            .expect("source_id must exist when titles_keyed_by_source_item");
                        if titles_has_video_game_id {
                            if !video_games_is_laravel {
                                anyhow::bail!(
                                    "video_game_titles.video_game_id is present but video_games is not Laravel-style (missing product_id/title)"
                                );
                            }
                            let canonical_vg_id = entity_cache
                                .ensure_video_game_for_product_laravel(
                                    product_id,
                                    name,
                                    Some(&slug),
                                    None,
                                    GIANTBOMB_PROVIDER_KEY,
                                )
                                .await?;
                            let title_id = entity_cache
                                .ensure_video_game_title_for_source_item(
                                    sid,
                                    guid,
                                    Some(product_id),
                                    Some(canonical_vg_id),
                                    name,
                                    Some(&slug),
                                    None,
                                    None,
                                )
                                .await?;
                            (title_id, canonical_vg_id)
                        } else {
                            let legacy_link_id = if titles_has_product_id {
                                Some(product_id)
                            } else {
                                None
                            };
                            let title_id = entity_cache
                                .ensure_video_game_title_for_source_item(
                                    sid,
                                    guid,
                                    legacy_link_id,
                                    None,
                                    name,
                                    Some(&slug),
                                    None,
                                    None,
                                )
                                .await?;
                            // Laravel schema: use product_id directly
                            let vg_id = entity_cache
                                .ensure_video_game_for_product_laravel(
                                    product_id,
                                    name,
                                    Some(&slug),
                                    None,
                                    GIANTBOMB_PROVIDER_KEY,
                                )
                                .await?;
                            (title_id, vg_id)
                        }
                    } else {
                        let title_id = entity_cache
                            .ensure_video_game_title(product_id, name, Some(&slug))
                            .await?;
                        // Laravel schema: use product_id directly
                        let vg_id = entity_cache
                            .ensure_video_game_for_product_laravel(
                                product_id,
                                name,
                                Some(&slug),
                                None,
                                GIANTBOMB_PROVIDER_KEY,
                            )
                            .await?;
                        (title_id, vg_id)
                    };
                    let video_game_source_id = if provider_items_exists {
                        Some(
                            entity_cache
                                .ensure_provider_item(provider_id, guid, None, false)
                                .await?,
                        )
                    } else {
                        None
                    };
                    if let Some(img) = obj.get("image") {
                        let original = img.get("original_url").and_then(|u| u.as_str());
                        let super_url = img.get("super_url").and_then(|u| u.as_str());
                        let small = img.get("small_url").and_then(|u| u.as_str());

                        let primary = original.or(super_url).or(small);

                        if let Some(image_url) = primary {
                            if !image_url.is_empty() {
                                let tuples = vec![(
                                    image_url.to_string(),
                                    Some("image".into()),
                                    Some("cover".into()),
                                    Some(name.to_string()),
                                )];
                                let meta = serde_json::json!({"source":"gb"});
                                // Align source naming with media_source enum variant 'giant_bomb'
                                if let Some(video_game_source_id) = video_game_source_id {
                                    let _ = ensure_vg_source_media_links_with_meta(
                                        db,
                                        video_game_source_id,
                                        Some(vg_id),
                                        &tuples,
                                        "giant_bomb",
                                        Some(meta),
                                    )
                                    .await?;
                                }
                                let pdata = serde_json::json!({
                                    "original_url": original,
                                    "super_url": super_url,
                                    "small_url": small,
                                    "source": "giant_bomb"
                                });
                                let _ = upsert_game_media(
                                    db,
                                    vg_id,
                                    "giant_bomb",
                                    image_url,
                                    "cover",
                                    image_url,
                                    pdata,
                                )
                                .await;
                            }
                        }
                    }
                    // Additional gallery images
                    if let Some(images) = obj.get("images").and_then(|v| v.as_array()) {
                        let mut tuples: Vec<(
                            String,
                            Option<String>,
                            Option<String>,
                            Option<String>,
                        )> = Vec::new();
                        for (idx, img) in images.iter().enumerate() {
                            // Extract all three URL variants from GiantBomb response
                            let original = img.get("original_url").and_then(|u| u.as_str());
                            let super_url = img.get("super_url").and_then(|u| u.as_str());
                            let small = img.get("small_url").and_then(|u| u.as_str());

                            // Select best available as primary URL
                            let primary = original.or(super_url).or(small);

                            if let Some(url) = primary {
                                if !url.is_empty() {
                                    tuples.push((
                                        url.to_string(),
                                        Some("image".into()),
                                        Some("gallery".into()),
                                        Some(format!("{} screenshot {}", name, idx + 1)),
                                    ));

                                    // Store in game_media with ALL URL variants in provider_data
                                    let provider_data = serde_json::json!({
                                        "original_url": original,
                                        "super_url": super_url,
                                        "small_url": small,
                                        "source": "giant_bomb"
                                    });

                                    let _ = upsert_game_media(
                                        db,
                                        vg_id,
                                        "giant_bomb",
                                        url,
                                        "screenshot",
                                        url,
                                        provider_data,
                                    )
                                    .await;
                                }
                            }
                        }
                        if !tuples.is_empty() {
                            let meta = serde_json::json!({"source":"gb","kind":"images"});
                            if let Some(video_game_source_id) = video_game_source_id {
                                let _ = ensure_vg_source_media_links_with_meta(
                                    db,
                                    video_game_source_id,
                                    Some(vg_id),
                                    &tuples,
                                    "giant_bomb",
                                    Some(meta),
                                )
                                .await?;
                            }
                        }
                    }
                    {
                        let mut tuples: Vec<(
                            String,
                            Option<String>,
                            Option<String>,
                            Option<String>,
                        )> = Vec::new();
                        let mut seen: HashSet<String> = HashSet::new();

                        let videos = obj.get("videos").and_then(|v| v.as_array()).or_else(|| {
                            obj.get("raw_results")
                                .and_then(|r| r.get("videos"))
                                .and_then(|v| v.as_array())
                        });
                        if let Some(videos) = videos {
                            for vid in videos.iter() {
                                let url = vid
                                    .get("hd_url")
                                    .and_then(|u| u.as_str())
                                    .or_else(|| vid.get("high_url").and_then(|u| u.as_str()))
                                    .or_else(|| vid.get("low_url").and_then(|u| u.as_str()))
                                    .or_else(|| vid.get("playable_url").and_then(|u| u.as_str()))
                                    .or_else(|| vid.get("url").and_then(|u| u.as_str()));
                                let Some(url) = url else { continue };
                                if url.is_empty() || !seen.insert(url.to_string()) {
                                    continue;
                                }
                                let title = vid
                                    .get("name")
                                    .and_then(|t| t.as_str())
                                    .map(|s| s.to_string())
                                    .or_else(|| Some(format!("{} video", name)));
                                tuples.push((
                                    url.to_string(),
                                    Some("video".into()),
                                    Some("video".into()),
                                    title,
                                ));
                                let vtype =
                                    vid.get("video_type").and_then(|t| t.as_str()).unwrap_or("");
                                let mt = if vtype.eq_ignore_ascii_case("trailer") {
                                    "trailer"
                                } else {
                                    "gameplay"
                                };
                                let pdata = serde_json::json!({
                                    "video_type": vtype,
                                    "source": "videos",
                                });
                                let _ =
                                    upsert_game_media(db, vg_id, "giant_bomb", url, mt, url, pdata)
                                        .await;
                            }
                        }

                        if let Some(payloads) =
                            obj.get("video_api_payloads").and_then(|v| v.as_array())
                        {
                            for p in payloads.iter() {
                                let res = p.pointer("/payload/results");
                                let Some(res) = res else { continue };
                                let url = res
                                    .get("hd_url")
                                    .and_then(|u| u.as_str())
                                    .or_else(|| res.get("high_url").and_then(|u| u.as_str()))
                                    .or_else(|| res.get("low_url").and_then(|u| u.as_str()));
                                let Some(url) = url else { continue };
                                if url.is_empty() || !seen.insert(url.to_string()) {
                                    continue;
                                }
                                let title = res
                                    .get("name")
                                    .and_then(|t| t.as_str())
                                    .map(|s| s.to_string())
                                    .or_else(|| Some(format!("{} video", name)));
                                tuples.push((
                                    url.to_string(),
                                    Some("video".into()),
                                    Some("video".into()),
                                    title,
                                ));
                                let vtype =
                                    res.get("video_type").and_then(|t| t.as_str()).unwrap_or("");
                                let mt = if vtype.eq_ignore_ascii_case("trailer") {
                                    "trailer"
                                } else {
                                    "gameplay"
                                };
                                let pdata = serde_json::json!({
                                    "video_type": vtype,
                                    "source": "video_api_payloads",
                                });
                                let _ =
                                    upsert_game_media(db, vg_id, "giant_bomb", url, mt, url, pdata)
                                        .await;
                            }
                        }

                        if !tuples.is_empty() {
                            let meta = serde_json::json!({"source":"gb","kind":"videos"});
                            if let Some(video_game_source_id) = video_game_source_id {
                                let _ = ensure_vg_source_media_links_with_meta(
                                    db,
                                    video_game_source_id,
                                    Some(vg_id),
                                    &tuples,
                                    "giant_bomb",
                                    Some(meta),
                                )
                                .await?;
                            }
                        }
                    }
                    count += 1;
                }
            }
        }
        _ => {}
    }

    if let Some(ref resume_guid) = resume_after_guid {
        if !resume_guid_found {
            warn!(%resume_guid, "GB_RESUME_AFTER_GUID was provided but never encountered; no records processed");
        }
    }

    Ok(count)
}

/// Ingest a GiantBomb NDJSON stream (one JSON object per line).
/// Path None means read from stdin. Supports GB_RESUME_AFTER_GUID and limit.
#[instrument(skip(db))]
pub async fn ingest_from_ndjson(
    db: &Db,
    path: Option<&str>,
    limit: Option<usize>,
) -> Result<usize> {
    // Ensure public schema
    let _ = sqlx::query("SET search_path TO public")
        .persistent(false)
        .execute(&db.pool)
        .await;
    // Year-range reminder (informational only). Print only when env overrides are provided to reduce log noise.
    let year_min_env = std::env::var("YEAR_MIN").ok();
    let year_max_env = std::env::var("YEAR_MAX").ok();
    if year_min_env.is_some() || year_max_env.is_some() {
        let year_min: i32 = year_min_env.and_then(|s| s.parse().ok()).unwrap_or(2020);
        let year_max: i32 = year_max_env.and_then(|s| s.parse().ok()).unwrap_or(2025);
        println!(
            "remember: restricting to releases between {year_min}-{year_max} inclusive (where applicable)\n"
        );
    }

    // Resume control
    let resume_after_guid = std::env::var("GB_RESUME_AFTER_GUID")
        .ok()
        .filter(|s| !s.trim().is_empty());
    let mut resume_passed = resume_after_guid.is_none();
    let mut resume_guid_found = resume_after_guid.is_none();

    // Prepare provider/platform once
    let provider_id = ensure_provider(db, "giantbomb", "catalog", Some("giantbomb")).await?;

    let provider_items_exists = table_exists(db, "provider_items").await?;
    if !provider_items_exists {
        warn!("provider_items table missing; skipping provider_items and provider_media_links writes (legacy DB compat)");
    }

    // Schema detection (same as JSON ingest)
    let titles_has_source_id =
        column_exists(db, "video_game_titles", "video_game_source_id").await?;
    let titles_has_vg_source_item_id =
        column_exists(db, "video_game_titles", "vg_source_item_id").await?;
    let sources_table_exists = table_exists(db, "video_game_sources").await?;
    if titles_has_source_id && titles_has_vg_source_item_id && !sources_table_exists {
        warn!(
            "video_game_titles has (video_game_source_id, vg_source_item_id) but video_game_sources table is missing; falling back to legacy title schema"
        );
    }
    let titles_keyed_by_source_item =
        titles_has_source_id && titles_has_vg_source_item_id && sources_table_exists;
    let titles_has_video_game_id = titles_keyed_by_source_item
        && column_exists(db, "video_game_titles", "video_game_id").await?;
    let titles_has_product_id =
        titles_keyed_by_source_item && column_exists(db, "video_game_titles", "product_id").await?;
    let video_games_is_laravel = if titles_has_video_game_id {
        column_exists(db, "video_games", "product_id").await?
            && column_exists(db, "video_games", "title").await?
    } else {
        false
    };
    let source_id = if titles_keyed_by_source_item {
        Some(ensure_video_game_source(db, "giantbomb", "Giant Bomb").await?)
    } else {
        None
    };

    let platform_id = ensure_platform(db, "PC", Some("pc")).await?;
    let mut entity_cache = ProviderEntityCache::new(db.clone());

    // Open reader
    let reader: Box<dyn BufRead> = match path {
        Some(p) => {
            let file = fs::File::open(p)?;
            Box::new(BufReader::new(file))
        }
        None => {
            let stdin = std::io::stdin();
            Box::new(BufReader::new(stdin))
        }
    };

    let mut count: usize = 0;
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        if let Some(lim) = limit {
            if count >= lim {
                break;
            }
        }
        let v: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error=%e, "skip malformed NDJSON line");
                continue;
            }
        };
        let obj = match v {
            Value::Object(o) => o,
            _ => {
                tracing::warn!("skip non-object NDJSON item");
                continue;
            }
        };
        // Resume gate based on guid field when present
        if !resume_passed {
            if let Some(guid_cur) = obj.get("guid").and_then(|g| g.as_str()) {
                if resume_after_guid.as_deref() == Some(guid_cur) {
                    resume_passed = true;
                    resume_guid_found = true;
                    continue; // skip the marker itself
                }
            }
            // guid missing or not matched yet; keep skipping until we encounter it
            continue;
        }

        // Extract name/slug
        let name = obj
            .get("name")
            .and_then(|n| n.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        if name.is_empty() {
            continue;
        }
        let slug = normalize_title(&name);

        // Ensure product/title/game
        // Provider item (require guid; fallback to slug)
        let guid = obj
            .get("guid")
            .and_then(|g| g.as_str())
            .unwrap_or_else(|| slug.as_str());
        let product_id = entity_cache
            .ensure_product_named("software", &slug, &name)
            .await?;
        entity_cache.ensure_software_row(product_id).await?;
        let (_title_id, vg_id) = if titles_keyed_by_source_item {
            let sid = source_id.expect("source_id must exist when titles_keyed_by_source_item");
            if titles_has_video_game_id {
                if !video_games_is_laravel {
                    anyhow::bail!(
                        "video_game_titles.video_game_id is present but video_games is not Laravel-style (missing product_id/title)"
                    );
                }
                let canonical_vg_id = entity_cache
                    .ensure_video_game_for_product_laravel(
                        product_id,
                        &name,
                        Some(&slug),
                        None,
                        GIANTBOMB_PROVIDER_KEY,
                    )
                    .await?;
                let title_id = entity_cache
                    .ensure_video_game_title_for_source_item(
                        sid,
                        guid,
                        Some(product_id),
                        Some(canonical_vg_id),
                        &name,
                        Some(&slug),
                        None,
                        None,
                    )
                    .await?;
                (title_id, canonical_vg_id)
            } else {
                let legacy_link_id = if titles_has_product_id {
                    Some(product_id)
                } else {
                    None
                };
                let title_id = entity_cache
                    .ensure_video_game_title_for_source_item(
                        sid,
                        guid,
                        legacy_link_id,
                        None,
                        &name,
                        Some(&slug),
                        None,
                        None,
                    )
                    .await?;
                // Laravel schema: use product_id directly
                let vg_id = entity_cache
                    .ensure_video_game_for_product_laravel(
                        product_id,
                        &name,
                        Some(&slug),
                        None,
                        GIANTBOMB_PROVIDER_KEY,
                    )
                    .await?;
                (title_id, vg_id)
            }
        } else {
            let title_id = entity_cache
                .ensure_video_game_title(product_id, &name, Some(&slug))
                .await?;
            // Laravel schema: use product_id directly
            let vg_id = entity_cache
                .ensure_video_game_for_product_laravel(
                    product_id,
                    &name,
                    Some(&slug),
                    None,
                    GIANTBOMB_PROVIDER_KEY,
                )
                .await?;
            (title_id, vg_id)
        };
        let video_game_source_id = if provider_items_exists {
            Some(
                entity_cache
                    .ensure_provider_item(provider_id, guid, None, false)
                    .await?,
            )
        } else {
            None
        };

        // Primary image - extract all URL variants
        if let Some(img) = obj.get("image") {
            let original = img.get("original_url").and_then(|u| u.as_str());
            let super_url = img.get("super_url").and_then(|u| u.as_str());
            let small = img.get("small_url").and_then(|u| u.as_str());

            let primary = original.or(super_url).or(small);

            if let Some(image_url) = primary {
                if !image_url.is_empty() {
                    let tuples = vec![(
                        image_url.to_string(),
                        Some("image".into()),
                        Some("cover".into()),
                        Some(name.clone()),
                    )];
                    let meta = serde_json::json!({"source":"gb"});
                    if let Some(video_game_source_id) = video_game_source_id {
                        let _ = ensure_vg_source_media_links_with_meta(
                            db,
                            video_game_source_id,
                            Some(vg_id),
                            &tuples,
                            "giant_bomb",
                            Some(meta),
                        )
                        .await?;
                    }
                    let pdata = serde_json::json!({
                        "original_url": original,
                        "super_url": super_url,
                        "small_url": small,
                        "source": "giant_bomb"
                    });
                    let _ = upsert_game_media(
                        db,
                        vg_id,
                        "giant_bomb",
                        image_url,
                        "cover",
                        image_url,
                        pdata,
                    )
                    .await;
                }
            }
        }

        // Gallery images
        if let Some(images) = obj.get("images").and_then(|v| v.as_array()) {
            let mut tuples: Vec<(String, Option<String>, Option<String>, Option<String>)> =
                Vec::new();
            for (idx, img) in images.iter().enumerate() {
                // Extract all three URL variants from GiantBomb response
                let original = img.get("original_url").and_then(|u| u.as_str());
                let super_url = img.get("super_url").and_then(|u| u.as_str());
                let small = img.get("small_url").and_then(|u| u.as_str());

                // Select best available as primary URL
                let primary = original.or(super_url).or(small);

                if let Some(url) = primary {
                    if !url.is_empty() {
                        tuples.push((
                            url.to_string(),
                            Some("image".into()),
                            Some("gallery".into()),
                            Some(format!("{} screenshot {}", name, idx + 1)),
                        ));

                        // Store in game_media with ALL URL variants in provider_data
                        let provider_data = serde_json::json!({
                            "original_url": original,
                            "super_url": super_url,
                            "small_url": small,
                            "source": "giant_bomb"
                        });

                        let _ = upsert_game_media(
                            db,
                            vg_id,
                            "giant_bomb",
                            url,
                            "screenshot",
                            url,
                            provider_data,
                        )
                        .await;
                    }
                }
            }
            if !tuples.is_empty() {
                let meta = serde_json::json!({"source":"gb","kind":"images"});
                if let Some(video_game_source_id) = video_game_source_id {
                    let _ = ensure_vg_source_media_links_with_meta(
                        db,
                        video_game_source_id,
                        Some(vg_id),
                        &tuples,
                        "giant_bomb",
                        Some(meta),
                    )
                    .await?;
                }
            }
        }

        // Videos
        if let Some(videos) = obj.get("videos").and_then(|v| v.as_array()) {
            let mut tuples: Vec<(String, Option<String>, Option<String>, Option<String>)> =
                Vec::new();
            for vid in videos.iter() {
                let url = vid
                    .get("hd_url")
                    .and_then(|u| u.as_str())
                    .or_else(|| vid.get("high_url").and_then(|u| u.as_str()))
                    .or_else(|| vid.get("low_url").and_then(|u| u.as_str()))
                    .or_else(|| vid.get("playable_url").and_then(|u| u.as_str()))
                    .or_else(|| vid.get("url").and_then(|u| u.as_str()));
                if let Some(url) = url {
                    if !url.is_empty() {
                        let title = vid
                            .get("name")
                            .and_then(|t| t.as_str())
                            .map(|s| s.to_string())
                            .or_else(|| Some(format!("{} trailer", name)));
                        tuples.push((
                            url.to_string(),
                            Some("video".into()),
                            Some("trailer".into()),
                            title,
                        ));
                        let vtype = vid.get("video_type").and_then(|t| t.as_str()).unwrap_or("");
                        let mt = if vtype.eq_ignore_ascii_case("trailer") {
                            "trailer"
                        } else {
                            "gameplay"
                        };
                        let pdata = serde_json::json!({"video_type": vtype});
                        let _ =
                            upsert_game_media(db, vg_id, "giant_bomb", url, mt, url, pdata).await;
                    }
                }
            }
            if !tuples.is_empty() {
                let meta = serde_json::json!({"source":"gb","kind":"videos"});
                if let Some(video_game_source_id) = video_game_source_id {
                    let _ = ensure_vg_source_media_links_with_meta(
                        db,
                        video_game_source_id,
                        Some(vg_id),
                        &tuples,
                        "giant_bomb",
                        Some(meta),
                    )
                    .await?;
                }
            }
        }

        count += 1;
        info!(count, guid = guid, "processed GiantBomb NDJSON entry");
    }

    if let Some(ref resume_guid) = resume_after_guid {
        if !resume_guid_found {
            warn!(%resume_guid, "GB_RESUME_AFTER_GUID was provided but never encountered; no records processed");
        }
    }

    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_video_urls_from_video_api_payloads_results() {
        let v = serde_json::json!({
            "video_api_payloads": [
                {
                    "payload": {
                        "results": {
                            "name": "Trailer One",
                            "video_type": "Trailer",
                            "high_url": "https://static.example.com/trailer.mp4",
                            "low_url": "https://static.example.com/trailer-low.mp4"
                        }
                    }
                },
                {
                    "payload": {
                        "results": {
                            "name": "Trailer One (duplicate)",
                            "video_type": "Trailer",
                            "high_url": "https://static.example.com/trailer.mp4"
                        }
                    }
                }
            ]
        });

        let obj = v.as_object().expect("object");
        let payloads = obj
            .get("video_api_payloads")
            .and_then(|v| v.as_array())
            .expect("payload array");

        let mut seen: HashSet<String> = HashSet::new();
        let mut urls: Vec<String> = Vec::new();
        for p in payloads.iter() {
            let Some(res) = p.pointer("/payload/results") else {
                continue;
            };
            let url = res
                .get("hd_url")
                .and_then(|u| u.as_str())
                .or_else(|| res.get("high_url").and_then(|u| u.as_str()))
                .or_else(|| res.get("low_url").and_then(|u| u.as_str()));
            let Some(url) = url else { continue };
            if url.is_empty() || !seen.insert(url.to_string()) {
                continue;
            }
            urls.push(url.to_string());
        }

        assert_eq!(
            urls,
            vec!["https://static.example.com/trailer.mp4".to_string()]
        );
    }
}
