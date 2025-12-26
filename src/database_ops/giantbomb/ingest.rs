use anyhow::Result;
use serde_json::Value;
use std::{
    fs,
    io::{BufRead, BufReader},
    path::Path,
};
use tracing::{info, instrument, warn};

use crate::database_ops::db::Db;
use crate::database_ops::ingest_providers::{
    ProviderEntityCache, ensure_platform, ensure_provider, ensure_provider_media_links_with_meta,
    upsert_game_media,
};
use crate::database_ops::media_map::normalize_title;

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
                    let product_id = entity_cache
                        .ensure_product_named("software", &slug, name)
                        .await?;
                    entity_cache.ensure_software_row(product_id).await?;
                    let title_id = entity_cache
                        .ensure_video_game_title(product_id, name, Some(&slug))
                        .await?;
                    // Create (or ensure) a single platform instance of the game; edition unknown.
                    let vg_id = entity_cache
                        .ensure_video_game(title_id, platform_id, None)
                        .await?;
                    // Provider item keyed by guid
                    let guid = obj
                        .get("guid")
                        .and_then(|g| g.as_str())
                        .unwrap_or_else(|| k.as_str());
                    let provider_item_id = entity_cache
                        .ensure_provider_item(provider_id, guid, None, false)
                        .await?;
                    // Optional primary cover image (small_url)
                    if let Some(image_url) = obj
                        .get("image")
                        .and_then(|img| img.get("small_url"))
                        .and_then(|u| u.as_str())
                    {
                        if !image_url.is_empty() {
                            let tuples = vec![(
                                image_url.to_string(),
                                Some("image".into()),
                                Some("cover".into()),
                                Some(name.to_string()),
                            )];
                            let meta = serde_json::json!({"source":"gb","size":"small"});
                            // Align source naming with media_source enum variant 'giant_bomb'
                            let _ = ensure_provider_media_links_with_meta(
                                db,
                                provider_item_id,
                                Some(vg_id),
                                &tuples,
                                "giant_bomb",
                                Some(meta),
                            )
                            .await?;
                            // Upsert into game_media
                            let pdata = serde_json::json!({"size":"small"});
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
                    // Additional gallery images under results.images[]
                    if let Some(images) = obj.get("images").and_then(|v| v.as_array()) {
                        let mut tuples: Vec<(
                            String,
                            Option<String>,
                            Option<String>,
                            Option<String>,
                        )> = Vec::new();
                        for (idx, img) in images.iter().enumerate() {
                            if let Some(url) = img
                                .get("original_url")
                                .and_then(|u| u.as_str())
                                .or_else(|| img.get("super_url").and_then(|u| u.as_str()))
                                .or_else(|| img.get("small_url").and_then(|u| u.as_str()))
                            {
                                if !url.is_empty() {
                                    tuples.push((
                                        url.to_string(),
                                        Some("image".into()),
                                        Some("gallery".into()),
                                        Some(format!("{} screenshot {}", name, idx + 1)),
                                    ));
                                    // Also persist into game_media as screenshot
                                    let _ = upsert_game_media(
                                        db,
                                        vg_id,
                                        "giant_bomb",
                                        url,
                                        "screenshot",
                                        url,
                                        serde_json::json!({"size":"orig|super|small"}),
                                    )
                                    .await;
                                }
                            }
                        }
                        if !tuples.is_empty() {
                            let meta = serde_json::json!({"source":"gb","kind":"images"});
                            let _ = ensure_provider_media_links_with_meta(
                                db,
                                provider_item_id,
                                Some(vg_id),
                                &tuples,
                                "giant_bomb",
                                Some(meta),
                            )
                            .await?;
                        }
                    }
                    // Videos under results.videos[] (prefer hd/high url)
                    if let Some(videos) = obj.get("videos").and_then(|v| v.as_array()) {
                        let mut tuples: Vec<(
                            String,
                            Option<String>,
                            Option<String>,
                            Option<String>,
                        )> = Vec::new();
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
                                    // Classify into trailer/gameplay based on video_type
                                    let vtype = vid
                                        .get("video_type")
                                        .and_then(|t| t.as_str())
                                        .unwrap_or("");
                                    let mt = if vtype.eq_ignore_ascii_case("trailer") {
                                        "trailer"
                                    } else {
                                        "gameplay"
                                    };
                                    let pdata = serde_json::json!({
                                        "video_type": vtype,
                                    });
                                    let _ = upsert_game_media(
                                        db,
                                        vg_id,
                                        "giant_bomb",
                                        url,
                                        mt,
                                        url,
                                        pdata,
                                    )
                                    .await;
                                }
                            }
                        }
                        if !tuples.is_empty() {
                            let meta = serde_json::json!({"source":"gb","kind":"videos"});
                            let _ = ensure_provider_media_links_with_meta(
                                db,
                                provider_item_id,
                                Some(vg_id),
                                &tuples,
                                "giant_bomb",
                                Some(meta),
                            )
                            .await?;
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
                    let product_id = entity_cache
                        .ensure_product_named("software", &slug, name)
                        .await?;
                    entity_cache.ensure_software_row(product_id).await?;
                    let title_id = entity_cache
                        .ensure_video_game_title(product_id, name, Some(&slug))
                        .await?;
                    let vg_id = entity_cache
                        .ensure_video_game(title_id, platform_id, None)
                        .await?;
                    // guid is required for provider item; fall back to slug if missing
                    let guid = obj
                        .get("guid")
                        .and_then(|g| g.as_str())
                        .unwrap_or_else(|| slug.as_str());
                    let provider_item_id = entity_cache
                        .ensure_provider_item(provider_id, guid, None, false)
                        .await?;
                    if let Some(image_url) = obj
                        .get("image")
                        .and_then(|img| img.get("small_url"))
                        .and_then(|u| u.as_str())
                    {
                        if !image_url.is_empty() {
                            let tuples = vec![(
                                image_url.to_string(),
                                Some("image".into()),
                                Some("cover".into()),
                                Some(name.to_string()),
                            )];
                            let meta = serde_json::json!({"source":"gb","size":"small"});
                            // Align source naming with media_source enum variant 'giant_bomb'
                            let _ = ensure_provider_media_links_with_meta(
                                db,
                                provider_item_id,
                                Some(vg_id),
                                &tuples,
                                "giant_bomb",
                                Some(meta),
                            )
                            .await?;
                            let pdata = serde_json::json!({"size":"small"});
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
                    // Additional gallery images
                    if let Some(images) = obj.get("images").and_then(|v| v.as_array()) {
                        let mut tuples: Vec<(
                            String,
                            Option<String>,
                            Option<String>,
                            Option<String>,
                        )> = Vec::new();
                        for (idx, img) in images.iter().enumerate() {
                            if let Some(url) = img
                                .get("original_url")
                                .and_then(|u| u.as_str())
                                .or_else(|| img.get("super_url").and_then(|u| u.as_str()))
                                .or_else(|| img.get("small_url").and_then(|u| u.as_str()))
                            {
                                if !url.is_empty() {
                                    tuples.push((
                                        url.to_string(),
                                        Some("image".into()),
                                        Some("gallery".into()),
                                        Some(format!("{} screenshot {}", name, idx + 1)),
                                    ));
                                    let _ = upsert_game_media(
                                        db,
                                        vg_id,
                                        "giant_bomb",
                                        url,
                                        "screenshot",
                                        url,
                                        serde_json::json!({"size":"orig|super|small"}),
                                    )
                                    .await;
                                }
                            }
                        }
                        if !tuples.is_empty() {
                            let meta = serde_json::json!({"source":"gb","kind":"images"});
                            let _ = ensure_provider_media_links_with_meta(
                                db,
                                provider_item_id,
                                Some(vg_id),
                                &tuples,
                                "giant_bomb",
                                Some(meta),
                            )
                            .await?;
                        }
                    }
                    // Videos
                    if let Some(videos) = obj.get("videos").and_then(|v| v.as_array()) {
                        let mut tuples: Vec<(
                            String,
                            Option<String>,
                            Option<String>,
                            Option<String>,
                        )> = Vec::new();
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
                                    let vtype = vid
                                        .get("video_type")
                                        .and_then(|t| t.as_str())
                                        .unwrap_or("");
                                    let mt = if vtype.eq_ignore_ascii_case("trailer") {
                                        "trailer"
                                    } else {
                                        "gameplay"
                                    };
                                    let pdata = serde_json::json!({"video_type": vtype});
                                    let _ = upsert_game_media(
                                        db,
                                        vg_id,
                                        "giant_bomb",
                                        url,
                                        mt,
                                        url,
                                        pdata,
                                    )
                                    .await;
                                }
                            }
                        }
                        if !tuples.is_empty() {
                            let meta = serde_json::json!({"source":"gb","kind":"videos"});
                            let _ = ensure_provider_media_links_with_meta(
                                db,
                                provider_item_id,
                                Some(vg_id),
                                &tuples,
                                "giant_bomb",
                                Some(meta),
                            )
                            .await?;
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
        let product_id = entity_cache
            .ensure_product_named("software", &slug, &name)
            .await?;
        entity_cache.ensure_software_row(product_id).await?;
        let title_id = entity_cache
            .ensure_video_game_title(product_id, &name, Some(&slug))
            .await?;
        let vg_id = entity_cache
            .ensure_video_game(title_id, platform_id, None)
            .await?;

        // Provider item (require guid; fallback to slug)
        let guid = obj
            .get("guid")
            .and_then(|g| g.as_str())
            .unwrap_or_else(|| slug.as_str());
        let provider_item_id = entity_cache
            .ensure_provider_item(provider_id, guid, None, false)
            .await?;

        // Primary image
        if let Some(image_url) = obj
            .get("image")
            .and_then(|img| img.get("small_url"))
            .and_then(|u| u.as_str())
        {
            if !image_url.is_empty() {
                let tuples = vec![(
                    image_url.to_string(),
                    Some("image".into()),
                    Some("cover".into()),
                    Some(name.clone()),
                )];
                let meta = serde_json::json!({"source":"gb","size":"small"});
                let _ = ensure_provider_media_links_with_meta(
                    db,
                    provider_item_id,
                    Some(vg_id),
                    &tuples,
                    "giant_bomb",
                    Some(meta),
                )
                .await?;
                let pdata = serde_json::json!({"size":"small"});
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

        // Gallery images
        if let Some(images) = obj.get("images").and_then(|v| v.as_array()) {
            let mut tuples: Vec<(String, Option<String>, Option<String>, Option<String>)> =
                Vec::new();
            for (idx, img) in images.iter().enumerate() {
                if let Some(url) = img
                    .get("original_url")
                    .and_then(|u| u.as_str())
                    .or_else(|| img.get("super_url").and_then(|u| u.as_str()))
                    .or_else(|| img.get("small_url").and_then(|u| u.as_str()))
                {
                    if !url.is_empty() {
                        tuples.push((
                            url.to_string(),
                            Some("image".into()),
                            Some("gallery".into()),
                            Some(format!("{} screenshot {}", name, idx + 1)),
                        ));
                        let _ = upsert_game_media(
                            db,
                            vg_id,
                            "giant_bomb",
                            url,
                            "screenshot",
                            url,
                            serde_json::json!({"size":"orig|super|small"}),
                        )
                        .await;
                    }
                }
            }
            if !tuples.is_empty() {
                let meta = serde_json::json!({"source":"gb","kind":"images"});
                let _ = ensure_provider_media_links_with_meta(
                    db,
                    provider_item_id,
                    Some(vg_id),
                    &tuples,
                    "giant_bomb",
                    Some(meta),
                )
                .await?;
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
                let _ = ensure_provider_media_links_with_meta(
                    db,
                    provider_item_id,
                    Some(vg_id),
                    &tuples,
                    "giant_bomb",
                    Some(meta),
                )
                .await?;
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
