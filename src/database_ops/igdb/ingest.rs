use anyhow::Result;
use serde_json::Value;
use std::{fs, path::Path};
use tracing::instrument;

use crate::database_ops::db::Db;
use crate::database_ops::ingest_providers::{
    ensure_platform, ensure_provider, ensure_vg_source_media_links_with_meta, upsert_game_media,
    ProviderEntityCache,
};
use crate::database_ops::media_map::normalize_title;

const IGDB_PROVIDER_KEY: &str = "igdb";

/// Ingest IGDB-style JSON (array or object) into core entities and media.
/// Expected fields per item (best-effort):
/// - id (number|string), name (string), slug (string?), cover.url (string?)
/// Unknown shapes are skipped gracefully.
#[instrument(skip(db))]
pub async fn ingest_from_file(db: &Db, path: &str, limit: Option<usize>) -> Result<usize> {
    let p = Path::new(path);
    if !p.exists() {
        return Ok(0);
    }
    let raw = fs::read_to_string(p)?;
    let v: Value = serde_json::from_str(&raw)?;

    let provider_id = ensure_provider(db, "igdb", "catalog", Some("igdb")).await?;
    // Use a generic platform to ensure a video_game row exists for media linking
    let platform_id = ensure_platform(db, "Playstation 5", Some("playstation-5")).await?;
    let mut entity_cache = ProviderEntityCache::new(db.clone());

    let mut count = 0usize;
    match v {
        Value::Array(items) => {
            for item in items.into_iter() {
                if let Some(lim) = limit {
                    if count >= lim {
                        break;
                    }
                }
                if let Some(handled) =
                    handle_one(db, &mut entity_cache, provider_id, platform_id, item).await?
                {
                    if handled {
                        count += 1;
                    }
                }
            }
        }
        Value::Object(map) => {
            for (_k, item) in map.into_iter() {
                if let Some(lim) = limit {
                    if count >= lim {
                        break;
                    }
                }
                if let Some(handled) =
                    handle_one(db, &mut entity_cache, provider_id, platform_id, item).await?
                {
                    if handled {
                        count += 1;
                    }
                }
            }
        }
        _ => {}
    }
    Ok(count)
}

#[instrument(skip(db, cache))]
async fn handle_one(
    db: &Db,
    cache: &mut ProviderEntityCache,
    provider_id: i64,
    platform_id: i64,
    item: Value,
) -> Result<Option<bool>> {
    let obj = match item {
        Value::Object(o) => o,
        _ => {
            return Ok(None);
        }
    };
    let name = obj
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    if name.is_empty() {
        return Ok(None);
    }
    let slug = obj
        .get("slug")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| normalize_title(&name));

    // Create product -> software -> title -> video_game
    let product_id = cache.ensure_product_named("software", &slug, &name).await?;
    cache.ensure_software_row(product_id).await?;
    let _title_id = cache
        .ensure_video_game_title(product_id, &name, Some(&slug))
        .await?;
    // Laravel schema: use product_id directly
    let vg_id = cache
        .ensure_video_game_for_product_laravel(
            product_id,
            &name,
            Some(&slug),
            Some(Value::Object(obj.clone())),
            IGDB_PROVIDER_KEY,
        )
        .await?;

    // External id (stringify whatever IGDB provides)
    let ext_id = obj
        .get("id")
        .map(|v| match v {
            Value::Number(n) => n.to_string(),
            Value::String(s) => s.clone(),
            _ => slug.clone(),
        })
        .unwrap_or_else(|| slug.clone());
    let video_game_source_id = cache
        .ensure_provider_item(provider_id, &ext_id, Some(Value::Object(obj.clone())), true)
        .await?;

    // Cover/url best-effort extraction
    let cover_url = obj
        .get("cover")
        .and_then(|c| c.get("url"))
        .and_then(|u| u.as_str())
        .or_else(|| {
            obj.get("image")
                .and_then(|i| i.get("url"))
                .and_then(|u| u.as_str())
        });

    if let Some(url) = cover_url {
        if !url.is_empty() {
            let tuples = vec![(
                url.to_string(),
                Some("image".into()),
                Some("cover".into()),
                Some(name.clone()),
            )];
            let meta = serde_json::json!({"source":"igdb","hint":"cover"});
            let _ = ensure_vg_source_media_links_with_meta(
                db,
                video_game_source_id,
                Some(vg_id),
                &tuples,
                "igdb",
                Some(meta),
            )
            .await?;
            let pdata = serde_json::json!({"kind":"cover"});
            let _ = upsert_game_media(db, vg_id, "igdb", url, "cover", url, pdata).await;
        }
    }

    // Screenshots array: expect [{ url, name? }]
    if let Some(Value::Array(shots)) = obj.get("screenshots") {
        let mut tuples: Vec<(String, Option<String>, Option<String>, Option<String>)> = Vec::new();
        for (idx, s) in shots.iter().enumerate() {
            if let Some(u) = s.get("url").and_then(|v| v.as_str()) {
                if !u.is_empty() {
                    let title = s
                        .get("name")
                        .and_then(|v| v.as_str())
                        .map(|t| t.to_string())
                        .or_else(|| Some(format!("{} - Screenshot {}", name, idx + 1)));
                    tuples.push((
                        u.to_string(),
                        Some("image".into()),
                        Some("screenshot".into()),
                        title,
                    ));
                    // game_media upsert
                    let pdata = serde_json::json!({"kind":"screenshot","index": idx});
                    let _ = upsert_game_media(db, vg_id, "igdb", u, "screenshot", u, pdata).await;
                }
            }
        }
        if !tuples.is_empty() {
            let meta =
                serde_json::json!({"source":"igdb","hint":"screenshots","count": tuples.len()});
            let _ = ensure_vg_source_media_links_with_meta(
                db,
                video_game_source_id,
                Some(vg_id),
                &tuples,
                "igdb",
                Some(meta),
            )
            .await?;
        }
    }

    // Videos array: expect [{ video_id, name? }], treat as YouTube by default
    if let Some(Value::Array(vids)) = obj.get("videos") {
        let mut tuples: Vec<(String, Option<String>, Option<String>, Option<String>)> = Vec::new();
        for v in vids.iter() {
            let vid = v
                .get("video_id")
                .or_else(|| v.get("id"))
                .and_then(|x| x.as_str())
                .unwrap_or("");
            if vid.is_empty() {
                continue;
            }
            let url = format!("https://www.youtube.com/watch?v={}", vid);
            let title = v
                .get("name")
                .or_else(|| v.get("title"))
                .and_then(|x| x.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("{} - Video", name));
            let role = if title.to_lowercase().contains("trailer") {
                "trailer"
            } else {
                "video"
            };
            tuples.push((
                url.clone(),
                Some("video".into()),
                Some(role.into()),
                Some(title.clone()),
            ));
            let pdata = serde_json::json!({"kind": role, "video_id": vid});
            let _ = upsert_game_media(db, vg_id, "igdb", vid, role, &url, pdata).await;
        }
        if !tuples.is_empty() {
            let meta = serde_json::json!({"source":"igdb","hint":"videos","count": tuples.len()});
            let _ = ensure_vg_source_media_links_with_meta(
                db,
                video_game_source_id,
                Some(vg_id),
                &tuples,
                "igdb",
                Some(meta),
            )
            .await?;
        }
    }

    Ok(Some(true))
}
