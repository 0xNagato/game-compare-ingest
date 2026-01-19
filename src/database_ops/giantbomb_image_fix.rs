// Fix for GiantBomb image ingestion to store all three URL variants
// This demonstrates the changes needed in src/database_ops/giantbomb/ingest.rs

use serde_json::Value;
use anyhow::Result;
use crate::Db;
use super::upsert_game_media;

/// Extract all three URL variants from a GiantBomb image object
///
/// GiantBomb provides three resolution variants:
/// - original_url: Highest quality, largest file
/// - super_url: Medium quality
/// - small_url: Smallest, suitable for thumbnails
///
/// This function extracts all three and returns them with the primary URL
/// (best available) plus a structured JSON for provider_data.
fn extract_gb_image_urls(img: &Value) -> Option<(String, Value)> {
    let original = img.get("original_url").and_then(|u| u.as_str()).map(|s| s.to_string());
    let super_url = img.get("super_url").and_then(|u| u.as_str()).map(|s| s.to_string());
    let small = img.get("small_url").and_then(|u| u.as_str()).map(|s| s.to_string());

    // Select best available as primary URL
    let primary = if let Some(ref url) = original {
        url.clone()
    } else if let Some(ref url) = super_url {
        url.clone()
    } else if let Some(ref url) = small {
        url.clone()
    } else {
        return None; // No valid URLs found
    };

    // Build structured provider_data with all variants
    let provider_data = serde_json::json!({
        "original_url": original,
        "super_url": super_url,
        "small_url": small,
        "source": "giant_bomb",
        "image_type": "cover"
    });

    Some((primary, provider_data))
}

/// REPLACEMENT for lines 271-296 in src/database_ops/giantbomb/ingest.rs (gb_game_from_api)
///
/// OLD CODE (lines 271-296):
/// ```rust
/// if let Some(url) = img
///     .get("original_url")
///     .and_then(|u| u.as_str())
///     .or_else(|| img.get("super_url").and_then(|u| u.as_str()))
///     .or_else(|| img.get("small_url").and_then(|u| u.as_str()))
/// {
///     if !url.is_empty() {
///         tuples.push((url.to_string(), Some("image".into()), Some("gallery".into()), Some(format!("{} screenshot {}", name, idx + 1))));
///         let _ = upsert_game_media(
///             db, vg_id, "giant_bomb", url, "screenshot", url,
///             serde_json::json!({"size":"orig|super|small"}),
///         ).await;
///     }
/// }
/// ```
///
/// NEW CODE:
pub async fn ingest_gb_screenshots_fixed(
    db: &Db,
    vg_id: i64,
    images: &[Value],
    name: &str,
) -> Result<Vec<(String, Option<String>, Option<String>, Option<String>)>> {
    let mut tuples: Vec<(String, Option<String>, Option<String>, Option<String>)> = Vec::new();

    for (idx, img) in images.iter().enumerate() {
        if let Some((primary_url, provider_data)) = extract_gb_image_urls(img) {
            if !primary_url.is_empty() {
                // Add to tuples for legacy provider_media_links table
                tuples.push((
                    primary_url.clone(),
                    Some("image".into()),
                    Some("gallery".into()),
                    Some(format!("{} screenshot {}", name, idx + 1)),
                ));

                // Store in game_media with ALL URL variants in provider_data
                // The upsert_game_media_batch function will extract:
                // - original_url from provider_data->>'original_url'
                // - thumbnail_url from provider_data->>'small_url'
                let _ = upsert_game_media(
                    db,
                    vg_id,
                    "giant_bomb",
                    &primary_url, // external_id
                    "screenshot",  // media_type
                    &primary_url,  // url (primary)
                    provider_data, // Contains all three URL variants
                )
                .await;
            }
        }
    }

    Ok(tuples)
}

/// Alternative: Enhanced upsert_game_media with explicit URL parameters
///
/// This approach modifies the upsert_game_media signature to accept
/// the URL variants directly instead of relying on JSON extraction.
///
/// Add this to src/database_ops/ingest_providers.rs:
pub async fn upsert_game_media_with_variants(
    db: &Db,
    video_game_id: i64,
    source: &str,
    external_id: &str,
    media_type: &str,
    url: &str,
    provider_data: Value,
    // New optional parameters for URL variants
    original_url: Option<&str>,
    thumbnail_url: Option<&str>,
) -> Result<()> {
    // Check column support
    let has_original = super::table_column_exists(db, "game_media", "original_url")
        .await
        .unwrap_or(false);
    let has_thumbnail = super::table_column_exists(db, "game_media", "thumbnail_url")
        .await
        .unwrap_or(false);

    // Build dynamic query
    let mut columns = vec!["video_game_id", "source", "external_id", "media_type", "url", "provider_data"];
    let mut placeholders = vec!["$1", "$2", "$3", "$4", "$5", "$6"];
    let mut update_clauses = vec!["url = EXCLUDED.url", "provider_data = EXCLUDED.provider_data"];
    let mut next_param = 7;

    let mut param_stack: Vec<ParamValue<'_>> = vec![];

    if has_original && original_url.is_some() {
        columns.push("original_url");
        placeholders.push(&format!("${}", next_param));
        update_clauses.push("original_url = EXCLUDED.original_url");
        param_stack.push(ParamValue::Str(original_url.unwrap()));
        next_param += 1;
    }

    if has_thumbnail && thumbnail_url.is_some() {
        columns.push("thumbnail_url");
        placeholders.push(&format!("${}", next_param));
        update_clauses.push("thumbnail_url = EXCLUDED.thumbnail_url");
        param_stack.push(ParamValue::Str(thumbnail_url.unwrap()));
        next_param += 1;
    }

    let sql = format!(
        "INSERT INTO game_media ({}) VALUES ({}) \
         ON CONFLICT (video_game_id, source, external_id) \
         DO UPDATE SET {}",
        columns.join(", "),
        placeholders.join(", "),
        update_clauses.join(", ")
    );

    let mut query = sqlx::query(&sql)
        .persistent(false)
        .bind(video_game_id)
        .bind(source)
        .bind(external_id)
        .bind(media_type)
        .bind(url)
        .bind(provider_data);

    for param in param_stack {
        query = match param {
            ParamValue::Str(s) => query.bind(s),
        };
    }

    query.execute(&db.pool).await?;
    Ok(())
}

/// Helper enum for parameter binding
enum ParamValue<'a> {
    Str(&'a str),
}

/* USAGE EXAMPLE in giantbomb/ingest.rs:

Replace lines 271-296 with:

```rust
if let Some(images) = obj.get("images").and_then(|v| v.as_array()) {
    for (idx, img) in images.iter().enumerate() {
        let original = img.get("original_url").and_then(|u| u.as_str());
        let super_url = img.get("super_url").and_then(|u| u.as_str());
        let small = img.get("small_url").and_then(|u| u.as_str());

        // Select best available as primary
        let primary = original.or(super_url).or(small);

        if let Some(url) = primary {
            if !url.is_empty() {
                tuples.push((
                    url.to_string(),
                    Some("image".into()),
                    Some("gallery".into()),
                    Some(format!("{} screenshot {}", name, idx + 1)),
                ));

                // Store with all URL variants
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
}
```

This ensures that:
1. All three URL variants are stored in provider_data JSON
2. The upsert_game_media_batch function can extract them via migration 0492
3. Frontend receives all resolution options for responsive images
*/
