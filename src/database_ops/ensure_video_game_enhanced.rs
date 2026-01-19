// Enhanced ensure_video_game() with comprehensive column support
// This file contains the improved implementation that should replace the current ensure_video_game() in ingest_providers.rs

use super::ingest_providers::{extract_normalized_rating_from_payload, table_column_exists};
use crate::Db;
use anyhow::Result;
use serde_json::Value;
use sqlx::Row;

/// Optional metadata for creating/updating video_games rows
#[derive(Debug, Default, Clone)]
pub struct VideoGameMetadata<'a> {
    pub provider_key: Option<&'a str>,
    pub slug: Option<&'a str>,
    pub release_date: Option<&'a str>, // ISO date string: "YYYY-MM-DD"
    pub display_title: Option<&'a str>,
    pub developer: Option<&'a str>,
    pub synopsis: Option<&'a str>,
    pub metadata: Option<&'a Value>,
    pub region_codes: Option<&'a [String]>,
    pub genres: Option<&'a [String]>,
    pub rating: Option<f64>,
    pub popularity_score: Option<f64>,
}

/// Dynamic column detection for video_games table
#[derive(Debug, Clone, Copy)]
struct VideoGamesColumns {
    has_edition: bool,
    has_created_at: bool,
    has_updated_at: bool,
    has_slug: bool,
    has_release_date: bool,
    has_display_title: bool,
    has_developer: bool,
    has_synopsis: bool,
    has_metadata: bool,
    has_region_codes: bool,
    has_genres: bool,
    has_rating: bool,
    has_average_rating: bool,
    has_popularity_score: bool,
}

impl VideoGamesColumns {
    async fn detect(db: &Db) -> Result<Self> {
        Ok(Self {
            has_edition: table_column_exists(db, "video_games", "edition").await?,
            has_created_at: table_column_exists(db, "video_games", "created_at").await?,
            has_updated_at: table_column_exists(db, "video_games", "updated_at").await?,
            has_slug: table_column_exists(db, "video_games", "slug").await?,
            has_release_date: table_column_exists(db, "video_games", "release_date").await?,
            has_display_title: table_column_exists(db, "video_games", "display_title").await?,
            has_developer: table_column_exists(db, "video_games", "developer").await?,
            has_synopsis: table_column_exists(db, "video_games", "synopsis").await?,
            has_metadata: table_column_exists(db, "video_games", "metadata").await?,
            has_region_codes: table_column_exists(db, "video_games", "region_codes").await?,
            has_genres: table_column_exists(db, "video_games", "genres").await?,
            has_rating: table_column_exists(db, "video_games", "rating").await?,
            has_average_rating: table_column_exists(db, "video_games", "average_rating").await?,
            has_popularity_score: table_column_exists(db, "video_games", "popularity_score")
                .await?,
        })
    }
}

/// Enhanced ensure_video_game with comprehensive column support and dynamic detection.
///
/// This function:
/// 1. Checks if a video_games row exists for the given title_id + platform_id (+ edition)
/// 2. If exists, returns the existing id
/// 3. If not, creates a new row with ALL available columns populated from metadata
/// 4. Uses dynamic column detection to support both unified and Laravel schemas
///
/// # Arguments
/// * `db` - Database connection
/// * `title_id` - Foreign key to video_game_titles
/// * `platform_id` - Foreign key to platforms
/// * `edition` - Optional edition string (e.g., "Standard", "Deluxe", "GOTY")
/// * `meta` - Optional metadata struct with additional fields
///
/// # Returns
/// The video_games.id (either existing or newly created)
///
/// # Example
/// ```
/// let meta = VideoGameMetadata {
///     slug: Some("helldivers-2"),
///     release_date: Some("2024-02-08"),
///     display_title: Some("HELLDIVERS 2"),
///     developer: Some("Arrowhead Game Studios"),
///     synopsis: Some("A cooperative third-person shooter..."),
///     metadata: Some(&serde_json::json!({"platform_codes": ["PS5"]})),
///     region_codes: Some(&vec!["US".to_string(), "EU".to_string()]),
///     genres: Some(&vec!["Shooter".to_string(), "Co-op".to_string()]),
///     ..Default::default()
/// };
/// let vg_id = ensure_video_game_enhanced(db, title_id, platform_id, Some("Standard"), Some(&meta)).await?;
/// ```
pub async fn ensure_video_game_enhanced(
    db: &Db,
    title_id: i64,
    platform_id: i64,
    edition: Option<&str>,
    meta: Option<&VideoGameMetadata<'_>>,
) -> Result<i64> {
    let cols = VideoGamesColumns::detect(db).await?;

    // =============================
    // Step 1: Check if row exists
    // =============================
    let rec = if cols.has_edition {
        match edition {
            Some(ed) => {
                sqlx::query(
                    "SELECT id FROM video_games WHERE title_id=$1 AND platform_id=$2 AND edition=$3",
                )
                .persistent(false)
                .bind(title_id)
                .bind(platform_id)
                .bind(ed)
                .fetch_optional(&db.pool)
                .await?
            }
            None => sqlx::query(
                "SELECT id FROM video_games WHERE title_id=$1 AND platform_id=$2 AND edition IS NULL",
            )
            .persistent(false)
            .bind(title_id)
            .bind(platform_id)
            .fetch_optional(&db.pool)
            .await?,
        }
    } else {
        // Catalog-style table without `edition` column
        sqlx::query("SELECT id FROM video_games WHERE title_id=$1 AND platform_id=$2")
            .persistent(false)
            .bind(title_id)
            .bind(platform_id)
            .fetch_optional(&db.pool)
            .await?
    };

    // If the row exists, opportunistically backfill missing columns from metadata.
    // We only write into NULL/empty/default fields to avoid overwriting user edits.
    if let Some(r) = rec {
        let id: i64 = r.get("id");

        let default_meta = VideoGameMetadata::default();
        let meta = meta.unwrap_or(&default_meta);

        // Derive a normalized 0–5 rating from payload if the caller didn't supply one.
        let derived_rating_five = meta.rating.or_else(|| {
            meta.metadata.and_then(|payload| {
                extract_normalized_rating_from_payload(meta.provider_key, payload)
            })
        });

        // Build a dynamic UPDATE that only sets fields when:
        //  - the column exists in this schema AND
        //  - we have a value in `meta` AND
        //  - the current DB value is NULL/empty/default.
        let mut sets: Vec<String> = Vec::new();
        let mut params: Vec<ParamValue<'_>> = Vec::new();
        let mut next_param = 2;

        if cols.has_slug {
            if let Some(v) = meta.slug {
                sets.push(format!(
                    "slug = CASE WHEN slug IS NULL OR slug = '' THEN ${} ELSE slug END",
                    next_param
                ));
                params.push(ParamValue::Str(v));
                next_param += 1;
            }
        }

        if cols.has_release_date {
            if let Some(v) = meta.release_date {
                sets.push(format!(
                    "release_date = COALESCE(release_date, ${})",
                    next_param
                ));
                params.push(ParamValue::Str(v));
                next_param += 1;
            }
        }

        if cols.has_display_title {
            if let Some(v) = meta.display_title {
                sets.push(format!(
                    "display_title = CASE WHEN display_title IS NULL OR display_title = '' THEN ${} ELSE display_title END",
                    next_param
                ));
                params.push(ParamValue::Str(v));
                next_param += 1;
            }
        }

        if cols.has_developer {
            if let Some(v) = meta.developer {
                sets.push(format!(
                    "developer = CASE WHEN developer IS NULL OR developer = '' THEN ${} ELSE developer END",
                    next_param
                ));
                params.push(ParamValue::Str(v));
                next_param += 1;
            }
        }

        if cols.has_synopsis {
            if let Some(v) = meta.synopsis {
                sets.push(format!(
                    "synopsis = CASE WHEN synopsis IS NULL OR synopsis = '' THEN ${} ELSE synopsis END",
                    next_param
                ));
                params.push(ParamValue::Str(v));
                next_param += 1;
            }
        }

        if cols.has_metadata {
            if let Some(v) = meta.metadata {
                sets.push(format!("metadata = COALESCE(metadata, ${})", next_param));
                params.push(ParamValue::Json(v));
                next_param += 1;
            }
        }

        if cols.has_region_codes {
            if let Some(v) = meta.region_codes {
                sets.push(format!(
                    "region_codes = CASE WHEN region_codes IS NULL OR cardinality(region_codes) = 0 THEN ${} ELSE region_codes END",
                    next_param
                ));
                params.push(ParamValue::StrArray(v));
                next_param += 1;
            }
        }

        if cols.has_genres {
            if let Some(v) = meta.genres {
                sets.push(format!(
                    "genres = CASE WHEN genres IS NULL OR cardinality(genres) = 0 THEN ${} ELSE genres END",
                    next_param
                ));
                params.push(ParamValue::StrArray(v));
                next_param += 1;
            }
        }

        // Prefer average_rating when available; fall back to rating for older schemas.
        if cols.has_average_rating {
            if let Some(v) = derived_rating_five {
                sets.push(format!(
                    "average_rating = CASE WHEN average_rating IS NULL OR average_rating = 0 THEN ${} ELSE average_rating END",
                    next_param
                ));
                params.push(ParamValue::F64(v));
                next_param += 1;
            }
        } else if cols.has_rating {
            if let Some(v) = derived_rating_five {
                sets.push(format!(
                    "rating = CASE WHEN rating IS NULL OR rating = 0 THEN ${} ELSE rating END",
                    next_param
                ));
                params.push(ParamValue::F64(v));
                next_param += 1;
            }
        }

        if cols.has_popularity_score {
            if let Some(v) = meta.popularity_score {
                sets.push(format!(
                    "popularity_score = CASE WHEN popularity_score IS NULL OR popularity_score = 0 THEN ${} ELSE popularity_score END",
                    next_param
                ));
                params.push(ParamValue::F64(v));
                next_param += 1;
            }
        }

        // Keep the bind counter logically "used" even if the last optional field is set.
        // (Avoids unused_assignments warnings under some lint configurations.)
        let _ = next_param;

        if cols.has_updated_at {
            sets.push("updated_at = now()".to_string());
        }

        if !sets.is_empty() {
            let sql = format!("UPDATE video_games SET {} WHERE id=$1", sets.join(", "));
            let mut q = sqlx::query(&sql).persistent(false).bind(id);
            for p in params {
                q = match p {
                    ParamValue::Str(s) => q.bind(s),
                    ParamValue::Json(j) => q.bind(j),
                    ParamValue::StrArray(a) => q.bind(a),
                    ParamValue::F64(f) => q.bind(f),
                };
            }
            let _ = q.execute(&db.pool).await?;
        }

        return Ok(id);
    }

    // =============================
    // Step 2: Build dynamic INSERT
    // =============================
    let mut columns = vec!["title_id", "platform_id"];
    let mut placeholders: Vec<String> = vec!["$1".to_string(), "$2".to_string()];
    let mut next_param = 3;

    // Required: edition (if column exists and value provided)
    if cols.has_edition {
        columns.push("edition");
        placeholders.push(format!("${}", next_param));
        next_param += 1;
    }

    // Timestamps (always use now() when columns exist)
    if cols.has_created_at {
        columns.push("created_at");
        placeholders.push("now()".to_string());
    }
    if cols.has_updated_at {
        columns.push("updated_at");
        placeholders.push("now()".to_string());
    }

    // Build parameter bindings based on available metadata
    let default_meta = VideoGameMetadata::default();
    let meta = meta.unwrap_or(&default_meta);

    // Derive a normalized 0–5 rating from payload if the caller didn't supply one.
    let derived_rating_five = meta.rating.or_else(|| {
        meta.metadata
            .and_then(|payload| extract_normalized_rating_from_payload(meta.provider_key, payload))
    });

    // Track which optional parameters we're adding (for proper $N indexing)
    let mut param_stack: Vec<ParamValue<'_>> = vec![];

    if cols.has_slug && meta.slug.is_some() {
        columns.push("slug");
        placeholders.push(format!("${}", next_param));
        param_stack.push(ParamValue::Str(meta.slug.unwrap()));
        next_param += 1;
    }

    if cols.has_release_date && meta.release_date.is_some() {
        columns.push("release_date");
        placeholders.push(format!("${}", next_param));
        param_stack.push(ParamValue::Str(meta.release_date.unwrap()));
        next_param += 1;
    }

    if cols.has_display_title && meta.display_title.is_some() {
        columns.push("display_title");
        placeholders.push(format!("${}", next_param));
        param_stack.push(ParamValue::Str(meta.display_title.unwrap()));
        next_param += 1;
    }

    if cols.has_developer && meta.developer.is_some() {
        columns.push("developer");
        placeholders.push(format!("${}", next_param));
        param_stack.push(ParamValue::Str(meta.developer.unwrap()));
        next_param += 1;
    }

    if cols.has_synopsis && meta.synopsis.is_some() {
        columns.push("synopsis");
        placeholders.push(format!("${}", next_param));
        param_stack.push(ParamValue::Str(meta.synopsis.unwrap()));
        next_param += 1;
    }

    if cols.has_metadata && meta.metadata.is_some() {
        columns.push("metadata");
        placeholders.push(format!("${}", next_param));
        param_stack.push(ParamValue::Json(meta.metadata.unwrap()));
        next_param += 1;
    }

    if cols.has_region_codes && meta.region_codes.is_some() {
        columns.push("region_codes");
        placeholders.push(format!("${}", next_param));
        param_stack.push(ParamValue::StrArray(meta.region_codes.unwrap()));
        next_param += 1;
    }

    if cols.has_genres && meta.genres.is_some() {
        columns.push("genres");
        placeholders.push(format!("${}", next_param));
        param_stack.push(ParamValue::StrArray(meta.genres.unwrap()));
        next_param += 1;
    }

    if cols.has_average_rating {
        if let Some(v) = derived_rating_five {
            columns.push("average_rating");
            placeholders.push(format!("${}", next_param));
            param_stack.push(ParamValue::F64(v));
            next_param += 1;
        }
    } else if cols.has_rating {
        if let Some(v) = derived_rating_five {
            columns.push("rating");
            placeholders.push(format!("${}", next_param));
            param_stack.push(ParamValue::F64(v));
            next_param += 1;
        }
    }

    if cols.has_popularity_score && meta.popularity_score.is_some() {
        columns.push("popularity_score");
        placeholders.push(format!("${}", next_param));
        param_stack.push(ParamValue::F64(meta.popularity_score.unwrap()));
        // next_param += 1; // No more params after this
    }

    // Build final query string
    let sql = format!(
        "INSERT INTO video_games ({}) VALUES ({}) RETURNING id",
        columns.join(", "),
        placeholders.join(", ")
    );

    // =============================
    // Step 3: Execute with bindings
    // =============================
    let mut query = sqlx::query(&sql).persistent(false);

    // Bind required parameters
    query = query.bind(title_id).bind(platform_id);

    // Bind edition if included
    if cols.has_edition {
        query = query.bind(edition);
    }

    // Bind optional parameters in order
    for param in param_stack {
        query = match param {
            ParamValue::Str(s) => query.bind(s),
            ParamValue::Json(j) => query.bind(j),
            ParamValue::StrArray(a) => query.bind(a),
            ParamValue::F64(f) => query.bind(f),
        };
    }

    let rec = query.fetch_one(&db.pool).await?;
    Ok(rec.get("id"))
}

/// Helper enum for dynamic parameter binding
#[derive(Debug)]
enum ParamValue<'a> {
    Str(&'a str),
    Json(&'a Value),
    StrArray(&'a [String]),
    F64(f64),
}
