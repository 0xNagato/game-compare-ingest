// Laravel-compatible ensure_video_game_for_product() with comprehensive column support.
// Historically this helper keyed rows via product_id, but canonical schema now stores
// product_id on video_game_titles. The logic below therefore supports both linkage paths
// so that only video_game_titles retain the direct product reference.

use super::ingest_providers::{extract_normalized_rating_from_payload, table_column_exists};
use crate::Db;
use anyhow::{anyhow, Result};
use tokio::sync::OnceCell;
use serde_json::Value;
use sqlx::Row;

/// Optional metadata for creating/updating video_games rows (Laravel schema)
#[derive(Debug, Default, Clone)]
pub struct VideoGameProductMetadata<'a> {
    pub title: &'a str,                    // Required for Laravel
    pub provider_key: Option<&'a str>,     // Used for rating alias lookup
    pub normalized_title: Option<&'a str>, // Auto-generated if not provided
    pub slug: Option<&'a str>,
    pub release_date: Option<&'a str>, // ISO date string: "YYYY-MM-DD"
    pub developer: Option<&'a str>,
    pub genre: Option<&'a str>, // Single genre (Laravel uses singular)
    pub genres: Option<&'a [String]>, // Array of genres (if column exists)
    pub metadata: Option<&'a Value>,
    pub region_codes: Option<&'a [String]>,
    pub platform_codes: Option<&'a [String]>,
    pub external_ids: Option<&'a Value>,
    pub external_links: Option<&'a Value>,
    pub title_keywords: Option<&'a [String]>,
    pub payload_hash: Option<&'a str>,
}

/// Dynamic column detection for video_games table (Laravel schema)
#[derive(Debug, Clone, Copy)]
struct VideoGamesLaravelColumns {
    // Core Laravel columns
    has_product_id: bool,
    has_title: bool,
    has_normalized_title: bool,
    has_title_id: bool,
    has_metadata: bool,
    has_created_at: bool,
    has_updated_at: bool,
    has_last_synced_at: bool,

    // Additional Laravel columns
    has_slug: bool,
    has_genre: bool,
    has_genres: bool,
    has_release_date: bool,
    has_developer: bool,
    has_region_codes: bool,
    has_platform_codes: bool,
    has_external_ids: bool,
    has_external_links: bool,
    has_title_keywords: bool,
    has_payload_hash: bool,

    // Optional rating columns (some schemas store these on video_games)
    has_rating: bool,
    has_average_rating: bool,
}

impl VideoGamesLaravelColumns {
    async fn detect(db: &Db) -> Result<Self> {
        Ok(Self {
            has_product_id: table_column_exists(db, "video_games", "product_id").await?,
            has_title: table_column_exists(db, "video_games", "title").await?,
            has_normalized_title: table_column_exists(db, "video_games", "normalized_title")
                .await?,
            has_title_id: table_column_exists(db, "video_games", "title_id").await?,
            has_metadata: table_column_exists(db, "video_games", "metadata").await?,
            has_created_at: table_column_exists(db, "video_games", "created_at").await?,
            has_updated_at: table_column_exists(db, "video_games", "updated_at").await?,
            has_last_synced_at: table_column_exists(db, "video_games", "last_synced_at").await?,
            has_slug: table_column_exists(db, "video_games", "slug").await?,
            has_genre: table_column_exists(db, "video_games", "genre").await?,
            has_genres: table_column_exists(db, "video_games", "genres").await?,
            has_release_date: table_column_exists(db, "video_games", "release_date").await?,
            has_developer: table_column_exists(db, "video_games", "developer").await?,
            has_region_codes: table_column_exists(db, "video_games", "region_codes").await?,
            has_platform_codes: table_column_exists(db, "video_games", "platform_codes").await?,
            has_external_ids: table_column_exists(db, "video_games", "external_ids").await?,
            has_external_links: table_column_exists(db, "video_games", "external_links").await?,
            has_title_keywords: table_column_exists(db, "video_games", "title_keywords").await?,
            has_payload_hash: table_column_exists(db, "video_games", "payload_hash").await?,

            has_rating: table_column_exists(db, "video_games", "rating").await?,
            has_average_rating: table_column_exists(db, "video_games", "average_rating").await?,
        })
    }
}

#[derive(Debug, Clone, Default)]
struct VideoGameTitleBridgeColumns {
    table_present: bool,
    title_column: Option<String>,
    has_product_id: bool,
    has_normalized_title: bool,
    has_created_at: bool,
    has_updated_at: bool,
    has_video_game_ids: bool,
}

impl VideoGameTitleBridgeColumns {
    fn supports_linkage(&self) -> bool {
        self.table_present
            && self.has_product_id
            && self.has_normalized_title
            && self.title_column.is_some()
    }

    fn title_column(&self) -> Option<&str> {
        self.title_column.as_deref()
    }
}

static VIDEO_GAME_TITLE_BRIDGE: OnceCell<VideoGameTitleBridgeColumns> = OnceCell::const_new();

async fn table_visible(db: &Db, name: &str) -> Result<bool> {
    let visible: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .persistent(false)
        .bind(name)
        .fetch_one(&db.pool)
        .await?;
    Ok(visible)
}

async fn video_game_title_bridge_columns(
    db: &Db,
) -> Result<&'static VideoGameTitleBridgeColumns> {
    VIDEO_GAME_TITLE_BRIDGE
        .get_or_try_init(|| async {
            let table_present = table_visible(db, "video_game_titles").await?;
            if !table_present {
                return Ok(VideoGameTitleBridgeColumns::default());
            }

            let has_product_id =
                table_column_exists(db, "video_game_titles", "product_id").await?;
            let has_normalized_title =
                table_column_exists(db, "video_game_titles", "normalized_title").await?;
            let has_created_at =
                table_column_exists(db, "video_game_titles", "created_at").await?;
            let has_updated_at =
                table_column_exists(db, "video_game_titles", "updated_at").await?;
            let has_video_game_ids =
                table_column_exists(db, "video_game_titles", "video_game_ids").await?;

            let title_column = if table_column_exists(db, "video_game_titles", "title")
                .await?
            {
                Some("title".to_string())
            } else if table_column_exists(db, "video_game_titles", "raw_title").await? {
                Some("raw_title".to_string())
            } else {
                None
            };

            Ok(VideoGameTitleBridgeColumns {
                table_present,
                title_column,
                has_product_id,
                has_normalized_title,
                has_created_at,
                has_updated_at,
                has_video_game_ids,
            })
        })
        .await
}

async fn ensure_title_record_for_product(
    db: &Db,
    product_id: i64,
    title: &str,
    normalized_title: &str,
) -> Result<Option<i64>> {
    let cols = video_game_title_bridge_columns(db).await?;
    if !cols.supports_linkage() {
        return Ok(None);
    }

    let title_column = cols
        .title_column()
        .ok_or_else(|| anyhow!("video_game_titles missing title/raw_title column"))?;

    let select_sql = format!(
        "SELECT id, {col} AS bridge_title, normalized_title FROM video_game_titles WHERE product_id=$1 ORDER BY id DESC LIMIT 1",
        col = title_column
    );

    if let Some(row) = sqlx::query(&select_sql)
        .persistent(false)
        .bind(product_id)
        .fetch_optional(&db.pool)
        .await?
    {
        let id: i64 = row.get("id");
        let existing_title: Option<String> = row.try_get("bridge_title").ok();
        let existing_norm: Option<String> = row.try_get("normalized_title").ok();

        let needs_title = existing_title
            .as_deref()
            .map(|s| s.trim().is_empty())
            .unwrap_or(true);
        let needs_norm = existing_norm
            .as_deref()
            .map(|s| s.trim().is_empty())
            .unwrap_or(true);

        if needs_title || needs_norm {
            let mut update_sql = format!(
                "UPDATE video_game_titles SET {col} = CASE WHEN {col} IS NULL OR {col} = '' THEN $1 ELSE {col} END, normalized_title = CASE WHEN normalized_title IS NULL OR normalized_title = '' THEN $2 ELSE normalized_title END",
                col = title_column
            );
            if cols.has_updated_at {
                update_sql.push_str(", updated_at = now()");
            }
            update_sql.push_str(" WHERE id=$3");

            sqlx::query(&update_sql)
                .persistent(false)
                .bind(title)
                .bind(normalized_title)
                .bind(id)
                .execute(&db.pool)
                .await?;
        }

        return Ok(Some(id));
    }

    let mut columns =
        vec!["product_id".to_string(), title_column.to_string(), "normalized_title".to_string()];
    let mut values = vec!["$1".to_string(), "$2".to_string(), "$3".to_string()];

    if cols.has_created_at {
        columns.push("created_at".to_string());
        values.push("now()".to_string());
    }
    if cols.has_updated_at {
        columns.push("updated_at".to_string());
        values.push("now()".to_string());
    }

    let insert_sql = format!(
        "INSERT INTO video_game_titles ({cols}) VALUES ({vals}) RETURNING id",
        cols = columns.join(", "),
        vals = values.join(", ")
    );

    let rec = sqlx::query(&insert_sql)
        .persistent(false)
        .bind(product_id)
        .bind(title)
        .bind(normalized_title)
        .fetch_one(&db.pool)
        .await?;

    Ok(Some(rec.get("id")))
}

/// Enhanced ensure_video_game_for_product with comprehensive column support.
///
/// **IMPORTANT:** This function is for Laravel schema (product_id-based).
/// For Unified schema (title_id-based), use ensure_video_game_enhanced instead.
///
/// # Arguments
/// * `db` - Database connection
/// * `product_id` - Foreign key to products table (Laravel schema)
/// * `meta` - Metadata struct with title (required) and optional fields
///
/// # Returns
/// The video_games.id (either existing or newly created)
///
/// # Example
/// ```
/// let meta = VideoGameProductMetadata {
///     title: "HELLDIVERS 2",
///     normalized_title: Some("helldivers-2"),
///     slug: Some("helldivers-2"),
///     release_date: Some("2024-02-08"),
///     developer: Some("Arrowhead Game Studios"),
///     genre: Some("Shooter"),
///     genres: Some(&vec!["Shooter".to_string(), "Co-op".to_string()]),
///     metadata: Some(&serde_json::json!({"platform_codes": ["PS5"]})),
///     region_codes: Some(&vec!["US".to_string(), "EU".to_string()]),
///     ..Default::default()
/// };
/// let vg_id = ensure_video_game_for_product_enhanced(db, product_id, &meta).await?;
/// ```
pub async fn ensure_video_game_for_product_enhanced(
    db: &Db,
    product_id: i64,
    meta: &VideoGameProductMetadata<'_>,
) -> Result<i64> {
    let cols = VideoGamesLaravelColumns::detect(db).await?;

    if !cols.has_title {
        anyhow::bail!("video_games table missing required title column");
    }

    if !cols.has_product_id && !cols.has_title_id {
        anyhow::bail!(
            "video_games table must expose either product_id or title_id for linkage; per schema the product_id foreign key now lives exclusively on video_game_titles"
        );
    }

    // Prepare the canonical title + normalized_title up front so we can use them for
    // both the existence check and any safe backfill.
    let title_trimmed = meta.title.trim();
    let title_final = if title_trimmed.is_empty() {
        "Untitled"
    } else {
        title_trimmed
    };

    // Auto-generate normalized_title if not provided
    let normalized_title = meta
        .normalized_title
        .map(|s| s.to_string())
        .unwrap_or_else(|| local_normalize_title(title_final));

    let linked_title_id = if cols.has_title_id {
        Some(
            ensure_title_record_for_product(db, product_id, title_final, &normalized_title)
                .await?
                .ok_or_else(|| {
                    anyhow!(
                        "video_games.title_id present but video_game_titles missing required columns"
                    )
                })?,
        )
    } else {
        None
    };

    // =============================
    // Step 1: Check if row exists
    // =============================
    let mut existing_id: Option<i64> = None;

    // Legacy Laravel schemas stored product_id on video_games. We still honor that path
    // first, but canonical deployments rely on title_id (via video_game_titles) which is
    // checked immediately afterward if no product-scoped row is found.

    if cols.has_product_id {
        existing_id = sqlx::query_scalar(
            "SELECT id FROM video_games WHERE product_id=$1 ORDER BY id DESC LIMIT 1",
        )
        .persistent(false)
        .bind(product_id)
        .fetch_optional(&db.pool)
        .await?;
    }

    if existing_id.is_none() && cols.has_title_id {
        if let Some(title_id) = linked_title_id {
            existing_id = sqlx::query_scalar(
                "SELECT id FROM video_games WHERE title_id=$1 ORDER BY id DESC LIMIT 1",
            )
            .persistent(false)
            .bind(title_id)
            .fetch_optional(&db.pool)
            .await?;
        }
    }

    if let Some(id) = existing_id {

        // Derive a normalized 0–5 rating from the provider payload (if we have metadata).
        let derived_rating_five = meta
            .metadata
            .and_then(|payload| extract_normalized_rating_from_payload(meta.provider_key, payload));

        // Safe backfill: only fill NULL/empty fields, never overwrite user edits.
        let mut sets: Vec<String> = Vec::new();
        let mut params: Vec<ParamValue<'_>> = Vec::new();
        let mut next_param = 2;

        if cols.has_normalized_title {
            sets.push(format!(
                "normalized_title = CASE WHEN normalized_title IS NULL OR normalized_title = '' THEN ${} ELSE normalized_title END",
                next_param
            ));
            params.push(ParamValue::Str(normalized_title.as_str()));
            next_param += 1;
        }

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

        if cols.has_genre {
            if let Some(v) = meta.genre {
                sets.push(format!(
                    "genre = CASE WHEN genre IS NULL OR genre = '' THEN ${} ELSE genre END",
                    next_param
                ));
                params.push(ParamValue::Str(v));
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

        if cols.has_platform_codes {
            if let Some(v) = meta.platform_codes {
                sets.push(format!(
                    "platform_codes = CASE WHEN platform_codes IS NULL OR cardinality(platform_codes) = 0 THEN ${} ELSE platform_codes END",
                    next_param
                ));
                params.push(ParamValue::StrArray(v));
                next_param += 1;
            }
        }

        if cols.has_external_ids {
            if let Some(v) = meta.external_ids {
                sets.push(format!(
                    "external_ids = COALESCE(external_ids, ${})",
                    next_param
                ));
                params.push(ParamValue::Json(v));
                next_param += 1;
            }
        }

        if cols.has_external_links {
            if let Some(v) = meta.external_links {
                sets.push(format!(
                    "external_links = COALESCE(external_links, ${})",
                    next_param
                ));
                params.push(ParamValue::Json(v));
                next_param += 1;
            }
        }

        if cols.has_title_keywords {
            if let Some(v) = meta.title_keywords {
                sets.push(format!(
                    "title_keywords = CASE WHEN title_keywords IS NULL OR cardinality(title_keywords) = 0 THEN ${} ELSE title_keywords END",
                    next_param
                ));
                params.push(ParamValue::StrArray(v));
                next_param += 1;
            }
        }

        if cols.has_payload_hash {
            if let Some(v) = meta.payload_hash {
                sets.push(format!(
                    "payload_hash = CASE WHEN payload_hash IS NULL OR payload_hash = '' THEN ${} ELSE payload_hash END",
                    next_param
                ));
                params.push(ParamValue::Str(v));
                next_param += 1;
            }
        }

        if cols.has_title_id {
            if let Some(title_id) = linked_title_id {
                sets.push(format!(
                    "title_id = COALESCE(title_id, ${})",
                    next_param
                ));
                params.push(ParamValue::I64(title_id));
                next_param += 1;
            }
        }

        // Prefer average_rating when available; fall back to rating.
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

        // Explicitly read the final parameter index to avoid unused assignment warnings in code paths
        // where no further bindings are appended.
        let _ = next_param;

        // Sync timestamps are safe to update: they reflect provider ingestion time.
        if cols.has_updated_at {
            sets.push("updated_at = now()".to_string());
        }
        if cols.has_last_synced_at {
            sets.push("last_synced_at = now()".to_string());
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
                    ParamValue::I64(v) => q.bind(v),
                };
            }
            let _ = q.execute(&db.pool).await?;
        }

        return Ok(id);
    }

    // =============================
    // Step 2: Build dynamic INSERT
    // =============================

    let mut columns: Vec<String> = Vec::new();
    let mut placeholders: Vec<String> = Vec::new();
    let mut param_stack: Vec<ParamValue<'_>> = Vec::new();
    let mut next_param = 1;

    // Helper macro to avoid closure borrowing issues
    macro_rules! push_param {
        ($column:expr, $value:expr) => {
            columns.push($column.to_string());
            placeholders.push(format!("${}", next_param));
            param_stack.push($value);
            next_param += 1;
        };
    }

    macro_rules! push_literal {
        ($column:expr, $literal:expr) => {
            columns.push($column.to_string());
            placeholders.push($literal.to_string());
        };
    }

    // Derive a normalized 0–5 rating from payload metadata (if present).
    let derived_rating_five = meta
        .metadata
        .and_then(|payload| extract_normalized_rating_from_payload(meta.provider_key, payload));

    if cols.has_product_id {
        push_param!("product_id", ParamValue::I64(product_id));
    }

    push_param!("title", ParamValue::Str(title_final));

    // normalized_title (always add if column exists)
    if cols.has_normalized_title {
        push_param!("normalized_title", ParamValue::Str(&normalized_title));
    }

    // Timestamps
    if cols.has_created_at {
        push_literal!("created_at", "now()");
    }
    if cols.has_updated_at {
        push_literal!("updated_at", "now()");
    }
    if cols.has_last_synced_at {
        push_literal!("last_synced_at", "now()");
    }

    // Optional fields from metadata
    if cols.has_slug && meta.slug.is_some() {
        push_param!("slug", ParamValue::Str(meta.slug.unwrap()));
    }

    if cols.has_release_date && meta.release_date.is_some() {
        push_param!("release_date", ParamValue::Str(meta.release_date.unwrap()));
    }

    if cols.has_developer && meta.developer.is_some() {
        push_param!("developer", ParamValue::Str(meta.developer.unwrap()));
    }

    if cols.has_genre && meta.genre.is_some() {
        push_param!("genre", ParamValue::Str(meta.genre.unwrap()));
    }

    if cols.has_genres && meta.genres.is_some() {
        push_param!("genres", ParamValue::StrArray(meta.genres.unwrap()));
    }

    if cols.has_metadata && meta.metadata.is_some() {
        push_param!("metadata", ParamValue::Json(meta.metadata.unwrap()));
    }

    if cols.has_region_codes && meta.region_codes.is_some() {
        push_param!("region_codes", ParamValue::StrArray(meta.region_codes.unwrap()));
    }

    if cols.has_platform_codes && meta.platform_codes.is_some() {
        push_param!("platform_codes", ParamValue::StrArray(meta.platform_codes.unwrap()));
    }

    if cols.has_external_ids && meta.external_ids.is_some() {
        push_param!("external_ids", ParamValue::Json(meta.external_ids.unwrap()));
    }

    if cols.has_external_links && meta.external_links.is_some() {
        push_param!("external_links", ParamValue::Json(meta.external_links.unwrap()));
    }

    if cols.has_title_keywords && meta.title_keywords.is_some() {
        push_param!("title_keywords", ParamValue::StrArray(meta.title_keywords.unwrap()));
    }

    if cols.has_payload_hash && meta.payload_hash.is_some() {
        push_param!("payload_hash", ParamValue::Str(meta.payload_hash.unwrap()));
    }

    if cols.has_title_id {
        if let Some(title_id) = linked_title_id {
            push_param!("title_id", ParamValue::I64(title_id));
        }
    }

    // Prefer average_rating when available; fall back to rating.
    if cols.has_average_rating {
        if let Some(v) = derived_rating_five {
            push_param!("average_rating", ParamValue::F64(v));
        }
    } else if cols.has_rating {
        if let Some(v) = derived_rating_five {
            push_param!("rating", ParamValue::F64(v));
        }
    }

    let _ = next_param;

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

    for param in param_stack {
        query = match param {
            ParamValue::Str(s) => query.bind(s),
            ParamValue::Json(j) => query.bind(j),
            ParamValue::StrArray(a) => query.bind(a),
            ParamValue::F64(f) => query.bind(f),
            ParamValue::I64(v) => query.bind(v),
        };
    }

    let rec = query.fetch_one(&db.pool).await?;
    let id: i64 = rec.get("id");

    // If we have a linked title, ensure this game is in its video_game_ids array
    if let Some(title_id) = linked_title_id {
        let title_cols = video_game_title_bridge_columns(db).await?;
        if title_cols.has_video_game_ids {
            // Append ID to array if not present.
            // We use a safe update that handles NULLs and duplicates.
            let update_sql = "UPDATE video_game_titles 
                              SET video_game_ids = (
                                  SELECT jsonb_agg(DISTINCT x) 
                                  FROM jsonb_array_elements(COALESCE(video_game_ids, '[]'::jsonb) || jsonb_build_array($1)) t(x)
                              )
                              WHERE id = $2";

            sqlx::query(update_sql)
                .bind(id)
                .bind(title_id)
                .execute(&db.pool)
                .await?;
        }
    }

    Ok(id)
}

/// Helper enum for dynamic parameter binding
#[derive(Debug)]
enum ParamValue<'a> {
    Str(&'a str),
    Json(&'a Value),
    StrArray(&'a [String]),
    F64(f64),
    I64(i64),
}

/// Simple title normalization (lowercase, alphanumeric only)
fn local_normalize_title(title: &str) -> String {
    title
        .chars()
        .filter(|c| c.is_alphanumeric() || c.is_whitespace())
        .collect::<String>()
        .to_lowercase()
        .split_whitespace()
        .collect::<Vec<&str>>()
        .join("-")
}
