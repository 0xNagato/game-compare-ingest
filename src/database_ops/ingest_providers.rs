use crate::database_ops::db::{CurrentPriceRow, Db, PriceRow};
use crate::database_ops::ensure_video_game_for_product_enhanced::{
    ensure_video_game_for_product_enhanced, VideoGameProductMetadata,
};
use crate::database_ops::exchange::ExchangeService;
use crate::normalization::platform::{PlatformKey, MIN_PLATFORM_SIMILARITY};
use crate::normalization::rating::{RatingAlias, RatingMapper, RatingStrategy};
use anyhow::{anyhow, bail, Result};
use serde_json::{json, Value};
use sha1::{Digest, Sha1};
use sqlx::Row;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock};
use tokio::sync::OnceCell;
use tracing::{debug, info, instrument, warn};
use uuid::Uuid;

const MAX_PROVIDER_KEY_LEN: usize = 64;
const MAX_SLUG_LEN: usize = 255;
const SLUG_CHECKSUM_HEX_LEN: usize = 8;

fn clamp_to_chars(input: &str, max_chars: usize) -> String {
    if input.chars().count() <= max_chars {
        return input.to_string();
    }
    let mut out = String::with_capacity(max_chars.min(input.len()));
    let mut count = 0;
    for ch in input.chars() {
        if count == max_chars {
            break;
        }
        out.push(ch);
        count += 1;
    }
    out
}

fn normalize_provider_key(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return "video-game-source".to_string();
    }
    let char_len = trimmed.chars().count();
    if char_len <= MAX_PROVIDER_KEY_LEN {
        return trimmed.to_string();
    }
    warn!(
        original_len = char_len,
        max_len = MAX_PROVIDER_KEY_LEN,
        "provider_key exceeded column length; truncating"
    );
    clamp_to_chars(trimmed, MAX_PROVIDER_KEY_LEN)
}

fn slug_checksum_fragment(primary: &str, fallback: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(primary.trim().as_bytes());
    hasher.update(b"|");
    hasher.update(fallback.trim().as_bytes());
    let digest = hasher.finalize();
    let hex = format!("{:x}", digest);
    hex.chars().take(SLUG_CHECKSUM_HEX_LEN).collect::<String>()
}

fn slugify_token(input: &str) -> String {
    let mut slug = String::new();
    let mut last_dash = false;
    for ch in input.chars() {
        if ch.is_ascii_alphanumeric() {
            slug.push(ch.to_ascii_lowercase());
            last_dash = false;
        } else if !last_dash {
            slug.push('-');
            last_dash = true;
        }
    }
    slug.trim_matches('-').to_string()
}

fn normalize_source_slug_value(primary: &str, fallback: &str) -> String {
    let primary_attempt = slugify_token(primary);
    let fallback_attempt = slugify_token(fallback);
    let mut base = if !primary_attempt.is_empty() {
        primary_attempt
    } else if !fallback_attempt.is_empty() {
        fallback_attempt
    } else if !primary.trim().is_empty() {
        primary
            .trim()
            .to_ascii_lowercase()
            .replace(|c: char| c.is_whitespace(), "-")
    } else if !fallback.trim().is_empty() {
        fallback
            .trim()
            .to_ascii_lowercase()
            .replace(|c: char| c.is_whitespace(), "-")
    } else {
        "video-game-source".to_string()
    };

    base = base.trim_matches('-').to_string();
    if base.is_empty() {
        base = "video-game-source".to_string();
    }

    // Clamp base to leave room for checksum.
    let checksum = slug_checksum_fragment(primary, fallback);
    let checksum_len = checksum.chars().count();
    let max_base_chars = MAX_SLUG_LEN.saturating_sub(checksum_len + 1).max(1);
    if base.chars().count() > max_base_chars {
        base = clamp_to_chars(&base, max_base_chars);
        base = base.trim_matches('-').to_string();
        if base.is_empty() {
            base = checksum.clone();
        }
    }

    format!("{}-{}", base, checksum)
}

/// Consolidated schema cache to minimize information_schema queries
/// Initialized once at application startup, reused across all ingestion operations
#[derive(Debug)]
pub struct SchemaCache {
    // Table existence flags
    pub provider_toplists_exists: bool,
    pub provider_toplist_items_exists: bool,
    pub provider_items_exists: bool,
    pub provider_media_links_exists: bool,
    pub provider_offers_exists: bool,
    pub jurisdictions_exists: bool,

    // Column existence checks (hot path)
    pub video_games_has_title_id: bool,
    pub video_games_has_platform_id: bool,
    pub products_has_uid: bool,
    pub platforms_has_id: bool,
    pub providers_has_kind: bool,
    pub game_media_has_source: bool,

    // Enum type checks
    pub game_media_source_is_enum: bool,
    pub game_media_media_type_is_enum: bool,
    pub media_source_supports_psstore: bool,
    pub media_type_supports_background: bool,

    // Content columns
    pub video_games_content_cols: VideoGamesContentColumns,
    pub country_schema: CountrySchema,
}

impl SchemaCache {
    /// Initialize schema cache by querying database once
    /// Should be called at application startup before any ingestion
    pub async fn init(db: &Db) -> Result<Self> {
        info!("Initializing schema cache...");

        let cache = Self {
            // Table existence
            provider_toplists_exists: table_exists(db, "provider_toplists").await.unwrap_or(false),
            provider_toplist_items_exists: table_exists(db, "provider_toplist_items")
                .await
                .unwrap_or(false),
            provider_items_exists: table_exists(db, "provider_items").await.unwrap_or(false),
            provider_media_links_exists: table_exists(db, "canonical_media")
                .await
                .unwrap_or(false),
            provider_offers_exists: table_exists(db, "provider_offers").await.unwrap_or(false),
            jurisdictions_exists: table_exists(db, "jurisdictions").await.unwrap_or(false),

            // Column existence (hot paths - checked thousands of times)
            video_games_has_title_id: table_column_exists(db, "video_games", "title_id")
                .await
                .unwrap_or(false),
            video_games_has_platform_id: table_column_exists(db, "video_games", "platform_id")
                .await
                .unwrap_or(false),
            products_has_uid: table_column_exists(db, "products", "uid")
                .await
                .unwrap_or(false),
            platforms_has_id: table_column_exists(db, "platforms", "id")
                .await
                .unwrap_or(false),
            providers_has_kind: table_column_exists(db, "video_game_sources", "category")
                .await
                .unwrap_or(false),
            game_media_has_source: table_column_exists(db, "game_media", "source")
                .await
                .unwrap_or(false),

            // Enum types
            game_media_source_is_enum: Self::check_game_media_source_is_enum(db)
                .await
                .unwrap_or(false),
            game_media_media_type_is_enum: Self::check_game_media_media_type_is_enum(db)
                .await
                .unwrap_or(false),
            media_source_supports_psstore: Self::check_media_source_supports_psstore(db)
                .await
                .unwrap_or(false),
            media_type_supports_background: Self::check_media_type_supports_background(db)
                .await
                .unwrap_or(false),

            // Content columns
            video_games_content_cols: Self::detect_video_games_content_columns(db).await?,
            country_schema: Self::detect_country_schema(db).await?,
        };

        info!("Schema cache initialized successfully");
        Ok(cache)
    }

    async fn check_game_media_source_is_enum(db: &Db) -> Result<bool> {
        let udt = table_column_udt_name(db, "game_media", "source").await?;
        Ok(udt.as_deref() == Some("media_source"))
    }

    async fn check_game_media_media_type_is_enum(db: &Db) -> Result<bool> {
        let udt = table_column_udt_name(db, "game_media", "media_type").await?;
        Ok(udt.as_deref() == Some("media_type"))
    }

    async fn check_media_source_supports_psstore(db: &Db) -> Result<bool> {
        let exists: Option<bool> = sqlx::query_scalar(
            "SELECT TRUE FROM pg_type t JOIN pg_enum e ON e.enumtypid = t.oid
             WHERE t.typname = 'media_source' AND e.enumlabel = 'psstore' LIMIT 1",
        )
        .persistent(false)
        .fetch_optional(&db.pool)
        .await?;
        Ok(exists.unwrap_or(false))
    }

    async fn check_media_type_supports_background(db: &Db) -> Result<bool> {
        let exists: Option<bool> = sqlx::query_scalar(
            "SELECT TRUE FROM pg_type t JOIN pg_enum e ON e.enumtypid = t.oid
             WHERE t.typname = 'media_type' AND e.enumlabel = 'background' LIMIT 1",
        )
        .persistent(false)
        .fetch_optional(&db.pool)
        .await?;
        Ok(exists.unwrap_or(false))
    }

    async fn detect_video_games_content_columns(db: &Db) -> Result<VideoGamesContentColumns> {
        if !table_exists(db, "video_games").await.unwrap_or(false) {
            return Ok(VideoGamesContentColumns::default());
        }

        let has_metadata = table_column_exists(db, "video_games", "metadata")
            .await
            .unwrap_or(false);
        let metadata_is_jsonb = if has_metadata {
            let udt = table_column_udt_name(db, "video_games", "metadata").await?;
            udt.as_deref() == Some("jsonb")
        } else {
            false
        };

        Ok(VideoGamesContentColumns {
            has_synopsis: table_column_exists(db, "video_games", "synopsis")
                .await
                .unwrap_or(false),
            has_display_title: table_column_exists(db, "video_games", "display_title")
                .await
                .unwrap_or(false),
            has_region_codes: table_column_exists(db, "video_games", "region_codes")
                .await
                .unwrap_or(false),
            has_genres: table_column_exists(db, "video_games", "genres")
                .await
                .unwrap_or(false),
            has_release_date: table_column_exists(db, "video_games", "release_date")
                .await
                .unwrap_or(false),
            has_developer: table_column_exists(db, "video_games", "developer")
                .await
                .unwrap_or(false),
            has_metadata,
            metadata_is_jsonb,
            has_average_rating: table_column_exists(db, "video_games", "average_rating")
                .await
                .unwrap_or(false),
            has_rating_count: table_column_exists(db, "video_games", "rating_count")
                .await
                .unwrap_or(false),
            has_rating_updated_at: table_column_exists(db, "video_games", "rating_updated_at")
                .await
                .unwrap_or(false),
            has_sellable_id: table_column_exists(db, "video_games", "sellable_id")
                .await
                .unwrap_or(false),
            has_title_id: table_column_exists(db, "video_games", "title_id")
                .await
                .unwrap_or(false),
        })
    }

    async fn detect_country_schema(db: &Db) -> Result<CountrySchema> {
        let cols: Vec<String> = sqlx::query_scalar(
            "SELECT column_name FROM information_schema.columns \
             WHERE table_schema = ANY (current_schemas(true)) AND table_name = 'countries'",
        )
        .persistent(false)
        .fetch_all(&db.pool)
        .await?;

        let has_iso2 = cols.iter().any(|c| c.eq_ignore_ascii_case("iso2"));
        let has_iso3 = cols.iter().any(|c| c.eq_ignore_ascii_case("iso3"));
        let has_code2 = cols.iter().any(|c| c.eq_ignore_ascii_case("code2"));
        let has_country_code = cols.iter().any(|c| c.eq_ignore_ascii_case("country_code"));
        let has_code = cols.iter().any(|c| c.eq_ignore_ascii_case("code"));
        let has_currency_id = cols.iter().any(|c| c.eq_ignore_ascii_case("currency_id"));
        let has_name = cols.iter().any(|c| c.eq_ignore_ascii_case("name"));

        let code_col = if has_iso2 {
            Some("iso2".to_string())
        } else if has_country_code {
            Some("country_code".to_string())
        } else if has_code2 {
            Some("code2".to_string())
        } else if has_code {
            Some("code".to_string())
        } else {
            None
        };
        let code_expr = code_col.as_ref().map(|c| format!("c.{}", c));

        Ok(CountrySchema {
            has_iso3,
            has_code2,
            has_currency_id,
            has_name,
            code_col,
            code_expr,
        })
    }
}

// Global schema cache instance
static SCHEMA_CACHE: OnceCell<Arc<SchemaCache>> = OnceCell::const_new();

/// Get the initialized schema cache
/// Panics if cache hasn't been initialized via init_schema_cache()
pub fn get_schema_cache() -> &'static SchemaCache {
    SCHEMA_CACHE
        .get()
        .expect("Schema cache not initialized - call init_schema_cache() first")
        .as_ref()
}

/// Initialize the global schema cache
/// Should be called once at application startup
pub async fn init_schema_cache(db: &Db) -> Result<()> {
    let cache = SchemaCache::init(db).await?;
    SCHEMA_CACHE
        .set(Arc::new(cache))
        .map_err(|_| anyhow!("Schema cache already initialized"))?;
    Ok(())
}

fn env_truthy(name: &str) -> bool {
    match std::env::var(name) {
        Ok(v) => matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "y"
        ),
        Err(_) => false,
    }
}

static PROVIDER_KIND_SUPPORTED: OnceCell<bool> = OnceCell::const_new();
static PHP_COMPAT_MODE: OnceCell<bool> = OnceCell::const_new();
static OFFER_COMPAT: OnceCell<(
    std::sync::atomic::AtomicI64,
    Mutex<HashMap<i64, (i64, i64, Option<String>)>>,
)> = OnceCell::const_new();
static SELLABLE_COMPAT: OnceCell<(
    std::sync::atomic::AtomicI64,
    Mutex<HashMap<(String, i64), i64>>,
)> = OnceCell::const_new();
static PROVIDER_ITEMS_PRESENT: OnceCell<bool> = OnceCell::const_new();
static PROVIDER_MEDIA_LINKS_PRESENT: OnceCell<bool> = OnceCell::const_new();
static PROVIDER_OFFERS_PRESENT: OnceCell<bool> = OnceCell::const_new();
static PROVIDER_TOPLISTS_PRESENT: OnceCell<bool> = OnceCell::const_new();
static PROVIDER_TOPLIST_ITEMS_PRESENT: OnceCell<bool> = OnceCell::const_new();
static COUNTRY_SCHEMA: OnceCell<CountrySchema> = OnceCell::const_new();
static JURISDICTIONS_PRESENT: OnceCell<bool> = OnceCell::const_new();
static VIDEO_GAMES_CONTENT_COLS: OnceCell<VideoGamesContentColumns> = OnceCell::const_new();
static GAME_MEDIA_HAS_SOURCE_COL: OnceCell<bool> = OnceCell::const_new();
static GAME_MEDIA_SOURCE_IS_ENUM: OnceCell<bool> = OnceCell::const_new();
static GAME_MEDIA_MEDIA_TYPE_IS_ENUM: OnceCell<bool> = OnceCell::const_new();
static MEDIA_SOURCE_SUPPORTS_PSSTORE: OnceCell<bool> = OnceCell::const_new();
static MEDIA_TYPE_SUPPORTS_BACKGROUND: OnceCell<bool> = OnceCell::const_new();

async fn provider_toplists_present(db: &Db) -> Result<bool> {
    let has = PROVIDER_TOPLISTS_PRESENT
        .get_or_try_init(|| async {
            Ok::<bool, anyhow::Error>(table_exists(db, "provider_toplists").await.unwrap_or(false))
        })
        .await?;
    Ok(*has)
}

async fn provider_toplist_items_present(db: &Db) -> Result<bool> {
    let has = PROVIDER_TOPLIST_ITEMS_PRESENT
        .get_or_try_init(|| async {
            Ok::<bool, anyhow::Error>(
                table_exists(db, "provider_toplist_items")
                    .await
                    .unwrap_or(false),
            )
        })
        .await?;
    Ok(*has)
}

async fn provider_items_present(db: &Db) -> Result<bool> {
    let has = PROVIDER_ITEMS_PRESENT
        .get_or_try_init(|| async {
            Ok::<bool, anyhow::Error>(table_exists(db, "provider_items").await.unwrap_or(false))
        })
        .await?;
    Ok(*has)
}

async fn provider_media_links_present(db: &Db) -> Result<bool> {
    let has = PROVIDER_MEDIA_LINKS_PRESENT
        .get_or_try_init(|| async {
            Ok::<bool, anyhow::Error>(
                table_exists(db, "canonical_media")
                    .await
                    .unwrap_or(false),
            )
        })
        .await?;
    Ok(*has)
}

async fn provider_offers_present(db: &Db) -> Result<bool> {
    let has = PROVIDER_OFFERS_PRESENT
        .get_or_try_init(|| async {
            Ok::<bool, anyhow::Error>(table_exists(db, "provider_offers").await.unwrap_or(false))
        })
        .await?;
    Ok(*has)
}

async fn media_source_supports_psstore(db: &Db) -> Result<bool> {
    let has = MEDIA_SOURCE_SUPPORTS_PSSTORE
        .get_or_try_init(|| async {
            let exists: Option<bool> = sqlx::query_scalar(
                "SELECT TRUE\n                 FROM pg_type t\n                 JOIN pg_enum e ON e.enumtypid = t.oid\n                 WHERE t.typname = 'media_source'\n                   AND e.enumlabel = 'psstore'\n                 LIMIT 1",
            )
            .persistent(false)
            .fetch_optional(&db.pool)
            .await?;
            Ok::<bool, anyhow::Error>(exists.unwrap_or(false))
        })
        .await?;
    Ok(*has)
}

async fn media_type_supports_background(db: &Db) -> Result<bool> {
    let has = MEDIA_TYPE_SUPPORTS_BACKGROUND
        .get_or_try_init(|| async {
            let exists: Option<bool> = sqlx::query_scalar(
                "SELECT TRUE\n                 FROM pg_type t\n                 JOIN pg_enum e ON e.enumtypid = t.oid\n                 WHERE t.typname = 'media_type'\n                   AND e.enumlabel = 'background'\n                 LIMIT 1",
            )
            .persistent(false)
            .fetch_optional(&db.pool)
            .await?;
            Ok::<bool, anyhow::Error>(exists.unwrap_or(false))
        })
        .await?;
    Ok(*has)
}

async fn game_media_has_source_col(db: &Db) -> Result<bool> {
    let has = GAME_MEDIA_HAS_SOURCE_COL
        .get_or_try_init(|| async {
            let exists: Option<bool> = sqlx::query_scalar(
                "SELECT TRUE FROM information_schema.columns \
                 WHERE table_schema = ANY (current_schemas(true)) \
                   AND table_name = 'game_media' \
                   AND column_name = 'source' \
                 LIMIT 1",
            )
            .persistent(false)
            .fetch_optional(&db.pool)
            .await?;
            Ok::<bool, anyhow::Error>(exists.unwrap_or(false))
        })
        .await?;
    Ok(*has)
}

async fn game_media_source_is_enum(db: &Db) -> Result<bool> {
    let is_enum = GAME_MEDIA_SOURCE_IS_ENUM
        .get_or_try_init(|| async {
            let udt = table_column_udt_name(db, "game_media", "source").await?;
            Ok::<bool, anyhow::Error>(udt.as_deref() == Some("media_source"))
        })
        .await?;
    Ok(*is_enum)
}

async fn game_media_media_type_is_enum(db: &Db) -> Result<bool> {
    let is_enum = GAME_MEDIA_MEDIA_TYPE_IS_ENUM
        .get_or_try_init(|| async {
            let udt = table_column_udt_name(db, "game_media", "media_type").await?;
            Ok::<bool, anyhow::Error>(udt.as_deref() == Some("media_type"))
        })
        .await?;
    Ok(*is_enum)
}

/// Detect whether the legacy PHP schema is present (sku_regions/region_prices) and usable.
pub async fn table_exists(db: &Db, name: &str) -> Result<bool> {
    // IMPORTANT: We intentionally check *visibility* via search_path resolution.
    // Using information_schema + current_schemas(true) can yield false positives when
    // multiple schemas contain the same table name (e.g., a shadowing schema earlier
    // in search_path). Our queries are unqualified, so to_regclass() is the correct
    // reflection of what will actually be queried at runtime.
    let visible: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .persistent(false)
        .bind(name)
        .fetch_one(&db.pool)
        .await?;
    Ok(visible)
}

pub async fn php_compat_schema(db: &Db) -> Result<bool> {
    let compat = PHP_COMPAT_MODE
        .get_or_try_init(|| async {
            let sku_regions = table_exists(db, "sku_regions").await.unwrap_or(false);
            let region_prices = table_exists(db, "region_prices").await.unwrap_or(false);
            if !sku_regions || !region_prices {
                return Ok::<bool, anyhow::Error>(false);
            }

            // Supporting tables are optional in legacy-only environments; warn if absent but keep compat on so callers can
            // take a best-effort path against sku_regions alone.
            let countries = table_exists(db, "countries").await.unwrap_or(false);
            let jurisdictions = table_exists(db, "jurisdictions").await.unwrap_or(false);
            if !countries || !jurisdictions {
                warn!(
                    countries,
                    jurisdictions,
                    "php compat: supporting tables partially missing; sku_regions/region_prices are present, but jurisdictions/countries may be absent. We will treat sku_regions.id as the canonical 'jurisdiction' identifier when possible. If a caller only has a country/jurisdiction id and needs region_code mapping, set GC_ALLOW_COUNTRY_ONLY_JURISDICTIONS=1 to allow treating jurisdiction_id as country_id (best-effort)."
                );
            }
            Ok::<bool, anyhow::Error>(true)
        })
        .await?;
    if *compat {
        debug!("php compatibility mode enabled (sku_regions detected)");
    }
    Ok(*compat)
}

async fn provider_kind_supported(db: &Db) -> Result<bool> {
    let supported = PROVIDER_KIND_SUPPORTED
        .get_or_try_init(|| async {
            let exists: Option<bool> = sqlx::query_scalar::<_, bool>(
                "SELECT TRUE FROM information_schema.columns \
                     WHERE table_name = 'video_game_sources' AND column_name = 'category' \
                       AND table_schema = ANY (current_schemas(true)) LIMIT 1",
            )
            .persistent(false)
            .fetch_optional(&db.pool)
            .await?;
            let supported = exists.unwrap_or(false);
            if supported {
                debug!("video_game_sources.category column detected");
            } else {
                warn!(
                    "video_game_sources.category column missing — falling back to legacy schema compatibility"
                );
            }
            Ok::<bool, anyhow::Error>(supported)
        })
        .await?;
    Ok(*supported)
}

pub struct IngestResult {
    pub offer_jurisdiction_ids: Vec<i64>,
    pub current_updates: Vec<CurrentPriceRow>,
}

#[derive(Debug, Default)]
pub struct PostIngestSummary {
    pub video_game_source_ids: HashSet<i64>,
    pub offer_jurisdiction_ids: HashSet<i64>,
    pub total_price_rows_written: usize,
    pub total_current_updates: usize,
    pub bundle_rows_ingested: usize,
    pub bundle_rows_skipped: usize,
    pub bundle_offer_jurisdictions_ingested: HashSet<i64>,
    pub bundle_offer_jurisdictions_skipped: HashSet<i64>,
}

impl PostIngestSummary {
    pub fn record_provider_item(&mut self, video_game_source_id: i64) {
        if video_game_source_id != 0 {
            self.video_game_source_ids.insert(video_game_source_id);
        }
    }

    pub fn record_provider_items<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = i64>,
    {
        self.video_game_source_ids
            .extend(iter.into_iter().filter(|id| *id != 0));
    }

    pub fn record_batch(&mut self, batch_len: usize, ingest_result: &IngestResult) {
        self.total_price_rows_written += batch_len;
        self.total_current_updates += ingest_result.current_updates.len();
        self.offer_jurisdiction_ids
            .extend(ingest_result.offer_jurisdiction_ids.iter().copied());
    }

    pub fn record_bundle_ingest(&mut self, offer_jurisdiction_id: i64) {
        self.bundle_rows_ingested += 1;
        self.bundle_offer_jurisdictions_ingested
            .insert(offer_jurisdiction_id);
    }

    pub fn record_bundle_skip(&mut self, offer_jurisdiction_id: i64) {
        self.bundle_rows_skipped += 1;
        self.bundle_offer_jurisdictions_skipped
            .insert(offer_jurisdiction_id);
    }

    pub async fn verify(&self, db: &Db, provider_id: i64) -> Result<()> {
        verify_post_ingest(
            db,
            provider_id,
            &self.video_game_source_ids,
            &self.offer_jurisdiction_ids,
            self.total_price_rows_written,
            self.total_current_updates,
        )
        .await?;

        info!(
            provider_id,
            bundle_rows_ingested = self.bundle_rows_ingested,
            bundle_rows_skipped = self.bundle_rows_skipped,
            bundle_offer_jurisdictions_ingested = self.bundle_offer_jurisdictions_ingested.len(),
            bundle_offer_jurisdictions_skipped = self.bundle_offer_jurisdictions_skipped.len(),
            "bundle price coverage summary"
        );
        Ok(())
    }
}

pub async fn verify_post_ingest(
    db: &Db,
    provider_id: i64,
    video_game_source_ids: &HashSet<i64>,
    offer_jurisdiction_ids: &HashSet<i64>,
    total_price_rows_written: usize,
    total_current_updates: usize,
) -> Result<()> {
    if total_price_rows_written == 0 {
        warn!(provider_id, "no price rows were written during ingest");
    } else {
        info!(
            provider_id,
            price_rows = total_price_rows_written,
            current_updates = total_current_updates,
            "price rows flushed and region_prices inserts attempted"
        );
    }

    if !offer_jurisdiction_ids.is_empty() {
        let oj_vec: Vec<i64> = offer_jurisdiction_ids.iter().copied().collect();
        let covered_regions: i64 = sqlx::query_scalar(
            "SELECT COUNT(DISTINCT sku_region_id) FROM region_prices WHERE sku_region_id = ANY($1)",
        )
        .persistent(false)
        .bind(&oj_vec)
        .fetch_one(&db.pool)
        .await?;
        if covered_regions < (offer_jurisdiction_ids.len() as i64) {
            warn!(
                provider_id,
                expected = offer_jurisdiction_ids.len(),
                actual = covered_regions,
                "region_prices coverage incomplete"
            );
        } else {
            info!(
                provider_id,
                regions_covered = covered_regions,
                expected = offer_jurisdiction_ids.len(),
                "region_prices coverage verified"
            );
        }
    } else {
        info!(
            provider_id,
            "no sku_regions touched; skipping region_prices coverage verification"
        );
    }

    if !video_game_source_ids.is_empty() {
        if !provider_media_links_present(db).await.unwrap_or(false) {
            info!(
                provider_id,
                "provider_media_links table missing; skipping media propagation verification"
            );
            return Ok(());
        }
        let items_vec: Vec<i64> = video_game_source_ids.iter().copied().collect();
        let media_total: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM vg_source_media_links WHERE video_game_source_id = ANY($1)",
        )
        .persistent(false)
        .bind(&items_vec)
        .fetch_one(&db.pool)
        .await?;
        let media_nulls: i64 = sqlx
            ::query_scalar(
                "SELECT COUNT(*) FROM vg_source_media_links WHERE video_game_source_id = ANY($1) AND url IS NULL"
            )
            .persistent(false)
            .bind(&items_vec)
            .fetch_one(&db.pool).await?;
        if media_total == 0 {
            info!(
                provider_id,
                "no provider media links associated with processed items; upstream feed may lack media"
            );
        } else if media_nulls > 0 {
            warn!(
                provider_id,
                missing = media_nulls,
                total = media_total,
                "provider media links contain NULL urls"
            );
        } else {
            info!(
                provider_id,
                media_links = media_total,
                "provider media propagation verified"
            );
        }
    } else {
        info!(
            provider_id,
            "no provider items processed; skipping media verification"
        );
    }

    // Strong invariant check (opt-in strictness via env): every processed provider_item should have
    // a canonical linkage row in video_game_titles: (video_game_source_id, vg_source_item_id).
    // This is the core guarantee of the title/source rewrite.
    //
    // We only run this when the relevant tables/columns exist.
    if !video_game_source_ids.is_empty()
        && table_exists(db, "video_game_sources")
            .await
            .unwrap_or(false)
        && table_exists(db, "video_game_titles").await.unwrap_or(false)
        && table_exists(db, "provider_items").await.unwrap_or(false)
    {
        let schema = get_video_game_title_schema(db).await?;
        if schema.has_video_game_source_id && schema.has_vg_source_item_id {
            let items_vec: Vec<i64> = video_game_source_ids.iter().copied().collect();

            let linked: i64 = sqlx::query_scalar(
                "SELECT COUNT(DISTINCT pi.id)\n                 FROM provider_items pi\n                 JOIN video_game_titles vgt\n                   ON vgt.video_game_source_id = $1\n                  AND vgt.vg_source_item_id = pi.external_id\n                 WHERE pi.id = ANY($2)",
            )
            .persistent(false)
            .bind(provider_id)
            .bind(&items_vec)
            .fetch_one(&db.pool)
            .await?;

            if linked < (video_game_source_ids.len() as i64) {
                let missing: Vec<(i64, String)> = sqlx::query(
                    "SELECT pi.id, pi.external_id\n                     FROM provider_items pi\n                     LEFT JOIN video_game_titles vgt\n                       ON vgt.video_game_source_id = $1\n                      AND vgt.vg_source_item_id = pi.external_id\n                     WHERE pi.id = ANY($2)\n                       AND vgt.id IS NULL\n                     ORDER BY pi.id\n                     LIMIT 25",
                )
                .persistent(false)
                .bind(provider_id)
                .bind(&items_vec)
                .fetch_all(&db.pool)
                .await?
                .into_iter()
                .map(|r| (r.get::<i64, _>("id"), r.get::<String, _>("external_id")))
                .collect();

                let allow = env_truthy("GC_ALLOW_UNLINKED_SOURCE_ITEMS");
                if allow {
                    warn!(
                        provider_id,
                        expected = video_game_source_ids.len(),
                        linked,
                        missing_sample = ?missing,
                        "video_game_titles linkage incomplete for processed provider_items (GC_ALLOW_UNLINKED_SOURCE_ITEMS=1: continuing)"
                    );
                } else {
                    anyhow::bail!(
                        "video_game_titles linkage incomplete for processed provider_items (provider_id={provider_id}, expected={}, linked={}, missing_sample={:?}). Set GC_ALLOW_UNLINKED_SOURCE_ITEMS=1 to downgrade to warning.",
                        video_game_source_ids.len(),
                        linked,
                        missing
                    );
                }
            } else {
                info!(
                    provider_id,
                    linked,
                    expected = video_game_source_ids.len(),
                    "video_game_titles linkage verified for processed provider_items"
                );
            }
        } else {
            // Old DB variant: linkage columns missing. We don't fail here because the DB cannot
            // satisfy the invariant.
            warn!(
                provider_id,
                "video_game_titles lacks (video_game_source_id, video_game_source_id); skipping strict source/title linkage verification"
            );
        }
    }

    Ok(())
}

#[instrument(skip(db))]
pub async fn ensure_provider(db: &Db, name: &str, kind: &str, slug: Option<&str>) -> Result<i64> {
    // Try by slug if provided
    if let Some(s) = slug {
        if let Some(r) = sqlx::query("SELECT id FROM providers WHERE slug = $1")
            .persistent(false)
            .bind(s)
            .fetch_optional(&db.pool)
            .await?
        {
            debug!(provider_slug=%s, provider_id=r.get::<i64, _>("id"), "provider exists via slug");
            return Ok(r.get::<i64, _>("id"));
        }
    }

    // Try by name
    if let Some(r) = sqlx::query("SELECT id FROM providers WHERE name = $1")
        .persistent(false)
        .bind(name)
        .fetch_optional(&db.pool)
        .await?
    {
        debug!(provider_name=%name, provider_id=r.get::<i64, _>("id"), "provider exists via name");
        return Ok(r.get::<i64, _>("id"));
    }

    // JIT Migration: Check legacy video_game_sources
    if let Some(s) = slug {
        // Check if it exists in video_game_sources
        if let Some(r) = sqlx::query("SELECT id, display_name, provider, kind FROM video_game_sources WHERE slug = $1")
            .persistent(false)
            .bind(s)
            .fetch_optional(&db.pool)
            .await?
        {
            let display_name: Option<String> = r.try_get("display_name").ok();
            let provider: Option<String> = r.try_get("provider").ok();
            let legacy_name = display_name.or(provider).unwrap_or_else(|| name.to_string());
            
            let legacy_kind_opt: Option<String> = r.try_get("kind").ok();
            let legacy_kind = legacy_kind_opt.unwrap_or_else(|| kind.to_string());
            
            info!(slug=%s, "migrating legacy video_game_source to providers");
            
            let inserted = sqlx::query(
                "INSERT INTO providers (name, kind, slug) VALUES ($1, $2, $3) RETURNING id",
            )
            .persistent(false)
            .bind(legacy_name)
            .bind(legacy_kind)
            .bind(s)
            .fetch_one(&db.pool)
            .await?;
            
            return Ok(inserted.get("id"));
        }
    }

    // Insert fresh row
    let inserted = sqlx::query(
        "INSERT INTO providers (name, kind, slug) VALUES ($1, $2, $3) RETURNING id",
    )
    .persistent(false)
    .bind(name)
    .bind(kind)
    .bind(slug)
    .fetch_one(&db.pool)
    .await?;

    debug!(provider_name=%name, provider_slug=?slug, provider_kind=%kind, provider_id=inserted.get::<i64,_>("id"), "provider inserted");
    Ok(inserted.get("id"))
}

/// Legacy PHP schema helper.
///
/// Returns a `game_providers.id` (NOT a `video_game_sources.id`).
/// Use this only for legacy pricing tables that require `game_provider_id`.
/// If `game_providers` table doesn't exist but `video_game_sources` does, uses that instead.
#[instrument(skip(db))]
pub async fn ensure_game_provider(
    db: &Db,
    name: &str,
    _kind: &str,
    slug: Option<&str>,
) -> Result<i64> {
    // Check if game_providers table exists
    if !table_exists(db, "game_providers").await.unwrap_or(false) {
        // Fall back to video_game_sources if available - directly query to avoid recursion
        if table_exists(db, "video_game_sources")
            .await
            .unwrap_or(false)
        {
            info!(
                name,
                slug,
                "ensure_game_provider: game_providers missing; using video_game_sources directly"
            );
            // Directly query video_game_sources to avoid infinite recursion with ensure_provider
            let key = slug.unwrap_or(name);
            if let Some(rec) =
                sqlx::query("SELECT id FROM video_game_sources WHERE provider_key = $1")
                    .persistent(false)
                    .bind(key)
                    .fetch_optional(&db.pool)
                    .await?
            {
                return Ok(rec.get("id"));
            }
            // Try by slug
            if let Some(rec) = sqlx::query("SELECT id FROM video_game_sources WHERE slug = $1")
                .persistent(false)
                .bind(slug)
                .fetch_optional(&db.pool)
                .await?
            {
                return Ok(rec.get("id"));
            }
            // Try by display_name
            if let Some(rec) =
                sqlx::query("SELECT id FROM video_game_sources WHERE display_name = $1")
                    .persistent(false)
                    .bind(name)
                    .fetch_optional(&db.pool)
                    .await?
            {
                return Ok(rec.get("id"));
            }
            // Create new video_game_source entry
            if let Some(row) = sqlx::query(
                "INSERT INTO video_game_sources (display_name, slug, provider_key) VALUES ($1,$2,$3) RETURNING id",
            )
            .persistent(false)
            .bind(name)
            .bind(slug)
            .bind(key)
            .fetch_optional(&db.pool)
            .await?
            {
                return Ok(row.get("id"));
            }
            // If insert did nothing, fetch by provider_key again
            if let Some(rec) =
                sqlx::query("SELECT id FROM video_game_sources WHERE provider_key = $1")
                    .persistent(false)
                    .bind(key)
                    .fetch_optional(&db.pool)
                    .await?
            {
                return Ok(rec.get("id"));
            }

            anyhow::bail!("Failed to create or fetch video_game_source for {}", name);
        }

        // Last resort: warn if neither table exists
        warn!(
            "ensure_game_provider: neither game_providers nor video_game_sources table exists. \
            Provider: {} (slug: {:?}). Returning 0.",
            name,
            slug
        );
        return Ok(0);
    }

    // PHP schema: game_providers(provider_key unique)
    let key = slug.unwrap_or(name);
    if let Some(rec) = sqlx::query("SELECT id FROM game_providers WHERE provider_key = $1")
        .persistent(false)
        .bind(key)
        .fetch_optional(&db.pool)
        .await?
    {
        return Ok(rec.get("id"));
    }
    // Upsert on the unique composite (provider_key, providable_type, providable_id)
    // to avoid race-induced 23505 errors and return the existing id in one round-trip.
    // First attempt: insert with ON CONFLICT DO NOTHING (handles duplicate PK/unique quietly)
    if let Some(row) = sqlx::query(
        "INSERT INTO game_providers (provider_key, name, slug, providable_type, providable_id) \
         VALUES ($1,$2,$3,$4,$5) \
         ON CONFLICT DO NOTHING \
         RETURNING id",
    )
    .persistent(false)
    .bind(key)
    .bind(name)
    .bind(slug)
    .bind("App\\Models\\GamesProvider")
    .bind(0_i64)
    .fetch_optional(&db.pool)
    .await?
    {
        return Ok(row.get("id"));
    }

    // If the insert did nothing (common when the sequence is behind existing IDs), bump the sequence once and retry.
    let _ = sqlx::query_scalar::<_, Option<i64>>(
        "SELECT setval(pg_get_serial_sequence('game_providers','id'), (SELECT COALESCE(MAX(id),0) FROM game_providers))",
    )
    .persistent(false)
    .fetch_optional(&db.pool)
    .await?;

    if let Some(row) = sqlx::query(
        "INSERT INTO game_providers (provider_key, name, slug, providable_type, providable_id) \
         VALUES ($1,$2,$3,$4,$5) \
         ON CONFLICT DO NOTHING \
         RETURNING id",
    )
    .persistent(false)
    .bind(key)
    .bind(name)
    .bind(slug)
    .bind("App\\Models\\GamesProvider")
    .bind(0_i64)
    .fetch_optional(&db.pool)
    .await?
    {
        return Ok(row.get("id"));
    }

    // Final fallback: fetch the existing row by provider_key (covers RLS-visible rows or concurrent insertions).
    let rec = sqlx::query("SELECT id FROM game_providers WHERE provider_key = $1")
        .persistent(false)
        .bind(key)
        .fetch_optional(&db.pool)
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "game_providers exists but no row found for provider_key {}",
                key
            )
        })?;
    Ok(rec.get("id"))
}

fn normalized_retailer_token(value: &str) -> String {
    value
        .chars()
        .filter(|c| !c.is_whitespace() && *c != '-' && *c != '_')
        .collect::<String>()
        .to_ascii_lowercase()
}

fn retailer_alias_from_token(token: &str) -> Option<&'static str> {
    match token {
        // PlayStation Store aliases
        "psstore"
        | "psn"
        | "playstation"
        | "playstationstore"
        | "sonyplaystation"
        | "sonyplaystationstore" => Some("psstore"),
        // Steam aliases
        "steam" | "steamstore" | "steampowered" | "steamcommunity" | "valvesteam" => Some("steam"),
        _ => None,
    }
}

fn canonicalize_retailer_identity(name: &str, slug: Option<&str>) -> (String, Option<String>) {
    let trimmed_name = name.trim();
    let slug_alias = slug
        .and_then(|s| {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(normalized_retailer_token(trimmed))
            }
        })
        .and_then(|token| retailer_alias_from_token(&token).map(|alias| alias.to_string()));
    let name_alias = retailer_alias_from_token(&normalized_retailer_token(trimmed_name))
        .map(|alias| alias.to_string());
    let canonical_slug = slug_alias.or(name_alias);

    let canonical_name = match canonical_slug.as_deref() {
        Some("psstore") => "PlayStation Store",
        Some("steam") => "Steam",
        _ => trimmed_name,
    }
    .to_string();

    (canonical_name, canonical_slug)
}

#[instrument(skip(db))]
pub async fn ensure_retailer(db: &Db, name: &str, slug: Option<&str>) -> Result<i64> {
    let trimmed_name = name.trim().to_string();
    let trimmed_slug = slug.and_then(|s| {
        let trimmed = s.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    });
    let (canonical_name, canonical_slug) =
        canonicalize_retailer_identity(&trimmed_name, trimmed_slug.as_deref());
    let slug_for_insert = canonical_slug.clone().or(trimmed_slug.clone());

    let compat_schema = php_compat_schema(db).await.unwrap_or(false);
    let legacy_retailers_available = if compat_schema {
        table_exists(db, "game_retailers").await.unwrap_or(false)
    } else {
        false
    };

    if compat_schema && legacy_retailers_available {
        // Legacy schema expects game_provider_id NOT NULL; reuse/create a matching game_provider first.
        let game_provider_id = ensure_game_provider(
            db,
            &canonical_name,
            "storefront",
            slug_for_insert.as_deref(),
        )
        .await?;

        let mut retailer_key_candidates: Vec<String> = Vec::new();
        let mut seen_lower: HashSet<String> = HashSet::new();
        let mut push_candidate = |value: &str| {
            if value.is_empty() {
                return;
            }
            let lower = value.to_ascii_lowercase();
            if seen_lower.insert(lower) {
                retailer_key_candidates.push(value.to_string());
            }
        };

        if let Some(ref canonical) = canonical_slug {
            push_candidate(canonical);
        }
        if let Some(ref provided) = trimmed_slug {
            push_candidate(provided);
        }
        push_candidate(&canonical_name);
        push_candidate(&trimmed_name);

        for candidate in &retailer_key_candidates {
            if let Some(rec) = sqlx::query("SELECT id FROM game_retailers WHERE retailer_key = $1")
                .persistent(false)
                .bind(candidate)
                .fetch_optional(&db.pool)
                .await?
            {
                return Ok(rec.get("id"));
            }
        }

        let key_for_insert = slug_for_insert
            .clone()
            .unwrap_or_else(|| canonical_name.clone());

        let rec = sqlx::query(
            "INSERT INTO game_retailers (game_provider_id, retailer_key, name, slug) VALUES ($1,$2,$3,$4) RETURNING id",
        )
        .persistent(false)
        .bind(game_provider_id)
        .bind(&key_for_insert)
        .bind(&canonical_name)
        .bind(slug_for_insert.as_deref())
        .fetch_one(&db.pool)
        .await?;
        return Ok(rec.get("id"));
    }

    if compat_schema && !legacy_retailers_available {
        warn!(
            compat_schema = compat_schema,
            "php compat schema detected but game_retailers table missing; falling back to retailers table"
        );
    }

    let retailers_available = table_exists(db, "retailers").await.unwrap_or(false);

    if !retailers_available {
        warn!(
            "ensure_retailer: retailers table not available (name: {}, slug: {:?}). Returning 0.",
            name,
            slug
        );
        return Ok(0);
    }

    let mut slug_candidates: Vec<String> = Vec::new();
    if let Some(ref canonical) = canonical_slug {
        slug_candidates.push(canonical.clone());
    }
    if let Some(ref provided) = trimmed_slug {
        if Some(provided.as_str()) != canonical_slug.as_deref() {
            slug_candidates.push(provided.clone());
        }
    }

    for candidate_slug in &slug_candidates {
        if let Some(rec) = sqlx::query("SELECT id FROM retailers WHERE slug = $1")
            .persistent(false)
            .bind(candidate_slug)
            .fetch_optional(&db.pool)
            .await?
        {
            return Ok(rec.get("id"));
        }
    }

    let mut name_candidates: Vec<String> = vec![canonical_name.clone()];
    if !canonical_name.eq_ignore_ascii_case(&trimmed_name) {
        name_candidates.push(trimmed_name.clone());
    }

    for candidate_name in &name_candidates {
        if let Some(rec) = sqlx::query("SELECT id FROM retailers WHERE name = $1")
            .persistent(false)
            .bind(candidate_name)
            .fetch_optional(&db.pool)
            .await?
        {
            return Ok(rec.get("id"));
        }
    }

    // Insert with ON CONFLICT to handle duplicate slug gracefully
    let inserted = sqlx::query(
        "INSERT INTO retailers (name, slug) VALUES ($1,$2) \
             ON CONFLICT (slug) DO UPDATE SET name=EXCLUDED.name \
             WHERE retailers.slug IS NOT NULL \
             RETURNING id",
    )
    .persistent(false)
    .bind(&canonical_name)
    .bind(slug_for_insert.as_deref())
    .fetch_optional(&db.pool)
    .await?;

    if let Some(row) = inserted {
        return Ok(row.get("id"));
    }

    // Fallback: insert without slug constraint conflict
    let fallback = sqlx::query("INSERT INTO retailers (name) VALUES ($1) RETURNING id")
        .persistent(false)
        .bind(&canonical_name)
        .fetch_one(&db.pool)
        .await?;
    Ok(fallback.get("id"))
}

#[instrument(skip(db, price_rows))]
pub async fn ingest_prices(db: &Db, price_rows: Vec<PriceRow>) -> Result<IngestResult> {
    // Canonical pricing write path: insert into Laravel-style sku_regions/region_prices.
    // Hard requirement: we do NOT fall back to legacy prices/current_price writes.
    let compat = php_compat_schema(db).await?;
    if !compat {
        return Err(anyhow!(
            "canonical pricing schema required (sku_regions + region_prices). Refusing to write to legacy prices/current_price tables."
        ));
    }

    if price_rows.is_empty() {
        return Ok(IngestResult {
            offer_jurisdiction_ids: Vec::new(),
            current_updates: Vec::new(),
        });
    }

    // php-compat assumption: region_prices inserts require a currencies FK id.
    // Some legacy deployments may have sku_regions/region_prices without currencies.
    if !table_exists(db, "currencies").await.unwrap_or(false) {
        return Err(anyhow!(
            "php compat: currencies table missing; cannot ingest prices because region_prices expects currency_id. Apply migrations/seeds to create currencies."
        ));
    }

    // Map sku_region_id -> currency_id, country_id, minor_unit, currency_code using dynamic country code detection when available.
    let mut sku_region_ids: Vec<i64> = price_rows.iter().map(|r| r.offer_jurisdiction_id).collect();
    let has_countries = table_exists(db, "countries").await.unwrap_or(false);
    // Canonical Laravel schema uses currencies.decimals; older DBs may have currencies.minor_unit.
    // If neither exists, treat minor_unit as 2.
    let currencies_has_minor_unit = table_column_exists(db, "currencies", "minor_unit")
        .await
        .unwrap_or(false);
    let currencies_has_decimals = table_column_exists(db, "currencies", "decimals")
        .await
        .unwrap_or(false);
    let currency_minor_expr = if currencies_has_minor_unit {
        "COALESCE(curr.minor_unit, 2)".to_string()
    } else if currencies_has_decimals {
        "COALESCE(curr.decimals, 2)".to_string()
    } else {
        "2::smallint".to_string()
    };
    let mapping_sql = if has_countries {
        let country_schema = country_schema(db).await?;
        let code_col = country_schema.code_column().ok_or_else(|| {
            anyhow::anyhow!("countries table missing code/iso2 column for php compat mapping")
        })?;
        format!(
            "SELECT sr.id AS sku_region_id, sr.region_code, sr.currency AS currency_code, c.id AS country_id, curr.id AS currency_id, {currency_minor_expr} AS minor_unit \
             FROM sku_regions sr \
             LEFT JOIN countries c ON UPPER(c.{code_col}) = UPPER(split_part(sr.region_code,'-',1)) \
             LEFT JOIN currencies curr ON UPPER(curr.code) = UPPER(sr.currency) \
             WHERE sr.id = ANY($1)",
            code_col = code_col,
            currency_minor_expr = currency_minor_expr
        )
    } else {
        // Countries table absent: best-effort mapping using sku_regions + currencies only.
        format!(
            "SELECT sr.id AS sku_region_id, sr.region_code, sr.currency AS currency_code, NULL::bigint AS country_id, curr.id AS currency_id, {currency_minor_expr} AS minor_unit \
             FROM sku_regions sr \
             LEFT JOIN currencies curr ON UPPER(curr.code) = UPPER(sr.currency) \
             WHERE sr.id = ANY($1)",
            currency_minor_expr = currency_minor_expr
        )
    };
    let rows = sqlx::query(&mapping_sql)
        .persistent(false)
        .bind(&sku_region_ids)
        .fetch_all(&db.pool)
        .await?;
    let mut map: HashMap<i64, (i64, Option<i64>, i16, String)> = HashMap::new();
    for row in rows {
        let sku_region_id: i64 = row.get("sku_region_id");
        let currency_id: Option<i64> = row.get("currency_id");
        let country_id: Option<i64> = row.get("country_id");
        let minor_unit: i16 = row.get("minor_unit");
        let currency_code: String = row
            .get::<Option<String>, _>("currency_code")
            .unwrap_or_default()
            .to_ascii_uppercase();
        if let Some(cur) = currency_id {
            map.insert(sku_region_id, (cur, country_id, minor_unit, currency_code));
        } else {
            warn!(
                sku_region_id,
                "skipping price row because currency lookup failed for sku_region"
            );
        }
    }

    // Fallback: ids that are not sku_regions yet may actually be jurisdiction ids. Map by region_code -> sku_regions if jurisdictions table exists.
    if has_countries && table_exists(db, "jurisdictions").await.unwrap_or(false) {
        let missing: Vec<i64> = sku_region_ids
            .iter()
            .copied()
            .filter(|id| !map.contains_key(id))
            .collect();
        if !missing.is_empty() {
            let country_schema = country_schema(db).await?;
            let code_expr = country_schema.code_expr().ok_or_else(|| {
                anyhow!("countries table missing code expression for jurisdiction mapping")
            })?;
            let j_sql = format!(
                "SELECT j.id AS jurisdiction_id, j.country_id, COALESCE(j.region_code, {code_expr}) AS region_code FROM jurisdictions j JOIN countries c ON c.id=j.country_id WHERE j.id = ANY($1)",
                code_expr = code_expr
            );
            let j_rows = sqlx::query(&j_sql)
                .persistent(false)
                .bind(&missing)
                .fetch_all(&db.pool)
                .await?;
            let mut region_codes: HashMap<i64, (String, Option<i64>)> = HashMap::new();
            for r in j_rows {
                let jid: i64 = r.get("jurisdiction_id");
                let rc: String = r
                    .get::<Option<String>, _>("region_code")
                    .unwrap_or_default();
                let country_id: Option<i64> = r.get("country_id");
                if !rc.is_empty() {
                    region_codes.insert(jid, (rc.to_ascii_uppercase(), country_id));
                }
            }
            if !region_codes.is_empty() {
                let codes: Vec<String> = region_codes.values().map(|(c, _)| c.clone()).collect();
                let sr_sql = format!(
                    "SELECT id, region_code, currency AS currency_code, curr.id AS currency_id, {currency_minor_expr} AS minor_unit \
                     FROM sku_regions sr \
                     LEFT JOIN currencies curr ON UPPER(curr.code)=UPPER(sr.currency) \
                     WHERE UPPER(sr.region_code) = ANY($1)",
                    currency_minor_expr = currency_minor_expr
                );
                let sr_rows = sqlx::query(&sr_sql)
                    .persistent(false)
                    .bind(&codes)
                    .fetch_all(&db.pool)
                    .await?;
                // Build lookup by region_code
                let mut by_code: HashMap<String, (i64, Option<i64>, i16, String)> = HashMap::new();
                for r in sr_rows {
                    let sid: i64 = r.get("id");
                    let rc: String = r
                        .get::<Option<String>, _>("region_code")
                        .unwrap_or_default()
                        .to_ascii_uppercase();
                    let currency_id: Option<i64> = r.get("currency_id");
                    let minor_unit: i16 = r.get("minor_unit");
                    let currency_code: String = r
                        .get::<Option<String>, _>("currency_code")
                        .unwrap_or_default()
                        .to_ascii_uppercase();
                    by_code.insert(rc, (sid, currency_id, minor_unit, currency_code));
                }
                for (jid, (rc, country_id)) in region_codes {
                    if let Some((sid, currency_id, minor_unit, currency_code)) = by_code.get(&rc) {
                        if let Some(cur) = currency_id {
                            map.insert(jid, (*cur, country_id, *minor_unit, currency_code.clone()));
                            sku_region_ids.push(*sid);
                        } else {
                            warn!(jurisdiction_id=jid, region_code=%rc, "php compat: currency missing for sku_region by region_code mapping");
                        }
                    } else {
                        warn!(jurisdiction_id=jid, region_code=%rc, "php compat: no sku_region found for jurisdiction region_code");
                    }
                }
            }
        }
    }

    // Compute snapshots required by Laravel region_prices (match PriceIngestionManager::persistPricePoint):
    // - btc_rate_snapshot: local currency -> BTC (required; if missing, skip insert)
    // - fx_rate_snapshot: local currency -> USD (optional; defaults to 1.0)
    // - btc_value: toSatoshiPrecision(fiat_amount * (local->BTC))
    let fx = ExchangeService::new(db.clone());

    fn to_satoshi_precision(value: f64) -> f64 {
        // Mirrors PHP number_format($value, 8, '.', '') behavior closely enough for storage.
        (value * 1e8_f64).round() / 1e8_f64
    }

    async fn resolve_rate_like_laravel(
        fx: &ExchangeService,
        base: &str,
        quote: &str,
    ) -> Result<Option<f64>> {
        let base = base.trim().to_ascii_uppercase();
        let quote = quote.trim().to_ascii_uppercase();

        if base.is_empty() || quote.is_empty() {
            return Ok(None);
        }

        if base == quote {
            return Ok(Some(1.0));
        }

        if let Some(direct) = fx.latest_rate(&base, &quote).await? {
            return Ok(Some(direct));
        }

        let inverse = fx.latest_rate(&quote, &base).await?;
        let Some(inverse) = inverse else {
            return Ok(None);
        };
        if inverse == 0.0 {
            return Ok(None);
        }

        // Laravel: round(1 / inverse, 12)
        let v = 1.0 / inverse;
        Ok(Some((v * 1e12_f64).round() / 1e12_f64))
    }

    let mut btc_rate_cache: HashMap<String, Option<f64>> = HashMap::new();
    let mut usd_rate_cache: HashMap<String, f64> = HashMap::new();
    for (_currency_id, _country_id, _minor_unit, currency_code) in map.values() {
        if currency_code.is_empty() {
            continue;
        }
        if btc_rate_cache.contains_key(currency_code) {
            continue;
        }

        let btc_rate = resolve_rate_like_laravel(&fx, currency_code, "BTC").await?;
        if btc_rate.is_none() {
            warn!(currency_code=%currency_code, "php compat: missing <currency>/BTC rate in exchange_rates; will skip related price rows");
        }
        btc_rate_cache.insert(currency_code.clone(), btc_rate);

        let usd_rate = resolve_rate_like_laravel(&fx, currency_code, "USD")
            .await?
            .unwrap_or(1.0);
        usd_rate_cache.insert(currency_code.clone(), usd_rate);
    }

    use sqlx::QueryBuilder;
    let now = chrono::Utc::now();
    let mut rows_to_insert: Vec<(
        i64,
        i64,
        Option<i64>,
        chrono::DateTime<chrono::Utc>,
        f64,
        f64,
        f64,
        bool,
        f64,
        f64,
        Value,
        chrono::DateTime<chrono::Utc>,
        chrono::DateTime<chrono::Utc>,
    )> = Vec::new();

    let mut touched_regions: HashSet<i64> = HashSet::new();
    let mut current_updates: Vec<CurrentPriceRow> = Vec::new();
    const DEFAULT_AGENT: &str = "pipeline";
    const DEFAULT_PRIORITY: i16 = 0;

    for r in &price_rows {
        let Some((currency_id, country_id, minor_unit, currency_code)) =
            map.get(&r.offer_jurisdiction_id)
        else {
            continue;
        };

        let scale = 10f64.powi((*minor_unit).max(0) as i32);
        let amount_major = (r.amount_minor as f64) / scale;

        let btc_rate = btc_rate_cache.get(currency_code).copied().flatten();
        let Some(btc_rate) = btc_rate else {
            // Match Laravel behavior: if BTC rate is missing, skip insert.
            warn!(
                sku_region_id = r.offer_jurisdiction_id,
                currency_code = %currency_code,
                "php compat: skipping price row (missing BTC rate)"
            );
            continue;
        };

        let fx_rate_snapshot = usd_rate_cache.get(currency_code).copied().unwrap_or(1.0);
        let btc_rate_snapshot = to_satoshi_precision(btc_rate);
        let btc_value = to_satoshi_precision(amount_major * btc_rate);

        rows_to_insert.push((
            r.offer_jurisdiction_id,
            *currency_id,
            *country_id,
            r.recorded_at,
            amount_major,
            amount_major,
            btc_value,
            r.tax_inclusive,
            fx_rate_snapshot,
            btc_rate_snapshot,
            r.meta.clone(),
            now,
            now,
        ));

        touched_regions.insert(r.offer_jurisdiction_id);
        current_updates.push(CurrentPriceRow {
            offer_jurisdiction_id: r.offer_jurisdiction_id,
            amount_minor: r.amount_minor,
            recorded_at: r.recorded_at,
            agent: DEFAULT_AGENT.to_string(),
            agent_priority: DEFAULT_PRIORITY,
        });
    }

    if !rows_to_insert.is_empty() {
        let mut qb: QueryBuilder<'_, sqlx::Postgres> = QueryBuilder::new(
            "INSERT INTO region_prices (sku_region_id, currency_id, country_id, recorded_at, fiat_amount, local_amount, btc_value, tax_inclusive, fx_rate_snapshot, btc_rate_snapshot, raw_payload, created_at, updated_at) ",
        );
        qb.push_values(rows_to_insert.iter(), |mut b, row| {
            let (
                sku_region_id,
                currency_id,
                country_id,
                recorded_at,
                fiat_amount,
                local_amount,
                btc_value,
                tax_inclusive,
                fx_rate_snapshot,
                btc_rate_snapshot,
                raw_payload,
                created_at,
                updated_at,
            ) = row;
            b.push_bind(*sku_region_id)
                .push_bind(*currency_id)
                .push_bind(*country_id)
                .push_bind(recorded_at)
                .push_bind(*fiat_amount)
                .push_bind(*local_amount)
                .push_bind(*btc_value)
                .push_bind(*tax_inclusive)
                .push_bind(*fx_rate_snapshot)
                .push_bind(*btc_rate_snapshot)
                .push_bind(raw_payload)
                .push_bind(created_at)
                .push_bind(updated_at);
        });

        let exec_res = qb.build().persistent(false).execute(&db.pool).await;
        match exec_res {
            Ok(_) => {}
            Err(e) => {
                // Supabase "no-migrate" environments sometimes have SERIAL sequences out of sync
                // (e.g., rows imported with explicit ids). Retry once after bumping the sequence.
                let is_dup_pk = match &e {
                    sqlx::Error::Database(db_err) => {
                        let code = db_err.code().map(|c| c.to_string());
                        let constraint = db_err.constraint().map(|c| c.to_string());
                        code.as_deref() == Some("23505")
                            && constraint.as_deref() == Some("region_prices_pkey")
                    }
                    _ => false,
                };
                if !is_dup_pk {
                    return Err(e.into());
                }

                if let Some(seq) = sqlx::query_scalar::<_, Option<String>>(
                    "SELECT pg_get_serial_sequence('region_prices','id')",
                )
                .persistent(false)
                .fetch_one(&db.pool)
                .await?
                {
                    let _ = sqlx::query(
                        "SELECT setval($1::regclass, (SELECT COALESCE(MAX(id),0)+1 FROM region_prices), false)",
                    )
                    .persistent(false)
                    .bind(seq)
                    .execute(&db.pool)
                    .await?;
                }

                let mut qb2: QueryBuilder<'_, sqlx::Postgres> = QueryBuilder::new(
                    "INSERT INTO region_prices (sku_region_id, currency_id, country_id, recorded_at, fiat_amount, local_amount, btc_value, tax_inclusive, fx_rate_snapshot, btc_rate_snapshot, raw_payload, created_at, updated_at) ",
                );
                qb2.push_values(rows_to_insert.iter(), |mut b, row| {
                    let (
                        sku_region_id,
                        currency_id,
                        country_id,
                        recorded_at,
                        fiat_amount,
                        local_amount,
                        btc_value,
                        tax_inclusive,
                        fx_rate_snapshot,
                        btc_rate_snapshot,
                        raw_payload,
                        created_at,
                        updated_at,
                    ) = row;
                    b.push_bind(*sku_region_id)
                        .push_bind(*currency_id)
                        .push_bind(*country_id)
                        .push_bind(recorded_at)
                        .push_bind(*fiat_amount)
                        .push_bind(*local_amount)
                        .push_bind(*btc_value)
                        .push_bind(*tax_inclusive)
                        .push_bind(*fx_rate_snapshot)
                        .push_bind(*btc_rate_snapshot)
                        .push_bind(raw_payload)
                        .push_bind(created_at)
                        .push_bind(updated_at);
                });
                qb2.build().persistent(false).execute(&db.pool).await?;
            }
        }
        info!(rows = rows_to_insert.len(), "inserted region_prices rows");
    } else {
        warn!("no region_prices rows inserted (missing currency mappings or BTC rate snapshots)");
    }

    Ok(IngestResult {
        offer_jurisdiction_ids: touched_regions.into_iter().collect(),
        current_updates,
    })
}

#[derive(Clone)]
pub struct ProviderEntityCache {
    db: Db,
    product_ids: HashMap<String, i64>,
    software_rows: HashSet<i64>,
    title_ids: HashMap<i64, i64>,
    title_ids_by_source_item: HashMap<(i64, String), i64>,
    video_game_ids: HashMap<(i64, i64, Option<String>), i64>,
    video_game_ids_by_product: HashMap<i64, i64>, // Laravel schema: cache by product_id
    sellable_ids: HashMap<(String, i64), i64>,
    offer_ids: HashMap<(i64, i64, Option<String>), i64>,
    offer_jurisdictions: HashMap<(i64, i64), i64>,
    provider_items: HashMap<(i64, String), i64>,
}

impl ProviderEntityCache {
    pub fn new(db: Db) -> Self {
        Self {
            db,
            product_ids: HashMap::new(),
            software_rows: HashSet::new(),
            title_ids: HashMap::new(),
            title_ids_by_source_item: HashMap::new(),
            video_game_ids: HashMap::new(),
            video_game_ids_by_product: HashMap::new(),
            sellable_ids: HashMap::new(),
            offer_ids: HashMap::new(),
            offer_jurisdictions: HashMap::new(),
            provider_items: HashMap::new(),
        }
    }

    pub fn clear(&mut self) {
        self.product_ids.clear();
        self.software_rows.clear();
        self.title_ids.clear();
        self.title_ids_by_source_item.clear();
        self.video_game_ids.clear();
        self.video_game_ids_by_product.clear();
        self.sellable_ids.clear();
        self.offer_ids.clear();
        self.offer_jurisdictions.clear();
        self.provider_items.clear();
    }

    pub fn db(&self) -> &Db {
        &self.db
    }

    fn validate_provider_key<'a>(provider_key: &'a str) -> Result<&'a str> {
        let normalized = provider_key.trim();
        if normalized.is_empty() {
            bail!("provider_key must be non-empty when ensuring video_game rows");
        }
        Ok(normalized)
    }

    fn build_laravel_video_game_metadata<'a>(
        title: &'a str,
        slug: Option<&'a str>,
        metadata: Option<&'a serde_json::Value>,
        provider_key: &'a str,
    ) -> Result<VideoGameProductMetadata<'a>> {
        let normalized_key = Self::validate_provider_key(provider_key)?;
        Ok(VideoGameProductMetadata {
            title,
            provider_key: Some(normalized_key),
            normalized_title: None,
            slug,
            metadata,
            ..Default::default()
        })
    }

    fn attach_provider_key<'a>(
        mut meta: VideoGameProductMetadata<'a>,
        provider_key: &'a str,
    ) -> Result<VideoGameProductMetadata<'a>> {
        if let Some(existing) = meta.provider_key {
            if existing.trim().is_empty() {
                bail!("provider_key supplied in metadata cannot be blank");
            }
            return Ok(meta);
        }

        let normalized_key = Self::validate_provider_key(provider_key)?;
        meta.provider_key = Some(normalized_key);
        Ok(meta)
    }

    pub async fn ensure_product_named(
        &mut self,
        kind: &str,
        slug: &str,
        name: &str,
    ) -> Result<i64> {
        if let Some(id) = self.product_ids.get(slug) {
            return Ok(*id);
        }
        let id = ensure_product_named(&self.db, kind, slug, name).await?;
        self.product_ids.insert(slug.to_string(), id);
        Ok(id)
    }

    pub async fn ensure_product_named_with_platform(
        &mut self,
        kind: &str,
        slug: &str,
        name: &str,
        platform: &str,
    ) -> Result<i64> {
        if let Some(id) = self.product_ids.get(slug) {
            // Even if cached, keep platform fresh when we learn it.
            let _ = sqlx::query(
                "UPDATE products SET platform=CASE WHEN platform='unknown' THEN $1 ELSE platform END WHERE id=$2",
            )
            .persistent(false)
            .bind(platform)
            .bind(*id)
            .execute(&self.db.pool)
            .await;
            return Ok(*id);
        }
        let id = ensure_product_named_with_platform(&self.db, kind, slug, name, platform).await?;
        self.product_ids.insert(slug.to_string(), id);
        Ok(id)
    }

    pub async fn ensure_software_row(&mut self, product_id: i64) -> Result<()> {
        if self.software_rows.insert(product_id) {
            ensure_software_row(&self.db, product_id).await?;
        }
        Ok(())
    }

    pub async fn ensure_video_game_title(
        &mut self,
        product_id: i64,
        title: &str,
        slug_opt: Option<&str>,
    ) -> Result<i64> {
        if let Some(id) = self.title_ids.get(&product_id) {
            return Ok(*id);
        }
        let id = ensure_video_game_title(&self.db, product_id, title, slug_opt).await?;
        self.title_ids.insert(product_id, id);
        Ok(id)
    }

    pub async fn ensure_video_game_title_for_source_item(
        &mut self,
        video_game_source_id: i64,
        provider_key: &str,
        product_id: Option<i64>,
        video_game_id: Option<i64>,
        raw_title: &str,
        normalized_title: Option<&str>,
        locale: Option<&str>,
        metadata: Option<Value>,
    ) -> Result<i64> {
        let key = (video_game_source_id, provider_key.to_string());
        if let Some(id) = self.title_ids_by_source_item.get(&key) {
            return Ok(*id);
        }
        let id = ensure_video_game_title_for_source_item(
            &self.db,
            video_game_source_id,
            provider_key,
            product_id,
            video_game_id,
            raw_title,
            normalized_title,
            locale,
            metadata,
        )
        .await?;
        self.title_ids_by_source_item.insert(key, id);
        Ok(id)
    }

    pub async fn ensure_video_game(
        &mut self,
        title_id: i64,
        platform_id: i64,
        edition: Option<&str>,
    ) -> Result<i64> {
        let key = (title_id, platform_id, edition.map(|s| s.to_string()));
        if let Some(id) = self.video_game_ids.get(&key) {
            return Ok(*id);
        }
        let id = ensure_video_game(&self.db, title_id, platform_id, edition).await?;
        self.video_game_ids.insert(key, id);
        Ok(id)
    }

    /// Laravel schema: ensure video_game row using product_id directly
    /// Accepts minimal parameters; use ensure_video_game_with_metadata for full enrichment
    /// `provider_key` identifies the upstream source powering rating normalization.
    pub async fn ensure_video_game_for_product_laravel(
        &mut self,
        product_id: i64,
        title: &str,
        slug: Option<&str>,
        metadata: Option<serde_json::Value>,
        provider_key: &str,
    ) -> Result<i64> {
        // Check cache first
        if let Some(id) = self.video_game_ids_by_product.get(&product_id) {
            return Ok(*id);
        }

        let meta = Self::build_laravel_video_game_metadata(
            title,
            slug,
            metadata.as_ref(),
            provider_key,
        )?;

        // Call enhanced function
        let id = ensure_video_game_for_product_enhanced(&self.db, product_id, &meta).await?;

        // Cache result
        self.video_game_ids_by_product.insert(product_id, id);
        Ok(id)
    }

    /// Laravel schema: ensure video_game row with full metadata enrichment
    /// Pass all available fields (rating, genres, developer, release_date, etc.)
    /// `provider_key` is required so downstream rating mappers can resolve aliases.
    pub async fn ensure_video_game_with_metadata(
        &mut self,
        product_id: i64,
        meta: VideoGameProductMetadata<'_>,
        provider_key: &str,
    ) -> Result<i64> {
        // Check cache first
        if let Some(id) = self.video_game_ids_by_product.get(&product_id) {
            return Ok(*id);
        }

        let meta = Self::attach_provider_key(meta, provider_key)?;

        // Call enhanced function with full metadata
        let id = ensure_video_game_for_product_enhanced(&self.db, product_id, &meta).await?;

        // Cache result
        self.video_game_ids_by_product.insert(product_id, id);
        Ok(id)
    }

    pub async fn ensure_sellable(&mut self, kind: &str, product_id: i64) -> Result<i64> {
        let key = (kind.to_string(), product_id);
        if let Some(id) = self.sellable_ids.get(&key) {
            return Ok(*id);
        }
        let id = ensure_sellable(&self.db, kind, product_id).await?;
        self.sellable_ids.insert(key, id);
        Ok(id)
    }

    pub async fn ensure_offer(
        &mut self,
        sellable_id: i64,
        retailer_id: i64,
        sku: Option<&str>,
    ) -> Result<i64> {
        let key = (sellable_id, retailer_id, sku.map(|s| s.to_string()));
        if let Some(id) = self.offer_ids.get(&key) {
            return Ok(*id);
        }
        let id = ensure_offer(&self.db, sellable_id, retailer_id, sku).await?;
        self.offer_ids.insert(key, id);
        Ok(id)
    }

    pub async fn ensure_offer_jurisdiction(
        &mut self,
        offer_id: i64,
        jurisdiction_id: i64,
        currency_id: i64,
    ) -> Result<i64> {
        let key = (offer_id, jurisdiction_id);
        if let Some(id) = self.offer_jurisdictions.get(&key) {
            return Ok(*id);
        }
        let id =
            ensure_offer_jurisdiction(&self.db, offer_id, jurisdiction_id, currency_id).await?;
        self.offer_jurisdictions.insert(key, id);
        Ok(id)
    }

    pub async fn ensure_provider_item(
        &mut self,
        provider_id: i64,
        external_id: &str,
        payload: Option<Value>,
        refresh_metadata: bool,
    ) -> Result<i64> {
        if !provider_items_present(&self.db).await.unwrap_or(false) {
            // php-compat / legacy DBs may not have provider_items at all.
            // We treat this feature as optional and return 0 to signal "unsupported".
            return Ok(0);
        }
        let key = (provider_id, external_id.to_string());
        if let Some(id) = self.provider_items.get(&key) {
            if refresh_metadata && *id != 0 {
                let _ = sqlx::query(
                    "UPDATE provider_items SET metadata=$1, updated_at=now() WHERE id=$2",
                )
                .persistent(false)
                .bind(&payload)
                .bind(id)
                .execute(&self.db.pool)
                .await?;
            }
            return Ok(*id);
        }
        let id = ensure_provider_item(&self.db, provider_id, external_id, payload.clone()).await?;
        self.provider_items.insert(key, id);
        Ok(id)
    }
}

#[cfg(test)]
mod provider_entity_cache_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn build_metadata_sets_provider_key_and_references_payload() {
        let payload = json!({"rating": 4.5});
        let meta = ProviderEntityCache::build_laravel_video_game_metadata(
            "Test Game",
            Some("test-game"),
            Some(&payload),
            "igdb",
        )
        .expect("metadata builder should succeed");

        assert_eq!(meta.title, "Test Game");
        assert_eq!(meta.provider_key, Some("igdb"));
        assert_eq!(meta.slug, Some("test-game"));
        assert!(matches!(meta.metadata, Some(m) if m == &payload));
    }

    #[test]
    fn build_metadata_rejects_blank_provider_key() {
        let err = ProviderEntityCache::build_laravel_video_game_metadata(
            "Test Game",
            None,
            None,
            "   ",
        )
        .unwrap_err();

        assert!(err
            .to_string()
            .contains("provider_key must be non-empty"));
    }

    #[test]
    fn attach_provider_key_preserves_existing_value() {
        let payload = json!({});
        let original = VideoGameProductMetadata {
            title: "Demo",
            provider_key: Some("steam"),
            metadata: Some(&payload),
            ..Default::default()
        };

        let meta = ProviderEntityCache::attach_provider_key(original, "igdb")
            .expect("existing provider_key should be honored");

        assert_eq!(meta.provider_key, Some("steam"));
    }

    #[test]
    fn attach_provider_key_sets_value_when_missing() {
        let payload = json!({});
        let original = VideoGameProductMetadata {
            title: "Demo",
            metadata: Some(&payload),
            ..Default::default()
        };

        let meta = ProviderEntityCache::attach_provider_key(original, "steam")
            .expect("provider_key should be injected");

        assert_eq!(meta.provider_key, Some("steam"));
    }

    #[test]
    fn attach_provider_key_rejects_blank_existing_value() {
        let payload = json!({});
        let original = VideoGameProductMetadata {
            title: "Demo",
            provider_key: Some(" "),
            metadata: Some(&payload),
            ..Default::default()
        };

        let err = ProviderEntityCache::attach_provider_key(original, "steam")
            .expect_err("blank provider_key should be rejected");
        assert!(err
            .to_string()
            .contains("provider_key supplied in metadata cannot be blank"));
    }
}

#[derive(Clone, Copy, Debug)]
enum VideoGameTitleNameColumn {
    Title,
    RawTitle,
}

impl VideoGameTitleNameColumn {
    fn as_str(self) -> &'static str {
        match self {
            VideoGameTitleNameColumn::Title => "title",
            VideoGameTitleNameColumn::RawTitle => "raw_title",
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct VideoGameTitleSchema {
    has_product_id: bool,
    has_video_game_id: bool,
    name_column: VideoGameTitleNameColumn,
    has_video_game_source_id: bool,
    has_vg_source_item_id: bool,
    has_locale: bool,
    has_version_hint: bool,
    has_metadata: bool,
    has_video_game_ids: bool,
}

static VIDEO_GAME_TITLE_SCHEMA: OnceCell<VideoGameTitleSchema> = OnceCell::const_new();

#[derive(Clone, Copy, Debug)]
enum SoftwareIdColumn {
    ProductId,
    VideoGameId,
}

static SOFTWARE_ID_COLUMN: OnceCell<SoftwareIdColumn> = OnceCell::const_new();
#[derive(Clone, Copy, Debug)]
struct SellableSchema {
    has_product_id: bool,
    has_software_title_id: bool,
    has_console_id: bool,
}

static SELLABLE_SCHEMA: OnceCell<SellableSchema> = OnceCell::const_new();

async fn get_video_game_title_schema(db: &Db) -> Result<VideoGameTitleSchema> {
    let schema = VIDEO_GAME_TITLE_SCHEMA
        .get_or_try_init(|| async { detect_video_game_title_schema(db).await })
        .await?;
    Ok(*schema)
}

async fn detect_video_game_title_schema(db: &Db) -> Result<VideoGameTitleSchema> {
    let has_video_game_id = video_game_titles_column_exists(db, "video_game_id").await?;
    let has_product_id = video_game_titles_column_exists(db, "product_id").await?;
    let has_raw_title = video_game_titles_column_exists(db, "raw_title").await?;
    let has_title = video_game_titles_column_exists(db, "title").await?;
    let name_column = if has_raw_title {
        VideoGameTitleNameColumn::RawTitle
    } else if has_title {
        VideoGameTitleNameColumn::Title
    } else {
        anyhow::bail!("video_game_titles missing both raw_title and title columns");
    };

    let has_video_game_source_id =
        video_game_titles_column_exists(db, "video_game_source_id").await?;
    let has_vg_source_item_id =
        video_game_titles_column_exists(db, "vg_source_item_id").await?;
    let has_locale = video_game_titles_column_exists(db, "locale").await?;
    let has_version_hint = video_game_titles_column_exists(db, "version_hint").await?;
    let has_metadata = video_game_titles_column_exists(db, "metadata").await?;
    let has_video_game_ids = video_game_titles_column_exists(db, "video_game_ids").await?;

    // Accept either legacy linkage via product_id/video_game_id OR the newer source linkage.
    if !has_video_game_id && !has_product_id && !has_video_game_source_id {
        anyhow::bail!(
            "video_game_titles schema unsupported (expected product_id/video_game_id or video_game_source_id)"
        );
    }

    Ok(VideoGameTitleSchema {
        has_product_id,
        has_video_game_id,
        name_column,
        has_video_game_source_id,
        has_vg_source_item_id,
        has_locale,
        has_version_hint,
        has_metadata,
        has_video_game_ids,
    })
}

async fn video_game_titles_column_exists(db: &Db, column: &str) -> Result<bool> {
    table_column_exists(db, "video_game_titles", column).await
}

async fn select_title_id_by_column(db: &Db, column: &str, product_id: i64) -> Result<Option<i64>> {
    let sql = format!("SELECT id FROM video_game_titles WHERE {}=$1", column);
    let rec = sqlx::query(&sql)
        .persistent(false)
        .bind(product_id)
        .fetch_optional(&db.pool)
        .await?;
    Ok(rec.map(|row| row.get::<i64, _>("id")))
}

fn build_video_game_title_insert_sql(schema: VideoGameTitleSchema) -> String {
    let mut columns: Vec<&str> = Vec::new();
    if schema.has_video_game_id {
        columns.push("video_game_id");
    }
    if schema.has_video_game_ids {
        columns.push("video_game_ids");
    }
    if schema.has_product_id {
        columns.push("product_id");
    }
    columns.push(schema.name_column.as_str());
    columns.push("normalized_title");

    let values: Vec<String> = (1..=columns.len()).map(|idx| format!("${}", idx)).collect();

    // Migration 0560 removed the unique constraint on video_game_id.
    // We cannot use ON CONFLICT (video_game_id) anymore.
    let on_conflict_clause = String::new();

    format!(
        "INSERT INTO video_game_titles ({}) VALUES ({}){} RETURNING id",
        columns.join(", "),
        values.join(", "),
        on_conflict_clause
    )
}

#[instrument(skip(db))]
pub async fn ensure_video_game_source(
    db: &Db,
    provider_key: &str,
    display_name: &str,
) -> Result<i64> {
    // Laravel source registry table (video_game_sources) may or may not exist depending on the target DB.
    // Additionally, column names vary across iterations (provider_key vs slug vs key; display_name vs name).
    if !table_exists(db, "video_game_sources")
        .await
        .unwrap_or(false)
    {
        warn!("video_game_sources table not found in target DB. Returning 0.");
        return Ok(0);
    }

    // Prefer stable key columns, in order.
    let has_provider_key = table_column_exists(db, "video_game_sources", "provider_key").await?;
    let has_slug = table_column_exists(db, "video_game_sources", "slug").await?;
    let has_key = table_column_exists(db, "video_game_sources", "key").await?;
    let has_source_key = table_column_exists(db, "video_game_sources", "source_key").await?;

    let key_col = if has_provider_key {
        "provider_key"
    } else if has_slug {
        "slug"
    } else if has_key {
        "key"
    } else if has_source_key {
        "source_key"
    } else {
        anyhow::bail!(
            "video_game_sources missing a usable key column (provider_key/slug/key/source_key)"
        );
    };

    // Prefer a human-readable name column.
    let has_display_name = table_column_exists(db, "video_game_sources", "display_name").await?;
    let has_name = table_column_exists(db, "video_game_sources", "name").await?;
    let name_col = if has_display_name {
        Some("display_name")
    } else if has_name {
        Some("name")
    } else {
        None
    };

    let normalized_provider_key = normalize_provider_key(provider_key);
    let display_trimmed = display_name.trim();
    let slug_value_owned = if has_slug {
        Some(normalize_source_slug_value(
            normalized_provider_key.as_str(),
            display_trimmed,
        ))
    } else {
        None
    };
    let key_value = if key_col == "slug" {
        slug_value_owned
            .as_deref()
            .unwrap_or_else(|| normalized_provider_key.as_str())
    } else {
        normalized_provider_key.as_str()
    };

    // 1) Try select existing by key
    let sel_sql = format!("SELECT id FROM video_game_sources WHERE {key_col} = $1 LIMIT 1");
    if let Some(rec) = sqlx::query(&sel_sql)
        .persistent(false)
        .bind(key_value)
        .fetch_optional(&db.pool)
        .await?
    {
        let id: i64 = rec.get("id");
        // Best-effort refresh name/display_name if the column exists.
        if let Some(nc) = name_col {
            let upd_sql = format!("UPDATE video_game_sources SET {nc} = $1 WHERE id = $2");
            let _ = sqlx::query(&upd_sql)
                .persistent(false)
                .bind(display_trimmed)
                .bind(id)
                .execute(&db.pool)
                .await;
        }
        return Ok(id);
    }

    // 2) Insert new row (best-effort). Avoid ON CONFLICT targets because unique constraints differ between DBs.
    let mut cols: Vec<&str> = vec![key_col];
    let mut binds: Vec<&str> = vec![key_value];
    if let Some(nc) = name_col {
        cols.push(nc);
        binds.push(display_trimmed);
    }
    if has_slug && key_col != "slug" {
        if let Some(slug_val) = slug_value_owned.as_deref() {
            cols.push("slug");
            binds.push(slug_val);
        }
    }
    let placeholders: Vec<String> = (1..=cols.len()).map(|i| format!("${i}")).collect();
    let ins_sql = format!(
        "INSERT INTO video_game_sources ({cols}) VALUES ({vals}) RETURNING id",
        cols = cols.join(", "),
        vals = placeholders.join(", ")
    );

    let mut q = sqlx::query(&ins_sql).persistent(false);
    for b in binds {
        q = q.bind(b);
    }
    match q.fetch_one(&db.pool).await {
        Ok(rec) => Ok(rec.get("id")),
        Err(e) => {
            // If we raced with another insert and a unique constraint exists, re-select.
            let is_unique = matches!(&e, sqlx::Error::Database(db_err) if db_err.code().as_deref() == Some("23505"));
            if is_unique {
                let rec = sqlx::query(&sel_sql)
                    .persistent(false)
                    .bind(provider_key)
                    .fetch_one(&db.pool)
                    .await?;
                Ok(rec.get("id"))
            } else {
                Err(e.into())
            }
        }
    }
}

#[instrument(skip(db))]
pub async fn ensure_video_game_title_for_source_item(
    db: &Db,
    video_game_source_id: i64,
    provider_key: &str,
    product_id: Option<i64>,
    video_game_id: Option<i64>,
    raw_title: &str,
    normalized_title: Option<&str>,
    locale: Option<&str>,
    metadata: Option<Value>,
) -> Result<i64> {
    use crate::database_ops::ensure_video_game_for_product_enhanced::{
        ensure_video_game_for_product_enhanced, VideoGameProductMetadata,
    };

    async fn resync_video_game_titles_id_sequence(db: &Db) -> Result<()> {
        sqlx::query(
            "SELECT setval(pg_get_serial_sequence('video_game_titles','id'), (SELECT COALESCE(MAX(id),0)+1 FROM video_game_titles), false)",
        )
        .persistent(false)
        .execute(&db.pool)
        .await?;
        Ok(())
    }

    fn is_video_game_titles_pkey_violation(e: &sqlx::Error) -> bool {
        match e {
            sqlx::Error::Database(db_err) => {
                db_err.code().as_deref() == Some("23505")
                    && db_err.constraint() == Some("video_game_titles_pkey")
            }
            _ => false,
        }
    }

    let schema = get_video_game_title_schema(db).await?;
    if !(schema.has_video_game_source_id && schema.has_vg_source_item_id) {
        anyhow::bail!(
            "video_game_titles does not support (video_game_source_id, vg_source_item_id) linkage in this DB"
        );
    }

    // CRITICAL FIX: Verify product exists before attempting FK reference (if product_id is provided)
    // This prevents FK violations when product creation and title creation are not in the same transaction
    if let Some(pid) = product_id {
        let product_exists = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM products WHERE id = $1)"
        )
        .bind(pid)
        .fetch_one(&db.pool)
        .await?;

        if !product_exists {
            return Err(anyhow::anyhow!(
                "Product {} does not exist - cannot create video_game_title. \
                This may indicate a race condition or stale cache in import/ingestion. \
                Title: {}, Provider: {}",
                pid, raw_title, provider_key
            ));
        }
    }

    // If the schema also includes legacy linkage columns, they may be required (NOT NULL) in some deployments.
    // When possible, we infer video_game_id from product_id using the Laravel-compatible helper.
    let mut derived_video_game_id = video_game_id;
    if schema.has_video_game_id && derived_video_game_id.is_none() {
        if let Some(pid) = product_id {
            let video_games_has_product_id = table_column_exists(db, "video_games", "product_id")
                .await
                .unwrap_or(false);
            let video_games_has_title = table_column_exists(db, "video_games", "title")
                .await
                .unwrap_or(false);

            if video_games_has_product_id && video_games_has_title {
                let meta = VideoGameProductMetadata {
                    title: raw_title,
                    provider_key: Some(provider_key),
                    normalized_title: normalized_title,
                    slug: normalized_title,
                    metadata: metadata.as_ref(),
                    ..Default::default()
                };
                derived_video_game_id =
                    Some(ensure_video_game_for_product_enhanced(db, pid, &meta).await?);
            }
        }
    }

    if schema.has_video_game_id && derived_video_game_id.is_none() {
        warn!(
            "video_game_titles.video_game_id available in schema but no canonical video_game row could be derived; inserting NULL and expecting downstream registry/backfill to populate"
        );
    }
    if schema.has_product_id && product_id.is_none() {
        anyhow::bail!(
            "video_game_titles requires product_id; provide product_id when calling ensure_video_game_title_for_source_item"
        );
    }

    let normalized_final = normalized_title
        .map(|s| s.to_string())
        .unwrap_or_else(|| local_normalize_title(raw_title));

    // 1) Try select existing. We avoid ON CONFLICT because many DBs don't have a unique index on
    // (video_game_source_id, vg_source_item_id) yet.
    if let Some(row) = sqlx::query(
        "SELECT id FROM video_game_titles WHERE video_game_source_id=$1 AND vg_source_item_id=$2 ORDER BY id DESC LIMIT 1",
    )
    .persistent(false)
    .bind(video_game_source_id)
    .bind(provider_key)
    .fetch_optional(&db.pool)
    .await?
    {
        let id: i64 = row.get("id");

        // Best-effort refresh fields.
        let mut set_parts: Vec<String> = vec![
            format!("{} = $1", schema.name_column.as_str()),
            "normalized_title = $2".to_string(),
        ];
        let mut bind_idx: i32 = 3;

        if schema.has_locale {
            set_parts.push(format!("locale = COALESCE(${bind_idx}, video_game_titles.locale)"));
            bind_idx += 1;
        }
        if schema.has_metadata {
            set_parts.push(format!(
                "metadata = COALESCE(${bind_idx}, video_game_titles.metadata)"
            ));
            bind_idx += 1;
        }
        if schema.has_version_hint {
            set_parts.push(format!(
                "version_hint = COALESCE(${bind_idx}, video_game_titles.version_hint)"
            ));
            bind_idx += 1;
        }
        if schema.has_video_game_id {
            // Only overwrite when caller provided a value.
            set_parts.push(format!(
                "video_game_id = COALESCE(${bind_idx}, video_game_titles.video_game_id)"
            ));
            bind_idx += 1;
        }
        if schema.has_product_id {
            set_parts.push(format!(
                "product_id = COALESCE(${bind_idx}, video_game_titles.product_id)"
            ));
            bind_idx += 1;
        }
        if schema.has_video_game_ids {
            // Append to array if supported
            set_parts.push(format!("video_game_ids = (
                SELECT jsonb_agg(DISTINCT x)
                FROM jsonb_array_elements(COALESCE(video_game_titles.video_game_ids, '[]'::jsonb) || ${bind_idx}::jsonb) t(x)
            )"));
            bind_idx += 1;
        }

        let sql = format!(
            "UPDATE video_game_titles SET {} WHERE id = ${}",
            set_parts.join(", "),
            bind_idx
        );

        let mut q = sqlx::query(&sql).persistent(false);
        q = q.bind(raw_title);
        q = q.bind(&normalized_final);
        if schema.has_locale {
            q = q.bind(locale);
        }
        if schema.has_metadata {
            q = q.bind(metadata.clone());
        }
        if schema.has_version_hint {
            q = q.bind(Option::<String>::None);
        }
        if schema.has_video_game_id {
            q = q.bind(derived_video_game_id);
        }
        if schema.has_product_id {
            q = q.bind(product_id);
        }
        if schema.has_video_game_ids {
            // Bind as single-element array to be appended
            let arr = match derived_video_game_id {
                Some(vid) => serde_json::json!([vid]),
                None => serde_json::json!([]),
            };
            q = q.bind(arr);
        }
        q = q.bind(id);
        let _ = q.execute(&db.pool).await;

        return Ok(id);
    }

    // 1.5) Check if video_game_id is already claimed (unique constraint).
    // If the schema enforces 1:1 video_game <-> video_game_title, we cannot create a second title
    // for the same game. However, if we have video_game_ids (1-to-many), we should allow this
    // or handle it by not setting the singular column if it conflicts.
    // For now, we skip this check if we have the array column, assuming the array is the source of truth.
    if schema.has_video_game_id && !schema.has_video_game_ids {
        if let Some(vid) = derived_video_game_id {
            if let Some(existing_id) = select_title_id_by_column(db, "video_game_id", vid).await? {
                warn!(
                    video_game_id = vid,
                    existing_title_id = existing_id,
                    new_source_id = video_game_source_id,
                    "video_game_titles unique constraint on video_game_id prevents creating new title for source; returning existing title"
                );
                return Ok(existing_id);
            }
        }
    }

    // 2) Insert new row.
    let mut columns: Vec<&str> = Vec::new();
    if schema.has_video_game_id {
        columns.push("video_game_id");
    }
    if schema.has_product_id {
        columns.push("product_id");
    }
    if schema.has_video_game_ids {
        columns.push("video_game_ids");
    }
    columns.extend([
        "video_game_source_id",
        "vg_source_item_id",
        schema.name_column.as_str(),
        "normalized_title",
    ]);
    if schema.has_locale {
        columns.push("locale");
    }
    if schema.has_metadata {
        columns.push("metadata");
    }
    if schema.has_version_hint {
        columns.push("version_hint");
    }

    let values: Vec<String> = (1..=columns.len()).map(|idx| format!("${}", idx)).collect();

    // Migration 0560 removed the unique constraint on video_game_id.
    // We cannot use ON CONFLICT (video_game_id) anymore.
    let on_conflict_clause = String::new();

    let insert_sql = format!(
        "INSERT INTO video_game_titles ({cols}) VALUES ({vals}){on_conflict} RETURNING id",
        cols = columns.join(", "),
        vals = values.join(", "),
        on_conflict = on_conflict_clause
    );

    let exec_insert = || {
        let mut q = sqlx::query(&insert_sql).persistent(false);
        if schema.has_video_game_id {
            q = q.bind(derived_video_game_id);
        }
        if schema.has_product_id {
            q = q.bind(product_id);
        }
        if schema.has_video_game_ids {
            let arr = match derived_video_game_id {
                Some(vid) => serde_json::json!([vid]),
                None => serde_json::json!([]),
            };
            q = q.bind(arr);
        }
        q = q.bind(video_game_source_id);
        q = q.bind(provider_key);
        q = q.bind(raw_title);
        q = q.bind(&normalized_final);
        if schema.has_locale {
            q = q.bind(locale);
        }
        if schema.has_metadata {
            q = q.bind(metadata.clone());
        }
        if schema.has_version_hint {
            q = q.bind(Option::<String>::None);
        }
        q
    };

    match exec_insert().fetch_one(&db.pool).await {
        Ok(rec) => Ok(rec.get("id")),
        Err(e) if is_video_game_titles_pkey_violation(&e) => {
            resync_video_game_titles_id_sequence(db).await?;
            let rec = exec_insert().fetch_one(&db.pool).await?;
            Ok(rec.get("id"))
        }
        Err(e) => Err(e.into()),
    }
}

async fn get_software_id_column(db: &Db) -> Result<SoftwareIdColumn> {
    let column = SOFTWARE_ID_COLUMN
        .get_or_try_init(|| async { detect_software_id_column(db).await })
        .await?;
    Ok(*column)
}

async fn detect_software_id_column(db: &Db) -> Result<SoftwareIdColumn> {
    if table_column_exists(db, "software", "video_game_id").await? {
        return Ok(SoftwareIdColumn::VideoGameId);
    }
    if table_column_exists(db, "software", "product_id").await? {
        return Ok(SoftwareIdColumn::ProductId);
    }
    anyhow::bail!("software table missing expected product/video_game id columns")
}

pub async fn table_column_exists(db: &Db, table: &str, column: &str) -> Result<bool> {
    // Match the table that the backend will resolve for an unqualified identifier.
    // This avoids taking a "modern schema" branch when the resolved table is a
    // legacy PHP/Laravel table missing that column (e.g., `video_games.title_id`).
    let exists: bool = sqlx::query_scalar(
        r#"SELECT EXISTS (
            SELECT 1
            FROM pg_attribute a
            WHERE a.attrelid = to_regclass($1)
              AND a.attname = $2
              AND a.attnum > 0
              AND NOT a.attisdropped
        )"#,
    )
    .persistent(false)
    .bind(table)
    .bind(column)
    .fetch_one(&db.pool)
    .await?;
    Ok(exists)
}

async fn table_column_udt_name(db: &Db, table: &str, column: &str) -> Result<Option<String>> {
    // Similar to `table_column_exists`, resolve against the visible table.
    // For enums, `pg_type.typname` is the stable identifier we care about.
    let udt = sqlx::query_scalar::<_, String>(
        r#"SELECT t.typname
                 FROM pg_attribute a
                 JOIN pg_type t ON t.oid = a.atttypid
                 WHERE a.attrelid = to_regclass($1)
                     AND a.attname = $2
                     AND a.attnum > 0
                     AND NOT a.attisdropped
                 LIMIT 1"#,
    )
    .persistent(false)
    .bind(table)
    .bind(column)
    .fetch_optional(&db.pool)
    .await?;
    Ok(udt)
}

async fn get_sellable_schema(db: &Db) -> Result<SellableSchema> {
    let schema = SELLABLE_SCHEMA
        .get_or_try_init(|| async { detect_sellable_schema(db).await })
        .await?;
    Ok(*schema)
}

async fn detect_sellable_schema(db: &Db) -> Result<SellableSchema> {
    Ok(SellableSchema {
        has_product_id: table_column_exists(db, "sellables", "product_id").await?,
        has_software_title_id: table_column_exists(db, "sellables", "software_title_id").await?,
        has_console_id: table_column_exists(db, "sellables", "console_id").await?,
    })
}

async fn fetch_title_id_for_product(db: &Db, product_id: i64) -> Result<Option<i64>> {
    let schema = get_video_game_title_schema(db).await?;
    let mut clauses: Vec<&str> = Vec::new();
    if schema.has_video_game_id {
        clauses.push("video_game_id = $1");
    }
    if schema.has_product_id {
        clauses.push("product_id = $1");
    }
    if clauses.is_empty() {
        return Ok(None);
    }
    let sql = format!(
        "SELECT id FROM video_game_titles WHERE {} ORDER BY updated_at DESC LIMIT 1",
        clauses.join(" OR ")
    );
    let rec = sqlx::query(&sql)
        .persistent(false)
        .bind(product_id)
        .fetch_optional(&db.pool)
        .await?;
    Ok(rec.map(|row| row.get::<i64, _>("id")))
}

async fn fetch_product_id_for_title(db: &Db, title_id: i64) -> Result<Option<i64>> {
    // Best-effort mapping used primarily for php-compat mode where we synthesize sellable ids.
    // Prefer `product_id` if present, else fall back to legacy `video_game_id`.
    let has_titles = table_exists(db, "video_game_titles").await.unwrap_or(false);
    if !has_titles {
        return Ok(None);
    }

    let has_product_id = video_game_titles_column_exists(db, "product_id")
        .await
        .unwrap_or(false);
    let has_video_game_id = video_game_titles_column_exists(db, "video_game_id")
        .await
        .unwrap_or(false);

    let sql = if has_product_id && has_video_game_id {
        "SELECT COALESCE(product_id, video_game_id) FROM video_game_titles WHERE id=$1"
    } else if has_product_id {
        "SELECT product_id FROM video_game_titles WHERE id=$1"
    } else if has_video_game_id {
        "SELECT video_game_id FROM video_game_titles WHERE id=$1"
    } else {
        return Ok(None);
    };

    let pid = sqlx::query_scalar::<_, i64>(sql)
        .persistent(false)
        .bind(title_id)
        .fetch_optional(&db.pool)
        .await?;
    Ok(pid)
}

// --------- Ingest run observability helpers ---------

#[instrument(skip(db, meta))]
pub async fn ingest_run_start(
    db: &Db,
    provider_id: i64,
    region_code: Option<&str>,
    meta: Option<Value>,
) -> Result<i64> {
    // If the tracking table doesn't exist (e.g., legacy PHP schema), skip run bookkeeping gracefully.
    let table_exists: bool = sqlx
        ::query_scalar::<_, bool>(
            "SELECT TRUE FROM information_schema.tables WHERE table_schema='public' AND table_name='provider_ingest_runs' LIMIT 1"
        )
        .persistent(false)
        .fetch_optional(&db.pool)
        .await?
        .unwrap_or(false);
    if !table_exists {
        return Ok(0);
    }
    // No runtime schema changes: detect optional columns and only write what exists.
    let region_code_exists: bool = sqlx
        ::query_scalar::<_, Option<String>>(
            "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='provider_ingest_runs' AND column_name='region_code'"
        )
        .persistent(false)
        .fetch_optional(&db.pool).await?
        .is_some();
    // Detect optional 'meta' column presence
    let meta_exists: bool = sqlx
        ::query_scalar::<_, Option<String>>(
            "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='provider_ingest_runs' AND column_name='meta'"
        )
        .persistent(false)
        .fetch_optional(&db.pool).await?
        .is_some();
    match (region_code_exists, meta_exists) {
        (true, true) => {
            sqlx
                ::query(
                    "INSERT INTO public.provider_ingest_runs (provider_id, region_code, meta) VALUES ($1,$2,$3)"
                )
                .persistent(false)
                .bind(provider_id)
                .bind(region_code)
                .bind(meta)
                .execute(&db.pool).await?;
        }
        (true, false) => {
            sqlx::query(
                "INSERT INTO public.provider_ingest_runs (provider_id, region_code) VALUES ($1,$2)",
            )
            .persistent(false)
            .bind(provider_id)
            .bind(region_code)
            .execute(&db.pool)
            .await?;
        }
        (false, true) => {
            sqlx::query(
                "INSERT INTO public.provider_ingest_runs (provider_id, meta) VALUES ($1,$2)",
            )
            .persistent(false)
            .bind(provider_id)
            .bind(meta)
            .execute(&db.pool)
            .await?;
        }
        (false, false) => {
            sqlx::query("INSERT INTO public.provider_ingest_runs (provider_id) VALUES ($1)")
                .persistent(false)
                .bind(provider_id)
                .execute(&db.pool)
                .await?;
        }
    }
    // Fallback: fetch latest run id for this provider
    let maybe = sqlx
        ::query(
            "SELECT id FROM public.provider_ingest_runs WHERE provider_id=$1 ORDER BY started_at DESC LIMIT 1"
        )
        .persistent(false)
        .bind(provider_id)
        .fetch_optional(&db.pool).await?;
    Ok(maybe.map(|r| r.get::<i64, _>("id")).unwrap_or(0))
}

#[instrument(skip(db, errors))]
pub async fn ingest_run_finish(
    db: &Db,
    run_id: i64,
    status: &str,
    items_processed: i64,
    prices_written: i64,
    errors: Option<Value>,
) -> Result<()> {
    if run_id == 0 {
        // No-op if run tracking table/row not available
        return Ok(());
    }

    // Mirror ingest_run_start: schemas vary, so only update columns that exist.
    if !table_exists(db, "provider_ingest_runs")
        .await
        .unwrap_or(false)
    {
        return Ok(());
    }

    let has_ended_at = table_column_exists(db, "provider_ingest_runs", "ended_at")
        .await
        .unwrap_or(false);
    let has_status = table_column_exists(db, "provider_ingest_runs", "status")
        .await
        .unwrap_or(false);
    let has_items_processed = table_column_exists(db, "provider_ingest_runs", "items_processed")
        .await
        .unwrap_or(false);
    let has_prices_written = table_column_exists(db, "provider_ingest_runs", "prices_written")
        .await
        .unwrap_or(false);
    let has_errors = table_column_exists(db, "provider_ingest_runs", "errors")
        .await
        .unwrap_or(false);

    use sqlx::QueryBuilder;
    let mut qb = QueryBuilder::new("UPDATE public.provider_ingest_runs SET ");
    let mut sep = qb.separated(", ");
    let mut wrote_any = false;
    if has_ended_at {
        sep.push("ended_at=now()");
        wrote_any = true;
    }
    if has_status {
        sep.push("status=").push_bind(status);
        wrote_any = true;
    }
    if has_items_processed {
        sep.push("items_processed=").push_bind(items_processed);
        wrote_any = true;
    }
    if has_prices_written {
        sep.push("prices_written=").push_bind(prices_written);
        wrote_any = true;
    }
    if has_errors {
        // Preserve prior errors if the caller doesn't provide any.
        sep.push("errors=COALESCE(")
            .push_bind(errors)
            .push(", errors)");
        wrote_any = true;
    }

    if !wrote_any {
        return Ok(());
    }

    qb.push(" WHERE id=").push_bind(run_id);
    qb.build().persistent(false).execute(&db.pool).await?;
    Ok(())
}

// --------- Provider toplists (ranked snapshots for Spotlight) ---------

/// Upsert a provider toplist snapshot and return its id.
///
/// This is intentionally schema-optional: in legacy DBs without toplist tables we return 0.
#[instrument(skip(db, meta))]
pub async fn upsert_provider_toplist(
    db: &Db,
    provider_key: &str,
    slug: &str,
    list_type: &str,
    period_start: Option<&str>,
    period_end: Option<&str>,
    genre_slug: Option<&str>,
    meta: Option<Value>,
) -> Result<i64> {
    if !provider_toplists_present(db).await.unwrap_or(false) {
        return Ok(0);
    }

    let rec = sqlx::query(
        "INSERT INTO provider_toplists (provider_key, slug, list_type, period_start, period_end, genre_slug, meta, snapshot_at, created_at, updated_at)\n         VALUES ($1,$2,$3,$4,$5,$6,$7, now(), now(), now())\n         ON CONFLICT (slug) DO UPDATE\n           SET provider_key = EXCLUDED.provider_key,\n               list_type = EXCLUDED.list_type,\n               period_start = EXCLUDED.period_start,\n               period_end = EXCLUDED.period_end,\n               genre_slug = EXCLUDED.genre_slug,\n               meta = COALESCE(EXCLUDED.meta, provider_toplists.meta),\n               snapshot_at = now(),\n               updated_at = now()\n         RETURNING id",
    )
    .persistent(false)
    .bind(provider_key)
    .bind(slug)
    .bind(list_type)
    .bind(period_start)
    .bind(period_end)
    .bind(genre_slug)
    .bind(meta)
    .fetch_one(&db.pool)
    .await?;

    Ok(rec.get::<i64, _>("id"))
}

/// Replace all items for a toplist id with the provided ranked product ids.
///
/// This uses a delete+insert strategy because lists are tiny (<= 50-100), and it keeps
/// the order semantics obvious.
#[instrument(skip(db, ranked_product_ids))]
pub async fn replace_provider_toplist_items(
    db: &Db,
    provider_toplist_id: i64,
    ranked_product_ids: &[(u32, i64)],
) -> Result<()> {
    if provider_toplist_id == 0 {
        return Ok(());
    }
    if !provider_toplist_items_present(db).await.unwrap_or(false) {
        return Ok(());
    }
    if ranked_product_ids.is_empty() {
        // Still clear items for this toplist so old snapshots don't linger.
        sqlx::query("DELETE FROM provider_toplist_items WHERE provider_toplist_id = $1")
            .persistent(false)
            .bind(provider_toplist_id)
            .execute(&db.pool)
            .await?;
        return Ok(());
    }

    let mut tx = db.pool.begin().await?;
    sqlx::query("DELETE FROM provider_toplist_items WHERE provider_toplist_id = $1")
        .persistent(false)
        .bind(provider_toplist_id)
        .execute(&mut *tx)
        .await?;

    use sqlx::QueryBuilder;
    let mut qb = QueryBuilder::new(
        "INSERT INTO provider_toplist_items (provider_toplist_id, rank, product_id, created_at, updated_at) ",
    );
    qb.push_values(ranked_product_ids, |mut b, (rank, product_id)| {
        b.push_bind(provider_toplist_id);
        b.push_bind(*rank as i32);
        b.push_bind(*product_id);
        b.push("now()");
        b.push("now()");
    });
    qb.build().persistent(false).execute(&mut *tx).await?;

    tx.commit().await?;
    Ok(())
}

#[instrument(skip(db))]
pub async fn sample_ingest_flow(db: &Db) -> Result<IngestResult> {
    // Demonstration: create minimal entities then ingest one price
    let _provider_id = ensure_provider(db, "steam", "retailer_api", Some("steam")).await?;
    let retailer_id = ensure_retailer(db, "Steam Store", Some("steam")).await?;
    let product_id = ensure_product(db, "software", Some("sample-game")).await?;
    let sellable_id = ensure_sellable(db, "software", product_id).await?;
    let _offer_id = ensure_offer(db, sellable_id, retailer_id, None).await?;

    // Ensure a basic USD / US national jurisdiction
    let usd_id = ensure_currency(db, "USD", "United States Dollar", 2).await?;
    let us_id = ensure_country(db, "US", "United States", usd_id).await?;

    // For simplicity, ensure jurisdiction and return empty ingest result placeholder
    let _jurisdiction_id = ensure_national_jurisdiction(db, us_id).await?;
    Ok(IngestResult {
        offer_jurisdiction_ids: Vec::new(),
        current_updates: Vec::new(),
    })
}

// --------- Provider items and metadata ---------

#[instrument(skip(db, metadata))]
pub async fn ensure_provider_item(
    db: &Db,
    provider_id: i64,
    external_id: &str,
    metadata: Option<Value>,
) -> Result<i64> {
    if !provider_items_present(db).await.unwrap_or(false) {
        return Ok(0);
    }

    if let Some(rec) =
        sqlx::query("SELECT id FROM provider_items WHERE provider_id=$1 AND external_id=$2")
            .persistent(false)
            .bind(provider_id)
            .bind(external_id)
            .fetch_optional(&db.pool)
            .await?
    {
        let existing_id: i64 = rec.get("id");
        // If metadata provided, refresh it (idempotent update)
        if metadata.is_some() {
            let _ =
                sqlx::query("UPDATE provider_items SET metadata=$1, updated_at=now() WHERE id=$2")
                    .persistent(false)
                    .bind(&metadata)
                    .bind(existing_id)
                    .execute(&db.pool)
                    .await?;
        }
        return Ok(existing_id);
    }
    let rec = sqlx
        ::query(
            "INSERT INTO provider_items (provider_id, external_id, metadata) VALUES ($1,$2,$3) RETURNING id"
        )
        .persistent(false)
        .bind(provider_id)
        .bind(external_id)
        .bind(&metadata)
        .fetch_one(&db.pool).await?;
    Ok(rec.get("id"))
}

// --------- Provider media links ---------

#[instrument(skip(db))]
pub async fn ensure_vg_source_media_link(
    db: &Db,
    video_game_source_id: i64,
    url: &str,
) -> Result<i64> {
    if video_game_source_id == 0 {
        return Ok(0);
    }
    if !provider_media_links_present(db).await.unwrap_or(false) {
        return Ok(0);
    }
    if let Some(rec) =
        sqlx::query("SELECT id FROM vg_source_media_links WHERE video_game_source_id=$1 AND url=$2")
            .persistent(false)
            .bind(video_game_source_id)
            .bind(url)
            .fetch_optional(&db.pool)
            .await?
    {
        return Ok(rec.get("id"));
    }
    let inserted = sqlx::query(
        "INSERT INTO vg_source_media_links (video_game_source_id, url) VALUES ($1,$2) RETURNING id",
    )
    .persistent(false)
    .bind(video_game_source_id)
    .bind(url)
    .fetch_one(&db.pool)
    .await?;
    Ok(inserted.get("id"))
}

/// Batch upsert variant to reduce per-row round trips.
/// Accepts a slice of tuples representing game media rows.
/// Each tuple: (video_game_id, source, external_id, media_type, url, provider_data_json)
/// Performs ON CONFLICT upsert mirroring single-row behavior.
#[instrument(skip(db, rows))]
pub async fn upsert_game_media_batch(
    db: &Db,
    rows: &[(i64, &str, &str, &str, &str, &Value)],
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    // Batch upsert is optional. If the table doesn't exist in this deployment, just no-op.
    if !table_exists(db, "game_media").await.unwrap_or(false) {
        return Ok(());
    }

    use sqlx::types::Json;
    use sqlx::QueryBuilder;

    const MAX_CHUNK_SIZE: usize = 64;

    let supports_psstore = media_source_supports_psstore(db).await.unwrap_or(false);
    let supports_background = media_type_supports_background(db).await.unwrap_or(false);

    // Legacy schema: no `source` column, different uniqueness constraints. Delegate to single-row
    // helper which already knows how to write legacy rows.
    if !game_media_has_source_col(db).await.unwrap_or(false) {
        for (video_game_id, source, external_id, media_type, url, pdata) in rows.iter().copied() {
            upsert_game_media(
                db,
                video_game_id,
                source,
                external_id,
                media_type,
                url,
                (*pdata).clone(),
            )
            .await?;
        }
        return Ok(());
    }

    // Even in the modern schema, `source` and `media_type` columns may not be enum-typed.
    // If they are plain text, we must not cast.
    let source_is_enum = game_media_source_is_enum(db).await.unwrap_or(false);
    let media_type_is_enum = game_media_media_type_is_enum(db).await.unwrap_or(false);

    // Deduplicate within the batch to avoid hitting the same target row twice in a single
    // INSERT ... ON CONFLICT statement, which PostgreSQL rejects.
    //
    // IMPORTANT: We dedupe on the *DB-normalized* source (e.g. psn/ps-store -> psn on older DBs,
    // and psn/ps-store -> psstore on newer DBs) so alias variants within the same batch don't
    // still collide at write-time.
    let mut deduped: Vec<(
        i64,
        std::borrow::Cow<'_, str>,
        &str,
        std::borrow::Cow<'_, str>,
        &str,
        &Value,
    )> = Vec::with_capacity(rows.len());
    let mut seen: HashSet<(i64, std::borrow::Cow<'_, str>, &str)> =
        HashSet::with_capacity(rows.len());
    for (video_game_id, source, external_id, media_type, url, pdata) in rows.iter().copied() {
        let source = normalize_media_source_for_db(source, supports_psstore);
        let media_type = normalize_media_type_for_db(media_type, supports_background);
        let key = (video_game_id, source.clone(), external_id);
        if seen.insert(key) {
            deduped.push((video_game_id, source, external_id, media_type, url, pdata));
        } else {
            debug!(
                video_game_id,
                source = source.as_ref(),
                external_id,
                "skipping duplicate media row in batch"
            );
        }
    }

    if deduped.is_empty() {
        return Ok(());
    }

    for chunk in deduped.chunks(MAX_CHUNK_SIZE) {
        let mut qb = QueryBuilder::new(
            "INSERT INTO game_media (video_game_id, source, external_id, media_type, title, url, original_url, thumbnail_url, stream_url, poster_url, provider_data) VALUES ",
        );

        for (idx, (vg_id, source, external_id, media_type, url, pdata)) in chunk.iter().enumerate()
        {
            if idx > 0 {
                qb.push(", ");
            }

            let title = media_type.as_ref();

            let (original_url, thumbnail_url, stream_url, poster_url) = {
                fn looks_like_url(s: &str) -> bool {
                    let ls = s.to_ascii_lowercase();
                    (ls.starts_with("http://") || ls.starts_with("https://"))
                        && ls.len() > 8
                        && ls.contains('.')
                }
                fn grab<'a>(
                    obj: &'a serde_json::Map<String, Value>,
                    keys: &[&str],
                ) -> Option<&'a str> {
                    for k in keys {
                        if let Some(v) = obj.get(*k) {
                            if let Some(s) = v.as_str() {
                                if looks_like_url(s) {
                                    return Some(s);
                                }
                            }
                        }
                    }
                    None
                }
                if let Some(obj) = pdata.as_object() {
                    let orig = grab(
                        obj,
                        &["original_url", "url", "high_url", "hd_url", "background"],
                    )
                    .unwrap_or(*url);
                    let thumb = grab(
                        obj,
                        &[
                            "thumbnail_url",
                            "thumb",
                            "thumbnail",
                            "small_url",
                            "image_thumb",
                        ],
                    );
                    let stream = grab(obj, &["stream_url", "video_url", "m3u8", "playable_url"]);
                    let poster = grab(
                        obj,
                        &[
                            "poster_url",
                            "preview_url",
                            "image",
                            "hero",
                            "cover",
                            "background",
                        ],
                    );
                    (Some(orig), thumb, stream, poster)
                } else {
                    (Some(*url), None, None, None)
                }
            };

            qb.push("(")
                .push_bind(*vg_id)
                .push(", ")
                .push_bind(source.as_ref());
            if source_is_enum {
                qb.push("::media_source");
            }
            qb.push(", ")
                .push_bind(*external_id)
                .push(", ")
                .push_bind(media_type.as_ref());
            if media_type_is_enum {
                qb.push("::media_type");
            }
            qb.push(", ")
                .push_bind(title)
                .push(", ")
                .push_bind(*url)
                .push(", ")
                .push_bind(original_url)
                .push(", ")
                .push_bind(thumbnail_url)
                .push(", ")
                .push_bind(stream_url)
                .push(", ")
                .push_bind(poster_url)
                .push(", ")
                .push_bind(Json((*pdata).clone()))
                .push(")");
        }

        qb.push(
            " ON CONFLICT (video_game_id, source, external_id) DO UPDATE SET \
              url=EXCLUDED.url, \
              provider_data=EXCLUDED.provider_data, \
              title=EXCLUDED.title, \
              original_url=COALESCE(EXCLUDED.original_url, game_media.original_url), \
              thumbnail_url=COALESCE(EXCLUDED.thumbnail_url, game_media.thumbnail_url), \
              stream_url=COALESCE(EXCLUDED.stream_url, game_media.stream_url), \
              poster_url=COALESCE(EXCLUDED.poster_url, game_media.poster_url)",
        );

        qb.build().execute(&db.pool).await?;
    }
    Ok(())
}

#[instrument(skip(db))]
pub async fn ensure_software_row(db: &Db, product_id: i64) -> Result<()> {
    // Legacy/Laravel pricing schema: `products`, `sku_regions`, `region_prices` exist, but the
    // modern catalog tables (`software`, `video_game_titles`, etc.) may not.
    // The `software` table has been deprecated and dropped in recent migrations (0535).
    // We check for its existence to support legacy schemas, but if it's missing,
    // we simply return Ok(()).
    if !table_exists(db, "software").await.unwrap_or(false) {
        return Ok(());
    }

    let column = get_software_id_column(db).await?;
    let column_name = match column {
        SoftwareIdColumn::VideoGameId => "video_game_id",
        SoftwareIdColumn::ProductId => "product_id",
    };
    let select_sql = format!(
        "SELECT EXISTS(SELECT 1 FROM software WHERE {}=$1) AS present",
        column_name
    );
    let exists: bool = sqlx::query_scalar(&select_sql)
        .persistent(false)
        .bind(product_id)
        .fetch_one(&db.pool)
        .await?;
    if exists {
        return Ok(());
    }
    let insert_sql = format!("INSERT INTO software ({}) VALUES ($1)", column_name);
    sqlx::query(&insert_sql)
        .persistent(false)
        .bind(product_id)
        .execute(&db.pool)
        .await?;
    Ok(())
}

#[instrument(skip(db))]
pub async fn ensure_video_game_title(
    db: &Db,
    product_id: i64,
    name: &str,
    slug: Option<&str>,
) -> Result<i64> {
    if !table_exists(db, "video_game_titles").await.unwrap_or(false) {
        return Ok(0);
    }

    // CRITICAL FIX: Verify product exists before attempting FK reference
    // This prevents FK violations when product creation and title creation
    // are not in the same transaction (e.g., during parallel provider ingestion)
    let product_exists = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM products WHERE id = $1)"
    )
    .bind(product_id)
    .fetch_one(&db.pool)
    .await?;

    if !product_exists {
        return Err(anyhow::anyhow!(
            "Product {} does not exist - cannot create video_game_title. \
            This may indicate a race condition in parallel provider ingestion.",
            product_id
        ));
    }

    let schema = get_video_game_title_schema(db).await?;

    // Some deployments use video_game_titles.video_game_id as a real FK to video_games.id.
    // In that case, we must NOT use products.id. We derive video_games.id via the Laravel-compatible helper.
    let mut derived_video_game_id: Option<i64> = None;
    if schema.has_video_game_id {
        let video_games_has_product_id = table_column_exists(db, "video_games", "product_id")
            .await
            .unwrap_or(false);
        let video_games_has_title = table_column_exists(db, "video_games", "title")
            .await
            .unwrap_or(false);

        if video_games_has_product_id && video_games_has_title {
            use crate::database_ops::ensure_video_game_for_product_enhanced::{
                ensure_video_game_for_product_enhanced, VideoGameProductMetadata,
            };

            let normalized_hint: Option<String> = slug.map(|s| s.to_string());
            let meta = VideoGameProductMetadata {
                title: name,
                provider_key: None,
                normalized_title: normalized_hint.as_deref(),
                slug: normalized_hint.as_deref(),
                metadata: None,
                ..Default::default()
            };
            derived_video_game_id =
                Some(ensure_video_game_for_product_enhanced(db, product_id, &meta).await?);

            if let Some(vg_id) = derived_video_game_id {
                if let Some(id) = select_title_id_by_column(db, "video_game_id", vg_id).await? {
                    return Ok(id);
                }
            }
        } else {
            // Legacy behavior: treat product_id as the linkage value only when we can't prove this is a real FK.
            if let Some(id) = select_title_id_by_column(db, "video_game_id", product_id).await? {
                return Ok(id);
            }
        }
    }
    if schema.has_product_id {
        if let Some(id) = select_title_id_by_column(db, "product_id", product_id).await? {
            return Ok(id);
        }
    }
    // Normalization: if slug provided, use it; else derive via DB function normalize_game_title
    // Prefer server-side; if function is missing (older schema), fall back to a local normalization.
    let normalized: Option<String> = match slug {
        Some(s) => Some(s.to_string()),
        None => {
            match sqlx::query("SELECT normalize_game_title($1) AS norm")
                .persistent(false)
                .bind(name)
                .fetch_one(&db.pool)
                .await
            {
                Ok(row) => {
                    let norm: String = row.get("norm");
                    Some(norm)
                }
                Err(sqlx::Error::Database(db_err)) => {
                    // 42883 = undefined_function
                    if db_err.code().map(|c| c.to_string()).as_deref() == Some("42883") {
                        warn!(
                            func = "normalize_game_title(text)",
                            "DB function missing; using local fallback normalization"
                        );
                        Some(local_normalize_title(name))
                    } else {
                        return Err(sqlx::Error::Database(db_err).into());
                    }
                }
                Err(e) => {
                    return Err(e.into());
                }
            }
        }
    };
    // If this DB has the source-registry linkage columns (video_game_source_id, video_game_source_id),
    // prefer that path even when legacy linkage columns also exist.
    //
    // Rationale: mixed-schema deployments sometimes make video_game_source_id/video_game_source_id
    // NOT NULL, which would make the legacy insert path crash.
    if schema.has_video_game_source_id && schema.has_vg_source_item_id {
        let manual_source_id = ensure_video_game_source(db, "manual", "Manual").await?;

        // Deterministic video_game_source_id for legacy-created titles.
        // We include the product_id to avoid accidental collisions.
        let video_game_source_id = normalized
            .as_deref()
            .filter(|s| !s.trim().is_empty())
            .map(|s| format!("manual:product:{product_id}:{s}"))
            .unwrap_or_else(|| format!("manual:product:{product_id}"));

        // We only have one "legacy id" here (the function parameter). Different deployments used
        // different column names historically:
        // - Newer: product_id
        // - Older: video_game_id
        // If both columns exist, we refuse to guess video_game_id.
        let derived_product_id = if schema.has_product_id {
            Some(product_id)
        } else {
            None
        };
        let derived_video_game_id = if schema.has_video_game_id {
            if let Some(vg_id) = derived_video_game_id {
                Some(vg_id)
            } else {
                None
            }
        } else {
            None
        };
        return ensure_video_game_title_for_source_item(
            db,
            manual_source_id,
            &video_game_source_id,
            derived_product_id,
            derived_video_game_id,
            name,
            normalized.as_deref(),
            None,
            None,
        )
        .await;
    }

    let insert_sql = build_video_game_title_insert_sql(schema);
    let mut query = sqlx::query(&insert_sql).persistent(false);
    if schema.has_video_game_id {
        query = query.bind(derived_video_game_id.unwrap_or(product_id));
    }
    if schema.has_video_game_ids {
        let id_to_use = derived_video_game_id.unwrap_or(product_id);
        query = query.bind(serde_json::json!([id_to_use]));
    }
    if schema.has_product_id {
        query = query.bind(product_id);
    }
    let rec = query
        .bind(name)
        .bind(&normalized)
        .fetch_one(&db.pool)
        .await?;
    Ok(rec.get("id"))
}

#[instrument(skip(db))]
pub async fn ensure_video_game_title_without_product(
    db: &Db,
    slug_hint: &str,
    name: &str,
) -> Result<i64> {
    if !table_exists(db, "video_game_titles").await.unwrap_or(false) {
        return Ok(0);
    }

    let schema = get_video_game_title_schema(db).await?;
    let slug_candidate = if !slug_hint.trim().is_empty() {
        local_normalize_title(slug_hint)
    } else {
        local_normalize_title(name)
    };
    let slug_final = if slug_candidate.is_empty() {
        format!("title-{}", Uuid::new_v4().simple())
    } else {
        slug_candidate
    };
    let title_trimmed = name.trim();
    let title_final = if title_trimmed.is_empty() {
        if !slug_hint.trim().is_empty() {
            slug_hint.trim().replace(['-', '_'], " ")
        } else {
            format!("Untitled {}", Uuid::new_v4().simple())
        }
    } else {
        title_trimmed.to_string()
    };

    // In source-registry schema, ensure a deterministic "manual" source so we can still create titles.
    let manual_source_id = if schema.has_video_game_source_id && schema.has_vg_source_item_id {
        Some(ensure_video_game_source(db, "manual", "Manual").await?)
    } else {
        None
    };

    if let Some(row) = sqlx::query(&format!(
        "SELECT id, {} AS title_col FROM video_game_titles WHERE normalized_title=$1 ORDER BY updated_at DESC LIMIT 1",
        schema.name_column.as_str()
    ))
    .persistent(false)
    .bind(&slug_final)
    .fetch_optional(&db.pool)
    .await?
    {
        let id: i64 = row.get("id");
        let existing_title: String = row.get("title_col");
        if !title_final.is_empty() && existing_title.trim() != title_final {
            let _ = sqlx::query(&format!(
                "UPDATE video_game_titles SET {}=$1 WHERE id=$2",
                schema.name_column.as_str()
            ))
            .persistent(false)
            .bind(&title_final)
            .bind(id)
            .execute(&db.pool)
            .await?;
        }
        return Ok(id);
    }

    let lower_name = title_final.to_lowercase();
    if !lower_name.is_empty() {
        if let Some(row) = sqlx::query(&format!(
            "SELECT id, normalized_title FROM video_game_titles WHERE lower({})=$1 LIMIT 1",
            schema.name_column.as_str()
        ))
        .persistent(false)
        .bind(&lower_name)
        .fetch_optional(&db.pool)
        .await?
        {
            let id: i64 = row.get("id");
            let existing_norm: String = row.get("normalized_title");
            if existing_norm.trim().is_empty() || existing_norm != slug_final {
                let _ = sqlx::query("UPDATE video_game_titles SET normalized_title=$1 WHERE id=$2")
                    .persistent(false)
                    .bind(&slug_final)
                    .bind(id)
                    .execute(&db.pool)
                    .await?;
            }
            if !title_final.is_empty() {
                let _ = sqlx::query(&format!(
                    "UPDATE video_game_titles SET {}=$1 WHERE id=$2",
                    schema.name_column.as_str()
                ))
                .persistent(false)
                .bind(&title_final)
                .bind(id)
                .execute(&db.pool)
                .await?;
            }
            return Ok(id);
        }
    }

    // Insert a bare title record for the detected schema.
    if let Some(source_id) = manual_source_id {
        if schema.has_video_game_id || schema.has_product_id {
            anyhow::bail!(
                "video_game_titles requires a product/video_game id in this DB; cannot create a title without one"
            );
        }
        // source-registry schema: video_game_source_id is required; use normalized slug as deterministic video_game_source_id.
        return ensure_video_game_title_for_source_item(
            db,
            source_id,
            &slug_final,
            None,
            None,
            &title_final,
            Some(&slug_final),
            None,
            None,
        )
        .await;
    }

    let insert_sql = format!(
        "INSERT INTO video_game_titles ({}, normalized_title) VALUES ($1,$2) RETURNING id",
        schema.name_column.as_str()
    );
    let inserted = sqlx::query(&insert_sql)
        .persistent(false)
        .bind(&title_final)
        .bind(&slug_final)
        .fetch_one(&db.pool)
        .await?;
    Ok(inserted.get("id"))
}

#[instrument(skip(db))]
pub async fn ensure_video_game(
    db: &Db,
    title_id: i64,
    platform_id: i64,
    edition: Option<&str>,
) -> Result<i64> {
    // Schema compatibility:
    // - "Unified" schema expects (title_id, platform_id[, edition]).
    // - Laravel-oriented schemas typically key `video_games` by `product_id` and store a single row.
    // In php-compat mode we should never hard-crash on missing columns.
    if !table_exists(db, "video_games").await.unwrap_or(false) {
        warn!("video_games table missing; skipping");
        return Ok(0);
    }

    let has_title_id = table_column_exists(db, "video_games", "title_id")
        .await
        .unwrap_or(false);
    let has_platform_id = table_column_exists(db, "video_games", "platform_id")
        .await
        .unwrap_or(false);

    if !(has_title_id && has_platform_id) {
        let has_product_id = table_column_exists(db, "video_games", "product_id")
            .await
            .unwrap_or(false);
        let has_title = table_column_exists(db, "video_games", "title")
            .await
            .unwrap_or(false);

        if has_product_id && has_title {
            // Best-effort: map title_id -> product_id, then ensure a Laravel-style `video_games` row.
            let Some(product_id) = fetch_product_id_for_title(db, title_id).await? else {
                warn!(
                    title_id,
                    "video_games missing (title_id, platform_id); cannot map title_id -> product_id; skipping video_game ensure"
                );
                return Ok(0);
            };

            // Best-effort title + normalized_title fetch for nicer Laravel rows.
            let mut title = format!("Title {title_id}");
            let mut normalized_title: Option<String> = None;
            if table_exists(db, "video_game_titles").await.unwrap_or(false) {
                // Try to use whichever title column exists.
                let title_schema = get_video_game_title_schema(db).await;
                let (expect_title_col, sql) = match title_schema {
                    Ok(schema) => {
                        let col = schema.name_column.as_str();
                        let sql = format!(
                            "SELECT {col} AS title_col, normalized_title FROM video_game_titles WHERE id=$1",
                        );
                        (Some(col), sql)
                    }
                    // If schema probing fails, avoid referencing unknown columns.
                    // We'll keep the default synthetic title and only try to pull normalized_title.
                    Err(_) => (
                        None,
                        "SELECT normalized_title FROM video_game_titles WHERE id=$1".to_string(),
                    ),
                };

                if let Some(row) = sqlx::query(&sql)
                    .persistent(false)
                    .bind(title_id)
                    .fetch_optional(&db.pool)
                    .await?
                {
                    if expect_title_col.is_some() {
                        let t: Option<String> = row.try_get("title_col").ok();
                        if let Some(t) = t {
                            if !t.trim().is_empty() {
                                title = t;
                            }
                        }
                    }

                    let n: Option<String> = row.try_get("normalized_title").ok();
                    normalized_title = n.filter(|s| !s.trim().is_empty());
                }
            }

            // Preserve the richer per-platform/per-edition context in JSON, even though
            // Laravel-style `video_games` rows are usually 1-per-product.
            let mut meta = serde_json::json!({
                "compat_source": "ensure_video_game",
                "title_id": title_id,
                "platform_id": platform_id,
            });
            if let Some(ed) = edition {
                meta["edition"] = serde_json::Value::String(ed.to_string());
            }

            return ensure_video_game_for_product(
                db,
                product_id,
                &title,
                normalized_title.as_deref(),
                Some(meta),
            )
            .await;
        }

        if php_compat_schema(db).await.unwrap_or(false) {
            warn!(
                has_product_id,
                has_title,
                has_title_id,
                has_platform_id,
                "video_games table schema unsupported; skipping ensure_video_game in php-compat mode"
            );
            return Ok(0);
        }

        warn!(
            "video_games schema unsupported (expected title_id+platform_id or product_id+title); skipping"
        );
        return Ok(0);
    }

    // Modern/unified schema: delegate to the enhanced implementation (safe because we just
    // validated that title_id + platform_id exist on the resolved video_games table).
    super::ensure_video_game_enhanced::ensure_video_game_enhanced(
        db,
        title_id,
        platform_id,
        edition,
        None,
    )
    .await
}

#[instrument(skip(db))]
pub async fn ensure_sellable_for_title(db: &Db, title_id: i64) -> Result<i64> {
    if !table_exists(db, "sellables").await.unwrap_or(false) {
        warn!("ensure_sellable_for_title: sellables table missing. Returning 0.");
        return Ok(0);
    }

    if php_compat_schema(db).await.unwrap_or(false) {
        // In php compat, we may not have (or want to depend on) the sellables table.
        // Synthesize a stable sellable id keyed by (kind, product_id). If we can't resolve a
        // product_id for this title, fall back to title_id to keep the mapping stable.
        let product_id = fetch_product_id_for_title(db, title_id)
            .await
            .unwrap_or(None)
            .unwrap_or(title_id);
        return ensure_sellable(db, "software", product_id).await;
    }

    if let Some(row) = sqlx::query("SELECT id FROM sellables WHERE software_title_id=$1")
        .persistent(false)
        .bind(title_id)
        .fetch_optional(&db.pool)
        .await?
    {
        return Ok(row.get("id"));
    }
    let row = sqlx::query(
        "INSERT INTO sellables (kind, software_title_id) VALUES ('software',$1) RETURNING id",
    )
    .persistent(false)
    .bind(title_id)
    .fetch_one(&db.pool)
    .await?;
    Ok(row.get("id"))
}

#[instrument(skip(db))]
pub async fn ensure_sellable(db: &Db, kind: &str, product_id: i64) -> Result<i64> {
    if !table_exists(db, "sellables").await.unwrap_or(false) {
        return Ok(0);
    }

    if php_compat_schema(db).await.unwrap_or(false) {
        let (counter, map) = SELLABLE_COMPAT
            .get_or_init(|| async {
                (
                    std::sync::atomic::AtomicI64::new(1),
                    Mutex::new(HashMap::new()),
                )
            })
            .await;

        let key = (kind.to_string(), product_id);
        let mut guard = map.lock().unwrap();
        if let Some(id) = guard.get(&key).copied() {
            return Ok(id);
        }
        let new_id = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        guard.insert(key, new_id);
        return Ok(new_id);
    }

    let schema = get_sellable_schema(db).await?;
    match kind {
        "software" => {
            if schema.has_software_title_id {
                let title_id = fetch_title_id_for_product(db, product_id)
                    .await?
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "no video_game_title found for product {} while creating sellable",
                            product_id
                        )
                    })?;
                if
                    let Some(rec) = sqlx
                        ::query(
                            "SELECT id FROM sellables WHERE kind=$1::sellable_kind AND software_title_id=$2"
                        )
                        .persistent(false)
                        .bind(kind)
                        .bind(title_id)
                        .fetch_optional(&db.pool).await?
                {
                    let id: i64 = rec.get("id");
                    let vg_cols = video_games_content_columns(db).await.unwrap_or_default();
                    if vg_cols.has_sellable_id && vg_cols.has_title_id {
                        let _ = sqlx
                            ::query(
                                "UPDATE video_games SET sellable_id=$1 WHERE title_id=$2 AND (sellable_id IS DISTINCT FROM $1)"
                            )
                            .persistent(false)
                            .bind(id)
                            .bind(title_id)
                            .execute(&db.pool)
                            .await?;
                    }
                    return Ok(id);
                }
                let inserted = sqlx
                    ::query(
                        "INSERT INTO sellables (kind, software_title_id) VALUES ($1::sellable_kind,$2) RETURNING id"
                    )
                    .persistent(false)
                    .bind(kind)
                    .bind(title_id)
                    .fetch_one(&db.pool).await?;
                let sellable_id: i64 = inserted.get("id");
                let vg_cols = video_games_content_columns(db).await.unwrap_or_default();
                if vg_cols.has_sellable_id && vg_cols.has_title_id {
                    let _ = sqlx
                        ::query(
                            "UPDATE video_games SET sellable_id=$1 WHERE title_id=$2 AND (sellable_id IS DISTINCT FROM $1)"
                        )
                        .persistent(false)
                        .bind(sellable_id)
                        .bind(title_id)
                        .execute(&db.pool)
                        .await?;
                }
                Ok(sellable_id)
            } else if schema.has_product_id {
                if let Some(rec) = sqlx::query(
                    "SELECT id FROM sellables WHERE kind=$1::sellable_kind AND product_id=$2",
                )
                .persistent(false)
                .bind(kind)
                .bind(product_id)
                .fetch_optional(&db.pool)
                .await?
                {
                    return Ok(rec.get("id"));
                }
                let inserted = sqlx
                    ::query(
                        "INSERT INTO sellables (kind, product_id) VALUES ($1::sellable_kind,$2) RETURNING id"
                    )
                    .persistent(false)
                    .bind(kind)
                    .bind(product_id)
                    .fetch_one(&db.pool).await?;
                Ok(inserted.get("id"))
            } else {
                anyhow::bail!("sellables table missing software linkage columns")
            }
        }
        "hardware" => {
            if schema.has_console_id {
                if let Some(rec) = sqlx::query(
                    "SELECT id FROM sellables WHERE kind=$1::sellable_kind AND console_id=$2",
                )
                .persistent(false)
                .bind(kind)
                .bind(product_id)
                .fetch_optional(&db.pool)
                .await?
                {
                    return Ok(rec.get("id"));
                }
                let inserted = sqlx
                    ::query(
                        "INSERT INTO sellables (kind, console_id) VALUES ($1::sellable_kind,$2) RETURNING id"
                    )
                    .persistent(false)
                    .bind(kind)
                    .bind(product_id)
                    .fetch_one(&db.pool).await?;
                Ok(inserted.get("id"))
            } else if schema.has_product_id {
                if let Some(rec) = sqlx::query(
                    "SELECT id FROM sellables WHERE kind=$1::sellable_kind AND product_id=$2",
                )
                .persistent(false)
                .bind(kind)
                .bind(product_id)
                .fetch_optional(&db.pool)
                .await?
                {
                    return Ok(rec.get("id"));
                }
                let inserted = sqlx
                    ::query(
                        "INSERT INTO sellables (kind, product_id) VALUES ($1::sellable_kind,$2) RETURNING id"
                    )
                    .persistent(false)
                    .bind(kind)
                    .bind(product_id)
                    .fetch_one(&db.pool).await?;
                Ok(inserted.get("id"))
            } else {
                anyhow::bail!("sellables table missing hardware linkage columns")
            }
        }
        _ => anyhow::bail!("unsupported sellable kind '{}'", kind),
    }
}

#[instrument(skip(db))]
pub async fn ensure_offer(
    db: &Db,
    sellable_id: i64,
    retailer_id: i64,
    sku: Option<&str>,
) -> Result<i64> {
    if !table_exists(db, "offers").await.unwrap_or(false) {
        return Ok(0);
    }

    if php_compat_schema(db).await.unwrap_or(false) {
        let (counter, map) = OFFER_COMPAT
            .get_or_init(|| async {
                (
                    std::sync::atomic::AtomicI64::new(1),
                    Mutex::new(HashMap::new()),
                )
            })
            .await;
        let mut guard = map.lock().unwrap();
        if let Some((id, _)) = guard
            .iter()
            .find(|(_id, (s, r, sk))| {
                *s == sellable_id && *r == retailer_id && sk.as_deref() == sku
            })
            .map(|(id, v)| (*id, v.clone()))
        {
            return Ok(id);
        }
        let new_id = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        guard.insert(
            new_id,
            (sellable_id, retailer_id, sku.map(|s| s.to_string())),
        );
        return Ok(new_id);
    }

    if
        let Some(rec) = sqlx
            ::query(
                "SELECT id FROM offers WHERE sellable_id=$1 AND retailer_id=$2 AND sku IS NOT DISTINCT FROM $3"
            )
            .persistent(false)
            .bind(sellable_id)
            .bind(retailer_id)
            .bind(sku)
            .fetch_optional(&db.pool).await?
    {
        return Ok(rec.get("id"));
    }
    let inserted = sqlx::query(
        "INSERT INTO offers (sellable_id, retailer_id, sku) VALUES ($1,$2,$3) RETURNING id",
    )
    .persistent(false)
    .bind(sellable_id)
    .bind(retailer_id)
    .bind(sku)
    .fetch_one(&db.pool)
    .await?;
    Ok(inserted.get("id"))
}

#[instrument(skip(db))]
pub async fn ensure_offer_jurisdiction(
    db: &Db,
    offer_id: i64,
    jurisdiction_id: i64,
    currency_id: i64,
) -> Result<i64> {
    let compat = php_compat_schema(db).await.unwrap_or(false);
    let has_jurisdictions = *JURISDICTIONS_PRESENT
        .get_or_try_init(|| async { table_exists(db, "jurisdictions").await })
        .await?;

    if compat {
        let has_countries = table_exists(db, "countries").await.unwrap_or(false);

        // Determine (sellable_id, retailer_id) for this offer (synthetic in php compat).
        let (sellable_id, retailer_id, _sku) = {
            let (_, map) = OFFER_COMPAT
                .get_or_init(|| async {
                    (
                        std::sync::atomic::AtomicI64::new(1),
                        Mutex::new(HashMap::new()),
                    )
                })
                .await;
            let guard = map.lock().unwrap();
            guard
                .get(&offer_id)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("offer {} missing for php compat", offer_id))?
        };

        // Derive product_id.
        // - In php compat, sellable_id is typically synthetic; recover product_id from SELLABLE_COMPAT.
        // - If a real sellables table exists, fall back to querying it.
        let product_id: i64 = {
            let mut from_compat: Option<i64> = None;
            if let Some((_, map)) = SELLABLE_COMPAT.get() {
                let guard = map.lock().unwrap();
                if let Some(((_kind, pid), _sid)) = guard
                    .iter()
                    .find(|((_k, _pid), sid)| **sid == sellable_id)
                    .map(|(k, v)| (k, v))
                {
                    from_compat = Some(*pid);
                }
            }
            if let Some(pid) = from_compat {
                pid
            } else {
                sqlx::query_scalar(
                    "SELECT COALESCE(product_id, software_title_id, console_id, id) FROM sellables WHERE id=$1",
                )
                .persistent(false)
                .bind(sellable_id)
                .fetch_optional(&db.pool)
                .await?
                .ok_or_else(|| anyhow::anyhow!("sellable {} missing when creating sku_region (offer {})", sellable_id, offer_id))?
            }
        };

        // retailer string (prefer modern retailers, else game_retailers)
        let retailer_key: String = if table_exists(db, "retailers").await.unwrap_or(false) {
            sqlx::query_scalar("SELECT COALESCE(slug, name, 'retailer') FROM retailers WHERE id=$1")
                .persistent(false)
                .bind(retailer_id)
                .fetch_optional(&db.pool)
                .await?
                .ok_or_else(|| {
                    anyhow::anyhow!("retailer {} missing (offer {})", retailer_id, offer_id)
                })?
        } else if table_exists(db, "game_retailers").await.unwrap_or(false) {
            sqlx::query_scalar(
                "SELECT COALESCE(retailer_key, slug, name, 'retailer') FROM game_retailers WHERE id=$1",
            )
            .persistent(false)
            .bind(retailer_id)
            .fetch_optional(&db.pool)
            .await?
            .unwrap_or_else(|| "retailer".to_string())
        } else {
            "retailer".to_string()
        };

        // php-compat default: treat sku_regions as jurisdictions.
        // If the caller is already passing sku_regions.id in the jurisdiction_id slot,
        // we can return it directly and avoid relying on jurisdictions/countries.
        if !has_jurisdictions {
            let existing_sku_region: Option<i64> = sqlx::query_scalar(
                "SELECT id FROM sku_regions WHERE id=$1 AND product_id=$2 LIMIT 1",
            )
            .persistent(false)
            .bind(jurisdiction_id)
            .bind(product_id)
            .fetch_optional(&db.pool)
            .await?;

            if let Some(id) = existing_sku_region {
                return Ok(id);
            }
        }

        // Resolve region_code.
        // - If jurisdictions exists: use it.
        // - Else: try to interpret jurisdiction_id as an existing sku_regions.id for this product+retailer (best fidelity).
        // - Else if countries exists: treat jurisdiction_id as country_id.
        // - Else: fail fast (ambiguous and can hide data integrity issues).
        //
        // NOTE: php-compat environments may not have `jurisdictions` at all. In that case, the
        // only deterministic mapping we can do is either:
        //   1) the caller already passed a sku_region id, or
        //   2) map by countries(id)->code.
        // This keeps multi-region ingestion working on Laravel schemas without requiring migrations.
        let region_code: String = if has_jurisdictions {
            let country_schema = country_schema(db).await?;
            let code_expr = country_schema.code_expr().ok_or_else(|| {
                anyhow::anyhow!("countries table missing code column for region lookup")
            })?;
            let region_sql = format!(
                "SELECT COALESCE(j.region_code, {}) FROM jurisdictions j JOIN countries c ON c.id=j.country_id WHERE j.id=$1",
                code_expr
            );
            sqlx::query_scalar(&region_sql)
                .persistent(false)
                .bind(jurisdiction_id)
                .fetch_optional(&db.pool)
                .await?
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "jurisdiction {} missing (offer {})",
                        jurisdiction_id,
                        offer_id
                    )
                })?
        } else if let Some(existing_region_code) = sqlx::query_scalar::<_, Option<String>>(
            "SELECT region_code FROM sku_regions WHERE id=$1 AND product_id=$2 AND retailer=$3",
        )
        .persistent(false)
        .bind(jurisdiction_id)
        .bind(product_id)
        .bind(&retailer_key)
        .fetch_one(&db.pool)
        .await?
        {
            existing_region_code
        } else if has_countries {
            let country_schema = country_schema(db).await?;
            let code_col = country_schema.code_column().ok_or_else(|| {
                anyhow::anyhow!("countries table missing code column for region lookup")
            })?;
            let country_sql = format!("SELECT {} FROM countries WHERE id=$1", code_col);
            let raw_code: String = sqlx::query_scalar(&country_sql)
                .persistent(false)
                .bind(jurisdiction_id)
                .fetch_optional(&db.pool)
                .await?
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "country {} missing for php compat region mapping (offer {})",
                        jurisdiction_id,
                        offer_id
                    )
                })?;

            // sku_regions.region_code is typically ISO2. If country codes are longer (e.g. ISO3, or "US-CA"),
            // normalize to the first component/first two letters.
            let mut code = raw_code.trim().to_ascii_uppercase();
            if let Some((head, _)) = code.split_once('-') {
                code = head.to_string();
            }
            if code.len() != 2 {
                // Best-effort: take first 2 ASCII letters if present.
                let letters: String = code
                    .chars()
                    .filter(|c| c.is_ascii_alphabetic())
                    .take(2)
                    .collect();
                if letters.len() == 2 {
                    warn!(offer_id, jurisdiction_id, country_code=%raw_code, normalized_region_code=%letters, "php compat: normalizing country code to 2-letter region_code");
                    code = letters;
                } else {
                    anyhow::bail!(
                        "php compat: cannot derive 2-letter region_code from country code '{}' (offer_id={}, jurisdiction_id={})",
                        raw_code,
                        offer_id,
                        jurisdiction_id
                    );
                }
            }
            code
        } else {
            anyhow::bail!(
                "php compat cannot derive region_code without jurisdictions or countries; refusing ambiguous fallback (offer_id={}, jurisdiction_id={})",
                offer_id,
                jurisdiction_id
            );
        };
        let region_code = region_code.to_ascii_uppercase();

        // currency code
        let currency: String = sqlx::query_scalar("SELECT code FROM currencies WHERE id=$1")
            .persistent(false)
            .bind(currency_id)
            .fetch_optional(&db.pool)
            .await?
            .unwrap_or_else(|| "USD".to_string());

        // Upsert sku_regions
        if let Some(existing) = sqlx::query(
            "SELECT id FROM sku_regions WHERE product_id=$1 AND region_code=$2 AND retailer=$3",
        )
        .persistent(false)
        .bind(product_id)
        .bind(&region_code)
        .bind(&retailer_key)
        .fetch_optional(&db.pool)
        .await?
        {
            return Ok(existing.get("id"));
        }
        let insert_res = sqlx::query(
            "INSERT INTO sku_regions (product_id, region_code, retailer, currency, sku, is_active, metadata) VALUES ($1,$2,$3,$4,$5,true, NULL) RETURNING id",
        )
        .persistent(false)
        .bind(product_id)
        .bind(&region_code)
        .bind(&retailer_key)
        .bind(&currency)
        .bind::<Option<String>>(None)
        .fetch_one(&db.pool)
        .await;

        let rec = match insert_res {
            Ok(rec) => rec,
            Err(e) => {
                // Supabase "no-migrate" / legacy tables sometimes have SERIAL sequences out of sync
                // (e.g., rows imported with explicit ids). Retry once after bumping the sequence.
                let is_dup_pk = match &e {
                    sqlx::Error::Database(db_err) => {
                        let code = db_err.code().map(|c| c.to_string());
                        let constraint = db_err.constraint().map(|c| c.to_string());
                        code.as_deref() == Some("23505")
                            && constraint.as_deref() == Some("sku_regions_pkey")
                    }
                    _ => false,
                };
                if !is_dup_pk {
                    return Err(e.into());
                }

                if let Some(seq) = sqlx::query_scalar::<_, Option<String>>(
                    "SELECT pg_get_serial_sequence('sku_regions','id')",
                )
                .persistent(false)
                .fetch_one(&db.pool)
                .await?
                {
                    let _ = sqlx::query(
                        "SELECT setval($1::regclass, (SELECT COALESCE(MAX(id),0)+1 FROM sku_regions), false)",
                    )
                    .persistent(false)
                    .bind(seq)
                    .execute(&db.pool)
                    .await?;
                }

                sqlx::query(
                    "INSERT INTO sku_regions (product_id, region_code, retailer, currency, sku, is_active, metadata) VALUES ($1,$2,$3,$4,$5,true, NULL) RETURNING id",
                )
                .persistent(false)
                .bind(product_id)
                .bind(&region_code)
                .bind(&retailer_key)
                .bind(&currency)
                .bind::<Option<String>>(None)
                .fetch_one(&db.pool)
                .await?
            }
        };
        return Ok(rec.get("id"));
    }

    // Modern schema: require jurisdictions table so we don't fail with relation errors.
    if !has_jurisdictions {
        warn!("jurisdictions table missing; cannot create offer_jurisdictions — skipping");
        return Ok(0);
    }

    if !table_exists(db, "offer_jurisdictions").await? {
        warn!("offer_jurisdictions table missing; skipping");
        return Ok(0);
    }

    if let Some(rec) =
        sqlx::query("SELECT id FROM offer_jurisdictions WHERE offer_id=$1 AND jurisdiction_id=$2")
            .persistent(false)
            .bind(offer_id)
            .bind(jurisdiction_id)
            .fetch_optional(&db.pool)
            .await?
    {
        return Ok(rec.get("id"));
    }
    let inserted = sqlx
        ::query(
            "INSERT INTO offer_jurisdictions (offer_id, jurisdiction_id, currency_id) VALUES ($1,$2,$3) RETURNING id"
        )
        .persistent(false)
        .bind(offer_id)
        .bind(jurisdiction_id)
        .bind(currency_id)
        .fetch_one(&db.pool).await?;
    Ok(inserted.get("id"))
}

/// Batch create or retrieve multiple offers.
/// Returns a Vec of offer IDs in the same order as input.
///
/// This is optimized for the common pattern of creating many offers at once.
/// Uses a single batch INSERT with UNNEST instead of N individual queries.
#[instrument(skip(db, offers))]
pub async fn ensure_offers_batch(
    db: &Db,
    offers: &[(i64, i64, Option<&str>)], // (sellable_id, retailer_id, sku)
) -> Result<Vec<i64>> {
    if offers.is_empty() {
        return Ok(Vec::new());
    }

    // For PHP compat mode, fall back to individual queries
    if php_compat_schema(db).await.unwrap_or(false) {
        let mut result = Vec::with_capacity(offers.len());
        for (sellable_id, retailer_id, sku) in offers {
            let id = ensure_offer(db, *sellable_id, *retailer_id, *sku).await?;
            result.push(id);
        }
        return Ok(result);
    }

    // Prepare arrays for batch processing
    let sellable_ids: Vec<i64> = offers.iter().map(|(s, _, _)| *s).collect();
    let retailer_ids: Vec<i64> = offers.iter().map(|(_, r, _)| *r).collect();
    let skus: Vec<Option<String>> = offers
        .iter()
        .map(|(_, _, s)| s.map(|x| x.to_string()))
        .collect();

    // Batch INSERT using UNNEST to expand arrays
    // First, try to get existing IDs, then insert missing ones
    // The DISTINCT ON ensures we return one ID per unique (sellable, retailer, sku) tuple
    let rows = sqlx::query(
        "WITH input AS (
            SELECT * FROM UNNEST($1::bigint[], $2::bigint[], $3::text[])
                AS t(sellable_id, retailer_id, sku)
        ),
        existing AS (
            SELECT i.*, o.id
            FROM input i
            LEFT JOIN offers o ON o.sellable_id = i.sellable_id
                AND o.retailer_id = i.retailer_id
                AND o.sku IS NOT DISTINCT FROM i.sku
        ),
        to_insert AS (
            SELECT sellable_id, retailer_id, sku
            FROM existing
            WHERE id IS NULL
        ),
        inserted AS (
            INSERT INTO offers (sellable_id, retailer_id, sku)
            SELECT sellable_id, retailer_id, sku FROM to_insert
            ON CONFLICT (sellable_id, retailer_id, sku) DO NOTHING
            RETURNING id, sellable_id, retailer_id, sku
        )
        SELECT COALESCE(e.id, ins.id) as id
        FROM existing e
        LEFT JOIN inserted ins ON ins.sellable_id = e.sellable_id
            AND ins.retailer_id = e.retailer_id
            AND ins.sku IS NOT DISTINCT FROM e.sku
        ORDER BY (
            SELECT idx FROM (
                SELECT ROW_NUMBER() OVER () as idx, *
                FROM input
            ) sub
            WHERE sub.sellable_id = e.sellable_id
                AND sub.retailer_id = e.retailer_id
                AND sub.sku IS NOT DISTINCT FROM e.sku
            LIMIT 1
        )",
    )
    .persistent(false)
    .bind(&sellable_ids)
    .bind(&retailer_ids)
    .bind(&skus)
    .fetch_all(&db.pool)
    .await?;

    let result: Vec<i64> = rows.iter().map(|r| r.get("id")).collect();
    Ok(result)
}

/// Batch create or retrieve multiple offer_jurisdictions.
/// Returns a Vec of offer_jurisdiction IDs in the same order as input.
///
/// This is optimized for the common pattern of creating many offer_jurisdictions at once.
/// Uses a single batch INSERT with UNNEST instead of N individual queries.
#[instrument(skip(db, offer_jurisdictions))]
pub async fn ensure_offer_jurisdictions_batch(
    db: &Db,
    offer_jurisdictions: &[(i64, i64, i64)], // (offer_id, jurisdiction_id, currency_id)
) -> Result<Vec<i64>> {
    if offer_jurisdictions.is_empty() {
        return Ok(Vec::new());
    }

    // For PHP compat mode, fall back to individual queries
    let compat = php_compat_schema(db).await.unwrap_or(false);
    if compat {
        let mut result = Vec::with_capacity(offer_jurisdictions.len());
        for (offer_id, jurisdiction_id, currency_id) in offer_jurisdictions {
            let id =
                ensure_offer_jurisdiction(db, *offer_id, *jurisdiction_id, *currency_id).await?;
            result.push(id);
        }
        return Ok(result);
    }

    // Prepare arrays for batch processing
    let offer_ids: Vec<i64> = offer_jurisdictions.iter().map(|(o, _, _)| *o).collect();
    let jurisdiction_ids: Vec<i64> = offer_jurisdictions.iter().map(|(_, j, _)| *j).collect();
    let currency_ids: Vec<i64> = offer_jurisdictions.iter().map(|(_, _, c)| *c).collect();

    // Batch INSERT using UNNEST to expand arrays
    // Similar pattern to ensure_offers_batch but for offer_jurisdictions table
    let rows = sqlx::query(
        "WITH input AS (
            SELECT * FROM UNNEST($1::bigint[], $2::bigint[], $3::bigint[])
                AS t(offer_id, jurisdiction_id, currency_id)
        ),
        existing AS (
            SELECT i.*, oj.id
            FROM input i
            LEFT JOIN offer_jurisdictions oj
                ON oj.offer_id = i.offer_id
                AND oj.jurisdiction_id = i.jurisdiction_id
        ),
        to_insert AS (
            SELECT offer_id, jurisdiction_id, currency_id
            FROM existing
            WHERE id IS NULL
        ),
        inserted AS (
            INSERT INTO offer_jurisdictions (offer_id, jurisdiction_id, currency_id)
            SELECT offer_id, jurisdiction_id, currency_id FROM to_insert
            ON CONFLICT (offer_id, jurisdiction_id) DO UPDATE
                SET currency_id = EXCLUDED.currency_id
            RETURNING id, offer_id, jurisdiction_id
        )
        SELECT COALESCE(e.id, ins.id) as id
        FROM existing e
        LEFT JOIN inserted ins
            ON ins.offer_id = e.offer_id
            AND ins.jurisdiction_id = e.jurisdiction_id
        ORDER BY (
            SELECT idx FROM (
                SELECT ROW_NUMBER() OVER () as idx, *
                FROM input
            ) sub
            WHERE sub.offer_id = e.offer_id
                AND sub.jurisdiction_id = e.jurisdiction_id
            LIMIT 1
        )",
    )
    .persistent(false)
    .bind(&offer_ids)
    .bind(&jurisdiction_ids)
    .bind(&currency_ids)
    .fetch_all(&db.pool)
    .await?;

    let result: Vec<i64> = rows.iter().map(|r| r.get("id")).collect();
    Ok(result)
}

#[instrument(skip(db))]
pub async fn ensure_product(db: &Db, category: &str, slug: Option<&str>) -> Result<i64> {
    // Laravel schema compatibility: products.name + products.platform are NOT NULL.
    const DEFAULT_PLATFORM: &str = "unknown";

    async fn resync_products_id_sequence(db: &Db) -> Result<()> {
        // If the `products_id_seq` (or equivalent) is behind MAX(id), inserts can fail with
        // `duplicate key value violates unique constraint "products_pkey"`.
        // This happens commonly after bulk imports with explicit ids.
        sqlx::query(
            "SELECT setval(pg_get_serial_sequence('products','id'), (SELECT COALESCE(MAX(id),0)+1 FROM products), false)",
        )
        .persistent(false)
        .execute(&db.pool)
        .await?;
        Ok(())
    }

    fn is_products_pkey_violation(e: &sqlx::Error) -> bool {
        match e {
            sqlx::Error::Database(db_err) => {
                // 23505 = unique_violation
                db_err.code().as_deref() == Some("23505")
                    && db_err.constraint() == Some("products_pkey")
            }
            _ => false,
        }
    }
    if let Some(slug_val) = slug {
        if let Some(rec) = sqlx::query("SELECT id FROM products WHERE slug=$1")
            .persistent(false)
            .bind(slug_val)
            .fetch_optional(&db.pool)
            .await?
        {
            return Ok(rec.get("id"));
        }
        match sqlx::query(
            "INSERT INTO products (slug, name, platform, category) VALUES ($1,$2,$3,$4) \
                     ON CONFLICT (slug) DO UPDATE SET \
                        name=EXCLUDED.name, \
                        platform=CASE \
                            WHEN products.platform='unknown' AND EXCLUDED.platform <> 'unknown' THEN EXCLUDED.platform \
                            ELSE products.platform \
                        END, \
                        category=EXCLUDED.category \
                     RETURNING id",
        )
        .persistent(false)
        .bind(slug_val)
        .bind(slug_val)
        .bind(DEFAULT_PLATFORM)
        .bind(category)
        .fetch_one(&db.pool)
        .await {
            Ok(rec) => return Ok(rec.get("id")),
            Err(e) if is_products_pkey_violation(&e) => {
                resync_products_id_sequence(db).await?;
                let rec = sqlx::query(
                    "INSERT INTO products (slug, name, platform, category) VALUES ($1,$2,$3,$4) \
                             ON CONFLICT (slug) DO UPDATE SET \
                                name=EXCLUDED.name, \
                                platform=CASE \
                                    WHEN products.platform='unknown' AND EXCLUDED.platform <> 'unknown' THEN EXCLUDED.platform \
                                    ELSE products.platform \
                                END, \
                                category=EXCLUDED.category \
                             RETURNING id",
                )
                .persistent(false)
                .bind(slug_val)
                .bind(slug_val)
                .bind(DEFAULT_PLATFORM)
                .bind(category)
                .fetch_one(&db.pool)
                .await?;
                return Ok(rec.get("id"));
            }
            Err(e) => return Err(e.into()),
        }
    }
    // Some call sites still use ensure_product(..., None). For schemas requiring slug/name/platform,
    // auto-provision a stable unique slug.
    let auto = Uuid::new_v4().simple().to_string();
    let auto_slug = format!("auto-{}", auto);
    let auto_name = format!("Untitled {}", auto);
    let inserted = match sqlx::query(
        "INSERT INTO products (slug, name, platform, category) VALUES ($1,$2,$3,$4) RETURNING id",
    )
    .persistent(false)
    .bind(&auto_slug)
    .bind(&auto_name)
    .bind(DEFAULT_PLATFORM)
    .bind(category)
    .fetch_one(&db.pool)
    .await
    {
        Ok(rec) => rec,
        Err(e) if is_products_pkey_violation(&e) => {
            resync_products_id_sequence(db).await?;
            sqlx::query(
                "INSERT INTO products (slug, name, platform, category) VALUES ($1,$2,$3,$4) RETURNING id",
            )
            .persistent(false)
            .bind(&auto_slug)
            .bind(&auto_name)
            .bind(DEFAULT_PLATFORM)
            .bind(category)
            .fetch_one(&db.pool)
            .await?
        }
        Err(e) => return Err(e.into()),
    };
    Ok(inserted.get("id"))
}

#[instrument(skip(db))]
pub async fn ensure_product_named(db: &Db, category: &str, slug: &str, name: &str) -> Result<i64> {
    if !table_exists(db, "products").await.unwrap_or(false) {
        warn!("ensure_product_named: products table missing. Returning 0.");
        return Ok(0);
    }

    // Current schema: products table has slug, name, category only (no platform column)
    async fn resync_products_id_sequence(db: &Db) -> Result<()> {
        sqlx::query(
            "SELECT setval(pg_get_serial_sequence('products','id'), (SELECT COALESCE(MAX(id),0)+1 FROM products), false)",
        )
        .persistent(false)
        .execute(&db.pool)
        .await?;
        Ok(())
    }
    fn is_products_pkey_violation(e: &sqlx::Error) -> bool {
        match e {
            sqlx::Error::Database(db_err) => {
                db_err.code().as_deref() == Some("23505")
                    && db_err.constraint() == Some("products_pkey")
            }
            _ => false,
        }
    }
    if let Some(rec) = sqlx::query("SELECT id FROM products WHERE slug=$1")
        .persistent(false)
        .bind(slug)
        .fetch_optional(&db.pool)
        .await?
    {
        // Keep name/category fresh if changed
        let existing_id: i64 = rec.get("id");
        let _ = sqlx::query(
            "UPDATE products SET name=$1, category=$2 WHERE id=$3",
        )
        .persistent(false)
        .bind(name)
        .bind(category)
        .bind(existing_id)
        .execute(&db.pool)
        .await?;
        return Ok(existing_id);
    }
    let inserted = match sqlx
        ::query(
            "INSERT INTO products (slug, name, category) VALUES ($1,$2,$3) \
             ON CONFLICT (slug) DO UPDATE SET \
                name=EXCLUDED.name, \
                category=EXCLUDED.category \
             RETURNING id"
        )
        .persistent(false)
        .bind(slug)
        .bind(name)
        .bind(category)
        .fetch_one(&db.pool)
        .await
    {
        Ok(rec) => rec,
        Err(e) if is_products_pkey_violation(&e) => {
            resync_products_id_sequence(db).await?;
            sqlx
                ::query(
                    "INSERT INTO products (slug, name, category) VALUES ($1,$2,$3) \
                     ON CONFLICT (slug) DO UPDATE SET \
                        name=EXCLUDED.name, \
                        category=EXCLUDED.category \
                     RETURNING id"
                )
                .persistent(false)
                .bind(slug)
                .bind(name)
                .bind(category)
                .fetch_one(&db.pool)
                .await?
        }
        Err(e) => return Err(e.into()),
    };
    Ok(inserted.get("id"))
}

#[instrument(skip(db))]
pub async fn ensure_product_named_with_platform(
    db: &Db,
    category: &str,
    slug: &str,
    name: &str,
    _platform: &str, // DEPRECATED: platform column removed from schema, parameter kept for backwards compatibility
) -> Result<i64> {
    // Delegate to ensure_product_named - platform info now stored in video_games table
    ensure_product_named(db, category, slug, name).await
}

// --------- Jurisdictional helpers (currencies, countries, jurisdictions) ---------

#[instrument(skip(db))]
pub async fn ensure_currency(db: &Db, code: &str, name: &str, minor_unit: i16) -> Result<i64> {
    if let Some(rec) = sqlx::query("SELECT id FROM currencies WHERE code=$1")
        .persistent(false)
        .bind(code)
        .fetch_optional(&db.pool)
        .await?
    {
        return Ok(rec.get::<i64, _>("id"));
    }
    let has_minor_unit = table_column_exists(db, "currencies", "minor_unit")
        .await
        .unwrap_or(false);
    // Some legacy schemas don't have minor_unit; default to 2 implicitly.
    let rec = if has_minor_unit {
        sqlx::query(
            "INSERT INTO currencies (code,name,minor_unit) VALUES ($1,$2,$3) \
             ON CONFLICT (code) DO UPDATE SET name=EXCLUDED.name, minor_unit=EXCLUDED.minor_unit \
             RETURNING id",
        )
        .persistent(false)
        .bind(code)
        .bind(name)
        .bind(minor_unit)
        .fetch_one(&db.pool)
        .await?
    } else {
        sqlx::query(
            "INSERT INTO currencies (code,name) VALUES ($1,$2) \
             ON CONFLICT (code) DO UPDATE SET name=EXCLUDED.name \
             RETURNING id",
        )
        .persistent(false)
        .bind(code)
        .bind(name)
        .fetch_one(&db.pool)
        .await?
    };
    Ok(rec.get("id"))
}

#[instrument(skip(db))]
pub async fn ensure_country(db: &Db, iso2: &str, name: &str, currency_id: i64) -> Result<i64> {
    let schema = country_schema(db).await?;
    let iso2_clean = iso2.trim().to_ascii_uppercase();
    if iso2_clean.len() != 2 {
        anyhow::bail!(
            "invalid ISO-3166 alpha-2 code '{}': expected 2 characters",
            iso2
        );
    }

    let code_col = schema.code_col.as_deref().ok_or_else(|| {
        anyhow::anyhow!("countries table missing code column (iso2/country_code/code2/code)")
    })?;

    let select_sql = format!("SELECT id FROM countries WHERE {}=$1", code_col);
    if let Some(rec) = sqlx::query(&select_sql)
        .persistent(false)
        .bind(&iso2_clean)
        .fetch_optional(&db.pool)
        .await?
    {
        return Ok(rec.get::<i64, _>("id"));
    }

    let iso3_val = if schema.has_iso3 {
        iso3_from_iso2(&iso2_clean)
    } else {
        None
    };

    use sqlx::QueryBuilder;
    let mut cols: Vec<&str> = Vec::new();
    cols.push(code_col);
    if schema.has_iso3 && iso3_val.is_some() && code_col != "iso3" {
        cols.push("iso3");
    }
    if schema.has_code2 && code_col != "code2" {
        cols.push("code2");
    }
    if schema.has_name {
        cols.push("name");
    }
    if schema.has_currency_id {
        cols.push("currency_id");
    }

    let mut qb: QueryBuilder<'_, sqlx::Postgres> = QueryBuilder::new("INSERT INTO countries (");
    for (i, c) in cols.iter().enumerate() {
        if i > 0 {
            qb.push(", ");
        }
        qb.push(*c);
    }
    qb.push(") VALUES (");
    let iso3_string = iso3_val
        .map(|s| s.to_string())
        .unwrap_or_else(|| iso2_clean.clone());
    for (i, col) in cols.iter().enumerate() {
        if i > 0 {
            qb.push(", ");
        }
        if *col == code_col {
            qb.push_bind(iso2_clean.clone());
        } else if *col == "iso3" {
            qb.push_bind(iso3_string.clone());
        } else if *col == "code2" {
            qb.push_bind(iso2_clean.clone());
        } else if *col == "name" {
            qb.push_bind(name.to_string());
        } else if *col == "currency_id" {
            qb.push_bind(currency_id);
        } else {
            qb.push_bind(iso2_clean.clone());
        }
    }
    qb.push(")");
    if let Some(col) = schema.code_col.as_ref() {
        qb.push(" ON CONFLICT (");
        qb.push(col.as_str());
        qb.push(") DO UPDATE SET ");

        // CRITICAL: Update all columns to prevent conflicts with triggers/constraints.
        // Similar to video_game_titles fix - incomplete ON CONFLICT causes duplicate key errors.
        let mut update_parts: Vec<String> = Vec::new();

        if schema.has_name {
            update_parts.push("name=EXCLUDED.name".to_string());
        }
        if schema.has_currency_id {
            update_parts.push("currency_id=EXCLUDED.currency_id".to_string());
        }
        if schema.has_iso3 && col.as_str() != "iso3" {
            update_parts.push("iso3=EXCLUDED.iso3".to_string());
        }
        if schema.has_code2 && col.as_str() != "code2" {
            update_parts.push("code2=EXCLUDED.code2".to_string());
        }

        if update_parts.is_empty() {
            // Fallback: update at least one column to make ON CONFLICT valid
            qb.push("name=EXCLUDED.name");
        } else {
            qb.push(&update_parts.join(", "));
        }
    }
    qb.push(" RETURNING id");

    let rec = qb.build().persistent(false).fetch_one(&db.pool).await?;
    Ok(rec.get("id"))
}

fn iso3_from_iso2(code: &str) -> Option<&'static str> {
    match code {
        // Some providers may pass language code 'ZH' where a country code is expected;
        // treat it as China to avoid ingestion failures.
        "ZH" => Some("CHN"),
        "US" => Some("USA"),
        "CA" => Some("CAN"),
        "MX" => Some("MEX"),
        "BR" => Some("BRA"),
        "AR" => Some("ARG"),
        "CL" => Some("CHL"),
        "CO" => Some("COL"),
        "PE" => Some("PER"),
        "UY" => Some("URY"),
        "GB" => Some("GBR"),
        "IE" => Some("IRL"),
        "FR" => Some("FRA"),
        "DE" => Some("DEU"),
        "ES" => Some("ESP"),
        "IT" => Some("ITA"),
        "NL" => Some("NLD"),
        "BE" => Some("BEL"),
        "PT" => Some("PRT"),
        "LU" => Some("LUX"),
        "AT" => Some("AUT"),
        "CH" => Some("CHE"),
        "SE" => Some("SWE"),
        "NO" => Some("NOR"),
        "DK" => Some("DNK"),
        "FI" => Some("FIN"),
        "IS" => Some("ISL"),
        "GR" => Some("GRC"),
        "PL" => Some("POL"),
        "CZ" => Some("CZE"),
        "SK" => Some("SVK"),
        "HU" => Some("HUN"),
        "SI" => Some("SVN"),
        "HR" => Some("HRV"),
        "RO" => Some("ROU"),
        "BG" => Some("BGR"),
        "EE" => Some("EST"),
        "LV" => Some("LVA"),
        "LT" => Some("LTU"),
        "MT" => Some("MLT"),
        "CY" => Some("CYP"),
        "UA" => Some("UKR"),
        "RU" => Some("RUS"),
        "TR" => Some("TUR"),
        "SA" => Some("SAU"),
        "AE" => Some("ARE"),
        "IL" => Some("ISR"),
        "EG" => Some("EGY"),
        "ZA" => Some("ZAF"),
        "NG" => Some("NGA"),
        "KE" => Some("KEN"),
        "JP" => Some("JPN"),
        "KR" => Some("KOR"),
        "HK" => Some("HKG"),
        "TW" => Some("TWN"),
        "SG" => Some("SGP"),
        "MY" => Some("MYS"),
        "TH" => Some("THA"),
        "ID" => Some("IDN"),
        "PH" => Some("PHL"),
        "VN" => Some("VNM"),
        "AU" => Some("AUS"),
        "NZ" => Some("NZL"),
        "KZ" => Some("KAZ"),
        "IN" => Some("IND"),
        "PK" => Some("PAK"),
        "BD" => Some("BGD"),
        "LK" => Some("LKA"),
        "QA" => Some("QAT"),
        "KW" => Some("KWT"),
        "BH" => Some("BHR"),
        "OM" => Some("OMN"),
        "JO" => Some("JOR"),
        "DZ" => Some("DZA"),
        "MA" => Some("MAR"),
        "TN" => Some("TUN"),
        "GH" => Some("GHA"),
        "CM" => Some("CMR"),
        "SN" => Some("SEN"),
        _ => None,
    }
}

#[derive(Debug)]
struct CountrySchema {
    has_iso3: bool,
    has_code2: bool,
    has_currency_id: bool,
    has_name: bool,
    code_col: Option<String>,
    code_expr: Option<String>,
}

impl CountrySchema {
    fn code_expr(&self) -> Option<&str> {
        self.code_expr.as_deref()
    }

    fn code_column(&self) -> Option<&str> {
        self.code_col.as_deref()
    }
}

async fn country_schema(db: &Db) -> Result<&'static CountrySchema> {
    COUNTRY_SCHEMA
        .get_or_try_init(|| async {
            let cols: Vec<String> = sqlx::query_scalar(
                "SELECT column_name FROM information_schema.columns \
                 WHERE table_schema = ANY (current_schemas(true)) AND table_name = 'countries'",
            )
            .persistent(false)
            .fetch_all(&db.pool)
            .await?;

            let has_iso2 = cols.iter().any(|c| c.eq_ignore_ascii_case("iso2"));
            let has_iso3 = cols.iter().any(|c| c.eq_ignore_ascii_case("iso3"));
            let has_code2 = cols.iter().any(|c| c.eq_ignore_ascii_case("code2"));
            let has_country_code = cols.iter().any(|c| c.eq_ignore_ascii_case("country_code"));
            let has_code = cols.iter().any(|c| c.eq_ignore_ascii_case("code"));
            let has_currency_id = cols.iter().any(|c| c.eq_ignore_ascii_case("currency_id"));
            let has_name = cols.iter().any(|c| c.eq_ignore_ascii_case("name"));

            let code_col = if has_iso2 {
                Some("iso2".to_string())
            } else if has_country_code {
                Some("country_code".to_string())
            } else if has_code {
                Some("code".to_string())
            } else if has_code2 {
                Some("code2".to_string())
            } else {
                None
            };
            let code_expr = code_col.as_ref().map(|c| format!("c.{}", c));

            Ok::<CountrySchema, anyhow::Error>(CountrySchema {
                has_iso3,
                has_code2,
                has_currency_id,
                has_name,
                code_col,
                code_expr,
            })
        })
        .await
}

#[instrument(skip(db))]
pub async fn ensure_national_jurisdiction(db: &Db, country_id: i64) -> Result<i64> {
    let compat = php_compat_schema(db).await.unwrap_or(false);
    let has_jurisdictions = *JURISDICTIONS_PRESENT
        .get_or_try_init(|| async { table_exists(db, "jurisdictions").await })
        .await?;

    // Supabase / no-migrate / legacy environments may not have jurisdictions.
    // In php-compat mode, we can treat `country_id` as a stand-in "jurisdiction id"
    // as long as `countries` exists (see ensure_offer_jurisdiction compat mapping).
    if !has_jurisdictions {
        if compat {
            let has_countries = table_exists(db, "countries").await.unwrap_or(false);
            if has_countries {
                debug!(
                    country_id,
                    "jurisdictions table missing; using country_id as compat jurisdiction_id"
                );
                return Ok(country_id);
            }
            anyhow::bail!(
                "jurisdictions table missing and countries table missing; cannot derive national jurisdiction in php compat mode (need countries or an existing sku_regions row)"
            );
        }
        warn!("jurisdictions table missing; cannot ensure national jurisdiction — skipping");
        return Ok(0);
    }

    if let Some(rec) =
        sqlx::query("SELECT id FROM jurisdictions WHERE country_id=$1 AND region_code IS NULL")
            .persistent(false)
            .bind(country_id)
            .fetch_optional(&db.pool)
            .await?
    {
        return Ok(rec.get::<i64, _>("id"));
    }

    // Prefer insert; if a concurrent insert happened, re-select.
    let inserted = sqlx::query(
        "INSERT INTO jurisdictions (country_id,region_code) VALUES ($1,NULL) RETURNING id",
    )
    .persistent(false)
    .bind(country_id)
    .fetch_one(&db.pool)
    .await;

    match inserted {
        Ok(rec) => Ok(rec.get("id")),
        Err(e) => {
            let is_unique = match &e {
                sqlx::Error::Database(db_err) => db_err.code().as_deref() == Some("23505"),
                _ => false,
            };
            if !is_unique {
                return Err(e.into());
            }
            let rec = sqlx::query(
                "SELECT id FROM jurisdictions WHERE country_id=$1 AND region_code IS NULL",
            )
            .persistent(false)
            .bind(country_id)
            .fetch_one(&db.pool)
            .await?;
            Ok(rec.get::<i64, _>("id"))
        }
    }
}

fn local_normalize_title(title: &str) -> String {
    title
        .trim()
        .to_lowercase()
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { '-' })
        .collect::<String>()
        .split('-')
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("-")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EditionHint {
    pub has_edition: bool,
    pub label: Option<String>,
}

fn strip_token_edges(token: &str) -> String {
    token
        .trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '\'' && c != '-')
        .to_string()
}

/// Infer whether a title is an "Edition" and (if present) extract the word immediately
/// preceding "Edition" (case-insensitive) from either the title or metadata text.
pub fn edition_hint_from_title_or_metadata(title: &str, metadata: Option<&Value>) -> EditionHint {
    fn scan_text(text: &str) -> EditionHint {
        let raw_tokens: Vec<&str> = text.split_whitespace().collect();
        for (idx, tok) in raw_tokens.iter().enumerate() {
            let clean = strip_token_edges(tok);
            if clean.eq_ignore_ascii_case("edition") {
                if idx == 0 {
                    return EditionHint {
                        has_edition: true,
                        label: None,
                    };
                }
                let prev = strip_token_edges(raw_tokens[idx - 1]);
                let label = if prev.is_empty() { None } else { Some(prev) };
                return EditionHint {
                    has_edition: true,
                    label,
                };
            }
        }
        EditionHint {
            has_edition: false,
            label: None,
        }
    }

    fn scan_json_strings(v: &Value) -> Option<EditionHint> {
        match v {
            Value::String(s) => {
                let hint = scan_text(s);
                if hint.has_edition {
                    Some(hint)
                } else {
                    None
                }
            }
            Value::Array(items) => {
                for item in items {
                    if let Some(hint) = scan_json_strings(item) {
                        return Some(hint);
                    }
                }
                None
            }
            Value::Object(map) => {
                for (_k, val) in map {
                    if let Some(hint) = scan_json_strings(val) {
                        return Some(hint);
                    }
                }
                None
            }
            _ => None,
        }
    }

    let hint = scan_text(title);
    if hint.has_edition {
        return hint;
    }

    if let Some(meta) = metadata {
        // Scan user-facing string values in the JSON (not its serialized representation)
        // so we don't accidentally treat JSON syntax as part of tokens.
        if let Some(hint2) = scan_json_strings(meta) {
            return hint2;
        }
    }

    EditionHint {
        has_edition: false,
        label: None,
    }
}

#[derive(Clone, Copy, Debug)]
struct VideoGamesLaravelColumns {
    has_product_id: bool,
    has_title: bool,
    has_normalized_title: bool,
    has_metadata: bool,
    has_created_at: bool,
    has_updated_at: bool,
    has_last_synced_at: bool,
}

static VIDEO_GAMES_LARAVEL_COLS: OnceCell<VideoGamesLaravelColumns> = OnceCell::const_new();

#[derive(Clone, Copy, Debug, Default)]
struct VideoGamesContentColumns {
    has_synopsis: bool,
    has_display_title: bool,
    has_region_codes: bool,
    has_genres: bool,
    has_release_date: bool,
    has_developer: bool,
    has_metadata: bool,
    metadata_is_jsonb: bool,
    has_average_rating: bool,
    has_rating_count: bool,
    has_rating_updated_at: bool,
    has_sellable_id: bool,
    has_title_id: bool,
}

async fn video_games_content_columns(db: &Db) -> Result<VideoGamesContentColumns> {
    let cols = VIDEO_GAMES_CONTENT_COLS
        .get_or_try_init(|| async {
            if !table_exists(db, "video_games").await.unwrap_or(false) {
                return Ok::<VideoGamesContentColumns, anyhow::Error>(VideoGamesContentColumns {
                    ..Default::default()
                });
            }

            let has_metadata = table_column_exists(db, "video_games", "metadata").await?;
            // Legacy schemas sometimes define metadata as JSON (not JSONB). Detect type to
            // generate a compatible merge/update expression.
            let metadata_is_jsonb = if has_metadata {
                table_column_udt_name(db, "video_games", "metadata")
                    .await?
                    .as_deref()
                    .is_some_and(|t| t.eq_ignore_ascii_case("jsonb"))
            } else {
                false
            };

            Ok::<VideoGamesContentColumns, anyhow::Error>(VideoGamesContentColumns {
                has_synopsis: table_column_exists(db, "video_games", "synopsis").await?,
                has_display_title: table_column_exists(db, "video_games", "display_title").await?,
                has_region_codes: table_column_exists(db, "video_games", "region_codes").await?,
                has_genres: table_column_exists(db, "video_games", "genres").await?,
                has_release_date: table_column_exists(db, "video_games", "release_date").await?,
                has_developer: table_column_exists(db, "video_games", "developer").await?,
                has_metadata,
                metadata_is_jsonb,
                has_average_rating: table_column_exists(db, "video_games", "average_rating")
                    .await?,
                has_rating_count: table_column_exists(db, "video_games", "rating_count").await?,
                has_rating_updated_at: table_column_exists(db, "video_games", "rating_updated_at")
                    .await?,
                has_sellable_id: table_column_exists(db, "video_games", "sellable_id").await?,
                has_title_id: table_column_exists(db, "video_games", "title_id").await?,
            })
        })
        .await?;

    Ok(*cols)
}

async fn best_effort_execute<'a>(
    db: &Db,
    q: sqlx::query::Query<'a, sqlx::Postgres, sqlx::postgres::PgArguments>,
    context: &'static str,
) {
    if let Err(e) = q.execute(&db.pool).await {
        warn!(error=%e, context, "best-effort video_games update failed");
    }
}

/// Best-effort: update `video_games.synopsis` when the column exists.
/// Prefers the longer synopsis to avoid overwriting rich content with shorter strings.
pub async fn update_video_game_synopsis_prefer_longer(
    db: &Db,
    video_game_id: i64,
    synopsis: &str,
) -> Result<()> {
    let cols = video_games_content_columns(db).await?;
    if !cols.has_synopsis {
        return Ok(());
    }
    best_effort_execute(
        db,
        sqlx::query(
            "UPDATE video_games SET synopsis = CASE WHEN synopsis IS NULL OR length(synopsis) < length($1) THEN $1 ELSE synopsis END WHERE id=$2",
        )
        .persistent(false)
        .bind(synopsis)
        .bind(video_game_id),
        "synopsis",
    )
    .await;
    Ok(())
}

/// Best-effort: update `video_games.display_title` and/or union `video_games.region_codes` when columns exist.
pub async fn update_video_game_display_title_and_region(
    db: &Db,
    video_game_id: i64,
    display_title: &str,
    region_code: &str,
) -> Result<()> {
    let cols = video_games_content_columns(db).await?;
    let rc = region_code.to_ascii_uppercase();

    match (cols.has_display_title, cols.has_region_codes) {
        (true, true) => {
            best_effort_execute(
                db,
                sqlx::query(
                    "UPDATE video_games\nSET\n  display_title = COALESCE(display_title, $1),\n  region_codes = CASE\n    WHEN region_codes IS NULL THEN ARRAY[$2]::text[]\n    WHEN array_position(region_codes, $2) IS NULL THEN region_codes || ARRAY[$2]::text[]\n    ELSE region_codes\n  END\nWHERE\n  id = $3\n  AND (display_title IS NULL OR region_codes IS NULL OR array_position(region_codes, $2) IS NULL)",
                )
                .persistent(false)
                .bind(display_title)
                .bind(&rc)
                .bind(video_game_id),
                "display_title+region_codes",
            )
            .await;
        }
        (true, false) => {
            best_effort_execute(
                db,
                sqlx::query(
                    "UPDATE video_games SET display_title = $1 WHERE id=$2 AND display_title IS NULL",
                )
                .persistent(false)
                .bind(display_title)
                .bind(video_game_id),
                "display_title",
            )
            .await;
        }
        (false, true) => {
            best_effort_execute(
                db,
                sqlx::query(
                    "UPDATE video_games\nSET\n  region_codes = CASE\n    WHEN region_codes IS NULL THEN ARRAY[$1]::text[]\n    WHEN array_position(region_codes, $1) IS NULL THEN region_codes || ARRAY[$1]::text[]\n    ELSE region_codes\n  END\nWHERE\n  id = $2\n  AND (region_codes IS NULL OR array_position(region_codes, $1) IS NULL)",
                )
                .persistent(false)
                .bind(&rc)
                .bind(video_game_id),
                "region_codes",
            )
            .await;
        }
        (false, false) => {}
    }

    Ok(())
}

/// Best-effort: overwrite `video_games.genres` when the column exists.
pub async fn update_video_game_genres(
    db: &Db,
    video_game_id: i64,
    genres: &[String],
) -> Result<()> {
    let cols = video_games_content_columns(db).await?;
    if !cols.has_genres {
        return Ok(());
    }
    best_effort_execute(
        db,
        sqlx::query("UPDATE video_games SET genres=$1 WHERE id=$2")
            .persistent(false)
            .bind(genres)
            .bind(video_game_id),
        "genres",
    )
    .await;
    Ok(())
}

/// Best-effort: set `video_games.genres` only if currently NULL/empty.
pub async fn update_video_game_genres_if_empty(
    db: &Db,
    video_game_id: i64,
    genres: &[String],
) -> Result<()> {
    let cols = video_games_content_columns(db).await?;
    if !cols.has_genres {
        return Ok(());
    }
    best_effort_execute(
        db,
        sqlx::query(
            "UPDATE video_games SET genres = CASE WHEN genres IS NULL OR array_length(genres,1)=0 THEN $1 ELSE genres END WHERE id=$2",
        )
        .persistent(false)
        .bind(genres)
        .bind(video_game_id),
        "genres_if_empty",
    )
    .await;
    Ok(())
}

/// Best-effort: set `video_games.release_date` if present (does not overwrite existing).
pub async fn update_video_game_release_date_if_null(
    db: &Db,
    video_game_id: i64,
    release_date: chrono::NaiveDate,
) -> Result<()> {
    let cols = video_games_content_columns(db).await?;
    if !cols.has_release_date {
        return Ok(());
    }
    best_effort_execute(
        db,
        sqlx::query("UPDATE video_games SET release_date = COALESCE(release_date, $1) WHERE id=$2")
            .persistent(false)
            .bind(release_date)
            .bind(video_game_id),
        "release_date",
    )
    .await;
    Ok(())
}

/// Best-effort: set `video_games.developer` if present (does not overwrite existing).
pub async fn update_video_game_developer_if_empty(
    db: &Db,
    video_game_id: i64,
    developer: &str,
) -> Result<()> {
    let cols = video_games_content_columns(db).await?;
    if !cols.has_developer {
        return Ok(());
    }

    let dev = developer.trim();
    if dev.is_empty() {
        return Ok(());
    }

    best_effort_execute(
        db,
        sqlx::query(
            "UPDATE video_games SET developer = CASE WHEN developer IS NULL OR length(trim(developer))=0 THEN $1 ELSE developer END WHERE id=$2",
        )
        .persistent(false)
        .bind(dev)
        .bind(video_game_id),
        "developer",
    )
    .await;

    Ok(())
}

/// Best-effort: merge a JSON object into `video_games.metadata` when the column exists.
pub async fn merge_video_game_metadata(db: &Db, video_game_id: i64, patch: Value) -> Result<()> {
    let cols = video_games_content_columns(db).await?;
    if !cols.has_metadata {
        return Ok(());
    }

    // NOTE: legacy schemas may define metadata as JSON (not JSONB).
    // Use explicit casts so both shapes can be updated without errors.
    let sql = if cols.metadata_is_jsonb {
        "UPDATE video_games SET metadata = (COALESCE(metadata, '{}'::jsonb) || $1::jsonb) WHERE id=$2"
    } else {
        "UPDATE video_games SET metadata = ((COALESCE(metadata::jsonb, '{}'::jsonb) || $1::jsonb)::json) WHERE id=$2"
    };
    best_effort_execute(
        db,
        sqlx::query(sql)
            .persistent(false)
            .bind(patch)
            .bind(video_game_id),
        "metadata_merge",
    )
    .await;
    Ok(())
}

/// Best-effort: set global rating fields if present and currently null.
/// Optimized to use a single UPDATE instead of 3 separate queries.
pub async fn update_video_game_global_rating_if_null(
    db: &Db,
    video_game_id: i64,
    average_rating: Option<f64>,
    rating_count: Option<i64>,
) -> Result<()> {
    let cols = video_games_content_columns(db).await?;
    if !(cols.has_average_rating || cols.has_rating_count || cols.has_rating_updated_at) {
        return Ok(());
    }

    // Build dynamic SQL to update only the columns that exist in the schema
    // This handles schema evolution gracefully while batching all updates into one query
    let mut updates: Vec<String> = Vec::new();
    let mut bind_idx = 2; // $1 is video_game_id

    if cols.has_average_rating && average_rating.is_some() {
        updates.push(format!(
            "average_rating = COALESCE(average_rating, ${})",
            bind_idx
        ));
        bind_idx += 1;
    }

    if cols.has_rating_count && rating_count.is_some() {
        updates.push(format!(
            "rating_count = COALESCE(rating_count, ${})",
            bind_idx
        ));
        bind_idx += 1;
    }

    if cols.has_rating_updated_at && (average_rating.is_some() || rating_count.is_some()) {
        updates.push("rating_updated_at = COALESCE(rating_updated_at, now())".to_string());
    }

    if updates.is_empty() {
        return Ok(());
    }

    // Single UPDATE batches all column updates together
    // COALESCE ensures we only update NULL values (preserves existing data)
    let sql = format!(
        "UPDATE video_games SET {} WHERE id = $1",
        updates.join(", ")
    );

    let mut query = sqlx::query(&sql).persistent(false).bind(video_game_id);

    if cols.has_average_rating && average_rating.is_some() {
        query = query.bind(average_rating);
    }

    if cols.has_rating_count && rating_count.is_some() {
        query = query.bind(rating_count);
    }

    best_effort_execute(db, query, "global_rating_batch_update").await;

    Ok(())
}

async fn video_games_laravel_columns(db: &Db) -> Result<VideoGamesLaravelColumns> {
    let cols = VIDEO_GAMES_LARAVEL_COLS
        .get_or_try_init(|| async {
            if !table_exists(db, "video_games").await.unwrap_or(false) {
                anyhow::bail!("video_games table not found in target DB");
            }

            Ok::<VideoGamesLaravelColumns, anyhow::Error>(VideoGamesLaravelColumns {
                has_product_id: table_column_exists(db, "video_games", "product_id").await?,
                has_title: table_column_exists(db, "video_games", "title").await?,
                has_normalized_title: table_column_exists(db, "video_games", "normalized_title")
                    .await?,
                has_metadata: table_column_exists(db, "video_games", "metadata").await?,
                has_created_at: table_column_exists(db, "video_games", "created_at").await?,
                has_updated_at: table_column_exists(db, "video_games", "updated_at").await?,
                has_last_synced_at: table_column_exists(db, "video_games", "last_synced_at")
                    .await?,
            })
        })
        .await?;
    Ok(*cols)
}

/// Ensure a `video_games` row for Laravel-oriented schemas keyed by `product_id`.
///
/// Returns `video_games.id`.
#[instrument(skip(db, metadata))]
pub async fn ensure_video_game_for_product(
    db: &Db,
    product_id: i64,
    title: &str,
    normalized_title: Option<&str>,
    metadata: Option<Value>,
) -> Result<i64> {
    let cols = video_games_laravel_columns(db).await?;
    if !(cols.has_product_id && cols.has_title) {
        anyhow::bail!("video_games missing required product_id/title columns for Laravel schema");
    }

    let title_trimmed = title.trim();
    let title_final = if title_trimmed.is_empty() {
        "Untitled"
    } else {
        title_trimmed
    };
    let norm_final = normalized_title
        .map(|s| s.to_string())
        .unwrap_or_else(|| local_normalize_title(title_final));
    let meta_final = metadata.unwrap_or_else(|| serde_json::json!({}));

    if let Some(row) =
        sqlx::query("SELECT id FROM video_games WHERE product_id=$1 ORDER BY id DESC LIMIT 1")
            .persistent(false)
            .bind(product_id)
            .fetch_optional(&db.pool)
            .await?
    {
        // For compatibility across varying Laravel schemas, don't attempt to update here.
        // Callers that require updates should perform them explicitly.
        return Ok(row.get("id"));
    }

    // Insert
    let mut columns: Vec<&str> = vec!["product_id", "title"];
    let mut values: Vec<String> = vec!["$1".to_string(), "$2".to_string()];
    let mut bind_norm = false;
    let mut bind_meta = false;
    let mut next_bind = 3;

    if cols.has_normalized_title {
        columns.push("normalized_title");
        values.push(format!("${}", next_bind));
        bind_norm = true;
        next_bind += 1;
    }
    if cols.has_metadata {
        columns.push("metadata");
        values.push(format!("${}", next_bind));
        bind_meta = true;
        // no need to advance further binds after the final optional column
    }
    if cols.has_created_at {
        columns.push("created_at");
        values.push("now()".to_string());
    }
    if cols.has_updated_at {
        columns.push("updated_at");
        values.push("now()".to_string());
    }
    if cols.has_last_synced_at {
        columns.push("last_synced_at");
        values.push("now()".to_string());
    }

    let sql = format!(
        "INSERT INTO video_games ({}) VALUES ({}) RETURNING id",
        columns.join(", "),
        values.join(", ")
    );

    // Some environments may have a desynced `video_games_id_seq` due to bulk imports.
    // If we hit a duplicate PK on insert, repair the sequence and retry once.
    for attempt in 0..2 {
        let mut q = sqlx::query(&sql).persistent(false);
        q = q.bind(product_id).bind(title_final);
        if bind_norm {
            q = q.bind(&norm_final);
        }
        if bind_meta {
            q = q.bind(meta_final.clone());
        }

        match q.fetch_one(&db.pool).await {
            Ok(rec) => return Ok(rec.get("id")),
            Err(e) => {
                let is_dup_pk = match &e {
                    sqlx::Error::Database(db_err) => {
                        db_err.constraint() == Some("video_games_pkey")
                            || db_err
                                .message()
                                .contains("duplicate key value violates unique constraint")
                    }
                    _ => false,
                };

                if attempt == 0 && is_dup_pk {
                    // Best-effort: align serial sequence to max(id)
                    let _ = sqlx::query(
                        r#"
                        SELECT CASE
                          WHEN pg_get_serial_sequence('video_games','id') IS NULL THEN NULL
                          ELSE setval(
                            pg_get_serial_sequence('video_games','id'),
                            (SELECT COALESCE(MAX(id),0) FROM video_games)
                          )
                        END
                        "#,
                    )
                    .persistent(false)
                    .execute(&db.pool)
                    .await;

                    continue;
                }

                return Err(e.into());
            }
        }
    }

    anyhow::bail!("unreachable: ensure_video_game_for_product insert retry loop")
}

#[cfg(test)]
mod edition_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn edition_hint_extracts_previous_word_from_title() {
        let hint = edition_hint_from_title_or_metadata("Mega Deluxe Edition", None);
        assert!(hint.has_edition);
        assert_eq!(hint.label.as_deref(), Some("Deluxe"));
    }

    #[test]
    fn edition_hint_handles_edition_without_previous_word() {
        let hint = edition_hint_from_title_or_metadata("Edition", None);
        assert!(hint.has_edition);
        assert_eq!(hint.label, None);
    }

    #[test]
    fn edition_hint_falls_back_to_metadata_string() {
        let meta = json!({"foo": "Collector's Edition"});
        let hint = edition_hint_from_title_or_metadata("Just a Game", Some(&meta));
        assert!(hint.has_edition);
        assert_eq!(hint.label.as_deref(), Some("Collector's"));
    }
}

// --------- Platform helpers ---------

// Detailed normalization + key generation lives in `crate::normalization::platform`.

#[instrument(skip(db))]
pub async fn ensure_platform(db: &Db, name: &str, _slug: Option<&str>) -> Result<i64> {
    if !table_exists(db, "platforms").await.unwrap_or(false) {
        return Ok(0);
    }

    fn normalize_platform_name_and_code(
        name: &str,
        slug: Option<&str>,
    ) -> (String, Option<String>, Vec<String>) {
        let raw = name.trim();
        let raw_lc = raw.to_ascii_lowercase();

        // Normalize PlayStation synonyms:
        // ps4/PS4 == playstation 4 == playstation-4 == playstation4
        // ps5/PS5 == playstation 5 == playstation-5 == playstation5
        if raw_lc == "ps4"
            || raw_lc == "playstation4"
            || raw_lc == "playstation-4"
            || raw_lc == "playstation 4"
        {
            let code = slug
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_ascii_lowercase())
                .unwrap_or_else(|| "ps4".to_string());
            let synonyms = vec![
                "ps4".to_string(),
                "playstation4".to_string(),
                "playstation-4".to_string(),
                "playstation 4".to_string(),
            ];
            return ("PS4".to_string(), Some(code), synonyms);
        }
        if raw_lc == "ps5"
            || raw_lc == "playstation5"
            || raw_lc == "playstation-5"
            || raw_lc == "playstation 5"
        {
            let code = slug
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_ascii_lowercase())
                .unwrap_or_else(|| "ps5".to_string());
            let synonyms = vec![
                "ps5".to_string(),
                "playstation5".to_string(),
                "playstation-5".to_string(),
                "playstation 5".to_string(),
            ];
            return ("PS5".to_string(), Some(code), synonyms);
        }

        // Default: keep name as-is (trimmed); compute a best-effort `code` if provided.
        let code = slug
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_ascii_lowercase());
        (raw.to_string(), code, vec![raw_lc])
    }

    let (name_norm, code_norm, name_synonyms_lc) = normalize_platform_name_and_code(name, _slug);

    if let Some(rec) = sqlx::query("SELECT id FROM platforms WHERE name=$1")
        .persistent(false)
        .bind(&name_norm)
        .fetch_optional(&db.pool)
        .await?
    {
        return Ok(rec.get("id"));
    }

    // Some schemas require a non-null `code` column. Detect it once and populate if present.
    static PLATFORM_CODE_PRESENT: OnceCell<bool> = OnceCell::const_new();
    static PLATFORM_FAMILY_PRESENT: OnceCell<bool> = OnceCell::const_new();
    let has_code = *PLATFORM_CODE_PRESENT
        .get_or_try_init(|| async {
            let exists: Option<bool> = sqlx::query_scalar(
                "SELECT TRUE FROM information_schema.columns WHERE table_name='platforms' AND column_name='code' AND table_schema = ANY (current_schemas(true)) LIMIT 1",
            )
            .persistent(false)
            .fetch_optional(&db.pool)
            .await?;
            Ok::<bool, anyhow::Error>(exists.unwrap_or(false))
        })
        .await?;
    let has_family = *PLATFORM_FAMILY_PRESENT
        .get_or_try_init(|| async {
            let exists: Option<bool> = sqlx::query_scalar(
                "SELECT TRUE FROM information_schema.columns WHERE table_name='platforms' AND column_name='family' AND table_schema = ANY (current_schemas(true)) LIMIT 1",
            )
            .persistent(false)
            .fetch_optional(&db.pool)
            .await?;
            Ok::<bool, anyhow::Error>(exists.unwrap_or(false))
        })
        .await?;

    // If we can, also match by code (or by known name synonyms) to collapse PS4/PS5 variants.
    if has_code {
        if let Some(code) = code_norm.as_ref() {
            if let Some(rec) =
                sqlx::query("SELECT id FROM platforms WHERE code=$1 OR lower(name) = ANY($2)")
                    .persistent(false)
                    .bind(code)
                    .bind(&name_synonyms_lc)
                    .fetch_optional(&db.pool)
                    .await?
            {
                return Ok(rec.get("id"));
            }
        } else if let Some(rec) =
            sqlx::query("SELECT id FROM platforms WHERE lower(name) = ANY($1)")
                .persistent(false)
                .bind(&name_synonyms_lc)
                .fetch_optional(&db.pool)
                .await?
        {
            return Ok(rec.get("id"));
        }
    } else if let Some(rec) = sqlx::query("SELECT id FROM platforms WHERE lower(name) = ANY($1)")
        .persistent(false)
        .bind(&name_synonyms_lc)
        .fetch_optional(&db.pool)
        .await?
    {
        return Ok(rec.get("id"));
    }

    // Fuzzy platform normalization (Jaro–Winkler ≥ 0.80), with numeric distinctions preserved.
    //
    // This collapses variants like:
    //  - "PlayStation 5" / "PlayStation®5" / "PAL-PlayStation 5" / "PS5"
    // into a single canonical platform row.
    let input_key = PlatformKey::new(&name_norm);

    let candidates = sqlx::query("SELECT id, name FROM platforms")
        .persistent(false)
        .fetch_all(&db.pool)
        .await?;

    let mut best_id: Option<i64> = None;
    let mut best_sim: f64 = 0.0;

    for row in candidates {
        let cand_id: i64 = row.get("id");
        let cand_name: String = row.get("name");
        let cand_key = PlatformKey::new(&cand_name);

        if !input_key.numeric_compatible(&cand_key) {
            continue;
        }

        let sim = input_key.similarity(&cand_key);
        if sim >= MIN_PLATFORM_SIMILARITY && sim > best_sim {
            best_sim = sim;
            best_id = Some(cand_id);
        }
    }

    if let Some(id) = best_id {
        return Ok(id);
    }

    // Insert without slug - let DB use defaults/constraints
    let inserted = if has_code && has_family {
        sqlx::query(
            "INSERT INTO platforms (name, code, family) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING RETURNING id",
        )
        .persistent(false)
        .bind(&name_norm)
        .bind(code_norm.as_deref().unwrap_or(&name_norm))
        .bind(&name_norm)
        .fetch_optional(&db.pool)
        .await?
    } else if has_code {
        sqlx::query(
            "INSERT INTO platforms (name, code) VALUES ($1,$2) ON CONFLICT DO NOTHING RETURNING id",
        )
        .persistent(false)
        .bind(&name_norm)
        .bind(code_norm.as_deref().unwrap_or(&name_norm))
        .fetch_optional(&db.pool)
        .await?
    } else if has_family {
        sqlx::query(
            "INSERT INTO platforms (name, family) VALUES ($1,$2) ON CONFLICT DO NOTHING RETURNING id",
        )
        .persistent(false)
        .bind(&name_norm)
        .bind(&name_norm)
        .fetch_optional(&db.pool)
        .await?
    } else {
        sqlx::query("INSERT INTO platforms (name) VALUES ($1) ON CONFLICT DO NOTHING RETURNING id")
            .persistent(false)
            .bind(&name_norm)
            .fetch_optional(&db.pool)
            .await?
    };

    // If insert returned nothing (conflict), fetch the existing ID
    if let Some(row) = inserted {
        return Ok(row.get("id"));
    }

    // Fetch existing ID after conflict (e.g., duplicate or RLS-hidden row)
    if let Some(rec) = sqlx::query("SELECT id FROM platforms WHERE name=$1")
        .persistent(false)
        .bind(&name_norm)
        .fetch_optional(&db.pool)
        .await?
    {
        return Ok(rec.get("id"));
    }

    // If still not found (likely RLS hiding the row), insert a compat row with a
    // distinct name to obtain an accessible id and keep ingest moving.
    let compat_name = format!("{} (compat)", name_norm);
    let compat_insert = if has_code && has_family {
        sqlx::query("INSERT INTO platforms (name, code, family) VALUES ($1,$2,$3) RETURNING id")
            .persistent(false)
            .bind(&compat_name)
            .bind(code_norm.as_deref().unwrap_or(&compat_name))
            .bind(&compat_name)
            .fetch_one(&db.pool)
            .await?
    } else if has_code {
        sqlx::query("INSERT INTO platforms (name, code) VALUES ($1,$2) RETURNING id")
            .persistent(false)
            .bind(&compat_name)
            .bind(code_norm.as_deref().unwrap_or(&compat_name))
            .fetch_one(&db.pool)
            .await?
    } else if has_family {
        sqlx::query("INSERT INTO platforms (name, family) VALUES ($1,$2) RETURNING id")
            .persistent(false)
            .bind(&compat_name)
            .bind(&compat_name)
            .fetch_one(&db.pool)
            .await?
    } else {
        sqlx::query("INSERT INTO platforms (name) VALUES ($1) RETURNING id")
            .persistent(false)
            .bind(&compat_name)
            .fetch_one(&db.pool)
            .await?
    };

    Ok(compat_insert.get("id"))
}

// --------- Rating normalization (provider field aliases) ---------

static RATING_MAPPER: OnceLock<RatingMapper> = OnceLock::new();

fn provider_rating_mapper() -> &'static RatingMapper {
    RATING_MAPPER.get_or_init(|| {
        RatingMapper::with_defaults()
            .register(
                "igdb",
                RatingAlias::new("aggregated_rating", RatingStrategy::ZeroToHundred),
            )
            .register(
                "playstation_store",
                RatingAlias::new("product_star_rating", RatingStrategy::StarString),
            )
            .register(
                "psstore",
                RatingAlias::new("product_star_rating", RatingStrategy::StarString),
            )
            .register(
                "provider_a",
                RatingAlias::new("user_ratings", RatingStrategy::ZeroToFive),
            )
            .register(
                "provider_b",
                RatingAlias::new("aggregated_rating", RatingStrategy::ZeroToHundred),
            )
            .register(
                "provider_c",
                RatingAlias::new("product_star_rating", RatingStrategy::StarString),
            )
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RatingScale {
    ZeroToFive,
    ZeroToHundred,
    StarString,
}

#[derive(Debug, Clone, Copy)]
struct RatingFieldMapping {
    field: &'static str,
    scale: RatingScale,
}

// Configuration-driven (static) mapping:
// - Provider A: user_ratings (float 0-5)
// - Provider B: aggregated_rating (int 0-100) → normalize to 0-5
// - Provider C: product_star_rating (string "1-5 stars")
const RATING_FIELD_MAPPINGS: &[RatingFieldMapping] = &[
    RatingFieldMapping {
        field: "user_ratings",
        scale: RatingScale::ZeroToFive,
    },
    RatingFieldMapping {
        field: "aggregated_rating",
        scale: RatingScale::ZeroToHundred,
    },
    RatingFieldMapping {
        field: "product_star_rating",
        scale: RatingScale::StarString,
    },
    // Common generic key:
    RatingFieldMapping {
        field: "rating",
        scale: RatingScale::ZeroToFive,
    },
    // Common variants:
    RatingFieldMapping {
        field: "average_rating",
        scale: RatingScale::ZeroToFive,
    },
    RatingFieldMapping {
        field: "averageRating",
        scale: RatingScale::ZeroToFive,
    },
    RatingFieldMapping {
        field: "userRating",
        scale: RatingScale::ZeroToFive,
    },
    RatingFieldMapping {
        field: "user_rating",
        scale: RatingScale::ZeroToFive,
    },
    RatingFieldMapping {
        field: "metacritic",
        scale: RatingScale::ZeroToHundred,
    },
    RatingFieldMapping {
        field: "metacritic_score",
        scale: RatingScale::ZeroToHundred,
    },
];

fn normalize_key_for_match(s: &str) -> String {
    // Lowercase + strip non-alphanumeric so we can match:
    // - averageRating == average_rating == average-rating
    // - userRatings == user_ratings
    s.chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .map(|c| c.to_ascii_lowercase())
        .collect::<String>()
}

fn key_tokens(key: &str) -> Vec<String> {
    // Tokenize camelCase + snake_case + kebab-case keys.
    let mut out: Vec<String> = Vec::new();
    let mut cur = String::new();
    let mut prev_lower = false;

    for ch in key.chars() {
        if ch.is_ascii_alphanumeric() {
            if ch.is_ascii_uppercase() && prev_lower {
                if !cur.is_empty() {
                    out.push(cur.to_ascii_lowercase());
                    cur.clear();
                }
            }
            cur.push(ch);
            prev_lower = ch.is_ascii_lowercase();
        } else {
            if !cur.is_empty() {
                out.push(cur.to_ascii_lowercase());
                cur.clear();
            }
            prev_lower = false;
        }
    }

    if !cur.is_empty() {
        out.push(cur.to_ascii_lowercase());
    }

    out
}

fn key_looks_like_rating(tokens: &[String]) -> bool {
    // Be strict enough to avoid matching unrelated words like "operating" (contains "rating" substring).
    // We rely on tokenization: operating -> ["operating"], not ["op", "rating"].
    let has_rating = tokens.iter().any(|t| t == "rating" || t == "ratings");
    let has_stars = tokens.iter().any(|t| t == "star" || t == "stars");
    let has_metacritic = tokens.iter().any(|t| t.contains("metacritic"));
    // Only treat "score" as rating-ish if it is paired with a strong qualifier.
    let has_score = tokens.iter().any(|t| t == "score" || t == "scores");
    let score_qualified = tokens
        .iter()
        .any(|t| t == "meta" || t == "critic" || t.contains("metacritic"));

    has_rating || has_stars || has_metacritic || (has_score && score_qualified)
}

fn normalize_rating_number_to_five(raw: f64, tokens: &[String]) -> Option<f64> {
    if !raw.is_finite() {
        return None;
    }

    // Ignore obviously-not-ratings.
    if raw.abs() > 10_000.0 {
        return None;
    }

    let mut r = if raw >= 0.0 && raw <= 5.0 {
        raw
    } else if raw > 5.0 && raw <= 10.0 {
        // Common 0..10 scale.
        raw / 2.0
    } else if raw > 10.0 && raw <= 100.0 {
        // Common 0..100 score (e.g., Metacritic).
        raw / 20.0
    } else {
        // Unknown scale.
        return None;
    };

    // If the key screams "metacritic" or "score", prefer treating 0..100 as score.
    if tokens
        .iter()
        .any(|t| t.contains("metacritic") || t == "score" || t == "scores")
    {
        if raw > 10.0 && raw <= 100.0 {
            r = raw / 20.0;
        }
    }

    if r < 0.0 {
        r = 0.0;
    }
    if r > 5.0 {
        r = 5.0;
    }
    Some(r)
}

fn parse_rating_string_to_five(s: &str, tokens: &[String]) -> Option<f64> {
    let mut s = s.trim().to_ascii_lowercase();
    if s.is_empty() {
        return None;
    }

    // Tolerate comma decimals ("4,5") in some feeds.
    if s.contains(',') && !s.contains('.') {
        s = s.replace(',', ".");
    }

    // Percent form: "80%".
    if let Some(idx) = s.find('%') {
        let left = s[..idx].trim();
        if let Ok(v) = left.parse::<f64>() {
            return normalize_rating_number_to_five(v, &vec!["score".to_string()]);
        }
    }

    // "x/y" form.
    if let Some((lhs, rhs)) = s.split_once('/') {
        let lhs = lhs.trim();
        let rhs = rhs
            .trim()
            .trim_end_matches("stars")
            .trim_end_matches("star")
            .trim();
        if let (Ok(num), Ok(den)) = (lhs.parse::<f64>(), rhs.parse::<f64>()) {
            if den > 0.0 {
                let scaled = (num / den) * 5.0;
                return normalize_rating_number_to_five(scaled, tokens);
            }
        }
    }

    // "x out of y" form.
    if s.contains("out of") {
        let parts: Vec<&str> = s.split("out of").collect();
        if parts.len() == 2 {
            let lhs = parts[0].trim();
            let rhs = parts[1]
                .trim()
                .trim_end_matches("stars")
                .trim_end_matches("star")
                .trim();
            if let (Ok(num), Ok(den)) = (lhs.parse::<f64>(), rhs.parse::<f64>()) {
                if den > 0.0 {
                    let scaled = (num / den) * 5.0;
                    return normalize_rating_number_to_five(scaled, tokens);
                }
            }
        }
    }

    // "stars" free-form.
    if s.contains("star") {
        if let Some(v) = parse_star_string_rating(&s) {
            return normalize_rating_number_to_five(v, tokens);
        }
    }

    // Fallback: parse leading number, then guess scale.
    let mut buf = String::new();
    for ch in s.chars() {
        if ch.is_ascii_digit() || ch == '.' {
            buf.push(ch);
        } else if !buf.is_empty() {
            break;
        }
    }
    let v = buf.parse::<f64>().ok()?;
    normalize_rating_number_to_five(v, tokens)
}

fn parse_star_string_rating(s: &str) -> Option<f64> {
    // Accept forms like:
    //  - "4"
    //  - "4.5"
    //  - "4.5 stars"
    //  - "4/5 stars"
    let s = s.trim().to_ascii_lowercase();
    let s = s.replace("stars", "").replace("star", "");
    let s = s.trim();
    if s.is_empty() {
        return None;
    }

    // Prefer the leading number.
    let mut buf = String::new();
    for ch in s.chars() {
        if ch.is_ascii_digit() || ch == '.' {
            buf.push(ch);
        } else {
            break;
        }
    }
    buf.parse::<f64>().ok()
}

fn normalize_rating_value_to_five(scale: RatingScale, value: &serde_json::Value) -> Option<f64> {
    let raw = match value {
        serde_json::Value::Number(n) => n.as_f64(),
        serde_json::Value::String(s) => match scale {
            RatingScale::StarString => parse_star_string_rating(s),
            _ => s.trim().parse::<f64>().ok(),
        },
        _ => None,
    }?;

    let mut r = match scale {
        RatingScale::ZeroToFive => raw,
        RatingScale::ZeroToHundred => raw / 20.0,
        RatingScale::StarString => raw,
    };

    if !r.is_finite() {
        return None;
    }

    // Clamp to 0..5 to keep bad provider data from poisoning the DB.
    if r < 0.0 {
        r = 0.0;
    }
    if r > 5.0 {
        r = 5.0;
    }

    Some(r)
}

fn find_value_by_key_recursive<'a>(
    v: &'a serde_json::Value,
    key: &str,
) -> Option<&'a serde_json::Value> {
    fn inner<'a>(
        v: &'a serde_json::Value,
        key: &str,
        key_norm: &str,
    ) -> Option<&'a serde_json::Value> {
        match v {
            serde_json::Value::Object(map) => {
                for (k, vv) in map {
                    if k.eq_ignore_ascii_case(key) || normalize_key_for_match(k) == key_norm {
                        return Some(vv);
                    }
                }
                for vv in map.values() {
                    if let Some(found) = inner(vv, key, key_norm) {
                        return Some(found);
                    }
                }
                None
            }
            serde_json::Value::Array(arr) => arr.iter().find_map(|vv| inner(vv, key, key_norm)),
            _ => None,
        }
    }

    let key_norm = normalize_key_for_match(key);
    inner(v, key, &key_norm)
}

/// Best-effort rating extraction from a provider payload.
///
/// Returns a normalized 0–5 rating by combining three strategies:
/// 1. **Provider overrides:** a registry of `provider_key -> field` mappings that allow
///    each ingestion job to declare its canonical rating column (e.g. `user_ratings` → `0..5`,
///    `aggregated_rating` → `0..100`). This covers the explicit requirements documented in the
///    Video Game Source registry (provider A/B/C mappings from the project charter).
/// 2. **Alias lookup:** a curated list of common rating field names (including casing/format
///    variants) that is scanned recursively through the payload. We normalize formats like
///    `snake_case`, `camelCase`, `kebab-case`, and nested JSON structures with `{ value, max }` pairs.
/// 3. **Fuzzy heuristics:** a depth-first walk across the payload that gives preference to keys
///    containing tokens such as `rating`, `stars`, `average`, or `metacritic`, then attempts to scale
///    whichever numeric/text value is found into the 0–5 range.
///
/// This layered approach ensures that every provider column defined in the ingestion requirements
/// (e.g. `user_ratings`, `aggregated_rating`, `product_star_rating`) is honored while still giving us
/// a resilient fallback for partially documented feeds. Providers adopting new aliases should register
/// them in the mapper so this function remains deterministic.
pub(crate) fn extract_normalized_rating_from_payload(
    provider_key: Option<&str>,
    payload: &serde_json::Value,
) -> Option<f64> {
    if let Some(provider) = provider_key {
        if let Some(mapped) = provider_rating_mapper().map(provider, payload) {
            return Some(mapped as f64);
        }
    }

    // 1) Fast path: known alias keys.
    for m in RATING_FIELD_MAPPINGS {
        if let Some(v) = find_value_by_key_recursive(payload, m.field) {
            if let Some(r) = normalize_rating_value_to_five(m.scale, v) {
                return Some(r);
            }

            // If the value is a string and the mapping scale isn't StarString, still try common formats.
            if let serde_json::Value::String(s) = v {
                let tokens = key_tokens(m.field);
                if let Some(r) = parse_rating_string_to_five(s, &tokens) {
                    return Some(r);
                }
            }
        }
    }

    // 2) Fuzzy scan: walk the payload and look for rating-like keys.
    fn walk(v: &serde_json::Value, best: &mut Option<(i32, f64)>) {
        match v {
            serde_json::Value::Object(map) => {
                for (k, vv) in map {
                    let tokens = key_tokens(k);
                    if key_looks_like_rating(&tokens) {
                        let candidate = match vv {
                            serde_json::Value::Number(n) => n
                                .as_f64()
                                .and_then(|f| normalize_rating_number_to_five(f, &tokens)),
                            serde_json::Value::String(s) => parse_rating_string_to_five(s, &tokens),
                            serde_json::Value::Object(obj) => {
                                // Common nested shapes: {value,max} or {score,max}.
                                let value = obj
                                    .get("value")
                                    .or_else(|| obj.get("rating"))
                                    .or_else(|| obj.get("score"))
                                    .or_else(|| obj.get("avg"))
                                    .or_else(|| obj.get("average"));

                                let max = obj
                                    .get("max")
                                    .or_else(|| obj.get("out_of"))
                                    .or_else(|| obj.get("scale"))
                                    .or_else(|| obj.get("denominator"));

                                match (value, max) {
                                    (Some(val), Some(maxv)) => {
                                        let max_num = match maxv {
                                            serde_json::Value::Number(n) => n.as_f64(),
                                            serde_json::Value::String(s) => s.parse::<f64>().ok(),
                                            _ => None,
                                        };

                                        let val_num = match val {
                                            serde_json::Value::Number(n) => n.as_f64(),
                                            serde_json::Value::String(s) => s.parse::<f64>().ok(),
                                            _ => None,
                                        };

                                        match (val_num, max_num) {
                                            (Some(vn), Some(mx)) if mx > 0.0 => {
                                                let scaled = (vn / mx) * 5.0;
                                                normalize_rating_number_to_five(scaled, &tokens)
                                            }
                                            (Some(vn), _) => {
                                                normalize_rating_number_to_five(vn, &tokens)
                                            }
                                            _ => None,
                                        }
                                    }
                                    (Some(val), None) => match val {
                                        serde_json::Value::Number(n) => n.as_f64().and_then(|f| {
                                            normalize_rating_number_to_five(f, &tokens)
                                        }),
                                        serde_json::Value::String(s) => {
                                            parse_rating_string_to_five(s, &tokens)
                                        }
                                        _ => None,
                                    },
                                    _ => None,
                                }
                            }
                            _ => None,
                        };

                        if let Some(r) = candidate {
                            // Score: prefer specific keys.
                            let mut score: i32 = 0;
                            if tokens.iter().any(|t| t.contains("metacritic")) {
                                score += 6;
                            }
                            if tokens.iter().any(|t| t == "average" || t == "avg") {
                                score += 4;
                            }
                            if tokens.iter().any(|t| t == "rating" || t == "ratings") {
                                score += 3;
                            }
                            if tokens.iter().any(|t| t == "star" || t == "stars") {
                                score += 2;
                            }
                            if tokens.iter().any(|t| t == "score" || t == "scores") {
                                score += 1;
                            }

                            match best {
                                Some((best_score, _)) if score <= *best_score => {}
                                _ => *best = Some((score, r)),
                            }
                        }
                    }

                    walk(vv, best);
                }
            }
            serde_json::Value::Array(arr) => {
                for vv in arr {
                    walk(vv, best);
                }
            }
            _ => {}
        }
    }

    let mut best: Option<(i32, f64)> = None;
    walk(payload, &mut best);
    best.map(|(_, r)| r)
}

#[cfg(test)]
mod rating_normalization_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn rating_user_ratings_zero_to_five_pass_through() {
        let payload = json!({"user_ratings": 4.25});
        let r = extract_normalized_rating_from_payload(Some("provider_a"), &payload).unwrap();
        assert!((r - 4.25).abs() < 0.0001);
    }

    #[test]
    fn rating_aggregated_rating_zero_to_hundred_normalizes() {
        let payload = json!({"aggregated_rating": 80});
        let r = extract_normalized_rating_from_payload(Some("provider_b"), &payload).unwrap();
        assert!((r - 4.0).abs() < 0.0001);
    }

    #[test]
    fn rating_product_star_rating_parses_string() {
        let payload = json!({"product_star_rating": "4.5 stars"});
        let r = extract_normalized_rating_from_payload(Some("provider_c"), &payload).unwrap();
        assert!((r - 4.5).abs() < 0.0001);
    }

    #[test]
    fn rating_accepts_key_variants_and_formats() {
        let payload = json!({
            "averageRating": "4.2/5",
            "meta": {"metacritic_score": 80},
            "other": {"userRating": {"value": 8, "max": 10}}
        });
        let r = extract_normalized_rating_from_payload(None, &payload).unwrap();
        // averageRating should win here.
        assert!((r - 4.2).abs() < 0.0001);
    }

    #[test]
    fn rating_does_not_match_operating_as_rating() {
        let payload = json!({"operating": "windows", "version": 11});
        assert!(extract_normalized_rating_from_payload(None, &payload).is_none());
    }
}

// --------- Provider offers ---------

#[instrument(skip(db))]
pub async fn link_provider_offer(
    db: &Db,
    video_game_source_id: i64,
    offer_id: i64,
    confidence: Option<f32>,
) -> Result<i64> {
    // Some legacy schemas don't have provider_items/provider_offers; treat this mapping as optional.
    if video_game_source_id == 0 {
        return Ok(0);
    }
    if !provider_offers_present(db).await.unwrap_or(false) {
        return Ok(0);
    }

    if let Some(rec) =
        sqlx::query("SELECT id FROM provider_offers WHERE video_game_source_id=$1 AND offer_id=$2")
            .persistent(false)
            .bind(video_game_source_id)
            .bind(offer_id)
            .fetch_optional(&db.pool)
            .await?
    {
        return Ok(rec.get("id"));
    }

    let has_confidence = table_column_exists(db, "provider_offers", "confidence")
        .await
        .unwrap_or(false);

    let inserted = if has_confidence {
        sqlx::query(
            "INSERT INTO provider_offers (video_game_source_id, offer_id, confidence) VALUES ($1,$2,$3) RETURNING id",
        )
        .persistent(false)
        .bind(video_game_source_id)
        .bind(offer_id)
        .bind(confidence)
        .fetch_one(&db.pool)
        .await?
    } else {
        sqlx::query(
            "INSERT INTO provider_offers (video_game_source_id, offer_id) VALUES ($1,$2) RETURNING id",
        )
        .persistent(false)
        .bind(video_game_source_id)
        .bind(offer_id)
        .fetch_one(&db.pool)
        .await?
    };
    Ok(inserted.get("id"))
}

// --------- Provider media links with metadata ---------

/// Normalize provider/source identifiers before persisting them into media tables.
///
/// Why this exists:
/// - In code we refer to PlayStation Store as `psstore`.
/// - Legacy imports (e.g. from SQLite) may derive `psn` (from hostnames) or `ps-store`
///   (provider slug) for the same underlying ecosystem.
/// - `game_media.source` is often an enum (`media_source`) and will hard-fail on unknown
///   values when we cast (e.g. `$2::media_source`).
///
/// Keep this mapping intentionally small and explicit to avoid accidentally remapping
/// other provider identifiers.
fn normalize_media_source(source: &str) -> std::borrow::Cow<'_, str> {
    let s = source.trim();
    if s.eq_ignore_ascii_case("psstore")
        || s.eq_ignore_ascii_case("ps-store")
        || s.eq_ignore_ascii_case("ps_store")
        || s.eq_ignore_ascii_case("psn")
        || s.eq_ignore_ascii_case("playstation")
        || s.eq_ignore_ascii_case("playstation-store")
        || s.eq_ignore_ascii_case("playstation_store")
    {
        // Canonical in this repo.
        return std::borrow::Cow::Borrowed("psstore");
    }
    std::borrow::Cow::Borrowed(s)
}

fn normalize_media_source_for_db(
    source: &str,
    supports_psstore: bool,
) -> std::borrow::Cow<'_, str> {
    let norm = normalize_media_source(source);
    if norm.eq_ignore_ascii_case("psstore") && !supports_psstore {
        // Compatibility: older DBs only have `psn` in the `media_source` enum.
        return std::borrow::Cow::Borrowed("psn");
    }
    norm
}

fn normalize_media_type_for_db(
    media_type: &str,
    supports_background: bool,
) -> std::borrow::Cow<'_, str> {
    let s = media_type.trim();
    if s.eq_ignore_ascii_case("background") && !supports_background {
        // Compatibility: older DBs don't have `background` in the `media_type` enum.
        return std::borrow::Cow::Borrowed("artwork");
    }
    std::borrow::Cow::Borrowed(s)
}

fn media_group_priority(
    media_type: Option<&str>,
    role: Option<&str>,
    url: &str,
) -> (i32, &'static str) {
    // Lower number = higher priority.
    let mt = media_type.unwrap_or("").trim().to_ascii_lowercase();
    let rl = role.unwrap_or("").trim().to_ascii_lowercase();
    let u = url.trim().to_ascii_lowercase();

    // Heuristics are intentionally conservative; we only depend on already-present DB-side data.
    // Covers/backgrounds/artwork should come before screenshots.
    if mt == "cover"
        || rl == "cover"
        || mt == "boxart"
        || rl == "boxart"
        || u.contains("cover")
        || u.contains("boxart")
    {
        return (0, "cover");
    }
    if mt == "background"
        || rl == "background"
        || u.contains("background")
        || u.contains("wallpaper")
    {
        return (1, "background");
    }
    if mt == "artwork" || rl == "artwork" || rl == "faart" || u.contains("keyart") {
        return (2, "artwork");
    }
    if mt == "screenshot" || rl == "screenshot" || u.contains("screenshot") {
        return (3, "screenshot");
    }
    (9, "other")
}

fn normalize_provider_media_role(media_group: &str, role_raw: Option<&str>) -> Option<String> {
    // Requirement: screenshots belong to gallery.
    if media_group.eq_ignore_ascii_case("screenshot") {
        return Some("gallery".to_string());
    }

    // Preserve upstream role if present, otherwise provide a stable, useful role.
    if let Some(r) = role_raw.map(|s| s.trim()).filter(|s| !s.is_empty()) {
        return Some(r.to_string());
    }

    match media_group {
        "cover" => Some("cover".to_string()),
        "background" => Some("background".to_string()),
        "artwork" => Some("artwork".to_string()),
        _ => None,
    }
}

#[instrument(skip(db, urls, meta))]
pub async fn ensure_vg_source_media_links_with_meta(
    db: &Db,
    video_game_source_id: i64,
    video_game_id: Option<i64>,
    urls: &[(String, Option<String>, Option<String>, Option<String>)],
    source: &str,
    meta: Option<Value>,
) -> Result<usize> {
    if video_game_source_id == 0 {
        return Ok(0);
    }
    if !provider_media_links_present(db).await.unwrap_or(false) {
        return Ok(0);
    }
    let supports_psstore = media_source_supports_psstore(db).await.unwrap_or(false);
    let supports_background = media_type_supports_background(db).await.unwrap_or(false);
    let source_norm = normalize_media_source_for_db(source, supports_psstore);

    let has_sort_order = table_column_exists(db, "canonical_media", "sort_order")
        .await
        .unwrap_or(false);
    let has_position = if has_sort_order {
        false
    } else {
        table_column_exists(db, "canonical_media", "position")
            .await
            .unwrap_or(false)
    };

    let metadata_base = meta.as_ref().filter(|v| !v.is_null()).cloned();

    // Pre-process all URLs and build arrays for batch insert
    // This replaces the individual INSERT loop with data preparation
    let mut seen: HashSet<String> = HashSet::new();
    let mut batch_urls: Vec<String> = Vec::new();
    let mut batch_video_game_ids: Vec<Option<i64>> = Vec::new();
    let mut batch_sources: Vec<Option<String>> = Vec::new();
    let mut batch_media_types: Vec<Option<String>> = Vec::new();
    let mut batch_roles: Vec<Option<String>> = Vec::new();
    let mut batch_titles: Vec<Option<String>> = Vec::new();
    let mut batch_sort_orders: Vec<i32> = Vec::new();
    let mut batch_metadata: Vec<Value> = Vec::new();

    for (idx, (url, media_type_raw, role_raw, title_raw)) in urls.iter().enumerate() {
        let trimmed = url.trim();
        if trimmed.is_empty() {
            continue;
        }
        let normalized = trimmed.to_string();
        if !seen.insert(normalized.clone()) {
            continue;
        }
        let media_type_candidate = media_type_raw
            .as_ref()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty());
        let role_candidate = role_raw
            .as_ref()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty());

        let (prio, group) = media_group_priority(media_type_candidate, role_candidate, &normalized);
        let sort_order: i32 = prio.saturating_mul(1000) + (idx as i32);

        let media_type = media_type_candidate
            .map(|s| normalize_media_type_for_db(s, supports_background).to_ascii_lowercase());

        let role = normalize_provider_media_role(group, role_candidate);

        let row_meta: Value = if !has_sort_order && !has_position {
            let mut row_meta: Value = metadata_base.clone().unwrap_or_else(|| json!({}));
            if let Value::Object(map) = &mut row_meta {
                map.insert("gc_sort_order".to_string(), json!(sort_order));
                map.insert("gc_media_group".to_string(), json!(group));
            }
            row_meta
        } else {
            metadata_base.clone().unwrap_or_else(|| json!({}))
        };

        let title = title_raw
            .as_ref()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());

        // Accumulate values for batch insert
        batch_urls.push(normalized);
        batch_video_game_ids.push(video_game_id);
        batch_sources.push(Some(source_norm.to_string()));
        batch_media_types.push(media_type);
        batch_roles.push(role);
        batch_titles.push(title);
        batch_sort_orders.push(sort_order);
        batch_metadata.push(row_meta);
    }

    if batch_urls.is_empty() {
        return Ok(0);
    }

    let count = batch_urls.len();

    // Execute single batch INSERT using UNNEST to expand arrays
    // UNNEST transforms arrays into rows, allowing us to insert all media links in one query
    // The ON CONFLICT clause maintains idempotency - re-running is safe
    if has_sort_order {
        sqlx::query(
            "INSERT INTO canonical_media
                (url, url_hash, metadata)
             SELECT
                url,
                canonical_media_url_hash(url),
                jsonb_build_object(
                    'video_game_source_id', $1::bigint,
                    'video_game_id', video_game_id,
                    'source', source,
                    'media_type', media_type,
                    'role', role,
                    'title', title,
                    'sort_order', sort_order
                ) || COALESCE(metadata, '{}'::jsonb)
             FROM UNNEST(
                $2::bigint[],
                $3::text[],
                $4::text[],
                $5::text[],
                $6::text[],
                $7::text[],
                $8::int[],
                $9::jsonb[]
             ) AS t(video_game_id, url, source, media_type, role, title, sort_order, metadata)
             ON CONFLICT (url_hash) DO UPDATE
             SET metadata = canonical_media.metadata || EXCLUDED.metadata,
                 updated_at = now()",
        )
        .persistent(false)
        .bind(video_game_source_id)
        .bind(&batch_video_game_ids)
        .bind(&batch_urls)
        .bind(&batch_sources)
        .bind(&batch_media_types)
        .bind(&batch_roles)
        .bind(&batch_titles)
        .bind(&batch_sort_orders)
        .bind(&batch_metadata)
        .execute(&db.pool)
        .await?;
    } else if has_position {
        sqlx::query(
            "INSERT INTO canonical_media
                (url, url_hash, metadata)
             SELECT
                url,
                canonical_media_url_hash(url),
                jsonb_build_object(
                    'video_game_source_id', $1::bigint,
                    'video_game_id', video_game_id,
                    'source', source,
                    'media_type', media_type,
                    'role', role,
                    'title', title,
                    'position', sort_order
                ) || COALESCE(metadata, '{}'::jsonb)
             FROM UNNEST(
                $2::bigint[],
                $3::text[],
                $4::text[],
                $5::text[],
                $6::text[],
                $7::text[],
                $8::int[],
                $9::jsonb[]
             ) AS t(video_game_id, url, source, media_type, role, title, sort_order, metadata)
             ON CONFLICT (url_hash) DO UPDATE
             SET metadata = canonical_media.metadata || EXCLUDED.metadata,
                 updated_at = now()",
        )
        .persistent(false)
        .bind(video_game_source_id)
        .bind(&batch_video_game_ids)
        .bind(&batch_urls)
        .bind(&batch_sources)
        .bind(&batch_media_types)
        .bind(&batch_roles)
        .bind(&batch_titles)
        .bind(&batch_sort_orders)
        .bind(&batch_metadata)
        .execute(&db.pool)
        .await?;
    } else {
        sqlx::query(
            "INSERT INTO canonical_media
                (url, url_hash, metadata)
             SELECT
                url,
                canonical_media_url_hash(url),
                jsonb_build_object(
                    'video_game_source_id', $1::bigint,
                    'video_game_id', video_game_id,
                    'source', source,
                    'media_type', media_type,
                    'role', role,
                    'title', title
                ) || COALESCE(metadata, '{}'::jsonb)
             FROM UNNEST(
                $2::bigint[],
                $3::text[],
                $4::text[],
                $5::text[],
                $6::text[],
                $7::text[],
                $8::jsonb[]
             ) AS t(video_game_id, url, source, media_type, role, title, metadata)
             ON CONFLICT (url_hash) DO UPDATE
             SET metadata = canonical_media.metadata || EXCLUDED.metadata,
                 updated_at = now()",
        )
        .persistent(false)
        .bind(video_game_source_id)
        .bind(&batch_video_game_ids)
        .bind(&batch_urls)
        .bind(&batch_sources)
        .bind(&batch_media_types)
        .bind(&batch_roles)
        .bind(&batch_titles)
        .bind(&batch_metadata)
        .execute(&db.pool)
        .await?;
    }

    Ok(count)
}

// --------- Provider media links (simple version) ---------

#[instrument(skip(db, urls))]
pub async fn ensure_vg_source_media_links(
    db: &Db,
    video_game_source_id: i64,
    urls: &[String],
) -> Result<()> {
    for url in urls {
        let _ = ensure_vg_source_media_link(db, video_game_source_id, url).await?;
    }
    Ok(())
}

// --------- Game media upsert (single row) ---------

#[instrument(skip(db, provider_data))]
pub async fn upsert_game_media(
    db: &Db,
    video_game_id: i64,
    source: &str,
    external_id: &str,
    media_type: &str,
    url: &str,
    provider_data: Value,
) -> Result<()> {
    // If the media table isn't present in this deployment, treat as best-effort.
    if !table_exists(db, "game_media").await.unwrap_or(false) {
        return Ok(());
    }

    let supports_psstore = media_source_supports_psstore(db).await.unwrap_or(false);
    let supports_background = media_type_supports_background(db).await.unwrap_or(false);
    let source_norm = normalize_media_source_for_db(source, supports_psstore);
    let media_type_norm = normalize_media_type_for_db(media_type, supports_background);
    // Modern schema:
    //   game_media(video_game_id, source, external_id, media_type, title, url, provider_data, ...)
    // Legacy PHP/Laravel schema (observed on older Supabase DBs):
    //   game_media(video_game_id, kind, slug, title, fetched_at, metadata, ...)
    //   UNIQUE (video_game_id, kind, slug)
    // For legacy we store url + provider_data inside `metadata` JSON.
    if game_media_has_source_col(db).await.unwrap_or(false) {
        let source_is_enum = game_media_source_is_enum(db).await.unwrap_or(false);
        let media_type_is_enum = game_media_media_type_is_enum(db).await.unwrap_or(false);
        let source_cast = if source_is_enum { "::media_source" } else { "" };
        let media_type_cast = if media_type_is_enum {
            "::media_type"
        } else {
            ""
        };
        let sql = format!(
            "INSERT INTO game_media (video_game_id, source, external_id, media_type, title, url, provider_data) \
               VALUES ($1, $2{source_cast}, $3, $4{media_type_cast}, $5, $6, $7) \
             ON CONFLICT (video_game_id, source, external_id) DO UPDATE \
             SET url = EXCLUDED.url, provider_data = EXCLUDED.provider_data, title = EXCLUDED.title",
            source_cast = source_cast,
            media_type_cast = media_type_cast
        );
        for attempt in 0..2 {
            let res = sqlx::query(&sql)
                .persistent(false)
                .bind(video_game_id)
                .bind(source_norm.as_ref())
                .bind(external_id)
                .bind(media_type_norm.as_ref())
                .bind(media_type_norm.as_ref())
                .bind(url)
                .bind(provider_data.clone())
                .execute(&db.pool)
                .await;

            match res {
                Ok(_) => return Ok(()),
                Err(e) => {
                    let is_dup_pk = match &e {
                        sqlx::Error::Database(db_err) => {
                            db_err.constraint() == Some("game_media_pkey")
                                || db_err
                                    .message()
                                    .contains("duplicate key value violates unique constraint")
                        }
                        _ => false,
                    };

                    if attempt == 0 && is_dup_pk {
                        let _ = sqlx::query(
                            r#"
                            SELECT CASE
                              WHEN pg_get_serial_sequence('game_media','id') IS NULL THEN NULL
                              ELSE setval(
                                pg_get_serial_sequence('game_media','id'),
                                (SELECT COALESCE(MAX(id),0) FROM game_media)
                              )
                            END
                            "#,
                        )
                        .persistent(false)
                        .execute(&db.pool)
                        .await;
                        continue;
                    }

                    return Err(e.into());
                }
            }
        }

        anyhow::bail!("unreachable: upsert_game_media modern insert retry loop")
    }

    let kind = format!("{}:{}", source_norm.as_ref(), media_type_norm.as_ref());
    let slug = external_id.trim();
    if slug.is_empty() {
        return Ok(());
    }
    let legacy_meta = serde_json::json!({
        "source": source_norm.as_ref(),
        "external_id": external_id,
        "media_type": media_type_norm.as_ref(),
        "url": url,
        "provider_data": provider_data,
    })
    .to_string();

    for attempt in 0..2 {
        let res = sqlx::query(
            "INSERT INTO game_media (video_game_id, kind, slug, title, fetched_at, metadata, created_at, updated_at) \
               VALUES ($1, $2, $3, $4, now(), $5::json, now(), now()) \
             ON CONFLICT (video_game_id, kind, slug) DO UPDATE \
             SET title = EXCLUDED.title, fetched_at = EXCLUDED.fetched_at, metadata = EXCLUDED.metadata, updated_at = now()",
        )
        .persistent(false)
        .bind(video_game_id)
        .bind(&kind)
        .bind(slug)
        .bind(media_type)
        .bind(legacy_meta.clone())
        .execute(&db.pool)
        .await;

        match res {
            Ok(_) => return Ok(()),
            Err(e) => {
                let is_dup_pk = match &e {
                    sqlx::Error::Database(db_err) => {
                        db_err.constraint() == Some("game_media_pkey")
                            || db_err
                                .message()
                                .contains("duplicate key value violates unique constraint")
                    }
                    _ => false,
                };

                if attempt == 0 && is_dup_pk {
                    let _ = sqlx::query(
                        r#"
                        SELECT CASE
                          WHEN pg_get_serial_sequence('game_media','id') IS NULL THEN NULL
                          ELSE setval(
                            pg_get_serial_sequence('game_media','id'),
                            (SELECT COALESCE(MAX(id),0) FROM game_media)
                          )
                        END
                        "#,
                    )
                    .persistent(false)
                    .execute(&db.pool)
                    .await;
                    continue;
                }

                return Err(e.into());
            }
        }
    }

    anyhow::bail!("unreachable: upsert_game_media legacy insert retry loop")
}

// =============================
// CANONICAL MEDIA DEDUPLICATION
// =============================

/// Check if canonical_media table exists
async fn canonical_media_exists(db: &Db) -> Result<bool> {
    table_exists(db, "canonical_media").await
}

/// Ensure a canonical media entry exists for a given URL
/// Returns the canonical_media_id for use in media tables
///
/// This function implements URL deduplication by:
/// 1. Computing SHA256 hash of the URL
/// 2. Upserting into canonical_media table
/// 3. Returning the canonical_media_id
///
/// # Arguments
/// * `db` - Database connection
/// * `url` - Media URL to deduplicate
/// * `width` - Optional image width
/// * `height` - Optional image height
/// * `mime_type` - Optional MIME type
/// * `size_bytes` - Optional file size
/// * `hash` - Optional content hash
///
/// # Example
/// ```rust
/// let canonical_id = ensure_canonical_media(
///     &db,
///     "https://cdn.example.com/image.jpg",
///     Some(1920),
///     Some(1080),
///     Some("image/jpeg"),
///     Some(524288),
///     None,
/// ).await?;
///
/// // Use canonical_id when inserting into game_media, game_images, etc.
/// ```
#[instrument(skip(db))]
pub async fn ensure_canonical_media(
    db: &Db,
    url: &str,
    width: Option<i32>,
    height: Option<i32>,
    mime_type: Option<&str>,
    size_bytes: Option<i64>,
    hash: Option<&str>,
) -> Result<i64> {
    // If canonical_media table doesn't exist, return 0 (no-op)
    if !canonical_media_exists(db).await.unwrap_or(false) {
        debug!("canonical_media table not present, skipping deduplication");
        return Ok(0);
    }

    // Validate URL
    if url.trim().is_empty() {
        return Err(anyhow!("URL cannot be empty"));
    }

    // Insert or get existing canonical_media entry
    // The database function canonical_media_url_hash(url) computes SHA256
    let result = sqlx::query_scalar::<_, i64>(
        r#"
        INSERT INTO canonical_media (url, url_hash, width, height, mime_type, size_bytes, hash)
        VALUES (
            $1,
            canonical_media_url_hash($1),
            $2,
            $3,
            $4,
            $5,
            $6
        )
        ON CONFLICT (url_hash) DO UPDATE
        SET access_count = canonical_media.access_count + 1,
            updated_at = now(),
            -- Update metadata if provided and previously NULL
            width = COALESCE(canonical_media.width, EXCLUDED.width),
            height = COALESCE(canonical_media.height, EXCLUDED.height),
            mime_type = COALESCE(canonical_media.mime_type, EXCLUDED.mime_type),
            size_bytes = COALESCE(canonical_media.size_bytes, EXCLUDED.size_bytes),
            hash = COALESCE(canonical_media.hash, EXCLUDED.hash)
        RETURNING id
        "#,
    )
    .persistent(false)
    .bind(url)
    .bind(width)
    .bind(height)
    .bind(mime_type)
    .bind(size_bytes)
    .bind(hash)
    .fetch_one(&db.pool)
    .await?;

    debug!(
        canonical_media_id = result,
        "Ensured canonical media for URL"
    );
    Ok(result)
}

/// Enhanced version of upsert_game_media that uses canonical_media deduplication
/// Falls back to standard upsert_game_media if canonical_media table doesn't exist
///
/// # Arguments
/// * `db` - Database connection
/// * `video_game_id` - Video game ID
/// * `source` - Provider source (e.g., "igdb", "rawg")
/// * `external_id` - External media ID from provider
/// * `media_type` - Type of media (e.g., "cover", "screenshot")
/// * `url` - Media URL
/// * `provider_data` - Additional provider metadata
/// * `width` - Optional image width
/// * `height` - Optional image height
/// * `mime_type` - Optional MIME type
///
/// # Example
/// ```rust
/// upsert_game_media_with_dedup(
///     &db,
///     video_game_id,
///     "igdb",
///     "12345",
///     "cover",
///     "https://images.igdb.com/cover.jpg",
///     json!({"size": "original"}),
///     Some(1920),
///     Some(1080),
///     Some("image/jpeg"),
/// ).await?;
/// ```
#[instrument(skip(db, provider_data))]
pub async fn upsert_game_media_with_dedup(
    db: &Db,
    video_game_id: i64,
    source: &str,
    external_id: &str,
    media_type: &str,
    url: &str,
    provider_data: Value,
    width: Option<i32>,
    height: Option<i32>,
    mime_type: Option<&str>,
) -> Result<()> {
    // First ensure canonical_media entry exists
    let canonical_media_id = ensure_canonical_media(
        db, url, width, height, mime_type, None, // size_bytes
        None, // hash
    )
    .await
    .ok(); // Ignore errors if table doesn't exist

    // If canonical_media table exists and we got an ID, use enhanced insert
    if let Some(canonical_id) = canonical_media_id {
        if canonical_id > 0 && table_exists(db, "game_media").await.unwrap_or(false) {
            let supports_psstore = media_source_supports_psstore(db).await.unwrap_or(false);
            let supports_background = media_type_supports_background(db).await.unwrap_or(false);
            let source_norm = normalize_media_source_for_db(source, supports_psstore);
            let media_type_norm = normalize_media_type_for_db(media_type, supports_background);

            if game_media_has_source_col(db).await.unwrap_or(false) {
                let source_is_enum = game_media_source_is_enum(db).await.unwrap_or(false);
                let media_type_is_enum = game_media_media_type_is_enum(db).await.unwrap_or(false);
                let source_cast = if source_is_enum { "::media_source" } else { "" };
                let media_type_cast = if media_type_is_enum {
                    "::media_type"
                } else {
                    ""
                };

                // Check if canonical_media_id column exists in game_media
                let has_canonical_col = sqlx::query_scalar::<_, bool>(
                    "SELECT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name = 'game_media'
                        AND column_name = 'canonical_media_id'
                    )",
                )
                .persistent(false)
                .fetch_one(&db.pool)
                .await
                .unwrap_or(false);

                if has_canonical_col {
                    let sql = format!(
                        "INSERT INTO game_media (video_game_id, source, external_id, media_type, title, url, provider_data, canonical_media_id, width, height, mime_type) \
                           VALUES ($1, $2{source_cast}, $3, $4{media_type_cast}, $5, $6, $7, $8, $9, $10, $11) \
                         ON CONFLICT (video_game_id, source, external_id) DO UPDATE \
                         SET url = EXCLUDED.url,
                             provider_data = EXCLUDED.provider_data,
                             title = EXCLUDED.title,
                             canonical_media_id = EXCLUDED.canonical_media_id,
                             width = COALESCE(EXCLUDED.width, game_media.width),
                             height = COALESCE(EXCLUDED.height, game_media.height),
                             mime_type = COALESCE(EXCLUDED.mime_type, game_media.mime_type)",
                        source_cast = source_cast,
                        media_type_cast = media_type_cast
                    );

                    sqlx::query(&sql)
                        .persistent(false)
                        .bind(video_game_id)
                        .bind(source_norm.as_ref())
                        .bind(external_id)
                        .bind(media_type_norm.as_ref())
                        .bind(media_type_norm.as_ref()) // title = media_type
                        .bind(url)
                        .bind(provider_data)
                        .bind(canonical_id)
                        .bind(width)
                        .bind(height)
                        .bind(mime_type)
                        .execute(&db.pool)
                        .await?;

                    debug!(
                        video_game_id,
                        canonical_media_id = canonical_id,
                        "Inserted game_media with canonical_media_id"
                    );
                    return Ok(());
                }
            }
        }
    }

    // Fallback to standard upsert_game_media if:
    // - canonical_media table doesn't exist
    // - canonical_media_id column doesn't exist in game_media
    // - Any error occurred during canonical media creation
    upsert_game_media(
        db,
        video_game_id,
        source,
        external_id,
        media_type,
        url,
        provider_data,
    )
    .await
}

/// Update existing game_images rows to reference canonical_media
/// This is a migration helper to backfill canonical_media_id on existing rows
///
/// # Arguments
/// * `db` - Database connection
/// * `batch_size` - Number of rows to process per batch (default: 1000)
///
/// # Returns
/// Number of rows updated
///
/// # Example
/// ```rust
/// let updated = backfill_game_images_canonical_media(&db, 5000).await?;
/// info!("Updated {} game_images rows with canonical_media_id", updated);
/// ```
#[instrument(skip(db))]
pub async fn backfill_game_images_canonical_media(db: &Db, batch_size: i64) -> Result<i64> {
    if !canonical_media_exists(db).await.unwrap_or(false) {
        return Ok(0);
    }

    if !table_exists(db, "game_images").await.unwrap_or(false) {
        return Ok(0);
    }

    // Check if canonical_media_id column exists
    let has_col = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'game_images'
            AND column_name = 'canonical_media_id'
        )",
    )
    .persistent(false)
    .fetch_one(&db.pool)
    .await
    .unwrap_or(false);

    if !has_col {
        debug!("game_images.canonical_media_id column not present, skipping backfill");
        return Ok(0);
    }

    let result = sqlx::query_scalar::<_, i64>(
        r#"
        WITH batch AS (
            SELECT DISTINCT gi.id, gi.url
            FROM game_images gi
            WHERE gi.canonical_media_id IS NULL
              AND gi.url IS NOT NULL
              AND length(gi.url) > 0
            LIMIT $1
        ),
        ensured AS (
            INSERT INTO canonical_media (url, url_hash, width, height, mime_type)
            SELECT DISTINCT
                b.url,
                canonical_media_url_hash(b.url),
                gi.width,
                gi.height,
                gi.mime_type
            FROM batch b
            JOIN game_images gi ON gi.url = b.url
            ON CONFLICT (url_hash) DO UPDATE
            SET access_count = canonical_media.access_count + 1
            RETURNING id, url
        ),
        updated AS (
            UPDATE game_images gi
            SET canonical_media_id = e.id
            FROM ensured e
            WHERE gi.url = e.url
              AND gi.canonical_media_id IS NULL
            RETURNING gi.id
        )
        SELECT count(*) FROM updated
        "#,
    )
    .persistent(false)
    .bind(batch_size)
    .fetch_one(&db.pool)
    .await?;

    info!(
        rows_updated = result,
        "Backfilled game_images canonical_media_id"
    );
    Ok(result)
}

/// Update existing game_videos rows to reference canonical_media
/// Similar to backfill_game_images_canonical_media but for game_videos table
#[instrument(skip(db))]
pub async fn backfill_game_videos_canonical_media(db: &Db, batch_size: i64) -> Result<i64> {
    if !canonical_media_exists(db).await.unwrap_or(false) {
        return Ok(0);
    }

    if !table_exists(db, "game_videos").await.unwrap_or(false) {
        return Ok(0);
    }

    let has_col = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'game_videos'
            AND column_name = 'canonical_media_id'
        )",
    )
    .persistent(false)
    .fetch_one(&db.pool)
    .await
    .unwrap_or(false);

    if !has_col {
        debug!("game_videos.canonical_media_id column not present, skipping backfill");
        return Ok(0);
    }

    let result = sqlx::query_scalar::<_, i64>(
        r#"
        WITH batch AS (
            SELECT DISTINCT gv.id, gv.stream_url as url
            FROM game_videos gv
            WHERE gv.canonical_media_id IS NULL
              AND gv.stream_url IS NOT NULL
              AND length(gv.stream_url) > 0
            LIMIT $1
        ),
        ensured AS (
            INSERT INTO canonical_media (url, url_hash, mime_type)
            SELECT DISTINCT
                b.url,
                canonical_media_url_hash(b.url),
                'video/mp4'
            FROM batch b
            ON CONFLICT (url_hash) DO UPDATE
            SET access_count = canonical_media.access_count + 1
            RETURNING id, url
        ),
        updated AS (
            UPDATE game_videos gv
            SET canonical_media_id = e.id
            FROM ensured e
            WHERE gv.stream_url = e.url
              AND gv.canonical_media_id IS NULL
            RETURNING gv.id
        )
        SELECT count(*) FROM updated
        "#,
    )
    .persistent(false)
    .bind(batch_size)
    .fetch_one(&db.pool)
    .await?;

    info!(
        rows_updated = result,
        "Backfilled game_videos canonical_media_id"
    );
    Ok(result)
}
