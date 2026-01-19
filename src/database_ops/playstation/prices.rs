use crate::util::env::{self as env_util, preflight_check};
use anyhow::{bail, Result};
use chrono::{DateTime, Duration as ChronoDuration, NaiveDate, NaiveDateTime, Utc};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    env,
};
use tracing::{info, warn};

use crate::database_ops::db::{CurrentPriceRow, Db, PriceRow};
use crate::database_ops::ingest_providers::{
    edition_hint_from_title_or_metadata, ensure_country, ensure_currency, ensure_game_provider,
    ensure_national_jurisdiction, ensure_platform, ensure_provider, ensure_retailer,
    ensure_video_game_source, ingest_run_finish, ingest_run_start, link_provider_offer,
    php_compat_schema, update_video_game_display_title_and_region, update_video_game_genres,
    update_video_game_release_date_if_null, update_video_game_synopsis_prefer_longer,
    PostIngestSummary, ProviderEntityCache,
};
use crate::database_ops::media_map::MediaMap;
use psstore_client::{PsConfig, PsProductSummary, PsStoreClient};

const PS_STORE_PROVIDER_KEY: &str = "ps-store";

fn sha256_hex(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    let digest = hasher.finalize();
    let mut out = String::with_capacity(64);
    for b in digest {
        use std::fmt::Write;
        let _ = write!(&mut out, "{:02x}", b);
    }
    out
}

fn normalize_url_varchar_255(url: &str) -> Option<String> {
    // Our consolidated schema stores URLs in varchar(255) for game_images/game_videos.
    // Prefer preserving a usable remote URL over storing a truncated/broken string.
    if url.len() <= 255 {
        return Some(url.to_string());
    }

    // Strip fragment then query string.
    let no_fragment = url.split('#').next().unwrap_or(url);
    let no_query = no_fragment.split('?').next().unwrap_or(no_fragment);

    if no_query.len() <= 255 {
        Some(no_query.to_string())
    } else {
        None
    }
}

fn truncate_varchar_255(s: &str) -> String {
    if s.len() <= 255 {
        return s.to_string();
    }

    // Strings here are expected to be ASCII-ish titles; keep it simple and safe.
    s.chars().take(255).collect::<String>()
}

fn psstore_is_allowed_image(url: &str, role: Option<&str>) -> bool {
    // Keep only representative artwork from PS Store.
    // Explicitly exclude screenshots/logos/icons (and thumbnails), which were cluttering the DB
    // and are not desired for our canonical media pipeline.
    let url_lc = url.to_ascii_lowercase();
    let role_lc = role.unwrap_or("").to_ascii_lowercase();

    // Denylist first.
    let deny =
        |s: &str| s.contains("screenshot") || s.contains("screenshots") || s.contains("thumb");
    if deny(&url_lc) || deny(&role_lc) {
        return false;
    }

    // Allowlist: keep obvious cover/hero/keyart/background/artwork.
    let allow = |s: &str| {
        s.contains("cover")
            || s.contains("hero")
            || s.contains("background")
            || s.contains("keyart")
            || s.contains("artwork")
    };
    if allow(&url_lc) {
        return true;
    }

    // Some PS payloads provide role reliably even when the URL isn't descriptive.
    if allow(&role_lc) {
        return true;
    }

    // Default: treat as screenshot-like and skip.
    false
}

fn normalize_title(s: &str) -> String {
    s.to_lowercase()
        .replace(|c: char| !c.is_ascii_alphanumeric(), "-")
        .trim_matches('-')
        .to_string()
}

fn psstore_is_allowed_image_url(url: &str) -> bool {
    // Keep only representative artwork from PS Store.
    // Explicitly exclude screenshots/logos/icons (and thumbnails), which were cluttering the DB
    // and are not desired for our canonical media pipeline.
    let url_lc = url.to_ascii_lowercase();

    // Denylist first.
    if url_lc.contains("screenshot")
        || url_lc.contains("screenshots")
        || url_lc.contains("logo")
        || url_lc.contains("icon")
        || url_lc.contains("thumbnail")
        || url_lc.contains("thumb")
        || url_lc.contains("character")
    {
        return false;
    }

    // Allowlist: keep obvious cover/hero/keyart/background/artwork.
    if url_lc.contains("cover")
        || url_lc.contains("hero")
        || url_lc.contains("background")
        || url_lc.contains("keyart")
        || url_lc.contains("artwork")
        || url_lc.contains("character")
    {
        return true;
    }

    // Default: treat as screenshot-like and skip.
    false
}

#[cfg(test)]
mod tests {
    use super::psstore_is_allowed_image_url;

    #[test]
    fn psstore_media_url_filter_rejects_screenshots_logos_and_icons() {
        assert!(!psstore_is_allowed_image_url(
            "https://example.test/foo/screenshot_01.jpg"
        ));
        assert!(!psstore_is_allowed_image_url(
            "https://example.test/assets/logo.png"
        ));
        assert!(!psstore_is_allowed_image_url(
            "https://example.test/assets/icon.png"
        ));
        assert!(!psstore_is_allowed_image_url(
            "https://example.test/assets/thumbnail.png"
        ));
        assert!(!psstore_is_allowed_image_url(
            "https://example.test/assets/thumb.png"
        ));

        // Unknown-ish assets default to excluded.
        assert!(!psstore_is_allowed_image_url(
            "https://example.test/assets/image_01.jpg"
        ));
    }

    #[test]
    fn psstore_media_url_filter_allows_cover_hero_background_artwork() {
        assert!(psstore_is_allowed_image_url(
            "https://example.test/assets/cover.jpg"
        ));
        assert!(psstore_is_allowed_image_url(
            "https://example.test/assets/hero.jpg"
        ));
        assert!(psstore_is_allowed_image_url(
            "https://example.test/assets/background.jpg"
        ));
        assert!(psstore_is_allowed_image_url(
            "https://example.test/assets/keyart.jpg"
        ));
        assert!(psstore_is_allowed_image_url(
            "https://example.test/assets/artwork.jpg"
        ));
    }
}

fn parse_release_date_any(raw: &str) -> Option<NaiveDate> {
    if let Ok(date) = NaiveDate::parse_from_str(raw, "%Y-%m-%d") {
        return Some(date);
    }
    if let Ok(date_time) = NaiveDateTime::parse_from_str(raw, "%Y-%m-%dT%H:%M:%S") {
        return Some(date_time.date());
    }
    if let Ok(date_time) = NaiveDateTime::parse_from_str(raw, "%Y-%m-%dT%H:%M:%S%.f") {
        return Some(date_time.date());
    }
    if let Ok(dt) = DateTime::parse_from_rfc3339(raw) {
        return Some(dt.date_naive());
    }
    None
}

async fn ratings_conflict_supported(db: &Db) -> Result<bool> {
    // Check for a unique or primary key that covers (video_game_id, locale) on video_game_ratings_by_locale.
    // Fallback to false if table or constraint is absent, so callers can avoid ON CONFLICT targets on legacy schemas.
    let exists: Option<bool> = sqlx::query_scalar(
                "SELECT TRUE FROM pg_constraint c
                 JOIN pg_class t ON t.oid = c.conrelid
                 JOIN pg_namespace n ON n.oid = t.relnamespace
                 WHERE t.relname = 'video_game_ratings_by_locale'
                     AND n.nspname = ANY (current_schemas(true))
                     AND c.contype IN ('u','p')
                     AND c.conkey::text = (SELECT array_agg(attnum)::text FROM pg_attribute WHERE attrelid = t.oid AND attname IN ('video_game_id','locale') ORDER BY attname)
                 LIMIT 1"
        )
        .persistent(false)
        .fetch_optional(&db.pool)
        .await?;
    Ok(exists.unwrap_or(false))
}

fn release_within_window(release: Option<&str>, cutoff: NaiveDate) -> bool {
    match release.and_then(parse_release_date_any) {
        Some(date) => date >= cutoff,
        None => true, // keep items with unknown release dates
    }
}

fn detail_product_node(detail: &Value) -> Option<&Value> {
    detail
        .get("data")
        .and_then(|d| d.get("metGetProductById"))
        .filter(|node| node.is_object())
}

// Shared with ratings.rs to avoid duplicating rating parsing logic.
pub(crate) fn extract_rating_from_detail(detail: Option<&Value>) -> Option<(f32, i64)> {
    let obj = detail?.as_object()?;
    let candidates = [
        "starRating",
        "rating",
        "ratings",
        "aggregateRating",
        "criticScore",
    ];
    for key in candidates {
        if let Some(Value::Object(r)) = obj.get(key) {
            let avg = r
                .get("averageRating")
                .and_then(|v| v.as_f64())
                .or_else(|| r.get("avg").and_then(|v| v.as_f64()));
            let cnt = r
                .get("ratingCount")
                .and_then(|v| v.as_i64())
                .or_else(|| r.get("count").and_then(|v| v.as_i64()));
            if let (Some(a), Some(c)) = (avg, cnt) {
                return Some((a as f32, c));
            }
        }
    }
    // Some payloads store rating directly on the product node
    let avg = obj
        .get("averageRating")
        .and_then(|v| v.as_f64())
        .or_else(|| obj.get("avg").and_then(|v| v.as_f64()));
    let cnt = obj
        .get("ratingCount")
        .and_then(|v| v.as_i64())
        .or_else(|| obj.get("count").and_then(|v| v.as_i64()));
    match (avg, cnt) {
        (Some(a), Some(c)) => Some((a as f32, c)),
        _ => None,
    }
}

fn collect_genre_values(value: &Value, out: &mut Vec<String>) {
    match value {
        Value::String(s) => {
            let trimmed = s.trim();
            if !trimmed.is_empty() {
                out.push(trimmed.to_string());
            }
        }
        Value::Array(arr) => {
            for el in arr {
                collect_genre_values(el, out);
            }
        }
        Value::Object(obj) => {
            for key in ["displayName", "name", "key", "genre"] {
                if let Some(Value::String(s)) = obj.get(key) {
                    let trimmed = s.trim();
                    if !trimmed.is_empty() {
                        out.push(trimmed.to_string());
                    }
                }
            }
            for (_, nested) in obj {
                collect_genre_values(nested, out);
            }
        }
        _ => {}
    }
}

fn extract_genres_from_detail(detail: Option<&Value>) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let obj = match detail.and_then(|v| v.as_object()) {
        Some(o) => o,
        None => {
            return out;
        }
    };
    for key in ["productGenres", "genres", "genre", "productGenre"] {
        if let Some(v) = obj.get(key) {
            collect_genre_values(v, &mut out);
        }
    }
    for (k, v) in obj.iter() {
        if k.to_lowercase().contains("genre") {
            collect_genre_values(v, &mut out);
        }
    }
    out.sort();
    out.dedup();
    out
}

fn provider_payload(detail: Option<&Value>, summary: &PsProductSummary) -> Value {
    if let Some(d) = detail {
        return d.clone();
    }
    json!({
        "productId": summary.product_id,
        "conceptId": summary.concept_id,
        "name": summary.name,
        "releaseDate": summary.release_date,
        "genres": summary.genres,
        "mediaImageUrls": summary.media_image_urls,
        "mediaVideoUrls": summary.media_video_urls,
    })
}

fn extract_synopsis_from_detail(detail: Option<&Value>) -> Option<String> {
    // Fast path: PS-specific structure { __typename: "Description", type: "LONG", value: "..." }
    fn find_ps_long_description(v: &Value) -> Option<&str> {
        match v {
            Value::Object(obj) => {
                let typename = obj
                    .get("__typename")
                    .and_then(|t| t.as_str())
                    .unwrap_or("")
                    .to_ascii_lowercase();
                let t_field = obj
                    .get("type")
                    .and_then(|t| t.as_str())
                    .unwrap_or("")
                    .to_ascii_lowercase();
                if typename == "description" && t_field == "long" {
                    if let Some(Value::String(s)) = obj.get("value") {
                        return Some(s.as_str());
                    }
                }
                for (_, vv) in obj {
                    if let Some(s) = find_ps_long_description(vv) {
                        return Some(s);
                    }
                }
                None
            }
            Value::Array(arr) => {
                for el in arr {
                    if let Some(s) = find_ps_long_description(el) {
                        return Some(s);
                    }
                }
                None
            }
            _ => None,
        }
    }
    fn clean_text(input: &str) -> String {
        let mut out = String::with_capacity(input.len());
        let mut in_tag = false;
        let mut prev_space = false;
        for ch in input.chars() {
            match ch {
                '<' => {
                    in_tag = true;
                }
                '>' => {
                    in_tag = false;
                }
                c => {
                    if !in_tag {
                        let mapped = if c.is_whitespace() { ' ' } else { c };
                        if mapped == ' ' {
                            if !prev_space {
                                out.push(' ');
                                prev_space = true;
                            }
                        } else {
                            out.push(mapped);
                            prev_space = false;
                        }
                    }
                }
            }
        }
        out.trim().to_string()
    }

    fn consider_value(val: &Value, best: &mut Option<String>) {
        match val {
            Value::String(s) => {
                let cleaned = clean_text(s);
                if cleaned.is_empty() {
                    return;
                }
                match best {
                    Some(existing) => {
                        if cleaned.len() > existing.len() {
                            *existing = cleaned;
                        }
                    }
                    None => {
                        *best = Some(cleaned);
                    }
                }
            }
            Value::Array(arr) => {
                for v in arr {
                    consider_value(v, best);
                }
            }
            Value::Object(obj) => {
                // Special-case: PlayStation often nests description as { __typename: "Description", type: "LONG", value: "..." }
                let typename = obj
                    .get("__typename")
                    .and_then(|t| t.as_str())
                    .unwrap_or("")
                    .to_ascii_lowercase();
                let type_field = obj
                    .get("type")
                    .and_then(|t| t.as_str())
                    .unwrap_or("")
                    .to_ascii_lowercase();
                if typename.contains("description") || type_field == "long" {
                    if let Some(Value::String(v)) = obj.get("value") {
                        // Prefer this value directly
                        let cleaned = clean_text(v);
                        if !cleaned.is_empty() {
                            match best {
                                Some(existing) => {
                                    if cleaned.len() > existing.len() {
                                        *existing = cleaned;
                                    }
                                }
                                None => {
                                    *best = Some(cleaned);
                                }
                            }
                        }
                    }
                }
                for (_, v) in obj {
                    consider_value(v, best);
                }
            }
            _ => {}
        }
    }

    fn walk(value: &Value, best: &mut Option<String>) {
        match value {
            Value::Object(obj) => {
                for (k, v) in obj {
                    let key = k.to_ascii_lowercase();
                    let matches = key.contains("description")
                        || key.contains("descriptions")
                        || key.contains("summary")
                        || key.contains("synopsis")
                        || key.contains("about")
                        || key.contains("overview");
                    if matches {
                        consider_value(v, best);
                    }
                    walk(v, best);
                }
            }
            Value::Array(arr) => {
                for v in arr {
                    walk(v, best);
                }
            }
            _ => {}
        }
    }

    // Prefer PS long Description if present
    if let Some(d) = detail {
        if let Some(raw) = find_ps_long_description(d) {
            return Some(clean_text(raw));
        }
    }

    let mut best: Option<String> = None;
    if let Some(d) = detail {
        walk(d, &mut best);
    }
    best
}

pub struct IngestSummary {
    pub regions: Vec<String>,
    pub items: usize,
    pub price_points: usize,
    pub current_upserts: usize,
}

pub async fn run_from_env() -> Result<()> {
    dotenv::dotenv().ok();
    // Pre-flight: ensure minimal config and log snapshot
    preflight_check(
        "psstore-prices-ingest",
        &["SUPABASE_IPV6_DB"],
        &[
            "SUPABASE_IPV6_DB",
            "SUPABASE_DB_URL",
            "PS_STORE_REGIONS",
            "PS_PAGE_SIZE",
            "PS_MAX_PAGES",
            "PS4_CATEGORY",
            "PS5_CATEGORY",
            "PS_HASH",
            "YEAR_MIN",
            "YEAR_MAX",
        ],
    )?;
    let db_url = env_util::db_url()?;
    // Year-range reminder (filtering is enforced in psstore_seed_pipeline; this module logs intent)
    let year_min: i32 = env::var("YEAR_MIN")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2020);
    let year_max: i32 = env::var("YEAR_MAX")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2025);
    println!("remember: restricting to releases between {year_min}-{year_max} inclusive\n");
    let regions_raw = env::var("PS_STORE_REGIONS").unwrap_or_else(|_| "en-us".into());
    let regions: Vec<String> = regions_raw
        .split(|c: char| (c == ',' || c == ' '))
        .filter(|s| !s.is_empty())
        .map(|s| s.trim().to_lowercase())
        .collect();
    if regions.is_empty() {
        eprintln!("No regions specified via PS_STORE_REGIONS; aborting");
        return Ok(());
    }
    let pages: u32 = env::var("PS_MAX_PAGES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    let page_size: u32 = env::var("PS_PAGE_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100);
    let cat_ps4 =
        env::var("PS4_CATEGORY").unwrap_or_else(|_| "44d8bb20-653e-431e-8ad0-c0a365f68d2f".into());
    let cat_ps5 =
        env::var("PS5_CATEGORY").unwrap_or_else(|_| "4cbf39e2-5749-4970-ba81-93a489e4570c".into());
    let sha = env::var("PSSTORE_SHA256")
        .or_else(|_| env::var("PS_HASH"))
        .unwrap_or_else(|_| {
            "9845afc0dbaab4965f6563fffc703f588c8e76792000e8610843b8d3ee9c4c09".into()
        });
    info!(sha256 = %sha, "psstore ingest using persisted query hash override");

    // Start ingest run observability
    // Never run migrations on ingest paths
    let provider_id = ensure_provider(
        &Db::connect_no_migrate(&db_url, 2).await?,
        "playstation_store",
        "storefront",
        Some(PS_STORE_PROVIDER_KEY),
    )
    .await?;
    let run_id = ingest_run_start(
        &Db::connect_no_migrate(&db_url, 2).await?,
        provider_id,
        regions.get(0).map(|s| s.as_str()),
        Some(serde_json::json!({"pages": pages, "page_size": page_size})),
    )
    .await?;

    let summary_res = ingest_prices(&db_url, &regions, pages, page_size, &cat_ps4, &cat_ps5).await;
    let (status, items_processed, prices_written) = match &summary_res {
        Ok(s) => ("ok", s.items as i64, s.price_points as i64),
        Err(_) => ("error", 0, 0),
    };
    let _ = ingest_run_finish(
        &Db::connect_no_migrate(&db_url, 2).await?,
        run_id,
        status,
        items_processed,
        prices_written,
        None,
    )
    .await;
    if let Err(e) = summary_res {
        return Err(e);
    }
    Ok(())
}

pub async fn ingest_prices(
    db_url: &str,
    regions: &[String],
    pages: u32,
    page_size: u32,
    cat_ps4: &str,
    cat_ps5: &str,
) -> Result<IngestSummary> {
    // Never run migrations in ingest worker
    let db = Db::connect_no_migrate(db_url, 10).await?;
    // Ensure we are writing to public.* explicitly for this session
    let _ = sqlx::query("SET search_path TO public")
        .persistent(false)
        .execute(&db.pool)
        .await;

    let default_cutoff = (Utc::now() - ChronoDuration::days(365 * 5)).date_naive();
    let cutoff_date = env::var("YEAR_MIN")
        .ok()
        .and_then(|s| s.parse::<i32>().ok())
        .and_then(|year| NaiveDate::from_ymd_opt(year, 1, 1))
        .unwrap_or(default_cutoff);

    // Lightweight schema probe so we can run against Supabase-lite instances that lack countries/jurisdictions.
    async fn table_exists(db: &Db, name: &str) -> Result<bool> {
        let exists: Option<bool> = sqlx::query_scalar(
            "SELECT TRUE FROM information_schema.tables WHERE table_schema = ANY (current_schemas(true)) AND table_name = $1 LIMIT 1",
        )
        .persistent(false)
        .bind(name)
        .fetch_optional(&db.pool)
        .await?;
        Ok(exists.unwrap_or(false))
    }

    let mut locale_contexts: Vec<LocaleContext> = Vec::new();
    for loc in regions {
        let mut code2 = loc.split('-').nth(1).unwrap_or("us").to_uppercase();
        // Some callers provide non-standard locales like "zh-ZH". Treat them as Mainland China.
        if code2 == "ZH" {
            code2 = "CN".to_string();
        }
        let (cur_code, cur_name) = match code2.as_str() {
            "US" => ("USD", "US Dollar"),
            "GB" => ("GBP", "British Pound"),
            "DE" => ("EUR", "Euro"),
            "FR" => ("EUR", "Euro"),
            "CA" => ("CAD", "Canadian Dollar"),
            "AU" => ("AUD", "Australian Dollar"),
            "JP" => ("JPY", "Japanese Yen"),
            "ES" => ("EUR", "Euro"),
            "CN" => ("CNY", "Chinese Yuan"),
            "PT" => ("EUR", "Euro"),
            "IT" => ("EUR", "Euro"),
            "NL" => ("EUR", "Euro"),
            "BR" => ("BRL", "Brazilian Real"),
            "MX" => ("MXN", "Mexican Peso"),
            "SE" => ("SEK", "Swedish Krona"),
            "NO" => ("NOK", "Norwegian Krone"),
            "DK" => ("DKK", "Danish Krone"),
            "CH" => ("CHF", "Swiss Franc"),
            "KR" => ("KRW", "South Korean Won"),
            "TW" => ("TWD", "New Taiwan Dollar"),
            "HK" => ("HKD", "Hong Kong Dollar"),
            "PL" => ("PLN", "Polish Zloty"),
            "RU" => ("RUB", "Russian Ruble"),
            "ZA" => ("ZAR", "South African Rand"),
            "SA" => ("SAR", "Saudi Riyal"),
            "AE" => ("AED", "UAE Dirham"),
            "IN" => ("INR", "Indian Rupee"),
            "AR" => ("ARS", "Argentine Peso"),
            "TR" => ("TRY", "Turkish Lira"),
            "BU" => ("BGN", "Bulgarian Lev"),
            "CZ" => ("CZK", "Czech Koruna"),
            "HU" => ("HUF", "Hungarian Forint"),
            "GR" => ("EUR", "Euro"),
            _ => ("USD", "US Dollar"),
        };
        let cur_id = ensure_currency(&db, cur_code, cur_name, 2).await?;

        // Legacy PHP compat: jurisdictions may be absent. Prefer countries if present (so sku_regions can be created per-product).
        // Final fallback: reuse any existing sku_regions row by region_code so ingestion can proceed in Supabase-lite instances.
        let has_jurisdictions = table_exists(&db, "jurisdictions").await.unwrap_or(false);
        let has_countries = table_exists(&db, "countries").await.unwrap_or(false);
        let jurisdiction_id = if !has_jurisdictions {
            if has_countries {
                // In compat mode, treat jurisdiction_id as country_id.
                ensure_country(&db, &code2, &code2, cur_id).await?
            } else if let Some(sr_id) = sqlx::query_scalar::<_, Option<i64>>(
                "SELECT id FROM sku_regions WHERE UPPER(region_code) = $1 LIMIT 1",
            )
            .persistent(false)
            .bind(&code2)
            .fetch_optional(&db.pool)
            .await?
            .flatten()
            {
                sr_id
            } else {
                bail!(
                    "jurisdictions table missing and no countries/sku_regions row for region_code {}; please seed countries (preferred) or sku_regions, or run against full schema",
                    code2
                );
            }
        } else {
            let country_id = ensure_country(&db, &code2, &code2, cur_id).await?;
            ensure_national_jurisdiction(&db, country_id).await?
        };

        let rps_env: u32 = env::var("PS_STORE_RPS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(3);
        let max_retries_env: u32 = env::var("PS_STORE_MAX_RETRIES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(5);
        let backoff_ms_env: u64 = env::var("PS_STORE_BACKOFF_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1500);
        let cfg = PsConfig {
            locales: vec![loc.clone()],
            rps: rps_env,
            retry_attempts: max_retries_env,
            retry_base_delay_ms: backoff_ms_env,
            ..PsConfig::default()
        };

        locale_contexts.push(LocaleContext {
            locale: loc.clone(),
            region_code: code2,
            currency_id: cur_id,
            jurisdiction_id,
            client: PsStoreClient::new(cfg),
        });
    }

    let provider_id =
        ensure_provider(&db, "playstation_store", "storefront", Some(PS_STORE_PROVIDER_KEY))
            .await?;

    // Detect whether video_game_titles must be written via the Laravel source registry path.
    // In some deployments `video_game_titles.title` doesn't exist (only `raw_title`), and the
    // table is keyed by (video_game_source_id, video_game_source_id). In that case we MUST ensure a
    // `video_game_sources` row and use the source-item title helper.
    async fn titles_has_column(db: &Db, column: &str) -> Result<bool> {
        let present: Option<bool> = sqlx::query_scalar(
            "SELECT TRUE\n             WHERE EXISTS (\n               SELECT 1 FROM information_schema.columns\n               WHERE table_schema = ANY (current_schemas(true))\n                 AND table_name = 'video_game_titles'\n                 AND column_name = $1\n             )\n             LIMIT 1",
        )
        .persistent(false)
        .bind(column)
        .fetch_optional(&db.pool)
        .await?;
        Ok(present.unwrap_or(false))
    }

    async fn titles_are_source_keyed(db: &Db) -> Result<bool> {
        Ok(titles_has_column(db, "video_game_source_id").await?
            && titles_has_column(db, "video_game_source_id").await?)
    }

    async fn titles_column_is_not_null(db: &Db, column: &str) -> Result<bool> {
        let present: Option<bool> = sqlx::query_scalar(
            "SELECT TRUE\n             WHERE EXISTS (\n               SELECT 1 FROM information_schema.columns\n               WHERE table_schema = ANY (current_schemas(true))\n                 AND table_name = 'video_game_titles'\n                 AND column_name = $1\n                 AND is_nullable = 'NO'\n             )\n             LIMIT 1",
        )
        .persistent(false)
        .bind(column)
        .fetch_optional(&db.pool)
        .await?;
        Ok(present.unwrap_or(false))
    }

    async fn video_games_has_column(db: &Db, column: &str) -> Result<bool> {
        let present: Option<bool> = sqlx::query_scalar(
            "SELECT TRUE\n             WHERE EXISTS (\n               SELECT 1 FROM information_schema.columns\n               WHERE table_schema = ANY (current_schemas(true))\n                 AND table_name = 'video_games'\n                 AND column_name = $1\n             )\n             LIMIT 1",
        )
        .persistent(false)
        .bind(column)
        .fetch_optional(&db.pool)
        .await?;
        Ok(present.unwrap_or(false))
    }

    async fn video_games_is_laravel_schema(db: &Db) -> Result<bool> {
        Ok(video_games_has_column(db, "product_id").await?
            && video_games_has_column(db, "title").await?
            && !video_games_has_column(db, "title_id").await?)
    }

    let titles_have_title = titles_has_column(&db, "title").await.unwrap_or(false);
    let titles_have_raw_title = titles_has_column(&db, "raw_title").await.unwrap_or(false);
    let titles_source_keyed = titles_are_source_keyed(&db).await.unwrap_or(false);

    let titles_require_video_game_id = titles_column_is_not_null(&db, "video_game_id")
        .await
        .unwrap_or(false);
    let video_games_laravel = video_games_is_laravel_schema(&db).await.unwrap_or(false);

    // If the legacy `title` column is missing but `raw_title` exists, prefer/require the source registry path.
    let use_source_registry_titles =
        titles_source_keyed || (!titles_have_title && titles_have_raw_title);

    let video_game_source_id = if use_source_registry_titles {
        Some(
            ensure_video_game_source(&db, PS_STORE_PROVIDER_KEY, "PlayStation Store").await?,
        )
    } else {
        None
    };

    // Some Laravel installations do not include the Rust provider mirror tables.
    let provider_items_supported = table_exists(&db, "provider_items").await.unwrap_or(false);
    let retailer_id = ensure_retailer(&db, "PlayStation", Some("playstation")).await?;
    let ps4_platform_id = ensure_platform(&db, "PS4", Some("ps4")).await?;
    let ps5_platform_id = ensure_platform(&db, "PS5", Some("ps5")).await?;

    let mut ingestor = ProductIngestor::new(
        db.clone(),
        provider_id,
        retailer_id,
        ps4_platform_id,
        ps5_platform_id,
        video_game_source_id,
        provider_items_supported,
        page_size,
        titles_require_video_game_id,
        video_games_laravel,
    );

    // DIRECT MODE: bypass category grids and ingest specific product IDs.
    if let Ok(raw_ids) = env::var("PS_DIRECT_PRODUCT_IDS") {
        let ids: Vec<String> = raw_ids
            .split(|c: char| (c == ',' || c == ' '))
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();
        if !ids.is_empty() {
            let force_platform = env::var("PS_DIRECT_PLATFORM")
                .ok()
                .unwrap_or_else(|| "PS5".to_string());

            fn normalize_ps_platform_label(input: &str) -> &'static str {
                let s = input.trim().to_ascii_lowercase();
                if s.is_empty() {
                    return "PS5";
                }
                // Accept: ps4/PS4/playstation 4/playstation-4/playstation4
                if s == "ps4" || s == "playstation4" || s == "playstation-4" || s == "playstation 4"
                {
                    return "PS4";
                }
                // Accept: ps5/PS5/playstation 5/playstation-5/playstation5
                if s == "ps5" || s == "playstation5" || s == "playstation-5" || s == "playstation 5"
                {
                    return "PS5";
                }
                // Default safely to PS5.
                "PS5"
            }

            let platform_label = normalize_ps_platform_label(&force_platform);
            let platform_id = if platform_label == "PS4" {
                ps4_platform_id
            } else {
                ps5_platform_id
            };
            for ctx in &locale_contexts {
                process_direct_products(&mut ingestor, ctx, &ids, platform_label, platform_id)
                    .await?;
            }
            return ingestor.finalize(regions).await;
        }
    }

    for ctx in &locale_contexts {
        ingestor
            .process_locale(ctx, cat_ps4, cat_ps5, pages, cutoff_date)
            .await?;
    }

    ingestor.finalize(regions).await
}

struct LocaleContext {
    locale: String,
    region_code: String,
    currency_id: i64,
    jurisdiction_id: i64,
    client: PsStoreClient,
}

#[derive(Default, Clone, Debug)]
struct MemBagEntry {
    pub base_minor: Option<i64>,
    pub discount_minor: Option<i64>,
    pub image_url: Option<String>,
    pub video_url: Option<String>,
}

struct ProductIngestor {
    db: Db,
    provider_id: i64,
    retailer_id: i64,
    ps4_platform_id: i64,
    ps5_platform_id: i64,
    video_game_source_id: Option<i64>,
    provider_items_supported: bool,
    page_size: u32,
    price_rows: Vec<PriceRow>,
    total_items: usize,
    total_prices: usize,
    total_current_upserts: usize,
    seen_offer_jurisdictions: HashSet<i64>,
    entity_cache: ProviderEntityCache,
    // In-memory index keyed by "slug::locale::platform" aggregating price/media
    mem_bag: HashMap<String, MemBagEntry>,
    // Fallback/media source map (title -> media) built from merged_final.json (or env path)
    media_map: MediaMap,
    // Toggle to use mem_bag/media_map-first behavior
    use_mem_bag: bool,
    post_summary: PostIngestSummary,

    // Cached legacy PHP provider mirror row for split media tables (game_images/game_videos).
    psstore_game_provider_id: Option<i64>,

    // Schema compatibility flags (computed once at startup)
    titles_require_video_game_id: bool,
    video_games_is_laravel: bool,
}

impl ProductIngestor {
    fn new(
        db: Db,
        provider_id: i64,
        retailer_id: i64,
        ps4_platform_id: i64,
        ps5_platform_id: i64,
        video_game_source_id: Option<i64>,
        provider_items_supported: bool,
        page_size: u32,
        titles_require_video_game_id: bool,
        video_games_is_laravel: bool,
    ) -> Self {
        let cache_db = db.clone();
        // Load media map once (best-effort)
        let media_limit = std::env::var("MEDIA_MAP_LIMIT")
            .ok()
            .and_then(|s| s.parse().ok());
        let media_path =
            std::env::var("MEDIA_MAP_FILE").unwrap_or_else(|_| "merged_final.json".to_string());
        let media_map =
            MediaMap::from_file(&media_path, media_limit).unwrap_or_else(|_| MediaMap::empty());
        let use_mem_bag = std::env::var("PS_USE_MEM_INDEX")
            .ok()
            .map(|v| (v == "1" || v.eq_ignore_ascii_case("true")))
            .unwrap_or(true);
        Self {
            db,
            provider_id,
            retailer_id,
            ps4_platform_id,
            ps5_platform_id,
            video_game_source_id,
            provider_items_supported,
            page_size,
            price_rows: Vec::new(),
            total_items: 0,
            total_prices: 0,
            total_current_upserts: 0,
            seen_offer_jurisdictions: HashSet::new(),
            entity_cache: ProviderEntityCache::new(cache_db),
            mem_bag: HashMap::new(),
            media_map,
            use_mem_bag,
            post_summary: PostIngestSummary::default(),

            psstore_game_provider_id: None,

            titles_require_video_game_id,
            video_games_is_laravel,
        }
    }

    async fn ensure_psstore_game_provider_id(&mut self) -> Result<i64> {
        if let Some(id) = self.psstore_game_provider_id {
            return Ok(id);
        }

        // IMPORTANT: The Laravel contract uses provider_key = "psstore" for PS Store media.
        let id = ensure_game_provider(&self.db, "PlayStation Store", "storefront", Some("psstore"))
            .await?;

        // Best-effort: keep legacy game_providers row linked to the canonical video_game_source_id (when present).
        if let Some(source_id) = self.video_game_source_id {
            let _ = sqlx::query(
                "UPDATE game_providers SET video_game_source_id = $1, updated_at = now() \
                 WHERE id = $2 AND (video_game_source_id IS DISTINCT FROM $1)",
            )
            .persistent(false)
            .bind(source_id)
            .bind(id)
            .execute(&self.db.pool)
            .await;
        }

        self.psstore_game_provider_id = Some(id);
        Ok(id)
    }

    async fn process_locale(
        &mut self,
        ctx: &LocaleContext,
        cat_ps4: &str,
        cat_ps5: &str,
        max_pages: u32,
        cutoff_date: NaiveDate,
    ) -> Result<()> {
        self.process_platform(
            ctx,
            cat_ps4,
            "PS4",
            self.ps4_platform_id,
            max_pages,
            cutoff_date,
        )
        .await?;
        self.process_platform(
            ctx,
            cat_ps5,
            "PS5",
            self.ps5_platform_id,
            max_pages,
            cutoff_date,
        )
        .await?;
        Ok(())
    }

    async fn process_platform(
        &mut self,
        ctx: &LocaleContext,
        category_id: &str,
        platform_label: &str,
        platform_id: i64,
        max_pages: u32,
        cutoff_date: NaiveDate,
    ) -> Result<()> {
        // Sorting resilience:
        // - "productReleaseDate" has been observed to work reliably.
        // - "releaseDate" is the older/default sort used by some clients but can trigger upstream ES failures.
        // - final fallback is no sort (sortBy=null).
        let sort_candidates: Vec<String> = env::var("PS_SORTS")
            .ok()
            .map(|s| {
                s.split(',')
                    .map(|x| x.trim().to_string())
                    .filter(|x| !x.is_empty())
                    .collect::<Vec<String>>()
            })
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| vec!["productReleaseDate".to_string(), "releaseDate".to_string()]);
        let try_nosort = env::var("PS_TRY_NOSORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1)
            == 1;

        let fallback_sizes: Vec<u32> = env::var("PS_FALLBACK_PAGE_SIZES")
            .ok()
            .map(|s| {
                s.split(',')
                    .filter_map(|x| x.trim().parse::<u32>().ok())
                    .filter(|v| *v > 0)
                    .collect::<Vec<u32>>()
            })
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| {
                let mut defaults = vec![self.page_size / 2, 50, 24, 12];
                defaults.retain(|sz| *sz > 0 && *sz != self.page_size);
                defaults
            });

        let mut page = 0u32;
        while page < max_pages {
            let offset = page.saturating_mul(self.page_size);
            let mut selected_items: Option<Vec<PsProductSummary>> = None;
            let mut used_size = self.page_size;
            let mut used_sort: Option<String> = None;
            let mut attempted: HashSet<u32> = HashSet::new();

            // First: try sorted queries with multiple sort candidates.
            'sorts: for sort_name in &sort_candidates {
                attempted.clear();
                for size in std::iter::once(self.page_size).chain(fallback_sizes.iter().copied()) {
                    if size == 0 || !attempted.insert(size) {
                        continue;
                    }
                    match ctx
                        .client
                        .category_grid_retrieve_sorted(
                            &ctx.locale,
                            category_id,
                            size,
                            offset,
                            sort_name,
                            false,
                        )
                        .await
                    {
                        Ok(items) => {
                            selected_items = Some(items);
                            used_size = size;
                            used_sort = Some(sort_name.to_string());
                            break 'sorts;
                        }
                        Err(err) => {
                            warn!(
                                locale = %ctx.locale,
                                platform = %platform_label,
                                offset,
                                size,
                                sort = %sort_name,
                                error = %err,
                                "psstore page fetch failed; trying fallback size/sort"
                            );
                            continue;
                        }
                    }
                }
            }

            // Last: try no-sort if all sorted attempts failed.
            if selected_items.is_none() && try_nosort {
                attempted.clear();
                for size in std::iter::once(self.page_size).chain(fallback_sizes.iter().copied()) {
                    if size == 0 || !attempted.insert(size) {
                        continue;
                    }
                    match ctx
                        .client
                        .category_grid_retrieve(&ctx.locale, category_id, size, offset)
                        .await
                    {
                        Ok(items) => {
                            selected_items = Some(items);
                            used_size = size;
                            used_sort = None;
                            break;
                        }
                        Err(err) => {
                            warn!(
                                locale = %ctx.locale,
                                platform = %platform_label,
                                offset,
                                size,
                                error = %err,
                                "psstore page fetch failed (no-sort); trying fallback size"
                            );
                            continue;
                        }
                    }
                }
            }

            let items = match selected_items {
                Some(v) => v,
                None => {
                    let skip_on_error = env::var("PS_SKIP_ON_PAGE_ERROR")
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(1)
                        == 1;
                    warn!(
                        locale = %ctx.locale,
                        platform = %platform_label,
                        offset,
                        skip=?skip_on_error,
                        "psstore exhausted fallback sizes"
                    );
                    if skip_on_error {
                        // Skip this page and continue pagination to avoid stalls due to transient ES errors
                        page = page.saturating_add(1);
                        continue;
                    } else {
                        break;
                    }
                }
            };

            if items.is_empty() {
                let stop_on_empty = env::var("PS_STOP_ON_EMPTY")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0)
                    == 1;
                info!(
                    locale = %ctx.locale,
                    platform = %platform_label,
                    offset,
                    size = used_size,
                    sort = ?used_sort,
                    stop_on_empty,
                    "psstore empty page"
                );
                if stop_on_empty {
                    break;
                } else {
                    // Continue to next page (may have been an intermittent ES error that returned 0)
                    page = page.saturating_add(1);
                    continue;
                }
            }

            let mut continue_paging = true;
            for summary in items {
                if !release_within_window(summary.release_date.as_deref(), cutoff_date) {
                    continue_paging = false;
                    break;
                }
                let external_product_id =
                    match summary.product_id.clone().or(summary.concept_id.clone()) {
                        Some(id) => id,
                        None => {
                            warn!(
                                locale = %ctx.locale,
                                platform = %platform_label,
                                "psstore summary missing product_id; skipping item"
                            );
                            continue;
                        }
                    };
                // Try to fetch detail by product_id. If it fails, fall back to summary-only ingestion
                let detail_opt = match ctx
                    .client
                    .product_detail_raw(&ctx.locale, &external_product_id)
                    .await
                {
                    Ok(v) => Some(v),
                    Err(e) => {
                        warn!(
                            locale = %ctx.locale,
                            platform = %platform_label,
                            product_id = %external_product_id,
                            error = %e,
                            "psstore detail fetch failed; proceeding with summary-only fallback"
                        );
                        None
                    }
                };
                // Concept-first pricing: resolve conceptId (prefer summary.concept_id, else query), then fetch pricing
                let mut summary_adjusted = summary.clone();
                let mut concept_id_opt = summary_adjusted.concept_id.clone();
                if concept_id_opt.is_none() {
                    concept_id_opt = match ctx
                        .client
                        .concept_by_product_id_raw(&ctx.locale, &external_product_id)
                        .await
                    {
                        Ok(v) => v
                            .get("data")
                            .and_then(|d| d.get("metGetConceptByProductIdQuery"))
                            .and_then(|n| n.get("conceptId"))
                            .and_then(|s| s.as_str())
                            .map(|s| s.to_string()),
                        Err(_) => None,
                    };
                }
                if let Some(concept_id) = concept_id_opt {
                    if let Ok(pricing) = ctx
                        .client
                        .concept_pricing_raw(&ctx.locale, &concept_id)
                        .await
                    {
                        let (base_minor, discount_minor) = parse_pricing_minor(&pricing);
                        // Override when parsed values are present and positive
                        if let Some(b) = base_minor.filter(|v| *v > 0) {
                            summary_adjusted.base_price_minor = Some(b);
                        }
                        if let Some(d) = discount_minor.filter(|v| *v > 0) {
                            summary_adjusted.discounted_price_minor = Some(d);
                        }
                    }
                }
                self.ingest_product(
                    ctx,
                    platform_label,
                    platform_id,
                    summary_adjusted,
                    external_product_id,
                    detail_opt.unwrap_or(serde_json::Value::Null),
                )
                .await?;
            }

            if !continue_paging {
                break;
            }
            page = page.saturating_add(1);
        }
        Ok(())
    }

    async fn ingest_product(
        &mut self,
        ctx: &LocaleContext,
        platform_label: &str,
        _platform_id: i64,
        summary: PsProductSummary,
        external_product_id: String,
        detail_raw: Value,
    ) -> Result<()> {
        let detail_node = detail_product_node(&detail_raw);
        let title_name = detail_node
            .and_then(|node| node.get("name").and_then(Value::as_str))
            .or_else(|| summary.name.as_deref())
            .and_then(|s| {
                let trimmed = s.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed.to_string())
                }
            })
            .unwrap_or_else(|| "Untitled".to_string());
        let product_slug = normalize_title(&title_name);

        // Update in-memory bag with price + media derived using media_map (preferred) for this (title, locale, platform)
        if self.use_mem_bag {
            let key = format!("{}::{}::{}", product_slug, ctx.locale, platform_label);
            let entry = self.mem_bag.entry(key).or_insert_with(MemBagEntry::default);
            // Prices from adjusted summary
            if let Some(b) = summary.base_price_minor.filter(|v| *v > 0) {
                entry.base_minor = Some(b);
            }
            if let Some(d) = summary.discounted_price_minor.filter(|v| *v > 0) {
                entry.discount_minor = Some(d);
            }
            // Media from media_map by normalized title
            if entry.image_url.is_none() {
                if let Some(img) = self.media_map.get(&title_name) {
                    entry.image_url = Some(img.to_string());
                }
            }
            if entry.video_url.is_none() {
                if let Some(vid) = self.media_map.get_video(&title_name) {
                    entry.video_url = Some(vid.to_string());
                }
            }
        }

        let product_id = self
            .entity_cache
            .ensure_product_named_with_platform(
                "software",
                &product_slug,
                &title_name,
                platform_label,
            )
            .await?;
        self.entity_cache.ensure_software_row(product_id).await?;

        let mut provider_payload_value = provider_payload(detail_node, &summary);
        let edition_hint =
            edition_hint_from_title_or_metadata(&title_name, Some(&provider_payload_value));
        if edition_hint.has_edition {
            if let Some(obj) = provider_payload_value.as_object_mut() {
                obj.entry("gc_has_edition".to_string())
                    .or_insert_with(|| Value::Bool(true));
                if let Some(label) = edition_hint.label.as_deref() {
                    obj.entry("gc_edition_label".to_string())
                        .or_insert_with(|| Value::String(label.to_string()));
                }
            }
        }
        let video_game_source_id = if self.provider_items_supported {
            let id = self
                .entity_cache
                .ensure_provider_item(
                    self.provider_id,
                    &external_product_id,
                    Some(provider_payload_value.clone()),
                    detail_node.is_some(),
                )
                .await?;
            self.post_summary.record_provider_item(id);
            Some(id)
        } else {
            None
        };

        let (_title_id, video_game_id) = if let Some(source_id) = self.video_game_source_id {
            if self.titles_require_video_game_id {
                if !self.video_games_is_laravel {
                    bail!(
                        "video_game_titles.video_game_id is NOT NULL but video_games is not Laravel-style (missing product_id/title); cannot satisfy FK"
                    );
                }

                // Laravel schema: use product_id directly
                let video_game_id = self
                    .entity_cache
                    .ensure_video_game_for_product_laravel(
                        product_id,
                        &title_name,
                        Some(&product_slug),
                        Some(provider_payload_value.clone()),
                        PS_STORE_PROVIDER_KEY,
                    )
                    .await?;

                let title_id = self
                    .entity_cache
                    .ensure_video_game_title_for_source_item(
                        source_id,
                        &external_product_id,
                        Some(product_id),
                        Some(video_game_id),
                        &title_name,
                        Some(&product_slug),
                        Some(&ctx.locale),
                        None,
                    )
                    .await?;

                (title_id, video_game_id)
            } else {
                let title_id = self
                    .entity_cache
                    .ensure_video_game_title_for_source_item(
                        source_id,
                        &external_product_id,
                        Some(product_id),
                        None,
                        &title_name,
                        Some(&product_slug),
                        Some(&ctx.locale),
                        None,
                    )
                    .await?;
                // Laravel schema: use product_id directly
                let video_game_id = self
                    .entity_cache
                    .ensure_video_game_for_product_laravel(
                        product_id,
                        &title_name,
                        Some(&product_slug),
                        None,
                        PS_STORE_PROVIDER_KEY,
                    )
                    .await?;
                (title_id, video_game_id)
            }
        } else {
            if self.titles_require_video_game_id {
                bail!(
                    "video_game_titles.video_game_id is NOT NULL but ingestion is not using source-keyed titles; enable the source registry title path"
                );
            }
            let title_id = self
                .entity_cache
                .ensure_video_game_title(product_id, &title_name, Some(&product_slug))
                .await?;
            // Laravel schema: use product_id directly
            let video_game_id = self
                .entity_cache
                .ensure_video_game_for_product_laravel(
                    product_id,
                    &title_name,
                    Some(&product_slug),
                    None,
                    PS_STORE_PROVIDER_KEY,
                )
                .await?;
            (title_id, video_game_id)
        };
        let sellable_id = self
            .entity_cache
            .ensure_sellable("software", product_id)
            .await?;
        let offer_id = self
            .entity_cache
            .ensure_offer(sellable_id, self.retailer_id, None)
            .await?;
        let offer_jurisdiction_id = self
            .entity_cache
            .ensure_offer_jurisdiction(offer_id, ctx.jurisdiction_id, ctx.currency_id)
            .await?;

        let genres = if summary.genres.is_empty() {
            extract_genres_from_detail(detail_node)
        } else {
            summary.genres.clone()
        };
        if !genres.is_empty() {
            let _ = update_video_game_genres(&self.db, video_game_id, &genres).await;
        }

        if let Some(pid) = video_game_source_id {
            link_provider_offer(&self.db, pid, offer_id, Some(0.8)).await?;
        }

        if let Some((avg, cnt)) = extract_rating_from_detail(detail_node) {
            if ratings_conflict_supported(&self.db).await.unwrap_or(false) {
                let _ = sqlx
                    ::query(
                        "INSERT INTO video_game_ratings_by_locale (video_game_id, locale, average_rating, rating_count, rating_updated_at) VALUES ($1,$2,$3,$4, now()) ON CONFLICT (video_game_id, locale) DO UPDATE SET average_rating=EXCLUDED.average_rating, rating_count=EXCLUDED.rating_count, rating_updated_at=now()"
                    )
                    .persistent(false)
                    .bind(video_game_id)
                    .bind(&ctx.locale)
                    .bind(avg)
                    .bind(cnt)
                    .execute(&self.db.pool).await;
            } else {
                // Legacy schema without a unique constraint on (video_game_id, locale); best-effort insert.
                let _ = sqlx
                    ::query(
                        "INSERT INTO video_game_ratings_by_locale (video_game_id, locale, average_rating, rating_count, rating_updated_at) VALUES ($1,$2,$3,$4, now()) ON CONFLICT DO NOTHING"
                    )
                    .persistent(false)
                    .bind(video_game_id)
                    .bind(&ctx.locale)
                    .bind(avg)
                    .bind(cnt)
                    .execute(&self.db.pool).await;
            }
        }

        // Persist release date if available (from summary or detail); do not overwrite existing
        let release_str = summary
            .release_date
            .as_deref()
            .or_else(|| detail_node.and_then(|n| n.get("releaseDate").and_then(Value::as_str)))
            .or_else(|| detail_node.and_then(|n| n.get("release_date").and_then(Value::as_str)));
        if let Some(date_naive) = release_str.and_then(|s| parse_release_date_any(s)) {
            let _ =
                update_video_game_release_date_if_null(&self.db, video_game_id, date_naive).await;
        }

        if let Some(desc) = extract_synopsis_from_detail(detail_node) {
            let _ = update_video_game_synopsis_prefer_longer(&self.db, video_game_id, &desc).await;
        }

        let _ = update_video_game_display_title_and_region(
            &self.db,
            video_game_id,
            &title_name,
            &ctx.region_code,
        )
        .await;

        self.record_media_and_prices(
            &summary,
            ctx,
            platform_label,
            video_game_id,
            video_game_source_id,
            offer_jurisdiction_id,
        )
        .await?;
        self.total_items += 1;
        self.flush_if_needed().await?;
        Ok(())
    }

    async fn finalize(mut self, regions: &[String]) -> Result<IngestSummary> {
        self.flush_prices().await?;

        if php_compat_schema(&self.db).await.unwrap_or(false) {
            info!(
                provider_id = self.provider_id,
                price_rows = self.post_summary.total_price_rows_written,
                "psstore ingest complete (php compat: skipped alerts + verification)"
            );
            return Ok(IngestSummary {
                regions: regions.to_vec(),
                items: self.total_items,
                price_points: self.total_prices,
                current_upserts: 0,
            });
        }

        if !self.seen_offer_jurisdictions.is_empty() {
            let touched: Vec<i64> = self.seen_offer_jurisdictions.iter().copied().collect();
            crate::database_ops::alerts::evaluate_alerts(&self.db, &touched).await?;
        }
        self.post_summary
            .offer_jurisdiction_ids
            .extend(self.seen_offer_jurisdictions.iter().copied());
        self.post_summary.verify(&self.db, self.provider_id).await?;
        info!(
            provider_id = self.provider_id,
            price_rows = self.post_summary.total_price_rows_written,
            provider_items = self.post_summary.video_game_source_ids.len(),
            offer_jurisdictions = self.post_summary.offer_jurisdiction_ids.len(),
            "psstore ingest verification complete"
        );
        Ok(IngestSummary {
            regions: regions.to_vec(),
            items: self.total_items,
            price_points: self.total_prices,
            current_upserts: self.total_current_upserts,
        })
    }

    async fn flush_if_needed(&mut self) -> Result<()> {
        const PRICE_BATCH_SIZE: usize = 200;
        if self.price_rows.len() >= PRICE_BATCH_SIZE {
            self.flush_prices().await?;
        }
        Ok(())
    }

    async fn flush_prices(&mut self) -> Result<()> {
        if self.price_rows.is_empty() {
            return Ok(());
        }

        // Legacy Laravel/PHP schema: insert into region_prices and skip modern tables.
        if php_compat_schema(&self.db).await.unwrap_or(false) {
            let rows = std::mem::take(&mut self.price_rows);
            let n = rows.len();
            crate::database_ops::ingest_providers::ingest_prices(&self.db, rows).await?;
            self.total_prices += n;
            self.post_summary.total_price_rows_written += n;
            info!(rows = n, "psstore php compat: inserted region_prices rows");
            return Ok(());
        }
        const BATCH_SIZE: usize = 200;
        const CP_AGENT: &str = PS_STORE_PROVIDER_KEY;
        const CP_PRIORITY: i16 = 100;
        let mut idx = 0usize;
        while idx < self.price_rows.len() {
            let end = (idx + BATCH_SIZE).min(self.price_rows.len());
            let slice = &self.price_rows[idx..end];
            self.db.bulk_insert_prices(slice).await?;

            let mut latest_map: HashMap<i64, &PriceRow> = HashMap::new();
            for row in slice {
                self.post_summary
                    .offer_jurisdiction_ids
                    .insert(row.offer_jurisdiction_id);
                if let Some(pid) = row.video_game_source_id {
                    self.post_summary.record_provider_item(pid);
                }
                latest_map
                    .entry(row.offer_jurisdiction_id)
                    .and_modify(|cur| {
                        if row.recorded_at > cur.recorded_at {
                            *cur = row;
                        }
                    })
                    .or_insert(row);
            }
            self.post_summary.total_price_rows_written += slice.len();

            let mut current_rows: Vec<CurrentPriceRow> = Vec::with_capacity(latest_map.len());
            for (_, row) in latest_map {
                current_rows.push(CurrentPriceRow {
                    offer_jurisdiction_id: row.offer_jurisdiction_id,
                    amount_minor: row.amount_minor,
                    recorded_at: row.recorded_at,
                    agent: CP_AGENT.to_string(),
                    agent_priority: CP_PRIORITY,
                });
            }
            self.db.upsert_current_prices(&current_rows).await?;
            self.total_current_upserts += current_rows.len();
            self.post_summary.total_current_updates += current_rows.len();
            for row in &current_rows {
                self.seen_offer_jurisdictions
                    .insert(row.offer_jurisdiction_id);
                self.post_summary
                    .offer_jurisdiction_ids
                    .insert(row.offer_jurisdiction_id);
            }

            idx = end;
            self.total_prices += slice.len();
            info!(
                processed = idx,
                total_pending = self.price_rows.len(),
                "psstore ingested price rows batch"
            );
        }
        self.price_rows.clear();
        Ok(())
    }

    async fn record_media_and_prices(
        &mut self,
        summary: &PsProductSummary,
        ctx: &LocaleContext,
        platform_label: &str,
        _video_game_id: i64,
        video_game_source_id: Option<i64>,
        offer_jurisdiction_id: i64,
    ) -> Result<()> {
        // Contract:
        // - Do NOT persist screenshots/logos/icons/thumbs/thumbnails.
        // - Do NOT persist any semantic “role” / subtype system.
        // - Persist only photo/video by writing into split tables (game_images/game_videos).

        let game_provider_id = self.ensure_psstore_game_provider_id().await?;
        let video_game_source_id_text: Option<String> = summary
            .product_id
            .as_deref()
            .or(summary.concept_id.as_deref())
            .map(|s| s.to_string());

        let page_url_opt = summary.product_id.as_ref().map(|pid| {
            format!(
                "https://store.playstation.com/{}/product/{}",
                ctx.locale, pid
            )
        });
        // --- Images (photo) → game_images ---
        const MAX_IMAGES: usize = 6;
        let mut seen_image_urls: HashSet<String> = HashSet::new();
        let mut image_rank: i32 = 0;
        for media in &summary.media_images {
            if image_rank as usize >= MAX_IMAGES {
                break;
            }
            let url = match media.url.as_deref() {
                Some(u) if !u.is_empty() => u,
                _ => continue,
            };
            if !seen_image_urls.insert(url.to_string()) {
                continue;
            }
            if !psstore_is_allowed_image(url, media.role.as_deref()) {
                continue;
            }
            let stored_url = match normalize_url_varchar_255(url) {
                Some(u) => u,
                None => {
                    warn!(
                        url_len = url.len(),
                        "psstore image url too long for varchar(255); skipping"
                    );
                    continue;
                }
            };
            let image_key = sha256_hex(url);
            let platforms: Vec<String> = vec![platform_label.to_string()];
            let metadata = json!({
                "source": "psstore",
                "locale": ctx.locale,
                "platform": platform_label,
                "page_url": page_url_opt,
            });
            // NOTE: provider_payload must NOT include role (contract: no persisted role/subtype system).
            let provider_payload = json!({
                "typename": media.typename,
                "media_type": media.media_type,
            });
            let _ = sqlx::query(
                "INSERT INTO game_images \
                    (game_provider_id, image_key, url, rank, metadata, created_at, updated_at, vg_source_item_id, video_game_source_id, provider_payload, platforms) \
                 VALUES ($1,$2,$3,$4,$5,now(),now(),$6,$7,$8,$9) \
                 ON CONFLICT (game_provider_id, image_key) DO UPDATE SET \
                    url = EXCLUDED.url, \
                    rank = LEAST(game_images.rank, EXCLUDED.rank), \
                    metadata = EXCLUDED.metadata, \
                    vg_source_item_id = COALESCE(game_images.vg_source_item_id, EXCLUDED.vg_source_item_id), \
                    video_game_source_id = COALESCE(game_images.video_game_source_id, EXCLUDED.video_game_source_id), \
                    provider_payload = EXCLUDED.provider_payload, \
                    platforms = (\
                        SELECT ARRAY(\
                            SELECT DISTINCT e \
                            FROM unnest(COALESCE(game_images.platforms, '{}'::text[]) || COALESCE(EXCLUDED.platforms, '{}'::text[])) AS e\
                        )\
                    ), \
                    updated_at = now()",
            )
            .persistent(false)
            .bind(game_provider_id)
            .bind(&image_key)
            .bind(&stored_url)
            .bind(image_rank)
            .bind(metadata)
            .bind(video_game_source_id_text.as_deref())
            .bind(self.video_game_source_id)
            .bind(provider_payload)
            .bind(&platforms)
            .execute(&self.db.pool)
            .await;

            image_rank += 1;
        }

        // Fall back to simple URL list if structured media is absent.
        if image_rank == 0 {
            for url in &summary.media_image_urls {
                if image_rank as usize >= MAX_IMAGES {
                    break;
                }
                if url.is_empty() || !seen_image_urls.insert(url.to_string()) {
                    continue;
                }
                if !psstore_is_allowed_image(url, None) {
                    continue;
                }
                let stored_url = match normalize_url_varchar_255(url) {
                    Some(u) => u,
                    None => {
                        warn!(
                            url_len = url.len(),
                            "psstore image url too long for varchar(255); skipping"
                        );
                        continue;
                    }
                };
                let image_key = sha256_hex(url);
                let platforms: Vec<String> = vec![platform_label.to_string()];
                let metadata = json!({
                    "source": "psstore",
                    "locale": ctx.locale,
                    "platform": platform_label,
                    "page_url": page_url_opt,
                });
                let provider_payload = json!({});
                let _ = sqlx::query(
                    "INSERT INTO game_images \
                        (game_provider_id, image_key, url, rank, metadata, created_at, updated_at, vg_source_item_id, video_game_source_id, provider_payload, platforms) \
                     VALUES ($1,$2,$3,$4,$5,now(),now(),$6,$7,$8,$9) \
                     ON CONFLICT (game_provider_id, image_key) DO UPDATE SET \
                        url = EXCLUDED.url, \
                        rank = LEAST(game_images.rank, EXCLUDED.rank), \
                        metadata = EXCLUDED.metadata, \
                        vg_source_item_id = COALESCE(game_images.vg_source_item_id, EXCLUDED.vg_source_item_id), \
                        video_game_source_id = COALESCE(game_images.video_game_source_id, EXCLUDED.video_game_source_id), \
                        provider_payload = EXCLUDED.provider_payload, \
                        platforms = (\
                            SELECT ARRAY(\
                                SELECT DISTINCT e \
                                FROM unnest(COALESCE(game_images.platforms, '{}'::text[]) || COALESCE(EXCLUDED.platforms, '{}'::text[])) AS e\
                            )\
                        ), \
                        updated_at = now()",
                )
                .persistent(false)
                .bind(game_provider_id)
                .bind(&image_key)
                .bind(&stored_url)
                .bind(image_rank)
                .bind(metadata)
                .bind(video_game_source_id_text.as_deref())
                .bind(self.video_game_source_id)
                .bind(provider_payload)
                .bind(&platforms)
                .execute(&self.db.pool)
                .await;
                image_rank += 1;
            }
        }

        // --- Videos → game_videos ---
        const MAX_VIDEOS: usize = 3;
        let video_name = truncate_varchar_255(summary.name.as_deref().unwrap_or(""));
        let mut seen_video_urls: HashSet<String> = HashSet::new();
        let mut video_count: usize = 0;
        for media in &summary.media_videos {
            if video_count >= MAX_VIDEOS {
                break;
            }
            let url = match media.url.as_deref() {
                Some(u) if !u.is_empty() => u,
                _ => continue,
            };
            if !seen_video_urls.insert(url.to_string()) {
                continue;
            }
            let stored_stream_url = normalize_url_varchar_255(url);
            if stored_stream_url.is_none() {
                warn!(
                    url_len = url.len(),
                    "psstore video url too long for varchar(255); storing only page_url"
                );
            }
            let video_key = sha256_hex(url);
            let metadata = json!({
                "source": "psstore",
                "locale": ctx.locale,
                "platform": platform_label,
                "page_url": page_url_opt,
                "stream_url_full": url,
            });
            // NOTE: provider_payload must NOT include role (contract: no persisted role/subtype system).
            let provider_payload = json!({
                "typename": media.typename,
                "media_type": media.media_type,
            });
            let stored_page_url = page_url_opt.as_deref().and_then(normalize_url_varchar_255);
            let _ = sqlx::query(
                "INSERT INTO game_videos \
                    (game_provider_id, video_key, name, site_detail_url, stream_url, metadata, created_at, updated_at, vg_source_item_id, video_game_source_id, provider_payload) \
                 VALUES ($1,$2,$3,$4,$5,$6,now(),now(),$7,$8,$9) \
                 ON CONFLICT (game_provider_id, video_key) DO UPDATE SET \
                    name = EXCLUDED.name, \
                    site_detail_url = COALESCE(EXCLUDED.site_detail_url, game_videos.site_detail_url), \
                    stream_url = COALESCE(EXCLUDED.stream_url, game_videos.stream_url), \
                    metadata = EXCLUDED.metadata, \
                    vg_source_item_id = COALESCE(game_videos.vg_source_item_id, EXCLUDED.vg_source_item_id), \
                    video_game_source_id = COALESCE(game_videos.video_game_source_id, EXCLUDED.video_game_source_id), \
                    provider_payload = EXCLUDED.provider_payload, \
                    updated_at = now()",
            )
            .persistent(false)
            .bind(game_provider_id)
            .bind(&video_key)
            .bind(&video_name)
            .bind(stored_page_url)
            .bind(stored_stream_url)
            .bind(metadata)
            .bind(video_game_source_id_text.as_deref())
            .bind(self.video_game_source_id)
            .bind(provider_payload)
            .execute(&self.db.pool)
            .await;

            video_count += 1;
        }
        if video_count == 0 {
            for url in &summary.media_video_urls {
                if video_count >= MAX_VIDEOS {
                    break;
                }
                if url.is_empty() || !seen_video_urls.insert(url.to_string()) {
                    continue;
                }
                let stored_stream_url = normalize_url_varchar_255(url);
                if stored_stream_url.is_none() {
                    warn!(
                        url_len = url.len(),
                        "psstore video url too long for varchar(255); storing only page_url"
                    );
                }
                let video_key = sha256_hex(url);
                let metadata = json!({
                    "source": "psstore",
                    "locale": ctx.locale,
                    "platform": platform_label,
                    "page_url": page_url_opt,
                    "stream_url_full": url,
                });
                let provider_payload = json!({});
                let stored_page_url = page_url_opt.as_deref().and_then(normalize_url_varchar_255);
                let _ = sqlx::query(
                    "INSERT INTO game_videos \
                        (game_provider_id, video_key, name, site_detail_url, stream_url, metadata, created_at, updated_at, vg_source_item_id, video_game_source_id, provider_payload) \
                     VALUES ($1,$2,$3,$4,$5,$6,now(),now(),$7,$8,$9) \
                     ON CONFLICT (game_provider_id, video_key) DO UPDATE SET \
                        name = EXCLUDED.name, \
                        site_detail_url = COALESCE(EXCLUDED.site_detail_url, game_videos.site_detail_url), \
                        stream_url = COALESCE(EXCLUDED.stream_url, game_videos.stream_url), \
                        metadata = EXCLUDED.metadata, \
                        vg_source_item_id = COALESCE(game_videos.vg_source_item_id, EXCLUDED.vg_source_item_id), \
                        video_game_source_id = COALESCE(game_videos.video_game_source_id, EXCLUDED.video_game_source_id), \
                        provider_payload = EXCLUDED.provider_payload, \
                        updated_at = now()",
                )
                .persistent(false)
                .bind(game_provider_id)
                .bind(&video_key)
                .bind(&video_name)
                .bind(stored_page_url)
                .bind(stored_stream_url)
                .bind(metadata)
                .bind(video_game_source_id_text.as_deref())
                .bind(self.video_game_source_id)
                .bind(provider_payload)
                .execute(&self.db.pool)
                .await;
                video_count += 1;
            }
        }

        let recorded_at = Utc::now();
        // Ignore zero/negative amounts to avoid polluting current_price and hot facts
        if let Some(base) = summary.base_price_minor.filter(|v| *v > 0) {
            self.price_rows.push(PriceRow {
                offer_jurisdiction_id,
                video_game_source_id,
                recorded_at,
                amount_minor: base,
                tax_inclusive: true,
                fx_minor_per_unit: None,
                btc_sats_per_unit: None,
                meta: json!({
                    "src": "psstore",
                    "kind": "base",
                    "locale": ctx.locale.clone(),
                    "platform": platform_label,
                    "region_code": ctx.region_code.clone(),
                }),
            });
        }
        if let Some(discount) = summary.discounted_price_minor.filter(|v| *v > 0) {
            self.price_rows.push(PriceRow {
                offer_jurisdiction_id,
                video_game_source_id,
                recorded_at,
                amount_minor: discount,
                tax_inclusive: true,
                fx_minor_per_unit: None,
                btc_sats_per_unit: None,
                meta: json!({
                    "src": "psstore",
                    "kind": "discount",
                    "locale": ctx.locale.clone(),
                    "platform": platform_label,
                    "region_code": ctx.region_code.clone(),
                }),
            });
        }
        Ok(())
    }
}

/// Ingest a fixed set of PS product IDs directly (concept-first pricing), bypassing category grids.
async fn process_direct_products(
    ing: &mut ProductIngestor,
    ctx: &LocaleContext,
    product_ids: &[String],
    platform_label: &str,
    platform_id: i64,
) -> Result<()> {
    for external_product_id in product_ids {
        // Try to fetch detail; proceed even if it fails
        let detail_opt = match ctx
            .client
            .product_detail_raw(&ctx.locale, external_product_id)
            .await
        {
            Ok(v) => Some(v),
            Err(e) => {
                warn!(locale=%ctx.locale, product_id=%external_product_id, error=%e, "psstore detail fetch failed in direct mode; continuing");
                None
            }
        };

        // Build a minimal summary from detail (if present) else from product id; pricing will override amounts
        let mut summary = PsProductSummary {
            product_id: Some(external_product_id.clone()),
            concept_id: None,
            name: detail_opt
                .as_ref()
                .and_then(|d| {
                    d.get("data")
                        .and_then(|dd| dd.get("metGetProductById"))
                        .and_then(|m| m.get("name"))
                        .and_then(|s| s.as_str())
                })
                .map(|s| s.to_string()),
            release_date: detail_opt
                .as_ref()
                .and_then(|d| {
                    d.get("data")
                        .and_then(|dd| dd.get("metGetProductById"))
                        .and_then(|m| m.get("releaseDate"))
                        .and_then(|s| s.as_str())
                })
                .map(|s| s.to_string()),
            base_price_minor: None,
            discounted_price_minor: None,
            is_free: None,
            media_urls: Vec::new(),
            media_image_urls: Vec::new(),
            media_video_urls: Vec::new(),
            media_images: Vec::new(),
            media_videos: Vec::new(),
            genres: Vec::new(),
            average_rating: None,
            rating_count: None,
        };

        // Resolve concept and fetch pricing
        if summary.concept_id.is_none() {
            summary.concept_id = match ctx
                .client
                .concept_by_product_id_raw(&ctx.locale, external_product_id)
                .await
            {
                Ok(v) => v
                    .get("data")
                    .and_then(|d| d.get("metGetConceptByProductIdQuery"))
                    .and_then(|n| n.get("conceptId"))
                    .and_then(|s| s.as_str())
                    .map(|s| s.to_string()),
                Err(_) => None,
            };
        }
        if let Some(concept_id) = summary.concept_id.clone() {
            if let Ok(pricing) = ctx
                .client
                .concept_pricing_raw(&ctx.locale, &concept_id)
                .await
            {
                let (base_minor, discount_minor) = parse_pricing_minor(&pricing);
                if let Some(b) = base_minor.filter(|v| *v > 0) {
                    summary.base_price_minor = Some(b);
                }
                if let Some(d) = discount_minor.filter(|v| *v > 0) {
                    summary.discounted_price_minor = Some(d);
                }
            }
        }

        ing.ingest_product(
            ctx,
            platform_label,
            platform_id,
            summary,
            external_product_id.clone(),
            detail_opt.unwrap_or(Value::Null),
        )
        .await?;
        ing.flush_if_needed().await?;
    }
    Ok(())
}

/// Attempt to parse base/discounted minor units from a concept pricing payload.
/// This function is defensive against schema changes: it searches for string numeric
/// fields commonly used by Sony (basePrice, discountedPrice) and falls back to numbers.
pub fn parse_pricing_minor(v: &Value) -> (Option<i64>, Option<i64>) {
    fn parse_money_str_to_minor(s: &str) -> Option<i64> {
        let mut normalized = s.replace(',', ".");
        normalized.retain(|c| (c.is_ascii_digit() || c == '.'));
        if normalized.is_empty() {
            return None;
        }
        let mut parts = normalized.splitn(2, '.');
        let int_part = parts.next().unwrap_or("");
        let frac_part = parts.next();
        if let Some(frac) = frac_part {
            let mut frac_norm = frac
                .chars()
                .filter(|c| c.is_ascii_digit())
                .collect::<String>();
            if frac_norm.len() == 0 {
                frac_norm.push('0');
                frac_norm.push('0');
            } else if frac_norm.len() == 1 {
                frac_norm.push('0');
            } else if frac_norm.len() > 2 {
                frac_norm.truncate(2);
            }
            let total = format!("{}{}", int_part, frac_norm);
            total.parse::<i64>().ok()
        } else {
            int_part.parse::<i64>().ok().map(|v| v * 100)
        }
    }
    fn find_prices(obj: &Value, out: &mut (Option<i64>, Option<i64>)) {
        match obj {
            Value::Object(map) => {
                // Preferred fields
                if let Some(Value::String(s)) = map.get("basePrice") {
                    if out.0.is_none() {
                        out.0 = parse_money_str_to_minor(s);
                    }
                }
                if let Some(Value::String(s)) = map.get("discountedPrice") {
                    if out.1.is_none() {
                        out.1 = parse_money_str_to_minor(s);
                    }
                }
                // Some variants embed numeric cents
                if let Some(Value::Number(n)) = map.get("basePriceMinor") {
                    if out.0.is_none() {
                        out.0 = n.as_i64();
                    }
                }
                if let Some(Value::Number(n)) = map.get("discountedPriceMinor") {
                    if out.1.is_none() {
                        out.1 = n.as_i64();
                    }
                }
                for (_k, vv) in map {
                    find_prices(vv, out);
                }
            }
            Value::Array(arr) => {
                for el in arr {
                    find_prices(el, out);
                }
            }
            _ => {}
        }
    }
    let mut out = (None, None);
    if let Some(data) = v.get("data") {
        if let Some(node) = data.get("metGetPricingDataByConceptId") {
            find_prices(node, &mut out);
        } else {
            find_prices(data, &mut out);
        }
    } else {
        find_prices(v, &mut out);
    }
    out
}
