use crate::util::env::{self as env_util, preflight_check};
use anyhow::{Result, bail};
use chrono::{DateTime, Duration as ChronoDuration, NaiveDate, NaiveDateTime, Utc};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    env,
};
use tracing::{info, warn};

use crate::database_ops::db::{CurrentPriceRow, Db, PriceRow};
use crate::database_ops::ingest_providers::{
    PostIngestSummary, ProviderEntityCache, ensure_country, ensure_currency,
    ensure_national_jurisdiction, ensure_platform, ensure_provider,
    ensure_provider_media_links_with_meta, ensure_retailer, ingest_run_finish, ingest_run_start,
    link_provider_offer, upsert_game_media,
};
use crate::database_ops::media_map::MediaMap;
use psstore_client::{PsConfig, PsMedia, PsProductSummary, PsStoreClient};

fn normalize_title(s: &str) -> String {
    s.to_lowercase()
        .replace(|c: char| !c.is_ascii_alphanumeric(), "-")
        .trim_matches('-')
        .to_string()
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

fn extract_rating_from_detail(detail: Option<&Value>) -> Option<(f32, i64)> {
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
        Some("ps-store"),
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
        let code2 = loc.split('-').nth(1).unwrap_or("us").to_uppercase();
        let (cur_code, cur_name) = match code2.as_str() {
            "US" => ("USD", "US Dollar"),
            "GB" => ("GBP", "British Pound"),
            "DE" => ("EUR", "Euro"),
            _ => ("USD", "US Dollar"),
        };
        let cur_id = ensure_currency(&db, cur_code, cur_name, 2).await?;

        // Legacy PHP compat: jurisdictions table may be absent. In that case, try to reuse an existing sku_regions row
        // keyed by region_code to drive downstream compat mappings. If none found, fall back to the modern path (will error)
        // so the caller sees a clear message.
        let jurisdiction_id = if !table_exists(&db, "jurisdictions").await.unwrap_or(false) {
            if let Some(sr_id) = sqlx::query_scalar::<_, Option<i64>>("SELECT id FROM sku_regions WHERE UPPER(region_code) = $1 LIMIT 1")
                .persistent(false)
                .bind(&code2)
                .fetch_optional(&db.pool)
                .await?
                .flatten()
            {
                sr_id
            } else {
                bail!(
                    "jurisdictions table missing and no sku_regions row for region_code {}; please pre-seed sku_regions or run against full schema",
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
        ensure_provider(&db, "playstation_store", "storefront", Some("ps-store")).await?;
    let retailer_id = ensure_retailer(&db, "PlayStation", Some("playstation")).await?;
    let ps4_platform_id = ensure_platform(&db, "PS4", Some("ps4")).await?;
    let ps5_platform_id = ensure_platform(&db, "PS5", Some("ps5")).await?;

    let mut ingestor = ProductIngestor::new(
        db.clone(),
        provider_id,
        retailer_id,
        ps4_platform_id,
        ps5_platform_id,
        page_size,
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
            let (platform_label, platform_id) = if force_platform.eq_ignore_ascii_case("PS4") {
                ("PS4", ps4_platform_id)
            } else {
                ("PS5", ps5_platform_id)
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
}

impl ProductIngestor {
    fn new(
        db: Db,
        provider_id: i64,
        retailer_id: i64,
        ps4_platform_id: i64,
        ps5_platform_id: i64,
        page_size: u32,
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
        }
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
            let mut attempted: HashSet<u32> = HashSet::new();
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
                        "releaseDate",
                        false,
                    )
                    .await
                {
                    Ok(items) => {
                        selected_items = Some(items);
                        used_size = size;
                        break;
                    }
                    Err(err) => {
                        warn!(
                            locale = %ctx.locale,
                            platform = %platform_label,
                            offset,
                            size,
                            error = %err,
                            "psstore page fetch failed; trying fallback size"
                        );
                        continue;
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
        platform_id: i64,
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
            .ensure_product_named("software", &product_slug, &title_name)
            .await?;
        self.entity_cache.ensure_software_row(product_id).await?;
        let title_id = self
            .entity_cache
            .ensure_video_game_title(product_id, &title_name, Some(&product_slug))
            .await?;
        let video_game_id = self
            .entity_cache
            .ensure_video_game(title_id, platform_id, None)
            .await?;
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
            let _ = sqlx::query("UPDATE video_games SET genres=$1 WHERE id=$2")
                .persistent(false)
                .bind(&genres)
                .bind(video_game_id)
                .execute(&self.db.pool)
                .await;
        }

        let provider_payload_value = provider_payload(detail_node, &summary);
        let provider_item_id = self
            .entity_cache
            .ensure_provider_item(
                self.provider_id,
                &external_product_id,
                Some(provider_payload_value.clone()),
                detail_node.is_some(),
            )
            .await?;
        self.post_summary.record_provider_item(provider_item_id);
        link_provider_offer(&self.db, provider_item_id, offer_id, Some(0.8)).await?;

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
            let _ = sqlx::query("UPDATE video_games SET release_date = $1 WHERE id=$2")
                .persistent(false)
                .bind(date_naive)
                .bind(video_game_id)
                .execute(&self.db.pool)
                .await;
        }

        if let Some(desc) = extract_synopsis_from_detail(detail_node) {
            let _ = sqlx::query("UPDATE video_games SET synopsis = $1 WHERE id=$2")
                .persistent(false)
                .bind(&desc)
                .bind(video_game_id)
                .execute(&self.db.pool)
                .await;
        }

        let _ = sqlx
            ::query(
                "UPDATE video_games SET display_title = $1, region_codes = CASE WHEN region_codes IS NULL THEN ARRAY[$2]::text[] ELSE (SELECT ARRAY(SELECT DISTINCT UNNEST(region_codes || ARRAY[$2]))) END WHERE id=$3"
            )
            .persistent(false)
            .bind(&title_name)
            .bind(&ctx.region_code)
            .bind(video_game_id)
            .execute(&self.db.pool).await;

        self.record_media_and_prices(
            &summary,
            ctx,
            platform_label,
            video_game_id,
            provider_item_id,
            offer_jurisdiction_id,
        )
        .await?;
        self.total_items += 1;
        self.flush_if_needed().await?;
        Ok(())
    }

    async fn finalize(mut self, regions: &[String]) -> Result<IngestSummary> {
        self.flush_prices().await?;
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
            provider_items = self.post_summary.provider_item_ids.len(),
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
        const BATCH_SIZE: usize = 200;
        const CP_AGENT: &str = "ps-store";
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
                if let Some(pid) = row.provider_item_id {
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
        video_game_id: i64,
        provider_item_id: i64,
        offer_jurisdiction_id: i64,
    ) -> Result<()> {
        let media_title = summary.name.clone();
        let mut meta_urls: Vec<(String, Option<String>, Option<String>, Option<String>)> =
            Vec::new();
        // Prefer media from in-memory bag (media_map) keyed by slug::locale::platform
        let product_slug = summary
            .name
            .as_deref()
            .map(normalize_title)
            .unwrap_or_else(|| "untitled".to_string());
        let bag_key = format!("{}::{}::{}", product_slug, ctx.locale, platform_label);
        let (image_from_bag, video_from_bag) = if self.use_mem_bag {
            if let Some(e) = self.mem_bag.get(&bag_key) {
                (e.image_url.clone(), e.video_url.clone())
            } else {
                (None, None)
            }
        } else {
            (None, None)
        };

        // If mem bag has image/video, use those first; else fall back to summary-provided lists
        if let Some(img) = image_from_bag {
            meta_urls.push((
                img,
                Some("image".into()),
                Some("gallery".into()),
                media_title.clone(),
            ));
        } else {
            for url in &summary.media_image_urls {
                meta_urls.push((
                    url.clone(),
                    Some("image".into()),
                    Some("gallery".into()),
                    media_title.clone(),
                ));
            }
        }

        let page_url_opt = summary.product_id.as_ref().map(|pid| {
            format!(
                "https://store.playstation.com/{}/product/{}",
                ctx.locale, pid
            )
        });
        if let Some(vid) = video_from_bag {
            let canonical = page_url_opt.clone().unwrap_or_else(|| vid.clone());
            meta_urls.push((
                canonical,
                Some("video".into()),
                Some("trailer".into()),
                media_title.clone(),
            ));
        } else {
            for url in &summary.media_video_urls {
                let canonical = page_url_opt.clone().unwrap_or_else(|| url.clone());
                meta_urls.push((
                    canonical,
                    Some("video".into()),
                    Some("trailer".into()),
                    media_title.clone(),
                ));
            }
        }
        if !meta_urls.is_empty() {
            let meta = json!({
                "locale": ctx.locale.clone(),
                "platform": platform_label,
                "product_id": summary.product_id,
                "play_urls": summary.media_video_urls,
                "page_url": page_url_opt
            });
            let _ = ensure_provider_media_links_with_meta(
                &self.db,
                provider_item_id,
                Some(video_game_id),
                &meta_urls,
                "psstore",
                Some(meta),
            )
            .await?;
        }

        let classify_media = |media: &PsMedia, default_video: bool| -> Option<(String, Value)> {
            media.url.as_ref()?;
            let role = media.role.as_deref().unwrap_or("");
            let is_image = media
                .typename
                .as_deref()
                .map(|s| s.eq_ignore_ascii_case("IMAGE"))
                .unwrap_or(false)
                || media
                    .media_type
                    .as_deref()
                    .map(|s| s.eq_ignore_ascii_case("IMAGE"))
                    .unwrap_or(false);
            let semantic_type = if is_image {
                if role.eq_ignore_ascii_case("HERO") {
                    "hero"
                } else if role.eq_ignore_ascii_case("COVER") {
                    "cover"
                } else if role.eq_ignore_ascii_case("LOGO") {
                    "logo"
                } else {
                    "screenshot"
                }
            } else if role.eq_ignore_ascii_case("TRAILER") || default_video {
                "trailer"
            } else {
                "gameplay"
            };
            let provider_data = json!({
                "typename": media.typename,
                "media_type": media.media_type,
                "role": media.role,
            });
            Some((semantic_type.to_string(), provider_data))
        };

        // When using mem bag, we won't have structured PsMedia; we still upsert provider_media_links above.
        // Keep fine-grained upserts when summary provided structured media.
        for media in &summary.media_images {
            if let Some((semantic, provider_meta)) = classify_media(media, false) {
                let _ = upsert_game_media(
                    &self.db,
                    video_game_id,
                    "psn",
                    media.url.as_ref().unwrap_or(&semantic),
                    &semantic,
                    media.url.as_ref().unwrap_or(&semantic),
                    provider_meta,
                )
                .await;
            }
        }
        for media in &summary.media_videos {
            if let Some((semantic, mut provider_meta)) = classify_media(media, true) {
                // Attach stable page_url (if any) for reference alongside the playable url
                if let Some(ref pid) = summary.product_id {
                    provider_meta["page_url"] = json!(format!(
                        "https://store.playstation.com/{}/product/{}",
                        ctx.locale, pid
                    ));
                }
                let _ = upsert_game_media(
                    &self.db,
                    video_game_id,
                    "psn",
                    media.url.as_ref().unwrap_or(&semantic),
                    &semantic,
                    media.url.as_ref().unwrap_or(&semantic),
                    provider_meta,
                )
                .await;
            }
        }

        let recorded_at = Utc::now();
        // Ignore zero/negative amounts to avoid polluting current_price and hot facts
        if let Some(base) = summary.base_price_minor.filter(|v| *v > 0) {
            self.price_rows.push(PriceRow {
                offer_jurisdiction_id,
                provider_item_id: Some(provider_item_id),
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
                provider_item_id: Some(provider_item_id),
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
