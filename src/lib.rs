pub mod api;
pub mod cli;
pub mod database_ops;
pub mod env_boot;
pub mod normalization;
pub mod orchestrator;

pub mod util {
    pub mod env;
}
pub use actix_web::http::header;

// PS Store seeding pipeline (library function, not a bin)
// Loops through categories and pages with offset to seed up to TOTAL_PAGES per locale
use anyhow::{anyhow, Result};
use chrono::Utc;
use serde::Serialize;
use serde_json::{json, Value};

use database_ops::db::{Db, PriceRow};
use database_ops::ingest_providers::{
    ensure_country, ensure_currency, ensure_national_jurisdiction, ensure_offer,
    ensure_offer_jurisdiction, ensure_platform, ensure_product_named, ensure_provider,
    ensure_provider_item, ensure_retailer, ensure_sellable, ensure_software_row,
    ensure_vg_source_media_links_with_meta, ensure_video_game, ensure_video_game_title,
    ingest_prices, link_provider_offer, merge_video_game_metadata,
    update_video_game_display_title_and_region, update_video_game_genres,
    update_video_game_genres_if_empty, update_video_game_global_rating_if_null,
    update_video_game_synopsis_prefer_longer, PostIngestSummary,
};
use database_ops::playstation::prices::parse_pricing_minor;
// collections used later in function scope; kept minimal here

use psstore_client::PsMedia;
use psstore_client::{PsConfig, PsStoreClient};

#[derive(Clone)]
struct LocaleContext {
    jurisdiction_id: i64,
    currency_id: i64,
    currency_code: String,
}

pub async fn psstore_seed_pipeline(db: &Db) -> Result<PostIngestSummary> {
    async fn visible_table_exists(db: &Db, table: &str) -> Result<bool> {
        let visible: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
            .persistent(false)
            .bind(table)
            .fetch_one(&db.pool)
            .await?;
        Ok(visible)
    }

    // Config via env
    // Centralized dotenv & env helpers
    crate::util::env::init_env();
    let regions = load_regions();
    if regions.is_empty() {
        return Ok(PostIngestSummary::default());
    }

    // php-compat: if the target database doesn't have the tables the PS store pipeline
    // requires, skip gracefully instead of failing hard.
    //
    // NOTE: we intentionally check *visibility* via search_path resolution because all
    // queries in this binary are unqualified.
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
        if !visible_table_exists(db, t).await.unwrap_or(false) {
            missing.push(t);
        }
    }
    if !missing.is_empty() {
        use crate::database_ops::ingest_providers::php_compat_schema;
        let compat = php_compat_schema(db).await.unwrap_or(false);
        tracing::warn!(
            missing_tables = ?missing,
            php_compat = compat,
            "psstore_seed_pipeline: required schema missing; skipping PlayStation seed to preserve php-compat (no migrations)"
        );
        return Ok(PostIngestSummary::default());
    }
    // Year window controls (inclusive)
    use crate::util::env::{env_flag, env_opt, env_parse};
    let year_min: i32 = env_parse("YEAR_MIN", 2020);
    let year_max: i32 = env_parse("YEAR_MAX", 2025);
    println!("remember: restricting to releases between {year_min}-{year_max} inclusive\n");
    // Prefer PSSTORE_SHA256 if provided, else fall back to PS_HASH, else default
    let _ps_hash = env_opt("PSSTORE_SHA256")
        .or_else(|| env_opt("PS_HASH"))
        .unwrap_or_else(|| {
            "9845afc0dbaab4965f6563fffc703f588c8e76792000e8610843b8d3ee9c4c09".into()
        });
    let cat_ps4 =
        env_opt("PS4_CATEGORY").unwrap_or_else(|| "44d8bb20-653e-431e-8ad0-c0a365f68d2f".into());
    let cat_ps5 =
        env_opt("PS5_CATEGORY").unwrap_or_else(|| "4cbf39e2-5749-4970-ba81-93a489e4570c".into());
    let rps_per_locale: u32 = env_parse("PS_STORE_RPS", 3u32);
    let retry_attempts: u32 = env_parse("PS_STORE_MAX_RETRIES", 3u32);
    let retry_base_ms: u64 = env_parse("PS_STORE_BACKOFF_MS", 300u64);
    let page_size: u32 = env_parse("PS_PAGE_SIZE", 100u32);
    let total_pages: u32 = env_parse("PS_TOTAL_PAGES", 500u32);
    let start_page: u32 = env_parse("PS_PAGE_START", 0u32);
    let backfill_mode: bool = env_flag("PS_BACKFILL", true);
    // Deprecated: PS_CUTOFF_YEAR; superseded by YEAR_MIN/YEAR_MAX
    let _cutoff_year: i32 = env_parse("PS_CUTOFF_YEAR", year_min);

    // Ensure base static entities
    let ps5_platform_id = ensure_platform(db, "PS5", Some("ps5")).await?;
    let ps4_platform_id = ensure_platform(db, "PS4", Some("ps4")).await?;
    let provider_id =
        ensure_provider(db, "playstation_store", "storefront", Some("ps-store")).await?;
    let retailer_id = ensure_retailer(db, "PlayStation", Some("playstation")).await?;
    let mut post_summary = PostIngestSummary::default();

    // Pre-create per-locale jurisdiction and cache currency_id to avoid repeated lookups later
    let mut locale_ctx: std::collections::HashMap<String, LocaleContext> =
        std::collections::HashMap::new();
    for loc in &regions {
        let code2 = loc.split('-').nth(1).unwrap_or("us").to_uppercase();
        let (cur_code, cur_name) = currency_for_country(&code2);
        let mu = currency_minor_unit(cur_code);
        let currency_id = ensure_currency(db, cur_code, cur_name, mu).await?;
        let country_id = ensure_country(db, &code2, &code2, currency_id).await?;
        let juris_id = ensure_national_jurisdiction(db, country_id).await?;

        // Cache both juris_id and currency_id per locale for later use
        locale_ctx.insert(
            loc.clone(),
            LocaleContext {
                jurisdiction_id: juris_id,
                currency_id,
                currency_code: cur_code.to_string(),
            },
        );
    }

    // Aggregation maps across all locales
    // (HashMap, HashSet already imported above)
    struct GlobalAgg {
        genres: std::collections::HashSet<String>,
        rating_sum: f64,
        rating_count: i64,
        vg_id: i64,
    }
    let mut global_aggs: std::collections::HashMap<String, GlobalAgg> =
        std::collections::HashMap::new(); // product_key -> agg
    let mut provider_item_cache: std::collections::HashMap<String, i64> =
        std::collections::HashMap::new(); // product_id -> video_game_source_id
    let mut sellable_cache: std::collections::HashMap<i64, i64> = std::collections::HashMap::new(); // video_game_title_id -> sellable_id
    let mut offer_cache: std::collections::HashMap<i64, i64> = std::collections::HashMap::new(); // sellable_id -> offer_id
    let mut offer_juris_cache: std::collections::HashMap<(i64, i64), i64> =
        std::collections::HashMap::new(); // (offer_id,jurisdiction_id) -> offer_jurisdiction_id
    let mut price_ladder_snapshots: Vec<PriceLadderSnapshot> = Vec::new();

    for locale in &regions {
        // HashSet already imported at module scope
        use std::time::Instant;
        let mut concept_id_cache: std::collections::HashMap<String, Option<String>> =
            std::collections::HashMap::new();
        let mut concept_price_cache: std::collections::HashMap<String, (Option<i64>, Option<i64>)> =
            std::collections::HashMap::new();
        let mut processed_products: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        let mut ensure_durations: Vec<std::time::Duration> = Vec::new();
        let cfg = PsConfig {
            locales: vec![locale.clone()],
            rps: rps_per_locale,
            retry_attempts,
            retry_base_delay_ms: retry_base_ms,
            ..PsConfig::default()
        };
        let client = PsStoreClient::new(cfg);
        if let Some(ctx) = locale_ctx.get(locale).cloned() {
            for cat_id in [&cat_ps5, &cat_ps4] {
                match fetch_price_buckets(&client, locale, cat_id).await {
                    Ok(buckets) if !buckets.is_empty() => {
                        log_price_buckets(locale, cat_id, &buckets);
                        price_ladder_snapshots.push(PriceLadderSnapshot {
                            locale: locale.clone(),
                            category_id: cat_id.clone(),
                            currency_code: ctx.currency_code.clone(),
                            buckets,
                        });
                    }
                    Ok(_) => {}
                    Err(err) => {
                        tracing::warn!(locale=%locale, category=%cat_id, error=%err, "psstore price ladder capture failed");
                    }
                }
            }
        }
        for (cat_id, platform_id) in [(&cat_ps5, ps5_platform_id), (&cat_ps4, ps4_platform_id)] {
            let mut page = start_page;
            let mut stop_due_to_year = false;
            while page < start_page + total_pages && !stop_due_to_year {
                let offset = page * page_size;
                // Descending by release date to walk backwards in time; enforce YEAR_MIN..=YEAR_MAX
                // Use productReleaseDate (PlayStation API expects this key); using releaseDate can cause ES shard errors
                let list = client
                    .category_grid_retrieve_sorted(
                        locale,
                        cat_id,
                        page_size,
                        offset,
                        "productReleaseDate",
                        false,
                    )
                    .await
                    .unwrap_or_default();
                if list.is_empty() {
                    page += 1;
                    continue;
                }

                // Collect rows for this page and write in batch
                let mut price_rows: Vec<PriceRow> = Vec::with_capacity(list.len() * 2);

                // Rating + detail fetch with semaphore to bound concurrency; maintain original order mapping
                use futures::stream::{FuturesUnordered, StreamExt};
                use futures::Future;
                use std::pin::Pin;
                use tokio::sync::Semaphore;
                let rating_sem = std::sync::Arc::new(Semaphore::new(
                    std::env::var("PS_RATING_CONCURRENCY")
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(4),
                ));
                let items = list; // rename for reuse
                let mut tasks: FuturesUnordered<
                    Pin<
                        Box<
                            dyn Future<Output = (usize, Option<(f32, i64)>, serde_json::Value)>
                                + Send,
                        >,
                    >,
                > = FuturesUnordered::new();
                for (idx, it) in items.iter().enumerate() {
                    let fut: Pin<
                        Box<
                            dyn Future<Output = (usize, Option<(f32, i64)>, serde_json::Value)>
                                + Send,
                        >,
                    > = if let Some(pid) = &it.product_id {
                        let client_cloned = client.clone();
                        let locale_cloned = locale.clone();
                        let pid_cloned = pid.clone();
                        let sem = rating_sem.clone();
                        Box::pin(async move {
                            let _permit = sem.acquire().await.ok();
                            let rating = client_cloned
                                .product_star_rating(&locale_cloned, &pid_cloned)
                                .await
                                .ok()
                                .flatten();
                            let detail = client_cloned
                                .product_detail_raw(&locale_cloned, &pid_cloned)
                                .await
                                .unwrap_or_else(|_| serde_json::Value::Null);
                            (idx, rating, detail)
                        })
                    } else {
                        Box::pin(async move { (idx, None, serde_json::Value::Null) })
                    };
                    tasks.push(fut);
                }

                // Collect ratings + details into vectors indexed by original item order
                let mut ratings: Vec<Option<(f32, i64)>> = vec![None; items.len()];
                let mut details: Vec<serde_json::Value> =
                    vec![serde_json::Value::Null; items.len()];
                while let Some((idx, r, detail)) = tasks.next().await {
                    if idx < ratings.len() {
                        ratings[idx] = r;
                        details[idx] = detail;
                    }
                }

                // Collect batch media rows for this page (will flush once)
                let mut rating_rows: Vec<(i64, String, f32, i64)> = Vec::new();
                for (idx, it) in items.into_iter().enumerate() {
                    let mut it = it;
                    let product_id_for_lookup = it.product_id.clone();
                    let mut concept_id = it.concept_id.clone();
                    if concept_id.is_none() {
                        if let Some(pid) = product_id_for_lookup.as_ref() {
                            if let Some(cached) = concept_id_cache.get(pid) {
                                concept_id = cached.clone();
                            } else {
                                let fetched = match client
                                    .concept_by_product_id_raw(locale, pid)
                                    .await
                                {
                                    Ok(payload) => extract_concept_id_from_response(&payload),
                                    Err(err) => {
                                        tracing::warn!(locale=%locale, product_id=%pid, error=%err, "psstore concept lookup failed");
                                        None
                                    }
                                };
                                concept_id_cache.insert(pid.clone(), fetched.clone());
                                concept_id = fetched;
                            }
                        }
                    }
                    if it.concept_id.is_none() {
                        it.concept_id = concept_id.clone();
                    }
                    if let Some(concept_id_value) = concept_id.clone() {
                        let (base_minor, discount_minor) = if let Some(cached) =
                            concept_price_cache.get(&concept_id_value)
                        {
                            *cached
                        } else {
                            match client.concept_pricing_raw(locale, &concept_id_value).await {
                                Ok(payload) => {
                                    let parsed = parse_pricing_minor(&payload);
                                    concept_price_cache.insert(concept_id_value.clone(), parsed);
                                    parsed
                                }
                                Err(err) => {
                                    tracing::warn!(locale=%locale, concept_id=%concept_id_value, error=%err, "psstore concept pricing fetch failed");
                                    concept_price_cache
                                        .insert(concept_id_value.clone(), (None, None));
                                    (None, None)
                                }
                            }
                        };
                        if let Some(b) = base_minor.filter(|v| *v > 0) {
                            it.base_price_minor = Some(b);
                        }
                        if let Some(d) = discount_minor.filter(|v| *v > 0) {
                            it.discounted_price_minor = Some(d);
                        }
                    } else if product_id_for_lookup.is_some() {
                        tracing::debug!(locale=%locale, product_id=?product_id_for_lookup, "psstore conceptId unavailable after lookup");
                    }

                    // Title + slug
                    let title = it.name.clone().unwrap_or_else(|| "unknown".to_string());
                    let slug = normalize_title(&title);

                    // Check release year for window (parse first 4 digits if present)
                    if let Some(release_year) = it
                        .release_date
                        .as_ref()
                        .and_then(|d| d.get(0..4))
                        .and_then(|y| y.parse::<i32>().ok())
                    {
                        // Skip items newer than YEAR_MAX to keep the window tight
                        if release_year > year_max {
                            continue;
                        }
                        // Stop once we've crossed below YEAR_MIN (we are in descending order)
                        if release_year < year_min {
                            stop_due_to_year = true;
                            break;
                        }
                    }

                    // Ensure product hierarchy only once per product across locales
                    let product_key = it.product_id.clone().unwrap_or_else(|| slug.clone());
                    let (_product_id, _title_id, _vg_id, _sellable_id, offer_id) =
                        if !processed_products.contains(&product_key) {
                            let t0 = Instant::now();
                            let product_id =
                                ensure_product_named(db, "software", &slug, &title).await?;
                            ensure_software_row(db, product_id).await?;
                            let title_id =
                                ensure_video_game_title(db, product_id, &title, Some(&slug))
                                    .await?;
                            let _vg_id = ensure_video_game(db, title_id, platform_id, None).await?;
                            let sellable_id = match sellable_cache.get(&title_id) {
                                Some(&sid) => sid,
                                None => {
                                    let sid = ensure_sellable(db, "software", product_id).await?;
                                    sellable_cache.insert(title_id, sid);
                                    sid
                                }
                            };
                            let offer_id = match offer_cache.get(&sellable_id) {
                                Some(&oid) => oid,
                                None => {
                                    let oid =
                                        ensure_offer(db, sellable_id, retailer_id, None).await?;
                                    offer_cache.insert(sellable_id, oid);
                                    oid
                                }
                            };
                            processed_products.insert(product_key.clone());
                            ensure_durations.push(t0.elapsed());
                            (product_id, title_id, _vg_id, sellable_id, offer_id)
                        } else {
                            // Lookup existing rows cheaply
                            let row = sqlx
                            ::query(
                                "SELECT p.id as product_id, vgt.id as title_id, vg.id as vg_id, s.id as sellable_id, o.id as offer_id FROM products p JOIN video_game_titles vgt ON vgt.video_game_id=p.id JOIN video_games vg ON vg.title_id=vgt.id JOIN sellables s ON s.software_title_id=vgt.id JOIN offers o ON o.sellable_id=s.id WHERE vgt.normalized_title=$1 LIMIT 1"
                            )
                            .persistent(false)
                            .bind(&slug)
                            .fetch_optional(&db.pool).await?;
                            use sqlx::Row;
                            if let Some(r) = row {
                                (
                                    r.get::<i64, _>("product_id"),
                                    r.get::<i64, _>("title_id"),
                                    r.get::<i64, _>("vg_id"),
                                    r.get::<i64, _>("sellable_id"),
                                    r.get::<i64, _>("offer_id"),
                                )
                            } else {
                                // Fallback: run ensures (rare)
                                let product_id =
                                    ensure_product_named(db, "software", &slug, &title).await?;
                                ensure_software_row(db, product_id).await?;
                                let title_id =
                                    ensure_video_game_title(db, product_id, &title, Some(&slug))
                                        .await?;
                                let _vg_id =
                                    ensure_video_game(db, title_id, platform_id, None).await?;
                                let sellable_id =
                                    ensure_sellable(db, "software", product_id).await?;
                                let offer_id =
                                    ensure_offer(db, sellable_id, retailer_id, None).await?;
                                (product_id, title_id, _vg_id, sellable_id, offer_id)
                            }
                        };

                    // Map locale to offer jurisdiction and cached currency_id
                    let locale_meta = locale_ctx
                        .get(locale)
                        .expect("jurisdiction & currency for locale");
                    let juris_id = locale_meta.jurisdiction_id;
                    let currency_id = locale_meta.currency_id;
                    let oj_id = match offer_juris_cache.get(&(offer_id, juris_id)) {
                        Some(&cached) => cached,
                        None => {
                            let new_id =
                                ensure_offer_jurisdiction(db, offer_id, juris_id, currency_id)
                                    .await?;
                            offer_juris_cache.insert((offer_id, juris_id), new_id);
                            new_id
                        }
                    };
                    post_summary.offer_jurisdiction_ids.insert(oj_id);

                    // Extract genres from detail payload if available (metGetProductById response)
                    let mut genres: Vec<String> = Vec::new();
                    let mut detail_media_sets: Option<(Vec<PsMedia>, Vec<PsMedia>)> = None;
                    if let Some(detail_obj) = details.get(idx) {
                        genres = extract_genres(detail_obj);
                        if !genres.is_empty() {
                            tracing::debug!(video_game_id=_vg_id, %slug, genres=?genres, "psstore genres extracted");
                        }
                        if let Some(syn) = extract_synopsis(detail_obj) {
                            let _ =
                                update_video_game_synopsis_prefer_longer(db, _vg_id, &syn).await;
                        }
                        // Backfill: also set display_title and union region code, mirroring prices ingest behavior
                        let _ = update_video_game_display_title_and_region(
                            db,
                            _vg_id,
                            &title,
                            locale.split('-').nth(1).unwrap_or("us"),
                        )
                        .await;
                        detail_media_sets = extract_detail_media(detail_obj);
                    }

                    // Backfill: write genres array directly on video_games if present
                    if !genres.is_empty() {
                        let _ = update_video_game_genres(db, _vg_id, &genres).await;
                    }

                    // Provider mapping (if we have an external id)
                    let mut video_game_source_id: Option<i64> = None;
                    if let Some(ext) = &it.product_id {
                        let pid = if let Some(cached) = provider_item_cache.get(ext) {
                            *cached
                        } else {
                            let pid = ensure_provider_item(db, provider_id, ext, None).await?;
                            provider_item_cache.insert(ext.clone(), pid);
                            pid
                        };
                        post_summary.record_provider_item(pid);
                        video_game_source_id = Some(pid);
                        link_provider_offer(db, pid, offer_id, Some(0.9)).await?;
                        // Media links (images + videos) with refined role classification & meta
                        let mut urls: Vec<(
                            String,
                            Option<String>,
                            Option<String>,
                            Option<String>,
                        )> = Vec::new();
                        // Attempt to use detailed media (higher fidelity) if present
                        let push_summary_media = |targets: &mut Vec<(
                            String,
                            Option<String>,
                            Option<String>,
                            Option<String>,
                        )>| {
                            for u in &it.media_image_urls {
                                targets.push((
                                    u.clone(),
                                    Some("image".into()),
                                    Some("screenshot".into()),
                                    it.name.clone(),
                                ));
                            }
                            for u in &it.media_video_urls {
                                targets.push((
                                    u.clone(),
                                    Some("video".into()),
                                    Some("trailer".into()),
                                    it.name.clone(),
                                ));
                            }
                        };
                        if let Some((ref detail_images, ref detail_videos)) = detail_media_sets {
                            let mut detail_added = false;
                            for m in detail_images {
                                if let Some(url) = m.url.as_deref() {
                                    detail_added = true;
                                    let role_raw = m.role.as_deref().unwrap_or("");
                                    let classified = classify_image_role(role_raw)
                                        .unwrap_or("screenshot")
                                        .to_string();
                                    urls.push((
                                        url.to_string(),
                                        Some("image".into()),
                                        Some(classified),
                                        it.name.clone(),
                                    ));
                                }
                            }
                            for m in detail_videos {
                                if let Some(url) = m.url.as_deref() {
                                    detail_added = true;
                                    let role_raw = m.role.as_deref().unwrap_or("");
                                    urls.push((
                                        url.to_string(),
                                        Some("video".into()),
                                        Some(classify_video_role(role_raw).to_string()),
                                        it.name.clone(),
                                    ));
                                }
                            }
                            if !detail_added {
                                push_summary_media(&mut urls);
                            }
                        } else {
                            push_summary_media(&mut urls);
                        }
                        if !urls.is_empty() {
                            let meta = serde_json::json!({
                                "locale": locale,
                                "platform_id": platform_id,
                                "genres": genres,
                            });
                            let _ = ensure_vg_source_media_links_with_meta(
                                db,
                                video_game_source_id.unwrap(),
                                Some(_vg_id),
                                &urls,
                                "psstore",
                                Some(meta),
                            )
                            .await?;
                        }


                    }

                    // Prices: only enqueue when not in backfill mode
                    if !backfill_mode {
                        let now = Utc::now();
                        if let Some(base) = it.base_price_minor {
                            if base > 0 {
                                price_rows.push(PriceRow {
                                    offer_jurisdiction_id: oj_id,
                                    video_game_source_id,
                                    recorded_at: now,
                                    amount_minor: base,
                                    tax_inclusive: true,
                                    fx_minor_per_unit: None,
                                    btc_sats_per_unit: None,
                                    meta: json!({"src":"psstore","kind":"base","locale":locale}),
                                });
                            }
                        }
                        if let Some(discount) = it.discounted_price_minor {
                            if discount > 0 {
                                price_rows.push(PriceRow {
                                    offer_jurisdiction_id: oj_id,
                                    video_game_source_id,
                                    recorded_at: now,
                                    amount_minor: discount,
                                    tax_inclusive: true,
                                    fx_minor_per_unit: None,
                                    btc_sats_per_unit: None,
                                    meta: json!({"src":"psstore","kind":"discount","locale":locale}),
                                });
                            }
                        }
                    }

                    // Per-locale rating row
                    if let Some((avg, cnt)) = ratings.get(idx).cloned().flatten() {
                        rating_rows.push((_vg_id, locale.clone(), avg, cnt));
                        // Global aggregation
                        let entry = global_aggs.entry(product_key.clone()).or_insert(GlobalAgg {
                            genres: std::collections::HashSet::new(),
                            rating_sum: 0.0,
                            rating_count: 0,
                            vg_id: _vg_id,
                        });
                        entry.rating_sum += (avg as f64) * (cnt as f64);
                        entry.rating_count += cnt;
                        for g in &genres {
                            entry.genres.insert(g.clone());
                        }
                    } else {
                        // Still aggregate genres even if rating missing
                        let entry = global_aggs.entry(product_key.clone()).or_insert(GlobalAgg {
                            genres: std::collections::HashSet::new(),
                            rating_sum: 0.0,
                            rating_count: 0,
                            vg_id: _vg_id,
                        });
                        for g in &genres {
                            entry.genres.insert(g.clone());
                        }
                    }
                }

                if !rating_rows.is_empty() {
                    // Bulk upsert ratings_by_locale
                    use sqlx::QueryBuilder;
                    let mut qb = QueryBuilder::new(
                        "INSERT INTO video_game_ratings_by_locale (video_game_id, locale, average_rating, rating_count, rating_updated_at) VALUES ",
                    );
                    let mut sep = qb.separated(", ");
                    for (vg_id, loc, avg, cnt) in &rating_rows {
                        sep.push("(")
                            .push_bind(vg_id)
                            .push(", ")
                            .push_bind(loc)
                            .push(", ")
                            .push_bind(avg)
                            .push(", ")
                            .push_bind(cnt)
                            .push(", now())");
                    }
                    qb.push(
                        " ON CONFLICT (video_game_id, locale) DO UPDATE SET average_rating=EXCLUDED.average_rating, rating_count=EXCLUDED.rating_count, rating_updated_at=now()"
                    );
                    qb.build().execute(&db.pool).await?;
                    // Realtime notify (optional)
                    let _ = sqlx::query("SELECT pg_notify('ratings_upsert', $1)")
                        .persistent(false)
                        .bind(format!("{{\"count\":{}}}", rating_rows.len()))
                        .execute(&db.pool)
                        .await;
                }

                if !backfill_mode {
                    if !price_rows.is_empty() {
                        let batch_len = price_rows.len();
                        let ingest_result = ingest_prices(db, price_rows).await?;
                        post_summary.record_batch(batch_len, &ingest_result);
                    }
                }

                page += 1;
            }
        }
        if !ensure_durations.is_empty() {
            let total = ensure_durations.len();
            let sum: std::time::Duration = ensure_durations
                .iter()
                .copied()
                .reduce(|a, b| a + b)
                .unwrap();
            let avg_ms = (sum.as_secs_f64() * 1000.0) / (total as f64);
            let mut sorted = ensure_durations.clone();
            sorted.sort();
            let p95 = sorted
                .get(((total as f64) * 0.95).floor() as usize)
                .unwrap_or(sorted.last().unwrap());
            println!(
                "[psstore] ensure metrics locale={locale} count={total} avg_ms={avg_ms:.2} p95_ms={:.2}",
                p95.as_secs_f64() * 1000.0
            );
        }
    }

    if !price_ladder_snapshots.is_empty() {
        let ladder_export = PriceLadderExport {
            generated_at: chrono::Utc::now().to_rfc3339(),
            ladders: price_ladder_snapshots,
        };
        let ladder_path = format!(
            "exports/psstore_price_ladders_{}.json",
            chrono::Utc::now().format("%Y%m%d_%H%M%S")
        );
        if std::path::Path::new("exports").exists() || std::fs::create_dir_all("exports").is_ok() {
            let _ = std::fs::write(
                &ladder_path,
                serde_json::to_string_pretty(&ladder_export).unwrap_or_else(|_| "{}".into()),
            );
            println!(
                "[psstore] price ladders captured locales={} file={}",
                ladder_export
                    .ladders
                    .iter()
                    .map(|l| l.locale.as_str())
                    .collect::<std::collections::HashSet<&str>>()
                    .len(),
                ladder_path
            );
        }
    }

    // Persist aggregated metadata per product
    for (_key, agg) in &global_aggs {
        let genres_vec: Vec<String> = agg.genres.iter().cloned().collect();
        let genres_json = serde_json::Value::from(genres_vec.clone());
        let genres_array = if genres_vec.is_empty() {
            None
        } else {
            Some(genres_vec)
        };
        let global_avg = if agg.rating_count > 0 {
            Some(agg.rating_sum / (agg.rating_count as f64))
        } else {
            None
        };
        let global_count = if agg.rating_count > 0 {
            Some(agg.rating_count)
        } else {
            None
        };
        let patch = serde_json::json!({
            "genres_union": genres_json,
            "rating_global": global_avg,
            "rating_count_global": agg.rating_count,
        });
        merge_video_game_metadata(db, agg.vg_id, patch).await?;
        if let Some(ref g) = genres_array {
            let _ = update_video_game_genres_if_empty(db, agg.vg_id, g).await;
        }
        let _ =
            update_video_game_global_rating_if_null(db, agg.vg_id, global_avg, global_count).await;
    }

    // Persist a PS Store-derived monthly toplist based on aggregated star ratings.
    // This is the missing bridge that lets Laravel Spotlight consume PS Store ratings
    // via `provider_toplists`/`provider_toplist_items` just like RAWG/IGDB.
    {
        use chrono::Datelike;
        use database_ops::ingest_providers::{
            replace_provider_toplist_items, upsert_provider_toplist,
        };

        let min_count: i64 = std::env::var("PSSTORE_TOP_RATED_MIN_COUNT")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(50);

        let limit: usize = std::env::var("PSSTORE_TOP_RATED_LIMIT")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(80);

        let mut scored: Vec<(i64, f64, i64)> = global_aggs
            .values()
            .filter_map(|agg| {
                if agg.rating_count < min_count {
                    return None;
                }
                if agg.rating_count <= 0 {
                    return None;
                }
                let avg = agg.rating_sum / (agg.rating_count as f64);
                if !avg.is_finite() {
                    return None;
                }
                Some((agg.vg_id, avg, agg.rating_count))
            })
            .collect();

        // Highest average rating first; then prefer higher rating counts for stability.
        scored.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| b.2.cmp(&a.2))
                .then_with(|| a.0.cmp(&b.0))
        });
        scored.truncate(limit);

        let ranked_products: Vec<(u32, i64)> = scored
            .iter()
            .enumerate()
            .map(|(idx, (product_id, _avg, _cnt))| ((idx as u32) + 1, *product_id))
            .collect();

        if !ranked_products.is_empty() {
            let today = chrono::Utc::now().date_naive();
            let period_start = chrono::NaiveDate::from_ymd_opt(today.year(), today.month(), 1)
                .unwrap_or(today)
                .format("%Y-%m-%d")
                .to_string();
            let (ny, nm) = if today.month() == 12 {
                (today.year() + 1, 1)
            } else {
                (today.year(), today.month() + 1)
            };
            let period_end = chrono::NaiveDate::from_ymd_opt(ny, nm, 1)
                .unwrap_or(today)
                .format("%Y-%m-%d")
                .to_string();

            let toplist_slug = format!("psstore:top_monthly:{period_start}:{period_end}");
            let meta = serde_json::json!({
                "kind": "top_rated",
                "metric": "weighted_average_star_rating",
                "min_rating_count": min_count,
                "limit": ranked_products.len(),
                "regions": regions.clone(),
            });

            let toplist_id = upsert_provider_toplist(
                db,
                "psstore",
                &toplist_slug,
                "top_monthly",
                Some(period_start.as_str()),
                Some(period_end.as_str()),
                None,
                Some(meta),
            )
            .await?;

            replace_provider_toplist_items(db, toplist_id, &ranked_products).await?;

            eprintln!("INFO: psstore toplist snapshot written - provider=psstore, list_type=top_monthly, period={} to {}, items={}, min_count={}",
                     period_start, period_end, ranked_products.len(), min_count);
        } else {
            eprintln!("INFO: psstore toplist snapshot skipped (no rated items over threshold) - provider=psstore, list_type=top_monthly, min_count={}", min_count);
        }
    }

    // Export metrics JSON
    let metrics: Vec<serde_json::Value> = global_aggs
        .iter()
        .map(|(k, agg)| {
            let rating_val = if agg.rating_count > 0 {
                serde_json::json!(agg.rating_sum / (agg.rating_count as f64))
            } else {
                serde_json::Value::Null
            };
            serde_json::json!({
                "product_key": k,
                "genres_union": agg.genres,
                "rating_count_global": agg.rating_count,
                "rating_global": rating_val,
                "video_game_id": agg.vg_id
            })
        })
        .collect();
    let snapshot = serde_json::json!({
        "generated_at": chrono::Utc::now().to_rfc3339(),
        "products": metrics
    });
    let metrics_path = format!(
        "exports/psstore_metrics_{}.json",
        chrono::Utc::now().format("%Y%m%d_%H%M%S")
    );
    if std::path::Path::new("exports").exists() || std::fs::create_dir_all("exports").is_ok() {
        let _ = std::fs::write(
            &metrics_path,
            serde_json::to_string_pretty(&snapshot).unwrap_or_else(|_| "{}".into()),
        );
    }
    println!(
        "[psstore] metrics written {} products={} file={}",
        global_aggs.len(),
        snapshot["products"]
            .as_array()
            .map(|a| a.len())
            .unwrap_or(0),
        metrics_path
    );
    // Telemetry streaming (optional)
    if let Ok(endpoint) = std::env::var("PSSTORE_TELEMETRY_ENDPOINT") {
        if !endpoint.is_empty() {
            if let Ok(client) = reqwest::Client::builder().build() {
                let body = serde_json::to_string(&snapshot).unwrap_or_else(|_| "{}".into());
                let _ = client
                    .post(&endpoint)
                    .header("content-type", "application/json")
                    .body(body)
                    .send()
                    .await;
            }
        }
    }
    // RLS adaptation note: if Supabase RLS is enabled, ensure service role key is used; this pipeline relies on unrestricted access for bulk ingestion.
    // Optionally: set SUPABASE_SERVICE_ROLE env and verify connection role.

    post_summary.verify(db, provider_id).await?;
    eprintln!("INFO: psstore seed pipeline summary - provider_id={}, price_rows={}, provider_items={}, offer_jurisdictions={}",
             provider_id, post_summary.total_price_rows_written, post_summary.video_game_source_ids.len(), post_summary.offer_jurisdiction_ids.len());

    Ok(post_summary)
}

fn normalize_title(s: &str) -> String {
    s.to_lowercase()
        .replace(|c: char| !c.is_ascii_alphanumeric(), "-")
        .trim_matches('-')
        .to_string()
}

fn load_regions() -> Vec<String> {
    // Normalize to IETF-style locale tags: language (lowercase) + '-' + region (uppercase)
    // Examples: "en-US", "en-GB", "de-DE", "ja-JP". Underscores are accepted and converted to '-'.
    let raw = std::env::var("PS_STORE_REGIONS").unwrap_or_else(|_| "en-us en-gb de-de".into());
    raw.split(|c: char| (c == ',' || c == ' '))
        .filter(|s| !s.is_empty())
        .map(|s| s.trim())
        .map(|tok| {
            let t = tok.replace('_', "-");
            let mut parts = t.splitn(2, '-');
            let lang = parts.next().unwrap_or("").to_ascii_lowercase();
            let region = parts.next().unwrap_or("").to_ascii_uppercase();
            if !lang.is_empty() && !region.is_empty() {
                format!("{}-{}", lang, region)
            } else {
                // Fallback: if region missing, return normalized language only
                lang
            }
        })
        .filter(|s| !s.is_empty())
        .collect()
}

fn currency_minor_unit(code: &str) -> i16 {
    match code.to_ascii_uppercase().as_str() {
        "JPY" | "KRW" | "VND" | "CLP" | "ISK" | "HUF" => 0,
        "BHD" | "IQD" | "KWD" | "JOD" | "OMR" | "TND" => 3,
        _ => 2,
    }
}

pub(crate) fn currency_for_country(code2: &str) -> (&'static str, &'static str) {
    match code2 {
        "US" => ("USD", "US Dollar"),
        "CA" => ("CAD", "Canadian Dollar"),
        "AU" => ("AUD", "Australian Dollar"),
        "NZ" => ("NZD", "New Zealand Dollar"),
        "GB" => ("GBP", "British Pound"),
        "DE" | "FR" | "ES" | "IT" | "NL" | "BE" | "PT" | "IE" | "FI" | "GR" | "AT" | "LU"
        | "SI" | "SK" | "LV" | "LT" | "EE" | "MT" | "CY" => ("EUR", "Euro"),
        "PL" => ("PLN", "Polish Zloty"),
        "RU" => ("RUB", "Russian Ruble"),
        "TR" => ("TRY", "Turkish Lira"),
        "JP" => ("JPY", "Japanese Yen"),
        "KR" => ("KRW", "South Korean Won"),
        "BR" => ("BRL", "Brazilian Real"),
        "HK" => ("HKD", "Hong Kong Dollar"),
        "TW" => ("TWD", "New Taiwan Dollar"),
        "SE" => ("SEK", "Swedish Krona"),
        "NO" => ("NOK", "Norwegian Krone"),
        "DK" => ("DKK", "Danish Krone"),
        "ZA" => ("ZAR", "South African Rand"),
        "SA" => ("SAR", "Saudi Riyal"),
        "AR" => ("ARS", "Argentine Peso"),
        "MX" => ("MXN", "Mexican Peso"),
        _ => ("USD", "US Dollar"),
    }
}

#[derive(Debug, Clone, Serialize)]
struct PriceBucketValue {
    display_name: String,
    key: String,
    count: i64,
}

#[derive(Debug, Serialize)]
struct PriceLadderSnapshot {
    locale: String,
    category_id: String,
    currency_code: String,
    buckets: Vec<PriceBucketValue>,
}

#[derive(Debug, Serialize)]
struct PriceLadderExport {
    generated_at: String,
    ladders: Vec<PriceLadderSnapshot>,
}

async fn fetch_price_buckets(
    client: &PsStoreClient,
    locale: &str,
    category_id: &str,
) -> Result<Vec<PriceBucketValue>> {
    let req = client.category_request(
        category_id,
        24,
        0,
        None,
        None,
        None,
        Some(vec!["webBasePrice".to_string()]),
    );
    let raw = client
        .category_grid_raw(locale, &req)
        .await
        .map_err(|err| anyhow!(err.to_string()))?;
    Ok(extract_price_buckets_from_value(&raw))
}

fn extract_price_buckets_from_value(v: &Value) -> Vec<PriceBucketValue> {
    let mut out = Vec::new();
    let Some(facets) = v
        .get("data")
        .and_then(|d| d.get("categoryGridRetrieve"))
        .and_then(|grid| grid.get("facetOptions"))
        .and_then(|opts| opts.as_array())
    else {
        return out;
    };
    for facet in facets {
        if facet
            .get("name")
            .and_then(|n| n.as_str())
            .map(|s| s.eq_ignore_ascii_case("webBasePrice"))
            .unwrap_or(false)
        {
            if let Some(values) = facet.get("values").and_then(|v| v.as_array()) {
                for bucket in values {
                    if let (Some(key), Some(display)) = (
                        bucket.get("key").and_then(|k| k.as_str()),
                        bucket.get("displayName").and_then(|d| d.as_str()),
                    ) {
                        let count = bucket.get("count").and_then(|c| c.as_i64()).unwrap_or(0);
                        out.push(PriceBucketValue {
                            display_name: display.to_string(),
                            key: key.to_string(),
                            count,
                        });
                    }
                }
            }
            break;
        }
    }
    out
}

fn log_price_buckets(locale: &str, category_id: &str, buckets: &[PriceBucketValue]) {
    if buckets.is_empty() {
        return;
    }
    println!(
        "[psstore] price ladder locale={} category={} buckets={}",
        locale,
        category_id,
        buckets.len()
    );
    for bucket in buckets {
        println!(
            "    {:>18} | key={} | count={}",
            bucket.display_name, bucket.key, bucket.count
        );
    }
}

fn extract_concept_id_from_response(v: &Value) -> Option<String> {
    v.get("data")
        .and_then(|d| d.get("metGetConceptByProductIdQuery"))
        .and_then(|node| {
            node.get("conceptId")
                .or_else(|| node.get("concept").and_then(|c| c.get("conceptId")))
        })
        .and_then(|val| val.as_str())
        .map(|s| s.to_string())
}

// --- Media & Genre Helpers ---
fn classify_video_role(role: &str) -> &'static str {
    match role.to_ascii_uppercase().as_str() {
        "TRAILER" => "trailer",
        "PREVIEW" => "preview",
        "GAMEPLAY" => "gameplay",
        "CINEMATIC" | "CUTSCENE" => "cinematic",
        "TEASER" => "teaser",
        _ => "gameplay",
    }
}

fn classify_image_role(role: &str) -> Option<&'static str> {
    match role.to_ascii_uppercase().as_str() {
        "HERO" => Some("hero"),
        "COVER" | "GAMEHUB_COVER_ART" => Some("cover"),
        "LOGO" => Some("logo"),
        "SCREENSHOT" => Some("screenshot"),
        // High-level art variants normalize to artwork for enum compatibility
        "BACKGROUND"
        | "PORTRAIT_BANNER"
        | "FOUR_BY_THREE_BANNER"
        | "SIXTEEN_BY_NINE_BANNER"
        | "EDITION_KEY_ART"
        | "MASTER" => Some("artwork"),
        _ => None,
    }
}

fn detail_product_node<'a>(detail: &'a serde_json::Value) -> Option<&'a serde_json::Value> {
    detail.get("data").and_then(|d| {
        d.get("metGetProductById")
            .or_else(|| d.get("productRetrieve"))
            .or_else(|| d.get("productRetrieveById"))
    })
}

fn extract_genres(detail: &serde_json::Value) -> Vec<String> {
    let mut genres: Vec<String> = Vec::new();
    if let Some(prod) = detail_product_node(detail) {
        for key in [
            "productGenres",
            "genres",
            "genre",
            "combinedLocalizedGenres",
        ]
        .iter()
        {
            if let Some(val) = prod.get(*key) {
                collect_genre_strings(val, &mut genres);
            }
        }
        if let Some(concept) = prod.get("concept") {
            for key in [
                "productGenres",
                "genres",
                "genre",
                "combinedLocalizedGenres",
            ]
            .iter()
            {
                if let Some(val) = concept.get(*key) {
                    collect_genre_strings(val, &mut genres);
                }
            }
        }
    }
    genres.sort();
    genres.dedup();
    genres
}

fn collect_genre_strings(v: &serde_json::Value, out: &mut Vec<String>) {
    match v {
        serde_json::Value::String(s) => {
            if !s.is_empty() {
                out.push(s.to_string());
            }
        }
        serde_json::Value::Array(arr) => {
            for el in arr {
                collect_genre_strings(el, out);
            }
        }
        serde_json::Value::Object(o) => {
            for k in ["displayName", "name", "key", "genre"].iter() {
                if let Some(serde_json::Value::String(s)) = o.get(*k) {
                    if !s.is_empty() {
                        out.push(s.to_string());
                    }
                }
            }
            for (_k, vv) in o {
                collect_genre_strings(vv, out);
            }
        }
        _ => {}
    }
}

fn extract_detail_media(detail: &serde_json::Value) -> Option<(Vec<PsMedia>, Vec<PsMedia>)> {
    let Some(prod) = detail_product_node(detail) else {
        return None;
    };
    let mut images: Vec<PsMedia> = Vec::new();
    let mut videos: Vec<PsMedia> = Vec::new();
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    append_media_entries(prod.get("media"), &mut seen, &mut images, &mut videos);
    if let Some(concept) = prod.get("concept") {
        append_media_entries(concept.get("media"), &mut seen, &mut images, &mut videos);
    }
    if images.is_empty() && videos.is_empty() {
        None
    } else {
        Some((images, videos))
    }
}

fn append_media_entries(
    source: Option<&serde_json::Value>,
    seen: &mut std::collections::HashSet<String>,
    images: &mut Vec<PsMedia>,
    videos: &mut Vec<PsMedia>,
) {
    let Some(arr) = source.and_then(|m| m.as_array()) else {
        return;
    };
    for entry in arr {
        let Some(url) = media_field(entry, "url") else {
            continue;
        };
        let url = url.trim();
        if url.is_empty() {
            continue;
        }
        if !seen.insert(url.to_string()) {
            continue;
        }
        let typename = media_field(entry, "__typename").map(|s| s.to_string());
        let media_type = media_field(entry, "type").map(|s| s.to_string());
        let role = media_field(entry, "role").map(|s| s.to_string());
        let ps_media = PsMedia {
            typename: typename.clone(),
            media_type: media_type.clone(),
            role: role.clone(),
            url: Some(url.to_string()),
        };
        let is_image = typename
            .as_deref()
            .map(|s| (s.eq_ignore_ascii_case("IMAGE") || s.eq_ignore_ascii_case("IMAGEMEDIA")))
            .unwrap_or(false)
            || media_type
                .as_deref()
                .map(|s| s.eq_ignore_ascii_case("IMAGE"))
                .unwrap_or(false);
        if is_image {
            images.push(ps_media);
        } else {
            videos.push(ps_media);
        }
    }
}

fn media_field<'a>(entry: &'a serde_json::Value, key: &str) -> Option<&'a str> {
    entry
        .get(key)
        .and_then(|v| v.as_str())
        .or_else(|| {
            entry
                .get("media")
                .and_then(|m| m.get(key))
                .and_then(|v| v.as_str())
        })
        .or_else(|| {
            entry
                .get("media")
                .and_then(|m| m.get("media"))
                .and_then(|mm| mm.get(key))
                .and_then(|v| v.as_str())
        })
}

fn extract_synopsis(detail: &serde_json::Value) -> Option<String> {
    // Prefer PlayStation specific LONG Description: { __typename:"Description", type:"LONG", value:"..." }
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
    fn find_ps_long_desc(v: &serde_json::Value) -> Option<String> {
        match v {
            serde_json::Value::Object(obj) => {
                let tn = obj
                    .get("__typename")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_ascii_lowercase();
                let ty = obj
                    .get("type")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_ascii_lowercase();
                if tn == "description" && ty == "long" {
                    if let Some(serde_json::Value::String(s)) = obj.get("value") {
                        return Some(clean_text(s));
                    }
                }
                for (_k, vv) in obj {
                    if let Some(s) = find_ps_long_desc(vv) {
                        return Some(s);
                    }
                }
                None
            }
            serde_json::Value::Array(arr) => {
                for el in arr {
                    if let Some(s) = find_ps_long_desc(el) {
                        return Some(s);
                    }
                }
                None
            }
            _ => None,
        }
    }
    if let Some(prod) = detail_product_node(detail) {
        if let Some(ps) = find_ps_long_desc(prod) {
            return Some(ps);
        }
    }
    // Heuristic fallback below

    fn consider_value(val: &serde_json::Value, best: &mut Option<String>) {
        match val {
            serde_json::Value::String(s) => {
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
            serde_json::Value::Array(arr) => {
                for v in arr {
                    consider_value(v, best);
                }
            }
            serde_json::Value::Object(obj) => {
                for (_, v) in obj {
                    consider_value(v, best);
                }
            }
            _ => {}
        }
    }

    fn walk(value: &serde_json::Value, best: &mut Option<String>) {
        match value {
            serde_json::Value::Object(obj) => {
                for (k, v) in obj {
                    let key = k.to_ascii_lowercase();
                    if key.contains("description")
                        || key.contains("summary")
                        || key.contains("synopsis")
                        || key.contains("about")
                    {
                        consider_value(v, best);
                    }
                    walk(v, best);
                }
            }
            serde_json::Value::Array(arr) => {
                for v in arr {
                    walk(v, best);
                }
            }
            _ => {}
        }
    }

    let mut best: Option<String> = None;
    if let Some(prod) = detail_product_node(detail) {
        walk(prod, &mut best);
    } else {
        walk(detail, &mut best);
    }
    best
}
