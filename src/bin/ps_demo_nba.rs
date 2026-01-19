use anyhow::{Context, Result};
use chrono::Utc;
use futures::{stream, StreamExt};
use rayon::prelude::*; // for optional parallel post-processing
use serde_json::json;
use sqlx::Row;
use std::env;

use i_miss_rust::database_ops::db::{CurrentPriceRow, Db, PriceRow};
use i_miss_rust::database_ops::exchange::ExchangeService;
use i_miss_rust::database_ops::ingest_providers::{
    ensure_country, ensure_currency, ensure_national_jurisdiction, ensure_offer,
    ensure_offer_jurisdiction, ensure_platform, ensure_product_named, ensure_provider,
    ensure_provider_item, ensure_retailer, ensure_sellable, ensure_software_row,
    ensure_vg_source_media_links_with_meta, ensure_video_game, ensure_video_game_title,
    link_provider_offer,
};
use i_miss_rust::database_ops::media_map::MediaMap;
use psstore_client::{PsConfig, PsProductSummary, PsStoreClient};

// In-memory aggregation structs
#[derive(Debug, Clone)]
struct LocaleFetchResult {
    locale: String,
    product: Option<PsProductSummary>,
    rating: Option<(f32, i64)>,
}

#[tokio::main]
async fn main() -> Result<()> {
    i_miss_rust::util::env::bootstrap_cli("ps_demo_nba");
    dotenv::dotenv().ok();
    let db_url = env::var("SUPABASE_DB_URL").or_else(|_| env::var("DATABASE_URL"))?;
    let db = Db::connect(&db_url, 10).await?;
    println!("[ps_demo_nba] Connected to DB");

    // Env config
    let ps_hash = env::var("PSSTORE_SHA256")
        .or_else(|_| env::var("PS_HASH"))
        .unwrap_or_else(|_| {
            "9845afc0dbaab4965f6563fffc703f588c8e76792000e8610843b8d3ee9c4c09".into()
        });
    let cat_ps4 =
        env::var("PS4_CATEGORY").unwrap_or_else(|_| "44d8bb20-653e-431e-8ad0-c0a365f68d2f".into());
    let cat_ps5 =
        env::var("PS5_CATEGORY").unwrap_or_else(|_| "4cbf39e2-5749-4970-ba81-93a489e4570c".into());
    // Load regions list (comma or space separated) from env, with global default set.
    let regions = load_regions();
    if regions.is_empty() {
        eprintln!("No regions specified via PS_STORE_REGIONS; aborting");
        return Ok(());
    }

    // Tuning knobs for global runs
    let rps_per_locale: u32 = env::var("PS_STORE_RPS")
        .ok()
        .and_then(|s| s.parse::<f32>().ok().map(|f| f.ceil() as u32))
        .filter(|v| *v > 0)
        .unwrap_or(3);
    let retry_attempts: u32 = env::var("PS_STORE_MAX_RETRIES")
        .ok()
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(5);
    let retry_base_ms: u64 = env::var("PS_STORE_BACKOFF_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(1200);
    let fetch_concurrency: usize = env::var("PS_STORE_CONCURRENCY")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or_else(|| regions.len().min(8).max(2));

    // Defer all ensures; set target title only
    let title = "NBA 2K26";
    let slug = normalize_title(title);
    println!(
        "[ps_demo_nba] Fetch-first mode: target title='{}' slug='{}'",
        title, slug
    );

    // (Offer jurisdictions deferred to persistence phase)

    // Phase 1: concurrent fetch to memory
    println!(
        "[ps_demo_nba] Phase 1: concurrent fetch for {} locales (concurrency={}, rps={}, retries={}, backoff_ms={})",
        regions.len(),
        fetch_concurrency,
        rps_per_locale,
        retry_attempts,
        retry_base_ms
    );

    async fn find_product_for_locale(
        client: &PsStoreClient,
        locale: &str,
        cat_ps5: &str,
        cat_ps4: &str,
        title: &str,
        page_size: u32,
        start_page: u32,
        page_depth: u32,
    ) -> Result<Option<PsProductSummary>> {
        for cat in [cat_ps5, cat_ps4] {
            for page in start_page..start_page + page_depth {
                let offset = page * page_size;
                let list = client
                    .category_grid_retrieve_sorted(locale, cat, page_size, offset, "name", true)
                    .await
                    .unwrap_or_default();
                for it in list {
                    if let Some(name) = it.name.as_ref() {
                        if normalize_title(name) == normalize_title(title) {
                            return Ok(Some(it));
                        }
                    }
                }
            }
        }
        Ok(None)
    }

    async fn fetch_locale_once(
        locale: String,
        cat_ps5: String,
        cat_ps4: String,
        title: String,
        rps: u32,
        retries: u32,
        backoff_ms: u64,
        page_size: u32,
        start_page: u32,
        page_depth: u32,
    ) -> Result<LocaleFetchResult> {
        let cfg = PsConfig {
            locales: vec![locale.clone()],
            rps,
            retry_attempts: retries,
            retry_base_delay_ms: backoff_ms,
            ..PsConfig::default()
        };
        let client = PsStoreClient::new(cfg);
        let product = find_product_for_locale(
            &client, &locale, &cat_ps5, &cat_ps4, &title, page_size, start_page, page_depth,
        )
        .await?;
        let rating = if let Some(p) = &product {
            if let Some(pid) = &p.product_id {
                client
                    .product_star_rating(&locale, pid)
                    .await
                    .ok()
                    .flatten()
            } else {
                None
            }
        } else {
            None
        };
        Ok(LocaleFetchResult {
            locale,
            product,
            rating,
        })
    }

    let page_size_env: u32 = env::var("PS_PAGE_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);
    let start_page_env: u32 = env::var("PS_PAGE_START")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);
    let page_depth_env: u32 = env::var("PS_PAGE_DEPTH")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);
    let fetch_inputs: Vec<(String, String, String, String, u32, u32, u64, u32, u32, u32)> = regions
        .iter()
        .map(|loc| {
            (
                loc.clone(),
                cat_ps5.clone(),
                cat_ps4.clone(),
                title.to_string(),
                rps_per_locale,
                retry_attempts,
                retry_base_ms,
                page_size_env,
                start_page_env,
                page_depth_env,
            )
        })
        .collect();
    let results: Vec<LocaleFetchResult> = stream::iter(fetch_inputs.into_iter().map(
        |(locale, a, b, d, rps, retries, backoff, ps, sp, depth)| async move {
            fetch_locale_once(locale, a, b, d, rps, retries, backoff, ps, sp, depth).await
        },
    ))
    .buffer_unordered(fetch_concurrency)
    .collect::<Vec<_>>()
    .await
    .into_iter()
    .filter_map(|r| match r {
        Ok(v) => Some(v),
        Err(e) => {
            eprintln!("[ps_demo_nba] fetch error: {e:?}");
            None
        }
    })
    .collect();

    let product_locales: Vec<String> = results
        .par_iter()
        .filter(|r| r.product.is_some())
        .map(|r| r.locale.clone())
        .collect();
    println!(
        "[ps_demo_nba] Fetched locales with product match: {:?}",
        product_locales
    );

    // Phase 2: DB materialization (ensures + jurisdictions + writes after fetch)
    println!("[ps_demo_nba] Phase 2: ensures + materialization");
    use std::collections::HashMap;
    let mut offer_juris_to_region: HashMap<i64, String> = HashMap::new();
    // Ensure base entities now
    let ps5_platform_id = ensure_platform(&db, "PS5", Some("ps5")).await?;
    let _ps4_platform_id = ensure_platform(&db, "PS4", Some("ps4")).await?;
    let product_id = ensure_product_named(&db, "software", &slug, title).await?;
    ensure_software_row(&db, product_id).await?;
    let title_id = ensure_video_game_title(&db, product_id, title, Some(&slug)).await?;
    let vg_ps5 = ensure_video_game(&db, title_id, ps5_platform_id, None).await?;
    let provider_id =
        ensure_provider(&db, "playstation_store", "storefront", Some("ps-store")).await?;
    let retailer_id = ensure_retailer(&db, "PlayStation", Some("playstation")).await?;
    let sellable_id = ensure_sellable(&db, "software", product_id).await?;
    let offer_id = ensure_offer(&db, sellable_id, retailer_id, None).await?;
    println!(
        "[ps_demo_nba] Ensures done: product_id={} title_id={} vg_ps5={} offer_id={}",
        product_id, title_id, vg_ps5, offer_id
    );
    // Build offer jurisdictions now
    for loc in &regions {
        let code2 = loc.split('-').nth(1).unwrap_or("us").to_uppercase();
        let (cur_code, cur_name) = match code2.as_str() {
            "US" | "CA" | "AU" | "NZ" => ("USD", "US Dollar"),
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
            "AR" => ("ARS", "Argentine Peso"),
            "ZH" => ("CNY", "Chinese Yuan"),
            "MX" => ("MXN", "Mexican Peso"),
            _ => ("USD", "US Dollar"),
        };
        let mu = currency_minor_unit(cur_code);
        let currency_id = ensure_currency(&db, cur_code, cur_name, mu).await?;
        let country_id = ensure_country(&db, &code2, &code2, currency_id).await?;
        let juris_id = ensure_national_jurisdiction(&db, country_id).await?;
        let oj_id = ensure_offer_jurisdiction(&db, offer_id, juris_id, currency_id).await?;
        offer_juris_to_region.insert(oj_id, loc.clone());
    }
    println!(
        "[ps_demo_nba] Offer jurisdictions ensured: count={}",
        offer_juris_to_region.len()
    );
    // Load optional media fallback map once (best-effort); limits to avoid heavy scans
    let media_fallback =
        MediaMap::from_file("merged_final.json", Some(10000)).unwrap_or_else(|_| MediaMap::empty());
    let mut inserted_media = 0usize;
    let mut price_rows: Vec<PriceRow> = Vec::new();
    let mut linked_video_game_source_id: Option<i64> = None;

    for r in &results {
        if let Some(prod) = &r.product {
            if let Some(ext_id) = &prod.product_id {
                let video_game_source_id =
                    ensure_provider_item(&db, provider_id, ext_id, None).await?;
                link_provider_offer(&db, video_game_source_id, offer_id, Some(0.9)).await?;
                linked_video_game_source_id = Some(video_game_source_id);
                // Print all media links for this locale before dedupe insertion
                let img_count = prod.media_image_urls.len();
                let vid_count = prod.media_video_urls.len();
                let total_count = img_count + vid_count;
                println!(
                    "[ps_demo_nba][media][{}] images={} videos={} total={} (product_id={:?})",
                    r.locale, img_count, vid_count, total_count, prod.product_id
                );
                if img_count > 0 {
                    for (idx, u) in prod.media_image_urls.iter().enumerate() {
                        println!(
                            "  [ps_demo_nba][media][{}][image][{}/{}] {}",
                            r.locale,
                            idx + 1,
                            img_count,
                            u
                        );
                    }
                }
                if vid_count > 0 {
                    for (idx, u) in prod.media_video_urls.iter().enumerate() {
                        println!(
                            "  [ps_demo_nba][media][{}][video][{}/{}] {}",
                            r.locale,
                            idx + 1,
                            vid_count,
                            u
                        );
                    }
                }
                let mut urls: Vec<String> = Vec::new();
                urls.extend(prod.media_image_urls.iter().cloned());
                urls.extend(prod.media_video_urls.iter().cloned());
                // Fallback: if PS Store returned no media, consult MediaMap by normalized title
                if urls.is_empty() {
                    if let Some(img) = media_fallback.get(title) {
                        println!(
                            "  [ps_demo_nba][media][{}][fallback][image] {}",
                            r.locale, img
                        );
                        urls.push(img.to_string());
                    }
                    if let Some(vid) = media_fallback.get_video(title) {
                        println!(
                            "  [ps_demo_nba][media][{}][fallback][video] {}",
                            r.locale, vid
                        );
                        urls.push(vid.to_string());
                    }
                }
                urls.sort();
                urls.dedup();
                if !urls.is_empty() {
                    // Map URLs to structured tuples (url, media_type, role, title) and classify
                    let structured: Vec<(String, Option<String>, Option<String>, Option<String>)> =
                        urls.iter()
                            .map(|u| {
                                let ul = u.to_ascii_lowercase();
                                let is_video = ul.ends_with(".mp4")
                                    || ul.ends_with(".webm")
                                    || ul.ends_with(".m3u8");
                                let media_type = if is_video {
                                    Some("trailer".to_string())
                                } else {
                                    Some("screenshot".to_string())
                                };
                                let role = None; // role not derived here
                                let title = media_type.clone();
                                (u.clone(), media_type, role, title)
                            })
                            .collect();
                    inserted_media += ensure_vg_source_media_links_with_meta(
                        &db,
                        video_game_source_id,
                        Some(vg_ps5),
                        &structured,
                        "psstore",
                        Some(json!({"context":"demo-locale","locale":r.locale})),
                    )
                    .await?;
                }
                if let Some((avg, cnt)) = r.rating {
                    // Upsert per-locale rating
                    let _ = sqlx
                        ::query(
                            "INSERT INTO public.video_game_ratings_by_locale (video_game_id, locale, average_rating, rating_count, rating_updated_at) VALUES ($1,$2,$3,$4, now()) ON CONFLICT (video_game_id, locale) DO UPDATE SET average_rating=EXCLUDED.average_rating, rating_count=EXCLUDED.rating_count, rating_updated_at=now()"
                        )
                        .persistent(false)
                        .bind(vg_ps5)
                        .bind(&r.locale)
                        .bind(avg)
                        .bind(cnt)
                        .execute(&db.pool).await?;
                }
                let now = Utc::now();
                let oj_id = offer_juris_to_region
                    .iter()
                    .find_map(|(k, v)| if *v == r.locale { Some(*k) } else { None })
                    .context("oj_id missing for locale")?;
                if let Some(base) = prod.base_price_minor {
                    price_rows.push(PriceRow {
                        offer_jurisdiction_id: oj_id,
                        video_game_source_id: Some(video_game_source_id),
                        recorded_at: now,
                        amount_minor: base,
                        tax_inclusive: true,
                        fx_minor_per_unit: None,
                        btc_sats_per_unit: None,
                        meta: json!({"src":"psstore","kind":"base","locale":r.locale}),
                    });
                }
                if let Some(discount) = prod.discounted_price_minor {
                    price_rows.push(PriceRow {
                        offer_jurisdiction_id: oj_id,
                        video_game_source_id: Some(video_game_source_id),
                        recorded_at: now,
                        amount_minor: discount,
                        tax_inclusive: true,
                        fx_minor_per_unit: None,
                        btc_sats_per_unit: None,
                        meta: json!({"src":"psstore","kind":"discount","locale":r.locale}),
                    });
                }
                // Persist video URLs into game_videos (idempotent per (video_game_id,url))
                for vurl in &prod.media_video_urls {
                    let _ = ensure_game_video(&db, vg_ps5, video_game_source_id, vurl).await?;
                }
            }
        }
    }

    // Aggregated media summary across all locales
    {
        use std::collections::HashSet;
        let mut all_images: HashSet<String> = HashSet::new();
        let mut all_videos: HashSet<String> = HashSet::new();
        for r in &results {
            if let Some(prod) = &r.product {
                for u in &prod.media_image_urls {
                    all_images.insert(u.clone());
                }
                for u in &prod.media_video_urls {
                    all_videos.insert(u.clone());
                }
            }
        }
        println!(
            "[ps_demo_nba][media][aggregate] unique_images={} unique_videos={} unique_total={}",
            all_images.len(),
            all_videos.len(),
            all_images.len() + all_videos.len()
        );
        if !all_images.is_empty() {
            println!("[ps_demo_nba][media][aggregate] -- IMAGES --");
        }
        for (idx, u) in all_images.iter().enumerate() {
            println!(
                "  [aggregate][image][{}/{}] {}",
                idx + 1,
                all_images.len(),
                u
            );
        }
        if !all_videos.is_empty() {
            println!("[ps_demo_nba][media][aggregate] -- VIDEOS --");
        }
        for (idx, u) in all_videos.iter().enumerate() {
            println!(
                "  [aggregate][video][{}/{}] {}",
                idx + 1,
                all_videos.len(),
                u
            );
        }
    }

    if results.iter().all(|r| r.product.is_none()) {
        println!("[ps_demo_nba] No locales matched product; fallback synthetic path");
        let video_game_source_id = ensure_provider_item(
            &db,
            provider_id,
            "psstore:nba-2k26",
            Some(json!({"source":"fallback"})),
        )
        .await?;
        link_provider_offer(&db, video_game_source_id, offer_id, Some(0.8)).await?;
        linked_video_game_source_id = Some(video_game_source_id);
        let demo_urls: Vec<String> = vec![
            "https://images.playstation.net/nba2k26/cover.jpg".to_string(),
            "https://videos.playstation.net/nba2k26/trailer.mp4".to_string(),
        ];
        let structured: Vec<(String, Option<String>, Option<String>, Option<String>)> = demo_urls
            .iter()
            .map(|u| {
                let ul = u.to_ascii_lowercase();
                let is_video =
                    ul.ends_with(".mp4") || ul.ends_with(".webm") || ul.ends_with(".m3u8");
                let media_type = if is_video {
                    Some("trailer".to_string())
                } else {
                    Some(
                        "sc                                                                                                                                                                                                                                ````````````````````````````````````reenshot".to_string()
                    )
                };
                let role = None;
                let title = media_type.clone();
                (u.clone(), media_type, role, title)
            })
            .collect();
        inserted_media += ensure_vg_source_media_links_with_meta(
            &db,
            video_game_source_id,
            Some(vg_ps5),
            &structured,
            "psstore",
            Some(json!({"context":"demo-fallback"})),
        )
        .await?;
        // Fallback synthetic aggregate rating + per-locale row
        let _ = sqlx
            ::query(
                "UPDATE public.video_games SET average_rating=$1, rating_count=$2, rating_updated_at=now() WHERE id=$3"
            )
            .persistent(false)
            .bind(4.6f32)
            .bind(3251i64)
            .bind(vg_ps5)
            .execute(&db.pool).await?;
        let _ = sqlx
            ::query(
                "INSERT INTO public.video_game_ratings_by_locale (video_game_id, locale, average_rating, rating_count, rating_updated_at) VALUES ($1,$2,$3,$4, now()) ON CONFLICT (video_game_id, locale) DO UPDATE SET average_rating=EXCLUDED.average_rating, rating_count=EXCLUDED.rating_count, rating_updated_at=now()"
            )
            .persistent(false)
            .bind(vg_ps5)
            .bind("fallback")
            .bind(4.6f32)
            .bind(3251i64)
            .execute(&db.pool).await?;
        // Persist demo videos too
        for vurl in &["https://videos.playstation.net/nba2k26/trailer.mp4"] {
            let _ = ensure_game_video(&db, vg_ps5, video_game_source_id, vurl).await?;
        }
        let now = Utc::now();
        for (oj_id, loc) in offer_juris_to_region.iter() {
            let amt = match loc.as_str() {
                "en-gb" => 5499,
                "de-de" => 5899,
                _ => 5999,
            };
            price_rows.push(PriceRow {
                offer_jurisdiction_id: *oj_id,
                video_game_source_id: linked_video_game_source_id,
                recorded_at: now - chrono::Duration::days(2),
                amount_minor: amt + 1000,
                tax_inclusive: true,
                fx_minor_per_unit: None,
                btc_sats_per_unit: None,
                meta: json!({"src":"psstore","kind":"base","locale":loc}),
            });
            price_rows.push(PriceRow {
                offer_jurisdiction_id: *oj_id,
                video_game_source_id: linked_video_game_source_id,
                recorded_at: now,
                amount_minor: amt,
                tax_inclusive: true,
                fx_minor_per_unit: None,
                btc_sats_per_unit: None,
                meta: json!({"src":"psstore","kind":"discount","locale":loc}),
            });
        }
    }

    // ------------------------------
    // FX enrichment (populate fx_minor_per_unit relative to base currency, default USD)
    // fx_minor_per_unit semantic: number of minor units of the price currency equal to 1 base currency unit.
    // For USD base, EUR minor_unit=2 and rate USD->EUR=0.92 => fx_minor_per_unit = 92.
    // If currency == base, fx_minor_per_unit = 10^minor_unit.
    // ------------------------------
    if !price_rows.is_empty() {
        let base_ccy = env::var("FX_BASE_CURRENCY").unwrap_or_else(|_| "USD".to_string());
        // Collect distinct offer_jurisdiction_ids
        use std::collections::HashSet;
        let mut ojs: HashSet<i64> = HashSet::new();
        for r in &price_rows {
            ojs.insert(r.offer_jurisdiction_id);
        }
        let oj_list: Vec<i64> = ojs.into_iter().collect();
        // Fetch currency code + minor_unit per OJ
        let rows = sqlx::query(
            "SELECT oj.id, c.code, c.minor_unit \
             FROM public.offer_jurisdictions oj \
             JOIN public.currencies c ON c.id=oj.currency_id \
             WHERE oj.id = ANY($1)",
        )
        .bind(&oj_list)
        .fetch_all(&db.pool)
        .await?;
        use std::collections::HashMap;
        let mut oj_currency: HashMap<i64, (String, i16)> = HashMap::new();
        for r in rows {
            let id: i64 = r.get("id");
            let code: String = r.get::<Option<String>, _>("code").unwrap_or_default();
            let mu: i16 = r.get::<Option<i16>, _>("minor_unit").unwrap_or(2);
            oj_currency.insert(id, (code, mu));
        }
        let fx_svc = ExchangeService::new(db.clone());
        // Build rate cache per distinct currency
        // targets set already declared above; HashMap is already in scope
        let mut targets: HashSet<String> = HashSet::new();
        for (_oj, (ccy, _mu)) in oj_currency.iter() {
            targets.insert(ccy.clone());
        }
        let mut rate_cache: HashMap<String, Option<f64>> = HashMap::new();
        for ccy in targets {
            let key = format!("{}->{}", base_ccy, ccy);
            let rate_opt = if base_ccy.eq_ignore_ascii_case(&ccy) {
                Some(1.0)
            } else if let Ok(Some(r)) = fx_svc.latest_rate(&base_ccy, &ccy).await {
                Some(r)
            } else if let Ok(Some(rinv)) = fx_svc.latest_rate(&ccy, &base_ccy).await {
                if rinv > 0.0 {
                    Some(1.0 / rinv)
                } else {
                    None
                }
            } else {
                None
            };
            rate_cache.insert(key, rate_opt);
        }
        for row in &mut price_rows {
            if let Some((ccy, mu)) = oj_currency.get(&row.offer_jurisdiction_id) {
                let key = format!("{}->{}", base_ccy, ccy);
                if let Some(Some(rate)) = rate_cache.get(&key) {
                    let scale = (10i64).pow((*mu).max(0) as u32);
                    let minor_per_base = (*rate * (scale as f64)).round() as i64;
                    if row.fx_minor_per_unit.is_none() {
                        row.fx_minor_per_unit = Some(minor_per_base);
                    }
                }
            }
        }
        println!(
            "[ps_demo_nba] FX enrichment completed base={} applied_rows={}",
            base_ccy,
            price_rows
                .iter()
                .filter(|r| r.fx_minor_per_unit.is_some())
                .count()
        );
    }

    // Persist prices and current_price upserts in batches
    const BATCH: usize = 512;
    let mut i = 0usize;
    let mut total_current = 0usize;
    while i < price_rows.len() {
        let end = (i + BATCH).min(price_rows.len());
        let slice = &price_rows[i..end];
        db.bulk_insert_prices(slice).await?;
        println!("[ps_demo_nba] Inserted prices batch: {} rows", slice.len());
        // collapse to latest per offer_jurisdiction
        use std::collections::HashMap;
        let mut latest: HashMap<i64, &PriceRow> = HashMap::new();
        for r in slice {
            latest
                .entry(r.offer_jurisdiction_id)
                .and_modify(|cur| {
                    if r.recorded_at > cur.recorded_at {
                        *cur = r;
                    }
                })
                .or_insert(r);
        }
        const CP_AGENT: &str = "ps-store";
        const CP_PRIORITY: i16 = 100;
        let updates: Vec<CurrentPriceRow> = latest
            .values()
            .map(|r| CurrentPriceRow {
                offer_jurisdiction_id: r.offer_jurisdiction_id,
                amount_minor: r.amount_minor,
                recorded_at: r.recorded_at,
                agent: CP_AGENT.to_string(),
                agent_priority: CP_PRIORITY,
            })
            .collect();
        db.upsert_current_prices(&updates).await?;
        println!(
            "[ps_demo_nba] Upserted current_price rows: {}",
            updates.len()
        );
        total_current += updates.len();
        i = end;
    }

    // Console verification: product chain, media count, ratings, and per-locale current prices
    let row = sqlx
        ::query(
            "SELECT p.id as product_id, vgt.id as title_id, vg.id as video_game_id FROM public.products p JOIN public.video_game_titles vgt ON vgt.video_game_id=p.id JOIN public.video_games vg ON vg.title_id=vgt.id WHERE p.slug=$1 LIMIT 1"
        )
        .persistent(false)
        .bind(&slug)
        .fetch_one(&db.pool).await?;
    let product_id: i64 = row.get("product_id");
    let title_id: i64 = row.get("title_id");
    let video_game_id: i64 = row.get("video_game_id");

    let media_count: i64 = if let Some(pid) = linked_video_game_source_id {
        sqlx::query_scalar(
            "SELECT COUNT(*) FROM public.provider_media_links WHERE video_game_source_id=$1",
        )
        .bind(pid)
        .persistent(false)
        .fetch_one(&db.pool)
        .await?
    } else {
        0
    };

    let rating_row =
        sqlx::query("SELECT average_rating, rating_count FROM public.video_games WHERE id=$1")
            .bind(video_game_id)
            .persistent(false)
            .fetch_one(&db.pool)
            .await
            .ok();
    let (avg_rating, rating_count): (Option<f32>, Option<i64>) = match rating_row {
        Some(r) => (
            r.try_get("average_rating").ok(),
            r.try_get("rating_count").ok(),
        ),
        None => (None, None),
    };

    println!(
        "NBA 2K26 -> product_id={} title_id={} video_game_id={} provider_media_links={} avg_rating={:?} rating_count={:?}",
        product_id, title_id, video_game_id, media_count, avg_rating, rating_count
    );

    for (oj_id, loc) in offer_juris_to_region.iter() {
        let row = sqlx
            ::query(
                "SELECT amount_minor, recorded_at FROM public.current_price WHERE offer_jurisdiction_id=$1"
            )
            .bind(oj_id)
            .persistent(false)
            .fetch_optional(&db.pool).await?;
        match row {
            Some(r) => {
                let amt: i64 = r.get("amount_minor");
                let ts: chrono::DateTime<chrono::Utc> = r.get("recorded_at");
                println!(
                    "current_price[{loc}] -> amount_minor={} recorded_at={}",
                    amt, ts
                );
            }
            None => println!("current_price[{loc}] -> <none>"),
        }
    }

    println!(
        "Inserted price rows: {} | current_price upserts: {} | media links added: {}",
        price_rows.len(),
        total_current,
        inserted_media
    );
    // Recompute aggregate rating on video_games from per-locale rows (simple average weighted by count)
    let agg = sqlx
        ::query(
            "SELECT \
            COALESCE(SUM(rating_count)::BIGINT, 0) AS total_count, \
            COALESCE(SUM((average_rating::double precision) * (rating_count::double precision)), 0)::double precision AS weighted_sum \
         FROM public.video_game_ratings_by_locale \
         WHERE video_game_id=$1"
        )
        .persistent(false)
        .bind(video_game_id)
        .fetch_one(&db.pool).await?;
    let total_count: i64 = agg.try_get::<i64, _>("total_count").unwrap_or(0);
    let weighted_sum: f64 = agg.try_get::<f64, _>("weighted_sum").unwrap_or(0.0);
    if total_count > 0 {
        let new_avg = (weighted_sum / (total_count as f64)) as f32;
        let _ = sqlx
            ::query(
                "UPDATE public.video_games SET average_rating=$1, rating_count=$2, rating_updated_at=now() WHERE id=$3"
            )
            .persistent(false)
            .bind(new_avg)
            .bind(total_count)
            .bind(video_game_id)
            .execute(&db.pool).await?;
        println!(
            "[ps_demo_nba] Aggregated rating updated: avg={:.3} count={}",
            new_avg, total_count
        );
    } else {
        println!("[ps_demo_nba] No per-locale ratings to aggregate");
    }
    Ok(())
}

fn normalize_title(s: &str) -> String {
    s.to_lowercase()
        .replace(|c: char| !c.is_ascii_alphanumeric(), "-")
        .trim_matches('-')
        .to_string()
}

// Local helpers (non-DB)

// Map ISO 4217 currency minor units (fraction digits). Defaults to 2 when unknown.
fn currency_minor_unit(code: &str) -> i16 {
    match code.to_ascii_uppercase().as_str() {
        // zero-decimal currencies
        "JPY" | "KRW" | "VND" | "CLP" | "ISK" | "HUF" => 0,
        // three-decimal currencies (rare for our locales, but include as examples)
        "BHD" | "IQD" | "KWD" | "JOD" | "OMR" | "TND" => 3,
        // default two-decimal
        _ => 2,
    }
}

// Helper to load PS Store regions from env into a Vec<String>.
// Accepts comma or space separated values; trims and lowercases; provides a wide global default.
fn load_regions() -> Vec<String> {
    let raw = env
        ::var("PS_STORE_REGIONS")
        .unwrap_or_else(|_|
            "en-us en-gb de-de fr-fr es-es pt-br ja-jp ko-kr zh-hk zh-tw it-it nl-nl no-no sv-se fi-fi da-dk pl-pl ru-ru tr-tr en-ca en-au en-nz".into()
        );
    raw.split(|c: char| (c == ',' || c == ' ')) // support both separators
        .filter(|s| !s.is_empty())
        .map(|s| s.trim().to_lowercase())
        .collect()
}

// Insert a game_videos row if not already present for (video_game_id, url).
async fn ensure_game_video(
    db: &Db,
    video_game_id: i64,
    video_game_source_id: i64,
    url: &str,
) -> Result<i64> {
    if let Some(rec) =
        sqlx::query("SELECT id FROM public.game_videos WHERE video_game_id=$1 AND url=$2 LIMIT 1")
            .persistent(false)
            .bind(video_game_id)
            .bind(url)
            .fetch_optional(&db.pool)
            .await?
    {
        return Ok(rec.get("id"));
    }
    let mime = guess_mime(url);
    let rec = sqlx
        ::query(
            "INSERT INTO public.game_videos (video_game_id, video_game_source_id, kind, mime_type, duration_seconds, url) VALUES ($1,$2,$3,$4,$5,$6) RETURNING id"
        )
        .persistent(false)
        .bind(video_game_id)
        .bind(video_game_source_id)
        .bind("trailer")
        .bind(mime)
        .bind(Option::<i32>::None)
        .bind(url)
        .fetch_one(&db.pool).await?;
    Ok(rec.get("id"))
}

fn guess_mime(url: &str) -> &str {
    if let Some(lower) = url.split('?').next().map(|s| s.to_ascii_lowercase()) {
        if lower.ends_with(".mp4") {
            return "video/mp4";
        }
        if lower.ends_with(".m3u8") {
            return "application/vnd.apple.mpegurl";
        }
        if lower.ends_with(".webm") {
            return "video/webm";
        }
        if lower.ends_with(".mov") {
            return "video/quicktime";
        }
    }
    "video/mp4"
}
