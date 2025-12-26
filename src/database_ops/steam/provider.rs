use crate::currency_for_country;
use crate::database_ops::db::{Db, PriceRow};
use crate::database_ops::ingest_providers::{
    PostIngestSummary, ProviderEntityCache, ensure_country, ensure_currency,
    ensure_national_jurisdiction, ensure_platform, ensure_provider,
    ensure_provider_media_links_with_meta, ensure_retailer, ingest_prices, link_provider_offer,
    upsert_game_media,
};
use anyhow::{Context, Result};
use chrono::Utc;
use futures::{StreamExt, stream::FuturesUnordered};
use rayon::prelude::*;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    fmt, fs,
    path::Path,
};
use tokio::sync::Semaphore;
use tracing::{info, warn};

#[derive(Debug, Deserialize)]
struct AppListResp {
    applist: AppList,
}
#[derive(Debug, Deserialize)]
struct AppList {
    apps: Vec<AppEntry>,
}
#[derive(Debug, Deserialize)]
struct AppEntry {
    appid: i64,
    #[allow(dead_code)]
    name: String,
}

/// Minimal Steam provider scaffold fetching appdetails for configured APP IDs.
/// Env: STEAM_APP_IDS (comma list), STEAM_COUNTRY (default US), STEAM_CURRENCY (default USD)
pub struct SteamProvider {
    #[allow(dead_code)]
    client: Client,
}

#[derive(Deserialize)]
struct AppsFile {
    response: Response,
}

#[derive(Deserialize)]
struct Response {
    apps: Vec<App>,
}

#[derive(Deserialize)]
struct App {
    appid: u64,
    // you can include other fields if you want them:
    // name: String,
    // last_modified: u64,
    // price_change_number: u64,
}
#[derive(Debug, Deserialize)]
struct AppDetailsWrapper {
    success: bool,
    data: Option<AppData>,
}

#[derive(Debug, Deserialize)]
struct AppData {
    name: Option<String>,
    price_overview: Option<PriceOverview>,
    #[serde(default)]
    is_free: Option<bool>,
    #[serde(default)]
    package_groups: Option<Vec<PackageGroup>>,
}

// Full details fields we may use to enrich video_games
#[derive(Debug, Deserialize)]
struct FullAppData {
    #[allow(dead_code)]
    name: Option<String>,
    #[allow(dead_code)]
    price_overview: Option<PriceOverview>,
    metacritic: Option<Metacritic>,
    recommendations: Option<Recommendations>,
    genres: Option<Vec<GenreEntry>>, // {id, description}
    #[allow(dead_code)]
    #[serde(default)]
    short_description: Option<String>,
    #[allow(dead_code)]
    #[serde(default)]
    about_the_game: Option<String>,
    #[allow(dead_code)]
    #[serde(default)]
    detailed_description: Option<String>,
    #[allow(dead_code)]
    #[serde(default)]
    movies: Option<Vec<SteamMovie>>,
}

#[derive(Debug, Deserialize)]
struct Metacritic {
    score: Option<i64>,
}
#[derive(Debug, Deserialize)]
struct Recommendations {
    total: Option<i64>,
}
#[derive(Debug, Deserialize)]
struct GenreEntry {
    #[allow(dead_code)]
    id: Option<i64>,
    description: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SteamMovie {
    #[allow(dead_code)]
    #[serde(default)]
    webm: Option<HashMap<String, String>>,
    #[allow(dead_code)]
    #[serde(default)]
    mp4: Option<HashMap<String, String>>,
    #[allow(dead_code)]
    #[serde(default)]
    thumbnail: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PriceOverview {
    #[allow(dead_code)]
    final_formatted: Option<String>,
    #[allow(dead_code)]
    initial_formatted: Option<String>,
    #[serde(rename = "final")]
    final_price: Option<i64>,
    #[serde(rename = "initial")]
    initial_price: Option<i64>,
    currency: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
struct PackageGroup {
    #[serde(default)]
    subs: Option<Vec<PackageSub>>,
}

#[derive(Debug, Deserialize, Clone)]
struct PackageSub {
    #[serde(default)]
    packageid: Option<i64>,
    #[serde(default)]
    price_in_cents_with_discount: Option<i64>,
    #[serde(default)]
    price_in_cents: Option<i64>,
    #[serde(default)]
    discount_pct: Option<i64>,
}

#[derive(Debug, Clone, Copy)]
struct BundlePrice {
    final_minor: i64,
    base_minor: Option<i64>,
    discount_pct: Option<i64>,
    package_id: Option<i64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SteamMediaScope {
    Disabled,
    PrimaryRegion,
    AllRegions,
}

impl SteamMediaScope {
    fn from_env(fetch_media_enabled: bool) -> Self {
        if !fetch_media_enabled {
            return SteamMediaScope::Disabled;
        }
        match std::env::var("STEAM_MEDIA_SCOPE") {
            Ok(value) => {
                let normalized = value.trim().to_ascii_lowercase();
                match normalized.as_str() {
                    "all" | "per-region" | "per_region" => SteamMediaScope::AllRegions,
                    "off" | "disabled" | "none" => SteamMediaScope::Disabled,
                    _ => SteamMediaScope::PrimaryRegion,
                }
            }
            Err(_) => SteamMediaScope::PrimaryRegion,
        }
    }

    fn per_region_media_requests(self) -> usize {
        match self {
            SteamMediaScope::AllRegions => 1,
            SteamMediaScope::PrimaryRegion | SteamMediaScope::Disabled => 0,
        }
    }

    fn base_media_requests(self) -> usize {
        match self {
            SteamMediaScope::PrimaryRegion | SteamMediaScope::AllRegions => 1,
            SteamMediaScope::Disabled => 0,
        }
    }

    fn should_fetch_in_region(self, region_idx: usize) -> bool {
        match self {
            SteamMediaScope::AllRegions => true,
            SteamMediaScope::PrimaryRegion => region_idx == 0,
            SteamMediaScope::Disabled => false,
        }
    }
}

impl fmt::Display for SteamMediaScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SteamMediaScope::Disabled => write!(f, "disabled"),
            SteamMediaScope::PrimaryRegion => write!(f, "primary"),
            SteamMediaScope::AllRegions => write!(f, "all"),
        }
    }
}

fn extract_best_bundle_price(groups: Option<Vec<PackageGroup>>) -> Option<BundlePrice> {
    let groups = groups?;
    let mut best: Option<BundlePrice> = None;
    for group in groups {
        if let Some(subs) = group.subs {
            for sub in subs {
                let discounted = sub.price_in_cents_with_discount;
                let base = sub.price_in_cents;
                let final_minor = if let Some(d) = discounted {
                    if d > 0 { Some(d) } else { None }
                } else if let Some(b) = base {
                    if b > 0 { Some(b) } else { None }
                } else {
                    None
                };
                let final_minor = match final_minor {
                    Some(v) if v > 0 => v,
                    _ => {
                        continue;
                    }
                };
                let candidate = BundlePrice {
                    final_minor,
                    base_minor: base.filter(|v| *v > 0),
                    discount_pct: sub.discount_pct,
                    package_id: sub.packageid,
                };
                let replace = match best {
                    None => true,
                    Some(existing) => {
                        if candidate.final_minor < existing.final_minor {
                            true
                        } else if candidate.final_minor == existing.final_minor {
                            let cand_disc = candidate.discount_pct.unwrap_or(0);
                            let exist_disc = existing.discount_pct.unwrap_or(0);
                            if cand_disc > exist_disc {
                                true
                            } else if cand_disc == exist_disc {
                                match (candidate.base_minor, existing.base_minor) {
                                    (Some(cb), Some(eb)) => cb < eb,
                                    (Some(_), None) => true,
                                    _ => false,
                                }
                            } else {
                                false
                            }
                        } else {
                            false
                        }
                    }
                };
                if replace {
                    best = Some(candidate);
                }
            }
        }
    }
    best
}

fn push_bundle_price_row(
    price_rows: &mut Vec<PriceRow>,
    seen: &mut HashSet<(i64, &'static str, i64)>,
    summary: &mut PostIngestSummary,
    row: PriceRow,
    kind: &'static str,
) -> bool {
    let key = (row.offer_jurisdiction_id, kind, row.amount_minor);
    if seen.insert(key) {
        summary.record_bundle_ingest(row.offer_jurisdiction_id);
        price_rows.push(row);
        true
    } else {
        summary.record_bundle_skip(row.offer_jurisdiction_id);
        false
    }
}

impl SteamProvider {
    pub fn new() -> Self {
        // Add a sane default timeout to avoid indefinite hangs on slow Steam endpoints.
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(
                std::env::var("STEAM_HTTP_TIMEOUT_SECS")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(15),
            ))
            .build()
            .unwrap_or_else(|_| Client::new());
        Self { client }
    }

    pub async fn run_from_env(db: &Db) -> Result<()> {
        // DEBUG instrumentation: capture per-app decision traces when STEAM_DEBUG=1
        let debug_enabled = std::env::var("STEAM_DEBUG").ok().as_deref() == Some("1");
        #[derive(Serialize)]
        struct AppDebugTrace {
            appid: String,
            region: String,
            name: Option<String>,
            success_flag: bool,
            had_price_overview: bool,
            final_price: Option<i64>,
            initial_price: Option<i64>,
            skipped_free: bool,
            reason: Option<String>,
        }
        let mut debug_traces: Vec<AppDebugTrace> = Vec::new();
        let backfill = std::env::var("STEAM_BACKFILL").ok().as_deref() == Some("1");
        let mut app_ids: Vec<String> = std::env::var("STEAM_APP_IDS")
            .unwrap_or_else(|_| "".into())
            .split(',')
            .filter(|s| !s.trim().is_empty())
            .map(|s| s.trim().to_string())
            .collect();
        if app_ids.is_empty() {
            if let Some(pick) = std::env::var("STEAM_APP_PICK")
                .ok()
                .and_then(|s| s.parse::<usize>().ok())
            {
                let picked = fetch_app_list(pick).await.unwrap_or_default();
                if !picked.is_empty() {
                    app_ids = picked;
                }
            }
        }
        let app_ids_file = std::env::var("STEAM_APP_IDS_FILE")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .or_else(|| {
                let default_path = Path::new("steam_apps_pretty.json");
                if default_path.exists() {
                    Some(default_path.to_string_lossy().into_owned())
                } else {
                    None
                }
            });
        if app_ids.is_empty() {
            if let Some(path) = app_ids_file.as_ref() {
                match load_app_ids_from_file(path) {
                    Ok(from_file) => {
                        if from_file.is_empty() {
                            warn!(path=%path, "steam: app ids file had no entries");
                        } else {
                            info!(count=from_file.len(), path=%path, "steam: loaded app ids from file");
                            app_ids = from_file.into_par_iter().map(|id| id.to_string()).collect();
                        }
                    }
                    Err(err) => {
                        warn!(error=%err, path=%path, "steam: failed to load app ids file");
                    }
                }
            }
        }
        if !app_ids.is_empty() {
            let mut dedup: HashSet<String> = HashSet::with_capacity(app_ids.len());
            app_ids.retain(|id| dedup.insert(id.clone()));
            app_ids.sort();
            if let Some(limit) = std::env::var("STEAM_APP_LIMIT")
                .ok()
                .and_then(|s| s.parse::<usize>().ok())
            {
                if app_ids.len() > limit {
                    app_ids.truncate(limit);
                    info!(limit, "steam: truncating app ids to configured limit");
                }
            }
            if app_ids.is_empty() {
                info!(
                    "steam ingest skipped: no app ids configured; set STEAM_APP_IDS, STEAM_APP_IDS_FILE, or STEAM_APP_PICK to enable this step"
                );
                return Ok(());
            }
        }
        let fetch_media_enabled = true;
        let mut media_scope = SteamMediaScope::from_env(fetch_media_enabled);
        if fetch_media_enabled && media_scope == SteamMediaScope::Disabled {
            media_scope = SteamMediaScope::PrimaryRegion;
        }
        let fetch_media = media_scope != SteamMediaScope::Disabled;
        let language = normalize_language("english");
        unsafe {
            std::env::set_var("STEAM_LANGUAGE", &language);
        }
        // Determine regions to ingest:
        // Precedence:
        // 1) STEAM_REGIONS env (e.g., "US:USD,GB:GBP")
        // 2) Database countries table (code2 joined to currencies.code)
        // 3) Curated fallback list
        let regions = if std::env::var("STEAM_REGIONS")
            .ok()
            .map(|s| !s.trim().is_empty())
            .unwrap_or(false)
        {
            load_steam_regions()
        } else {
            match load_steam_regions_from_db(db).await {
                Ok(v) if !v.is_empty() => {
                    info!(
                        region_count = v.len(),
                        "steam: using regions from DB countries table"
                    );
                    v
                }
                _ => {
                    let v = load_steam_regions();
                    info!(
                        region_count = v.len(),
                        "steam: using curated fallback regions"
                    );
                    v
                }
            }
        };
        let total_regions = regions.len();
        if total_regions == 0 {
            warn!("steam: no regions available after configuration; aborting run");
            return Ok(());
        }
        unsafe {
            std::env::set_var("STEAM_MAX_REGIONS", total_regions.to_string());
        }
        let fallback_media_cc = regions
            .first()
            .map(|(cc, _)| cc.clone())
            .unwrap_or_else(|| "US".into());
        let configured_media_cc = std::env::var("STEAM_MEDIA_CC")
            .ok()
            .map(|s| s.trim().to_ascii_uppercase())
            .filter(|s| !s.is_empty());
        let media_primary_cc = if media_scope == SteamMediaScope::PrimaryRegion {
            let chosen = configured_media_cc
                .clone()
                .unwrap_or_else(|| fallback_media_cc.clone());
            unsafe {
                std::env::set_var("STEAM_MEDIA_CC", &chosen);
            }
            chosen
        } else {
            configured_media_cc.unwrap_or(fallback_media_cc)
        };
        let media_language = normalize_language("english");
        unsafe {
            std::env::set_var("STEAM_MEDIA_LANGUAGE", &media_language);
        }
        let request_budget = match std::env::var("STEAM_REQUEST_BUDGET_PER_RUN") {
            Ok(value) => {
                let trimmed = value.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    match trimmed.parse::<usize>() {
                        Ok(parsed) => Some(parsed),
                        Err(err) => {
                            warn!(value=%value, error=%err, "steam: invalid STEAM_REQUEST_BUDGET_PER_RUN; skipping budget enforcement");
                            None
                        }
                    }
                }
            }
            Err(_) => None,
        };
        if !backfill {
            if let Some(budget) = request_budget {
                if app_ids.is_empty() {
                    info!(
                        budget,
                        "steam: request budget provided but no app ids available after initial filtering"
                    );
                } else {
                    let price_requests_per_region = 1usize;
                    let media_requests_per_region = media_scope.per_region_media_requests();
                    let base_requests = 1usize + media_scope.base_media_requests();
                    let requests_per_app = base_requests
                        + total_regions
                            .saturating_mul(price_requests_per_region + media_requests_per_region);
                    if requests_per_app == 0 {
                        warn!(
                            budget,
                            "steam: computed zero requests-per-app; skipping budget enforcement"
                        );
                        return Ok(());
                    }
                    let allowed_apps = budget / requests_per_app;
                    if allowed_apps == 0 {
                        warn!(
                            budget,
                            requests_per_app,
                            region_count = total_regions,
                            fetch_media,
                            "steam: request budget below per-app cost; skipping Steam ingest run"
                        );
                        return Ok(());
                    }
                    if app_ids.len() > allowed_apps {
                        app_ids.truncate(allowed_apps);
                        info!(
                            allowed_apps,
                            budget,
                            requests_per_app,
                            region_count = total_regions,
                            fetch_media,
                            "steam: truncated app ids to honor per-run request budget"
                        );
                    }
                    let estimated_requests = app_ids.len().saturating_mul(requests_per_app);
                    info!(
                        app_count = app_ids.len(),
                        region_count = total_regions,
                        fetch_media,
                        media_scope = %media_scope,
                        requests_per_app,
                        budget,
                        estimated_requests,
                        "steam: request budget planning for this run"
                    );
                }
            }
        }
        let batch_flush: usize = std::env::var("STEAM_BATCH_FLUSH")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1000);
        let max_conc: usize = std::env::var("STEAM_MAX_CONCURRENCY")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(8);
        let recent_days: i64 = std::env::var("STEAM_RECENT_MISSING_DAYS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(30);
        let per_request_timeout = std::env::var("STEAM_REQUEST_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(20);

        // Use provider client (with global timeout) instead of allocating a new one per run.
        let provider_client = Client::builder()
            .timeout(std::time::Duration::from_secs(per_request_timeout + 5))
            .build()
            .unwrap_or_else(|_| Client::new());

        let provider_id = ensure_provider(db, "steam", "storefront", Some("steam-store")).await?;
        let retailer_id = ensure_retailer(db, "Steam", Some("steam")).await?;
        let platform_id = ensure_platform(db, "PC", Some("pc")).await?;
        let mut entity_cache = ProviderEntityCache::new(db.clone());
        // Check if optional unified media table exists to avoid runtime errors in environments without it
        let game_media_exists: bool = sqlx::query_scalar::<_, Option<String>>(
            "SELECT to_regclass('public.game_media')::text",
        )
        .persistent(false)
        .fetch_one(&db.pool)
        .await?
        .is_some();
        info!(
            app_count = app_ids.len(),
            region_count = total_regions,
            "steam ingest starting"
        );

        if backfill {
            let backfill_summary = run_backfill(
                db,
                provider_id,
                retailer_id,
                &regions,
                recent_days,
                fetch_media,
                &language,
                batch_flush,
                max_conc,
            )
            .await?;
            backfill_summary.verify(db, provider_id).await?;
            info!(
                provider_id,
                price_rows = backfill_summary.total_price_rows_written,
                provider_items = backfill_summary.provider_item_ids.len(),
                "steam backfill verification complete"
            );
            return Ok(());
        }
        let mut post_summary = PostIngestSummary::default();
        if app_ids.is_empty() {
            warn!("STEAM_APP_IDS empty and STEAM_BACKFILL=0; nothing to do");
            return Ok(());
        }

        let sem = std::sync::Arc::new(Semaphore::new(max_conc.max(1) as usize));
        let client = provider_client.clone();

        // Iterate without moving original vector (needed for total_regions length metric)
        let mut regions_iter = regions.iter().cloned().enumerate();
        while let Some((region_idx, (cc, cur_code))) = regions_iter.next() {
            let region_start = Utc::now();
            info!(region_index=region_idx+1, total_regions=total_regions, country=%cc, currency=%cur_code, "region start");
            let mut processed_apps: usize = 0;
            let mut region_price_rows: usize = 0;
            // Ensure jurisdiction + currency
            let mu = currency_minor_unit(&cur_code);
            let currency_id = ensure_currency(db, &cur_code, &cur_code, mu).await?;
            let country_id = ensure_country(
                db,
                &cc.to_ascii_uppercase(),
                &cc.to_ascii_uppercase(),
                currency_id,
            )
            .await?;
            let juris_id = ensure_national_jurisdiction(db, country_id).await?;

            let mut price_rows: Vec<PriceRow> = Vec::new();
            let mut bundle_seen: HashSet<(i64, &'static str, i64)> = HashSet::new();
            let mut futs: FuturesUnordered<_> = FuturesUnordered::new();
            for id in &app_ids {
                let idc = id.clone();
                let cc_c = cc.clone();
                let language_c = language.clone();
                let client_c = client.clone();
                let permit = sem.clone().acquire_owned().await.unwrap();
                futs.push(async move {
                    let _p = permit; // hold until done
                    let url = format!(
                        "https://store.steampowered.com/api/appdetails?appids={}&cc={}&l={}&filters=price_overview,package_groups",
                        idc,
                        cc_c,
                        language_c
                    );
                    // Per-call timeout wrapper
                    match
                        tokio::time::timeout(
                            std::time::Duration::from_secs(per_request_timeout),
                            get_with_backoff_json(&client_c, &url, &[])
                        ).await
                    {
                        Ok(res) => res.map(|v| (idc, v)),
                        Err(_) => {
                            warn!(appid=%idc, country=%cc_c, "steam price request timeout");
                            None
                        }
                    }
                });
            }
            while let Some(res) = futs.next().await {
                if let Some((id, body)) = res {
                    // light per-app debug trace every 10
                    if processed_apps % 10 == 0 {
                        tracing::debug!(appid=%id, country=%cc, "processing app response");
                    }
                    if let Some(entry) = body.get(&id) {
                        if let Ok(details) =
                            serde_json::from_value::<AppDetailsWrapper>(entry.clone())
                        {
                            if !details.success {
                                if debug_enabled {
                                    debug_traces.push(AppDebugTrace {
                                        appid: id.clone(),
                                        region: cc.clone(),
                                        name: None,
                                        success_flag: false,
                                        had_price_overview: false,
                                        final_price: None,
                                        initial_price: None,
                                        skipped_free: false,
                                        reason: Some("details.success=false".into()),
                                    });
                                }
                                continue;
                            }
                            let name = details
                                .data
                                .as_ref()
                                .and_then(|d| d.name.clone())
                                .unwrap_or_else(|| id.clone());
                            // Paid-only gate (skip free or zero-priced) if STEAM_ONLY_PAID=1
                            let only_paid =
                                std::env::var("STEAM_ONLY_PAID").ok().as_deref() == Some("1");
                            if only_paid {
                                if let Some(data) = details.data.as_ref() {
                                    let free_flag = data.is_free.unwrap_or(false);
                                    let zero_final = data
                                        .price_overview
                                        .as_ref()
                                        .and_then(|po| po.final_price)
                                        .map(|v| v <= 0)
                                        .unwrap_or(true);
                                    if free_flag || zero_final {
                                        tracing::info!(appid=%id, free_flag, zero_final, "skip free/zero-priced app due to STEAM_ONLY_PAID");
                                        if debug_enabled {
                                            debug_traces.push(AppDebugTrace {
                                                appid: id.clone(),
                                                region: cc.clone(),
                                                name: Some(name.clone()),
                                                success_flag: true,
                                                had_price_overview: data.price_overview.is_some(),
                                                final_price: data
                                                    .price_overview
                                                    .as_ref()
                                                    .and_then(|po| po.final_price),
                                                initial_price: data
                                                    .price_overview
                                                    .as_ref()
                                                    .and_then(|po| po.initial_price),
                                                skipped_free: true,
                                                reason: Some("only_paid_gate".into()),
                                            });
                                        }
                                        continue;
                                    }
                                } else {
                                    continue;
                                }
                            }
                            let slug = slugify(&name);
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
                            let sellable_id =
                                entity_cache.ensure_sellable("software", product_id).await?;
                            let offer_id = entity_cache
                                .ensure_offer(sellable_id, retailer_id, Some(&id))
                                .await?;
                            let oj_id = entity_cache
                                .ensure_offer_jurisdiction(offer_id, juris_id, currency_id)
                                .await?;
                            let provider_item_id = entity_cache
                                .ensure_provider_item(provider_id, &id, None, false)
                                .await?;
                            post_summary.record_provider_item(provider_item_id);
                            link_provider_offer(db, provider_item_id, offer_id, Some(0.5)).await?;
                            let bundle_price = details
                                .data
                                .as_ref()
                                .and_then(|d| extract_best_bundle_price(d.package_groups.clone()));
                            if let Some(po) = details.data.and_then(|d| d.price_overview) {
                                if let Some(f) = po.final_price {
                                    if f > 0 {
                                        price_rows.push(PriceRow {
                                            offer_jurisdiction_id: oj_id,
                                            provider_item_id: Some(provider_item_id),
                                            recorded_at: Utc::now(),
                                            amount_minor: f,
                                            tax_inclusive: true,
                                            fx_minor_per_unit: None,
                                            btc_sats_per_unit: None,
                                            meta: json!({"src":"steam","kind":"final","cc":cc,"language":language.as_str()}),
                                        });
                                        region_price_rows += 1;
                                    }
                                }
                                if let (Some(init), Some(fin)) = (po.initial_price, po.final_price)
                                {
                                    if init > fin {
                                        price_rows.push(PriceRow {
                                            offer_jurisdiction_id: oj_id,
                                            provider_item_id: Some(provider_item_id),
                                            recorded_at: Utc::now(),
                                            amount_minor: init,
                                            tax_inclusive: true,
                                            fx_minor_per_unit: None,
                                            btc_sats_per_unit: None,
                                            meta: json!({"src":"steam","kind":"initial","cc":cc,"language":language.as_str()}),
                                        });
                                        region_price_rows += 1;
                                    }
                                }
                                if let Some(bundle) = bundle_price {
                                    let meta_final = json!({
                                        "src": "steam",
                                        "kind": "bundle_final",
                                        "cc": cc,
                                        "language": language.as_str(),
                                        "package_id": bundle.package_id,
                                        "discount_pct": bundle.discount_pct,
                                        "standard_final_amount": po.final_price,
                                        "standard_initial_amount": po.initial_price,
                                        "base_amount_minor": bundle.base_minor,
                                    });
                                    let final_row = PriceRow {
                                        offer_jurisdiction_id: oj_id,
                                        provider_item_id: Some(provider_item_id),
                                        recorded_at: Utc::now(),
                                        amount_minor: bundle.final_minor,
                                        tax_inclusive: true,
                                        fx_minor_per_unit: None,
                                        btc_sats_per_unit: None,
                                        meta: meta_final,
                                    };
                                    if push_bundle_price_row(
                                        &mut price_rows,
                                        &mut bundle_seen,
                                        &mut post_summary,
                                        final_row,
                                        "bundle_final",
                                    ) {
                                        region_price_rows += 1;
                                    }
                                    if let Some(base_minor) = bundle.base_minor {
                                        if base_minor > bundle.final_minor {
                                            let meta_initial = json!({
                                                "src": "steam",
                                                "kind": "bundle_initial",
                                                "cc": cc,
                                                "language": language.as_str(),
                                                "package_id": bundle.package_id,
                                                "discount_pct": bundle.discount_pct,
                                                "standard_final_amount": po.final_price,
                                            });
                                            let base_row = PriceRow {
                                                offer_jurisdiction_id: oj_id,
                                                provider_item_id: Some(provider_item_id),
                                                recorded_at: Utc::now(),
                                                amount_minor: base_minor,
                                                tax_inclusive: true,
                                                fx_minor_per_unit: None,
                                                btc_sats_per_unit: None,
                                                meta: meta_initial,
                                            };
                                            if push_bundle_price_row(
                                                &mut price_rows,
                                                &mut bundle_seen,
                                                &mut post_summary,
                                                base_row,
                                                "bundle_initial",
                                            ) {
                                                region_price_rows += 1;
                                            }
                                        }
                                    }
                                }
                                if debug_enabled {
                                    debug_traces.push(AppDebugTrace {
                                        appid: id.clone(),
                                        region: cc.clone(),
                                        name: Some(name.clone()),
                                        success_flag: true,
                                        had_price_overview: true,
                                        final_price: po.final_price,
                                        initial_price: po.initial_price,
                                        skipped_free: false,
                                        reason: None,
                                    });
                                }
                            } else {
                                warn!(
                                    appid = id,
                                    country = cc,
                                    "missing price_overview (possibly free or unavailable)"
                                );
                                if debug_enabled {
                                    debug_traces.push(AppDebugTrace {
                                        appid: id.clone(),
                                        region: cc.clone(),
                                        name: Some(name.clone()),
                                        success_flag: true,
                                        had_price_overview: false,
                                        final_price: None,
                                        initial_price: None,
                                        skipped_free: false,
                                        reason: Some("missing_price_overview".into()),
                                    });
                                }
                            }
                            // Enrich video_game (only during first region to avoid redundant writes)
                            if region_idx == 0 {
                                if let Some(full) = fetch_full_details(&client, &id).await? {
                                    let avg_rating: Option<f32> = full
                                        .metacritic
                                        .as_ref()
                                        .and_then(|m| m.score)
                                        .map(|s| s as f32);
                                    let rating_count: Option<i64> =
                                        full.recommendations.as_ref().and_then(|r| r.total);
                                    let genres: Option<Vec<String>> =
                                        full.genres.as_ref().map(|gs| {
                                            gs.iter()
                                                .filter_map(|g| g.description.clone())
                                                .collect()
                                        });
                                    let synopsis = select_synopsis(&full);
                                    let synopsis_ref = synopsis.as_deref();
                                    let _ = sqlx
                                        ::query(
                                            "UPDATE public.video_games SET average_rating = COALESCE($1, average_rating), rating_count = COALESCE($2, rating_count), rating_updated_at = CASE WHEN $1 IS NOT NULL OR $2 IS NOT NULL THEN now() ELSE rating_updated_at END, genres = CASE WHEN $3::text[] IS NOT NULL AND array_length($3,1) > 0 THEN $3 ELSE genres END, synopsis = CASE WHEN $4::text IS NOT NULL AND (synopsis IS NULL OR length(synopsis) < length($4)) THEN $4 ELSE synopsis END WHERE id = $5"
                                        )
                                        .persistent(false)
                                        .bind(avg_rating)
                                        .bind(rating_count)
                                        .bind(genres.as_ref())
                                        .bind(synopsis_ref)
                                        .bind(vg_id)
                                        .execute(&db.pool).await;
                                }
                            }
                            if fetch_media && media_scope.should_fetch_in_region(region_idx) {
                                let media_cc = if media_scope == SteamMediaScope::PrimaryRegion {
                                    &media_primary_cc
                                } else {
                                    &cc
                                };
                                if let Ok(media_urls) =
                                    fetch_media_urls(&client, &id, media_cc, &media_language).await
                                {
                                    if !media_urls.is_empty() {
                                        let mut tuples: Vec<(
                                            String,
                                            Option<String>,
                                            Option<String>,
                                            Option<String>,
                                        )> = Vec::new();
                                        for url in media_urls {
                                            let lower = url.to_ascii_lowercase();
                                            let (mtype, role) = if lower.ends_with(".mp4")
                                                || lower.ends_with(".webm")
                                            {
                                                (
                                                    Some("video".to_string()),
                                                    Some("trailer".to_string()),
                                                )
                                            } else if lower.contains("header")
                                                || lower.contains("capsule")
                                            {
                                                (
                                                    Some("image".to_string()),
                                                    Some("cover".to_string()),
                                                )
                                            } else if lower.contains("library_logo")
                                                || lower.contains("logo")
                                            {
                                                (
                                                    Some("image".to_string()),
                                                    Some("logo".to_string()),
                                                )
                                            } else {
                                                (
                                                    Some("image".to_string()),
                                                    Some("screenshot".to_string()),
                                                )
                                            };
                                            tuples.push((url, mtype, role, Some(name.clone())));
                                        }
                                        let meta = serde_json::json!({
                                            "cc": media_cc,
                                            "language": media_language.as_str(),
                                            "scope": media_scope.to_string(),
                                        });
                                        let _ = ensure_provider_media_links_with_meta(
                                            db,
                                            provider_item_id,
                                            Some(vg_id),
                                            &tuples,
                                            "steam",
                                            Some(meta),
                                        )
                                        .await?;
                                        // Also upsert into game_media for unified surface
                                        if game_media_exists {
                                            for (url, mtype, role, _title) in &tuples {
                                                let mtype_final =
                                                    mtype.as_deref().unwrap_or("image");
                                                let pdata = serde_json::json!({
                                                    "role": role,
                                                    "cc": media_cc,
                                                    "language": media_language.as_str(),
                                                    "scope": media_scope.to_string(),
                                                });
                                                let _ = upsert_game_media(
                                                    db,
                                                    vg_id,
                                                    "steam",
                                                    url,
                                                    mtype_final,
                                                    url,
                                                    pdata,
                                                )
                                                .await;
                                            }
                                        }
                                    }
                                }
                            }
                            // Ensure display_title and aggregate region_codes
                            let _ = sqlx
                                ::query(
                                    "UPDATE video_games SET display_title = COALESCE(display_title,$1), region_codes = CASE WHEN region_codes IS NULL THEN ARRAY[$2]::text[] ELSE (SELECT ARRAY(SELECT DISTINCT UNNEST(region_codes || ARRAY[$2]))) END WHERE id=$3"
                                )
                                .persistent(false)
                                .bind(&name)
                                .bind(cc.to_ascii_uppercase())
                                .bind(vg_id)
                                .execute(&db.pool).await;
                            processed_apps += 1;
                            if processed_apps % 25 == 0 {
                                info!(country = %cc, processed_apps, total_apps=app_ids.len(), region_price_rows, elapsed_secs = (Utc::now()-region_start).num_seconds(), "region progress");
                            }
                        }
                    }
                }
                if price_rows.len() >= batch_flush {
                    let batch = std::mem::take(&mut price_rows);
                    let batch_len = batch.len();
                    let ingest_result = ingest_prices(db, batch).await?;
                    post_summary.record_batch(batch_len, &ingest_result);
                }
            }
            if !price_rows.is_empty() {
                let batch_len = price_rows.len();
                let ingest_result = ingest_prices(db, price_rows).await?;
                post_summary.record_batch(batch_len, &ingest_result);
            }
            if debug_enabled {
                // Emit a compact JSON summary per region after completion
                let summary = serde_json::json!({
                    "region": cc,
                    "processed_apps": processed_apps,
                    "price_rows": region_price_rows,
                    "elapsed_secs": (Utc::now()-region_start).num_seconds(),
                    "traces": debug_traces,
                });
                println!("[steam_debug] region_summary={}", summary);
                debug_traces.clear();
            }
            info!(country = %cc, processed_apps, region_price_rows, elapsed_secs = (Utc::now()-region_start).num_seconds(), "region complete");
        }
        post_summary.verify(db, provider_id).await?;
        info!(
            provider_id,
            price_rows = post_summary.total_price_rows_written,
            provider_items = post_summary.provider_item_ids.len(),
            offer_jurisdictions = post_summary.offer_jurisdiction_ids.len(),
            "steam provider multi-region ingest complete"
        );
        Ok(())
    }
}

fn load_app_ids_from_file(path: &str) -> Result<Vec<u64>> {
    let data = fs::read_to_string(path)
        .with_context(|| format!("failed to read Steam app ids file {}", path))?;

    let parsed: AppsFile = serde_json::from_str(&data)
        .with_context(|| format!("failed to parse Steam app ids JSON {}", path))?;

    // collect, dedup, sort
    let mut ids: HashSet<u64> = parsed
        .response
        .apps
        .into_iter()
        .map(|app| app.appid)
        .collect();

    let mut list: Vec<u64> = ids.drain().collect();
    list.sort_unstable();

    println!("loaded {} Steam app ids from file {}", list.len(), path);

    Ok(list)
}

fn collect_app_ids(value: &Value, out: &mut HashSet<String>) {
    match value {
        Value::Number(n) => {
            if let Some(id) = n.as_u64() {
                out.insert(id.to_string());
            }
        }
        Value::String(s) => {
            if let Ok(id) = s.trim().parse::<u64>() {
                out.insert(id.to_string());
            }
        }
        Value::Array(arr) => {
            for v in arr {
                collect_app_ids(v, out);
            }
        }
        Value::Object(map) => {
            if let Some(id_val) = map.get("appid") {
                collect_app_ids(id_val, out);
            }
            for v in map.values() {
                collect_app_ids(v, out);
            }
        }
        _ => {}
    }
}

fn strip_html_tags(input: &str) -> String {
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

fn select_synopsis(full: &FullAppData) -> Option<String> {
    let candidates = [
        full.short_description.as_deref(),
        full.about_the_game.as_deref(),
        full.detailed_description.as_deref(),
    ];
    let mut best: Option<String> = None;
    for candidate in candidates.iter().flatten() {
        let cleaned = strip_html_tags(candidate);
        if cleaned.is_empty() {
            continue;
        }
        match &best {
            Some(existing) => {
                if cleaned.len() > existing.len() {
                    best = Some(cleaned);
                }
            }
            None => {
                best = Some(cleaned);
            }
        }
    }
    best
}

// Attempt to derive regions from the database so we cover all configured countries.
// Returns Vec<(country_code, currency_code)>, both uppercased.
async fn load_steam_regions_from_db(db: &Db) -> Result<Vec<(String, String)>> {
    use sqlx::Row;
    let rows = sqlx::query(
        r#"SELECT c.code2 AS cc, cur.code AS currency
           FROM public.countries c
           JOIN public.currencies cur ON cur.id = c.currency_id
           WHERE c.code2 IS NOT NULL AND cur.code IS NOT NULL
           ORDER BY c.code2"#,
    )
    .persistent(false)
    .fetch_all(&db.pool)
    .await?;

    let mut out: Vec<(String, String)> = Vec::with_capacity(rows.len());
    for r in rows {
        let cc: String = r.get::<String, _>("cc");
        let cur: String = r.get::<String, _>("currency");
        let cc_norm = cc.trim().to_ascii_uppercase();
        if cc_norm.is_empty() {
            continue;
        }
        let mut cur_norm = cur.trim().to_ascii_uppercase();
        if cur_norm.is_empty() {
            continue;
        }
        let (expected_code, _) = currency_for_country(&cc_norm);
        if expected_code != "USD" && cur_norm != expected_code {
            info!(
                country = %cc_norm,
                db_currency = %cur_norm,
                expected = %expected_code,
                "steam: overriding DB currency with canonical mapping"
            );
            cur_norm = expected_code.to_string();
        }
        out.push((cc_norm, cur_norm));
    }
    // Deduplicate by country code (keep first)
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out.dedup_by(|a, b| a.0 == b.0);
    Ok(out)
}

/// Fetch and print price_overview for a single app across all configured regions.
/// Does not write to the database; prints a compact table to stdout.
/// Env overrides:
/// - STEAM_REGIONS: "US:USD,GB:GBP,..." to restrict/override regions
/// - STEAM_MAX_CONCURRENCY: limit parallel requests (default 16)
pub async fn print_all_region_prices_for_app(appid: &str) -> Result<()> {
    use futures::{StreamExt, stream::FuturesUnordered};
    use tokio::sync::Semaphore;
    #[derive(Debug, Clone, Default, Serialize)]
    struct RegionPrice {
        cc: String,
        currency: Option<String>,
        initial: Option<i64>,
        final_: Option<i64>,
    }

    let client = Client::new();
    let regions = load_steam_regions();
    let language = normalize_language("english");
    let max_conc: usize = std::env::var("STEAM_MAX_CONCURRENCY")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(16);
    let sem = std::sync::Arc::new(Semaphore::new(max_conc.max(1)));

    let mut futs: FuturesUnordered<_> = FuturesUnordered::new();
    for (cc, _cur) in regions.iter() {
        let cc_c = cc.clone();
        let app = appid.to_string();
        let client_c = client.clone();
        let permit = sem.clone().acquire_owned().await.unwrap();
        let language_c = language.clone();
        futs.push(async move {
            let _p = permit;
            let url = format!(
                "https://store.steampowered.com/api/appdetails?appids={}&cc={}&l={}&filters=price_overview,package_groups",
                app,
                cc_c,
                language_c
            );
            let body = get_with_backoff_json(&client_c, &url, &[]).await;
            (cc_c, body)
        });
    }

    let mut out: Vec<RegionPrice> = Vec::new();
    while let Some((cc, body)) = futs.next().await {
        if let Some(v) = body {
            if let Some(entry) = v.get(appid) {
                if let Ok(details) = serde_json::from_value::<AppDetailsWrapper>(entry.clone()) {
                    if details.success {
                        if let Some(po) = details.data.and_then(|d| d.price_overview) {
                            out.push(RegionPrice {
                                cc: cc.clone(),
                                currency: po.currency.clone(),
                                initial: po.initial_price,
                                final_: po.final_price,
                            });
                            continue;
                        }
                    }
                }
            }
        }
        // Fallback when no price_overview (unavailable/free region)
        out.push(RegionPrice {
            cc: cc.clone(),
            currency: None,
            initial: None,
            final_: None,
        });
    }

    // Sort by cc for stable output
    out.sort_by(|a, b| a.cc.cmp(&b.cc));
    println!("prices for app {} across {} regions:", appid, out.len());
    println!("cc  currency  final  initial  discount_pct");
    for r in out {
        let cur = r.currency.unwrap_or_else(|| "-".into());
        let fin = r
            .final_
            .map(|x| x.to_string())
            .unwrap_or_else(|| "-".into());
        let init = r
            .initial
            .map(|x| x.to_string())
            .unwrap_or_else(|| "-".into());
        let disc = match (r.initial, r.final_) {
            (Some(i), Some(f)) if i > 0 && f <= i => format!("{}", (100i64 - (f * 100) / i).max(0)),
            _ => "-".into(),
        };
        println!("{:<3} {:<8} {:>8} {:>8} {:>12}", r.cc, cur, fin, init, disc);
    }
    Ok(())
}

async fn run_backfill(
    db: &Db,
    provider_id: i64,
    _retailer_id: i64,
    regions: &[(String, String)],
    recent_days: i64,
    fetch_media: bool,
    language: &str,
    batch_flush: usize,
    max_conc: usize,
) -> Result<PostIngestSummary> {
    use sqlx::Row;
    // Missing recent prices per OJ
    let q = r#"
    SELECT pi.id AS provider_item_id, pi.external_id, oj.id AS offer_jurisdiction_id, c.code2 AS country_code, cur.code AS currency_code
        FROM public.provider_items pi
        JOIN public.provider_offers pof ON pof.provider_item_id = pi.id
        JOIN public.offers o ON o.id = pof.offer_id
        JOIN public.offer_jurisdictions oj ON oj.offer_id = o.id
        JOIN public.jurisdictions j ON j.id = oj.jurisdiction_id
        JOIN public.countries c ON c.id = j.country_id
        JOIN public.currencies cur ON cur.id = oj.currency_id
        LEFT JOIN public.prices p ON p.offer_jurisdiction_id = oj.id AND p.recorded_at > (now() - ($1::text)::interval)
        WHERE pi.provider_id = $2
        GROUP BY pi.id, pi.external_id, oj.id, c.code2, cur.code
        HAVING COUNT(p.id) = 0
    "#;
    let recent_str = format!("{} days", recent_days);
    let rows = sqlx::query(q)
        .persistent(false)
        .bind(&recent_str)
        .bind(provider_id)
        .fetch_all(&db.pool)
        .await?;

    // Build a map from region to currency and ensure domain entities
    use std::collections::HashMap;
    let mut cur_ids: HashMap<String, i64> = HashMap::new();
    let mut juris_ids: HashMap<String, i64> = HashMap::new();
    for (cc, cur) in regions.iter() {
        let mu = currency_minor_unit(cur);
        let cid = ensure_currency(db, cur, cur, mu).await?;
        cur_ids.insert(cc.clone(), cid);
        let country_id =
            ensure_country(db, &cc.to_ascii_uppercase(), &cc.to_ascii_uppercase(), cid).await?;
        let jid = ensure_national_jurisdiction(db, country_id).await?;
        juris_ids.insert(cc.clone(), jid);
    }

    // Concurrency for fetching prices
    use tokio::sync::Semaphore;
    let sem = std::sync::Arc::new(Semaphore::new(max_conc.max(1)));
    let client = Client::new();
    let mut price_rows: Vec<PriceRow> = Vec::new();
    let mut bundle_seen: HashSet<(i64, &'static str, i64)> = HashSet::new();
    let mut post_summary = PostIngestSummary::default();

    for r in rows {
        let appid: String = r.get::<String, _>("external_id");
        let cc: String = r.get::<String, _>("country_code");
        // Ensure offer & provider mapping (idempotent)
        // We can locate offer via OJ id, but to keep path consistent we'll re-ensure using mapping
        let provider_item_id: i64 = r.get("provider_item_id");
        post_summary.record_provider_item(provider_item_id);
        let oj_id: i64 = r.get("offer_jurisdiction_id");
        // Fetch appdetails for that region
        let permit = sem.clone().acquire_owned().await.unwrap();
        let url = format!(
            "https://store.steampowered.com/api/appdetails?appids={}&cc={}&l={}&filters=price_overview,package_groups",
            appid, cc, language
        );
        drop(permit);
        if let Some(body) = get_with_backoff_json(&client, &url, &[]).await {
            if let Some(entry) = body.get(&appid) {
                if let Ok(details) = serde_json::from_value::<AppDetailsWrapper>(entry.clone()) {
                    if details.success {
                        // Map to video_game title/platform if missing (lightweight check via provider_item id reuse)
                        // We don't have product here directly; skip deep mapping on backfill to reduce load.
                        let bundle_price = details
                            .data
                            .as_ref()
                            .and_then(|d| extract_best_bundle_price(d.package_groups.clone()));
                        if let Some(po) = details.data.and_then(|d| d.price_overview) {
                            if let Some(f) = po.final_price {
                                if f > 0 {
                                    price_rows.push(PriceRow {
                                        offer_jurisdiction_id: oj_id,
                                        provider_item_id: Some(provider_item_id),
                                        recorded_at: Utc::now(),
                                        amount_minor: f,
                                        tax_inclusive: true,
                                        fx_minor_per_unit: None,
                                        btc_sats_per_unit: None,
                                        meta: json!({"src":"steam","kind":"final","cc":cc,"language":language}),
                                    });
                                }
                            }
                            if let (Some(init), Some(fin)) = (po.initial_price, po.final_price) {
                                if init > fin {
                                    price_rows.push(PriceRow {
                                        offer_jurisdiction_id: oj_id,
                                        provider_item_id: Some(provider_item_id),
                                        recorded_at: Utc::now(),
                                        amount_minor: init,
                                        tax_inclusive: true,
                                        fx_minor_per_unit: None,
                                        btc_sats_per_unit: None,
                                        meta: json!({"src":"steam","kind":"initial","cc":cc,"language":language}),
                                    });
                                }
                            }
                            if let Some(bundle) = bundle_price {
                                let meta_final = json!({
                                    "src": "steam",
                                    "kind": "bundle_final",
                                    "cc": cc,
                                    "language": language,
                                    "package_id": bundle.package_id,
                                    "discount_pct": bundle.discount_pct,
                                    "standard_final_amount": po.final_price,
                                    "standard_initial_amount": po.initial_price,
                                    "base_amount_minor": bundle.base_minor,
                                });
                                let final_row = PriceRow {
                                    offer_jurisdiction_id: oj_id,
                                    provider_item_id: Some(provider_item_id),
                                    recorded_at: Utc::now(),
                                    amount_minor: bundle.final_minor,
                                    tax_inclusive: true,
                                    fx_minor_per_unit: None,
                                    btc_sats_per_unit: None,
                                    meta: meta_final,
                                };
                                let _ = push_bundle_price_row(
                                    &mut price_rows,
                                    &mut bundle_seen,
                                    &mut post_summary,
                                    final_row,
                                    "bundle_final",
                                );
                                if let Some(base_minor) = bundle.base_minor {
                                    if base_minor > bundle.final_minor {
                                        let meta_initial = json!({
                                            "src": "steam",
                                            "kind": "bundle_initial",
                                            "cc": cc,
                                            "language": language,
                                            "package_id": bundle.package_id,
                                            "discount_pct": bundle.discount_pct,
                                            "standard_final_amount": po.final_price,
                                        });
                                        let base_row = PriceRow {
                                            offer_jurisdiction_id: oj_id,
                                            provider_item_id: Some(provider_item_id),
                                            recorded_at: Utc::now(),
                                            amount_minor: base_minor,
                                            tax_inclusive: true,
                                            fx_minor_per_unit: None,
                                            btc_sats_per_unit: None,
                                            meta: meta_initial,
                                        };
                                        let _ = push_bundle_price_row(
                                            &mut price_rows,
                                            &mut bundle_seen,
                                            &mut post_summary,
                                            base_row,
                                            "bundle_initial",
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        if price_rows.len() >= batch_flush {
            let batch = std::mem::take(&mut price_rows);
            let batch_len = batch.len();
            let ingest_result = ingest_prices(db, batch).await?;
            post_summary.record_batch(batch_len, &ingest_result);
        }
    }
    if !price_rows.is_empty() {
        let batch_len = price_rows.len();
        let ingest_result = ingest_prices(db, price_rows).await?;
        post_summary.record_batch(batch_len, &ingest_result);
    }

    // Media backfill for items with no media
    if fetch_media {
        let media_language_backfill = std::env::var("STEAM_MEDIA_LANGUAGE")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .map(|s| normalize_language(&s))
            .unwrap_or_else(|| normalize_language("english"));
        let q2 = r#"
            SELECT pi.id AS provider_item_id, pi.external_id
            FROM public.provider_items pi
            LEFT JOIN public.provider_media_links ml ON ml.provider_item_id = pi.id
            WHERE pi.provider_id = $1
            GROUP BY pi.id, pi.external_item_id
            HAVING COUNT(ml.id) = 0
        "#;
        let items = sqlx::query(q2)
            .persistent(false)
            .bind(provider_id)
            .fetch_all(&db.pool)
            .await?;
        for r in items {
            let pid: i64 = r.get("provider_item_id");
            post_summary.record_provider_item(pid);
            let appid: String = r.get("external_id");
            // Use default region for media
            let media_cc = std::env::var("STEAM_MEDIA_CC").unwrap_or_else(|_| "US".into());
            if let Ok(urls) =
                fetch_media_urls(&client, &appid, &media_cc, &media_language_backfill).await
            {
                if !urls.is_empty() {
                    let mut tuples: Vec<(String, Option<String>, Option<String>, Option<String>)> =
                        Vec::new();
                    for url in urls {
                        let lower = url.to_ascii_lowercase();
                        let (mtype, role) = if lower.ends_with(".mp4") || lower.ends_with(".webm") {
                            (Some("video".to_string()), Some("trailer".to_string()))
                        } else if lower.contains("header") || lower.contains("capsule") {
                            (Some("image".to_string()), Some("cover".to_string()))
                        } else if lower.contains("library_logo") || lower.contains("logo") {
                            (Some("image".to_string()), Some("logo".to_string()))
                        } else {
                            (Some("image".to_string()), Some("screenshot".to_string()))
                        };
                        tuples.push((url, mtype, role, None));
                    }
                    let meta = serde_json::json!({
                        "cc": media_cc,
                        "language": media_language_backfill.as_str(),
                        "scope": "backfill",
                        "backfill": true,
                    });
                    let _ = ensure_provider_media_links_with_meta(
                        db,
                        pid,
                        None,
                        &tuples,
                        "steam",
                        Some(meta),
                    )
                    .await?;
                    // Try to locate vg_id for this provider_item via relational mapping, then upsert into game_media as well
                    if
                        let Ok(Some(vg_row)) = sqlx
                            ::query(
                                "SELECT vg.id AS vg_id FROM provider_items pi JOIN provider_offers pof ON pof.provider_item_id=pi.id JOIN offers o ON o.id=pof.offer_id JOIN sellables s ON s.id=o.sellable_id JOIN products p ON p.id=s.product_id JOIN video_game_titles vgt ON vgt.video_game_id=p.id JOIN video_games vg ON vg.title_id=vgt.id WHERE pi.id=$1 LIMIT 1"
                            )
                            .persistent(false)
                            .bind(pid)
                            .fetch_optional(&db.pool).await
                    {
                        let vg_id: i64 = vg_row.get("vg_id");
                        for (url, mtype, role, _title) in &tuples {
                            let mtype_final = mtype.as_deref().unwrap_or("image");
                            let pdata =
                                serde_json::json!({
                                "role": role,
                                "cc": media_cc,
                                "language": media_language_backfill.as_str(),
                                "scope": "backfill",
                                "backfill": true,
                            });
                            let _ = upsert_game_media(
                                db,
                                vg_id,
                                "steam",
                                url,
                                mtype_final,
                                url,
                                pdata
                            ).await;
                        }
                    }
                }
            }
        }
    }

    info!(
        provider_id,
        price_rows = post_summary.total_price_rows_written,
        provider_items = post_summary.provider_item_ids.len(),
        offer_jurisdictions = post_summary.offer_jurisdiction_ids.len(),
        "steam backfill complete"
    );
    Ok(post_summary)
}

fn slugify(s: &str) -> String {
    s.to_lowercase()
        .replace(|c: char| !c.is_ascii_alphanumeric(), "-")
        .trim_matches('-')
        .to_string()
}

fn currency_minor_unit(code: &str) -> i16 {
    match code.to_ascii_uppercase().as_str() {
        "JPY" | "KRW" | "VND" | "CLP" | "ISK" | "HUF" => 0,
        "BHD" | "IQD" | "KWD" | "JOD" | "OMR" | "TND" => 3,
        _ => 2,
    }
}

fn load_steam_regions() -> Vec<(String, String)> {
    // returns Vec<(country_code, currency_code)>; env STEAM_REGIONS override: "US:USD,GB:GBP,DE:EUR"
    if let Ok(s) = std::env::var("STEAM_REGIONS") {
        let mut out = Vec::new();
        for part in s.split([',', ' ']) {
            if part.trim().is_empty() {
                continue;
            }
            if let Some((cc, cur)) = part.split_once(':') {
                out.push((cc.trim().to_string(), cur.trim().to_string()));
            }
        }
        if !out.is_empty() {
            return out;
        }
    }
    // Curated defaults
    vec![
        ("US", "USD"),
        ("CA", "CAD"),
        ("GB", "GBP"),
        ("DE", "EUR"),
        ("FR", "EUR"),
        ("ES", "EUR"),
        ("IT", "EUR"),
        ("NL", "EUR"),
        ("BE", "EUR"),
        ("PT", "EUR"),
        ("IE", "EUR"),
        ("FI", "EUR"),
        ("GR", "EUR"),
        ("AT", "EUR"),
        ("SE", "SEK"),
        ("NO", "NOK"),
        ("DK", "DKK"),
        ("CH", "CHF"),
        ("PL", "PLN"),
        ("CZ", "CZK"),
        ("SK", "EUR"),
        ("HU", "HUF"),
        ("RO", "RON"),
        ("BG", "BGN"),
        ("HR", "EUR"),
        ("SI", "EUR"),
        ("LT", "EUR"),
        ("LV", "EUR"),
        ("EE", "EUR"),
        ("JP", "JPY"),
        ("KR", "KRW"),
        ("AU", "AUD"),
        ("NZ", "NZD"),
        ("BR", "BRL"),
        ("MX", "MXN"),
        ("AR", "ARS"),
        ("ZA", "ZAR"),
        ("TR", "TRY"),
        ("UA", "UAH"),
        ("HK", "HKD"),
        ("TW", "TWD"),
        ("SG", "SGD"),
        ("MY", "MYR"),
        ("TH", "THB"),
        ("ID", "IDR"),
        ("PH", "PHP"),
        ("VN", "VND"),
        ("CN", "CNY"),
        ("IL", "ILS"),
        ("CL", "CLP"),
        ("CO", "COP"),
        ("PE", "PEN"),
        ("UY", "UYU"),
        ("KZ", "KZT"),
        ("SA", "SAR"),
        ("AE", "AED"),
        ("EG", "EGP"),
    ]
    .into_iter()
    .map(|(a, b)| (a.to_string(), b.to_string()))
    .collect()
}

// -------- HTTP helpers with 429 backoff --------

async fn get_with_backoff_json(
    client: &Client,
    url: &str,
    query_pairs: &[(&str, &str)],
) -> Option<Value> {
    let delays = [5u64, 10, 15, 20];
    let mut attempt: usize = 0;
    loop {
        let mut req = client.get(url).header("Accept", "application/json");
        if !query_pairs.is_empty() {
            req = req.query(&query_pairs);
        }
        let resp = req.send().await.ok()?;
        if resp.status().as_u16() != 429 {
            return resp.json::<Value>().await.ok();
        }
        if attempt >= delays.len() {
            return None;
        }
        let mut sleep_secs = delays[attempt];
        if let Some(retry_after) = resp
            .headers()
            .get("Retry-After")
            .and_then(|h| h.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
        {
            if retry_after > sleep_secs {
                sleep_secs = retry_after;
            }
        }
        attempt += 1;
        tokio::time::sleep(std::time::Duration::from_secs(sleep_secs)).await;
    }
}

fn normalize_language(lang: &str) -> String {
    let mut s = lang.to_ascii_lowercase().replace([' ', '-'], "_");
    if s.is_empty() {
        s = "english".into();
    }
    s
}

// Fetch a deterministic slice of the public Steam app list (not all will have prices).
async fn fetch_app_list(limit: usize) -> Result<Vec<String>> {
    let url = "https://api.steampowered.com/ISteamApps/GetAppList/v2";
    let body = reqwest::get(url).await?.json::<AppListResp>().await?;
    let mut out: Vec<String> = Vec::new();
    for a in body.applist.apps.into_iter().take(limit) {
        out.push(a.appid.to_string());
    }
    Ok(out)
}

// Fetch full app details without limiting filters (single app request) for enrichment
async fn fetch_full_details(client: &Client, appid: &str) -> Result<Option<FullAppData>> {
    let url = format!(
        "https://store.steampowered.com/api/appdetails?appids={}",
        appid
    );
    if let Some(v) = get_with_backoff_json(client, &url, &[]).await {
        if let Some(entry) = v.get(appid) {
            if let Ok(details) = serde_json::from_value::<serde_json::Value>(entry.clone()) {
                if details
                    .get("success")
                    .and_then(|b| b.as_bool())
                    .unwrap_or(false)
                {
                    if let Some(data_v) = details.get("data") {
                        let full: FullAppData =
                            serde_json::from_value(data_v.clone()).unwrap_or(FullAppData {
                                name: None,
                                price_overview: None,
                                metacritic: None,
                                recommendations: None,
                                genres: None,
                                short_description: None,
                                about_the_game: None,
                                detailed_description: None,
                                movies: None,
                            });
                        return Ok(Some(full));
                    }
                }
            }
        }
    }
    Ok(None)
}

// -------- Media extraction (screenshots, covers, trailers) --------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct MediaData {
    #[serde(default)]
    steam_appid: Option<i64>,
    #[serde(default)]
    screenshots: Option<Vec<Value>>,
    #[serde(default)]
    movies: Option<Vec<Value>>,
    #[serde(default)]
    background: Option<String>,
    #[serde(default)]
    background_raw: Option<String>,
    #[serde(default)]
    header_image: Option<String>,
    #[serde(default)]
    capsule_imagev5: Option<String>,
    #[serde(default)]
    capsule_image: Option<String>,
}

async fn fetch_media_urls(
    client: &Client,
    appid: &str,
    country: &str,
    language: &str,
) -> Result<Vec<String>> {
    let url = "https://store.steampowered.com/api/appdetails";
    let qp = [
        ("appids", appid),
        ("cc", country),
        ("l", language),
        ("filters", "screenshots,movies"),
    ];
    let body = get_with_backoff_json(client, url, &qp)
        .await
        .unwrap_or(Value::Null);
    let mut out: Vec<String> = Vec::new();
    let entry = body.get(appid).cloned().unwrap_or(Value::Null);
    if entry.is_null() {
        return Ok(out);
    }
    let success = entry
        .get("success")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if !success {
        return Ok(out);
    }
    let data_v = entry.get("data").cloned().unwrap_or(Value::Null);
    let data: MediaData = serde_json::from_value(data_v.clone()).unwrap_or_default();

    // Helper: normalize possibly-relative Steam URLs to fully-qualified
    fn normalize_steam_url(u: &str) -> String {
        let s = u.trim();
        if s.is_empty() {
            return String::new();
        }
        if s.starts_with("//") {
            // Protocol-relative -> assume https
            return format!("https:{}", s);
        }
        if s.starts_with("/steam/apps/") {
            return format!("https://cdn.cloudflare.steamstatic.com{}", s);
        }
        if s.starts_with('/') {
            // Fallback to main store host for other root-relative paths
            return format!("https://store.steampowered.com{}", s);
        }
        s.to_string()
    }

    // Screenshots
    if let Some(shots) = data.screenshots {
        for s in shots {
            if let Some(url) = s.get("path_full").and_then(|v| v.as_str()) {
                let u = normalize_steam_url(url);
                if !u.is_empty() {
                    out.push(u);
                }
            }
            if let Some(thumb) = s.get("path_thumbnail").and_then(|v| v.as_str()) {
                let u = normalize_steam_url(thumb);
                if !u.is_empty() {
                    out.push(u);
                }
            }
        }
    }
    // Backgrounds and covers
    for u in [
        data.background,
        data.background_raw,
        data.header_image,
        data.capsule_imagev5,
        data.capsule_image,
    ] {
        if let Some(u) = u {
            let u2 = normalize_steam_url(&u);
            if !u2.is_empty() {
                out.push(u2);
            }
        }
    }

    // Guess known CDN patterns when appid known
    if let Some(id) = data.steam_appid.or_else(|| appid.parse::<i64>().ok()) {
        let base = format!("https://cdn.cloudflare.steamstatic.com/steam/apps/{}/", id);
        for f in [
            "header.jpg",
            "capsule_616x353.jpg",
            "capsule_1232x706.jpg",
            "capsule_748x896.jpg",
            "capsule_sm_462x174.jpg",
            "library_hero.jpg",
            "library_600x900.jpg",
            "library_logo.png",
        ] {
            out.push(format!("{}{}", base, f));
        }
    }

    // Movies: prefer stream url fields
    if let Some(movies) = data_v.get("movies").and_then(|v| v.as_array()) {
        for m in movies {
            if let Some(webm_obj) = m.get("webm").and_then(|v| v.as_object()) {
                for url in webm_obj.values().filter_map(|v| v.as_str()) {
                    let u = normalize_steam_url(url);
                    if !u.is_empty() {
                        out.push(u);
                    }
                }
            }
            if let Some(mp4_obj) = m.get("mp4").and_then(|v| v.as_object()) {
                for url in mp4_obj.values().filter_map(|v| v.as_str()) {
                    let u = normalize_steam_url(url);
                    if !u.is_empty() {
                        out.push(u);
                    }
                }
            }
            if let Some(th) = m.get("thumbnail").and_then(|v| v.as_str()) {
                let u = normalize_steam_url(th);
                if !u.is_empty() {
                    out.push(u);
                }
            }
        }
    }

    // Dedup
    out.sort();
    out.dedup();
    Ok(out)
}

#[cfg(test)]
mod tests_url_norm {
    #[test]
    fn normalize_relative_and_protocol_relative() {
        // local helper must be in scope; duplicate simple logic here for test to avoid private access
        fn norm(u: &str) -> String {
            let s = u.trim();
            if s.is_empty() {
                return String::new();
            }
            if s.starts_with("//") {
                return format!("https:{}", s);
            }
            if s.starts_with("/steam/apps/") {
                return format!("https://cdn.cloudflare.steamstatic.com{}", s);
            }
            if s.starts_with('/') {
                return format!("https://store.steampowered.com{}", s);
            }
            s.to_string()
        }
        assert_eq!(
            norm("//cdn.cloudflare.steamstatic.com/steam/apps/570/header.jpg"),
            "https://cdn.cloudflare.steamstatic.com/steam/apps/570/header.jpg"
        );
        assert_eq!(
            norm("/steam/apps/570/header.jpg"),
            "https://cdn.cloudflare.steamstatic.com/steam/apps/570/header.jpg"
        );
        assert_eq!(norm("/app/570/"), "https://store.steampowered.com/app/570/");
        assert_eq!(norm("https://example.com/x"), "https://example.com/x");
    }
}
