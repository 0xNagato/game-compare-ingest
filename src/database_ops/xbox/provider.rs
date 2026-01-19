use crate::database_ops::db::{Db, PriceRow};
use crate::database_ops::ingest_providers::{
    ensure_country, ensure_currency, ensure_national_jurisdiction, ensure_platform,
    ensure_provider, ensure_retailer, ensure_vg_source_media_links,
    ensure_vg_source_media_links_with_meta, ingest_prices, ingest_run_finish, ingest_run_start,
    link_provider_offer, upsert_game_media, PostIngestSummary, ProviderEntityCache,
};
use crate::database_ops::media_filter::{
    classify_image_from_url, classify_video_from_url, filter_images, filter_videos,
    should_include_screenshots, MediaStats,
};
use super::headers::{
    build_xbox_live_headers, contract_versions, CorrelationVector, XboxLiveConfig,
};
use super::xbl_auth;
use anyhow::{anyhow, Context, Result};
use chrono::{NaiveDate, NaiveDateTime, Utc};
use reqwest::Client;
use serde_json::{json, Value};
use sqlx::types::Json;
use sqlx::Row;
use std::collections::HashSet;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, warn};

// NOTE:
// The Xbox/Microsoft Store Display Catalog API (`displaycatalog.mp.microsoft.com`) requires
// Xbox Live authentication. We obtain an XSTS token via the `xbl_auth` module.
//
// Important terminology note:
// - This provider uses the *traditional* Xbox Live auth chain (device/user token -> XSTS).
// - We intentionally avoid the SISU flow due to response-shape instability.
const XBOX_PROVIDER_KEY: &str = "xbox-store";

/// Minimal Microsoft Store (Display Catalog) ingestion for specified product IDs.
#[derive(Clone)]
pub struct XboxProvider {
    client: Client,
    auth: Option<XboxAuth>,
    correlation_vector: Arc<Mutex<CorrelationVector>>,
    xbox_config: XboxLiveConfig,
}

#[derive(Clone)]
enum XboxAuth {
    Static(String),
    Xsts(Arc<Mutex<Option<String>>>), // Cached XSTS token from Xbox Live authentication
}

impl XboxAuth {
    fn from_env() -> Result<Self> {
        // Check for static token override (for testing/debugging)
        let direct = std::env::var("XBOX_ACCESS_TOKEN")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        if let Some(token) = direct {
            info!("Using static Xbox access token from XBOX_ACCESS_TOKEN env var");
            return Ok(Self::Static(token));
        }

        // Default to Xbox Live (traditional) authentication via `xbl_auth`.
        // Initialize with empty cache - token will be fetched on first use.
        info!("Using Xbox Live authentication (XSTS via traditional flow)");
        Ok(Self::Xsts(Arc::new(Mutex::new(None))))
    }
}



#[derive(Debug, Clone)]
pub struct XboxOptions {
    pub market: String,           // e.g., "US"
    pub language: String,         // e.g., "en-US"
    pub product_ids: Vec<String>, // list of bigIds
    // Note: MS-CV is now auto-generated via CorrelationVector
}

impl Default for XboxOptions {
    fn default() -> Self {
        Self {
            market: std::env::var("XBOX_MARKET").unwrap_or_else(|_| "US".to_string()),
            language: std::env::var("XBOX_LANGUAGE").unwrap_or_else(|_| "en".to_string()),
            product_ids: load_product_ids_from_env(),
        }
    }
}

impl XboxProvider {
    pub fn new() -> Result<Self> {
        let client = Client::new();
        let auth = XboxAuth::from_env()?;
        let correlation_vector = Arc::new(Mutex::new(CorrelationVector::new()));
        let xbox_config = XboxLiveConfig::from_env();
        Ok(Self {
            client,
            auth: Some(auth),
            correlation_vector,
            xbox_config,
        })
    }

    async fn build_request(
        &self,
        url: &str,
        _opts: &XboxOptions,
    ) -> Result<reqwest::RequestBuilder> {
        // Get auth token
        let token = self
            .auth_token()
            .await?
            .ok_or_else(|| anyhow!("Xbox authentication token not available"))?;

        // Get next correlation vector value
        let cv = {
            let mut cv_guard = self.correlation_vector.lock().await;
            cv_guard.next()
        };

        // Build Xbox Live headers with proper contract version
        let headers = build_xbox_live_headers(
            &token,
            contract_versions::DISPLAY_CATALOG,
            &cv,
            &self.xbox_config,
        )
        .context("Failed to build Xbox Live headers")?;

        // Create request with all headers
        let req = self.client.get(url).headers(headers);

        Ok(req)
    }

    async fn auth_token(&self) -> Result<Option<String>> {
        match &self.auth {
            Some(XboxAuth::Static(token)) => {
                info!("Using static token for Xbox authentication");
                Ok(Some(token.clone()))
            }
            Some(XboxAuth::Xsts(token_cache)) => {
                // Check if we have a cached token
                {
                    let cache_guard = token_cache.lock().await;
                    if let Some(cached_token) = cache_guard.as_ref() {
                        info!("Using cached XSTS token");
                        return Ok(Some(cached_token.clone()));
                    }
                }

                // No cached token - fetch new one
                info!("Fetching XSTS token via Xbox Live authentication (traditional)");
                let xsts_token = xbl_auth::get_xsts_token()
                    .await
                    .context("Failed to get XSTS token. Run 'cargo run --bin xbox_auth_setup' to authenticate.")?;

                // Cache the token for future requests
                {
                    let mut cache_guard = token_cache.lock().await;
                    *cache_guard = Some(xsts_token.clone());
                    info!("XSTS token cached for subsequent requests");
                }

                Ok(Some(xsts_token))
            }
            None => Ok(None),
        }
    }

    fn pace_ms() -> Option<u64> {
        // Prefer RPM over RPS; fall back to None (no pacing)
        if let Ok(v) = std::env::var("XBOX_REQS_PER_MIN") {
            if let Ok(rpm) = v.parse::<u32>() {
                if rpm > 0 {
                    return Some(60_000u64 / (rpm as u64));
                }
            }
        }
        if let Ok(v) = std::env::var("XBOX_RPS") {
            if let Ok(rps) = v.parse::<f32>() {
                if rps > 0.0 {
                    return Some((1000.0 / rps) as u64);
                }
            }
        }
        None
    }

    fn retry_policy() -> (u32, u64) {
        let max_retries: u32 = std::env::var("XBOX_MAX_RETRIES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(3);
        let backoff_ms: u64 = std::env::var("XBOX_BACKOFF_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1000);
        (max_retries, backoff_ms)
    }

    fn toplist_debug_enabled() -> bool {
        std::env::var("XBOX_TOPLIST_DEBUG")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    }

    fn truncate_log_body(body: &str) -> String {
        // Keep logs readable and avoid dumping huge HTML/JSON bodies.
        const MAX: usize = 400;
        let trimmed = body.trim();
        if trimmed.len() <= MAX {
            return trimmed.to_string();
        }
        let mut out = trimmed[..MAX].to_string();
        out.push_str("…");
        out
    }

    fn candidate_collection_keys(input: &str) -> Vec<String> {
        // Historically, some collection slugs appear in different forms:
        //   "New" vs "newGames", and potentially "TopPaid" vs "topPaidGames".
        // We try multiple variants (keeping caller-provided value first).
        let raw = input.trim();
        if raw.is_empty() {
            return Vec::new();
        }

        let mut keys: Vec<String> = Vec::new();
        keys.push(raw.to_string());

        // Known/likely alias for New.
        if raw.eq_ignore_ascii_case("new") {
            keys.push("newGames".to_string());
        }

        // Try lowerCamel + "Games" suffix.
        if raw.chars().all(|c| c.is_ascii_alphanumeric()) {
            let mut chars = raw.chars();
            if let Some(first) = chars.next() {
                let mut lower_camel = String::new();
                lower_camel.push(first.to_ascii_lowercase());
                lower_camel.extend(chars);

                if !lower_camel.ends_with("Games") {
                    keys.push(format!("{}Games", lower_camel));
                }
                keys.push(lower_camel);
            }
        }

        // Try all-lowercase variants.
        let lower = raw.to_ascii_lowercase();
        keys.push(lower.clone());
        if !lower.ends_with("games") {
            keys.push(format!("{}games", lower));
        }

        keys.sort();
        keys.dedup();

        // Re-prioritize: keep the exact input first.
        if let Some(pos) = keys.iter().position(|k| k == raw) {
            let first = keys.remove(pos);
            keys.insert(0, first);
        }

        keys
    }

    /// Dry-run: fetch and dump Microsoft DC JSON without touching the database.
    /// Returns the number of products observed across all chunks.
    pub async fn run_dump_only(&self, opts: XboxOptions) -> Result<usize> {
        if opts.product_ids.is_empty() {
            warn!("XBOX_PRODUCT_IDS empty; dry-run has nothing to fetch");
            return Ok(0);
        }

        let dump_json = std::env::var("XBOX_DUMP_JSON")
            .map(|v| (v == "1" || v.eq_ignore_ascii_case("true")))
            .unwrap_or(false);
        let dump_stdout = std::env::var("XBOX_DUMP_STDOUT")
            .map(|v| (v == "1" || v.eq_ignore_ascii_case("true")))
            .unwrap_or(false);
        let dump_dir =
            std::env::var("XBOX_DUMP_DIR").unwrap_or_else(|_| "exports/xbox".to_string());
        let run_dir: Option<PathBuf> = if dump_json {
            let ts = chrono::Utc::now().format("%Y%m%d_%H%M%S");
            let dir = PathBuf::from(dump_dir).join(format!("run_{}", ts));
            let _ = fs::create_dir_all(&dir);
            Some(dir)
        } else {
            None
        };
        let mut ndjson_file: Option<std::fs::File> = if let Some(dir) = &run_dir {
            let path = dir.join("logs.ndjson");
            OpenOptions::new().create(true).append(true).open(path).ok()
        } else {
            None
        };

        let mut total_products = 0usize;
        let mut had_error = false;
        let chunk_size: usize = std::env::var("XBOX_CHUNK_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(30)
            .max(1);
        for (chunk_idx, chunk) in opts.product_ids.chunks(chunk_size).enumerate() {
            let url = format!(
                "https://displaycatalog.mp.microsoft.com/v7.0/products?bigIds={}&market={}&languages={}",
                chunk.join(","),
                opts.market,
                opts.language
            );
            let (max_retries, backoff_ms) = Self::retry_policy();
            let pace_ms = Self::pace_ms();
            // do request with retry policy
            let mut tries: u32 = 0;
            let resp = loop {
                tries += 1;
                if let Some(ms) = pace_ms {
                    tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
                }
                let request = self.build_request(&url, &opts).await?;
                match request.send().await {
                    Ok(r) => {
                        if r.status().as_u16() == 429 {
                            let mut sleep_ms: u64 = std::env::var("XBOX_CHUNK_SLEEP_MS")
                                .ok()
                                .and_then(|s| s.parse().ok())
                                .unwrap_or(backoff_ms);
                            if let Some(ra) = r
                                .headers()
                                .get("Retry-After")
                                .and_then(|h| h.to_str().ok())
                                .and_then(|s| s.parse::<u64>().ok())
                            {
                                sleep_ms = (ra * 1000).max(sleep_ms);
                            }
                            warn!(chunk=%chunk_idx, sleep_ms, "xbox 429 throttled; sleeping before retry (dry-run)");
                            tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;
                            if tries <= max_retries {
                                continue;
                            }
                            break r;
                        } else {
                            break r;
                        }
                    }
                    Err(e) => {
                        if tries <= max_retries {
                            tokio::time::sleep(std::time::Duration::from_millis(
                                backoff_ms.saturating_mul(tries as u64),
                            ))
                            .await;
                            continue;
                        }
                        had_error = true;
                        tracing::error!(error=%e, url, "xbox http error (dry-run)");
                        if let Some(f) = ndjson_file.as_mut() {
                            let _ = writeln!(
                                f,
                                "{}",
                                serde_json::json!({"ts": chrono::Utc::now(), "chunk": chunk_idx, "status":"http_error", "error": e.to_string(), "ids": chunk})
                            );
                        }
                        continue;
                    }
                }
            };
            let resp = match resp.error_for_status() {
                Ok(ok) => ok,
                Err(e) => {
                    had_error = true;
                    tracing::error!(error=%e, url, "xbox non-success status (dry-run)");
                    if let Some(f) = ndjson_file.as_mut() {
                        let _ = writeln!(
                            f,
                            "{}",
                            serde_json::json!({"ts": chrono::Utc::now(), "chunk": chunk_idx, "status":"bad_status", "error": e.to_string(), "ids": chunk})
                        );
                    }
                    continue;
                }
            };
            let body: Value = match resp.json().await {
                Ok(b) => b,
                Err(e) => {
                    had_error = true;
                    tracing::error!(error=%e, url, "xbox json decode failed (dry-run)");
                    if let Some(f) = ndjson_file.as_mut() {
                        let _ = writeln!(
                            f,
                            "{}",
                            serde_json::json!({"ts": chrono::Utc::now(), "chunk": chunk_idx, "status":"json_error", "error": e.to_string(), "ids": chunk})
                        );
                    }
                    continue;
                }
            };
            if dump_stdout {
                println!("===== xbox chunk {} ({} ids) =====", chunk_idx, chunk.len());
                match serde_json::to_string_pretty(&body) {
                    Ok(pretty) => println!("{}", pretty),
                    Err(_) => println!("{}", body),
                }
            }
            if let Some(dir) = &run_dir {
                let first = chunk.get(0).cloned().unwrap_or_else(|| "chunk".to_string());
                let path = dir.join(format!("chunk_{:04}_{}.json", chunk_idx, first));
                if let Ok(mut f) = OpenOptions::new().create(true).write(true).open(path) {
                    let _ = write!(
                        f,
                        "{}",
                        serde_json::to_string_pretty(&body).unwrap_or_else(|_| "{}".into())
                    );
                }
            }
            let products = body
                .get("Products")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            total_products += products.len();
            if let Some(f) = ndjson_file.as_mut() {
                let _ = writeln!(
                    f,
                    "{}",
                    serde_json::json!({
                        "ts": chrono::Utc::now(),
                        "chunk": chunk_idx,
                        "status": "ok",
                        "ids": chunk,
                        "products": products.len(),
                    })
                );
            }
        }
        info!(products=total_products, status = if had_error { "partial" } else { "ok" }, market=%opts.market, language=%opts.language, "xbox dry-run complete");
        Ok(total_products)
    }

    async fn fetch_categories(&self, market: &str, language: &str) -> Result<Value> {
        let ms_cv = std::env::var("XBOX_MS_CV").unwrap_or_else(|_| "DGU1mcuYo0WMMp+".to_string());
        let urls = vec![
            format!(
                "https://displaycatalog.mp.microsoft.com/v7.0/categories?market={}&languages={}&categoryType=Games&deviceFamily=Windows.Xbox",
                market, language
            ),
            format!(
                "https://displaycatalog.mp.microsoft.com/v7.0/categories?market={}&languages={}&deviceFamily=Windows.Xbox",
                market, language
            ),
        ];

        let debug = Self::toplist_debug_enabled();
        let mut attempts: Vec<String> = Vec::new();

        for url in urls {
            let mut req = self.client.get(&url).header("MS-CV", &ms_cv);
            if let Some(token) = self.auth_token().await? {
                req = req.bearer_auth(token);
            }

            let resp = req.send().await?;
            if resp.status().is_success() {
                return Ok(resp.json().await?);
            }

            if debug {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                attempts.push(format!(
                    "{} => {} — {}",
                    url,
                    status,
                    Self::truncate_log_body(&body)
                ));
            }
        }

        if debug && !attempts.is_empty() {
            return Err(anyhow!(
                "category discovery failed for market {} (attempts: {})",
                market,
                attempts.join(" | ")
            ));
        }

        Err(anyhow!("category discovery failed for market {}", market))
    }

    fn extract_categories(payload: &Value) -> Vec<(String, String)> {
        let mut categories = Vec::new();

        if let Some(items) = payload.get("Categories").and_then(|v| v.as_array()) {
            for item in items {
                if let Some(id) = item.get("Id").and_then(|v| v.as_str()) {
                    let name = item
                        .get("Name")
                        .and_then(|v| v.as_str())
                        .or_else(|| {
                            item.get("LocalizedProperties").and_then(|lp| {
                                lp.as_array()
                                    .and_then(|arr| arr.first())
                                    .and_then(|first| first.get("Name").and_then(|v| v.as_str()))
                            })
                        })
                        .unwrap_or(id)
                        .to_string();
                    categories.push((id.to_string(), name));
                }
            }
        }

        if categories.is_empty() {
            if let Some(items) = payload.get("Items").and_then(|v| v.as_array()) {
                for item in items {
                    if let Some(id) = item.get("CategoryId").and_then(|v| v.as_str()) {
                        let name = item
                            .get("Name")
                            .and_then(|v| v.as_str())
                            .unwrap_or(id)
                            .to_string();
                        categories.push((id.to_string(), name));
                    }
                }
            }
        }

        categories
    }

    async fn fetch_category_top(
        &self,
        market: &str,
        category_id: &str,
        top: usize,
    ) -> Result<Value> {
        let language =
            std::env::var("XBOX_CATEGORY_LANGUAGE").unwrap_or_else(|_| "en-us".to_string());
        let ms_cv = std::env::var("XBOX_MS_CV").unwrap_or_else(|_| "DGU1mcuYo0WMMp+".to_string());
        let url = format!(
            "https://displaycatalog.mp.microsoft.com/v7.0/products?market={}&languages={}&categoryId={}&deviceFamily=Windows.Xbox&productFamilyNames=Games&orderBy=rank&top={}&skipItems=0",
            market, language, category_id, top
        );

        let mut req = self.client.get(&url).header("MS-CV", &ms_cv);
        if let Some(token) = self.auth_token().await? {
            req = req.bearer_auth(token);
        }
        let resp = req.send().await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!(
                "category {} ({}): request failed {} — {}",
                category_id,
                market,
                status,
                body
            ));
        }

        Ok(resp.json().await?)
    }

    async fn fetch_collection(&self, market: &str, collection: &str, top: usize) -> Result<Value> {
        let language =
            std::env::var("XBOX_CATEGORY_LANGUAGE").unwrap_or_else(|_| "en-us".to_string());
        let ms_cv = std::env::var("XBOX_MS_CV").unwrap_or_else(|_| "DGU1mcuYo0WMMp+".to_string());
        let debug = Self::toplist_debug_enabled();
        let mut attempts: Vec<String> = Vec::new();

        let keys = Self::candidate_collection_keys(collection);
        for key in keys {
            // v7 pattern A: with deviceFamily
            let url_v7_a = format!(
                "https://displaycatalog.mp.microsoft.com/v7.0/products/collections/{}?market={}&languages={}&count={}&deviceFamily=Windows.Xbox",
                key, market, language, top
            );
            // v7 pattern B: (observed in standalone tooling) add MS-CV query param; omit deviceFamily
            let url_v7_b = format!(
                "https://displaycatalog.mp.microsoft.com/v7.0/products/collections/{}?market={}&languages={}&count={}&MS-CV=1",
                key, market, language, top
            );

            for url in [url_v7_a, url_v7_b] {
                let mut req = self.client.get(&url).header("MS-CV", &ms_cv);
                if let Some(token) = self.auth_token().await? {
                    req = req.bearer_auth(token);
                }
                let resp = req.send().await?;
                if resp.status().is_success() {
                    return Ok(resp.json().await?);
                }
                if debug {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    attempts.push(format!(
                        "{} => {} — {}",
                        url,
                        status,
                        Self::truncate_log_body(&body)
                    ));
                }
            }

            // v8 computed fallback
            let url_v8 = format!(
                "https://displaycatalog.mp.microsoft.com/v8.0/products/collections/Computed/{}?market={}&languages={}&itemType=Game&deviceFamily=Windows.Xbox&count={}",
                key, market, language, top
            );
            let mut req2 = self.client.get(&url_v8).header("MS-CV", &ms_cv);
            if let Some(token) = self.auth_token().await? {
                req2 = req2.bearer_auth(token);
            }
            let resp2 = req2.send().await?;
            if resp2.status().is_success() {
                return Ok(resp2.json().await?);
            }
            if debug {
                let status = resp2.status();
                let body = resp2.text().await.unwrap_or_default();
                attempts.push(format!(
                    "{} => {} — {}",
                    url_v8,
                    status,
                    Self::truncate_log_body(&body)
                ));
            }
        }

        // Fallback: "New" toplists sometimes work via the browse endpoint even when
        // categories/collections are unavailable for a given market.
        let is_new = collection.eq_ignore_ascii_case("new")
            || collection.eq_ignore_ascii_case("newGames")
            || collection.eq_ignore_ascii_case("newgames");
        if is_new {
            let url = format!(
                "https://displaycatalog.mp.microsoft.com/v7.0/products/browse?market={}&languages={}&skipItems=0&top={}&categoryId=Games&productFamilyNames=Games&deviceFamily=Windows.Xbox&orderBy=releaseDate&sortOrder=desc",
                market, language, top
            );
            let mut req = self.client.get(&url).header("MS-CV", &ms_cv);
            if let Some(token) = self.auth_token().await? {
                req = req.bearer_auth(token);
            }
            let resp = req.send().await?;
            if resp.status().is_success() {
                return Ok(resp.json().await?);
            }
            if debug {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                attempts.push(format!(
                    "{} => {} — {}",
                    url,
                    status,
                    Self::truncate_log_body(&body)
                ));
            }
        }

        if debug && !attempts.is_empty() {
            return Err(anyhow!(
                "collection {} ({}): all attempts failed: {}",
                collection,
                market,
                attempts.join(" | ")
            ));
        }

        Err(anyhow!(
            "collection {} ({}): request failed (enable XBOX_TOPLIST_DEBUG=1 for details)",
            collection,
            market
        ))
    }

    fn extract_product_ids(payload: &Value) -> Vec<String> {
        let mut ids = Vec::new();

        if let Some(products) = payload.get("Products").and_then(|v| v.as_array()) {
            for product in products {
                if let Some(id) = product.get("ProductId").and_then(|v| v.as_str()) {
                    ids.push(id.to_string());
                }
            }
        }

        if let Some(items) = payload.get("Items").and_then(|v| v.as_array()) {
            for item in items {
                if let Some(id) = item.get("ProductId").and_then(|v| v.as_str()) {
                    ids.push(id.to_string());
                }
            }
        }

        if let Some(payload) = payload.get("Payload") {
            if let Some(results) = payload.get("Results").and_then(|v| v.as_array()) {
                for res in results {
                    if let Some(id) = res.get("ProductId").and_then(|v| v.as_str()) {
                        ids.push(id.to_string());
                    }
                }
            }
        }

        ids
    }

    async fn harvest_toplists(
        &self,
        markets: &[String],
        top_sizes: &[usize],
        collections: &[String],
    ) -> Result<Vec<String>> {
        let mut out: Vec<String> = Vec::new();
        let language =
            std::env::var("XBOX_CATEGORY_LANGUAGE").unwrap_or_else(|_| "en-us".to_string());

        for market in markets {
            let categories_payload = match self.fetch_categories(market, &language).await {
                Ok(data) => data,
                Err(err) => {
                    warn!(market=%market, error=%err, "xbox toplist: skipping categories");
                    Value::Null
                }
            };

            let categories = Self::extract_categories(&categories_payload);
            for (cat_id, cat_name) in categories {
                for &top in top_sizes {
                    if top == 0 {
                        continue;
                    }
                    match self.fetch_category_top(market, &cat_id, top).await {
                        Ok(payload) => {
                            let ids = Self::extract_product_ids(&payload);
                            if !ids.is_empty() {
                                info!(market=%market, category=%cat_name, top=top, ids=%ids.len(), "xbox toplist: captured category chart");
                                out.extend(ids);
                            }
                        }
                        Err(err) => {
                            warn!(market=%market, category=%cat_name, top=top, error=%err, "xbox toplist: category fetch failed");
                        }
                    }
                }
            }

            for collection in collections {
                for &top in top_sizes {
                    if top == 0 {
                        continue;
                    }
                    match self.fetch_collection(market, collection, top).await {
                        Ok(payload) => {
                            let ids = Self::extract_product_ids(&payload);
                            if !ids.is_empty() {
                                info!(market=%market, collection=%collection, top=top, ids=%ids.len(), "xbox toplist: captured collection chart");
                                out.extend(ids);
                            }
                        }
                        Err(err) => {
                            warn!(market=%market, collection=%collection, top=top, error=%err, "xbox toplist: collection fetch failed");
                        }
                    }
                }
            }
        }

        // Deduplicate while preserving order of first occurrence
        let mut seen = HashSet::new();
        out.retain(|id| seen.insert(id.clone()));

        Ok(out)
    }

    /// Year-based comprehensive catalog browsing (similar to PlayStation Store approach).
    /// Systematically discovers games by release year across all markets.
    pub async fn browse_by_year_range(
        &self,
        markets: &[String],
        year_min: i32,
        year_max: i32,
        page_size: usize,
        max_pages_per_year: usize,
    ) -> Result<Vec<String>> {
        let mut all_product_ids = HashSet::new();

        for market in markets {
            info!(market=%market, year_min, year_max, "xbox_browse: starting market");

            // Iterate years descending (newest first) to prioritize current catalog
            for year in (year_min..=year_max).rev() {
                match self.browse_year(market, year, page_size, max_pages_per_year).await {
                    Ok(year_product_ids) => {
                        info!(
                            market=%market,
                            year=year,
                            products=%year_product_ids.len(),
                            "xbox_browse: year complete"
                        );
                        all_product_ids.extend(year_product_ids);
                    }
                    Err(err) => {
                        warn!(
                            market=%market,
                            year=year,
                            error=%err,
                            "xbox_browse: year failed"
                        );
                    }
                }
            }
        }

        Ok(all_product_ids.into_iter().collect())
    }

    async fn browse_year(
        &self,
        market: &str,
        year: i32,
        page_size: usize,
        max_pages: usize,
    ) -> Result<Vec<String>> {
        let mut product_ids = Vec::new();
        let language =
            std::env::var("XBOX_BROWSE_LANGUAGE").unwrap_or_else(|_| "en-us".to_string());
        let ms_cv = std::env::var("XBOX_MS_CV").unwrap_or_else(|_| "DGU1mcuYo0WMMp+".to_string());

        let mut page = 0;
        let mut skip = 0;

        loop {
            if page >= max_pages {
                warn!(
                    market=%market,
                    year=year,
                    page=page,
                    "xbox_browse: hit max page limit"
                );
                break;
            }

            let url = format!(
                "https://displaycatalog.mp.microsoft.com/v7.0/products/browse?\
                 market={}&languages={}&skipItems={}&top={}&\
                 categoryId=Games&productFamilyNames=Games&\
                 deviceFamily=Windows.Xbox&\
                 orderBy=releaseDate&sortOrder=desc",
                market, language, skip, page_size
            );

            let mut req = self.client.get(&url).header("MS-CV", &ms_cv);
            if let Some(token) = self.auth_token().await? {
                req = req.bearer_auth(token);
            }

            let pace_ms = Self::pace_ms();
            if let Some(ms) = pace_ms {
                tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
            }

            let response = match req.send().await {
                Ok(r) => r,
                Err(e) => {
                    warn!(
                        market=%market,
                        year=year,
                        page=page,
                        error=%e,
                        "xbox_browse: request failed"
                    );
                    break;
                }
            };

            if !response.status().is_success() {
                warn!(
                    market=%market,
                    year=year,
                    page=page,
                    status=%response.status(),
                    "xbox_browse: non-success status"
                );
                break;
            }

            let payload: Value = match response.json().await {
                Ok(p) => p,
                Err(e) => {
                    warn!(
                        market=%market,
                        year=year,
                        page=page,
                        error=%e,
                        "xbox_browse: json decode failed"
                    );
                    break;
                }
            };

            let page_products = Self::extract_product_ids(&payload);

            if page_products.is_empty() {
                // No more results
                break;
            }

            // Check if we've crossed year boundary by examining release dates
            let mut found_older_year = false;
            if let Some(products) = payload.get("Products").and_then(|v| v.as_array()) {
                for product in products {
                    // Try multiple paths to find release date
                    let date_str = product
                        .get("MarketProperties")
                        .and_then(|v| v.as_array())
                        .and_then(|arr| arr.first())
                        .and_then(|mp| mp.get("OriginalReleaseDate"))
                        .and_then(|v| v.as_str())
                        .or_else(|| {
                            product
                                .get("LocalizedProperties")
                                .and_then(|v| v.as_array())
                                .and_then(|arr| arr.first())
                                .and_then(|lp| lp.get("OriginalReleaseDate"))
                                .and_then(|v| v.as_str())
                        });

                    if let Some(date_str) = date_str {
                        if let Some(release_year) = parse_year_from_date(date_str) {
                            if release_year < year {
                                found_older_year = true;
                                break;
                            }
                        }
                    }
                }
            }

            product_ids.extend(page_products.clone());

            if found_older_year {
                info!(
                    market=%market,
                    year=year,
                    page=page,
                    "xbox_browse: crossed year boundary, stopping"
                );
                break;
            }

            if page_products.len() < page_size {
                // Last page (partial results)
                break;
            }

            page += 1;
            skip += page_size;
        }

        Ok(product_ids)
    }

    /// Execute one pass of fetching & ingesting prices.
    pub async fn run_once(&self, db: &Db, opts: XboxOptions) -> Result<usize> {
        if opts.product_ids.is_empty() {
            warn!("XBOX_PRODUCT_IDS empty; skipping");
            return Ok(0);
        }

        // Ensure base domain entities
        let provider_id =
            ensure_provider(db, "microsoft_store", "storefront", Some("xbox-store")).await?;
        let retailer_id = ensure_retailer(db, "Microsoft", Some("microsoft")).await?;
        let (cur_code, cur_name) = currency_for_market(&opts.market);
        let mu = currency_minor_unit(cur_code);
        let currency_id = ensure_currency(db, cur_code, cur_name, mu).await?;
        let country_code2 = opts.market.to_ascii_uppercase();
        let country_id = ensure_country(db, &country_code2, &country_code2, currency_id).await?;
        let juris_id = ensure_national_jurisdiction(db, country_id).await?;
        let mut entity_cache = ProviderEntityCache::new(db.clone());

        // ingest run start
        let run_id = ingest_run_start(
            db,
            provider_id,
            Some(&opts.market),
            Some(serde_json::json!({"language": opts.language})),
        )
        .await?;

        let mut post_summary = PostIngestSummary::default();

        // Optional JSON dumping
        let dump_json = std::env::var("XBOX_DUMP_JSON")
            .map(|v| (v == "1" || v.eq_ignore_ascii_case("true")))
            .unwrap_or(false);
        let dump_dir =
            std::env::var("XBOX_DUMP_DIR").unwrap_or_else(|_| "exports/xbox".to_string());
        let run_dir: Option<PathBuf> = if dump_json {
            let ts = chrono::Utc::now().format("%Y%m%d_%H%M%S");
            let dir = PathBuf::from(dump_dir).join(format!("run_{}", ts));
            let _ = fs::create_dir_all(&dir);
            Some(dir)
        } else {
            None
        };
        let mut ndjson_file: Option<std::fs::File> = if let Some(dir) = &run_dir {
            let path = dir.join("logs.ndjson");
            OpenOptions::new().create(true).append(true).open(path).ok()
        } else {
            None
        };

        let mut total_products = 0usize;
        let mut prices_written: i64 = 0;
        let mut had_error = false;
        let chunk_size: usize = std::env::var("XBOX_CHUNK_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(30)
            .max(1);
        for (chunk_idx, chunk) in opts.product_ids.chunks(chunk_size).enumerate() {
            let url = format!(
                "https://displaycatalog.mp.microsoft.com/v7.0/products?bigIds={}&market={}&languages={}",
                chunk.join(","),
                opts.market,
                opts.language
            );
            let (max_retries, backoff_ms) = Self::retry_policy();
            let pace_ms = Self::pace_ms();
            let mut tries: u32 = 0;
            let resp = loop {
                tries += 1;
                if let Some(ms) = pace_ms {
                    tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
                }
                let request = self.build_request(&url, &opts).await?;
                match request.send().await {
                    Ok(r) => {
                        if r.status().as_u16() == 429 {
                            let mut sleep_ms: u64 = std::env::var("XBOX_CHUNK_SLEEP_MS")
                                .ok()
                                .and_then(|s| s.parse().ok())
                                .unwrap_or(backoff_ms);
                            if let Some(ra) = r
                                .headers()
                                .get("Retry-After")
                                .and_then(|h| h.to_str().ok())
                                .and_then(|s| s.parse::<u64>().ok())
                            {
                                sleep_ms = (ra * 1000).max(sleep_ms);
                            }
                            warn!(chunk=%chunk_idx, sleep_ms, "xbox 429 throttled; sleeping before retry");
                            tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;
                            if tries <= max_retries {
                                continue;
                            }
                            break r;
                        } else {
                            break r;
                        }
                    }
                    Err(e) => {
                        if tries <= max_retries {
                            tokio::time::sleep(std::time::Duration::from_millis(
                                backoff_ms.saturating_mul(tries as u64),
                            ))
                            .await;
                            continue;
                        }
                        had_error = true;
                        tracing::error!(error=%e, url, "xbox http error");
                        if let Some(f) = ndjson_file.as_mut() {
                            let _ = writeln!(
                                f,
                                "{}",
                                serde_json::json!({"ts": chrono::Utc::now(), "chunk": chunk_idx, "status":"http_error", "error": e.to_string(), "ids": chunk})
                            );
                        }
                        continue;
                    }
                }
            };
            let resp = match resp.error_for_status() {
                Ok(ok) => ok,
                Err(e) => {
                    had_error = true;
                    tracing::error!(error=%e, url, "xbox non-success status");
                    if let Some(f) = ndjson_file.as_mut() {
                        let _ = writeln!(
                            f,
                            "{}",
                            serde_json::json!({"ts": chrono::Utc::now(), "chunk": chunk_idx, "status":"bad_status", "error": e.to_string(), "ids": chunk})
                        );
                    }
                    continue;
                }
            };
            let body: Value = match resp.json().await {
                Ok(b) => b,
                Err(e) => {
                    had_error = true;
                    tracing::error!(error=%e, url, "xbox json decode failed");
                    if let Some(f) = ndjson_file.as_mut() {
                        let _ = writeln!(
                            f,
                            "{}",
                            serde_json::json!({"ts": chrono::Utc::now(), "chunk": chunk_idx, "status":"json_error", "error": e.to_string(), "ids": chunk})
                        );
                    }
                    continue;
                }
            };
            if let Some(dir) = &run_dir {
                // Save raw JSON for this chunk
                let first = chunk.get(0).cloned().unwrap_or_else(|| "chunk".to_string());
                let path = dir.join(format!("chunk_{:04}_{}.json", chunk_idx, first));
                if let Ok(mut f) = OpenOptions::new().create(true).write(true).open(path) {
                    let _ = write!(
                        f,
                        "{}",
                        serde_json::to_string_pretty(&body).unwrap_or_else(|_| "{}".into())
                    );
                }
            }
            let products = body
                .get("Products")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            if products.is_empty() {
                warn!(chunk=%chunk_idx, ids=?chunk, market=%opts.market, language=%opts.language, "xbox products array empty for chunk (possible invalid bigIds)");
            }

            let mut price_rows: Vec<PriceRow> = Vec::new();
            for p in &products {
                let pid = p.get("ProductId").and_then(|v| v.as_str()).unwrap_or("");
                if pid.is_empty() {
                    continue;
                }
                let localized = p
                    .get("LocalizedProperties")
                    .and_then(|v| v.as_array())
                    .and_then(|arr| arr.get(0));
                let title = localized
                    .and_then(|lp| {
                        lp.get("ProductTitle")
                            .or_else(|| lp.get("Title"))
                            .and_then(|v| v.as_str())
                    })
                    .unwrap_or(pid);
                let synopsis = localized.and_then(|lp| {
                    first_clean_string(&[
                        lp.get("ShortDescription").and_then(|v| v.as_str()),
                        lp.get("Description").and_then(|v| v.as_str()),
                        lp.get("ProductDescription").and_then(|v| v.as_str()),
                    ])
                });
                let publisher = localized
                    .and_then(|lp| lp.get("PublisherName").and_then(|v| v.as_str()))
                    .map(|s| s.to_string());
                let mut genres: Vec<String> = Vec::new();
                if let Some(props) = p.get("Properties") {
                    if let Some(cat) = props.get("Category").and_then(|v| v.as_str()) {
                        if !cat.trim().is_empty() {
                            genres.push(cat.trim().to_string());
                        }
                    }
                    if let Some(subcat) = props.get("SubCategory") {
                        match subcat {
                            Value::String(s) => {
                                if !s.trim().is_empty() {
                                    genres.push(s.trim().to_string());
                                }
                            }
                            Value::Array(arr) => {
                                for val in arr {
                                    if let Some(s) = val.as_str() {
                                        if !s.trim().is_empty() {
                                            genres.push(s.trim().to_string());
                                        }
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    if let Some(cat_arr) = props.get("Categories").and_then(|v| v.as_array()) {
                        for val in cat_arr {
                            if let Some(s) = val.as_str() {
                                if !s.trim().is_empty() {
                                    genres.push(s.trim().to_string());
                                }
                            }
                        }
                    }
                }
                if let Some(mps) = p.get("MarketProperties").and_then(|v| v.as_array()) {
                    for mp in mps {
                        if let Some(gen_arr) = mp.get("Genres").and_then(|v| v.as_array()) {
                            for g in gen_arr {
                                if let Some(s) = g.as_str() {
                                    if !s.trim().is_empty() {
                                        genres.push(s.trim().to_string());
                                    }
                                }
                            }
                        }
                    }
                }
                genres.sort();
                genres.dedup();
                let mut content_ratings: Vec<String> = Vec::new();
                if let Some(mps) = p.get("MarketProperties").and_then(|v| v.as_array()) {
                    for mp in mps {
                        if let Some(crs) = mp.get("ContentRatings").and_then(|v| v.as_array()) {
                            for cr in crs {
                                if let Some(id) = cr.get("RatingId").and_then(|v| v.as_str()) {
                                    content_ratings.push(id.to_string());
                                }
                            }
                        }
                    }
                }
                content_ratings.sort();
                content_ratings.dedup();
                let release_date = p
                    .get("MarketProperties")
                    .and_then(|v| v.as_array())
                    .and_then(|arr| arr.get(0))
                    .and_then(|mp| mp.get("OriginalReleaseDate"))
                    .and_then(|v| v.as_str())
                    .and_then(parse_original_release_date);
                let slug = slugify(title);
                let product_id = entity_cache
                    .ensure_product_named("software", &slug, title)
                    .await?;
                entity_cache.ensure_software_row(product_id).await?;
                let title_id = entity_cache
                    .ensure_video_game_title(product_id, title, Some(&slug))
                    .await?;
                // generic platform stub
                let platform_id = ensure_platform(db, "XBOX", Some("xbox")).await?;
                // Laravel schema: use product_id directly
                let vg_id = entity_cache
                    .ensure_video_game_for_product_laravel(
                        product_id,
                        title,
                        Some(&slug),
                        None,
                        XBOX_PROVIDER_KEY,
                    )
                    .await?;
                let sellable_id = entity_cache.ensure_sellable("software", product_id).await?;
                let offer_id = entity_cache
                    .ensure_offer(sellable_id, retailer_id, Some(pid))
                    .await?;
                let oj_id = entity_cache
                    .ensure_offer_jurisdiction(offer_id, juris_id, currency_id)
                    .await?;
                let video_game_source_id = entity_cache
                    .ensure_provider_item(provider_id, pid, Some(p.clone()), true)
                    .await?;
                post_summary.record_provider_item(video_game_source_id);
                link_provider_offer(db, video_game_source_id, offer_id, Some(0.85)).await?;
                {
                    let synopsis_ref = synopsis.as_deref();
                    let genre_opt: Option<&Vec<String>> = if genres.is_empty() {
                        None
                    } else {
                        Some(&genres)
                    };
                    let metadata_patch = if content_ratings.is_empty() && publisher.is_none() {
                        None
                    } else {
                        Some(json!({
                            "xbox": {
                                "content_ratings": content_ratings.clone(),
                                "publisher": publisher.clone(),
                            }
                        }))
                    };
                    if synopsis_ref.is_some()
                        || genre_opt.is_some()
                        || metadata_patch.is_some()
                        || release_date.is_some()
                    {
                        let _ = sqlx
                            ::query(
                                "UPDATE video_games
                             SET synopsis = CASE WHEN $1::text IS NOT NULL AND (synopsis IS NULL OR length(synopsis) < length($1)) THEN $1 ELSE synopsis END,
                                 genres = CASE WHEN $2::text[] IS NOT NULL AND array_length($2,1) > 0 THEN $2 ELSE genres END,
                                 metadata = CASE WHEN $3::jsonb IS NOT NULL THEN COALESCE(metadata, '{}'::jsonb) || $3 ELSE metadata END,
                                 release_date = COALESCE(release_date, $4)
                             WHERE id = $5"
                            )
                            .persistent(false)
                            .bind(synopsis_ref)
                            .bind(genre_opt)
                            .bind(metadata_patch.clone().map(Json))
                            .bind(release_date)
                            .bind(vg_id)
                            .execute(&db.pool).await;
                    }
                }
                let mut final_price_minor: Option<i64> = None;
                {
                    // Comprehensive media enrichment using media_filter utility
                    let mut media_stats = MediaStats::new();
                    let include_screenshots = should_include_screenshots();

                    // Extract localized properties
                    if let Some(localized) = p
                        .get("LocalizedProperties")
                        .and_then(|v| v.as_array())
                        .and_then(|arr| arr.get(0))
                    {
                        // Process all images from Images array
                        if let Some(images_array) = localized
                            .get("Images")
                            .and_then(|imgs| imgs.as_array())
                        {
                            // Collect and classify all images
                            let mut classified_images = Vec::new();
                            for img in images_array {
                                if let Some(uri) = img.get("Uri").and_then(|u| u.as_str()) {
                                    // Extract ImagePurpose for classification hint
                                    let purpose = img
                                        .get("ImagePurpose")
                                        .and_then(|p| p.as_str());

                                    let img_type = classify_image_from_url(uri, purpose);
                                    let included = img_type.should_include(include_screenshots);
                                    media_stats.record_image(img_type, included);

                                    if included {
                                        classified_images.push((
                                            uri.to_string(),
                                            img_type,
                                            img.clone(),
                                        ));
                                    }
                                }
                            }

                            // Filter and prioritize images
                            let filtered_images = filter_images(classified_images, include_screenshots);

                            // Ingest filtered images
                            if !filtered_images.is_empty() {
                                let mut tuples = Vec::new();
                                for (url, img_type, _) in &filtered_images {
                                    let media_type = img_type.to_media_type();
                                    tuples.push((
                                        url.clone(),
                                        Some(media_type.to_string()),
                                        Some(media_type.to_string()),
                                        Some(title.to_string()),
                                    ));
                                }

                                let meta = serde_json::json!({
                                    "source": "xbox",
                                    "market": opts.market,
                                    "total_images": filtered_images.len(),
                                });

                                if let Err(err) = ensure_vg_source_media_links_with_meta(
                                    db,
                                    video_game_source_id,
                                    Some(vg_id),
                                    &tuples,
                                    "xbox",
                                    Some(meta),
                                )
                                .await
                                {
                                    if err.to_string().contains("media_kind") {
                                        let simple_urls: Vec<String> =
                                            tuples.iter().map(|(u, _, _, _)| u.clone()).collect();
                                        let _ = ensure_vg_source_media_links(
                                            db,
                                            video_game_source_id,
                                            &simple_urls,
                                        )
                                        .await;
                                    } else {
                                        return Err(err);
                                    }
                                }

                                // Insert into game_media for primary media
                                for (url, img_type, _) in filtered_images.iter().take(5) {
                                    let media_type = img_type.to_media_type();
                                    let pdata = serde_json::json!({"market": opts.market});
                                    let _ = upsert_game_media(
                                        db, vg_id, "xbox", url, media_type, url, pdata,
                                    )
                                    .await;
                                }
                            }
                        }

                        // Process all videos from Videos array
                        if let Some(videos_array) = localized
                            .get("Videos")
                            .and_then(|vids| vids.as_array())
                        {
                            // Collect and classify all videos
                            let mut classified_videos = Vec::new();
                            for vid in videos_array {
                                if let Some(uri) = vid.get("Uri").and_then(|u| u.as_str()) {
                                    // Extract VideoType or Title for classification hint
                                    let video_type_hint = vid
                                        .get("VideoType")
                                        .and_then(|vt| vt.as_str())
                                        .or_else(|| vid.get("Title").and_then(|t| t.as_str()));

                                    let vid_type = classify_video_from_url(uri, video_type_hint);
                                    media_stats.record_video(vid_type);

                                    classified_videos.push((
                                        uri.to_string(),
                                        vid_type,
                                        vid.clone(),
                                    ));
                                }
                            }

                            // Filter and prioritize videos
                            let filtered_videos = filter_videos(classified_videos);

                            // Ingest filtered videos
                            if !filtered_videos.is_empty() {
                                let mut tuples = Vec::new();
                                for (url, vid_type, _) in &filtered_videos {
                                    let media_type = vid_type.to_media_type();
                                    tuples.push((
                                        url.clone(),
                                        Some(media_type.to_string()),
                                        Some(media_type.to_string()),
                                        Some(title.to_string()),
                                    ));
                                }

                                let meta = serde_json::json!({
                                    "source": "xbox",
                                    "market": opts.market,
                                    "total_videos": filtered_videos.len(),
                                });

                                if let Err(err) = ensure_vg_source_media_links_with_meta(
                                    db,
                                    video_game_source_id,
                                    Some(vg_id),
                                    &tuples,
                                    "xbox",
                                    Some(meta),
                                )
                                .await
                                {
                                    if err.to_string().contains("media_kind") {
                                        let simple_urls: Vec<String> =
                                            tuples.iter().map(|(u, _, _, _)| u.clone()).collect();
                                        let _ = ensure_vg_source_media_links(
                                            db,
                                            video_game_source_id,
                                            &simple_urls,
                                        )
                                        .await;
                                    } else {
                                        return Err(err);
                                    }
                                }

                                // Insert into game_media for primary videos
                                for (url, vid_type, _) in filtered_videos.iter().take(3) {
                                    let media_type = vid_type.to_media_type();
                                    let pdata = serde_json::json!({"market": opts.market});
                                    let _ = upsert_game_media(
                                        db, vg_id, "xbox", url, media_type, url, pdata,
                                    )
                                    .await;
                                }
                            }
                        }
                    }

                    // Log media statistics every 100 products
                    if total_products % 100 == 0 && total_products > 0 {
                        media_stats.log_summary("xbox");
                    }
                }

                if let Some((amount_minor, tax_inclusive)) = find_price_minor(&p, mu) {
                    final_price_minor = Some(amount_minor);
                    price_rows.push(PriceRow {
                        offer_jurisdiction_id: oj_id,
                        video_game_source_id: Some(video_game_source_id),
                        recorded_at: Utc::now(),
                        amount_minor,
                        tax_inclusive,
                        fx_minor_per_unit: None,
                        btc_sats_per_unit: None,
                        meta: serde_json::json!({"src":"xbox","market":opts.market,"lang":opts.language}),
                    });
                }
                total_products += 1;

                info!(
                    xbox_product = %pid,
                    title = %title,
                    synopsis = synopsis.as_deref().unwrap_or(""),
                    genres = ?genres,
                    content_ratings = ?content_ratings,
                    final_price_minor = final_price_minor,
                    release_date = ?release_date,
                    "xbox product metadata captured"
                );
            }

            if !price_rows.is_empty() {
                let batch = std::mem::take(&mut price_rows);
                let batch_len = batch.len();
                match ingest_prices(db, batch).await {
                    Ok(res) => {
                        prices_written += res.current_updates.len() as i64;
                        post_summary.record_batch(batch_len, &res);
                    }
                    Err(e) => {
                        had_error = true;
                        tracing::error!(error=%e, "xbox ingest_prices failed");
                    }
                }
            }
            if let Some(f) = ndjson_file.as_mut() {
                let _ = writeln!(
                    f,
                    "{}",
                    serde_json::json!({
                        "ts": chrono::Utc::now(),
                        "chunk": chunk_idx,
                        "status": "ok",
                        "ids": chunk,
                        "products": products.len(),
                        "prices_written": prices_written,
                    })
                );
            }
        }
        let status = if had_error {
            if total_products == 0 {
                "error"
            } else {
                "partial"
            }
        } else {
            "ok"
        };
        if let Err(e) = ingest_run_finish(
            db,
            run_id,
            status,
            total_products as i64,
            prices_written,
            None,
        )
        .await
        {
            tracing::error!(error=%e, run_id, "ingest_run_finish failed");
        }
        info!(count=total_products, market=%opts.market, language=%opts.language, prices_written, status, "xbox: processed products");
        // Diagnostics: media classification summary (kind distribution + samples)
        // Safe best-effort logging; failures are non-fatal.
        if
            let Ok(media_counts) = sqlx
                ::query(
                    "SELECT kind::text, COUNT(*)::bigint AS cnt FROM vg_source_media_links WHERE video_game_source_id IN (SELECT id FROM provider_items WHERE provider_id=$1) GROUP BY kind ORDER BY cnt DESC"
                )
                .bind(provider_id)
                .fetch_all(&db.pool).await
        {
            for row in media_counts {
                let kind: Option<String> = row.try_get("kind").ok();
                let cnt: i64 = row.try_get("cnt").unwrap_or(0);
                info!(media_kind=?kind.unwrap_or_else(||"<null>".into()), count=cnt, "xbox media kind count");
            }
        }
        if
            let Ok(samples) = sqlx
                ::query(
                    "SELECT kind::text, title, url FROM vg_source_media_links WHERE video_game_source_id IN (SELECT id FROM provider_items WHERE provider_id=$1) ORDER BY id DESC LIMIT 5"
                )
                .bind(provider_id)
                .fetch_all(&db.pool).await
        {
            for r in samples {
                let kind: Option<String> = r.try_get("kind").ok();
                let title: Option<String> = r.try_get("title").ok();
                let url: String = r.try_get("url").unwrap_or_default();
                info!(media_kind=?kind.unwrap_or_else(||"<null>".into()), media_title=?title.unwrap_or_default(), url=%url, "xbox media sample");
            }
        }
        post_summary.verify(db, provider_id).await?;
        info!(
            provider_id,
            price_rows = post_summary.total_price_rows_written,
            provider_items = post_summary.video_game_source_ids.len(),
            offer_jurisdictions = post_summary.offer_jurisdiction_ids.len(),
            "xbox provider verification complete"
        );
        Ok(total_products)
    }

    pub async fn run_ingest_cycle(&self, db: &Db) -> Result<()> {
        let mut opts = XboxOptions::default();
        let mut product_ids: Vec<String> = opts.product_ids.clone();

        // Toplist harvesting
        let harvest_toplists = std::env::var("XBOX_HARVEST_TOPLISTS")
            .map(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
            .unwrap_or(true);

        if harvest_toplists {
            let markets = parse_markets_env(&opts.market);
            let top_sizes = parse_top_sizes_env();
            let collections = parse_collections_env();
            match self
                .harvest_toplists(&markets, &top_sizes, &collections)
                .await
            {
                Ok(mut ids) => {
                    info!(toplist_products=%ids.len(), "xbox toplist harvesting complete");
                    product_ids.append(&mut ids);
                }
                Err(err) => {
                    warn!(error=%err, "xbox toplist harvesting failed; falling back to env ids only");
                }
            }
        }

        // Year-based browse discovery
        let enable_browse = std::env::var("XBOX_ENABLE_BROWSE")
            .map(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
            .unwrap_or(false);

        if enable_browse {
            let year_min: i32 = std::env::var("XBOX_YEAR_MIN")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(2020);
            let year_max: i32 = std::env::var("XBOX_YEAR_MAX")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(2025);
            let browse_page_size: usize = std::env::var("XBOX_BROWSE_PAGE_SIZE")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(200);
            let browse_max_pages: usize = std::env::var("XBOX_BROWSE_MAX_PAGES")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(100);
            let browse_markets_str = std::env::var("XBOX_BROWSE_MARKETS")
                .unwrap_or_else(|_| opts.market.clone());
            let browse_markets = parse_markets_env(&browse_markets_str);

            info!(
                year_min,
                year_max,
                markets=?browse_markets,
                page_size=browse_page_size,
                max_pages=browse_max_pages,
                "xbox year-based browse discovery enabled"
            );

            match self
                .browse_by_year_range(
                    &browse_markets,
                    year_min,
                    year_max,
                    browse_page_size,
                    browse_max_pages,
                )
                .await
            {
                Ok(mut ids) => {
                    info!(browse_products=%ids.len(), "xbox browse discovery complete");
                    product_ids.append(&mut ids);
                }
                Err(err) => {
                    warn!(error=%err, "xbox browse discovery failed");
                }
            }
        }

        // Add IDs from env file if any
        let env_file_ids = load_product_ids_from_env();
        if !env_file_ids.is_empty() {
             product_ids.extend(env_file_ids);
        }

        if product_ids.is_empty() {
            warn!("xbox product list empty after toplist + browse + env merge; skipping");
            return Ok(());
        }

        product_ids.sort();
        product_ids.dedup();
        info!(total_unique_products=%product_ids.len(), "xbox product discovery complete");
        opts.product_ids = product_ids;

        // Dry-run option: skip database writes and only dump JSON if XBOX_DRY_RUN=1
        if std::env::var("XBOX_DRY_RUN").ok().as_deref() == Some("1") {
            let _ = self.run_dump_only(opts).await?;
        } else {
            let _ = self.run_once(db, opts).await?;
        }
        Ok(())
    }
}

pub fn parse_product_ids(input: &str) -> Vec<String> {
    input
        .split(|c: char| (c == '\n' || c == '\r' || c == ',' || c.is_whitespace()))
        .map(|s| s.trim())
        .filter(|s| !s.is_empty() && !s.starts_with('#'))
        .map(|s| s.to_string())
        .collect()
}

fn load_product_ids_from_env() -> Vec<String> {
    if let Ok(path) = std::env::var("XBOX_PRODUCT_IDS_FILE") {
        if let Ok(contents) = std::fs::read_to_string(&path) {
            let parsed = parse_product_ids(&contents);
            if !parsed.is_empty() {
                return parsed;
            }
        }
    }
    std::env::var("XBOX_PRODUCT_IDS")
        .ok()
        .map(|s| parse_product_ids(&s))
        .unwrap_or_default()
}

fn parse_markets_env(default_market: &str) -> Vec<String> {
    let raw = std::env::var("XBOX_MARKETS").unwrap_or_default();
    let mut markets: Vec<String> = raw
        .split(',')
        .filter_map(|s| {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_ascii_uppercase())
            }
        })
        .collect();

    if markets.is_empty() {
        markets.push(default_market.to_ascii_uppercase());
    }

    markets.sort();
    markets.dedup();
    markets
}

fn parse_top_sizes_env() -> Vec<usize> {
    let raw = std::env::var("XBOX_TOP_SIZES").unwrap_or_else(|_| "20,50,100".to_string());
    let mut sizes: Vec<usize> = raw
        .split(',')
        .filter_map(|s| s.trim().parse::<usize>().ok())
        .filter(|v| *v > 0)
        .collect();
    if sizes.is_empty() {
        sizes = vec![20, 50, 100];
    }
    sizes.sort();
    sizes.dedup();
    sizes
}

fn parse_collections_env() -> Vec<String> {
    let raw = std::env::var("XBOX_COLLECTIONS")
        .unwrap_or_else(|_| "TopPaid,TopFree,BestRated,MostPlayed".to_string());
    let mut cols: Vec<String> = raw
        .split(',')
        .filter_map(|s| {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        })
        .collect();
    if cols.is_empty() {
        cols = vec![
            "TopPaid".to_string(),
            "TopFree".to_string(),
            "BestRated".to_string(),
            "MostPlayed".to_string(),
        ];
    }
    cols.sort();
    cols.dedup();
    cols
}

fn currency_minor_unit(code: &str) -> i16 {
    match code.to_ascii_uppercase().as_str() {
        "JPY" | "KRW" | "VND" | "CLP" | "ISK" | "HUF" => 0,
        "BHD" | "IQD" | "KWD" | "JOD" | "OMR" | "TND" => 3,
        _ => 2,
    }
}

fn currency_for_market(market: &str) -> (&'static str, &'static str) {
    match market.to_ascii_uppercase().as_str() {
        "US" | "CA" | "AU" | "NZ" => ("USD", "US Dollar"),
        "GB" => ("GBP", "British Pound"),
        "DE" | "FR" | "ES" | "IT" | "NL" | "BE" | "PT" | "IE" | "FI" | "GR" | "AT" | "LU"
        | "SI" | "SK" | "LV" | "LT" | "EE" | "MT" | "CY" => ("EUR", "Euro"),
        "JP" => ("JPY", "Japanese Yen"),
        "KR" => ("KRW", "South Korean Won"),
        _ => ("USD", "US Dollar"),
    }
}

fn slugify(s: &str) -> String {
    s.to_lowercase()
        .replace(|c: char| !c.is_ascii_alphanumeric(), "-")
        .trim_matches('-')
        .to_string()
}

fn find_price_minor(v: &Value, minor_unit: i16) -> Option<(i64, bool)> {
    if let Some(arr) = v.get("DisplaySkuAvailabilities").and_then(|a| a.as_array()) {
        for dsa in arr {
            if let Some(lp_arr) = dsa
                .get("Sku")
                .and_then(|s| s.get("LocalizedProperties"))
                .and_then(|x| x.as_array())
            {
                for lp in lp_arr {
                    if let Some(p) = lp.get("Price") {
                        if let Some((amt, ti)) = extract_common_price(p, minor_unit) {
                            return Some((amt, ti));
                        }
                    }
                }
            }
            if let Some(omd_price) = dsa.get("OrderManagementData").and_then(|o| o.get("Price")) {
                if let Some((amt, ti)) = extract_common_price(omd_price, minor_unit) {
                    return Some((amt, ti));
                }
            }
        }
    }
    extract_common_price(v, minor_unit)
}

fn extract_common_price(p: &Value, minor_unit: i16) -> Option<(i64, bool)> {
    let candidates = ["ListPrice", "MSRP", "Amount", "CurrentPrice", "Price"];
    for k in candidates {
        if let Some(n) = p.get(k) {
            if let Some(f) = n.as_f64() {
                return Some(((f * (10f64).powi(minor_unit as i32)).round() as i64, true));
            }
            if let Some(s) = n.as_str() {
                if let Ok(f) = s.parse::<f64>() {
                    return Some(((f * (10f64).powi(minor_unit as i32)).round() as i64, true));
                }
            }
        }
    }
    None
}

fn first_clean_string(candidates: &[Option<&str>]) -> Option<String> {
    for cand in candidates {
        if let Some(val) = cand {
            let cleaned = strip_html_tags(val);
            if !cleaned.is_empty() {
                return Some(cleaned);
            }
        }
    }
    None
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

fn parse_original_release_date(raw: &str) -> Option<NaiveDate> {
    const FORMATS: [&str; 2] = ["%Y-%m-%dT%H:%M:%S%.fZ", "%Y-%m-%dT%H:%M:%S%.f"];
    for fmt in FORMATS {
        if let Ok(dt) = NaiveDateTime::parse_from_str(raw, fmt) {
            return Some(dt.date());
        }
    }
    if let Ok(date) = NaiveDate::parse_from_str(raw, "%Y-%m-%d") {
        return Some(date);
    }
    None
}

fn parse_year_from_date(date_str: &str) -> Option<i32> {
    // Parse ISO 8601 date: "2024-03-15T00:00:00Z"
    date_str.split('-').next()?.parse().ok()
}

pub async fn run_from_env(db: &Db) -> Result<()> {
    let prov = XboxProvider::new()?;
    run_with_provider(db, prov).await
}

pub async fn run_with_provider(db: &Db, prov: XboxProvider) -> Result<()> {
    let mut opts = XboxOptions::default();
    let mut product_ids: Vec<String> = opts.product_ids.clone();

    // Toplist harvesting (existing behavior)
    let harvest_toplists = std::env::var("XBOX_HARVEST_TOPLISTS")
        .map(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(true);

    if harvest_toplists {
        let markets = parse_markets_env(&opts.market);
        let top_sizes = parse_top_sizes_env();
        let collections = parse_collections_env();
        match prov
            .harvest_toplists(&markets, &top_sizes, &collections)
            .await
        {
            Ok(mut ids) => {
                info!(toplist_products=%ids.len(), "xbox toplist harvesting complete");
                product_ids.append(&mut ids);
            }
            Err(err) => {
                warn!(error=%err, "xbox toplist harvesting failed; falling back to env ids only");
            }
        }
    }

    // Year-based browse discovery (NEW - similar to PlayStation Store)
    let enable_browse = std::env::var("XBOX_ENABLE_BROWSE")
        .map(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(false);

    if enable_browse {
        let year_min: i32 = std::env::var("XBOX_YEAR_MIN")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(2020);
        let year_max: i32 = std::env::var("XBOX_YEAR_MAX")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(2025);
        let browse_page_size: usize = std::env::var("XBOX_BROWSE_PAGE_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(200);
        let browse_max_pages: usize = std::env::var("XBOX_BROWSE_MAX_PAGES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(100);
        let browse_markets_str = std::env::var("XBOX_BROWSE_MARKETS")
            .unwrap_or_else(|_| opts.market.clone());
        let browse_markets = parse_markets_env(&browse_markets_str);

        info!(
            year_min,
            year_max,
            markets=?browse_markets,
            page_size=browse_page_size,
            max_pages=browse_max_pages,
            "xbox year-based browse discovery enabled"
        );

        match prov
            .browse_by_year_range(
                &browse_markets,
                year_min,
                year_max,
                browse_page_size,
                browse_max_pages,
            )
            .await
        {
            Ok(mut ids) => {
                info!(browse_products=%ids.len(), "xbox browse discovery complete");
                product_ids.append(&mut ids);
            }
            Err(err) => {
                warn!(error=%err, "xbox browse discovery failed");
            }
        }
    }

    if product_ids.is_empty() {
        warn!("xbox product list empty after toplist + browse + env merge; skipping");
        return Ok(());
    }

    product_ids.sort();
    product_ids.dedup();
    info!(total_unique_products=%product_ids.len(), "xbox product discovery complete");
    opts.product_ids = product_ids;

    // Dry-run option: skip database writes and only dump JSON if XBOX_DRY_RUN=1
    if std::env::var("XBOX_DRY_RUN").ok().as_deref() == Some("1") {
        let _ = prov.run_dump_only(opts).await?;
    } else {
        let _ = prov.run_once(db, opts).await?;
    }
    Ok(())
}
