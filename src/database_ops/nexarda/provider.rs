use anyhow::{anyhow, Result};
use chrono::Utc;
use reqwest::{header, Client};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use tokio::sync::Mutex as AsyncMutex;
use tokio::time::{sleep, Duration};
use tracing::{info, warn};

const NEXARDA_PROVIDER_KEY: &str = "nexarda";
const NEXARDA_CATALOG_PROVIDER_KEY: &str = "nexarda-cat";

/// Rust port of the PHP NexardaProvider.
///
/// Core behavior:
/// - Accepts a list of products (id, type, regions, optional fields)
/// - For each region (product-specific + defaults), calls Nexarda Prices API
/// - Builds a compact deals list, one best deal per store_id
/// - Returns structured results + meta
#[derive(Debug, Clone)]
pub struct NexardaProvider {
    base_url: String,
    timeout_secs: u64,
    dynamic_store_cache: Arc<Mutex<HashMap<String, StoreConfig>>>,
    /// Current call-scoped options cache used by dynamic store registration
    current_options: Arc<AsyncMutex<NexardaOptions>>, // shallow copy, not mutated except overrides read
    http: Client,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NexardaOptions {
    pub products: Vec<Product>,
    pub store_map: HashMap<String, HashMap<String, StoreConfig>>, // store_name_normalized -> currency -> config
    pub base_url: Option<String>,
    pub timeout: Option<u64>,
    pub api_key: Option<String>,
    pub auto_register_stores: Option<bool>,
    pub default_regions: Vec<RegionDefinition>,
    pub dynamic_store_overrides: HashMap<String, HashMap<String, StoreOverride>>, // normalized store -> currency -> override
    pub default_tax_inclusive: Option<bool>,
    pub context: Option<Value>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Product {
    pub id: serde_json::Value, // accept string or number; will serialize into query
    #[serde(default = "default_type_game")]
    pub r#type: String,
    pub title: Option<String>,
    pub slug: Option<String>,
    pub platform: Option<String>,
    pub category: Option<String>,
    #[serde(default)]
    pub regions: Vec<RegionDefinition>,
}

fn default_type_game() -> String {
    "game".to_string()
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RegionDefinition {
    pub currency: Option<String>,
    pub region_code: Option<String>,
    pub store_id: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StoreConfig {
    pub store_id: String,
    pub region_code: String,
    pub currency: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StoreOverride {
    pub store_id: Option<String>,
    pub region_code: Option<String>,
    pub tax_inclusive: Option<bool>,
    pub retailer: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DealsResponse {
    pub results: Vec<GameDeals>,
    pub meta: HashMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GameDeals {
    pub game: GameDescriptor,
    pub deals: Vec<Deal>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GameDescriptor {
    pub title: String,
    pub slug: String,
    pub platform: String,
    pub category: String,
    pub metadata: HashMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Deal {
    pub deal_id: String,
    pub store_id: String,
    pub sale_price: f64,
    pub normal_price: f64,
    pub currency: String,
    pub region_code: String,
    pub last_change: i64,
    pub extras: HashMap<String, Value>,
}

impl NexardaProvider {
    pub fn new(base_url: Option<&str>, timeout_secs: Option<u64>) -> Result<Self> {
        let base_url = base_url
            .unwrap_or("https://www.nexarda.com/api/v3")
            .trim_end_matches('/')
            .to_string();
        let timeout_secs = timeout_secs.unwrap_or(15);
        let http = Client::builder()
            .user_agent("NexardaProvider/1.0")
            .timeout(Duration::from_secs(timeout_secs))
            .build()?;

        Ok(Self {
            base_url,
            timeout_secs,
            dynamic_store_cache: Arc::new(Mutex::new(HashMap::new())),
            current_options: Arc::new(AsyncMutex::new(NexardaOptions::default())),
            http,
        })
    }

    pub async fn fetch_deals(&self, options: NexardaOptions) -> Result<DealsResponse> {
        // Save current options snapshot for dynamic store registration
        {
            let mut guard = self.current_options.lock().await;
            *guard = options.clone();
        }

        let products: Vec<Product> = options
            .products
            .iter()
            .filter(|p| !p.id.is_null())
            .cloned()
            .collect();

        if products.is_empty() {
            let mut meta = HashMap::new();
            meta.insert("provider".to_string(), json!("nexarda"));
            meta.insert("generated_at".to_string(), json!(Utc::now().to_rfc3339()));
            meta.insert("product_count".to_string(), json!(0));
            meta.insert(
                "message".to_string(),
                json!("No NEXARDA products configured for ingestion."),
            );
            return Ok(DealsResponse {
                results: vec![],
                meta,
            });
        }

        // Normalize store map
        let mut store_map = normalize_store_map(&options.store_map);

        let base_url = options
            .base_url
            .clone()
            .unwrap_or_else(|| self.base_url.clone());
        let timeout = options.timeout.unwrap_or(self.timeout_secs);
        let api_key = options.api_key.clone();

        let mut results: Vec<GameDeals> = Vec::new();

        for product in products {
            let regions = self.resolve_regions(&product, &options);
            if regions.is_empty() {
                continue;
            }

            let mut info_context: HashMap<String, Value> = HashMap::new();
            let mut all_deals: Vec<Deal> = Vec::new();

            for region in regions {
                let payload = self
                    .request_prices(
                        &base_url,
                        timeout,
                        api_key.as_deref(),
                        &product.r#type,
                        &product.id,
                        &region.currency,
                    )
                    .await?;

                if info_context.is_empty() {
                    if let Some(obj) = payload.get("info").and_then(|v| v.as_object()) {
                        info_context = obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                    }
                }

                let deals = self.build_deals_from_offers(
                    &payload,
                    &region,
                    &mut store_map,
                    &product,
                    &options,
                )?;
                all_deals.extend(deals);
            }

            // keep best deal per store_id
            let best_by_store = best_deal_per_store(&all_deals);
            if best_by_store.is_empty() {
                continue;
            }

            let info =
                self.build_game_descriptor(&product, &best_by_store, &options, &info_context);
            results.push(GameDeals {
                game: info,
                deals: best_by_store,
            });
        }

        let mut meta = HashMap::new();
        meta.insert("provider".to_string(), json!("nexarda"));
        meta.insert("generated_at".to_string(), json!(Utc::now().to_rfc3339()));
        meta.insert("product_count".to_string(), json!(options.products.len()));

        Ok(DealsResponse { results, meta })
    }

    fn resolve_regions(&self, product: &Product, options: &NexardaOptions) -> Vec<RegionConfig> {
        let mut regions = Vec::new();
        for r in &product.regions {
            if let Some(rc) = normalize_region_definition(r, None) {
                regions.push(rc);
            }
        }
        for r in &options.default_regions {
            if let Some(rc) = normalize_region_definition(r, None) {
                regions.push(rc);
            }
        }

        // filter missing, unique by currency
        let mut seen = HashSet::new();
        regions
            .into_iter()
            .filter(|r| !r.currency.is_empty() && !r.region_code.is_empty())
            .filter(|r| seen.insert(r.currency.clone()))
            .collect()
    }

    fn build_game_descriptor(
        &self,
        product: &Product,
        deals: &Vec<Deal>,
        options: &NexardaOptions,
        info_ctx: &HashMap<String, Value>,
    ) -> GameDescriptor {
        let mut metadata: HashMap<String, Value> = HashMap::new();
        metadata.insert("source".into(), json!("nexarda"));
        metadata.insert("nexarda_id".into(), product.id.clone());
        let resolved_slug = self.resolve_slug(product, info_ctx);
        if !resolved_slug.is_empty() {
            metadata.insert("nexarda_slug".into(), json!(resolved_slug));
        }
        let currencies: Vec<String> = deals
            .iter()
            .map(|d| d.currency.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        metadata.insert("currencies".into(), json!(currencies));
        let regions: Vec<String> = deals
            .iter()
            .map(|d| d.region_code.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        metadata.insert("region_codes".into(), json!(regions));
        if let Some(ctx) = options.context.clone() {
            metadata.insert("ingest_context".into(), ctx);
        }
        if let Some(v) = info_ctx.get("cover") {
            metadata.insert("cover".into(), v.clone());
        }
        if let Some(v) = info_ctx.get("banner") {
            metadata.insert("banner".into(), v.clone());
        }
        if let Some(v) = info_ctx.get("release") {
            metadata.insert("release_timestamp".into(), v.clone());
        }

        GameDescriptor {
            title: product
                .title
                .clone()
                .or_else(|| {
                    info_ctx
                        .get("name")
                        .and_then(|v| v.as_str().map(|s| s.to_string()))
                })
                .unwrap_or_else(|| "Unknown Title".into()),
            slug: product
                .slug
                .clone()
                .unwrap_or_else(|| self.resolve_slug(product, info_ctx)),
            platform: product.platform.clone().unwrap_or_else(|| "Unknown".into()),
            category: product.category.clone().unwrap_or_else(|| "Game".into()),
            metadata,
        }
    }

    fn resolve_slug(&self, product: &Product, info_ctx: &HashMap<String, Value>) -> String {
        if let Some(slug) = product.slug.clone() {
            return slug;
        }
        if let Some(info_slug) = info_ctx.get("slug").and_then(|v| v.as_str()) {
            return slugify(&info_slug.replace(['(', ')'], ""));
        }
        slugify(product.title.as_deref().unwrap_or("unknown"))
    }

    fn build_deals_from_offers(
        &self,
        payload: &Value,
        region: &RegionConfig,
        store_map: &mut HashMap<String, HashMap<String, StoreConfig>>,
        product: &Product,
        options: &NexardaOptions,
    ) -> Result<Vec<Deal>> {
        let offers = payload.pointer("/prices/list").and_then(|v| v.as_array());
        if offers.is_none() {
            return Ok(vec![]);
        }
        let offers = offers.unwrap();
        if offers.is_empty() {
            return Ok(vec![]);
        }

        let currency = region.currency.to_uppercase();
        let mut out: Vec<Deal> = Vec::new();

        for offer in offers {
            if !offer.is_object() {
                continue;
            }
            let price = offer.get("price").and_then(|v| v.as_f64()).unwrap_or(0.0);
            if price <= 0.0 {
                continue;
            }

            let store_name = offer
                .pointer("/store/name")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let store_cfg =
                self.resolve_store_config(store_name, region, store_map, &currency, options);
            if store_cfg.store_id.is_empty() {
                continue;
            }

            let sale_price = (price * 100.0).round() / 100.0; // round to 2dp
            if sale_price <= 0.0 {
                continue;
            }

            let normal_price = offer
                .pointer("/coupon/price_without")
                .and_then(|v| v.as_f64())
                .or_else(|| payload.pointer("/prices/highest").and_then(|v| v.as_f64()))
                .unwrap_or(sale_price);
            let normal_price = (normal_price * 100.0).round() / 100.0;

            let deal_id = format!(
                "nexarda:{}:{}:{}:{}",
                &product.r#type,
                product.id,
                currency.to_lowercase(),
                slugify(store_name)
            );

            let mut extras = HashMap::new();
            if let Some(url) = offer.get("url").and_then(|v| v.as_str()) {
                extras.insert("offer_url".into(), json!(url));
            }
            if let Some(store_obj) = offer.get("store") {
                extras.insert("store".into(), store_obj.clone());
            }
            if let Some(max_discount) = payload.pointer("/prices/max_discount") {
                extras.insert("max_discount".into(), max_discount.clone());
            }
            if let Some(list) = payload.pointer("/prices/list").and_then(|v| v.as_array()) {
                extras.insert("offers_considered".into(), json!(list.len()));
            }
            extras.insert(
                "price_text".into(),
                json!(format!("{} {:.2}", currency, sale_price)),
            );
            extras.insert("is_free".into(), json!(false));

            out.push(Deal {
                deal_id,
                store_id: store_cfg.store_id.clone(),
                sale_price,
                normal_price,
                currency: store_cfg.currency.clone(),
                region_code: store_cfg.region_code.clone(),
                last_change: Utc::now().timestamp(),
                extras,
            });
        }

        Ok(out)
    }

    fn resolve_store_config(
        &self,
        store_name: &str,
        region: &RegionConfig,
        store_map: &mut HashMap<String, HashMap<String, StoreConfig>>,
        currency: &str,
        options: &NexardaOptions,
    ) -> StoreConfig {
        let normalized_name = store_name.trim().to_lowercase();
        let currency_up = currency.to_uppercase();

        if !normalized_name.is_empty() {
            if let Some(by_cur) = store_map.get(&normalized_name) {
                if let Some(cfg) = by_cur.get(&currency_up) {
                    return cfg.clone();
                }
            }
        }

        let auto_register = options.auto_register_stores.unwrap_or(true);
        if !normalized_name.is_empty() && auto_register {
            return self.register_dynamic_store(
                store_name,
                &normalized_name,
                &currency_up,
                region,
                store_map,
                options,
            );
        }

        StoreConfig {
            store_id: region.store_id.clone(),
            region_code: region.region_code.clone(),
            currency: currency_up,
        }
    }

    fn register_dynamic_store(
        &self,
        store_name: &str,
        normalized_name: &str,
        currency: &str,
        region: &RegionConfig,
        store_map: &mut HashMap<String, HashMap<String, StoreConfig>>,
        options: &NexardaOptions,
    ) -> StoreConfig {
        let cache_key = format!("{}::{}", normalized_name, currency);

        if let Some(cfg) = self
            .dynamic_store_cache
            .lock()
            .unwrap()
            .get(&cache_key)
            .cloned()
        {
            return cfg;
        }

        let store_slug = if !store_name.is_empty() {
            slugify(store_name)
        } else {
            "nexarda-store".into()
        };
        let overrides_for_store = options.dynamic_store_overrides.get(normalized_name);
        let mut overrides = overrides_for_store
            .and_then(|m| m.get(currency))
            .cloned()
            .unwrap_or_default();

        let default_tax_inclusive = options.default_tax_inclusive.unwrap_or(true);

        let store_id = overrides
            .store_id
            .take()
            .unwrap_or_else(|| format!("nexarda_{}_{}", store_slug, currency.to_lowercase()));
        let region_code = overrides
            .region_code
            .take()
            .unwrap_or_else(|| region.region_code.clone());
        let _tax_inclusive = overrides.tax_inclusive.unwrap_or(default_tax_inclusive);
        let _retailer_label = overrides
            .retailer
            .unwrap_or_else(|| format!("{} (Via NEXARDA)", store_name));

        let cfg = StoreConfig {
            store_id: store_id.clone(),
            region_code: region_code.clone(),
            currency: currency.to_string(),
        };

        let entry = store_map.entry(normalized_name.to_string()).or_default();
        entry.insert(currency.to_string(), cfg.clone());

        self.dynamic_store_cache
            .lock()
            .unwrap()
            .insert(cache_key, cfg.clone());

        cfg
    }

    async fn request_prices(
        &self,
        base_url: &str,
        _timeout: u64,
        api_key: Option<&str>,
        r#type: &str,
        id: &serde_json::Value,
        currency: &str,
    ) -> Result<Value> {
        let mut query: Vec<(String, String)> = vec![
            ("type".into(), r#type.to_string()),
            (
                "id".into(),
                match id {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                },
            ),
            ("currency".into(), currency.to_string()),
        ];
        if let Some(k) = api_key {
            if !k.is_empty() {
                query.push(("key".into(), k.to_string()));
            }
        }

        let mut headers = header::HeaderMap::new();
        headers.insert(
            header::ACCEPT,
            header::HeaderValue::from_static("application/json"),
        );
        if let Some(k) = api_key {
            headers.insert(
                "X-Api-Key",
                header::HeaderValue::from_str(k).unwrap_or(header::HeaderValue::from_static("")),
            );
        }

        let url = format!("{}/prices", base_url.trim_end_matches('/'));

        // simple retry with env-configurable pacing and backoff
        let mut attempt = 0;
        let max_attempts: u32 = std::env::var("NEXARDA_MAX_RETRIES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(3);
        let backoff_ms_base: u64 = std::env::var("NEXARDA_BACKOFF_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(200);
        // pacing: prefer RPM over RPS
        let pace_ms: Option<u64> = std::env::var("NEXARDA_REQS_PER_MIN")
            .ok()
            .and_then(|s| s.parse::<u32>().ok())
            .and_then(|rpm| {
                if rpm > 0 {
                    Some(60_000u64 / (rpm as u64))
                } else {
                    None
                }
            })
            .or_else(|| {
                std::env::var("NEXARDA_RPS")
                    .ok()
                    .and_then(|s| s.parse::<f32>().ok())
                    .and_then(|rps| {
                        if rps > 0.0 {
                            Some((1000.0 / rps) as u64)
                        } else {
                            None
                        }
                    })
            });
        loop {
            attempt += 1;
            if let Some(ms) = pace_ms {
                sleep(Duration::from_millis(ms)).await;
            }
            let resp = self
                .http
                .get(&url)
                .headers(headers.clone())
                .query(&query)
                .send()
                .await;

            match resp {
                Ok(r) if r.status().is_success() => {
                    let payload = r.json::<Value>().await?;
                    if payload.get("success").and_then(|v| v.as_bool()) == Some(true) {
                        return Ok(payload);
                    } else {
                        return Err(anyhow!("Unexpected NEXARDA response for ID [{}].", id));
                    }
                }
                Ok(r) => {
                    if attempt >= max_attempts {
                        return Err(anyhow!(
                            "NEXARDA price request failed for ID [{}] ({}).",
                            id,
                            r.status()
                        ));
                    }
                }
                Err(e) => {
                    if attempt >= max_attempts {
                        return Err(anyhow!(
                            "NEXARDA price request error for ID [{}]: {}",
                            id,
                            e
                        ));
                    }
                }
            }
            // linear backoff between attempts (can be adjusted to exponential later)
            let sleep_ms = backoff_ms_base.saturating_mul(attempt as u64);
            sleep(Duration::from_millis(sleep_ms)).await;
        }
    }

    /// Ingest fetched deals directly into DB using existing ensure_* + ingest_prices helpers.
    /// This keeps provider self-sufficient while allowing orchestration loop to call one method.
    pub async fn ingest_to_db(
        &self,
        db: &crate::database_ops::db::Db,
        options: NexardaOptions,
    ) -> Result<usize> {
        use crate::database_ops::db::PriceRow;
        use crate::database_ops::ingest_providers::{
            ensure_country, ensure_currency, ensure_national_jurisdiction, ensure_platform,
            ensure_provider, ensure_retailer, ensure_vg_source_media_links_with_meta,
            ingest_prices, link_provider_offer, update_video_game_display_title_and_region,
            PostIngestSummary, ProviderEntityCache,
        };
        use chrono::Utc;

        let provider_id = ensure_provider(
            db,
            "nexarda",
            "pricing_catalog",
            Some(NEXARDA_PROVIDER_KEY),
        )
        .await?;
        let mut entity_cache = ProviderEntityCache::new(db.clone());
        let mut post_summary = PostIngestSummary::default();

        let resp = self.fetch_deals(options.clone()).await?; // cloning for potential reuse
        if resp.results.is_empty() {
            info!(
                product_count = options.products.len(),
                "nexarda ingest returned no results (likely NEXARDA_PRODUCTS not configured or provider returned empty payload)"
            );
            return Ok(0);
        }

        let mut total_prices = 0usize;
        for gd in resp.results {
            let slug = slugify(&gd.game.slug);
            let product_id = entity_cache
                .ensure_product_named("software", &slug, &gd.game.title)
                .await?;
            entity_cache.ensure_software_row(product_id).await?;
            let sellable_id = entity_cache.ensure_sellable("software", product_id).await?;
            let platform_label = gd.game.platform.clone();
            let platform_slug = slugify(&platform_label);
            let platform_id = ensure_platform(db, &platform_label, Some(&platform_slug)).await?;
            let title_id = entity_cache
                .ensure_video_game_title(product_id, &gd.game.title, Some(&slug))
                .await?;
            // Laravel schema: use product_id directly
            let vg_id = entity_cache
                .ensure_video_game_for_product_laravel(
                    product_id,
                    &gd.game.title,
                    Some(&slug),
                    None,
                    NEXARDA_PROVIDER_KEY,
                )
                .await?;

            let mut last_video_game_source_id: Option<i64> = None;
            let mut price_rows: Vec<PriceRow> = Vec::new();
            for deal in gd.deals.iter() {
                // ensure currency / country / jurisdiction
                let cur_code = deal.currency.to_uppercase();
                let minor_unit = match cur_code.as_str() {
                    "JPY" | "KRW" | "VND" | "CLP" | "ISK" | "HUF" => 0,
                    "BHD" | "IQD" | "KWD" | "JOD" | "OMR" | "TND" => 3,
                    _ => 2,
                };
                let currency_id = ensure_currency(db, &cur_code, &cur_code, minor_unit).await?;
                let country_id = ensure_country(
                    db,
                    &deal.region_code.to_uppercase(),
                    &deal.region_code.to_uppercase(),
                    currency_id,
                )
                .await?;
                let juris_id = ensure_national_jurisdiction(db, country_id).await?;
                // Map Nexarda store_id to a concrete retailer and offer per sellable
                let retailer_slug = deal.store_id.to_lowercase();
                let retailer_name = deal
                    .extras
                    .get("store")
                    .and_then(|s| s.get("name"))
                    .and_then(|v| v.as_str())
                    .unwrap_or(&retailer_slug);
                let retailer_id = ensure_retailer(db, retailer_name, Some(&retailer_slug)).await?;
                let offer_id = entity_cache
                    .ensure_offer(sellable_id, retailer_id, None)
                    .await?;
                let oj_id = entity_cache
                    .ensure_offer_jurisdiction(offer_id, juris_id, currency_id)
                    .await?;
                // provider item + linking (deal_id acts as external id)
                let video_game_source_id = entity_cache
                    .ensure_provider_item(
                        provider_id,
                        &deal.deal_id,
                        Some(serde_json::to_value(&deal.extras)?),
                        true,
                    )
                    .await?;
                link_provider_offer(db, video_game_source_id, offer_id, Some(0.9)).await?;
                last_video_game_source_id = Some(video_game_source_id);
                post_summary.record_provider_item(video_game_source_id);

                let recorded_at = Utc::now();
                // store both sale and normal if distinct
                let sale_minor = (deal.sale_price * 100.0).round() as i64; // assume 2dp for now
                price_rows.push(PriceRow {
                    offer_jurisdiction_id: oj_id,
                    video_game_source_id: Some(video_game_source_id),
                    recorded_at,
                    amount_minor: sale_minor,
                    tax_inclusive: true,
                    fx_minor_per_unit: None,
                    btc_sats_per_unit: None,
                    meta: json!({"src":"nexarda","kind":"sale"}),
                });
                if (deal.normal_price - deal.sale_price).abs() > f64::EPSILON {
                    let normal_minor = (deal.normal_price * 100.0).round() as i64;
                    price_rows.push(PriceRow {
                        offer_jurisdiction_id: oj_id,
                        video_game_source_id: Some(video_game_source_id),
                        recorded_at,
                        amount_minor: normal_minor,
                        tax_inclusive: true,
                        fx_minor_per_unit: None,
                        btc_sats_per_unit: None,
                        meta: json!({"src":"nexarda","kind":"normal"}),
                    });
                }
            }
            if !price_rows.is_empty() {
                let batch_len = price_rows.len();
                let ingest_result = ingest_prices(db, price_rows).await?;
                post_summary.record_batch(batch_len, &ingest_result);
                total_prices += 1;
            }
            // Update display_title & region_codes aggregation
            let regions: Vec<String> = gd
                .deals
                .iter()
                .map(|d| d.region_code.to_ascii_uppercase())
                .collect();
            for rc in regions.iter() {
                let _ =
                    update_video_game_display_title_and_region(db, vg_id, &gd.game.title, rc).await;
            }
            // Media ingestion (cover/banner from metadata)
            if let Some(pid) = last_video_game_source_id {
                let mut media_tuples: Vec<(
                    String,
                    Option<String>,
                    Option<String>,
                    Option<String>,
                )> = Vec::new();
                if let Some(cover) = gd.game.metadata.get("cover").and_then(|v| v.as_str()) {
                    media_tuples.push((
                        cover.to_string(),
                        Some("image".into()),
                        Some("cover".into()),
                        Some(gd.game.title.clone()),
                    ));
                }
                if let Some(banner) = gd.game.metadata.get("banner").and_then(|v| v.as_str()) {
                    media_tuples.push((
                        banner.to_string(),
                        Some("image".into()),
                        Some("banner".into()),
                        Some(gd.game.title.clone()),
                    ));
                }
                if !media_tuples.is_empty() {
                    let meta = serde_json::json!({"source":"nexarda","regions": regions});
                    let _ = ensure_vg_source_media_links_with_meta(
                        db,
                        pid,
                        Some(vg_id),
                        &media_tuples,
                        "nexarda",
                        Some(meta),
                    )
                    .await?;
                    // Upsert into game_media
                    for (url, mtype, role, _title) in &media_tuples {
                        let mtype_final = mtype.as_deref().unwrap_or("image");
                        let pdata = serde_json::json!({"role": role, "regions": regions});
                        let _ = crate::database_ops::ingest_providers::upsert_game_media(
                            db,
                            vg_id,
                            "nexarda",
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
        post_summary.verify(db, provider_id).await?;
        Ok(total_prices)
    }

    /// Ingest a pre-fetched Nexarda catalogue JSON file (one-time seed or batch).
    /// Env fallbacks:
    /// - NEXARDA_CATALOGUE_PATH (preferred)
    /// - NEXARDA_CATALOGUE_FILE (legacy)
    /// Defaults to ./nexarda_product_catalogue.json
    pub async fn ingest_catalogue_file(
        db: &crate::database_ops::db::Db,
        path_opt: Option<&str>,
        simple_mode: Option<bool>,
        limit_opt: Option<usize>,
    ) -> anyhow::Result<usize> {
        use crate::database_ops::db::PriceRow;
        use crate::database_ops::ingest_providers::{
            ensure_country, ensure_currency, ensure_national_jurisdiction, ensure_platform,
            ensure_provider, ensure_retailer, ingest_prices, link_provider_offer,
            ProviderEntityCache,
        };
        use chrono::Utc;
        use serde::Deserialize;
        use std::collections::HashMap;
        use std::fs;
        use tracing::{info, warn};

        #[derive(Debug, Deserialize)]
        struct CatalogueRoot {
            success: bool,
            code: Option<String>,
            message: Option<String>,
            games: Vec<CatalogueGame>,
        }
        #[derive(Debug, Deserialize)]
        struct CatalogueGame {
            id: i64,
            name: String,
            slug: String,
            prices: HashMap<String, serde_json::Value>,
            discounts: HashMap<String, serde_json::Value>,
        }

        let path = path_opt
            .map(|s| s.to_string())
            .or_else(|| std::env::var("NEXARDA_CATALOGUE_PATH").ok())
            .or_else(|| std::env::var("NEXARDA_CATALOGUE_FILE").ok())
            .unwrap_or_else(|| "nexarda_product_catalogue.json".into());
        let raw = fs::read_to_string(&path)
            .map_err(|e| anyhow::anyhow!("read catalogue file {}: {}", path, e))?;
        let cat: CatalogueRoot = serde_json::from_str(&raw)
            .map_err(|e| anyhow::anyhow!("parse catalogue json {}: {}", path, e))?;
        if !cat.success {
            warn!(code=?cat.code, msg=?cat.message, "nexarda catalogue marked unsuccessful");
        }
        info!(games = cat.games.len(), path=%path, "nexarda catalogue loaded");

        let provider_id = ensure_provider(
            db,
            "nexarda_catalogue",
            "catalog",
            Some(NEXARDA_CATALOG_PROVIDER_KEY),
        )
        .await?;
        let retailer_id = ensure_retailer(db, "NEXARDA Catalogue", Some("nexarda-cat")).await?;
        let mut entity_cache = ProviderEntityCache::new(db.clone());

        // simple mode (titles only)
        let simple = simple_mode
            .or_else(|| {
                std::env::var("NEXARDA_SIMPLE")
                    .ok()
                    .map(|s| (s == "1" || s.eq_ignore_ascii_case("true")))
            })
            .unwrap_or(false);
        let limit_env: Option<usize> = std::env::var("NEXARDA_CATALOGUE_LIMIT")
            .ok()
            .and_then(|s| s.parse().ok());
        let offset_env: usize = std::env::var("NEXARDA_CATALOGUE_OFFSET")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let limit = limit_opt.or(limit_env);

        if simple {
            let platform_id = ensure_platform(db, "PC", Some("pc")).await?;
            let mut processed = 0usize;
            for (idx, g) in cat.games.iter().enumerate() {
                if idx < offset_env {
                    continue;
                }
                if let Some(lim) = limit {
                    if processed >= lim {
                        info!(limit = lim, "nexarda simple limit reached");
                        break;
                    }
                }
                let name = if g.name.trim().is_empty() {
                    g.slug.clone()
                } else {
                    g.name.clone()
                };
                let slug = slugify(&g.slug);
                let product_id = entity_cache
                    .ensure_product_named("software", &slug, &name)
                    .await?;
                entity_cache.ensure_software_row(product_id).await?;
                let title_id = entity_cache
                    .ensure_video_game_title(product_id, &name, Some(&slug))
                    .await?;
                // Laravel schema: use product_id directly
                let _vg_id = entity_cache
                    .ensure_video_game_for_product_laravel(
                        product_id,
                        &name,
                        Some(&slug),
                        None,
                        NEXARDA_CATALOG_PROVIDER_KEY,
                    )
                    .await?;
                let _pi = entity_cache
                    .ensure_provider_item(
                        provider_id,
                        &format!("nexarda:{}", g.id),
                        Some(serde_json::json!({"orig_slug": g.slug})),
                        true,
                    )
                    .await?;
                processed += 1;
                if processed % 250 == 0 {
                    info!(processed, "nexarda simple progress");
                }
            }
            info!(processed, "nexarda simple ingest complete");
            return Ok(processed);
        }

        let mut price_rows: Vec<PriceRow> = Vec::new();
        let mut processed: usize = 0;
        for (idx, g) in cat.games.iter().enumerate() {
            if idx < offset_env {
                continue;
            }
            if let Some(lim) = limit {
                if processed >= lim {
                    info!(limit = lim, "nexarda catalogue limit reached");
                    break;
                }
            }
            let title_name = if g.name.trim().is_empty() {
                g.slug.clone()
            } else {
                g.name.clone()
            };
            let slug_norm = slugify(&g.slug).trim_start_matches('/').to_string();
            let product_id = entity_cache
                .ensure_product_named("software", &slug_norm, &title_name)
                .await?;
            entity_cache.ensure_software_row(product_id).await?;
            let _title_id = entity_cache
                .ensure_video_game_title(product_id, &title_name, Some(&slug_norm))
                .await?;
            let sellable_id = entity_cache.ensure_sellable("software", product_id).await?;
            let offer_id = entity_cache
                .ensure_offer(sellable_id, retailer_id, None)
                .await?;
            let video_game_source_id = entity_cache
                .ensure_provider_item(
                    provider_id,
                    &format!("nexarda:{}", g.id),
                    Some(serde_json::json!({"orig_slug": g.slug})),
                    true,
                )
                .await?;
            link_provider_offer(db, video_game_source_id, offer_id, Some(0.9)).await?;

            for (ccy_raw, price_val) in &g.prices {
                let ccy = ccy_raw.to_ascii_uppercase();
                if price_val.is_string() && price_val.as_str() == Some("unavailable") {
                    continue;
                }
                let price_f = extract_price_f64_cat(price_val).unwrap_or(0.0);
                if price_f <= 0.0 {
                    continue;
                }
                let minor_unit = currency_minor_unit_cat(&ccy);
                let currency_id = ensure_currency(db, &ccy, &ccy, minor_unit).await?;
                let cc2 = currency_to_country_code_cat(&ccy);
                let country_id = ensure_country(db, &cc2, &cc2, currency_id).await?;
                let juris_id = ensure_national_jurisdiction(db, country_id).await?;
                let oj_id = entity_cache
                    .ensure_offer_jurisdiction(offer_id, juris_id, currency_id)
                    .await?;
                let amount_minor = (price_f * (10f64).powi(minor_unit as i32)).round() as i64;
                price_rows.push(PriceRow {
                    offer_jurisdiction_id: oj_id,
                    video_game_source_id: Some(video_game_source_id),
                    recorded_at: Utc::now(),
                    amount_minor,
                    tax_inclusive: true,
                    fx_minor_per_unit: None,
                    btc_sats_per_unit: None,
                    meta: serde_json::json!({"src":"nexarda_catalogue","discounts": g.discounts.get(ccy_raw)}),
                });
            }
            processed += 1;
            if processed % 250 == 0 {
                info!(
                    processed,
                    price_buffer = price_rows.len(),
                    "nexarda progress"
                );
            }
            if price_rows.len() >= 1000 {
                info!(
                    flush = price_rows.len(),
                    processed, "nexarda flushing price batch"
                );
                ingest_prices(db, std::mem::take(&mut price_rows)).await?;
            }
        }
        if !price_rows.is_empty() {
            info!(
                final_flush = price_rows.len(),
                processed, "nexarda final flush"
            );
            ingest_prices(db, price_rows).await?;
        }
        info!(processed, "nexarda catalogue ingest complete");
        Ok(processed)
    }
}

#[derive(Debug, Clone)]
struct RegionConfig {
    currency: String,
    region_code: String,
    store_id: String,
}

fn normalize_store_map(
    input: &HashMap<String, HashMap<String, StoreConfig>>,
) -> HashMap<String, HashMap<String, StoreConfig>> {
    let mut out: HashMap<String, HashMap<String, StoreConfig>> = HashMap::new();
    for (store_name, currencies) in input.iter() {
        let mut norm_cur: HashMap<String, StoreConfig> = HashMap::new();
        for (cur, cfg) in currencies {
            let code = cur.to_uppercase();
            norm_cur.insert(
                code.clone(),
                StoreConfig {
                    store_id: cfg.store_id.clone(),
                    region_code: cfg.region_code.clone().to_uppercase(),
                    currency: code,
                },
            );
        }
        out.insert(store_name.to_lowercase(), norm_cur);
    }
    out
}

fn normalize_region_definition(
    region: &RegionDefinition,
    default_store_id: Option<String>,
) -> Option<RegionConfig> {
    let currency = region.currency.as_deref().unwrap_or("USD").to_uppercase();
    let region_code = region.region_code.as_deref().unwrap_or("US").to_uppercase();

    let store_id = region
        .store_id
        .clone()
        .or(default_store_id)
        .unwrap_or_else(|| format!("nexarda_{}", currency.to_lowercase()));

    Some(RegionConfig {
        currency,
        region_code,
        store_id,
    })
}

fn best_deal_per_store(deals: &Vec<Deal>) -> Vec<Deal> {
    let mut best: HashMap<String, Deal> = HashMap::new();
    for d in deals {
        best.entry(d.store_id.clone())
            .and_modify(|e| {
                if d.sale_price < e.sale_price {
                    *e = d.clone();
                }
            })
            .or_insert_with(|| d.clone());
    }
    best.into_values().collect()
}

fn slugify(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('-');
        }
    }
    out
}

// Lightweight helpers for catalogue JSON parsing/mapping (mirror of bin logic)
fn extract_price_f64_cat(v: &serde_json::Value) -> Option<f64> {
    if let Some(f) = v.as_f64() {
        return Some(f);
    }
    if let Some(s) = v.as_str() {
        let cleaned = s.replace(',', ".").trim().to_string();
        if let Ok(f) = cleaned.parse::<f64>() {
            return Some(f);
        }
    }
    if let Some(obj) = v.as_object() {
        for key in ["price", "value", "amount"].iter() {
            if let Some(inner) = obj.get(*key) {
                if let Some(f) = inner.as_f64() {
                    return Some(f);
                }
                if let Some(s) = inner.as_str() {
                    let cleaned = s.replace(',', ".").trim().to_string();
                    if let Ok(f) = cleaned.parse::<f64>() {
                        return Some(f);
                    }
                }
            }
        }
    }
    None
}

fn currency_minor_unit_cat(code: &str) -> i16 {
    match code {
        "JPY" | "KRW" | "VND" | "CLP" | "ISK" | "HUF" => 0,
        "BHD" | "IQD" | "KWD" | "JOD" | "OMR" | "TND" => 3,
        _ => 2,
    }
}

fn currency_to_country_code_cat(code: &str) -> String {
    (match code {
        "USD" => "US",
        "EUR" => "DE",
        "GBP" => "GB",
        "JPY" => "JP",
        "CNY" => "CN",
        "TWD" => "TW",
        "HKD" => "HK",
        "KRW" => "KR",
        "AUD" => "AU",
        "NZD" => "NZ",
        "CAD" => "CA",
        "BRL" => "BR",
        "MXN" => "MX",
        "ARS" => "AR",
        "CLP" => "CL",
        "COP" => "CO",
        "PEN" => "PE",
        "INR" => "IN",
        "IDR" => "ID",
        "PLN" => "PL",
        "SEK" => "SE",
        "NOK" => "NO",
        "DKK" => "DK",
        "CHF" => "CH",
        "HUF" => "HU",
        "CZK" => "CZ",
        "TRY" => "TR",
        "ZAR" => "ZA",
        _ => "ZZ",
    })
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slugify_basic() {
        assert_eq!(slugify("Hello World!"), "hello-world-");
        assert_eq!(slugify("(Test) Game"), "-test--game");
    }

    #[test]
    fn normalize_store_map_uppercases_currency() {
        let mut m = HashMap::new();
        let mut by_cur = HashMap::new();
        by_cur.insert(
            "usd".to_string(),
            StoreConfig {
                store_id: "id".into(),
                region_code: "us".into(),
                currency: "usd".into(),
            },
        );
        m.insert("Steam".into(), by_cur);
        let out = normalize_store_map(&m);
        let steam = out.get("steam").unwrap();
        let usd = steam.get("USD").unwrap();
        assert_eq!(usd.currency, "USD");
        assert_eq!(usd.region_code, "US");
    }
}
