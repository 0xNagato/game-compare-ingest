use crate::database_ops::db::{Db, PriceRow};
use crate::database_ops::ingest_providers::{
    ensure_country, ensure_currency, ensure_national_jurisdiction, ensure_platform,
    ensure_provider, ensure_retailer, ingest_prices, PostIngestSummary,
};
use anyhow::{Context, Result};
use chrono::Utc;
use reqwest::Client;
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;
use std::time::Duration as StdDuration;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration, Instant};
use tracing::{info, warn};

/// Xbox Store API wrapper provider
/// Uses lucasromerodb/xbox-store-api endpoints
pub struct XboxStoreProvider {
    client: Client,
    base_url: String,
    rate_limiter: RequestThrottle,
}

#[derive(Debug, Deserialize)]
struct XboxGame {
    #[serde(rename = "ProductId")]
    product_id: Option<String>,
    #[serde(rename = "ProductTitle")]
    product_title: Option<String>,
    #[serde(rename = "LocalizedProperties")]
    localized_properties: Option<Vec<LocalizedProperty>>,
}

#[derive(Debug, Deserialize)]
struct LocalizedProperty {
    #[serde(rename = "ProductTitle")]
    product_title: Option<String>,
    #[serde(rename = "Price")]
    price: Option<Price>,
}

#[derive(Debug, Deserialize)]
struct Price {
    #[serde(rename = "MSRP")]
    msrp: Option<f64>,
    #[serde(rename = "ListPrice")]
    list_price: Option<f64>,
    #[serde(rename = "CurrencyCode")]
    currency_code: Option<String>,
}

impl XboxStoreProvider {
    pub fn new(base_url: String, rate_limiter: RequestThrottle) -> Self {
        let client = Client::builder()
            .timeout(StdDuration::from_secs(30))
            .build()
            .unwrap_or_else(|_| Client::new());

        Self {
            client,
            base_url,
            rate_limiter,
        }
    }

    /// Main entry point for Xbox Store API ingestion
    pub async fn run_from_env(db: &Db) -> Result<()> {
        // Load configuration from environment
        let base_url = std::env::var("XBOX_STORE_API_URL")
            .unwrap_or_else(|_| "http://localhost:3000".to_string());

        let markets: Vec<String> = std::env::var("XBOX_STORE_MARKETS")
            .unwrap_or_else(|_| "US,GB,CA,AU".to_string())
            .split(',')
            .map(|s| s.trim().to_uppercase())
            .filter(|s| !s.is_empty())
            .collect();

        let categories: Vec<String> = std::env::var("XBOX_STORE_CATEGORIES")
            .unwrap_or_else(|_| "TopPaid,TopFree,New".to_string())
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        let rate_limit_settings = RateLimitSettings::from_env(
            "XBOX_STORE_MAX_OPS_PER_WINDOW",
            "XBOX_STORE_WINDOW_SECS",
            100,
            300,
        );
        let rate_limiter = RequestThrottle::new(rate_limit_settings.clone());

        match rate_limit_settings.per_request_interval() {
            Some(interval) => info!(
                max_ops_per_window = rate_limit_settings.max_ops_per_window,
                window_secs = rate_limit_settings.window_secs,
                interval_ms = interval.as_millis(),
                "xbox_store_api: HTTP throttle configured"
            ),
            None => info!(
                "xbox_store_api: HTTP throttle disabled; requests will fire immediately"
            ),
        }

        if markets.is_empty() {
            info!("xbox_store_api: no markets configured, skipping");
            return Ok(());
        }

        if categories.is_empty() {
            info!("xbox_store_api: no categories configured, skipping");
            return Ok(());
        }

        info!(
            markets = ?markets,
            categories = ?categories,
            "xbox_store_api: starting ingestion"
        );

        let provider = Self::new(base_url, rate_limiter);

        // Ensure provider entities exist
        let provider_id = ensure_provider(db, "xbox_store_api", "storefront", Some("xbox-api"))
            .await
            .context("Failed to ensure provider")?;

        let retailer_id = ensure_retailer(db, "Xbox Store", Some("xbox"))
            .await
            .context("Failed to ensure retailer")?;

        let _platform_id = ensure_platform(db, "Xbox", Some("xbox"))
            .await
            .context("Failed to ensure platform")?;

        // Entity cache for reducing DB lookups
        let mut post_summary = PostIngestSummary::default();

        // Process each market
        for market in &markets {
            info!(market = %market, "xbox_store_api: processing market");

            // Get currency for market
            let currency_code = provider
                .get_currency_for_market(market)
                .await
                .unwrap_or_else(|| "USD".to_string());

            let currency_id = match ensure_currency(db, &currency_code, &currency_code, 2).await {
                Ok(id) => id,
                Err(e) => {
                    warn!(
                        market = %market,
                        currency = %currency_code,
                        error = %e,
                        "xbox_store_api: failed to ensure currency, skipping market"
                    );
                    continue;
                }
            };

            // Ensure country and jurisdiction
            let country_id = match ensure_country(db, market, market, currency_id).await {
                Ok(id) => id,
                Err(e) => {
                    warn!(
                        market = %market,
                        error = %e,
                        "xbox_store_api: failed to ensure country, skipping market"
                    );
                    continue;
                }
            };

            let jurisdiction_id = match ensure_national_jurisdiction(db, country_id).await {
                Ok(id) => id,
                Err(e) => {
                    warn!(
                        market = %market,
                        error = %e,
                        "xbox_store_api: failed to ensure jurisdiction, skipping market"
                    );
                    continue;
                }
            };

            // Process each category
            for category in &categories {
                info!(
                    market = %market,
                    category = %category,
                    "xbox_store_api: fetching games"
                );

                match provider.fetch_games(market, category).await {
                    Ok(games) => {
                        info!(
                            market = %market,
                            category = %category,
                            count = games.len(),
                            "xbox_store_api: fetched games"
                        );

                        let mut price_rows = Vec::new();

                        for game in games {
                            if let Some(product_id) = game.product_id.as_ref() {
                                if let Some(props) = game.localized_properties.as_ref() {
                                    if let Some(prop) = props.first() {
                                        if let Some(price_info) = prop.price.as_ref() {
                                            // Extract price
                                            let list_price = price_info.list_price.unwrap_or(0.0);
                                            if list_price > 0.0 {
                                                // Convert to minor units (cents)
                                                let amount_minor = (list_price * 100.0) as i64;

                                                // Create video game source
                                                let title = prop
                                                    .product_title
                                                    .clone()
                                                    .or_else(|| game.product_title.clone())
                                                    .unwrap_or_else(|| "Unknown".to_string());

                                                // For now, create a simple placeholder for video game source
                                                // In production, you'd want to link this properly
                                                let meta = json!({
                                                    "product_id": product_id,
                                                    "title": title,
                                                    "category": category,
                                                    "market": market,
                                                    "currency": currency_code,
                                                });

                                                // Build price row
                                                price_rows.push(PriceRow {
                                                    offer_jurisdiction_id: jurisdiction_id,
                                                    video_game_source_id: None, // Would need to create proper video game entities
                                                    recorded_at: Utc::now(),
                                                    amount_minor,
                                                    tax_inclusive: true, // Xbox prices are typically tax-inclusive
                                                    fx_minor_per_unit: None,
                                                    btc_sats_per_unit: None,
                                                    meta,
                                                });
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        // Batch insert prices
                        if !price_rows.is_empty() {
                            info!(
                                market = %market,
                                category = %category,
                                price_count = price_rows.len(),
                                "xbox_store_api: inserting prices"
                            );

                            match ingest_prices(db, price_rows.clone()).await {
                                Ok(ingest_result) => {
                                    post_summary.record_batch(price_rows.len(), &ingest_result);
                                }
                                Err(e) => {
                                    warn!(
                                        market = %market,
                                        category = %category,
                                        error = %e,
                                        "xbox_store_api: failed to ingest prices"
                                    );
                                }
                            }
                        }
                    }
                    Err(e) => {
                        warn!(
                            market = %market,
                            category = %category,
                            error = %e,
                            "xbox_store_api: failed to fetch games"
                        );
                    }
                }
            }
        }

        info!(
            summary = ?post_summary,
            "xbox_store_api: ingestion complete"
        );

        Ok(())
    }

    /// Fetch games from Xbox Store API for a specific market and category
    async fn fetch_games(&self, market: &str, category: &str) -> Result<Vec<XboxGame>> {
        let url = format!(
            "{}/api/games/{}?market={}&language=en",
            self.base_url, category, market
        );

        self.rate_limiter.wait().await;
        info!(url = %url, "xbox_store_api: requesting");

        let response = self
            .client
            .get(&url)
            .header("Accept", "application/json")
            .send()
            .await
            .context("Failed to send request")?;

        let status = response.status();

        if !status.is_success() {
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unable to read error".to_string());
            anyhow::bail!("Xbox Store API returned status {}: {}", status, error_text);
        }

        let games: Vec<XboxGame> = response
            .json()
            .await
            .context("Failed to parse JSON response")?;

        Ok(games)
    }

    /// Get appropriate currency code for a market
    async fn get_currency_for_market(&self, market: &str) -> Option<String> {
        // Simple mapping - in production, you might fetch this from DB or a config
        let currency = match market {
            "US" => "USD",
            "GB" => "GBP",
            "CA" => "CAD",
            "AU" => "AUD",
            "EU" | "DE" | "FR" | "IT" | "ES" => "EUR",
            "JP" => "JPY",
            "BR" => "BRL",
            "MX" => "MXN",
            "IN" => "INR",
            "CN" => "CNY",
            "KR" => "KRW",
            "RU" => "RUB",
            _ => "USD", // Default fallback
        };

        Some(currency.to_string())
    }
}

#[derive(Clone, Debug)]
struct RateLimitSettings {
    max_ops_per_window: u32,
    window_secs: u64,
}

impl RateLimitSettings {
    fn from_env(max_key: &str, window_key: &str, default_max: u32, default_window: u64) -> Self {
        let max_ops = Self::parse_u32(max_key, default_max);
        let window_secs = Self::parse_u64(window_key, default_window);
        Self {
            max_ops_per_window: max_ops,
            window_secs,
        }
    }

    fn per_request_interval(&self) -> Option<Duration> {
        if self.max_ops_per_window == 0 || self.window_secs == 0 {
            None
        } else {
            Some(Duration::from_secs_f64(
                self.window_secs as f64 / self.max_ops_per_window as f64,
            ))
        }
    }

    fn parse_u32(key: &str, default: u32) -> u32 {
        match std::env::var(key) {
            Ok(raw) => match raw.parse::<u32>() {
                Ok(value) => value,
                Err(_) => {
                    warn!(
                        env_key = key,
                        raw_value = %raw,
                        fallback = default,
                        "xbox_store_api: invalid integer env value; using default"
                    );
                    default
                }
            },
            Err(_) => default,
        }
    }

    fn parse_u64(key: &str, default: u64) -> u64 {
        match std::env::var(key) {
            Ok(raw) => match raw.parse::<u64>() {
                Ok(value) => value,
                Err(_) => {
                    warn!(
                        env_key = key,
                        raw_value = %raw,
                        fallback = default,
                        "xbox_store_api: invalid integer env value; using default"
                    );
                    default
                }
            },
            Err(_) => default,
        }
    }
}

#[derive(Clone)]
struct RequestThrottle {
    settings: RateLimitSettings,
    state: Option<Arc<Mutex<Instant>>>,
}

impl RequestThrottle {
    fn new(settings: RateLimitSettings) -> Self {
        let state = settings
            .per_request_interval()
            .map(|_| Arc::new(Mutex::new(Instant::now())));
        Self { settings, state }
    }

    async fn wait(&self) {
        let Some(interval) = self.settings.per_request_interval() else {
            return;
        };

        let Some(state) = &self.state else {
            return;
        };

        let mut next_allowed = state.lock().await;
        let now = Instant::now();
        let wait_until = if now >= *next_allowed {
            now
        } else {
            *next_allowed
        };
        let sleep_duration = wait_until.saturating_duration_since(now);
        *next_allowed = wait_until + interval;
        drop(next_allowed);

        if !sleep_duration.is_zero() {
            sleep(sleep_duration).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{RateLimitSettings, RequestThrottle};
    use tokio::time::{Duration, Instant};

    #[tokio::test]
    async fn throttle_sleeps_between_requests() {
        let settings = RateLimitSettings {
            max_ops_per_window: 100,
            window_secs: 1,
        };
        let delay = settings.per_request_interval().unwrap();
        let throttle = RequestThrottle::new(settings.clone());

        // First call should be immediate.
        throttle.wait().await;

        let start = Instant::now();
        // Second call should respect the configured delay.
        throttle.wait().await;
        let elapsed = start.elapsed();

        assert!(
            elapsed >= delay,
            "expected wait of at least {:?}, but got {:?}",
            delay,
            elapsed
        );
    }
}
