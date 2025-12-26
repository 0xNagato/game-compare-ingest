use anyhow::Result;
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::BTreeMap;

use crate::database_ops::db::Db;

#[derive(Clone)]
pub struct ExchangeService {
    pub db: Db,
    pub http: Client,
}

impl ExchangeService {
    pub fn new(db: Db) -> Self {
        let http = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .expect("reqwest client");
        Self { db, http }
    }

    pub fn supported_currencies() -> BTreeMap<&'static str, &'static str> {
        // Keep deterministic order for predictable batching
        BTreeMap::from([
            // Majors
            ("USD", "United States Dollar"),
            ("EUR", "Euro"),
            ("GBP", "British Pound Sterling"),
            ("JPY", "Japanese Yen"),
            ("CAD", "Canadian Dollar"),
            ("AUD", "Australian Dollar"),
            ("CHF", "Swiss Franc"),
            ("CNY", "Chinese Yuan"),
            ("KRW", "South Korean Won"),
            ("SGD", "Singapore Dollar"),
            ("HKD", "Hong Kong Dollar"),
            ("NOK", "Norwegian Krone"),
            ("SEK", "Swedish Krona"),
            ("DKK", "Danish Krone"),
            ("PLN", "Polish Zloty"),
            ("CZK", "Czech Koruna"),
            ("HUF", "Hungarian Forint"),
            ("RUB", "Russian Ruble"),
            ("INR", "Indian Rupee"),
            ("BRL", "Brazilian Real"),
            ("MXN", "Mexican Peso"),
            ("ZAR", "South African Rand"),
            ("THB", "Thai Baht"),
            ("TRY", "Turkish Lira"),
            ("NZD", "New Zealand Dollar"),
            ("ILS", "Israeli New Shekel"),
            ("AED", "UAE Dirham"),
            ("SAR", "Saudi Riyal"),
            // Crypto
            ("BTC", "Bitcoin"),
            ("ETH", "Ethereum"),
        ])
    }

    pub fn region_currency_map() -> BTreeMap<&'static str, &'static str> {
        BTreeMap::from([
            // PS-style locales
            ("en-us", "USD"),
            ("en-gb", "GBP"),
            ("fr-fr", "EUR"),
            ("de-de", "EUR"),
            ("es-es", "EUR"),
            ("it-it", "EUR"),
            ("ja-jp", "JPY"),
            ("en-au", "AUD"),
            ("en-ca", "CAD"),
            ("pt-br", "BRL"),
            ("es-mx", "MXN"),
            ("ru-ru", "RUB"),
            ("ko-kr", "KRW"),
            ("zh-cn", "CNY"),
            ("zh-tw", "TWD"),
            ("zh-hk", "HKD"),
            ("th-th", "THB"),
            ("tr-tr", "TRY"),
            ("pl-pl", "PLN"),
            ("cs-cz", "CZK"),
            ("hu-hu", "HUF"),
            ("sv-se", "SEK"),
            ("no-no", "NOK"),
            ("da-dk", "DKK"),
            ("nl-nl", "EUR"),
            ("en-sg", "SGD"),
            ("en-in", "INR"),
            ("en-za", "ZAR"),
            ("he-il", "ILS"),
            ("ar-ae", "AED"),
            ("ar-sa", "SAR"),
            ("en-nz", "NZD"),
            ("de-ch", "CHF"),
            // Steam-like country codes
            ("US", "USD"),
            ("GB", "GBP"),
            ("EU", "EUR"),
            ("JP", "JPY"),
            ("CA", "CAD"),
            ("AU", "AUD"),
            ("CH", "CHF"),
            ("CN", "CNY"),
            ("KR", "KRW"),
            ("SG", "SGD"),
            ("HK", "HKD"),
            ("NO", "NOK"),
            ("SE", "SEK"),
            ("DK", "DKK"),
            ("PL", "PLN"),
            ("CZ", "CZK"),
            ("HU", "HUF"),
            ("RU", "RUB"),
            ("IN", "INR"),
            ("BR", "BRL"),
            ("MX", "MXN"),
            ("ZA", "ZAR"),
            ("TH", "THB"),
            ("TR", "TRY"),
            ("NZ", "NZD"),
            ("IL", "ILS"),
            ("AE", "AED"),
            ("SA", "SAR"),
        ])
    }

    pub fn currency_for_region(region: &str) -> Option<&'static str> {
        let r = region;
        let map = Self::region_currency_map();
        map.get(r.to_lowercase().as_str())
            .copied()
            .or_else(|| map.get(r.to_uppercase().as_str()).copied())
    }

    // Fetch BTC→fiat rates for supported currencies
    pub async fn fetch_btc_rates(&self) -> Result<Vec<RateRow>> {
        let currencies: Vec<&str> = Self::supported_currencies()
            .keys()
            .cloned()
            .filter(|c| *c != "BTC" && *c != "ETH")
            .collect();
        let vs = currencies.join(",").to_lowercase();
        let url = format!(
            "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies={}&include_24hr_change=true&include_last_updated_at=true",
            vs
        );
        let resp = self.http.get(url).send().await?.error_for_status()?;
        let v: serde_json::Value = resp.json().await?;
        let m = v
            .get("bitcoin")
            .and_then(|x| x.as_object())
            .cloned()
            .unwrap_or_default();
        let last_updated = m.get("last_updated_at").and_then(|x| x.as_i64());
        let fetched_at: DateTime<Utc> = last_updated
            .map(|ts| chrono::DateTime::from_timestamp(ts, 0).unwrap_or_else(|| Utc::now()))
            .unwrap_or_else(|| Utc::now());
        let mut out = Vec::new();
        for code in currencies {
            let key = code.to_lowercase();
            if let Some(rate) = m.get(&key).and_then(|x| x.as_f64()) {
                let change = m
                    .get(&(key.clone() + "_24h_change"))
                    .and_then(|x| x.as_f64());
                out.push(RateRow {
                    base_currency: "BTC".to_string(),
                    quote_currency: code.to_string(),
                    rate,
                    provider: "coingecko".to_string(),
                    fetched_at,
                    metadata: json!({"change_24h": change, "src":"simple/price"}),
                });
            }
        }
        Ok(out)
    }

    // Compute USD→fiat via BTC cross: USD/X = BTC/X / BTC/USD
    pub async fn fetch_usd_rates_via_btc(&self) -> Result<Vec<RateRow>> {
        let currencies: Vec<&str> = Self::supported_currencies()
            .keys()
            .cloned()
            .filter(|c| *c != "BTC" && *c != "ETH")
            .collect();
        let mut list = currencies.clone();
        list.push("USD");
        let vs = list.join(",").to_lowercase();
        let url = format!(
            "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies={}",
            vs
        );
        let resp = self.http.get(url).send().await?.error_for_status()?;
        let v: serde_json::Value = resp.json().await?;
        let m = v
            .get("bitcoin")
            .and_then(|x| x.as_object())
            .cloned()
            .unwrap_or_default();
        let btc_usd = m.get("usd").and_then(|x| x.as_f64()).unwrap_or(1.0);
        let fetched_at = Utc::now();
        let mut out = Vec::new();
        for code in currencies {
            let key = code.to_lowercase();
            if let Some(btc_x) = m.get(&key).and_then(|x| x.as_f64()) {
                if btc_x > 0.0 && btc_usd > 0.0 {
                    let usd_per_x = btc_x / btc_usd;
                    out.push(RateRow {
                        base_currency: "USD".to_string(),
                        quote_currency: code.to_string(),
                        rate: usd_per_x,
                        provider: "coingecko".to_string(),
                        fetched_at,
                        metadata: json!({"calculated_from_btc": true, "btc_usd_rate": btc_usd, "btc_currency_rate": btc_x}),
                    });
                }
            }
        }
        Ok(out)
    }

    pub async fn fetch_all_rates(&self) -> Result<Vec<RateRow>> {
        let (btc, usd) = tokio::try_join!(self.fetch_btc_rates(), self.fetch_usd_rates_via_btc())?;
        let mut all = btc;
        all.extend(usd);
        // Additional FX sources: ECB (Euro foreign exchange reference), exchangerate.host fallback
        if let Ok(ecb) = self.fetch_ecb_daily().await {
            all.extend(ecb);
        }
        if let Ok(host) = self.fetch_exchangerate_host_latest().await {
            all.extend(host);
        }
        Ok(all)
    }

    pub async fn store_rates(&self, rates: &[RateRow]) -> Result<usize> {
        if rates.is_empty() {
            return Ok(0);
        }
        let mut stored = 0usize;
        for r in rates {
            let _ = sqlx::query(
                    "INSERT INTO exchange_rates (base_currency, quote_currency, rate, provider, fetched_at, metadata) \
                 VALUES ($1,$2,$3,$4,$5,$6) \
                 ON CONFLICT (base_currency, quote_currency, provider) \
                 DO UPDATE SET rate=EXCLUDED.rate, fetched_at=EXCLUDED.fetched_at, metadata=EXCLUDED.metadata"
            )
            .bind(&r.base_currency)
            .bind(&r.quote_currency)
            .bind(r.rate)
            .bind(&r.provider)
            .bind(r.fetched_at)
            .bind(&r.metadata)
            .execute(&self.db.pool)
            .await?;
            stored += 1;
        }
        Ok(stored)
    }

    pub async fn latest_rate(&self, base: &str, quote: &str) -> Result<Option<f64>> {
        // Prefer provider from env if set, else any latest rate
        if let Ok(provider) = std::env::var("FX_PREFERRED_PROVIDER") {
            if !provider.is_empty() {
                let rec = sqlx::query_scalar(
                        "SELECT rate FROM exchange_rates WHERE base_currency=$1 AND quote_currency=$2 AND provider=$3 ORDER BY fetched_at DESC LIMIT 1"
                )
                .bind(base)
                .bind(quote)
                .bind(provider)
                .fetch_optional(&self.db.pool)
                .await?;
                if rec.is_some() {
                    return Ok(rec);
                }
            }
        }
        let rec = sqlx::query_scalar(
                "SELECT rate FROM exchange_rates WHERE base_currency=$1 AND quote_currency=$2 ORDER BY fetched_at DESC LIMIT 1"
        )
        .bind(base)
        .bind(quote)
        .fetch_optional(&self.db.pool)
        .await?;
        Ok(rec)
    }

    pub async fn convert(&self, amount: f64, from: &str, to: &str) -> Result<Option<f64>> {
        if from.eq_ignore_ascii_case(to) {
            return Ok(Some(amount));
        }
        // direct
        if let Some(r) = self.latest_rate(from, to).await? {
            return Ok(Some(amount * r));
        }
        // inverse
        if let Some(inv) = self.latest_rate(to, from).await? {
            if inv > 0.0 {
                return Ok(Some(amount / inv));
            }
        }
        // cross via USD
        if !from.eq_ignore_ascii_case("USD") && !to.eq_ignore_ascii_case("USD") {
            if let (Some(r1), Some(r2)) = (
                self.latest_rate(from, "USD").await?,
                self.latest_rate("USD", to).await?,
            ) {
                return Ok(Some(amount * r1 * r2));
            }
            if let (Some(r1_inv), Some(r2_inv)) = (
                self.latest_rate("USD", from).await?,
                self.latest_rate(to, "USD").await?,
            ) {
                if r1_inv > 0.0 && r2_inv > 0.0 {
                    return Ok(Some(amount / r1_inv / r2_inv));
                }
            }
        }
        // cross via BTC
        if !from.eq_ignore_ascii_case("BTC") && !to.eq_ignore_ascii_case("BTC") {
            if let (Some(r1), Some(r2)) = (
                self.latest_rate(from, "BTC").await?,
                self.latest_rate("BTC", to).await?,
            ) {
                return Ok(Some(amount * r1 * r2));
            }
            if let (Some(r1_inv), Some(r2_inv)) = (
                self.latest_rate("BTC", from).await?,
                self.latest_rate(to, "BTC").await?,
            ) {
                if r1_inv > 0.0 && r2_inv > 0.0 {
                    return Ok(Some(amount / r1_inv / r2_inv));
                }
            }
        }
        Ok(None)
    }

    pub async fn sync_all(&self) -> Result<SyncSummary> {
        let rates = self.fetch_all_rates().await?;
        let stored = self.store_rates(&rates).await?;
        Ok(SyncSummary {
            fetched: rates.len(),
            stored,
            timestamp: Utc::now(),
        })
    }

    // ECB daily reference rates (EUR base). Source: https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml
    // We'll use the JSON proxy at exchangerate.host for simplicity to avoid XML parsing.
    async fn fetch_ecb_daily(&self) -> Result<Vec<RateRow>> {
        let url = "https://api.exchangerate.host/latest?base=EUR";
        let resp = self.http.get(url).send().await?.error_for_status()?;
        let v: serde_json::Value = resp.json().await?;
        let fetched_at = Utc::now();
        let mut out = Vec::new();
        if v.get("success").and_then(|b| b.as_bool()) != Some(true) {
            return Ok(out);
        }
        if let Some(rates) = v.get("rates").and_then(|r| r.as_object()) {
            for (quote, val) in rates.iter() {
                if let Some(rate) = val.as_f64() {
                    out.push(RateRow {
                        base_currency: "EUR".into(),
                        quote_currency: quote.to_uppercase(),
                        rate,
                        provider: "ecb".into(),
                        fetched_at,
                        metadata: json!({"src":"exchangerate.host","kind":"daily"}),
                    });
                }
            }
        }
        Ok(out)
    }

    // Latest cross rates from exchangerate.host (configurable base via env FX_BASE_CURRENCY)
    async fn fetch_exchangerate_host_latest(&self) -> Result<Vec<RateRow>> {
        let base = std::env::var("FX_BASE_CURRENCY")
            .unwrap_or_else(|_| "USD".into())
            .to_uppercase();
        let symbols = Self::supported_currencies()
            .keys()
            .cloned()
            .collect::<Vec<_>>()
            .join(",");
        let url = format!(
            "https://api.exchangerate.host/latest?base={}&symbols={}",
            base, symbols
        );
        let resp = self.http.get(url).send().await?.error_for_status()?;
        let v: serde_json::Value = resp.json().await?;
        let fetched_at = Utc::now();
        let mut out = Vec::new();
        if v.get("success").and_then(|b| b.as_bool()) != Some(true) {
            return Ok(out);
        }
        if let Some(rates) = v.get("rates").and_then(|r| r.as_object()) {
            for (quote, val) in rates.iter() {
                if quote.eq_ignore_ascii_case(&base) {
                    continue;
                }
                if let Some(rate) = val.as_f64() {
                    out.push(RateRow {
                        base_currency: base.clone(),
                        quote_currency: quote.to_uppercase(),
                        rate,
                        provider: "exchangerate.host".into(),
                        fetched_at,
                        metadata: json!({"kind":"latest"}),
                    });
                }
            }
        }
        Ok(out)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateRow {
    pub base_currency: String,
    pub quote_currency: String,
    pub rate: f64,
    pub provider: String,
    pub fetched_at: DateTime<Utc>,
    pub metadata: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncSummary {
    pub fetched: usize,
    pub stored: usize,
    pub timestamp: DateTime<Utc>,
}
