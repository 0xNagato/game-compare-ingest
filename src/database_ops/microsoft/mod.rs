use anyhow::Result;
use chrono::Utc;
use serde_json::Value;
use tracing::{ info, warn, error };
use reqwest::Client;

use crate::database_ops::db::{ Db, PriceRow };
use crate::database_ops::ingest_providers::{
    PostIngestSummary,
    ensure_currency,
    ensure_country,
    ensure_national_jurisdiction,
    ensure_offer,
    ensure_offer_jurisdiction,
    ensure_product,
    ensure_provider,
    ensure_provider_item,
    ensure_retailer,
    ensure_sellable,
    ingest_prices,
    link_provider_offer,
    ingest_run_start,
    ingest_run_finish,
};

/// Minimal Microsoft/Xbox Store ingest using public DisplayCatalog API.
/// This is a simplified implementation: given a list of product IDs (SKUs),
/// it fetches price info for a single market (US) and records prices.
pub struct MicrosoftStoreIngest {
    client: Client,
    product_ids: Vec<String>,
    market: String, // e.g., US
    locale: String, // e.g., en-US
}

impl MicrosoftStoreIngest {
    pub fn from_env() -> Self {
        let product_ids: Vec<String> = std::env
            ::var("XBOX_PRODUCT_IDS")
            .unwrap_or_default()
            .split([',', ' '])
            .filter(|s| !s.trim().is_empty())
            .map(|s| s.trim().to_string())
            .collect();
        let market = std::env::var("XBOX_MARKET").unwrap_or_else(|_| "US".to_string());
        let locale = std::env::var("XBOX_LOCALE").unwrap_or_else(|_| "en-US".to_string());
        let client = Client::builder()
            .user_agent("GameCompareBot/1.0 (Microsoft Store Ingest)")
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .unwrap_or_else(|_| Client::new());
        Self { client, product_ids, market, locale }
    }

    pub async fn run(&self, db: &Db) -> Result<()> {
        if self.product_ids.is_empty() {
            warn!("XBOX_PRODUCT_IDS empty; nothing to ingest");
            return Ok(());
        }
        // Ensure domain entities
        let usd_id = ensure_currency(db, "USD", "US Dollar", 2).await?;
        let us_id = ensure_country(db, "US", "United States", usd_id).await?;
        let us_nat_id = ensure_national_jurisdiction(db, us_id).await?;
        let provider_id = ensure_provider(db, "Microsoft", "retailer_api", Some("xbox")).await?;
        let retailer_id = ensure_retailer(db, "Microsoft Store", Some("xbox")).await?;

        let run_id = ingest_run_start(
            db,
            provider_id,
            Some(&self.market),
            Some(Value::from("initial"))
        ).await?;

        let mut post_summary = PostIngestSummary::default();
        let mut items_processed: i64 = 0;
        let mut prices_written: i64 = 0;
        let mut errors: Vec<Value> = Vec::new();

        for pid in &self.product_ids {
            match self.ingest_one(db, provider_id, retailer_id, usd_id, us_nat_id, pid).await {
                Ok((provider_item_id, rows)) => {
                    post_summary.record_provider_item(provider_item_id);
                    if rows.is_empty() {
                        warn!(product_id=%pid, "no prices extracted");
                        items_processed += 1;
                        continue;
                    }
                    let batch_len = rows.len();
                    match ingest_prices(db, rows).await {
                        Ok(ingest_result) => {
                            items_processed += 1;
                            prices_written += ingest_result.current_updates.len() as i64;
                            post_summary.record_batch(batch_len, &ingest_result);
                        }
                        Err(e) => {
                            error!(product_id=%pid, err=%e, "xbox ingest failed for product");
                            errors.push(Value::from(format!("{}: {}", pid, e)));
                        }
                    }
                }
                Err(e) => {
                    error!(product_id=%pid, err=%e, "xbox ingest failed for product");
                    errors.push(Value::from(format!("{}: {}", pid, e)));
                }
            }
        }
        let status = if errors.is_empty() {
            "ok"
        } else if items_processed == 0 {
            "error"
        } else {
            "partial"
        };
        ingest_run_finish(db, run_id, status, items_processed, prices_written, if errors.is_empty() {
            None
        } else {
            Some(Value::from(errors))
        }).await?;
        info!(run_id, status, items_processed, prices_written, "xbox ingest run complete");
        post_summary.verify(db, provider_id).await?;
        info!(
            provider_id,
            price_rows = post_summary.total_price_rows_written,
            provider_items = post_summary.provider_item_ids.len(),
            offer_jurisdictions = post_summary.offer_jurisdiction_ids.len(),
            "xbox ingest verification complete"
        );
        Ok(())
    }

    async fn ingest_one(
        &self,
        db: &Db,
        provider_id: i64,
        retailer_id: i64,
        currency_id: i64,
        jurisdiction_id: i64,
        product_id: &str
    ) -> Result<(i64, Vec<PriceRow>)> {
        // API: https://displaycatalog.mp.microsoft.com/v7.0/products?bigIds=<ID>&market=US&languages=en-US&MS-CV=DGU1mcuYo0WMMp+
        // bigIds accepts multiple; we call per item for simplicity.
        let url = format!(
            "https://displaycatalog.mp.microsoft.com/v7.0/products?bigIds={}&market={}&languages={}&MS-CV=DGU1mcuYo0WMMp+",
            product_id,
            self.market,
            self.locale
        );
        let resp = self.client.get(&url).send().await?;
        if !resp.status().is_success() {
            anyhow::bail!("HTTP {}", resp.status());
        }
        let body: Value = resp.json().await?;
        let products = body
            .get("Products")
            .and_then(|v| v.as_array())
            .unwrap_or(&vec![]);
        if products.is_empty() {
            warn!(product_id, "no product data returned");
            return Ok(0);
        }
        // Extract pricing info (simplified path)
        // Real payload has nested DisplaySkuAvailabilities; we scan them for price amount.
        let mut price_minor: Option<i64> = None;
        let mut original_minor: Option<i64> = None;
        if let Some(prod) = products.first() {
            if let Some(avails) = prod.get("DisplaySkuAvailabilities").and_then(|v| v.as_array()) {
                for avail in avails {
                    if let Some(pricing) = avail.get("Availabilities").and_then(|v| v.as_array()) {
                        for p in pricing {
                            if let Some(price) = p.get("Price") {
                                // list: ListPrice, msrp: MSRP, price: Price, currency: CurrencyCode
                                if
                                    let Some(list_price) = price
                                        .get("ListPrice")
                                        .and_then(|v| v.as_f64())
                                {
                                    original_minor = Some((list_price * 100.0).round() as i64);
                                }
                                if
                                    let Some(sale_price) = price
                                        .get("Price")
                                        .and_then(|v| v.as_f64())
                                {
                                    price_minor = Some((sale_price * 100.0).round() as i64);
                                }
                            }
                        }
                    }
                }
            }
        }

        // Build domain entities
        let product_slug = format!("xbox-{}", product_id.to_lowercase());
        let product_row_id = ensure_product(db, "software", Some(&product_slug)).await?;
        ensure_sellable(db, "software", product_row_id).await?; // canonical sellable via title path
        let sellable_id = {
            let rec = sqlx
                ::query(
                    "SELECT s.id FROM sellables s JOIN video_game_titles vgt ON vgt.id=s.software_title_id JOIN products p ON p.id=vgt.video_game_id WHERE p.id=$1 LIMIT 1"
                )
                .persistent(false)
                .bind(product_row_id)
                .fetch_one(&db.pool).await?;
            rec.get::<i64, _>("id")
        };
        let offer_id = ensure_offer(db, sellable_id, retailer_id, Some(product_id)).await?;
        let oj_id = ensure_offer_jurisdiction(db, offer_id, jurisdiction_id, currency_id).await?;
        let provider_item_id = ensure_provider_item(
            db,
            provider_id,
            product_id,
            Some(body.clone())
        ).await?;
        link_provider_offer(db, provider_item_id, offer_id, Some(0.9)).await?;

        let mut rows: Vec<PriceRow> = Vec::new();
        if let Some(fin) = price_minor {
            rows.push(PriceRow {
                offer_jurisdiction_id: oj_id,
                provider_item_id: Some(provider_item_id),
                recorded_at: Utc::now(),
                amount_minor: fin,
                tax_inclusive: true,
                fx_minor_per_unit: None,
                btc_sats_per_unit: None,
                meta: Value::from(json!({"src":"xbox","kind":"final"})),
            });
        }
        if let (Some(orig), Some(fin)) = (original_minor, price_minor) {
            if orig > fin {
                // discount
                rows.push(PriceRow {
                    offer_jurisdiction_id: oj_id,
                    provider_item_id: Some(provider_item_id),
                    recorded_at: Utc::now(),
                    amount_minor: orig,
                    tax_inclusive: true,
                    fx_minor_per_unit: None,
                    btc_sats_per_unit: None,
                    meta: Value::from(json!({"src":"xbox","kind":"original"})),
                });
            }
        }
        Ok((provider_item_id, rows))
    }
}
