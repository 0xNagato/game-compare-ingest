use anyhow::{Context, Result};
use i_miss_rust::util::env;
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::io::{BufRead, BufReader};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use chrono::Utc;
use i_miss_rust::database_ops::db::{Db, PriceRow};
use i_miss_rust::database_ops::ingest_providers::{
    ensure_country, ensure_currency, ensure_national_jurisdiction, ensure_offer,
    ensure_offer_jurisdiction, ensure_provider, ensure_provider_item, ensure_retailer,
    ensure_sellable_for_title, ensure_video_game_title_without_product, ingest_prices,
    link_provider_offer, PostIngestSummary,
};
use serde_json::json;

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
    prices: HashMap<String, serde_json::Value>, // currency code -> price or "unavailable"
    discounts: HashMap<String, serde_json::Value>,
}

#[tokio::main]
async fn main() -> Result<()> {
    env::bootstrap_cli("nexarda_catalogue_ingest");
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    // Year range hint for consistency across providers
    let year_min: i32 = std::env::var("YEAR_MIN")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2020);
    let year_max: i32 = std::env::var("YEAR_MAX")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2025);
    println!(
        "remember: restricting to releases between {year_min}-{year_max} inclusive (where applicable)\n"
    );

    // Reduce PgBouncer issues: disable statement cache and force simple protocol
    unsafe {
        std::env::set_var("SQLX_DISABLE_STATEMENT_CACHE", "1");
        std::env::set_var("SQLX_PG_SIMPLE", "1");
    }

    let database_url = std::env::var("SUPABASE_DB_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .context("Set DATABASE_URL or SUPABASE_DB_URL")?;
    let db = Db::connect(
        &database_url,
        std::env::var("DB_MAX_CONNS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(8),
    )
    .await?;

    // Ensure writes go to public.* explicitly for this one-shot session
    let _ = sqlx::query("SET search_path TO public")
        .persistent(false)
        .execute(&db.pool)
        .await;
    // Optional statement timeout override
    if let Ok(ms) = std::env::var("SQL_STATEMENT_TIMEOUT_MS") {
        if let Ok(n) = ms.parse::<u64>() {
            let _ = sqlx::query(&format!("SET LOCAL statement_timeout TO '{}ms'", n))
                .persistent(false)
                .execute(&db.pool)
                .await;
        }
    }

    // If an NDJSON path is provided, stream it line-by-line for constant memory usage
    if let Ok(ndjson_path) = std::env::var("NEXARDA_CATALOGUE_NDJSON_PATH") {
        if !ndjson_path.trim().is_empty() {
            return run_ndjson_stream(&db, &ndjson_path).await;
        }
    }

    let path = std::env::var("NEXARDA_CATALOGUE_PATH")
        .unwrap_or_else(|_| "nexarda_product_catalogue.json".into());
    let raw = fs::read_to_string(&path).with_context(|| format!("read catalogue file: {path}"))?;
    let cat: CatalogueRoot = serde_json::from_str(&raw).context("parse catalogue json")?;
    if !cat.success {
        warn!(code=?cat.code, msg=?cat.message, "catalogue marked unsuccessful");
    }
    info!(games = cat.games.len(), "nexarda catalogue loaded");

    // Ensure provider + optional retailer (retailer only used in full pricing mode)
    let provider_id =
        ensure_provider(&db, "nexarda_catalogue", "catalog", Some("nexarda-cat")).await?;
    let retailer_id = ensure_retailer(&db, "NEXARDA Catalogue", Some("nexarda-cat")).await?;

    // Simple mode: mimic GiantBomb flow (no offers/prices), controlled by env NEXARDA_SIMPLE=1
    let simple_mode = std::env::var("NEXARDA_SIMPLE")
        .ok()
        .map(|s| (s == "1" || s.eq_ignore_ascii_case("true")))
        .unwrap_or(false);
    // Optional chunking controls
    let offset: usize = std::env::var("NEXARDA_CATALOGUE_OFFSET")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    if simple_mode {
        use i_miss_rust::database_ops::ingest_providers::{
            ensure_platform, ensure_provider_item, ensure_sellable_for_title, ensure_video_game,
            ensure_video_game_title_without_product,
        };
        let platform_id = ensure_platform(&db, "PC", Some("pc")).await?;
        let limit: Option<usize> = std::env::var("NEXARDA_CATALOGUE_LIMIT")
            .ok()
            .and_then(|s| s.parse().ok());
        let mut processed = 0usize;
        for (idx, g) in cat.games.iter().enumerate() {
            if idx < offset {
                continue;
            }
            if let Some(lim) = limit {
                if processed >= lim {
                    info!(limit = lim, "nexarda simple limit reached");
                    break;
                }
            }
            let name = if g.name.trim().is_empty() {
                if g.slug.trim().is_empty() {
                    format!("nexarda-{}", g.id)
                } else {
                    g.slug.clone()
                }
            } else {
                g.name.clone()
            };
            let slug_hint = if g.slug.trim().is_empty() {
                format!("nexarda-{}", g.id)
            } else {
                normalize_slug(&g.slug)
            };
            let title_id = ensure_video_game_title_without_product(&db, &slug_hint, &name).await?;
            let _sellable_id = ensure_sellable_for_title(&db, title_id).await?;
            let _vg_id = ensure_video_game(&db, title_id, platform_id, None).await?;
            let _pi = ensure_provider_item(
                &db,
                provider_id,
                &format!("nexarda:{}", g.id),
                Some(json!({"orig_slug": g.slug})),
            )
            .await?;
            processed += 1;
            if processed % 250 == 0 {
                info!(processed, "nexarda simple progress");
            }
        }
        info!(processed, "nexarda simple ingest complete");
        return Ok(());
    }

    // We will treat each currency mapping as a national jurisdiction using currency code as faux country code
    // (If a real mapping exists we could refine later.)
    let mut price_rows: Vec<PriceRow> = Vec::new();
    let mut post_summary = PostIngestSummary::default();
    let flush_every: usize = std::env::var("NEXARDA_FLUSH_EVERY")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000);

    let limit: Option<usize> = std::env::var("NEXARDA_CATALOGUE_LIMIT")
        .ok()
        .and_then(|s| s.parse().ok());
    let mut processed: usize = 0;
    // Optional offset for chunked runs
    let offset: usize = std::env::var("NEXARDA_CATALOGUE_OFFSET")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    // Resume-after controls
    let resume_after_id: Option<i64> = std::env::var("NEXARDA_RESUME_AFTER_ID")
        .ok()
        .and_then(|s| s.parse().ok());
    let resume_after_slug: Option<String> = std::env::var("NEXARDA_RESUME_AFTER_SLUG").ok();
    let mut resume_passed = resume_after_id.is_none() && resume_after_slug.is_none();
    for (idx, g) in cat.games.iter().enumerate() {
        if idx < offset {
            continue;
        }
        if !resume_passed {
            if resume_after_id.map(|id| id == g.id).unwrap_or(false)
                || resume_after_slug
                    .as_ref()
                    .map(|s| s == &g.slug)
                    .unwrap_or(false)
            {
                // Start after the matched row
                resume_passed = true;
                continue;
            } else {
                continue;
            }
        }
        if let Some(lim) = limit {
            if processed >= lim {
                info!(limit = lim, "nexarda catalogue limit reached");
                break;
            }
        }
        let slug_tmp = normalize_slug(&g.slug);
        let slug_norm = {
            let trimmed = slug_tmp.trim_start_matches('/').trim();
            if trimmed.is_empty() {
                format!("nexarda-{}", g.id)
            } else {
                trimmed.to_string()
            }
        };
        let title_name = if g.name.trim().is_empty() {
            if g.slug.trim().is_empty() {
                format!("Nexarda {}", g.id)
            } else {
                g.slug.clone()
            }
        } else {
            g.name.clone()
        };
        let title_id =
            ensure_video_game_title_without_product(&db, &slug_norm, &title_name).await?;
        let sellable_id = ensure_sellable_for_title(&db, title_id).await?;
        let offer_id = ensure_offer(&db, sellable_id, retailer_id, None).await?;
        let video_game_source_id = ensure_provider_item(
            &db,
            provider_id,
            &format!("nexarda:{}", g.id),
            Some(json!({"orig_slug": g.slug})),
        )
        .await?;
        link_provider_offer(&db, video_game_source_id, offer_id, Some(0.6)).await?;
        post_summary.record_provider_item(video_game_source_id);

        for (ccy_raw, price_val) in &g.prices {
            let ccy = ccy_raw.to_ascii_uppercase();
            if price_val.is_string() && price_val.as_str() == Some("unavailable") {
                continue;
            }
            let price_f = extract_price_f64(price_val).unwrap_or(0.0);
            if price_f <= 0.0 {
                continue;
            }
            let minor_unit = currency_minor_unit(&ccy);
            let currency_id = ensure_currency(&db, &ccy, &ccy, minor_unit).await?;
            let cc2 = currency_to_country_code(&ccy);
            let country_id = ensure_country(&db, &cc2, &cc2, currency_id).await?; // map currency to representative ISO-3166 alpha-2
            let juris_id = ensure_national_jurisdiction(&db, country_id).await?;
            let oj_id = ensure_offer_jurisdiction(&db, offer_id, juris_id, currency_id).await?;
            let amount_minor = (price_f * (10f64).powi(minor_unit as i32)).round() as i64;
            price_rows.push(PriceRow {
                offer_jurisdiction_id: oj_id,
                video_game_source_id: Some(video_game_source_id),
                recorded_at: Utc::now(),
                amount_minor,
                tax_inclusive: true,
                fx_minor_per_unit: None,
                btc_sats_per_unit: None,
                meta: json!({"src":"nexarda_catalogue","discounts": g.discounts.get(ccy_raw)}),
            });
            // offer_jurisdiction_ids aggregated via ingest result
        }
        // Flush in batches to keep memory bounded
        processed += 1;
        if processed % 250 == 0 {
            info!(
                processed,
                price_buffer = price_rows.len(),
                "nexarda progress"
            );
        }
        // Flush earlier for visibility
        if price_rows.len() >= flush_every {
            let batch_len = price_rows.len();
            info!(flush = batch_len, processed, "nexarda flushing price batch");
            let batch = std::mem::take(&mut price_rows);
            let ingest_result = ingest_prices(&db, batch).await?;
            post_summary.record_batch(batch_len, &ingest_result);
        }
    }
    if !price_rows.is_empty() {
        let batch_len = price_rows.len();
        info!(final_flush = batch_len, processed, "nexarda final flush");
        let ingest_result = ingest_prices(&db, price_rows).await?;
        post_summary.record_batch(batch_len, &ingest_result);
    }
    post_summary.verify(&db, provider_id).await?;
    info!(
        processed,
        prices_written = post_summary.total_price_rows_written,
        "nexarda catalogue ingest complete"
    );
    Ok(())
}

fn normalize_slug(s: &str) -> String {
    s.to_lowercase()
        .replace(|c: char| (c == ' ' || c == '\n' || c == '\r'), "-")
}

fn extract_price_f64(v: &serde_json::Value) -> Option<f64> {
    if let Some(f) = v.as_f64() {
        return Some(f);
    }
    if let Some(s) = v.as_str() {
        // Handle numeric strings like "19.99" or "19,99"
        let cleaned = s.replace(',', ".").trim().to_string();
        if let Ok(f) = cleaned.parse::<f64>() {
            return Some(f);
        }
    }
    if let Some(obj) = v.as_object() {
        // Common shapes: {"price": 19.99} or {"value": 19.99}
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

fn currency_minor_unit(code: &str) -> i16 {
    match code {
        "JPY" | "KRW" | "VND" | "CLP" | "ISK" | "HUF" => 0,
        "BHD" | "IQD" | "KWD" | "JOD" | "OMR" | "TND" => 3,
        _ => 2,
    }
}

fn currency_to_country_code(code: &str) -> String {
    (match code {
        "USD" => "US",
        "EUR" => "DE", // representative EU member; adjust as needed
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

// --- NDJSON streaming mode ---------------------------------------------------

#[derive(Debug, Deserialize)]
struct NdjsonGameRecord {
    id: i64,
    name: Option<String>,
    slug: Option<String>,
    prices: Option<HashMap<String, serde_json::Value>>,
    discounts: Option<HashMap<String, serde_json::Value>>,
}

async fn run_ndjson_stream(db: &Db, ndjson_path: &str) -> Result<()> {
    use chrono::Utc;
    use i_miss_rust::database_ops::ingest_providers::{
        ensure_country, ensure_currency, ensure_national_jurisdiction, ensure_offer,
        ensure_offer_jurisdiction, ensure_provider, ensure_provider_item, ensure_retailer,
        ensure_sellable_for_title, ensure_video_game_title_without_product, ingest_prices,
        link_provider_offer,
    };

    info!(path = ndjson_path, "nexarda ndjson: streaming ingest");
    let f = fs::File::open(ndjson_path).with_context(|| format!("open ndjson: {ndjson_path}"))?;
    let mut reader = BufReader::new(f);

    let provider_id =
        ensure_provider(db, "nexarda_catalogue", "catalog", Some("nexarda-cat")).await?;
    let retailer_id = ensure_retailer(db, "NEXARDA Catalogue", Some("nexarda-cat")).await?;

    let limit: Option<usize> = std::env::var("NEXARDA_CATALOGUE_LIMIT")
        .ok()
        .and_then(|s| s.parse().ok());
    let offset: usize = std::env::var("NEXARDA_CATALOGUE_OFFSET")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    let mut line = String::new();
    let mut idx: usize = 0;
    let mut processed: usize = 0;
    let mut price_rows: Vec<PriceRow> = Vec::new();
    let flush_every: usize = std::env::var("NEXARDA_FLUSH_EVERY")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000);
    let mut post_summary = PostIngestSummary::default();
    // Resume-after controls
    let resume_after_id: Option<i64> = std::env::var("NEXARDA_RESUME_AFTER_ID")
        .ok()
        .and_then(|s| s.parse().ok());
    let resume_after_slug: Option<String> = std::env::var("NEXARDA_RESUME_AFTER_SLUG").ok();
    let mut resume_passed = resume_after_id.is_none() && resume_after_slug.is_none();

    loop {
        line.clear();
        let n = reader.read_line(&mut line)?;
        if n == 0 {
            break;
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            idx += 1;
            continue;
        }
        if idx < offset {
            idx += 1;
            continue;
        }
        if let Some(lim) = limit {
            if processed >= lim {
                info!(processed, "nexarda ndjson limit reached");
                break;
            }
        }

        // Parse a single record
        let rec: NdjsonGameRecord = match serde_json::from_str(trimmed) {
            Ok(v) => v,
            Err(e) => {
                warn!(error=%e, line_index=idx, "ndjson parse error; skipping line");
                idx += 1;
                continue;
            }
        };

        if !resume_passed {
            if resume_after_id.map(|id| id == rec.id).unwrap_or(false)
                || resume_after_slug
                    .as_ref()
                    .map(|s| rec.slug.as_deref() == Some(s.as_str()))
                    .unwrap_or(false)
            {
                resume_passed = true;
                idx += 1;
                continue; // start from next line after match
            } else {
                idx += 1;
                continue;
            }
        }

        let slug_raw = rec.slug.clone().unwrap_or_else(|| rec.id.to_string());
        let slug_norm = {
            let normalized = normalize_slug(&slug_raw);
            let trimmed = normalized.trim_start_matches('/').trim();
            if trimmed.is_empty() {
                format!("nexarda-{}", rec.id)
            } else {
                trimmed.to_string()
            }
        };
        let title = rec
            .name
            .as_ref()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .unwrap_or_else(|| {
                if slug_raw.trim().is_empty() {
                    format!("Nexarda {}", rec.id)
                } else {
                    slug_raw.clone()
                }
            });

        let title_id = ensure_video_game_title_without_product(db, &slug_norm, &title).await?;
        let sellable_id = ensure_sellable_for_title(db, title_id).await?;
        let offer_id = ensure_offer(db, sellable_id, retailer_id, None).await?;
        let video_game_source_id = ensure_provider_item(
            db,
            provider_id,
            &format!("nexarda:{}", rec.id),
            Some(serde_json::json!({"orig_slug": slug_raw})),
        )
        .await?;
        link_provider_offer(db, video_game_source_id, offer_id, Some(0.6)).await?;
        post_summary.record_provider_item(video_game_source_id);

        if let Some(prices) = rec.prices.as_ref() {
            for (ccy_raw, price_val) in prices.iter() {
                let ccy = ccy_raw.to_ascii_uppercase();
                if price_val.is_string() && price_val.as_str() == Some("unavailable") {
                    continue;
                }
                let price_f = extract_price_f64(price_val).unwrap_or(0.0);
                if price_f <= 0.0 {
                    continue;
                }
                let minor_unit = currency_minor_unit(&ccy);
                let currency_id = ensure_currency(db, &ccy, &ccy, minor_unit).await?;
                let cc2 = currency_to_country_code(&ccy);
                let country_id = ensure_country(db, &cc2, &cc2, currency_id).await?;
                let juris_id = ensure_national_jurisdiction(db, country_id).await?;
                let oj_id = ensure_offer_jurisdiction(db, offer_id, juris_id, currency_id).await?;
                let amount_minor = (price_f * (10f64).powi(minor_unit as i32)).round() as i64;
                price_rows.push(PriceRow {
                    offer_jurisdiction_id: oj_id,
                    video_game_source_id: Some(video_game_source_id),
                    recorded_at: Utc::now(),
                    amount_minor,
                    tax_inclusive: true,
                    fx_minor_per_unit: None,
                    btc_sats_per_unit: None,
                    meta: serde_json::json!({
                        "src":"nexarda_catalogue",
                        "discounts": rec.discounts.as_ref().and_then(|d| d.get(ccy_raw)).cloned()
                    }),
                });
                // offer_jurisdiction_ids aggregated via ingest result
            }
        }

        processed += 1;
        if processed % 250 == 0 {
            info!(
                processed,
                price_buffer = price_rows.len(),
                "nexarda ndjson progress"
            );
        }
        if price_rows.len() >= flush_every {
            let batch_len = price_rows.len();
            info!(
                flush = batch_len,
                processed, "nexarda ndjson flushing price batch"
            );
            let batch = std::mem::take(&mut price_rows);
            let ingest_result = ingest_prices(db, batch).await?;
            post_summary.record_batch(batch_len, &ingest_result);
        }

        idx += 1;
    }

    if !price_rows.is_empty() {
        let batch_len = price_rows.len();
        info!(
            final_flush = batch_len,
            processed, "nexarda ndjson final flush"
        );
        let ingest_result = ingest_prices(db, price_rows).await?;
        post_summary.record_batch(batch_len, &ingest_result);
    }
    post_summary.verify(db, provider_id).await?;
    info!(
        processed,
        path = ndjson_path,
        prices_written = post_summary.total_price_rows_written,
        "nexarda ndjson ingest complete"
    );
    Ok(())
}
