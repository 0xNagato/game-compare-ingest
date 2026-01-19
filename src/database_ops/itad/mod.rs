pub mod provider;

pub use provider::{ItadDeal, ItadGame, ItadMedia, ItadProvider};

use std::collections::{BTreeSet, HashMap, HashSet};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use tracing::{debug, info, warn};

use crate::database_ops::{
    db::{Db, PriceRow},
    ingest_providers::{
        ensure_country, ensure_currency, ensure_national_jurisdiction, ensure_offer,
        ensure_offer_jurisdiction, ensure_product_named, ensure_provider, ensure_provider_item,
        ensure_retailer, ensure_sellable, ensure_vg_source_media_links_with_meta, ingest_prices,
        link_provider_offer, php_compat_schema, PostIngestSummary,
    },
};

async fn current_schema_tables(db: &Db) -> HashSet<String> {
    // One query to avoid repeated pool acquisition + network roundtrips.
    let query = r#"
		select table_name
		from information_schema.tables
		where table_schema = any (current_schemas(true))
	"#;

    match sqlx::query_scalar::<_, String>(query)
        .fetch_all(&db.pool)
        .await
    {
        Ok(rows) => rows.into_iter().collect(),
        Err(_) => HashSet::new(),
    }
}

fn slugify(s: &str) -> String {
    // Minimal slugifier (avoid pulling extra deps).
    let mut out = String::with_capacity(s.len());
    let mut last_dash = false;
    for ch in s.chars() {
        let c = ch.to_ascii_lowercase();
        if c.is_ascii_alphanumeric() {
            out.push(c);
            last_dash = false;
        } else if !last_dash {
            out.push('-');
            last_dash = true;
        }
    }
    out.trim_matches('-').to_string()
}

fn env_bool(key: &str, default: bool) -> bool {
    std::env::var(key)
        .ok()
        .map(|v| !(v == "0" || v.eq_ignore_ascii_case("false")))
        .unwrap_or(default)
}

fn env_usize(key: &str, default: usize, max: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .map(|v| v.min(max))
        .unwrap_or(default)
}

fn price_to_minor_units(price: f64, minor_unit: i16) -> i64 {
    let scale = 10_f64.powi(minor_unit as i32);
    (price * scale).round() as i64
}

/// Sync a small, bounded slice of ITAD (IsThereAnyDeal) into the canonical tables.
///
/// Design goals:
/// - Never run migrations; caller must provide a no-migrate `Db`.
/// - Be resilient to legacy/partial schemas by skipping optional linkage writes.
/// - Prefer existing shared ingestion helpers for schema drift tolerance.
pub async fn sync(db: &Db, api_key: Option<String>) -> Result<PostIngestSummary> {
    let mut summary = PostIngestSummary::default();

    let enabled = env_bool("ITAD_ENABLED", true);
    if !enabled {
        return Ok(summary);
    }

    // Schema mode detection.
    // - Modern mode writes to: sellables/offers/offer_jurisdictions/prices/current_price.
    // - PHP-compat mode writes to legacy Laravel tables: sku_regions/region_prices and uses
    //   synthetic offer/sellable ids (see ingest_providers.rs).
    let compat = php_compat_schema(db).await.unwrap_or(false);
    let tables = current_schema_tables(db).await;
    debug!(
        compat,
        table_count = tables.len(),
        "itad: discovered tables in current schemas"
    );
    let has_jurisdictions = tables.contains("jurisdictions");

    // Hard gate: without the required tables for the detected schema variant, skip.
    // (We intentionally avoid erroring; unified ingest should continue other providers.)
    let required_tables: &[&str] = if compat {
        &[
            "products",
            "currencies",
            "countries",
            "sku_regions",
            "region_prices",
        ]
    } else {
        &[
            "products",
            "currencies",
            "countries",
            "jurisdictions",
            "sellables",
            "offers",
            "offer_jurisdictions",
            "prices",
            "current_price",
        ]
    };
    for required in required_tables {
        if !tables.contains(*required) {
            warn!(
                table = *required,
                compat, "itad: required table missing; skipping ITAD sync"
            );
            return Ok(summary);
        }
    }

    // Optional linkage tables. We gate writes exactly like TGDB.
    let can_link_provider_items = tables.contains("providers") && tables.contains("provider_items");
    let can_link_provider_offers = can_link_provider_items
        && tables.contains("provider_offers")
        && tables.contains("video_game_sources");
    let can_link_media = can_link_provider_items && tables.contains("vg_source_media_links");

    if !can_link_provider_items {
        warn!(
            "itad: providers/provider_items missing; will ingest prices but skip provider linkage"
        );
    }

    // Provider client config
    let base_url =
        std::env::var("ITAD_BASE_URL").unwrap_or_else(|_| "https://api.isthereanydeal.com".into());
    let timeout_secs = std::env::var("ITAD_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(20);
    let country_code = std::env::var("ITAD_COUNTRY").unwrap_or_else(|_| "US".into());

    // Keep this intentionally small by default (portfolio + quota friendly).
    let deals_limit: u32 = env_usize("ITAD_DEALS_LIMIT", 50, 500) as u32;
    let max_game_overviews = env_usize("ITAD_MAX_GAME_OVERVIEWS", 40, 500);
    let default_tax_inclusive = env_bool("ITAD_DEFAULT_TAX_INCLUSIVE", true);
    let minor_unit_default: i16 = std::env::var("ITAD_MINOR_UNIT_DEFAULT")
        .ok()
        .and_then(|v| v.parse::<i16>().ok())
        .unwrap_or(2);

    let provider = ItadProvider::new(Some(&base_url), Some(timeout_secs))
        .context("itad: build provider")?
        .with_api_key(api_key);

    // Fetch recent deals.
    info!(
        country = %country_code,
        deals_limit,
        "itad: fetching latest deals"
    );
    let deals = provider
        .get_latest_deals_for_country(Some(deals_limit), Some(0), Some(&country_code))
        .await
        .context("itad: get_latest_deals")?;
    info!(deal_count = deals.len(), "itad: deals fetched");

    if deals.is_empty() {
        info!("itad: no deals returned");
        return Ok(summary);
    }

    // Collect unique game ids and pull overviews/media for a bounded subset.
    let mut unique_game_ids: BTreeSet<String> = BTreeSet::new();
    for d in &deals {
        if !d.game_id.trim().is_empty() {
            unique_game_ids.insert(d.game_id.clone());
        }
    }
    let mut game_overviews: HashMap<String, (ItadGame, Vec<ItadMedia>)> = HashMap::new();
    if max_game_overviews > 0 {
        info!(
            max_game_overviews,
            unique_game_count = unique_game_ids.len(),
            "itad: fetching game overviews (bounded)"
        );
    }
    for game_id in unique_game_ids.into_iter().take(max_game_overviews) {
        match provider.game_overview(&game_id).await {
            Ok((game, media)) => {
                game_overviews.insert(game_id, (game, media));
            }
            Err(err) => {
                warn!(%err, "itad: failed game_overview; continuing");
            }
        }
    }

    // Ensure country/jurisdiction once.
    // Note: for a production-grade solution we'd have a proper ISO country registry.
    // For now we keep it stable and harmless (name defaults to the code).
    let default_currency_code =
        std::env::var("ITAD_DEFAULT_CURRENCY").unwrap_or_else(|_| "USD".into());
    let country_name = std::env::var("ITAD_COUNTRY_NAME").unwrap_or_else(|_| country_code.clone());
    let fallback_currency_id = ensure_currency(
        db,
        &default_currency_code,
        &default_currency_code,
        minor_unit_default,
    )
    .await
    .context("itad: ensure fallback currency")?;
    let country_id = ensure_country(db, &country_code, &country_name, fallback_currency_id)
        .await
        .context("itad: ensure country")?;
    let jurisdiction_id = if compat {
        if has_jurisdictions {
            ensure_national_jurisdiction(db, country_id)
                .await
                .context("itad: ensure jurisdiction")?
        } else {
            // php compat fallback: allow ensure_offer_jurisdiction() to treat this value as a
            // country_id when jurisdictions table is absent.
            country_id
        }
    } else {
        ensure_national_jurisdiction(db, country_id)
            .await
            .context("itad: ensure jurisdiction")?
    };

    // Ensure provider (optional).
    let provider_id = if can_link_provider_items {
        Some(
            ensure_provider(db, "IsThereAnyDeal", "catalog", Some("itad"))
                .await
                .context("itad: ensure provider")?,
        )
    } else {
        None
    };

    // Ingest prices in batches.
    let now: DateTime<Utc> = Utc::now();
    let mut price_rows: Vec<PriceRow> = Vec::with_capacity(deals.len().min(1000));
    for deal in deals {
        if deal.game_id.trim().is_empty() {
            continue;
        }
        let game_id: &str = deal.game_id.as_str();

        // Title + media if we managed to fetch overview; otherwise fall back to the deal title.
        let (title, media) = match game_overviews.get(game_id) {
            Some((game, media)) => (game.title.clone(), media.clone()),
            None => (format!("ITAD {game_id}"), vec![]),
        };

        let product_slug = format!("itad-{}", slugify(&title));
        let product_id = ensure_product_named(db, "software", &product_slug, &title)
            .await
            .with_context(|| format!("itad: ensure product '{title}'"))?;

        let sellable_id = ensure_sellable(db, "software", product_id)
            .await
            .context("itad: ensure sellable")?;

        // Retailer is the store from the deal. We namespace slugs to avoid collisions with
        // first-party retailer entries (e.g., 'steam').
        let retailer_slug = format!("itad:shop:{}", deal.store_id);
        let retailer_id = ensure_retailer(db, &deal.store_name, Some(&retailer_slug))
            .await
            .context("itad: ensure retailer")?;

        // SKU isn't stable/available from ITAD for all shops; keep null.
        let offer_id = ensure_offer(db, sellable_id, retailer_id, None)
            .await
            .context("itad: ensure offer")?;

        // Determine currency for the deal; fall back to ITAD_DEFAULT_CURRENCY.
        let currency_code = if deal.currency.trim().is_empty() {
            default_currency_code.as_str()
        } else {
            deal.currency.as_str()
        };
        let currency_id = ensure_currency(db, currency_code, currency_code, minor_unit_default)
            .await
            .context("itad: ensure currency")?;

        let offer_jurisdiction_id =
            ensure_offer_jurisdiction(db, offer_id, jurisdiction_id, currency_id)
                .await
                .context("itad: ensure offer jurisdiction")?;

        // Optional provider linkage: we use a stable game-level provider item so media doesn't
        // duplicate per store.
        let video_game_source_id = if let Some(provider_id) = provider_id {
            let video_game_source_id = ensure_provider_item(
                db,
                provider_id,
                &format!("itad:{game_id}"),
                Some(serde_json::json!({"provider": "itad", "game_id": game_id})),
            )
            .await
            .context("itad: ensure provider item")?;
            summary.record_provider_item(video_game_source_id);
            Some(video_game_source_id)
        } else {
            None
        };

        if let (Some(video_game_source_id), true) = (video_game_source_id, can_link_provider_offers)
        {
            // Link provider item to offer.
            if let Err(err) =
                link_provider_offer(db, video_game_source_id, offer_id, Some(0.70)).await
            {
                warn!(%err, "itad: link_provider_offer failed; continuing");
            }
        }

        if let (Some(video_game_source_id), true) = (video_game_source_id, can_link_media) {
            if !media.is_empty() {
                // Best-effort media linking (skip if tables missing).
                let mut urls: Vec<(String, Option<String>, Option<String>, Option<String>)> =
                    Vec::with_capacity(media.len());
                for m in media {
                    if m.url.trim().is_empty() {
                        continue;
                    }
                    urls.push((m.url, Some(m.r#type), Some(m.role), None));
                }

                if !urls.is_empty() {
                    if let Err(err) = ensure_vg_source_media_links_with_meta(
                        db,
                        video_game_source_id,
                        None,
                        &urls,
                        "itad",
                        Some(serde_json::json!({"provider": "itad", "game_id": game_id})),
                    )
                    .await
                    {
                        warn!(%err, "itad: ensure_vg_source_media_links_with_meta failed; continuing");
                    }
                }
            }
        }

        // Convert price → minor units.
        let amount_minor = price_to_minor_units(deal.price, minor_unit_default);

        // Build a price row.
        price_rows.push(PriceRow {
            offer_jurisdiction_id,
            video_game_source_id,
            recorded_at: now,
            amount_minor,
            tax_inclusive: default_tax_inclusive,
            fx_minor_per_unit: None,
            btc_sats_per_unit: None,
            meta: serde_json::json!({
                "source": "itad",
                "deal": {
                    "store_id": deal.store_id,
                    "store_name": deal.store_name,
                    "game_id": game_id,
                    "url": deal.url,
                    "currency": deal.currency,
                    "price": deal.price,
                    "regular_price": deal.regular_price,
                    "discount": deal.discount,
                }
            }),
        });

        if price_rows.len() >= 500 {
            let batch_len = price_rows.len();
            let res = ingest_prices(db, std::mem::take(&mut price_rows))
                .await
                .context("itad: ingest_prices")?;
            summary.record_batch(batch_len, &res);
            price_rows.clear();
        }
    }

    if !price_rows.is_empty() {
        let batch_len = price_rows.len();
        let res = ingest_prices(db, price_rows)
            .await
            .context("itad: ingest_prices")?;
        summary.record_batch(batch_len, &res);
    }

    info!(
        total_price_rows_written = summary.total_price_rows_written,
        total_current_updates = summary.total_current_updates,
        touched_offer_jurisdictions = summary.offer_jurisdiction_ids.len(),
        "itad: sync complete"
    );

    Ok(summary)
}
