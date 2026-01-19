use anyhow::{anyhow, Result};
use chrono::Utc;
use i_miss_rust::database_ops::db::{Db, PriceRow};
use i_miss_rust::database_ops::ingest_providers::{
    ensure_offer, ensure_offer_jurisdiction, ensure_product_named, ensure_provider,
    ensure_provider_item, ensure_retailer, ensure_sellable, ingest_prices, link_provider_offer,
    PostIngestSummary,
};
use i_miss_rust::database_ops::media_map::MediaMap;
use i_miss_rust::util::env as env_util;
use serde::Deserialize;
use sqlx::Row;
use std::env;
use tracing_subscriber::{fmt, EnvFilter};

#[derive(Debug, Deserialize)]
struct ItadRow {
    id: String,
    #[serde(rename = "console-name")]
    console_name: String,
    #[serde(rename = "product-name")]
    product_name: String,
    #[serde(rename = "loose-price")]
    loose_price: Option<String>,
}

fn parse_usd_minor(price: &str) -> Option<i64> {
    // Expect formats like "$11.96" or empty
    let s = price.trim().trim_start_matches('$');
    if s.is_empty() {
        return None;
    }
    match s.parse::<f64>() {
        Ok(v) => Some((v * 100.0).round() as i64),
        Err(_) => None,
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_util::bootstrap_cli("ingest_itad");
    fmt().with_env_filter(EnvFilter::from_default_env()).init();
    let database_url = env::var("SUPABASE_DB_URL")
        .or_else(|_| env::var("DATABASE_URL"))
        .map_err(|_| anyhow!("DATABASE_URL not set"))?;
    let path = env::args()
        .nth(1)
        .unwrap_or_else(|| "price-guide.csv".to_string());

    let db = Db::connect(&database_url, 10).await?;

    // Optional media map
    let media_limit = std::env::var("MEDIA_MAP_LIMIT")
        .ok()
        .and_then(|s| s.parse::<usize>().ok());
    let media =
        MediaMap::from_file("merged_final.json", media_limit).unwrap_or_else(|_| MediaMap::empty());

    // Ensure baseline refs
    let usd_id = ensure_currency(&db, "USD", "US Dollar", 2).await?;
    let us_country = ensure_country(&db, "US", "United States", usd_id).await?;
    let us_nat = ensure_national_jurisdiction(&db, us_country).await?;
    let itad_provider = ensure_provider(&db, "itad", "pricing_catalogue", Some("itad")).await?;
    let retailer = ensure_retailer(
        &db,
        "nexarda:retailer_catalogue",
        Some("nexarda:retailer_catalogue"),
    )
    .await?;

    let mut rdr = csv::Reader::from_path(&path)?;
    let mut batch: Vec<PriceRow> = Vec::with_capacity(1000);
    let mut seen_count = 0usize;
    let mut post_summary = PostIngestSummary::default();

    for rec in rdr.deserialize() {
        let row: ItadRow = match rec {
            Ok(r) => r,
            Err(err) => {
                eprintln!("skip bad row: {err:?}");
                continue;
            }
        };
        let Some(price_str) = row.loose_price.as_deref() else {
            continue;
        };
        let Some(amount_minor) = parse_usd_minor(price_str) else {
            continue;
        };

        // Normalize product slug
        let slug = row.product_name.to_lowercase().replace(' ', "-");
        let product_id = ensure_product_named(&db, "software", &slug, &row.product_name).await?;
        let sellable_id = ensure_sellable(&db, "software", product_id).await?;
        let offer_id = ensure_offer(&db, sellable_id, retailer, None).await?;
        let oj_id = ensure_offer_jurisdiction(&db, offer_id, us_nat, usd_id).await?;
        let ext_id = format!("itad:{}:{}", row.console_name, row.id);
        let item_id = ensure_provider_item(&db, itad_provider, &ext_id, None).await?;
        link_provider_offer(&db, item_id, offer_id, Some(0.7)).await?;
        post_summary.record_provider_item(item_id);

        // Optionally link media URL
        if let Some(url) = media.get(&row.product_name) {
            upsert_provider_media_url(&db, item_id, url).await.ok();
        }

        let now = Utc::now();
        batch.push(PriceRow {
            offer_jurisdiction_id: oj_id,
            video_game_source_id: Some(item_id),
            recorded_at: now,
            amount_minor,
            tax_inclusive: true,
            fx_minor_per_unit: None,
            btc_sats_per_unit: None,
            meta: serde_json::json!({"source":"itad","console": row.console_name}),
        });

        if batch.len() >= 1000 {
            let take = std::mem::take(&mut batch);
            let batch_len = take.len();
            let ingest_result = ingest_prices(&db, take).await?;
            post_summary.record_batch(batch_len, &ingest_result);
            seen_count += batch_len;
            eprintln!("ingested {} rows...", seen_count);
        }
    }

    if !batch.is_empty() {
        let batch_len = batch.len();
        let ingest_result = ingest_prices(&db, batch).await?;
        post_summary.record_batch(batch_len, &ingest_result);
    }

    post_summary.verify(&db, itad_provider).await?;

    Ok(())
}

async fn ensure_currency(db: &Db, code: &str, name: &str, minor_unit: i16) -> Result<i64> {
    if let Some(rec) = sqlx::query("SELECT id FROM public.currencies WHERE code = $1")
        .bind(code)
        .fetch_optional(&db.pool)
        .await?
    {
        return Ok(rec.get::<i64, _>("id"));
    }
    let rec = sqlx::query(
        "INSERT INTO public.currencies (code, name, minor_unit) VALUES ($1,$2,$3) RETURNING id",
    )
    .bind(code)
    .bind(name)
    .bind(minor_unit)
    .fetch_one(&db.pool)
    .await?;
    Ok(rec.get::<i64, _>("id"))
}

async fn ensure_country(db: &Db, code2: &str, name: &str, currency_id: i64) -> Result<i64> {
    if let Some(rec) = sqlx::query("SELECT id FROM public.countries WHERE code2 = $1")
        .bind(code2)
        .fetch_optional(&db.pool)
        .await?
    {
        return Ok(rec.get::<i64, _>("id"));
    }
    let rec = sqlx::query(
        "INSERT INTO public.countries (code2, name, currency_id) VALUES ($1,$2,$3) RETURNING id",
    )
    .bind(code2)
    .bind(name)
    .bind(currency_id)
    .fetch_one(&db.pool)
    .await?;
    Ok(rec.get::<i64, _>("id"))
}

async fn ensure_national_jurisdiction(db: &Db, country_id: i64) -> Result<i64> {
    if let Some(rec) = sqlx::query(
        "SELECT id FROM public.jurisdictions WHERE country_id=$1 AND region_code IS NULL",
    )
    .bind(country_id)
    .fetch_optional(&db.pool)
    .await?
    {
        return Ok(rec.get::<i64, _>("id"));
    }
    let rec = sqlx::query(
        "INSERT INTO public.jurisdictions (country_id, region_code) VALUES ($1, NULL) RETURNING id",
    )
    .bind(country_id)
    .fetch_one(&db.pool)
    .await?;
    Ok(rec.get::<i64, _>("id"))
}

async fn upsert_provider_media_url(db: &Db, video_game_source_id: i64, url: &str) -> Result<i64> {
    if let Some(rec) = sqlx::query(
        "SELECT id FROM public.provider_media_links WHERE video_game_source_id=$1 AND url=$2",
    )
    .bind(video_game_source_id)
    .bind(url)
    .fetch_optional(&db.pool)
    .await?
    {
        return Ok(rec.get::<i64, _>("id"));
    }
    // Classify coarse kind and human-readable title
    let url_l = url.to_ascii_lowercase();
    let kind = if url_l.ends_with(".mp4") || url_l.ends_with(".webm") || url_l.ends_with(".m3u8") {
        "video"
    } else {
        "image"
    };
    let title = if kind == "video" {
        if url_l.contains("trailer") {
            "trailer"
        } else if url_l.contains("gameplay") {
            "gameplay"
        } else {
            "video"
        }
    } else if url_l.contains("cover") {
        "cover"
    } else if url_l.contains("hero") {
        "hero"
    } else {
        "screenshot"
    };
    let rec = sqlx
        ::query(
            "INSERT INTO public.provider_media_links (video_game_source_id, url, kind, title) VALUES ($1,$2,$3::media_kind,$4) RETURNING id"
        )
        .bind(video_game_source_id)
        .bind(url)
        .bind(kind)
        .bind(title)
        .fetch_one(&db.pool).await?;
    Ok(rec.get::<i64, _>("id"))
}
