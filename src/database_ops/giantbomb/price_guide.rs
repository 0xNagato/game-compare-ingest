use crate::database_ops::db::{CurrentPriceRow, Db, PriceRow};
use crate::database_ops::ingest_providers::{
    ensure_country, ensure_currency, ensure_national_jurisdiction, ensure_provider,
    ensure_retailer, link_provider_offer, ProviderEntityCache,
};
use crate::util::env as env_util;
use anyhow::{anyhow, Context, Result};
use atoi::atoi;
use chrono::{DateTime, Utc};
use csv::{ByteRecord, ReaderBuilder};
use memchr::memchr;
use serde_json::json;
use std::{collections::HashMap, env, fs::File, io::BufReader};

pub async fn run_import(fast: bool) -> Result<()> {
    let database_url = if let Some(v) = env_util::ipv6_db_url() {
        v
    } else {
        env_util::db_url_prefer_session().context(
            "Database URL not configured; set SUPABASE_IPV6_DB / SUPABASE_DB_URL / DATABASE_URL",
        )?
    };
    let path = env::args()
        .nth(1)
        .unwrap_or_else(|| "price-guide.csv".to_string());
    let db = Db::connect(&database_url, 10).await?;

    // Ensure base references (USD/US)
    let usd_id = ensure_currency(&db, "USD", "US Dollar", 2).await?;
    let us_id = ensure_country(&db, "US", "United States", usd_id).await?;
    let us_nat_id = ensure_national_jurisdiction(&db, us_id).await?;

    // Provider + Retailer (catalog style)
    let provider_id = ensure_provider(&db, "itad", "pricing_catalogue", Some("itad")).await?;
    let (retailer_name, retailer_slug, provider_namespace) = if fast {
        (
            "ITAD Price Guide Fast",
            Some("itad_price_guide_fast"),
            "itad_price_guide_fast",
        )
    } else {
        (
            "ITAD Price Guide",
            Some("itad_price_guide"),
            "itad_price_guide",
        )
    };
    let retailer_id = ensure_retailer(&db, retailer_name, retailer_slug).await?;

    let file = File::open(&path)?;
    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .flexible(false)
        .trim(csv::Trim::None)
        .from_reader(BufReader::with_capacity(8 << 20, file));

    let headers = rdr.byte_headers()?.clone();
    let idx_id = headers
        .iter()
        .position(|h| h == b"id")
        .ok_or_else(|| anyhow!("id col missing"))?;
    let _idx_console = headers
        .iter()
        .position(|h| h == b"console-name")
        .ok_or_else(|| anyhow!("console-name col missing"))?;
    let idx_name = headers
        .iter()
        .position(|h| h == b"product-name")
        .ok_or_else(|| anyhow!("product-name col missing"))?;
    let idx_price = headers
        .iter()
        .position(|h| h == b"loose-price")
        .ok_or_else(|| anyhow!("loose-price col missing"))?;

    let mut rec = ByteRecord::new();
    let mut price_rows: Vec<PriceRow> = Vec::with_capacity(4096);
    let mut latest: HashMap<i64, (DateTime<Utc>, i64)> = HashMap::new();

    let mut entity_cache = ProviderEntityCache::new(db.clone());

    let now = Utc::now();
    let mut total = 0u64;

    while rdr.read_byte_record(&mut rec)? {
        let price_b = &rec[idx_price];
        if price_b.is_empty() {
            rec.clear();
            continue;
        }

        let amount_minor = parse_usd_minor_bytes(price_b).unwrap_or(0);
        if amount_minor == 0 {
            rec.clear();
            continue;
        }

        let name_b = &rec[idx_name];
        if name_b.is_empty() {
            rec.clear();
            continue;
        }
        let name = std::str::from_utf8(name_b).unwrap_or("");
        if name.is_empty() {
            rec.clear();
            continue;
        }
        let slug = slugify_bytes(name_b);

        let product_id = entity_cache
            .ensure_product_named("software", &slug, name)
            .await?;
        let sellable_id = entity_cache.ensure_sellable("software", product_id).await?;
        let offer_id = entity_cache
            .ensure_offer(sellable_id, retailer_id, None)
            .await?;
        let oj_id = entity_cache
            .ensure_offer_jurisdiction(offer_id, us_nat_id, usd_id)
            .await?;
        let ext_id = std::str::from_utf8(&rec[idx_id]).unwrap_or("");
        let provider_nat = format!("{provider_namespace}:{ext_id}");
        let meta_value = json!({
            "provider": "itad",
            "source": provider_namespace
        });
        let video_game_source_id = entity_cache
            .ensure_provider_item(provider_id, &provider_nat, Some(meta_value.clone()), false)
            .await?;
        link_provider_offer(&db, video_game_source_id, offer_id, Some(0.8)).await?;

        let row = PriceRow {
            offer_jurisdiction_id: oj_id,
            video_game_source_id: Some(video_game_source_id),
            recorded_at: now,
            amount_minor,
            tax_inclusive: true,
            fx_minor_per_unit: None,
            btc_sats_per_unit: None,
            meta: meta_value,
        };
        price_rows.push(row);

        latest
            .entry(oj_id)
            .and_modify(|e| {
                if now > e.0 {
                    *e = (now, amount_minor);
                }
            })
            .or_insert((now, amount_minor));

        total += 1;
        if price_rows.len() >= 4000 {
            flush(&db, &mut price_rows, &mut latest).await?;
        }
        rec.clear();
    }

    if !price_rows.is_empty() {
        flush(&db, &mut price_rows, &mut latest).await?;
    }
    eprintln!("ingested rows: {total}");
    Ok(())
}

async fn flush(
    db: &Db,
    rows: &mut Vec<PriceRow>,
    latest: &mut HashMap<i64, (DateTime<Utc>, i64)>,
) -> Result<()> {
    db.bulk_insert_prices(rows).await?;
    const CP_AGENT: &str = "pricing_charts";
    const CP_PRIORITY: i16 = 10;
    let cps: Vec<CurrentPriceRow> = latest
        .iter()
        .map(|(oj, (ts, amt))| CurrentPriceRow {
            offer_jurisdiction_id: *oj,
            recorded_at: *ts,
            amount_minor: *amt,
            agent: CP_AGENT.to_string(),
            agent_priority: CP_PRIORITY,
        })
        .collect();
    db.upsert_current_prices(&cps).await?;
    rows.clear();
    latest.clear();
    Ok(())
}

fn parse_usd_minor_bytes(b: &[u8]) -> Option<i64> {
    let mut start = 0usize;
    if !b.is_empty() && b[0] == b'$' {
        start = 1;
    }
    let mut buf = [0u8; 32];
    let mut len = 0usize;
    for &c in &b[start..] {
        if c == b',' {
            continue;
        }
        if (c >= b'0' && c <= b'9') || c == b'.' {
            if len < buf.len() {
                buf[len] = c;
                len += 1;
            } else {
                break;
            }
        } else {
            break;
        }
    }
    if len == 0 {
        return None;
    }
    let slice = &buf[..len];
    if let Some(dot) = memchr(b'.', slice) {
        let dollars = atoi::<i64>(&slice[..dot])? as i64;
        let cents_part = &slice[dot + 1..];
        let mut cents_buf = [b'0'; 2];
        if !cents_part.is_empty() {
            cents_buf[0] = cents_part[0];
        }
        if cents_part.len() > 1 {
            cents_buf[1] = cents_part[1];
        }
        let cents = atoi::<i64>(&cents_buf)? as i64;
        Some(dollars * 100 + cents)
    } else {
        Some((atoi::<i64>(slice)? as i64) * 100)
    }
}

fn slugify_bytes(name: &[u8]) -> String {
    let mut out = Vec::with_capacity(name.len());
    for &c in name {
        match c {
            b'0'..=b'9' => out.push(c),
            b'A'..=b'Z' => out.push(c + 32),
            b'a'..=b'z' => out.push(c),
            b' ' | b'-' | b'_' | b'.' | b'/' => out.push(b'-'),
            _ => {}
        }
    }
    let mut collapsed = Vec::with_capacity(out.len());
    let mut prev_dash = false;
    for &c in &out {
        if c == b'-' {
            if !prev_dash {
                collapsed.push(c);
                prev_dash = true;
            }
        } else {
            collapsed.push(c);
            prev_dash = false;
        }
    }
    while collapsed.first() == Some(&b'-') {
        collapsed.remove(0);
    }
    while collapsed.last() == Some(&b'-') {
        collapsed.pop();
    }
    if collapsed.is_empty() {
        return "unnamed".to_string();
    }
    unsafe { String::from_utf8_unchecked(collapsed) }
}
