use std::collections::HashMap;
use std::collections::HashSet;
use std::ffi::OsStr;
use std::fs;
use std::io::Write;
use std::path::PathBuf;

use anyhow::{Context, Result};
use chrono::Utc;
use regex::Regex;
use reqwest::Client;
use serde_json::{json, Map, Value};
use tracing::info;
use url::Url;

use crate::database_ops::playstation::dump_categories;
use crate::database_ops::playstation::dump_detail::{self, DumpDetailOptions};
use crate::database_ops::playstation::{
    dump_prices, export_products, genre_scan, ingest_demo, prices, prices_debug, ratings, raw,
    search_categories,
};
use psstore_client::{PsConfig, PsStoreClient};

#[derive(Debug, Clone, Default)]
pub struct PricesCommandConfig {
    pub regions: Option<String>,
    pub max_pages: Option<u32>,
    pub page_size: Option<u32>,
}

#[derive(Debug, Clone, Default)]
pub struct RatingsCommandConfig {
    pub locale: Option<String>,
    pub max_pages_ps4: Option<u32>,
    pub max_pages_ps5: Option<u32>,
    pub page_size: Option<u32>,
    pub dry_run: bool,
}

#[derive(Debug, Clone, Default)]
pub struct DumpDetailCommandConfig {
    pub product_id: Option<String>,
    pub locale: Option<String>,
    pub out_path: Option<PathBuf>,
}

#[derive(Debug, Clone, Default)]
pub struct CountRangeCommandConfig {
    pub regions: Option<String>,
    pub ps4_category: Option<String>,
    pub ps5_category: Option<String>,
    pub page_size: Option<u32>,
    pub total_pages: Option<u32>,
    pub cutoff_year: Option<i32>,
    pub rps_per_locale: Option<u32>,
    pub retry_attempts: Option<u32>,
    pub retry_backoff_ms: Option<u64>,
}

#[derive(Debug, Clone, Default)]
pub struct PriceProbeCommandConfig {
    pub locale: Option<String>,
    pub product_id: Option<String>,
    pub concept_id: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct CaptureHeadersCommandConfig {
    pub regions: Option<String>,
    pub out_path: Option<PathBuf>,
    pub user_agent: Option<String>,
}

pub async fn run_prices(cfg: PricesCommandConfig) -> Result<()> {
    set_env_if_some("PS_STORE_REGIONS", cfg.regions);
    if let Some(v) = cfg.max_pages {
        set_env("PS_MAX_PAGES", v.to_string());
    }
    if let Some(v) = cfg.page_size {
        set_env("PS_PAGE_SIZE", v.to_string());
    }
    set_env("AUTO_MIGRATE", "0");
    prices::run_from_env().await
}

pub async fn run_ratings(cfg: RatingsCommandConfig) -> Result<()> {
    set_env_if_some("PS_LOCALE", cfg.locale);
    if let Some(v) = cfg.max_pages_ps4 {
        set_env("MAX_PAGES_PS4", v.to_string());
    }
    if let Some(v) = cfg.max_pages_ps5 {
        set_env("MAX_PAGES_PS5", v.to_string());
    }
    if let Some(v) = cfg.page_size {
        set_env("PAGE_SIZE", v.to_string());
    }
    if cfg.dry_run {
        set_env("DRY_RUN", "1");
    }
    set_env("AUTO_MIGRATE", "0");

    if try_enqueue_worker("ratings").await? {
        return Ok(());
    }

    ratings::run_from_env().await
}

pub fn run_dump_categories() -> Result<()> {
    let rows = dump_categories::run_from_env()?;
    dump_categories::write_csv(&rows)?;
    Ok(())
}

pub async fn run_dump_detail(cfg: DumpDetailCommandConfig) -> Result<()> {
    let options = DumpDetailOptions {
        product_id: cfg.product_id,
        locale: cfg.locale,
        out_path: cfg.out_path,
    };
    let res = dump_detail::run(options).await?;

    if res.wrote_path.is_none() {
        println!("{}", serde_json::to_string_pretty(&res.detail)?);
    } else if let Some(path) = res.wrote_path.as_ref() {
        println!("wrote detail JSON to {}", path.display());
    }

    if let Some(ps_long) = res.ps_long_description.as_ref() {
        eprintln!("\n---\nPS LONG Description (cleaned):\n{}\n---", ps_long);
    }
    if let Some(txt) = res.heuristic_description.as_ref() {
        eprintln!(
            "\n---\nHeuristic longest description-like text (cleaned):\n{}\n---",
            txt
        );
    }

    Ok(())
}

pub async fn run_dump_prices() -> Result<()> {
    dump_prices::run_from_env().await
}

pub async fn run_dump_raw() -> Result<()> {
    raw::print_from_env().await
}

pub async fn run_export_products() -> Result<()> {
    export_products::run_from_env().await
}

pub async fn run_genre_scan() -> Result<()> {
    genre_scan::run_from_env().await
}

pub async fn run_ingest_demo() -> Result<()> {
    ingest_demo::run_from_env().await
}

pub async fn run_prices_debug() -> Result<()> {
    prices_debug::run_from_env().await
}

pub async fn run_search_categories() -> Result<()> {
    search_categories::run_from_env().await
}

pub async fn run_count_range(cfg: CountRangeCommandConfig) -> Result<()> {
    init_tracing();
    let regions = parse_regions(
        cfg.regions
            .or_else(|| std::env::var("PS_STORE_REGIONS").ok()),
    );
    if regions.is_empty() {
        anyhow::bail!("no PS_STORE_REGIONS configured (env or CLI)");
    }

    let cat_ps4 = cfg
        .ps4_category
        .or_else(|| std::env::var("PS4_CATEGORY").ok())
        .unwrap_or_else(|| "44d8bb20-653e-431e-8ad0-c0a365f68d2f".into());
    let cat_ps5 = cfg
        .ps5_category
        .or_else(|| std::env::var("PS5_CATEGORY").ok())
        .unwrap_or_else(|| "4cbf39e2-5749-4970-ba81-93a489e4570c".into());
    let page_size = cfg
        .page_size
        .or_else(|| {
            std::env::var("PS_PAGE_SIZE")
                .ok()
                .and_then(|s| s.parse().ok())
        })
        .unwrap_or(500);
    let max_pages = cfg
        .total_pages
        .or_else(|| {
            std::env::var("PS_TOTAL_PAGES")
                .ok()
                .and_then(|s| s.parse().ok())
        })
        .unwrap_or(500);
    let cutoff_year = cfg
        .cutoff_year
        .or_else(|| {
            std::env::var("PS_CUTOFF_YEAR")
                .ok()
                .and_then(|s| s.parse().ok())
        })
        .unwrap_or(2018);
    let rps_per_locale = cfg
        .rps_per_locale
        .or_else(|| {
            std::env::var("PS_STORE_RPS")
                .ok()
                .and_then(|s| s.parse::<f32>().ok())
                .map(|f| f.ceil() as u32)
        })
        .filter(|v| *v > 0)
        .unwrap_or(3);
    let retry_attempts = cfg
        .retry_attempts
        .or_else(|| {
            std::env::var("PS_STORE_MAX_RETRIES")
                .ok()
                .and_then(|s| s.parse().ok())
        })
        .unwrap_or(3);
    let retry_base_ms = cfg
        .retry_backoff_ms
        .or_else(|| {
            std::env::var("PS_STORE_BACKOFF_MS")
                .ok()
                .and_then(|s| s.parse().ok())
        })
        .unwrap_or(300);

    let mut report: Map<String, Value> = Map::new();
    let mut grand_total = 0usize;

    for locale in regions {
        let cfg = PsConfig {
            locales: vec![locale.clone()],
            rps: rps_per_locale,
            retry_attempts,
            retry_base_delay_ms: retry_base_ms,
            ..PsConfig::default()
        };
        let client = PsStoreClient::new(cfg);

        let ps5 = client
            .category_grid_retrieve_desc_until_year(
                &locale,
                &cat_ps5,
                cutoff_year,
                page_size,
                max_pages,
            )
            .await
            .unwrap_or_default();
        let ps4 = client
            .category_grid_retrieve_desc_until_year(
                &locale,
                &cat_ps4,
                cutoff_year,
                page_size,
                max_pages,
            )
            .await
            .unwrap_or_default();

        info!(locale = %locale, ps5 = ps5.len(), ps4 = ps4.len(), "ps count page totals");
        let mut seen: HashSet<String> = HashSet::new();
        let mut total_locale = 0usize;
        for item in ps5.iter().chain(ps4.iter()) {
            if let Some(pid) = &item.product_id {
                if seen.insert(pid.clone()) {
                    total_locale += 1;
                }
            } else {
                total_locale += 1;
            }
        }
        grand_total += total_locale;
        report.insert(
            locale.clone(),
            json!({
                "total": total_locale,
                "ps5": ps5.len(),
                "ps4": ps4.len(),
            }),
        );
    }

    report.insert("grand_total".into(), json!(grand_total));
    let out = Value::Object(report);
    println!("{}", serde_json::to_string_pretty(&out)?);
    Ok(())
}

pub async fn run_price_probe(cfg: PriceProbeCommandConfig) -> Result<()> {
    init_tracing();
    let base_cfg = PsConfig::default();
    let locale = cfg
        .locale
        .or_else(|| std::env::var("PS_LOCALE").ok())
        .or_else(|| base_cfg.locales.get(0).cloned())
        .unwrap_or_else(|| "en-us".to_string());
    let client = PsStoreClient::new(base_cfg.clone());

    let concept_id = if let Some(c) = cfg
        .concept_id
        .or_else(|| std::env::var("PS_CONCEPT_ID").ok())
    {
        c
    } else if let Some(pid) = cfg
        .product_id
        .or_else(|| std::env::var("PS_PRODUCT_ID").ok())
    {
        let v = client
            .concept_by_product_id_raw(&locale, &pid)
            .await
            .context("metGetConceptByProductIdQuery failed")?;
        extract_concept_id(&v).context("conceptId not found in response")?
    } else {
        anyhow::bail!("provide --product-id / --concept-id or set PS_PRODUCT_ID / PS_CONCEPT_ID");
    };

    let pricing = client
        .concept_pricing_raw(&locale, &concept_id)
        .await
        .context("metGetPricingDataByConceptId failed")?;
    let (base_minor, discount_minor) = extract_prices_minor(&pricing);
    println!(
        "locale={} concept_id={} base_minor={:?} discount_minor={:?}",
        locale, concept_id, base_minor, discount_minor
    );
    Ok(())
}

pub async fn run_capture_headers(cfg: CaptureHeadersCommandConfig) -> Result<()> {
    init_tracing();
    let regions = parse_regions(
        cfg.regions
            .or_else(|| std::env::var("PS_STORE_REGIONS").ok()),
    );
    if regions.is_empty() {
        anyhow::bail!("no PS_STORE_REGIONS configured (env or CLI)");
    }

    let ua = cfg
        .user_agent
        .or_else(|| std::env::var("PS_STORE_UA").ok())
        .unwrap_or_else(|| {
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0 Safari/537.36".to_string()
        });

    let client = Client::builder()
        .user_agent(&ua)
        .build()
        .context("building reqwest client")?;

    let mut combined: HashMap<String, String> = HashMap::new();
    let mut captures: Vec<Value> = Vec::new();

    let script_re = Regex::new(r#"src=\"([^\"]+\.js[^\"]*)\""#)?;
    // Broaden to handle minified bundles, single quotes, and extra clutter between op name/hash
    let op_re = Regex::new(
        r#"operationName"\s*[:=]\s*[\"']([^\"']+?)[\"'][^\{\}]{0,600}?"sha256Hash"\s*[:=]\s*"([a-fA-F0-9]{64})""#,
    )?;

    let mut aggregated_hashes: HashMap<String, String> = HashMap::new();

    for locale in regions {
        let url = format!("https://store.playstation.com/{}/pages/latest", locale);
        let resp = client
            .get(&url)
            .send()
            .await
            .with_context(|| format!("request failed for locale {locale}"))?;
        let header_map = resp.headers().clone();
        let html = resp.text().await.unwrap_or_default();
        let set_cookie_raw: Vec<String> = header_map
            .get_all("set-cookie")
            .iter()
            .filter_map(|h| h.to_str().ok().map(|s| s.to_string()))
            .collect();

        let mut hashes: HashMap<String, String> = HashMap::new();
        // Inline HTML scan for any persistedQuery snippets (fallback if script fetch misses)
        for m in op_re.captures_iter(&html) {
            let op_name = m.get(1).map(|x| x.as_str()).unwrap_or("");
            let hash = m.get(2).map(|x| x.as_str()).unwrap_or("");
            if !op_name.is_empty() && !hash.is_empty() {
                hashes
                    .entry(op_name.to_string())
                    .or_insert_with(|| hash.to_string());
            }
        }

        if let Ok(base) = Url::parse(&url) {
            for cap in script_re.captures_iter(&html) {
                let src = cap.get(1).map(|m| m.as_str()).unwrap_or("");
                if src.is_empty() {
                    continue;
                }
                let script_url = if let Ok(u) = Url::parse(src) {
                    u
                } else if let Ok(u) = base.join(src) {
                    u
                } else {
                    continue;
                };
                // Fetch script body (best-effort, with 2 MB limit to avoid huge bundles)
                if let Ok(resp_js) = client.get(script_url.clone()).send().await {
                    if let Ok(bytes) = resp_js.bytes().await {
                        if bytes.len() > 2 * 1024 * 1024 {
                            continue;
                        }
                        if let Ok(text) = std::str::from_utf8(&bytes) {
                            for m in op_re.captures_iter(text) {
                                let op_name = m.get(1).map(|x| x.as_str()).unwrap_or("");
                                let hash = m.get(2).map(|x| x.as_str()).unwrap_or("");
                                if !op_name.is_empty() && !hash.is_empty() {
                                    hashes
                                        .entry(op_name.to_string())
                                        .or_insert_with(|| hash.to_string());
                                }
                            }
                        }
                    }
                }
            }
        }

        let mut pairs: Vec<String> = Vec::new();
        for v in &set_cookie_raw {
            if let Some((item, _rest)) = v.split_once(';') {
                if let Some((name, val)) = item.split_once('=') {
                    let name = name.trim();
                    let val = val.trim();
                    if !name.is_empty() && !val.is_empty() {
                        combined
                            .entry(name.to_string())
                            .or_insert_with(|| val.to_string());
                        pairs.push(format!("{}={}", name, val));
                    }
                }
            }
        }
        let cookie_header = pairs.join("; ");
        for (k, v) in &hashes {
            aggregated_hashes
                .entry(k.clone())
                .or_insert_with(|| v.clone());
        }

        captures.push(json!({
            "locale": locale,
            "url": url,
            "set_cookie_raw": set_cookie_raw,
            "cookie_header": cookie_header,
            "hashes": hashes,
            "fetched_at": Utc::now().to_rfc3339(),
            "user_agent": ua,
        }));
    }

    let combined_cookie_header: String = combined
        .iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect::<Vec<_>>()
        .join("; ");

    let out_json = json!({
        "user_agent": ua,
        "combined_cookie_header": combined_cookie_header,
        "locales": captures,
        "aggregated_hashes": aggregated_hashes,
        "generated_at": Utc::now().to_rfc3339(),
    });

    let out_path = cfg
        .out_path
        .unwrap_or_else(|| PathBuf::from("ps_cookies.json"));
    if let Some(parent) = out_path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).context("creating output parent directory")?;
        }
    }
    let mut f =
        fs::File::create(&out_path).with_context(|| format!("creating {}", out_path.display()))?;
    f.write_all(serde_json::to_string_pretty(&out_json)?.as_bytes())
        .with_context(|| format!("writing {}", out_path.display()))?;

    let cookie_txt_path = out_path.with_extension("cookie.txt");
    fs::write(&cookie_txt_path, &combined_cookie_header)
        .with_context(|| format!("writing {}", cookie_txt_path.display()))?;

    println!(
        "wrote PS Store cookie JSON to {} and combined header to {}",
        out_path.display(),
        cookie_txt_path.display()
    );
    println!(
        "Set PS_STORE_COOKIE_FILE={} or PS_STORE_COOKIE to reuse",
        cookie_txt_path.display()
    );

    Ok(())
}

fn set_env<K, V>(key: K, value: V)
where
    K: AsRef<OsStr>,
    V: AsRef<OsStr>,
{
    unsafe {
        std::env::set_var(key, value);
    }
}

fn set_env_if_some<K, V>(key: K, value: Option<V>)
where
    K: AsRef<OsStr>,
    V: AsRef<OsStr>,
{
    if let Some(v) = value {
        unsafe {
            std::env::set_var(key, v);
        }
    }
}

async fn try_enqueue_worker(task: &str) -> Result<bool> {
    let addr = match std::env::var("WORKER_HTTP_ADDR") {
        Ok(v) if !v.trim().is_empty() => v,
        _ => {
            return Ok(false);
        }
    };
    let url = format!("http://{addr}/api/enqueue");
    let body = json!({
        "provider": "ps",
        "task": task,
    });
    let client = Client::new();
    let res = client.post(&url).json(&body).send().await;
    match res {
        Ok(resp) if resp.status().is_success() => {
            println!("Enqueued PlayStation {task} via worker at {addr}");
            Ok(true)
        }
        Ok(resp) => {
            eprintln!(
                "PlayStation {task} worker enqueue failed with status {} — falling back to direct execution",
                resp.status()
            );
            Ok(false)
        }
        Err(err) => {
            eprintln!(
                "PlayStation {task} worker enqueue errored ({err}) — falling back to direct execution"
            );
            Ok(false)
        }
    }
}

fn parse_regions(raw: Option<String>) -> Vec<String> {
    raw.unwrap_or_else(|| "en-us en-gb de-de".into())
        .split(|c: char| (c == ',' || c == ' '))
        .filter(|s| !s.is_empty())
        .map(|s| s.trim().to_lowercase())
        .collect()
}

fn extract_concept_id(v: &Value) -> Option<String> {
    v.get("data")
        .and_then(|d| d.get("metGetConceptByProductIdQuery"))
        .and_then(|n| n.get("conceptId"))
        .and_then(|s| s.as_str())
        .map(|s| s.to_string())
}

fn extract_prices_minor(v: &Value) -> (Option<i64>, Option<i64>) {
    fn parse_money_str_to_minor(s: &str) -> Option<i64> {
        let mut normalized = s.replace(',', ".");
        normalized.retain(|c| (c.is_ascii_digit() || c == '.'));
        if normalized.is_empty() {
            return None;
        }
        let mut parts = normalized.splitn(2, '.');
        let int_part = parts.next().unwrap_or("");
        let frac_part = parts.next();
        if let Some(frac) = frac_part {
            let mut frac_norm = frac
                .chars()
                .filter(|c| c.is_ascii_digit())
                .collect::<String>();
            if frac_norm.is_empty() {
                frac_norm.push_str("00");
            } else if frac_norm.len() == 1 {
                frac_norm.push('0');
            } else if frac_norm.len() > 2 {
                frac_norm.truncate(2);
            }
            let total = format!("{}{}", int_part, frac_norm);
            total.parse::<i64>().ok()
        } else {
            int_part.parse::<i64>().ok().map(|v| v * 100)
        }
    }

    fn walk(obj: &Value, out: &mut (Option<i64>, Option<i64>)) {
        match obj {
            Value::Object(map) => {
                if let Some(Value::String(s)) = map.get("basePrice") {
                    if out.0.is_none() {
                        out.0 = parse_money_str_to_minor(s);
                    }
                }
                if let Some(Value::String(s)) = map.get("discountedPrice") {
                    if out.1.is_none() {
                        out.1 = parse_money_str_to_minor(s);
                    }
                }
                if let Some(Value::Number(n)) = map.get("basePriceMinor") {
                    if out.0.is_none() {
                        out.0 = n.as_i64();
                    }
                }
                if let Some(Value::Number(n)) = map.get("discountedPriceMinor") {
                    if out.1.is_none() {
                        out.1 = n.as_i64();
                    }
                }
                for (_k, v) in map {
                    walk(v, out);
                }
            }
            Value::Array(arr) => {
                for v in arr {
                    walk(v, out);
                }
            }
            _ => {}
        }
    }

    let mut out = (None, None);
    walk(v, &mut out);
    out
}

fn init_tracing() {
    if tracing::dispatcher::has_been_set() {
        return;
    }
    let filter = std::env::var("RUST_LOG").unwrap_or_else(|_| "debug".into());
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .without_time()
        .compact()
        .init();
}
