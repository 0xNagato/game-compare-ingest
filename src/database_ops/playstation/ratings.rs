use crate::database_ops::media_map::normalize_title;
use crate::database_ops::playstation::prices::extract_rating_from_detail;
use crate::util::env as env_util;
use anyhow::{Context, Result};
use psstore_client::{PsConfig, PsStoreClient};
use serde_json::Value;
use sqlx::Row;
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::BufReader,
};

pub async fn run_from_env() -> Result<()> {
    let _ = dotenv::dotenv();
    let database_url = env_util::db_url().unwrap_or_default();
    let dry_run = std::env::var("DRY_RUN")
        .map(|v| (v == "1" || v.eq_ignore_ascii_case("true")))
        .unwrap_or(false);

    let locale = std::env::var("PS_LOCALE").unwrap_or_else(|_| "en-us".into());
    let cat_ps4 = std::env::var("PS4_CATEGORY")
        .unwrap_or_else(|_| "44d8bb20-653e-431e-8ad0-c0a365f68d2f".into());
    let cat_ps5 = std::env::var("PS5_CATEGORY")
        .unwrap_or_else(|_| "4cbf39e2-5749-4970-ba81-93a489e4570c".into());
    let _hash = std::env::var("PS_HASH").unwrap_or_else(|_| {
        "9845afc0dbaab4965f6563fffc703f588c8e76792000e8610843b8d3ee9c4c09".into()
    });
    let max_pages_ps4: u32 = std::env::var("MAX_PAGES_PS4")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(120);
    let max_pages_ps5: u32 = std::env::var("MAX_PAGES_PS5")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(120);
    let page_size: u32 = std::env::var("PAGE_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);
    let file_path =
        std::env::var("GAMES_DETAILED_FILE").unwrap_or_else(|_| "games_detailed.json".into());
    let search_on_miss: bool = std::env::var("SEARCH_ON_MISS")
        .map(|v| (v == "1" || v.eq_ignore_ascii_case("true")))
        .unwrap_or(true);
    let title_search_pages: u32 = std::env::var("TITLE_SEARCH_PAGES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3);

    let cfg = PsConfig {
        locales: vec![locale.clone()],
        rps: 3,
        retry_attempts: 5,
        retry_base_delay_ms: 1500,
        ..PsConfig::default()
    };
    let client = PsStoreClient::new(cfg);

    let (titles_ps4, titles_ps5) = load_game_titles(&file_path)?;
    println!(
        "Loaded {} PS4 and {} PS5 candidate titles from {}",
        titles_ps4.len(),
        titles_ps5.len(),
        file_path
    );

    let ps4_map =
        fetch_platform_catalog(&client, &locale, &cat_ps4, page_size, max_pages_ps4).await?;
    let ps5_map =
        fetch_platform_catalog(&client, &locale, &cat_ps5, page_size, max_pages_ps5).await?;
    println!(
        "Catalog maps: PS4={} entries, PS5={} entries",
        ps4_map.len(),
        ps5_map.len()
    );

    let pool_opt = if dry_run || database_url.is_empty() {
        None
    } else {
        Some(sqlx::PgPool::connect(&database_url).await?)
    };

    let mut total_matched = 0usize;
    let mut total_rated = 0usize;
    let mut updates: Vec<(i64, f32, i64)> = Vec::new();

    if let Some(pool) = &pool_opt {
        ensure_platform_rows(pool).await?;
    }
    for (platform, titles, map) in [
        ("PS4", &titles_ps4, &ps4_map),
        ("PS5", &titles_ps5, &ps5_map),
    ] {
        for original_title in titles {
            let norm = normalize_title(original_title);
            let mut product_id_opt = map
                .get(&norm)
                .cloned()
                .or_else(|| fallback_match(map, &norm));
            if product_id_opt.is_none() && search_on_miss && title_search_pages > 0 {
                let cat = if platform == "PS4" {
                    &cat_ps4
                } else {
                    &cat_ps5
                };
                product_id_opt = resolve_product_by_title(
                    &client,
                    &locale,
                    cat,
                    &norm,
                    page_size,
                    title_search_pages,
                )
                .await
                .ok()
                .flatten();
            }
            if let Some(product_id) = product_id_opt {
                total_matched += 1;
                // NOTE: Use product_detail_raw() instead of product_star_rating() (2025-12-27)
                // The detail response already contains star rating data, avoiding redundant API calls
                // and "Unknown operation" errors for the separate star rating query.
                match client.product_detail_raw(&locale, &product_id).await {
                    Ok(detail) => {
                        // Extract rating from detail using same logic as prices.rs
                        if let Some(rating) = extract_rating_from_detail(Some(&detail)) {
                            let (avg, cnt) = rating;
                            total_rated += 1;
                            if let Some(pool) = &pool_opt {
                                if let Some(vg_id) =
                                    lookup_video_game(pool, &norm, platform).await?
                                {
                                    updates.push((vg_id, avg, cnt));
                                } else {
                                    tracing::debug!(slug = %norm, platform = %platform, "video_game row not found for rating update");
                                }
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!(title = %original_title, platform = %platform, error = %e, "detail fetch failed");
                    }
                }
            }
        }
    }

    println!(
        "Matched {} titles to product IDs; fetched ratings for {}",
        total_matched, total_rated
    );

    if dry_run || pool_opt.is_none() {
        println!("DRY_RUN complete; skipped {} DB updates", updates.len());
        return Ok(());
    }

    if let Some(pool) = &pool_opt {
        if !updates.is_empty() {
            let ids: Vec<i64> = updates.iter().map(|t| t.0).collect();
            let avgs: Vec<f32> = updates.iter().map(|t| t.1).collect();
            let counts: Vec<i64> = updates.iter().map(|t| t.2).collect();
            sqlx::query(
                r#"WITH data AS (
                    SELECT * FROM unnest($1::bigint[], $2::real[], $3::bigint[]) AS t(id,avg,cnt)
                )
                UPDATE public.video_games vg
                SET average_rating = data.avg,
                    rating_count = data.cnt,
                    rating_updated_at = now()
                FROM data
                WHERE vg.id = data.id"#,
            )
            .bind(&ids)
            .bind(&avgs)
            .bind(&counts)
            .execute(pool)
            .await?;
            println!("Applied {} rating updates", updates.len());
        } else {
            println!("No rating updates to apply");
        }
    }

    Ok(())
}

fn load_game_titles(path: &str) -> Result<(HashSet<String>, HashSet<String>)> {
    let file = File::open(path).with_context(|| format!("open {}", path))?;
    let reader = BufReader::new(file);
    let root: Value =
        serde_json::from_reader(reader).with_context(|| "parse games_detailed.json")?;
    let mut ps4: HashSet<String> = HashSet::new();
    let mut ps5: HashSet<String> = HashSet::new();

    fn title_from_object(obj: &serde_json::Map<String, Value>) -> Option<String> {
        if let Some(Value::String(s)) = obj.get("name") {
            return Some(s.trim().to_string());
        }
        if let Some(Value::Object(no)) = obj.get("name") {
            if let Some(s) = no.get("__cdata").and_then(|v| v.as_str()) {
                return Some(s.trim().to_string());
            }
        }
        if let Some(Value::String(s)) = obj.get("title") {
            return Some(s.trim().to_string());
        }
        if let Some(Value::Object(no)) = obj.get("title") {
            if let Some(s) = no.get("__cdata").and_then(|v| v.as_str()) {
                return Some(s.trim().to_string());
            }
        }
        None
    }

    fn collect_platforms(val: &Value, out: &mut HashSet<String>) {
        match val {
            Value::Array(arr) => {
                for p in arr {
                    collect_platform_abbr(p, out);
                }
            }
            Value::Object(o) => {
                if let Some(v) = o.get("platform") {
                    collect_platforms(v, out);
                } else {
                    collect_platform_abbr(&Value::Object(o.clone()), out);
                }
            }
            _ => {}
        }
    }

    fn walk(v: &Value, ps4: &mut HashSet<String>, ps5: &mut HashSet<String>) {
        match v {
            Value::Object(obj) => {
                if let Some(platforms_val) = obj.get("platforms") {
                    let mut set: HashSet<String> = HashSet::new();
                    collect_platforms(platforms_val, &mut set);
                    if !set.is_empty() {
                        if let Some(title) = title_from_object(obj) {
                            if set.contains("PS4") {
                                ps4.insert(title.clone());
                            }
                            if set.contains("PS5") {
                                ps5.insert(title.clone());
                            }
                        }
                    }
                }
                for (_k, child) in obj.iter() {
                    walk(child, ps4, ps5);
                }
            }
            Value::Array(arr) => {
                for el in arr {
                    walk(el, ps4, ps5);
                }
            }
            _ => {}
        }
    }

    walk(&root, &mut ps4, &mut ps5);
    eprintln!(
        "DEBUG games_detailed: ps4_candidates={} ps5_candidates={}",
        ps4.len(),
        ps5.len()
    );
    Ok((ps4, ps5))
}

fn collect_platform_abbr(p: &Value, out: &mut HashSet<String>) {
    if let Value::Object(po) = p {
        if let Some(abbr_val) = po.get("abbreviation") {
            match abbr_val {
                Value::String(s) => {
                    out.insert(s.trim().to_string());
                }
                Value::Object(o) => {
                    if let Some(s) = o.get("__cdata").and_then(|v| v.as_str()) {
                        out.insert(s.trim().to_string());
                    }
                }
                _ => {}
            }
        }
    }
}

async fn fetch_platform_catalog(
    client: &PsStoreClient,
    locale: &str,
    category_id: &str,
    page_size: u32,
    max_pages: u32,
) -> Result<HashMap<String, String>> {
    let mut out: HashMap<String, String> = HashMap::new();
    let mut page: u32 = 0;
    loop {
        if page >= max_pages {
            break;
        }
        let offset = page * page_size;
        let list = match client
            .category_grid_retrieve_sorted(locale, category_id, page_size, offset, "name", true)
            .await
        {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(page = page, error = %e, "category fetch failed; aborting early");
                break;
            }
        };
        if list.is_empty() {
            break;
        }
        for item in list {
            if let (Some(name), Some(pid)) = (item.name.clone(), item.product_id.clone()) {
                let norm = normalize_title(&name);
                out.entry(norm).or_insert(pid);
            }
        }
        page += 1;
    }
    Ok(out)
}

async fn resolve_product_by_title(
    client: &PsStoreClient,
    locale: &str,
    category_id: &str,
    normalized_title: &str,
    page_size: u32,
    max_pages: u32,
) -> Result<Option<String>> {
    let mut page: u32 = 0;
    while page < max_pages {
        let offset = page * page_size;
        let list = match client
            .category_grid_retrieve_sorted(locale, category_id, page_size, offset, "name", true)
            .await
        {
            Ok(v) => v,
            Err(_) => {
                break;
            }
        };
        if list.is_empty() {
            break;
        }
        for item in &list {
            if let (Some(name), Some(pid)) = (item.name.as_ref(), item.product_id.as_ref()) {
                let norm = normalize_title(name);
                if norm == normalized_title {
                    return Ok(Some(pid.clone()));
                }
            }
        }
        page += 1;
    }
    Ok(None)
}

fn fallback_match(map: &HashMap<String, String>, norm: &str) -> Option<String> {
    let mut s = norm.replace("-tm", "").replace("-r", "");
    while s.ends_with('-') {
        s.pop();
    }
    if let Some(pid) = map.get(&s) {
        return Some(pid.clone());
    }
    let mut candidate: Option<(String, usize)> = None;
    for (k, v) in map {
        if k.contains(&s) {
            let len = k.len();
            if candidate.as_ref().map(|c| len < c.1).unwrap_or(true) {
                candidate = Some((v.clone(), len));
            }
        }
    }
    candidate.map(|c| c.0)
}

async fn lookup_video_game(
    pool: &sqlx::PgPool,
    slug: &str,
    platform_name: &str,
) -> Result<Option<i64>> {
    let rec = sqlx
        ::query(
            "SELECT vg.id FROM public.video_games vg \n         JOIN public.video_game_titles vgt ON vg.title_id=vgt.id \n         JOIN public.platforms p ON vg.platform_id=p.id \n         WHERE vgt.slug=$1 AND p.name=$2 LIMIT 1"
        )
        .bind(slug)
        .bind(platform_name)
        .fetch_optional(pool).await?;
    Ok(rec.map(|r| r.get::<i64, _>("id")))
}

async fn ensure_platform_rows(pool: &sqlx::PgPool) -> Result<()> {
    for (name, slug) in [("PS4", "ps4"), ("PS5", "ps5")] {
        sqlx
            ::query(
                "INSERT INTO public.platforms (name, slug) VALUES ($1,$2) ON CONFLICT (name) DO NOTHING"
            )
            .bind(name)
            .bind(slug)
            .execute(pool).await?;
    }
    Ok(())
}
