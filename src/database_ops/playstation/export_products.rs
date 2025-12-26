use anyhow::Result;
use psstore_client::{PsConfig, PsProductSummary, PsStoreClient};
use serde::Serialize;
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::PathBuf;

#[derive(Serialize)]
pub struct ExportRow {
    pub product_id: String,
    pub name: String,
    pub category: String,
    pub locale: String,
    pub release_date: Option<String>,
    pub base_price_minor: Option<i64>,
    pub discounted_price_minor: Option<i64>,
    pub is_free: Option<bool>,
    pub media_urls: Vec<String>,
    pub media_image_urls: Vec<String>,
    pub media_video_urls: Vec<String>,
    pub average_rating: Option<f32>,
    pub rating_count: Option<i64>,
    pub genres: Vec<String>,
}

pub async fn run_from_env() -> Result<()> {
    // --- Inputs ---------------------------------------------------------------------
    let locales: Vec<String> = std::env::var("PS_LOCALES")
        .unwrap_or_else(|_| "en-us,en-gb,en-ca,fr-fr,de-de,it-it,es-es,pt-br,ja-jp,ko-kr".into())
        .split(|c: char| (c == ',' || c == ' '))
        .filter(|s| !s.is_empty())
        .map(|s| s.trim().to_lowercase())
        .collect();
    let size: u32 = std::env::var("PS_PAGE_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(50);
    let pages: u32 = std::env::var("PS_TOTAL_PAGES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(20);
    let cat_ps4 = std::env::var("PS4_CATEGORY")
        .unwrap_or_else(|_| "44d8bb20-653e-431e-8ad0-c0a365f68d2f".into());
    let cat_ps5 = std::env::var("PS5_CATEGORY")
        .unwrap_or_else(|_| "4cbf39e2-5749-4970-ba81-93a489e4570c".into());
    let rps_per_locale: u32 = std::env::var("PS_STORE_RPS")
        .ok()
        .and_then(|s| s.parse::<f32>().ok().map(|f| f.ceil() as u32))
        .filter(|v| *v > 0)
        .unwrap_or(3);
    let retry_attempts: u32 = std::env::var("PS_STORE_MAX_RETRIES")
        .ok()
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(3);
    let retry_base_ms: u64 = std::env::var("PS_STORE_BACKOFF_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(300);

    // Single client used for all locales (client itself stores locales list)
    let cfg = PsConfig {
        locales: locales.clone(),
        rps: rps_per_locale,
        retry_attempts,
        retry_base_delay_ms: retry_base_ms,
        ..PsConfig::default()
    };
    let client = PsStoreClient::new(cfg);

    // -------------------------------------------------------------------------------
    // Optional single-item dump mode for quick inspection
    if std::env::var("PS_DUMP_ONE")
        .ok()
        .map(|v| (v == "1" || v.eq_ignore_ascii_case("true")))
        .unwrap_or(false)
    {
        let locale_one = std::env::var("PS_DUMP_LOCALE")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| locales.get(0).cloned().unwrap_or_else(|| "en-us".into()));
        let category_one = std::env::var("PS_DUMP_CATEGORY")
            .ok()
            .unwrap_or_else(|| "ps5".into());
        let cat_id_one = if category_one.eq_ignore_ascii_case("ps4") {
            &cat_ps4
        } else {
            &cat_ps5
        };
        let size_one: u32 = 1;
        let offset_one: u32 = std::env::var("PS_DUMP_OFFSET")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        let mut list = match client
            .category_grid_retrieve_sorted(
                &locale_one,
                cat_id_one,
                size_one,
                offset_one,
                "productReleaseDate",
                false,
            )
            .await
        {
            Ok(list) => list,
            Err(_) => match client
                .category_grid_retrieve(&locale_one, cat_id_one, size_one, offset_one)
                .await
            {
                Ok(list) => list,
                Err(e) => {
                    eprintln!(
                        "error fetching one product for {} {}: {}",
                        locale_one, category_one, e
                    );
                    Vec::new()
                }
            },
        };
        // Optional enrichment toggle (default ON)
        let do_enrich = std::env::var("PS_ENRICH")
            .ok()
            .map(|v| (v == "1" || v.eq_ignore_ascii_case("true")))
            .unwrap_or(true);
        if do_enrich && !list.is_empty() {
            client.enrich_products(&locale_one, &mut list).await;
        }
        if let Some(prod) = list.into_iter().next() {
            // Print the parsed summary first
            println!("{:#?}", prod);

            // Then print raw JSON for the same request (sorted first, fallback to unsorted)
            let vars_sorted = json!({
                "id": cat_id_one,
                "pageArgs": { "size": size_one, "offset": offset_one },
                "sortBy": { "name": "productReleaseDate", "isAscending": false },
                "filterBy": [],
                "facetOptions": []
            });
            let raw = match client
                .op_get("categoryGridRetrieve", &vars_sorted, Some(&locale_one))
                .await
            {
                Ok(v) => v,
                Err(_) => {
                    let vars_unsorted = json!({
                        "id": cat_id_one,
                        "pageArgs": { "size": size_one, "offset": offset_one },
                        "sortBy": serde_json::Value::Null,
                        "filterBy": [],
                        "facetOptions": []
                    });
                    match client
                        .op_get("categoryGridRetrieve", &vars_unsorted, Some(&locale_one))
                        .await
                    {
                        Ok(v) => v,
                        Err(e2) => {
                            eprintln!(
                                "error fetching raw JSON for {} {} offset {}: {}",
                                locale_one, category_one, offset_one, e2
                            );
                            serde_json::Value::Null
                        }
                    }
                }
            };
            if raw != serde_json::Value::Null {
                if let Ok(s) = serde_json::to_string_pretty(&raw) {
                    println!("{}", s);
                }
            }
        } else {
            eprintln!("no product returned for {} {}", locale_one, category_one);
        }
        return Ok(());
    }

    // -------------------------------------------------------------------------------
    // Dump raw JSON for an arbitrary sorting option (e.g. "downloads30" for "Most Downloaded")
    if std::env::var("PS_DUMP_SORT_JSON")
        .ok()
        .map(|v| (v == "1" || v.eq_ignore_ascii_case("true")))
        .unwrap_or(false)
    {
        let locale = std::env::var("PS_DUMP_LOCALE")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| locales.get(0).cloned().unwrap_or_else(|| "en-us".into()));
        let category = std::env::var("PS_DUMP_CATEGORY")
            .ok()
            .unwrap_or_else(|| "ps5".into());
        let cat_id = if category.eq_ignore_ascii_case("ps4") {
            &cat_ps4
        } else {
            &cat_ps5
        };
        let sort_name = std::env::var("PS_DUMP_SORT_NAME")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "downloads30".into()); // Most Downloaded
        let size: u32 = std::env::var("PS_DUMP_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(50);
        let offset: u32 = std::env::var("PS_DUMP_OFFSET")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        // Build vars explicitly; use same persisted hash
        let vars_sorted = json!({
            "id": cat_id,
            "pageArgs": { "size": size, "offset": offset },
            "sortBy": { "name": sort_name, "isAscending": false },
            "filterBy": [],
            "facetOptions": []
        });
        let raw = match client
            .op_get("categoryGridRetrieve", &vars_sorted, Some(&locale))
            .await
        {
            Ok(v) => v,
            Err(e_sorted) => {
                eprintln!(
                    "warn: sorted request failed ({}) falling back to unsorted",
                    e_sorted
                );
                let vars_unsorted = json!({
                    "id": cat_id,
                    "pageArgs": { "size": size, "offset": offset },
                    "sortBy": serde_json::Value::Null,
                    "filterBy": [],
                    "facetOptions": []
                });
                match client
                    .op_get("categoryGridRetrieve", &vars_unsorted, Some(&locale))
                    .await
                {
                    Ok(v2) => v2,
                    Err(e_unsorted) => {
                        eprintln!("error: unsorted fallback also failed: {}", e_unsorted);
                        serde_json::Value::Null
                    }
                }
            }
        };
        // Optional: derive summaries again (sorted) and enrich for display
        if std::env::var("PS_DUMP_SORT_ENRICH")
            .ok()
            .map(|v| (v == "1" || v.eq_ignore_ascii_case("true")))
            .unwrap_or(true)
        {
            match client
                .category_grid_retrieve_sorted(&locale, cat_id, size, offset, &sort_name, false)
                .await
            {
                Ok(mut summaries) => {
                    if !summaries.is_empty() {
                        if std::env::var("PS_ENRICH")
                            .ok()
                            .map(|v| (v == "1" || v.eq_ignore_ascii_case("true")))
                            .unwrap_or(true)
                        {
                            client.enrich_products(&locale, &mut summaries).await;
                        }
                        if let Ok(s) = serde_json::to_string_pretty(&summaries) {
                            println!("-- ENRICHED_SUMMARIES --\n{}", s);
                        } else {
                            println!("-- ENRICHED_SUMMARIES_DUMP_FAIL -- len={}", summaries.len());
                        }
                    }
                }
                Err(e) => eprintln!("warn: unable to re-fetch summaries for enrichment: {}", e),
            }
        }
        if raw == serde_json::Value::Null {
            eprintln!(
                "no JSON returned for sort dump (sort_name={}, locale={}, category={}, size={}, offset={})",
                sort_name, locale, category, size, offset
            );
        } else if let Ok(s) = serde_json::to_string_pretty(&raw) {
            println!("{}", s);
        }
        return Ok(());
    }

    // -------------------------------------------------------------------------------
    // Full export across all locales & categories
    let mut seen: HashSet<(String, String)> = HashSet::new();
    let mut rows: Vec<ExportRow> = Vec::new();
    let mut per_locale_count: HashMap<String, usize> = HashMap::new();
    let do_enrich = std::env::var("PS_ENRICH")
        .ok()
        .map(|v| (v == "1" || v.eq_ignore_ascii_case("true")))
        .unwrap_or(true);

    for locale in &locales {
        for (category, cat_id) in [("ps4", &cat_ps4), ("ps5", &cat_ps5)] {
            let mut offset: u32 = 0;
            for _ in 0..pages {
                let mut batch: Vec<PsProductSummary> = match client
                    .category_grid_retrieve_sorted(
                        locale,
                        cat_id,
                        size,
                        offset,
                        "productReleaseDate",
                        false,
                    )
                    .await
                {
                    Ok(list) => list,
                    Err(e) => {
                        eprintln!(
                            "warn: {} {} page {}: {}",
                            locale,
                            category,
                            offset / size,
                            e
                        );
                        Vec::new()
                    }
                };
                if batch.is_empty() && offset == 0 {
                    if let Ok(fallback) = client
                        .category_grid_retrieve(locale, cat_id, size, offset)
                        .await
                    {
                        batch = fallback;
                    }
                }
                if batch.is_empty() {
                    break;
                }
                if do_enrich && !batch.is_empty() {
                    client.enrich_products(locale, &mut batch).await;
                }
                for item in batch.into_iter() {
                    if let Some(pid) = &item.product_id {
                        let key = (locale.clone(), pid.clone());
                        if seen.contains(&key) {
                            continue;
                        }
                        seen.insert(key);
                        rows.push(ExportRow {
                            product_id: pid.clone(),
                            name: item.name.clone().unwrap_or_default(),
                            category: category.to_string(),
                            locale: locale.clone(),
                            release_date: item.release_date.clone(),
                            base_price_minor: item.base_price_minor,
                            discounted_price_minor: item.discounted_price_minor,
                            is_free: item.is_free,
                            media_urls: item.media_urls.clone(),
                            media_image_urls: item.media_image_urls.clone(),
                            media_video_urls: item.media_video_urls.clone(),
                            average_rating: item.average_rating,
                            rating_count: item.rating_count,
                            genres: item.genres.clone(),
                        });
                        *per_locale_count.entry(locale.clone()).or_insert(0) += 1;
                    }
                }
                offset = offset.saturating_add(size);
            }
        }
    }

    let dir = PathBuf::from("exports");
    if !dir.exists() {
        fs::create_dir_all(&dir)?;
    }

    let ts = chrono::Utc::now().format("%Y%m%d_%H%M%S");
    let json_path = dir.join(format!("products_{}.json", ts));
    let csv_path = dir.join(format!("products_{}.csv", ts));

    let json_str = serde_json::to_string_pretty(&rows)?;
    fs::write(&json_path, &json_str)?;

    let mut wtr = csv::Writer::from_path(&csv_path)?;
    wtr.write_record([
        "product_id",
        "name",
        "category",
        "locale",
        "release_date",
        "base_price_minor",
        "discounted_price_minor",
        "is_free",
        "media_urls",
        "media_image_urls",
        "media_video_urls",
        "average_rating",
        "rating_count",
        "genres",
    ])?;
    for r in &rows {
        let media_joined = if r.media_urls.is_empty() {
            String::new()
        } else {
            r.media_urls.join("|")
        };
        let media_images = if r.media_image_urls.is_empty() {
            String::new()
        } else {
            r.media_image_urls.join("|")
        };
        let media_videos = if r.media_video_urls.is_empty() {
            String::new()
        } else {
            r.media_video_urls.join("|")
        };
        let genres_joined = if r.genres.is_empty() {
            String::new()
        } else {
            r.genres.join("|")
        };
        wtr.write_record([
            r.product_id.as_str(),
            r.name.as_str(),
            r.category.as_str(),
            r.locale.as_str(),
            r.release_date.as_deref().unwrap_or(""),
            r.base_price_minor
                .map(|v| v.to_string())
                .unwrap_or_default()
                .as_str(),
            r.discounted_price_minor
                .map(|v| v.to_string())
                .unwrap_or_default()
                .as_str(),
            r.is_free
                .map(|v| if v { "true" } else { "false" })
                .unwrap_or(""),
            media_joined.as_str(),
            media_images.as_str(),
            media_videos.as_str(),
            r.average_rating
                .map(|v| format!("{:.2}", v))
                .unwrap_or_default()
                .as_str(),
            r.rating_count
                .map(|v| v.to_string())
                .unwrap_or_default()
                .as_str(),
            genres_joined.as_str(),
        ])?;
    }
    wtr.flush()?;

    let root_json = format!("products_snapshot_{}.json", ts);
    let root_csv = format!("products_snapshot_{}.csv", ts);
    fs::write(&root_json, &json_str)?;
    fs::copy(&csv_path, &root_csv)?;

    println!("Exported {} rows", rows.len());
    for (loc, cnt) in per_locale_count.iter() {
        println!("  {:>10}: {:>4} rows", loc, cnt);
    }
    println!(
        "JSON: {}\nCSV: {}\nRoot JSON: {}\nRoot CSV: {}",
        json_path.display(),
        csv_path.display(),
        root_json,
        root_csv
    );
    Ok(())
}
