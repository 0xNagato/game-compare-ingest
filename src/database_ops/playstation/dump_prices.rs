use anyhow::{anyhow, Result};
use psstore_client::{PsConfig, PsProductSummary, PsStoreClient};
use std::{fs::File, io::Write};

// Dump price-related data for PS4/PS5 categories; optional CSV output
pub async fn run_from_env() -> Result<()> {
    let pages: u32 = std::env::var("PAGES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2);
    let size: u32 = std::env::var("PAGE_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);
    let locale = std::env::var("LOCALE").unwrap_or_else(|_| "en-us".into());
    let cat_ps4 = std::env::var("PS4_CATEGORY")
        .unwrap_or_else(|_| "44d8bb20-653e-431e-8ad0-c0a365f68d2f".into());
    let cat_ps5 = std::env::var("PS5_CATEGORY")
        .unwrap_or_else(|_| "4cbf39e2-5749-4970-ba81-93a489e4570c".into());
    let sort_field = std::env::var("SORT_FIELD").unwrap_or_else(|_| "productReleaseDate".into());
    let asc = std::env::var("ASC")
        .map(|v| (v == "1" || v.eq_ignore_ascii_case("true")))
        .unwrap_or(false);
    let output_csv = std::env::var("OUTPUT_CSV").ok();

    let cfg = PsConfig {
        locales: vec![locale.clone()],
        ..PsConfig::default()
    };
    let client = PsStoreClient::new(cfg);

    let mut all: Vec<(String, PsProductSummary)> = Vec::new();
    for (label, cat) in [("PS4", cat_ps4.as_str()), ("PS5", cat_ps5.as_str())] {
        let mut offset = 0u32;
        for page in 0..pages {
            let batch = match client
                .category_grid_retrieve_sorted(&locale, cat, size, offset, &sort_field, asc)
                .await
            {
                Ok(b) => b,
                Err(e) => {
                    eprintln!("warn: fetch {} page {} failed: {}", label, page, e);
                    Vec::new()
                }
            };
            if batch.is_empty() {
                break;
            }
            for item in batch {
                all.push((label.to_string(), item));
            }
            offset = offset.saturating_add(size);
        }
    }

    if all.is_empty() {
        return Err(anyhow!("no items fetched"));
    }

    let mut sum_base: i64 = 0;
    let mut cnt_base: i64 = 0;
    let mut sum_disc: i64 = 0;
    let mut cnt_disc: i64 = 0;
    let mut free_cnt = 0;
    let mut discounted_cnt = 0;
    for (_, it) in &all {
        if let Some(b) = it.base_price_minor {
            sum_base += b;
            cnt_base += 1;
        }
        if let Some(d) = it.discounted_price_minor {
            sum_disc += d;
            cnt_disc += 1;
        }
        if let Some(true) = it.is_free {
            free_cnt += 1;
        }
        if let (Some(b), Some(d)) = (it.base_price_minor, it.discounted_price_minor) {
            if d < b {
                discounted_cnt += 1;
            }
        }
    }

    println!("=== PRICE DUMP SUMMARY ===");
    println!("Items: {}", all.len());
    println!(
        "Base price count: {} avg=${:.2}",
        cnt_base,
        if cnt_base > 0 {
            (sum_base as f64) / 100.0 / (cnt_base as f64)
        } else {
            0.0
        }
    );
    println!(
        "Discounted price count: {} avg=${:.2}",
        cnt_disc,
        if cnt_disc > 0 {
            (sum_disc as f64) / 100.0 / (cnt_disc as f64)
        } else {
            0.0
        }
    );
    println!("Free items: {}", free_cnt);
    println!("Discounted items (discount < base): {}", discounted_cnt);

    for (idx, (plat, it)) in all.iter().enumerate() {
        let name = it.name.clone().unwrap_or_else(|| "<untitled>".into());
        let pid = it.product_id.clone().unwrap_or_else(|| "<none>".into());
        let base = it
            .base_price_minor
            .map(|v| format!("{:.2}", (v as f64) / 100.0))
            .unwrap_or_else(|| "-".into());
        let disc = it
            .discounted_price_minor
            .map(|v| format!("{:.2}", (v as f64) / 100.0))
            .unwrap_or_else(|| "-".into());
        let free_flag = it
            .is_free
            .map(|v| if v { "free" } else { "paid" })
            .unwrap_or("?");
        println!(
            "{:>4}. [{}] {} | product_id={} | base=${} | discounted=${} | {}",
            idx + 1,
            plat,
            name,
            pid,
            base,
            disc,
            free_flag
        );
    }

    if let Some(path) = output_csv {
        let mut w = File::create(&path)?;
        writeln!(
            w,
            "platform,product_id,name,base_price_minor,discounted_price_minor,is_free"
        )?;
        for (plat, it) in &all {
            let pid = it.product_id.clone().unwrap_or_else(|| "".into());
            let name = it.name.clone().unwrap_or_else(|| "".into());
            let b = it
                .base_price_minor
                .map(|v| v.to_string())
                .unwrap_or_default();
            let d = it
                .discounted_price_minor
                .map(|v| v.to_string())
                .unwrap_or_default();
            let f = it
                .is_free
                .map(|v| if v { "true" } else { "false" })
                .unwrap_or("");
            writeln!(
                w,
                "{},{},{},{},{},{}",
                plat,
                pid,
                escape_csv(&name),
                b,
                d,
                f
            )?;
        }
        println!("CSV written: {}", path);
    }

    Ok(())
}

fn escape_csv(s: &str) -> String {
    if s.contains(',') || s.contains('"') {
        format!("\"{}\"", s.replace('"', "\""))
    } else {
        s.to_string()
    }
}
