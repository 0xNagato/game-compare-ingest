use anyhow::Result;
use psstore_client::{PsConfig, PsStoreClient};

#[tokio::test]
async fn psstore_sanity_summary() -> Result<()> {
    // No DB needed; network call required. Skip silently on failure.
    let cfg = PsConfig {
        locales: vec!["en-us".into()],
        ..PsConfig::default()
    };
    let client = PsStoreClient::new(cfg);
    let ps4 = "44d8bb20-653e-431e-8ad0-c0a365f68d2f";
    let ps5 = "4cbf39e2-5749-4970-ba81-93a489e4570c";

    // Track aggregates; avoid assigning an initial value that is never read
    let mut img = 0usize;
    let mut vid = 0usize;
    let mut items = 0usize;
    let mut free_items = 0usize;
    let mut discounted_items = 0usize;
    let mut sum_base_minor: i64 = 0;
    let mut sum_discounted_minor: i64 = 0;
    for (plat, cat) in [("PS4", ps4), ("PS5", ps5)] {
        match client
            .category_grid_retrieve_sorted("en-us", cat, 24, 0, "productReleaseDate", false)
            .await
        {
            Ok(list) => {
                let count = list.len();
                items += count;
                println!("SANITY {} page0: {} items", plat, count);
                for (idx, it) in list.into_iter().enumerate() {
                    let name = it.name.clone().unwrap_or_else(|| "<untitled>".into());
                    let pid = it.product_id.clone().unwrap_or_else(|| "<none>".into());
                    let ic = it.media_image_urls.len();
                    let vc = it.media_video_urls.len();
                    img += ic;
                    vid += vc;
                    if let Some(true) = it.is_free {
                        free_items += 1;
                    }
                    if let (Some(bp), Some(dp)) = (it.base_price_minor, it.discounted_price_minor) {
                        if dp < bp {
                            discounted_items += 1;
                        }
                    }
                    if let Some(bp) = it.base_price_minor {
                        sum_base_minor += bp;
                    }
                    if let Some(dp) = it.discounted_price_minor {
                        sum_discounted_minor += dp;
                    }
                    let base_show = it
                        .base_price_minor
                        .map(|v| format!("{:.2}", (v as f64) / 100.0))
                        .unwrap_or_else(|| "-".into());
                    let disc_show = it
                        .discounted_price_minor
                        .map(|v| format!("{:.2}", (v as f64) / 100.0))
                        .unwrap_or_else(|| "-".into());
                    let free_show = it
                        .is_free
                        .map(|v| if v { "free" } else { "paid" })
                        .unwrap_or("?");
                    // Per-item proof line
                    println!(
                        "  {:>2}. {} | product_id={} | images={} | videos={} | base=${} | discounted=${} | {}",
                        idx + 1,
                        name,
                        pid,
                        ic,
                        vc,
                        base_show,
                        disc_show,
                        free_show
                    );
                }
            }
            Err(e) => {
                eprintln!("psstore sanity fetch {} failed: {}", plat, e);
            }
        }
    }
    println!("=== SANITY SUMMARY (test) ===");
    println!("Total items (first page each): {}", items);
    println!("Total image URLs: {}", img);
    println!("Total video URLs: {}", vid);
    println!("Free items: {}", free_items);
    println!("Discounted (discount < base) items: {}", discounted_items);
    if items > 0 {
        println!(
            "Avg base price: ${:.2}",
            (sum_base_minor as f64) / 100.0 / (items as f64)
        );
        println!(
            "Avg discounted price: ${:.2}",
            (sum_discounted_minor as f64) / 100.0 / (items as f64)
        );
    }
    // Don't assert network-dependent values; just ensure the test runs.
    Ok(())
}
