use anyhow::Result;
use psstore_client::{PsConfig, PsStoreClient};
use std::env;

pub async fn run_from_env() -> Result<()> {
    dotenv::dotenv().ok();
    let regions_raw = env::var("PS_STORE_REGIONS").unwrap_or_else(|_| "en-us".into());
    let regions: Vec<String> = regions_raw
        .split(|c: char| (c == ',' || c == ' '))
        .filter(|s| !s.is_empty())
        .map(|s| s.trim().to_lowercase())
        .collect();
    let locale = regions.get(0).map(|s| s.as_str()).unwrap_or("en-us");
    let cat =
        env::var("PS_CATEGORY").unwrap_or_else(|_| "4cbf39e2-5749-4970-ba81-93a489e4570c".into());
    let page_size: u32 = env::var("PS_PAGE_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    let offset: u32 = env::var("PS_OFFSET")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);

    eprintln!(
        "Debug: locale={} cat={} page_size={} offset= {}",
        locale, cat, page_size, offset
    );

    let cfg = PsConfig {
        locales: vec![locale.to_string()],
        rps: 3,
        retry_attempts: 5,
        retry_base_delay_ms: 1500,
        ..PsConfig::default()
    };
    let client = PsStoreClient::new(cfg);

    let items = client
        .category_grid_retrieve_sorted(locale, &cat, page_size, offset, "productReleaseDate", false)
        .await?;
    eprintln!("Fetched {} items", items.len());
    for (i, it) in items.into_iter().enumerate() {
        eprintln!("--- item {} ---", i);
        eprintln!(
            "name: {}",
            it.name.clone().unwrap_or_else(|| "<untitled>".into())
        );
        eprintln!("product_id: {:?}", it.product_id);
        eprintln!("concept_id: {:?}", it.concept_id);
        eprintln!("base_price_minor: {:?}", it.base_price_minor);
        eprintln!("discounted_price_minor: {:?}", it.discounted_price_minor);
        eprintln!("is_free: {:?}", it.is_free);
        eprintln!(
            "images: {} videos: {} total_media: {}",
            it.media_image_urls.len(),
            it.media_video_urls.len(),
            it.media_urls.len()
        );
        for (j, u) in it.media_urls.iter().enumerate() {
            eprintln!("  media[{}]: {}", j, u);
        }
    }
    Ok(())
}
