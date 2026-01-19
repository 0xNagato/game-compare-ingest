use crate::database_ops::db::Db;
use crate::database_ops::{ingest_providers, media_map::normalize_title};
use crate::util::env as env_util;
use anyhow::Result;
use psstore_client::{PsConfig, PsProductSummary, PsStoreClient};
use std::collections::HashMap;

// Mirrors original ps_ingest demo logic (SANITY-mode fetch and optional ascription)
pub async fn run_from_env() -> Result<()> {
    let _ = dotenv::dotenv();
    let locale = std::env::var("PS_LOCALE").unwrap_or_else(|_| "en-us".into());
    let cat_ps4 = std::env::var("PS4_CATEGORY")
        .unwrap_or_else(|_| "44d8bb20-653e-431e-8ad0-c0a365f68d2f".into());
    let cat_ps5 = std::env::var("PS5_CATEGORY")
        .unwrap_or_else(|_| "4cbf39e2-5749-4970-ba81-93a489e4570c".into());
    let database_url = env_util::db_url().ok();

    let cfg = PsConfig {
        locales: vec![locale.clone()],
        rps: 3,
        retry_attempts: 5,
        retry_base_delay_ms: 2000,
        ..PsConfig::default()
    };
    let client = PsStoreClient::new(cfg);
    let page_size = 24u32;
    let mut all_items: Vec<(String, PsProductSummary)> = Vec::new();
    for platform_label in ["PS4", "PS5"] {
        let cat = if platform_label == "PS4" {
            &cat_ps4
        } else {
            &cat_ps5
        };
        let mut offset = 0u32;
        let mut pages = 0u32;
        loop {
            pages += 1;
            if pages > 2 {
                break;
            }
            let list = match client
                .category_grid_retrieve_sorted(
                    &locale,
                    cat,
                    page_size,
                    offset,
                    "productReleaseDate",
                    false,
                )
                .await
            {
                Ok(v) => v,
                Err(e) => {
                    eprintln!(
                        "fetch {} page {} error: {}",
                        platform_label,
                        offset / page_size,
                        e
                    );
                    Vec::new()
                }
            };
            if list.is_empty() {
                break;
            }
            for it in list {
                all_items.push((platform_label.to_string(), it));
            }
            offset += page_size;
        }
    }

    println!(
        "Fetched {} items across PS4/PS5 (demo limit)",
        all_items.len()
    );

    if std::env::var("SANITY")
        .map(|v| (v == "1" || v.eq_ignore_ascii_case("true")))
        .unwrap_or(false)
    {
        let mut per_platform: HashMap<String, usize> = HashMap::new();
        let mut image_count = 0usize;
        let mut video_count = 0usize;
        let mut with_any_media = 0usize;
        let mut with_rating_hint = 0usize;
        let mut name_sample: Vec<String> = Vec::new();
        for (plat, item) in &all_items {
            *per_platform.entry(plat.clone()).or_default() += 1;
            let ic = item.media_image_urls.len();
            let vc = item.media_video_urls.len();
            if ic + vc > 0 {
                with_any_media += 1;
            }
            image_count += ic;
            video_count += vc;
            if item.product_id.is_some() {
                with_rating_hint += 1;
            }
            if name_sample.len() < 8 {
                name_sample.push(item.name.clone().unwrap_or_else(|| "<untitled>".into()));
            }
        }
        let mut platforms: Vec<_> = per_platform.into_iter().collect();
        platforms.sort_by(|a, b| a.0.cmp(&b.0));
        println!("=== SANITY SUMMARY ===");
        println!("Total items: {}", all_items.len());
        for (plat, cnt) in platforms {
            println!("  {}: {}", plat, cnt);
        }
        println!("Items with any media: {}", with_any_media);
        println!("Total image URLs: {}", image_count);
        println!("Total video URLs: {}", video_count);
        println!(
            "Items eligible for rating fetch (have product_id): {}",
            with_rating_hint
        );
        println!("Sample titles (up to 8): {}", name_sample.join(" | "));
        println!("SANITY mode complete; skipping DB writes.");
        return Ok(());
    }

    if let Some(db_url) = database_url {
        let db = Db::connect(&db_url, 10).await?;
        let ps4_id = ingest_providers::ensure_platform(&db, "PS4", Some("ps4")).await?;
        let ps5_id = ingest_providers::ensure_platform(&db, "PS5", Some("ps5")).await?;
        for (platform_label, item) in all_items.into_iter() {
            let title_name = item.name.clone().unwrap_or_else(|| "Untitled".into());
            let title_slug_str = normalize_title(&title_name);
            let product_id = ingest_providers::ensure_product_named(
                &db,
                "software",
                &title_slug_str,
                &title_name,
            )
            .await?;
            ingest_providers::ensure_software_row(&db, product_id).await?;
            let title_id = ingest_providers::ensure_video_game_title(
                &db,
                product_id,
                &title_name,
                Some(&title_slug_str),
            )
            .await?;
            let platform_id = if platform_label == "PS4" {
                ps4_id
            } else {
                ps5_id
            };
            let vg_id =
                ingest_providers::ensure_video_game(&db, title_id, platform_id, None).await?;
            // NOTE: Redundant star rating API call removed (2025-12-27)
            // The main prices.rs pipeline extracts ratings from product detail responses.
            // Demo ingestion should rely on that pipeline rather than making separate API calls.
            if !item.genres.is_empty() {
                let _ = sqlx::query("UPDATE public.video_games SET genres = $1 WHERE id=$2")
                    .bind(&item.genres)
                    .bind(vg_id)
                    .execute(&db.pool)
                    .await;
            }
            let provider_id = ingest_providers::ensure_provider(
                &db,
                "playstation_store",
                "storefront",
                Some("ps-store"),
            )
            .await?;
            if let Some(prod_id) = &item.product_id {
                let video_game_source_id =
                    ingest_providers::ensure_provider_item(&db, provider_id, prod_id, None).await?;
                let mut all_urls: Vec<String> = Vec::new();
                all_urls.extend(item.media_image_urls.iter().cloned());
                all_urls.extend(item.media_video_urls.iter().cloned());
                all_urls.retain(|u| {
                    let lc = u.to_ascii_lowercase();
                    !(lc.contains("screenshot")
                        || lc.contains("screenshots")
                        || lc.contains("logo")
                        || lc.contains("icon")
                        || lc.contains("thumbnail")
                        || lc.contains("thumb"))
                });
                all_urls.sort();
                all_urls
                    .drain(..)
                    .collect::<std::collections::HashSet<_>>()
                    .into_iter()
                    .for_each(|u| all_urls.push(u));
                let _ = ingest_providers::ensure_vg_source_media_links(
                    &db,
                    video_game_source_id,
                    &all_urls,
                )
                .await?;
            }
        }
        println!("Ascribed items into video_games (products/software/titles/games).");
    }
    Ok(())
}
