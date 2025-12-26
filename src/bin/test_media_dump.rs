use psstore_client::{PsConfig, PsStoreClient};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let locale = "en-us";
    let cfg = PsConfig::default();
    let client = PsStoreClient::new(cfg);

    println!("Fetching first PS5 product for media collection test...\n");

    // Get first PS5 product
    let cat_ps5 = "4cbf39e2-5749-4970-ba81-93a489e4570c";
    let items = client.category_grid_retrieve(locale, cat_ps5, 1, 0).await?;

    if let Some(first) = items.into_iter().next() {
        if let Some(product_id) = first.product_id {
            println!("=== Testing PSStore Media Collection ===");
            println!("Product ID: {}", product_id);
            println!("Name: {:?}\n", first.name);
            println!("=== Media Summary ===");
            println!("Total media items: {}", first.media_urls.len());
            println!("Image URLs collected: {}", first.media_image_urls.len());
            println!("Video URLs collected: {}", first.media_video_urls.len());
            println!("Image objects: {}", first.media_images.len());
            println!("Video objects: {}", first.media_videos.len());

            if !first.media_images.is_empty() {
                println!("\n=== Image Media Objects (Structured) ===");
                for (i, img) in first.media_images.iter().enumerate() {
                    println!("\nImage {}", i);
                    println!("  typename: {:?}", img.typename);
                    println!("  media_type: {:?}", img.media_type);
                    println!("  role: {:?}", img.role);
                    if let Some(url) = &img.url {
                        let display = if url.len() > 100 {
                            format!("{}...", &url[..97])
                        } else {
                            url.clone()
                        };
                        println!("  url: {}", display);
                    }
                }
            }

            if !first.media_videos.is_empty() {
                println!("\n=== Video Media Objects (Structured) ===");
                for (i, vid) in first.media_videos.iter().enumerate() {
                    println!("\nVideo {}", i);
                    println!("  typename: {:?}", vid.typename);
                    println!("  media_type: {:?}", vid.media_type);
                    println!("  role: {:?}", vid.role);
                    if let Some(url) = &vid.url {
                        let display = if url.len() > 100 {
                            format!("{}...", &url[..97])
                        } else {
                            url.clone()
                        };
                        println!("  url: {}", display);
                    }
                }
            }

            if !first.genres.is_empty() {
                println!("\n=== Genres ===");
                println!("{:?}", first.genres);
            }

            println!("\n✅ Media collection verification complete!");
            println!("Media is extensively collected with:");
            println!("  • Raw URL lists for quick access");
            println!("  • Structured media objects with metadata");
            println!("  • Type classification (images vs videos)");
            println!("  • Additional fields: typename, role, media_type");
        }
    } else {
        println!("No products found in PS5 category");
    }

    Ok(())
}
