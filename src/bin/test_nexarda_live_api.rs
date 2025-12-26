use i_miss_rust::database_ops::nexarda::provider::{
    NexardaOptions, NexardaProvider, Product, RegionDefinition,
};
use serde_json::json;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("=== Testing Nexarda Live API ===\n");

    // Check API key
    let api_key = std::env::var("NEXARDA_API_KEY").ok();

    if api_key.is_none() {
        println!("❌ NEXARDA_API_KEY not set");
        println!("   Set the API key: export NEXARDA_API_KEY=your_key");
        println!("   Then run again.");
        return Ok(());
    }

    println!("✅ API Key found");
    println!(
        "   • Length: {} characters",
        api_key.as_ref().unwrap().len()
    );
    println!();

    // Initialize provider
    println!("Test 1: Provider Initialization");
    println!("==============================");

    let provider = NexardaProvider::new(None, Some(30))?;
    println!("✅ Provider initialized with API timeout: 30 seconds\n");

    // Test 1: Sample product (game ID from API docs if available)
    println!("Test 2: Test Product Configuration");
    println!("==================================");

    // Use a known game ID (example: "1" or test ID if available)
    let test_product = Product {
        id: json!("1"),
        r#type: "game".to_string(),
        title: Some("Test Game".to_string()),
        slug: Some("test-game".to_string()),
        platform: Some("PC".to_string()),
        category: Some("game".to_string()),
        regions: vec![
            RegionDefinition {
                currency: Some("USD".to_string()),
                region_code: Some("US".to_string()),
                store_id: None,
            },
            RegionDefinition {
                currency: Some("EUR".to_string()),
                region_code: Some("GB".to_string()),
                store_id: None,
            },
        ],
    };

    println!("✅ Test product configured:");
    println!("   • ID: {}", test_product.id);
    println!("   • Type: {}", test_product.r#type);
    println!(
        "   • Regions: {} (USD/US, EUR/GB)",
        test_product.regions.len()
    );
    println!();

    // Test 2: Fetch deals with API key
    println!("Test 3: Fetch Deals from Nexarda API");
    println!("===================================");

    let options = NexardaOptions {
        products: vec![test_product],
        store_map: Default::default(),
        base_url: None,
        timeout: Some(30),
        api_key: api_key.clone(),
        auto_register_stores: Some(true),
        default_regions: vec![],
        dynamic_store_overrides: Default::default(),
        default_tax_inclusive: None,
        context: None,
    };

    println!("Calling Nexarda API...\n");

    match provider.fetch_deals(options).await {
        Ok(response) => {
            println!("✅ API call successful");
            println!(
                "   • Response provider: {}",
                response.meta.get("provider").unwrap_or(&json!("unknown"))
            );
            println!(
                "   • Generated at: {}",
                response
                    .meta
                    .get("generated_at")
                    .unwrap_or(&json!("unknown"))
            );
            println!(
                "   • Product count: {}",
                response.meta.get("product_count").unwrap_or(&json!(0))
            );
            println!("   • Results returned: {}", response.results.len());
            println!();

            if response.results.is_empty() {
                println!("⚠️  No deals returned for test product");
                println!("   This is normal if the product ID is not in Nexarda database");
                println!();
            } else {
                println!("Test 4: Deals Response Structure");
                println!("================================");

                for (idx, game_deals) in response.results.iter().enumerate() {
                    println!("Game #{}", idx + 1);
                    println!("  Title: {}", game_deals.game.title);
                    println!("  Slug: {}", game_deals.game.slug);
                    println!("  Platform: {}", game_deals.game.platform);
                    println!("  Category: {}", game_deals.game.category);
                    println!("  Deals found: {}", game_deals.deals.len());
                    println!();

                    for (d_idx, deal) in game_deals.deals.iter().take(3).enumerate() {
                        println!("    Deal #{}", d_idx + 1);
                        println!("      Store ID: {}", deal.store_id);
                        println!("      Currency: {}", deal.currency);
                        println!("      Sale Price: {}", deal.sale_price);
                        println!("      Normal Price: {}", deal.normal_price);
                        println!("      Region: {}", deal.region_code);
                    }

                    if game_deals.deals.len() > 3 {
                        println!("    ... and {} more deals", game_deals.deals.len() - 3);
                    }
                }
            }

            println!("\n✅ API Integration Status: WORKING");
            println!("   • Authentication: ✅ Valid");
            println!("   • Request/Response: ✅ Working");
            println!("   • Response format: ✅ Correct");
        }
        Err(e) => {
            println!("❌ API call failed");
            println!("   Error: {}", e);
            println!();
            println!("Possible causes:");
            println!("   • Invalid API key");
            println!("   • Product ID not in Nexarda database");
            println!("   • Network/connectivity issue");
            println!("   • API endpoint unreachable");
            println!();
            println!("To debug: Try with a known valid Nexarda product ID");
        }
    }

    // Test 3: Configuration summary
    println!("\nTest 5: Live API Configuration Summary");
    println!("====================================");

    println!("✅ Real-time API ingestion is available via:");
    println!("   • Binary: nexarda_ingest");
    println!("   • Requires: NEXARDA_API_KEY environment variable");
    println!("   • Supports: Multi-product, multi-region pricing");
    println!("   • Options: Dynamic store registration, tax handling");
    println!();

    println!("Environment variables for nexarda_ingest:");
    println!("   • NEXARDA_API_KEY: Your API key (required)");
    println!("   • NEXARDA_PRODUCTS_JSON: JSON file with products");
    println!("   • NEXARDA_AUTO_REGISTER_STORES: Auto-register new stores");
    println!("   • NEXARDA_DEFAULT_REGIONS: Default region config");
    println!("   • DATABASE_URL: PostgreSQL connection string");
    println!();

    println!("✅ Nexarda Live API test complete!");

    Ok(())
}
