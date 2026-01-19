/*!
 * Xbox Store API Price Checker - Standalone Version
 *
 * Fetches and displays pricing information for NBA Live 26 across multiple SKU regions.
 *
 * Usage:
 *   AZURE_TENANT_ID=xxx AZURE_CLIENT_ID=xxx AZURE_CLIENT_SECRET=xxx cargo script xbox_price_standalone.rs
 *
 * Or compile and run:
 *   rustc xbox_price_standalone.rs && ./xbox_price_standalone
 */

use std::env;

// Cargo dependencies
//! ```cargo
//! [dependencies]
//! anyhow = "1.0"
//! reqwest = { version = "0.12", features = ["json", "blocking"] }
//! serde = { version = "1.0", features = ["derive"] }
//! serde_json = "1.0"
//! tokio = { version = "1", features = ["full"] }
//! ```

fn main() {
    println!("\n🎮 Xbox Store API - NBA Live 26 Price Checker (Standalone)");
    println!("═══════════════════════════════════════════════════════════\n");

    // Load credentials from environment
    let tenant_id = env::var("AZURE_TENANT_ID")
        .expect("AZURE_TENANT_ID not set");
    let client_id = env::var("AZURE_CLIENT_ID")
        .expect("AZURE_CLIENT_ID not set");
    let client_secret = env::var("AZURE_CLIENT_SECRET")
        .expect("AZURE_CLIENT_SECRET not set");

    println!("🔑 Authenticating with Microsoft Entra ID...");

    // Get access token
    let client = reqwest::blocking::Client::new();
    let token_url = format!("https://login.microsoftonline.com/{}/oauth2/v2.0/token", tenant_id);

    let params = [
        ("client_id", client_id.as_str()),
        ("client_secret", client_secret.as_str()),
        ("grant_type", "client_credentials"),
            // NOTE: Even though we call displaycatalog.mp.microsoft.com, app-only tokens must be
            // minted for onestore to get `aud=https://onestore.microsoft.com`.
            ("scope", "https://onestore.microsoft.com/.default"),
    ];

    let response = client
        .post(&token_url)
        .form(&params)
        .send()
        .expect("Failed to send token request");

    let token_json: serde_json::Value = response.json().expect("Failed to parse token response");
    let access_token = token_json["access_token"]
        .as_str()
        .expect("No access token in response");

    println!("✅ Authentication successful!\n");

    // NBA 2K25 Product ID (using as example since NBA Live 26 may not be released)
    let product_id = "9NBLGGH537BL";
    let markets = vec!["US", "GB", "JP", "AU", "CA", "DE"];

    println!("🔍 Querying Xbox Store API for product: {}", product_id);
    println!("📍 Markets: {}\n", markets.join(", "));

    // Query Xbox Store API
    let api_url = "https://displaycatalog.mp.microsoft.com/v7.0/products/lookup";

    let query_json = serde_json::json!({
        "BigIds": [product_id],
        "Markets": markets,
    });

    let api_response = client
        .post(api_url)
        .bearer_auth(access_token)
        .json(&query_json)
        .send()
        .expect("Failed to send API request");

    let result: serde_json::Value = api_response.json().expect("Failed to parse API response");

    // Display results
    println!("═══════════════════════════════════════════════════════════");
    println!("                NBA LIVE 26 - PRICING RESULTS              ");
    println!("═══════════════════════════════════════════════════════════\n");

    if let Some(products) = result["Products"].as_array() {
        if products.is_empty() {
            println!("❌ No products found");
            println!("\nℹ️  This could mean:");
            println!("   • The product ID doesn't exist");
            println!("   • The product isn't available in the requested markets");
            println!("   • NBA Live 26 hasn't been released yet\n");
            return;
        }

        for product in products {
            let title = product["ProductTitle"].as_str().unwrap_or("Unknown Title");
            let prod_id = product["ProductId"].as_str().unwrap_or("Unknown ID");

            println!("🎮 {}", title);
            println!("📦 Product ID: {}\n", prod_id);

            if let Some(skus) = product["SkuAvailabilities"].as_array() {
                for (market_idx, market) in markets.iter().enumerate() {
                    println!("┌─ 📍 Market: {}", market);

                    let mut found_price = false;

                    for sku in skus {
                        if let Some(availabilities) = sku["Availabilities"].as_array() {
                            for availability in availabilities {
                                if let Some(price) = availability["OrderManagementData"]["Price"].as_object() {
                                    found_price = true;

                                    let list_price = price.get("ListPrice")
                                        .and_then(|v| v.as_f64())
                                        .unwrap_or(0.0);

                                    let currency = price.get("CurrencyCode")
                                        .and_then(|v| v.as_str())
                                        .unwrap_or("???");

                                    let sku_title = sku["Sku"]["SkuTitle"]
                                        .as_str()
                                        .unwrap_or("Standard Edition");

                                    println!("│  📋 {}", sku_title);
                                    println!("│  💰 List Price: {:.2} {}", list_price, currency);
                                }
                            }
                        }
                    }

                    if !found_price {
                        println!("│  ⚠️  No pricing information available");
                    }

                    if market_idx < markets.len() - 1 {
                        println!("└────────────────────────────────────────");
                    } else {
                        println!("└────────────────────────────────────────\n");
                    }
                }
            }
        }
    }

    println!("═══════════════════════════════════════════════════════════");
    println!("\n✅ Query complete!");
}
