/*!
 * Xbox Store Price Checker
 * Standalone binary to query Xbox Store prices across multiple SKU regions
 */

use anyhow::{Context, Result};
use psstore_client::get_env;
use serde::Deserialize;
use serde_json::Value;
use std::env;

#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    expires_in: i64,
}

async fn get_token() -> Result<String> {
    let tenant_id = get_env("AZURE_TENANT_ID");
    let client_id = get_env("AZURE_CLIENT_ID");
    let client_secret = get_env("AZURE_CLIENT_SECRET");

    let url = format!(
        "https://login.microsoftonline.com/{}/oauth2/v2.0/token",
        tenant_id
    );

    let params = [
        ("client_id", client_id.as_str()),
        ("client_secret", client_secret.as_str()),
        ("grant_type", "client_credentials"),
        ("scope", "https://onestore.microsoft.com/.default"),
    ];

    let client = reqwest::Client::new();
    let response = client.post(&url).form(&params).send().await?;

    if !response.status().is_success() {
        anyhow::bail!("Token request failed: {}", response.status());
    }

    let token_data: TokenResponse = response.json().await?;
    Ok(token_data.access_token)
}

async fn query_product(token: &str, product_id: &str, market: &str) -> Result<Value> {
    let url = format!(
        "https://displaycatalog.mp.microsoft.com/v7.0/products?bigIds={}&market={}&languages=en-us",
        product_id, market
    );

    let client = reqwest::Client::new();
    let response = client
        .get(&url)
        .bearer_auth(token)
        .header("MS-CV", "1")
        .send()
        .await?;

    if !response.status().is_success() {
        anyhow::bail!("API request failed: {}", response.status());
    }

    let result: Value = response.json().await?;
    Ok(result)
}

fn display_prices(result: &Value, game_name: &str, market: &str) {
    if let Some(products) = result["Products"].as_array() {
        if products.is_empty() {
            println!("  ⚠️  Not available in {} market", market);
            return;
        }

        for product in products {
            let title = product["ProductTitle"].as_str().unwrap_or(game_name);

            if let Some(skus) = product["DisplaySkuAvailabilities"].as_array() {
                for sku in skus {
                    if let Some(availabilities) = sku["Availabilities"].as_array() {
                        for availability in availabilities {
                            if let Some(price_obj) =
                                availability["OrderManagementData"]["Price"].as_object()
                            {
                                let list_price = price_obj
                                    .get("ListPrice")
                                    .and_then(|v| v.as_f64())
                                    .unwrap_or(0.0);

                                let msrp = price_obj
                                    .get("MSRP")
                                    .and_then(|v| v.as_f64())
                                    .unwrap_or(0.0);

                                let currency = price_obj
                                    .get("CurrencyCode")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("???");

                                println!("  ✅ {}", title);
                                println!("  💰 {}: {:.2} {}", market, list_price, currency);

                                if msrp > 0.0 && (msrp - list_price).abs() > 0.01 {
                                    let discount = ((msrp - list_price) / msrp * 100.0).round();
                                    println!(
                                        "  🎉 {}% OFF (was {:.2} {})",
                                        discount, msrp, currency
                                    );
                                }
                                return;
                            }
                        }
                    }
                }
            }

            println!("  📦 {} (Price not available)", title);
        }
    } else {
        println!("  ⚠️  No data for {} market", market);
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    println!("\n🎮 Xbox Store Price Checker");
    println!("═══════════════════════════════════════════════════════════\n");

    println!("🔑 Authenticating...");
    let token = get_token().await.context("Failed to get access token")?;
    println!("✅ Authenticated!\n");

    // Product catalog - add your own product IDs here
    let products = vec![
        ("9NKX70BBCDRN", "Forza Horizon 5"),
        ("9P2N57MC619K", "Minecraft"),
        ("9N8G2FQ76HVC", "Halo Infinite"),
    ];

    // Markets to query
    let markets = vec!["US", "GB", "JP", "AU"];

    for (product_id, game_name) in &products {
        println!("═══════════════════════════════════════════════════════════");
        println!("🎮 {}", game_name);
        println!("═══════════════════════════════════════════════════════════\n");

        for market in &markets {
            println!("📍 {}:", market);

            match query_product(&token, product_id, market).await {
                Ok(result) => display_prices(&result, game_name, market),
                Err(e) => println!("  ❌ Error: {}", e),
            }
            println!();
        }
    }

    println!("═══════════════════════════════════════════════════════════");
    println!(
        "\n✅ Done! To add more games, edit the products vector in src/bin/xbox_store_prices.rs"
    );

    Ok(())
}
