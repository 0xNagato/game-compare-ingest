/*!
 * Xbox Store API Price Checker
 *
 * Fetches and displays pricing information for NBA Live 26 across multiple SKU regions.
 *
 * Usage:
 *   cargo run --bin xbox_price_check
 *
 * Environment Variables Required:
 *   AZURE_TENANT_ID - Your Azure tenant ID
 *   AZURE_CLIENT_ID - Your Azure application (client) ID
 *   AZURE_CLIENT_SECRET - Your Azure client secret
 */

use anyhow::{Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::env;

#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    expires_in: i64,
    token_type: String,
}

#[derive(Debug, Serialize)]
struct PriceQuery {
    #[serde(rename = "BigIds")]
    big_ids: Vec<String>,
    #[serde(rename = "Markets")]
    markets: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ProductResponse {
    #[serde(rename = "Products")]
    products: Option<Vec<Product>>,
}

#[derive(Debug, Deserialize)]
struct Product {
    #[serde(rename = "ProductId")]
    product_id: Option<String>,
    #[serde(rename = "ProductTitle")]
    product_title: Option<String>,
    #[serde(rename = "SkuAvailabilities")]
    sku_availabilities: Option<Vec<SkuAvailability>>,
}

#[derive(Debug, Deserialize)]
struct SkuAvailability {
    #[serde(rename = "Sku")]
    sku: Option<Sku>,
    #[serde(rename = "Availabilities")]
    availabilities: Option<Vec<Availability>>,
}

#[derive(Debug, Deserialize)]
struct Sku {
    #[serde(rename = "SkuId")]
    sku_id: Option<String>,
    #[serde(rename = "SkuTitle")]
    sku_title: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Availability {
    #[serde(rename = "OrderManagementData")]
    order_management_data: Option<OrderManagementData>,
}

#[derive(Debug, Deserialize)]
struct OrderManagementData {
    #[serde(rename = "Price")]
    price: Option<Price>,
}

#[derive(Debug, Deserialize)]
struct Price {
    #[serde(rename = "ListPrice")]
    list_price: Option<f64>,
    #[serde(rename = "MSRP")]
    msrp: Option<f64>,
    #[serde(rename = "CurrencyCode")]
    currency_code: Option<String>,
}

/// Fetch OAuth access token from Microsoft Entra ID
async fn get_access_token(tenant_id: &str, client_id: &str, client_secret: &str) -> Result<String> {
    let url = format!(
        "https://login.microsoftonline.com/{}/oauth2/v2.0/token",
        tenant_id
    );

    let params = [
        ("client_id", client_id),
        ("client_secret", client_secret),
        ("grant_type", "client_credentials"),
        // NOTE: Even though we call displaycatalog.mp.microsoft.com, app-only tokens must be
        // minted for onestore to get `aud=https://onestore.microsoft.com`.
        ("scope", "https://onestore.microsoft.com/.default"),
    ];

    println!("🔑 Authenticating with Microsoft Entra ID...");

    let client = Client::new();
    let response = client
        .post(&url)
        .form(&params)
        .send()
        .await
        .context("Failed to send token request")?;

    if !response.status().is_success() {
        let status = response.status();
        let error_text = response.text().await.unwrap_or_default();
        anyhow::bail!("Token request failed: {} - {}", status, error_text);
    }

    let token_response = response
        .json::<TokenResponse>()
        .await
        .context("Failed to parse token response")?;

    println!(
        "✅ Authentication successful! Token expires in {} seconds\n",
        token_response.expires_in
    );

    Ok(token_response.access_token)
}

/// Query Xbox Store Display Catalog API for product pricing
async fn get_product_prices(
    access_token: &str,
    product_id: &str,
    markets: &[&str],
) -> Result<ProductResponse> {
    let url = "https://displaycatalog.mp.microsoft.com/v7.0/products/lookup";

    let query = PriceQuery {
        big_ids: vec![product_id.to_string()],
        markets: markets.iter().map(|s| s.to_string()).collect(),
    };

    println!("🔍 Querying Xbox Store API for product: {}", product_id);
    println!("📍 Markets: {}\n", markets.join(", "));

    let client = Client::new();
    let response = client
        .post(url)
        .bearer_auth(access_token)
        .json(&query)
        .send()
        .await
        .context("Failed to send product lookup request")?;

    if !response.status().is_success() {
        let status = response.status();
        let error_text = response.text().await.unwrap_or_default();
        anyhow::bail!("Product lookup failed: {} - {}", status, error_text);
    }

    let product_response = response
        .json::<ProductResponse>()
        .await
        .context("Failed to parse product response")?;

    Ok(product_response)
}

/// Display pricing information in a formatted table
fn display_prices(product_response: &ProductResponse, markets: &[&str]) {
    println!("═══════════════════════════════════════════════════════════");
    println!("                NBA LIVE 26 - PRICING RESULTS              ");
    println!("═══════════════════════════════════════════════════════════\n");

    let products = match &product_response.products {
        Some(p) => p,
        None => {
            println!("❌ No products found in response");
            return;
        }
    };

    if products.is_empty() {
        println!("❌ No products returned from API");
        println!("\nℹ️  This could mean:");
        println!("   • The product ID doesn't exist");
        println!("   • The product isn't available in the requested markets");
        println!("   • NBA Live 26 hasn't been released yet");
        return;
    }

    for product in products {
        let title = product.product_title.as_deref().unwrap_or("Unknown Title");

        let product_id = product.product_id.as_deref().unwrap_or("Unknown ID");

        println!("🎮 {}", title);
        println!("📦 Product ID: {}\n", product_id);

        let skus = match &product.sku_availabilities {
            Some(s) => s,
            None => {
                println!("⚠️  No SKU availabilities found\n");
                continue;
            }
        };

        for (market_idx, market) in markets.iter().enumerate() {
            println!("┌─ 📍 Market: {}", market);

            let mut found_price = false;

            for sku in skus {
                let sku_title = sku
                    .sku
                    .as_ref()
                    .and_then(|s| s.sku_title.as_deref())
                    .unwrap_or("Standard Edition");

                if let Some(availabilities) = &sku.availabilities {
                    for availability in availabilities {
                        if let Some(order_data) = &availability.order_management_data {
                            if let Some(price_info) = &order_data.price {
                                found_price = true;

                                let list_price = price_info.list_price.unwrap_or(0.0);
                                let msrp = price_info.msrp.unwrap_or(0.0);
                                let currency = price_info.currency_code.as_deref().unwrap_or("???");

                                println!("│  📋 {}", sku_title);
                                println!("│  💰 List Price: {:.2} {}", list_price, currency);
                                if msrp > 0.0 && (msrp - list_price).abs() > 0.01 {
                                    println!("│  🏷️  MSRP: {:.2} {}", msrp, currency);
                                    let discount = ((msrp - list_price) / msrp * 100.0).round();
                                    println!("│  🎉 Discount: {}%", discount);
                                }
                            }
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

    println!("═══════════════════════════════════════════════════════════");
}

#[tokio::main]
async fn main() -> Result<()> {
    // Bootstrap environment
    i_miss_rust::util::env::bootstrap_cli("xbox_price_check");
    dotenv::dotenv().ok();

    println!("\n🎮 Xbox Store API - NBA Live 26 Price Checker");
    println!("═══════════════════════════════════════════════════════════\n");

    // Load credentials from environment
    let tenant_id = env::var("AZURE_TENANT_ID").context("AZURE_TENANT_ID not set in .env")?;
    let client_id = env::var("AZURE_CLIENT_ID").context("AZURE_CLIENT_ID not set in .env")?;
    let client_secret =
        env::var("AZURE_CLIENT_SECRET").context("AZURE_CLIENT_SECRET not set in .env")?;

    // Get access token
    let access_token = get_access_token(&tenant_id, &client_id, &client_secret).await?;

    // NBA Live 26 Product ID (Note: This is a placeholder - replace with actual ID when available)
    // For testing, we'll use NBA 2K25 as NBA Live 26 may not be released yet
    let product_id = "9NBLGGH537BL"; // NBA 2K series example

    // Query multiple SKU regions
    let markets = vec!["US", "GB", "JP", "AU", "CA", "DE"];

    println!("ℹ️  Note: NBA Live 26 may not be released yet.");
    println!("   Using NBA 2K product for demonstration.\n");

    let product_response = get_product_prices(&access_token, product_id, &markets).await?;

    // Display results
    display_prices(&product_response, &markets);

    println!("\n💡 Tips:");
    println!("   • Update the product_id variable with the actual NBA Live 26 ID");
    println!("   • Add/remove markets in the markets vector as needed");
    println!("   • Common markets: US, GB, FR, DE, JP, AU, CA, BR, MX");

    Ok(())
}
