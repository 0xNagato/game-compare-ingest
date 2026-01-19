/// Xbox Direct API Integration
///
/// This module provides direct access to the Xbox Display Catalog API
/// for multi-region price ingestion. Unlike the wrapper-based approach,
/// this uses the official Microsoft API directly.
///
/// Architecture:
/// - auth: OAuth 2.0 authentication with Azure/Entra ID
/// - catalog: Product catalog and pricing queries
/// - regions: Market definitions and currency mappings
/// - database: Regional price storage in JSONB format

pub mod auth;
pub mod catalog;
pub mod regions;
pub mod database;

use anyhow::Result;
use tracing::info;

use crate::database_ops::db::Db;

/// Main entry point for Xbox Direct API ingestion
pub async fn run_from_env(db: &Db) -> Result<()> {
    info!("xbox_direct: starting ingestion");

    // Get authentication token
    let token = auth::get_azure_token().await?;

    // Get markets to process
    let markets = regions::get_markets_from_env();

    // Get product IDs to process
    let product_ids = get_product_ids_from_env()?;

    info!(
        markets = markets.len(),
        products = product_ids.len(),
        "xbox_direct: configuration loaded"
    );

    // Process each product across all markets
    for product_id in &product_ids {
        for market in &markets {
            match catalog::query_product(&token, product_id, &market.code).await {
                Ok(product_data) => {
                    if let Err(e) = database::store_regional_prices(
                        db,
                        &product_data,
                        &market.currency,
                        &market.code,
                    ).await {
                        tracing::warn!(
                            product_id = %product_id,
                            market = %market.code,
                            error = %e,
                            "xbox_direct: failed to store prices"
                        );
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        product_id = %product_id,
                        market = %market.code,
                        error = %e,
                        "xbox_direct: failed to query product"
                    );
                }
            }
        }
    }

    info!("xbox_direct: ingestion complete");
    Ok(())
}

/// Get product IDs from environment or use defaults
fn get_product_ids_from_env() -> Result<Vec<String>> {
    let product_ids = std::env::var("XBOX_PRODUCT_IDS")
        .unwrap_or_else(|_| {
            // Default to the 3 test products from standalone tools
            "9NHVR5PT3BBZ,9NBLGGH5KRT4,9PP5M8DTNN7J".to_string()
        });

    Ok(product_ids
        .split(',')
        .map(|s| s.trim().to_uppercase())
        .filter(|s| !s.is_empty())
        .collect())
}
