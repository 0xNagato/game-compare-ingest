/// Azure/Entra ID OAuth 2.0 Authentication
///
/// Provides OAuth token acquisition for Xbox Display Catalog API access

use anyhow::{Context, Result};
use serde::Deserialize;
use std::env;
use tracing::{debug, info};

#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    expires_in: i64,
    token_type: String,
}

/// Get an OAuth 2.0 access token from Azure/Entra ID
///
/// Reads configuration from environment variables:
/// - AZURE_TENANT_ID: Azure AD tenant ID
/// - AZURE_CLIENT_ID: Application (client) ID
/// - AZURE_CLIENT_SECRET: Client secret
/// - AZURE_SCOPE: OAuth scope (default: https://onestore.microsoft.com/.default)
///
/// Returns the Bearer token as a string
pub async fn get_azure_token() -> Result<String> {
    let tenant_id = env::var("AZURE_TENANT_ID")
        .context("AZURE_TENANT_ID not set - required for Xbox API authentication")?;
    let client_id = env::var("AZURE_CLIENT_ID")
        .context("AZURE_CLIENT_ID not set - required for Xbox API authentication")?;
    let client_secret = env::var("AZURE_CLIENT_SECRET")
        .context("AZURE_CLIENT_SECRET not set - required for Xbox API authentication")?;
    let scope = env::var("AZURE_SCOPE")
        .unwrap_or_else(|_| "https://onestore.microsoft.com/.default".to_string());

    debug!(
        tenant_id = %tenant_id,
        client_id = %client_id,
        scope = %scope,
        "xbox_direct::auth: requesting OAuth token"
    );

    let url = format!(
        "https://login.microsoftonline.com/{}/oauth2/v2.0/token",
        tenant_id
    );

    let params = [
        ("client_id", client_id.as_str()),
        ("client_secret", client_secret.as_str()),
        ("grant_type", "client_credentials"),
        ("scope", scope.as_str()),
    ];

    let client = reqwest::Client::new();
    let response = client
        .post(&url)
        .form(&params)
        .send()
        .await
        .context("Failed to send Azure OAuth token request")?;

    if !response.status().is_success() {
        let status = response.status();
        let error_text = response.text().await.unwrap_or_default();
        anyhow::bail!(
            "Azure OAuth token request failed: {} - {}",
            status,
            error_text
        );
    }

    let token_data = response
        .json::<TokenResponse>()
        .await
        .context("Failed to parse Azure OAuth token response")?;

    info!(
        token_type = %token_data.token_type,
        expires_in = token_data.expires_in,
        "xbox_direct::auth: token acquired"
    );

    Ok(token_data.access_token)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore] // Requires Azure credentials in environment
    async fn test_get_azure_token() {
        dotenv::dotenv().ok();
        let result = get_azure_token().await;
        assert!(result.is_ok(), "Token acquisition failed: {:?}", result.err());
        let token = result.unwrap();
        assert!(!token.is_empty(), "Token should not be empty");
        assert!(token.len() > 100, "Token should be substantial length");
    }
}
