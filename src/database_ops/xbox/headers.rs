/// Xbox Live Service Headers Construction
///
/// This module handles construction of required headers for Xbox Live API calls
/// based on environment variables and authentication tokens.
///
/// Required Headers for Xbox Live Services:
/// - Authorization: XSTSv3 t=<token>
/// - x-xbl-contract-version: API version (e.g., "2" for catalog APIs)
/// - MS-CV: Correlation Vector for request tracking
/// - Signature: (Optional) Request signature for some endpoints
///
/// Environment Variables:
/// - XBOX_CLIENT_ID: MSA app client ID
/// - XBOX_SANDBOX_ID: Sandbox identifier (e.g., MCCQXF.58)
/// - XBOX_WEB_SERVICE_ID: Service configuration ID
/// - XBOX_PACKAGE_FAMILY_NAME: App package family name
/// - XBOX_PACKAGE_SID: Package security identifier
/// - XBOX_APP_URI: App URI for token audience

use anyhow::{Context, Result};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use std::collections::HashMap;
use tracing::{debug, warn};

/// Xbox Live API contract versions for different services
pub mod contract_versions {
    /// Display Catalog API version
    pub const DISPLAY_CATALOG: &str = "2";
    
    /// EDS (Entertainment Discovery Services) version
    pub const EDS: &str = "3";
    
    /// Collections API version
    pub const COLLECTIONS: &str = "2";
    
    /// Profile API version
    pub const PROFILE: &str = "2";
}

/// Correlation Vector generator for MS-CV header
#[derive(Debug)]
pub struct CorrelationVector {
    base: String,
    counter: u32,
}

impl CorrelationVector {
    /// Create a new correlation vector with random base
    pub fn new() -> Self {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let base = format!(
            "{}.{}",
            rng.gen::<u64>(),
            rng.gen::<u32>()
        );
        Self { base, counter: 0 }
    }

    /// Get the next value in the correlation vector chain
    pub fn next(&mut self) -> String {
        let value = format!("{}.{}", self.base, self.counter);
        self.counter += 1;
        value
    }

    /// Get current value without incrementing
    pub fn current(&self) -> String {
        format!("{}.{}", self.base, self.counter)
    }
}

impl Default for CorrelationVector {
    fn default() -> Self {
        Self::new()
    }
}

/// Xbox Live service configuration from environment variables
#[derive(Debug, Clone)]
pub struct XboxLiveConfig {
    pub client_id: Option<String>,
    pub sandbox_id: Option<String>,
    pub web_service_id: Option<String>,
    pub package_family_name: Option<String>,
    pub package_sid: Option<String>,
    pub app_uri: Option<String>,
}

impl XboxLiveConfig {
    /// Load configuration from environment variables
    pub fn from_env() -> Self {
        Self {
            client_id: std::env::var("XBOX_CLIENT_ID").ok(),
            sandbox_id: std::env::var("XBOX_SANDBOX_ID").ok(),
            web_service_id: std::env::var("XBOX_WEB_SERVICE_ID").ok(),
            package_family_name: std::env::var("XBOX_PACKAGE_FAMILY_NAME").ok(),
            package_sid: std::env::var("XBOX_PACKAGE_SID").ok(),
            app_uri: std::env::var("XBOX_APP_URI").ok(),
        }
    }

    /// Validate that required fields are present for Xbox Live authentication
    pub fn validate_for_auth(&self) -> Result<()> {
        if self.client_id.is_none() {
            anyhow::bail!("XBOX_CLIENT_ID is required for Xbox Live authentication");
        }
        Ok(())
    }

    /// Get additional context headers that may be required by some endpoints
    pub fn context_headers(&self) -> HashMap<String, String> {
        let mut headers = HashMap::new();
        
        if let Some(sandbox) = &self.sandbox_id {
            headers.insert("X-XBL-Sandbox-ID".to_string(), sandbox.clone());
        }
        
        if let Some(service_id) = &self.web_service_id {
            headers.insert("X-XBL-WebService-ID".to_string(), service_id.clone());
        }
        
        headers
    }
}

/// Build standard Xbox Live API request headers
///
/// # Arguments
/// * `xsts_token` - XSTS token from authentication (without "XSTSv3 t=" prefix)
/// * `contract_version` - API contract version (use constants from contract_versions)
/// * `cv` - Correlation vector for request tracking
/// * `config` - Xbox Live configuration from environment
///
/// # Returns
/// HeaderMap ready to attach to reqwest request
pub fn build_xbox_live_headers(
    xsts_token: &str,
    contract_version: &str,
    cv: &str,
    config: &XboxLiveConfig,
) -> Result<HeaderMap> {
    let mut headers = HeaderMap::new();

    // Authorization header with XSTS token
    // Format: "XSTSv3 t=<token>"
    let auth_value = if xsts_token.starts_with("XSTSv3") || xsts_token.starts_with("XBL3.0") {
        // Token already has prefix
        xsts_token.to_string()
    } else {
        // Add XSTSv3 prefix
        format!("XSTSv3 t={}", xsts_token)
    };
    
    headers.insert(
        reqwest::header::AUTHORIZATION,
        HeaderValue::from_str(&auth_value)
            .context("Failed to create Authorization header")?,
    );

    // Contract version header
    headers.insert(
        HeaderName::from_static("x-xbl-contract-version"),
        HeaderValue::from_str(contract_version)
            .context("Failed to create x-xbl-contract-version header")?,
    );

    // Correlation Vector
    headers.insert(
        HeaderName::from_static("ms-cv"),
        HeaderValue::from_str(cv).context("Failed to create MS-CV header")?,
    );

    // Optional context headers
    for (key, value) in config.context_headers() {
        if let Ok(header_name) = HeaderName::from_bytes(key.to_lowercase().as_bytes()) {
            if let Ok(header_value) = HeaderValue::from_str(&value) {
                headers.insert(header_name, header_value);
            } else {
                warn!("Invalid header value for {}: {}", key, value);
            }
        } else {
            warn!("Invalid header name: {}", key);
        }
    }

    debug!(
        "Built Xbox Live headers: Authorization={}, contract_version={}, cv={}",
        if auth_value.len() > 20 {
            &auth_value[..20]
        } else {
            &auth_value
        },
        contract_version,
        cv
    );

    Ok(headers)
}

/// Helper to extract XSTS token authorization header value
///
/// Handles both formats:
/// - Raw token string
/// - Already formatted "XSTSv3 t=<token>"
pub fn format_xsts_authorization(token: &str) -> String {
    if token.starts_with("XSTSv3 ") || token.starts_with("XBL3.0 ") {
        token.to_string()
    } else {
        format!("XSTSv3 t={}", token)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_correlation_vector() {
        let mut cv = CorrelationVector::new();
        let v1 = cv.next();
        let v2 = cv.next();
        
        assert!(v1.contains('.'));
        assert!(v2.contains('.'));
        assert_ne!(v1, v2);
        
        // Second call should have incremented counter
        assert!(v2.ends_with(".1"));
    }

    #[test]
    fn test_format_xsts_authorization() {
        // Raw token should get prefix
        let raw = "eyJ0eXAiOiJKV1QiLCJhbGc...";
        assert_eq!(
            format_xsts_authorization(raw),
            format!("XSTSv3 t={}", raw)
        );

        // Already formatted should pass through
        let formatted = "XSTSv3 t=eyJ0eXAiOiJKV1QiLCJhbGc...";
        assert_eq!(format_xsts_authorization(formatted), formatted);
    }

    #[test]
    fn test_xbox_config_from_env() {
        std::env::set_var("XBOX_CLIENT_ID", "test-client-id");
        std::env::set_var("XBOX_SANDBOX_ID", "RETAIL.0");
        
        let config = XboxLiveConfig::from_env();
        
        assert_eq!(config.client_id.as_deref(), Some("test-client-id"));
        assert_eq!(config.sandbox_id.as_deref(), Some("RETAIL.0"));
        
        std::env::remove_var("XBOX_CLIENT_ID");
        std::env::remove_var("XBOX_SANDBOX_ID");
    }

    #[test]
    fn test_build_headers() {
        let config = XboxLiveConfig {
            client_id: Some("test-id".to_string()),
            sandbox_id: Some("RETAIL.0".to_string()),
            web_service_id: None,
            package_family_name: None,
            package_sid: None,
            app_uri: None,
        };

        let token = "test_token_value";
        let cv = "test.cv.0";
        
        let headers = build_xbox_live_headers(
            token,
            contract_versions::DISPLAY_CATALOG,
            cv,
            &config,
        )
        .unwrap();

        assert!(headers.contains_key(reqwest::header::AUTHORIZATION));
        assert!(headers.contains_key("x-xbl-contract-version"));
        assert!(headers.contains_key("ms-cv"));
        
        let auth = headers.get(reqwest::header::AUTHORIZATION).unwrap();
        assert!(auth.to_str().unwrap().starts_with("XSTSv3 t="));
    }
}
