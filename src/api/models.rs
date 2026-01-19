// API request/response models (DTOs)

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Standard API response wrapper
#[derive(Debug, Serialize, Deserialize)]
pub struct ApiResponse<T> {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<Meta>,
}

impl<T> ApiResponse<T> {
    pub fn success(data: T) -> Self {
        Self {
            success: true,
            data: Some(data),
            error: None,
            meta: Some(Meta::now()),
        }
    }

    pub fn error(message: impl Into<String>) -> Self {
        Self {
            success: false,
            data: None,
            error: Some(message.into()),
            meta: Some(Meta::now()),
        }
    }
}

/// Metadata included in all API responses
#[derive(Debug, Serialize, Deserialize)]
pub struct Meta {
    pub timestamp: DateTime<Utc>,
    pub request_id: String,
    pub version: String,
}

impl Meta {
    pub fn now() -> Self {
        Self {
            timestamp: Utc::now(),
            request_id: uuid::Uuid::new_v4().to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }
}

/// Health check response
#[derive(Debug, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
    pub database: String,
    pub uptime_seconds: u64,
}

/// Ingestion trigger request
#[derive(Debug, Serialize, Deserialize)]
pub struct IngestTriggerRequest {
    /// Provider IDs to run (e.g., ["playstation_store", "steam_store"])
    pub providers: Vec<String>,
    #[serde(default)]
    pub skip_alerts: bool,
    #[serde(default)]
    pub dry_run: bool,
}

/// Ingestion status response
#[derive(Debug, Serialize, Deserialize)]
pub struct IngestStatusResponse {
    pub providers: Vec<ProviderStatus>,
    pub total_active: usize,
    pub total_completed: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProviderStatus {
    pub provider_id: String,
    pub status: String, // "idle", "running", "completed", "failed"
    pub last_run: Option<DateTime<Utc>>,
    pub duration_seconds: Option<u64>,
    pub items_processed: Option<i64>,
}

/// Price query request
#[derive(Debug, Serialize, Deserialize)]
pub struct PriceQueryRequest {
    pub product_id: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub jurisdiction: Option<String>,
}

/// Current price response
#[derive(Debug, Serialize, Deserialize)]
pub struct PriceData {
    pub product_id: i64,
    pub provider: String,
    pub jurisdiction: String,
    pub currency: String,
    pub amount_minor: i64,
    pub amount: String,
    pub timestamp: DateTime<Utc>,
}

/// Error details for debugging
#[derive(Debug, Serialize, Deserialize)]
pub struct ErrorDetail {
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
}
