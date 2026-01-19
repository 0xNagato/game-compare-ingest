// HTTP request handlers for API endpoints

use crate::api::models::*;
use crate::database_ops::db::Db;
use actix_web::{web, HttpResponse, Result};
use std::time::SystemTime;

/// Health check endpoint
pub async fn health_check(db: web::Data<Db>) -> Result<HttpResponse> {
    // Quick database connectivity check
    let db_status = match sqlx::query_scalar::<_, bool>("SELECT true")
        .fetch_one(&db.pool)
        .await
    {
        Ok(_) => "connected",
        Err(_) => "disconnected",
    };

    let uptime = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let response = ApiResponse::success(HealthResponse {
        status: "healthy".to_string(),
        database: db_status.to_string(),
        uptime_seconds: uptime,
    });

    Ok(HttpResponse::Ok().json(response))
}

/// Trigger ingestion for specified providers
pub async fn trigger_ingestion(
    payload: web::Json<IngestTriggerRequest>,
    _db: web::Data<Db>,
) -> Result<HttpResponse> {
    tracing::info!(
        providers = ?payload.providers,
        skip_alerts = payload.skip_alerts,
        dry_run = payload.dry_run,
        "Ingestion trigger requested"
    );

    // TODO: Implement actual ingestion trigger
    // For now, return placeholder response

    let response = ApiResponse::success(serde_json::json!({
        "message": "Ingestion triggered",
        "providers": payload.providers,
        "status": "queued"
    }));

    Ok(HttpResponse::Accepted().json(response))
}

/// Get ingestion status for all providers
pub async fn get_ingestion_status(_db: web::Data<Db>) -> Result<HttpResponse> {
    // TODO: Query actual ingestion status from database
    // Placeholder response for now

    let response = ApiResponse::success(IngestStatusResponse {
        providers: vec![
            ProviderStatus {
                provider_id: "playstation_store".to_string(),
                status: "idle".to_string(),
                last_run: None,
                duration_seconds: None,
                items_processed: None,
            },
            ProviderStatus {
                provider_id: "steam_store".to_string(),
                status: "idle".to_string(),
                last_run: None,
                duration_seconds: None,
                items_processed: None,
            },
        ],
        total_active: 0,
        total_completed: 0,
    });

    Ok(HttpResponse::Ok().json(response))
}

/// Query current prices for a product
pub async fn get_current_prices(
    query: web::Query<PriceQueryRequest>,
    _db: web::Data<Db>,
) -> Result<HttpResponse> {
    tracing::info!(
        product_id = query.product_id,
        provider = ?query.provider,
        jurisdiction = ?query.jurisdiction,
        "Price query requested"
    );

    // TODO: Implement actual price query
    // Placeholder response

    let response = ApiResponse::success(vec![PriceData {
        product_id: query.product_id,
        provider: "playstation_store".to_string(),
        jurisdiction: "US".to_string(),
        currency: "USD".to_string(),
        amount_minor: 5999,
        amount: "59.99".to_string(),
        timestamp: chrono::Utc::now(),
    }]);

    Ok(HttpResponse::Ok().json(response))
}

/// List all providers with their configuration
pub async fn list_providers(_db: web::Data<Db>) -> Result<HttpResponse> {
    // TODO: Query providers from database

    let providers = vec![
        serde_json::json!({
            "id": "playstation_store",
            "name": "PlayStation Store",
            "enabled": true,
            "status": "active"
        }),
        serde_json::json!({
            "id": "steam_store",
            "name": "Steam Store",
            "enabled": true,
            "status": "active"
        }),
        serde_json::json!({
            "id": "microsoft_store",
            "name": "Microsoft Store",
            "enabled": true,
            "status": "active"
        }),
    ];

    let response = ApiResponse::success(providers);
    Ok(HttpResponse::Ok().json(response))
}

/// Get provider-specific statistics
pub async fn get_provider_stats(
    path: web::Path<String>,
    _db: web::Data<Db>,
) -> Result<HttpResponse> {
    let provider_slug = path.into_inner();

    tracing::info!(provider = %provider_slug, "Provider stats requested");

    // TODO: Query actual stats from database

    let stats = serde_json::json!({
        "provider": provider_slug,
        "total_products": 0,
        "total_prices": 0,
        "last_update": null,
        "status": "active"
    });

    let response = ApiResponse::success(stats);
    Ok(HttpResponse::Ok().json(response))
}

/// Force refresh from a specific provider
pub async fn refresh_provider(path: web::Path<String>, _db: web::Data<Db>) -> Result<HttpResponse> {
    let provider_slug = path.into_inner();

    tracing::info!(provider = %provider_slug, "Provider refresh requested");

    // TODO: Trigger actual provider refresh

    let response = ApiResponse::success(serde_json::json!({
        "message": format!("Refresh queued for {}", provider_slug),
        "status": "queued"
    }));

    Ok(HttpResponse::Accepted().json(response))
}
