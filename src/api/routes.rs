// API route configuration

use crate::api::handlers;
use actix_web::web;

pub fn configure_routes(cfg: &mut web::ServiceConfig) {
    cfg
        // Health check (no auth required)
        .route("/health", web::get().to(handlers::health_check))
        .route("/", web::get().to(handlers::health_check))
        // API v1 routes (all require authentication)
        .service(
            web::scope("/api/v1")
                // Ingestion control
                .route(
                    "/ingest/trigger",
                    web::post().to(handlers::trigger_ingestion),
                )
                .route(
                    "/ingest/status",
                    web::get().to(handlers::get_ingestion_status),
                )
                // Price queries
                .route(
                    "/prices/current",
                    web::get().to(handlers::get_current_prices),
                )
                // Provider management
                .route("/providers", web::get().to(handlers::list_providers))
                .route(
                    "/providers/{slug}/stats",
                    web::get().to(handlers::get_provider_stats),
                )
                .route(
                    "/providers/{slug}/refresh",
                    web::post().to(handlers::refresh_provider),
                ),
        );
}
