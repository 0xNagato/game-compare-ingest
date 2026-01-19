//! media_health_http.rs
//! Actix-web service exposing /health JSON with lifted media URL coverage metrics.
//! Env:
//!   DATABASE_URL=postgres://...
//!   PORT=8080 (optional)
//!   HEALTH_MIN_PERCENT_WITH_ANY=80 (optional threshold for degraded status)
//!   HEALTH_MAX_PERCENT_MISSING_ORIGINAL=50 (optional)

use actix_web::middleware::Logger;
use actix_web::{get, App, HttpResponse, HttpServer, Responder};
use anyhow::{Context, Result};
use serde::Serialize;
use sqlx::{Pool, Postgres};
use std::env;
use std::sync::Arc;
use tracing::info;
use tracing_subscriber::{fmt, EnvFilter};

#[derive(Serialize)]
struct HealthPayload {
    status: String,
    total_media: i64,
    lifted_any: i64,
    missing_original: i64,
    recent_lifted_30d: i64,
    percent_with_any: f64,
    percent_missing_original: f64,
    percent_recent_lifted: f64,
    thresholds: Thresholds,
}

#[derive(Serialize, Clone, Copy)]
struct Thresholds {
    min_percent_with_any: f64,
    max_percent_missing_original: Option<f64>,
}

struct AppState {
    pool: Pool<Postgres>,
    thresholds: Thresholds,
}

#[get("/health")]
async fn health(data: actix_web::web::Data<Arc<AppState>>) -> impl Responder {
    match gather_metrics(&data.pool).await {
        Ok(m) => {
            let status = derive_status(&m, data.thresholds);
            let payload = HealthPayload {
                status,
                total_media: m.total_media,
                lifted_any: m.lifted_any,
                missing_original: m.missing_original,
                recent_lifted_30d: m.recent_lifted_30d,
                percent_with_any: m.percent_with_any,
                percent_missing_original: m.percent_missing_original,
                percent_recent_lifted: m.percent_recent_lifted,
                thresholds: data.thresholds,
            };
            let code = if payload.status == "ok" { 200 } else { 503 };
            HttpResponse::build(actix_web::http::StatusCode::from_u16(code).unwrap()).json(payload)
        }
        Err(e) => {
            HttpResponse::InternalServerError().json(serde_json::json!({"error": e.to_string()}))
        }
    }
}

#[actix_web::main]
async fn main() -> Result<()> {
    i_miss_rust::util::env::bootstrap_cli("media_health_http");
    init_tracing();
    let db_url = env::var("DATABASE_URL").context("DATABASE_URL required")?;
    let pool = Pool::<Postgres>::connect(&db_url).await?;

    // Pre-warm by running one metric collection (fail fast on schema issues)
    let _ = gather_metrics(&pool).await?;
    let thresholds = Thresholds {
        min_percent_with_any: env::var("HEALTH_MIN_PERCENT_WITH_ANY")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0),
        max_percent_missing_original: env::var("HEALTH_MAX_PERCENT_MISSING_ORIGINAL")
            .ok()
            .and_then(|s| s.parse().ok()),
    };

    let state = actix_web::web::Data::new(Arc::new(AppState { pool, thresholds }));
    let port: u16 = env::var("PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8080);
    info!(port, "starting media health HTTP server");
    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .app_data(state.clone())
            .service(health)
    })
    .bind(("0.0.0.0", port))?
    .run()
    .await?;
    Ok(())
}

#[derive(Serialize)]
struct Metrics {
    total_media: i64,
    lifted_any: i64,
    missing_original: i64,
    recent_lifted_30d: i64,
    percent_with_any: f64,
    percent_missing_original: f64,
    percent_recent_lifted: f64,
}

async fn gather_metrics(pool: &Pool<Postgres>) -> Result<Metrics> {
    let total_media: i64 = sqlx::query_scalar("SELECT count(*) FROM game_media")
        .fetch_one(pool)
        .await?;
    let lifted_any: i64 = sqlx
        ::query_scalar(
            "SELECT count(*) FROM game_media WHERE original_url IS NOT NULL OR thumbnail_url IS NOT NULL OR stream_url IS NOT NULL OR poster_url IS NOT NULL"
        )
        .fetch_one(pool).await?;
    let missing_original: i64 =
        sqlx::query_scalar("SELECT count(*) FROM game_media WHERE original_url IS NULL")
            .fetch_one(pool)
            .await?;
    let recent_lifted_30d: i64 = sqlx
        ::query_scalar(
            "SELECT count(*) FROM game_media WHERE created_at > now() - interval '30 days' AND (original_url IS NOT NULL OR thumbnail_url IS NOT NULL)"
        )
        .fetch_one(pool).await?;
    Ok(Metrics {
        total_media,
        lifted_any,
        missing_original,
        recent_lifted_30d,
        percent_with_any: pct(lifted_any, total_media),
        percent_missing_original: pct(missing_original, total_media),
        percent_recent_lifted: pct(recent_lifted_30d, total_media),
    })
}

fn derive_status(m: &Metrics, t: Thresholds) -> String {
    if m.percent_with_any < t.min_percent_with_any {
        return "degraded".into();
    }
    if let Some(max_missing) = t.max_percent_missing_original {
        if m.percent_missing_original > max_missing {
            return "degraded".into();
        }
    }
    "ok".into()
}

fn pct(part: i64, whole: i64) -> f64 {
    if whole == 0 {
        0.0
    } else {
        ((part as f64) * 100.0) / (whole as f64)
    }
}

fn init_tracing() {
    let _ = fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .try_init();
}
