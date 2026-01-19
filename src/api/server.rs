// API server implementation using actix-web

use crate::api::{auth, middleware, routes};
use crate::database_ops::db::Db;
use actix_web::{web, App, HttpServer};
use anyhow::{Context, Result};
use std::env;

pub struct ApiServer {
    pub host: String,
    pub port: u16,
    pub api_secret: String,
    pub allowed_origins: String,
}

impl ApiServer {
    /// Create server from environment variables
    pub fn from_env() -> Result<Self> {
        crate::util::env::init_env();

        let host = env::var("API_HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
        let port = env::var("API_PORT")
            .unwrap_or_else(|_| "8080".to_string())
            .parse()
            .context("Invalid API_PORT")?;

        let api_secret =
            env::var("API_SECRET").context("API_SECRET environment variable is required")?;

        let allowed_origins = env::var("ALLOWED_ORIGINS")
            .unwrap_or_else(|_| "http://localhost:3000,http://localhost:8000".to_string());

        Ok(Self {
            host,
            port,
            api_secret,
            allowed_origins,
        })
    }

    /// Start the HTTP server
    pub async fn run(self, db: Db) -> Result<()> {
        let bind_addr = format!("{}:{}", self.host, self.port);

        tracing::info!(
            host = %self.host,
            port = %self.port,
            "Starting i-miss-rust API server"
        );

        let db_data = web::Data::new(db);
        let api_secret = self.api_secret.clone();
        let allowed_origins = self.allowed_origins.clone();

        HttpServer::new(move || {
            let (logger, compress) = middleware::setup_middleware();
            let cors = middleware::setup_cors(&allowed_origins);
            let auth = auth::Auth::new(api_secret.clone());

            App::new()
                .app_data(db_data.clone())
                .wrap(logger)
                .wrap(compress)
                .wrap(cors)
                .wrap(auth)
                .configure(routes::configure_routes)
        })
        .bind(&bind_addr)
        .with_context(|| format!("Failed to bind to {}", bind_addr))?
        .run()
        .await
        .context("HTTP server error")?;

        Ok(())
    }
}
