// Additional middleware for logging, CORS, etc.

use actix_web::middleware::{Compress, Logger};

pub fn setup_middleware() -> (Logger, Compress) {
    let logger = Logger::default();
    let compress = Compress::default();
    (logger, compress)
}

// CORS configuration
use actix_cors::Cors;
use actix_web::http::header;

pub fn setup_cors(allowed_origins: &str) -> Cors {
    let origins: Vec<&str> = allowed_origins.split(',').collect();

    let mut cors = Cors::default()
        .allowed_methods(vec!["GET", "POST", "PUT", "DELETE"])
        .allowed_headers(vec![
            header::AUTHORIZATION,
            header::ACCEPT,
            header::CONTENT_TYPE,
        ])
        .max_age(3600);

    for origin in origins {
        cors = cors.allowed_origin(origin.trim());
    }

    cors
}
