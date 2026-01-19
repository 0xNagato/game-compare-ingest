// Authentication middleware for API endpoints

use actix_web::{
    body::{BoxBody, EitherBody},
    dev::{forward_ready, Service, ServiceRequest, ServiceResponse, Transform},
    Error, HttpResponse,
};
use futures::future::LocalBoxFuture;
use std::future::{ready, Ready};

/// Authentication middleware that validates Bearer tokens
pub struct Auth {
    secret: String,
}

impl Auth {
    pub fn new(secret: String) -> Self {
        Self { secret }
    }
}

impl<S, B> Transform<S, ServiceRequest> for Auth
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<EitherBody<B, BoxBody>>;
    type Error = Error;
    type InitError = ();
    type Transform = AuthMiddleware<S>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(AuthMiddleware {
            service,
            secret: self.secret.clone(),
        }))
    }
}

pub struct AuthMiddleware<S> {
    service: S,
    secret: String,
}

impl<S, B> Service<ServiceRequest> for AuthMiddleware<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<EitherBody<B, BoxBody>>;
    type Error = Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    forward_ready!(service);

    fn call(&self, req: ServiceRequest) -> Self::Future {
        let secret = self.secret.clone();

        // Skip auth for health check
        if req.path() == "/health" || req.path() == "/" {
            let fut = self.service.call(req);
            return Box::pin(async move {
                let res = fut.await?;
                Ok(res.map_into_left_body())
            });
        }

        // Extract Authorization header
        let auth_header = req
            .headers()
            .get("Authorization")
            .and_then(|h| h.to_str().ok())
            .and_then(|h| h.strip_prefix("Bearer "));

        if let Some(token) = auth_header {
            if token == secret {
                // Valid token - proceed with request
                let fut = self.service.call(req);
                return Box::pin(async move {
                    let res = fut.await?;
                    Ok(res.map_into_left_body())
                });
            }
        }

        // Invalid or missing token
        Box::pin(async move {
            let response = HttpResponse::Unauthorized()
                .json(serde_json::json!({
                    "success": false,
                    "error": "Invalid or missing authentication token"
                }))
                .map_into_right_body();
            Ok(req.into_response(response))
        })
    }
}

/// Extract the authenticated request context
pub struct AuthContext {
    pub request_id: String,
}

impl AuthContext {
    pub fn from_request(_req: &ServiceRequest) -> Self {
        Self {
            request_id: uuid::Uuid::new_v4().to_string(),
        }
    }
}
