# Inter-Service API Architecture
## i-miss-rust ↔ game-compare Integration Design

**Created**: 2025-12-28
**Status**: Planning Phase

## Overview

This document outlines the API architecture for integrating the high-performance Rust ingestion backend (`i-miss-rust`) with the Laravel web application (`game-compare`).

## System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                                                               │
│  game-compare (Laravel/PHP) - User-Facing Web Application    │
│  - UI/UX Layer                                                │
│  - Business Logic                                             │
│  - API Endpoints for Frontend                                 │
│  - Admin Panel (Filament)                                     │
│                                                               │
└───────────────────────┬───────────────────────────────────────┘
                        │
                        │ HTTP/JSON APIs
                        │ (Authentication via Bearer Token)
                        │
                        ↓
┌─────────────────────────────────────────────────────────────┐
│                                                               │
│  i-miss-rust (Rust/Actix-Web) - Data Ingestion Backend       │
│  - High-Performance Data Ingestion                            │
│  - Provider Pipeline Management                               │
│  - Real-Time Price APIs                                       │
│  - Bulk Data Operations                                       │
│  - Webhook Event Processing                                   │
│                                                               │
└───────────────────────┬───────────────────────────────────────┘
                        │
                        │ Direct Connection
                        │
                        ↓
┌─────────────────────────────────────────────────────────────┐
│                                                               │
│  PostgreSQL (Supabase) - Shared Database                      │
│  - Time-series partitioned pricing data                       │
│  - Game catalog & metadata                                    │
│  - Provider configurations                                    │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

## Communication Patterns

### Pattern 1: Shared Database (Current)
Both services access the same PostgreSQL database directly.

**Pros**:
- No API latency for data access
- Transactional consistency
- Simple deployment

**Cons**:
- Tight coupling
- Schema coordination required
- No service isolation

### Pattern 2: API-First (Recommended for New Features)
Services communicate via RESTful APIs with database as implementation detail.

**Pros**:
- Service isolation
- Independent scaling
- Clear contracts
- Easier testing

**Cons**:
- Network latency
- More complex deployment

### Hybrid Approach (Recommended)
- **Shared Database**: For read-heavy operations (game listings, price queries)
- **APIs**: For write operations, real-time updates, and cross-service coordination

## API Endpoints Design

### Rust Service APIs (i-miss-rust)

#### 1. Ingestion Control APIs
```
POST   /api/v1/ingest/trigger
       Trigger provider ingestion manually
       Body: { "providers": ["playstation_store", "steam_store"], "options": {...} }

GET    /api/v1/ingest/status
       Get current ingestion status for all providers

GET    /api/v1/ingest/history
       Get ingestion run history with stats
```

#### 2. Real-Time Price APIs
```
GET    /api/v1/prices/current
       Query: product_id, provider, jurisdiction
       Get current prices for a product

GET    /api/v1/prices/compare
       Query: product_id, jurisdictions[]
       Compare prices across jurisdictions

GET    /api/v1/prices/history
       Query: product_id, provider, from_date, to_date
       Get historical price data (time-series)
```

#### 3. Provider Management APIs
```
GET    /api/v1/providers
       List all configured providers with status

GET    /api/v1/providers/{slug}/stats
       Get provider-specific statistics

POST   /api/v1/providers/{slug}/refresh
       Force refresh from a specific provider
```

#### 4. Webhook Event APIs
```
POST   /api/v1/webhooks/igdb
       Receive IGDB webhook events

POST   /api/v1/webhooks/price-alerts
       Process price alert triggers
```

#### 5. Data Export APIs
```
GET    /api/v1/export/prices
       Query: format (json|csv|parquet), filters
       Bulk export price data

GET    /api/v1/export/catalog
       Export game catalog data
```

### Laravel Service APIs (game-compare)

#### 1. Trigger Ingestion from Admin
```
POST   /api/admin/trigger-ingestion
       Admin endpoint to trigger Rust ingestion
       Calls: i-miss-rust /api/v1/ingest/trigger
```

#### 2. Real-Time Price Updates
```
POST   /api/webhooks/price-update
       Receive price updates from Rust service
       Update cache, trigger alerts
```

#### 3. Analytics & Reporting
```
GET    /api/analytics/price-trends
       Get price trend analytics (may call Rust for raw data)
```

## Authentication Strategy

### Option 1: Shared Secret (Simple)
- Environment variable `RUST_API_SECRET` / `LARAVEL_API_SECRET`
- Passed as `Authorization: Bearer <secret>` header
- Good for internal service-to-service communication

### Option 2: JWT Tokens (Recommended)
- Laravel generates JWT tokens for Rust API calls
- Rust validates JWT using shared secret/public key
- Supports expiration, scopes, and auditability

### Option 3: mTLS (Enterprise)
- Mutual TLS certificates
- Strongest security
- More complex setup

**Recommended**: Start with Option 1 (Shared Secret), migrate to Option 2 (JWT) in production.

## Implementation Phases

### Phase 1: Rust HTTP Server Setup ✅ (Actix-web already in dependencies)
1. Create API module in i-miss-rust: `src/api/`
2. Implement basic actix-web server with routes
3. Add authentication middleware
4. Health check endpoint

### Phase 2: Core Ingestion APIs
1. POST /api/v1/ingest/trigger
2. GET /api/v1/ingest/status
3. Add request/response DTOs with serde

### Phase 3: Laravel HTTP Client
1. Create `app/Services/RustApiClient.php`
2. Implement HTTP client using Guzzle/Laravel HTTP
3. Add retry logic and error handling
4. Add configuration in `config/services.php`

### Phase 4: Real-Time Price APIs
1. Implement Rust price query endpoints
2. Add caching layer (Redis)
3. Optimize database queries with prepared statements

### Phase 5: Webhooks & Events
1. IGDB webhook receiver in Rust
2. Price alert webhook from Rust to Laravel
3. Event-driven architecture with message queue (optional)

### Phase 6: Monitoring & Observability
1. Add tracing/logging to all API calls
2. Prometheus metrics endpoint
3. API rate limiting
4. Request/response logging

## Data Models & DTOs

### Rust API Response Format (Standard)
```rust
#[derive(Serialize, Deserialize)]
pub struct ApiResponse<T> {
    pub success: bool,
    pub data: Option<T>,
    pub error: Option<String>,
    pub meta: Option<Meta>,
}

#[derive(Serialize, Deserialize)]
pub struct Meta {
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub request_id: String,
    pub version: String,
}
```

### Example: Price Query Response
```json
{
  "success": true,
  "data": {
    "product_id": 123,
    "prices": [
      {
        "provider": "playstation_store",
        "jurisdiction": "US",
        "currency": "USD",
        "amount_minor": 5999,
        "amount": "59.99",
        "timestamp": "2025-12-28T00:00:00Z"
      }
    ]
  },
  "meta": {
    "timestamp": "2025-12-28T12:34:56Z",
    "request_id": "req_abc123",
    "version": "1.0.0"
  }
}
```

## Configuration

### Rust (.env)
```bash
# API Server
API_HOST=0.0.0.0
API_PORT=8080
API_SECRET=<secure-random-secret>

# CORS
ALLOWED_ORIGINS=https://game-compare.com,http://localhost:3000

# Database (already configured)
DATABASE_URL=postgres://...
```

### Laravel (.env)
```bash
# Rust API Client
RUST_API_URL=http://localhost:8080
RUST_API_SECRET=<same-secure-random-secret>
RUST_API_TIMEOUT=30
```

## Error Handling

### HTTP Status Codes
- **200 OK**: Successful request
- **201 Created**: Resource created
- **400 Bad Request**: Invalid parameters
- **401 Unauthorized**: Missing/invalid authentication
- **403 Forbidden**: Valid auth but insufficient permissions
- **404 Not Found**: Resource not found
- **429 Too Many Requests**: Rate limit exceeded
- **500 Internal Server Error**: Server error
- **503 Service Unavailable**: Service temporarily down

### Error Response Format
```json
{
  "success": false,
  "error": "Invalid provider specified",
  "meta": {
    "timestamp": "2025-12-28T12:34:56Z",
    "request_id": "req_xyz789",
    "version": "1.0.0"
  }
}
```

## Performance Considerations

1. **Database Connection Pooling**: Use SQLx connection pool (already configured)
2. **Request Batching**: Support bulk operations where possible
3. **Caching**: Redis cache for frequently accessed data
4. **Pagination**: Implement cursor-based pagination for large result sets
5. **Rate Limiting**: Prevent abuse with rate limits per API key
6. **Async Operations**: Use Tokio for non-blocking I/O

## Security Checklist

- [ ] API authentication implemented
- [ ] HTTPS/TLS in production
- [ ] Input validation on all endpoints
- [ ] SQL injection prevention (parameterized queries)
- [ ] CORS configuration for allowed origins
- [ ] Rate limiting per client
- [ ] Request size limits
- [ ] Audit logging for sensitive operations
- [ ] Secret rotation mechanism
- [ ] API versioning strategy

## Deployment Architecture

### Development
- Rust: `cargo run` on localhost:8080
- Laravel: `php artisan serve` on localhost:8000
- PostgreSQL: Local or Supabase dev instance

### Production
- Rust: Docker container behind nginx reverse proxy
- Laravel: PHP-FPM + nginx
- PostgreSQL: Supabase production cluster
- Load balancer for Rust API (multiple instances)

## Next Steps

1. Review and approve this architecture design
2. Create API module structure in i-miss-rust
3. Implement Phase 1 (HTTP server setup)
4. Write integration tests
5. Document all endpoints with OpenAPI/Swagger spec
6. Set up CI/CD for Rust API deployment

## Questions to Resolve

1. Should we use the same database or separate databases?
   - **Recommendation**: Same database initially, migrate to separate if needed
2. What's the expected API request volume?
   - Determines caching and rate limiting strategy
3. Real-time updates via WebSockets or polling?
   - WebSockets for live price updates if needed
4. Where should business logic live (Rust vs Laravel)?
   - **Recommendation**: Heavy computation in Rust, business rules in Laravel
