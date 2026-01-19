# API Integration Guide
## i-miss-rust ↔ game-compare Setup

**Last Updated**: 2025-12-28

## Quick Start

### 1. Generate API Secret

Generate a secure random secret for API authentication:

```bash
# Generate a 32-byte random secret (base64 encoded)
openssl rand -base64 32
```

Copy this secret - you'll need it for both Rust and Laravel configurations.

### 2. Configure i-miss-rust (.env)

Add these environment variables to `/Users/lowkey/Desktop/i-miss-rust/.env`:

```bash
# API Server Configuration
API_HOST=0.0.0.0
API_PORT=8080
API_SECRET=<your-generated-secret-here>

# CORS - allowed origins (comma-separated)
ALLOWED_ORIGINS=http://localhost:8000,http://localhost:3000,https://game-compare.com

# Database URL (should already be configured)
DATABASE_URL=postgres://user:password@host:port/database
```

### 3. Configure game-compare (.env)

Add these environment variables to `/Users/lowkey/Documents/game-compare/.env`:

```bash
# Rust API Client Configuration
RUST_API_URL=http://localhost:8080
RUST_API_SECRET=<same-generated-secret-here>
RUST_API_TIMEOUT=30
```

### 4. Start the Rust API Server

```bash
cd /Users/lowkey/Desktop/i-miss-rust

# Build in release mode (faster)
cargo build --release --bin api_server

# Run the server
RUST_LOG=info cargo run --release --bin api_server

# Or use the compiled binary directly
./target/release/api_server
```

The server will start on `http://localhost:8080`

### 5. Test the API from Laravel

Create a test route or Artisan command:

```php
// routes/web.php or routes/api.php
Route::get('/test-rust-api', function () {
    $client = app(\App\Services\RustApiClient::class);

    // Test health check (no auth required)
    $health = $client->health();
    dd($health);
});
```

Visit `http://localhost:8000/test-rust-api` to test the connection.

## Environment Variables Reference

### i-miss-rust Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `API_HOST` | No | `0.0.0.0` | Host to bind the HTTP server |
| `API_PORT` | No | `8080` | Port for the HTTP server |
| `API_SECRET` | **Yes** | - | Bearer token for authentication |
| `ALLOWED_ORIGINS` | No | `http://localhost:3000,http://localhost:8000` | CORS allowed origins (comma-separated) |
| `DATABASE_URL` | **Yes** | - | PostgreSQL connection string |
| `RUST_LOG` | No | `info` | Logging level (`debug`, `info`, `warn`, `error`) |

### game-compare Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `RUST_API_URL` | **Yes** | `http://localhost:8080` | Base URL of the Rust API server |
| `RUST_API_SECRET` | **Yes** | - | Bearer token (must match Rust `API_SECRET`) |
| `RUST_API_TIMEOUT` | No | `30` | HTTP request timeout in seconds |

## API Endpoints Reference

### Health Check
```
GET /health
GET /

No authentication required.

Response:
{
  "success": true,
  "data": {
    "status": "healthy",
    "database": "connected",
    "uptime_seconds": 12345
  },
  "meta": {
    "timestamp": "2025-12-28T12:34:56Z",
    "request_id": "...",
    "version": "0.1.0"
  }
}
```

### Trigger Ingestion
```
POST /api/v1/ingest/trigger

Headers:
  Authorization: Bearer <API_SECRET>

Body:
{
  "providers": ["playstation_store", "steam_store"],
  "skip_alerts": false,
  "dry_run": false
}

Response:
{
  "success": true,
  "data": {
    "message": "Ingestion triggered",
    "providers": ["playstation_store", "steam_store"],
    "status": "queued"
  }
}
```

### Get Ingestion Status
```
GET /api/v1/ingest/status

Headers:
  Authorization: Bearer <API_SECRET>

Response:
{
  "success": true,
  "data": {
    "providers": [...],
    "total_active": 0,
    "total_completed": 2
  }
}
```

### Get Current Prices
```
GET /api/v1/prices/current?product_id=123&provider=playstation_store&jurisdiction=US

Headers:
  Authorization: Bearer <API_SECRET>

Response:
{
  "success": true,
  "data": [
    {
      "product_id": 123,
      "provider": "playstation_store",
      "jurisdiction": "US",
      "currency": "USD",
      "amount_minor": 5999,
      "amount": "59.99",
      "timestamp": "2025-12-28T12:34:56Z"
    }
  ]
}
```

### List Providers
```
GET /api/v1/providers

Headers:
  Authorization: Bearer <API_SECRET>

Response:
{
  "success": true,
  "data": [
    {
      "id": "playstation_store",
      "name": "PlayStation Store",
      "enabled": true,
      "status": "active"
    },
    ...
  ]
}
```

### Get Provider Stats
```
GET /api/v1/providers/{slug}/stats

Headers:
  Authorization: Bearer <API_SECRET>

Response:
{
  "success": true,
  "data": {
    "provider": "playstation_store",
    "total_products": 1234,
    "total_prices": 5678,
    "last_update": "2025-12-28T12:00:00Z",
    "status": "active"
  }
}
```

### Refresh Provider
```
POST /api/v1/providers/{slug}/refresh

Headers:
  Authorization: Bearer <API_SECRET>

Response:
{
  "success": true,
  "data": {
    "message": "Refresh queued for playstation_store",
    "status": "queued"
  }
}
```

## Usage Examples

### From Laravel Controllers

```php
<?php

namespace App\Http\Controllers\Admin;

use App\Services\RustApiClient;
use Illuminate\Http\Request;

class IngestionController extends Controller
{
    public function __construct(
        private RustApiClient $rustApi
    ) {}

    public function trigger(Request $request)
    {
        $providers = $request->input('providers', [
            'playstation_store',
            'steam_store'
        ]);

        try {
            $result = $this->rustApi->triggerIngestion($providers);

            return response()->json([
                'message' => 'Ingestion triggered successfully',
                'data' => $result
            ]);
        } catch (\Exception $e) {
            return response()->json([
                'error' => $e->getMessage()
            ], 500);
        }
    }

    public function status()
    {
        $status = $this->rustApi->getIngestionStatus();

        return view('admin.ingestion.status', compact('status'));
    }
}
```

### From Laravel Commands

```php
<?php

namespace App\Console\Commands;

use App\Services\RustApiClient;
use Illuminate\Console\Command;

class TriggerRustIngestion extends Command
{
    protected $signature = 'rust:ingest {--providers=* : Provider IDs to run}';
    protected $description = 'Trigger Rust API ingestion';

    public function handle(RustApiClient $rustApi)
    {
        $providers = $this->option('providers') ?: [
            'playstation_store',
            'steam_store'
        ];

        $this->info('Triggering ingestion for: ' . implode(', ', $providers));

        try {
            $result = $rustApi->triggerIngestion($providers);
            $this->info('Ingestion triggered: ' . $result['status']);
        } catch (\Exception $e) {
            $this->error('Failed: ' . $e->getMessage());
            return 1;
        }

        return 0;
    }
}
```

## Troubleshooting

### Connection Refused
```
Error: Connection refused to http://localhost:8080
```

**Solution**: Make sure the Rust API server is running:
```bash
cargo run --release --bin api_server
```

### Authentication Failed
```
Error: Invalid or missing authentication token
```

**Solution**: Check that `API_SECRET` (Rust) matches `RUST_API_SECRET` (Laravel).

### CORS Error
```
Access to fetch at 'http://localhost:8080' from origin 'http://localhost:3000' has been blocked by CORS
```

**Solution**: Add your origin to `ALLOWED_ORIGINS` in i-miss-rust `.env`:
```bash
ALLOWED_ORIGINS=http://localhost:3000,http://localhost:8000
```

### Database Connection Error
```
Error: Connection pool exhausted
```

**Solution**: Check `DATABASE_URL` is correct and database is accessible.

## Production Deployment

### Rust API Server

1. **Build optimized binary**:
   ```bash
   cargo build --release --bin api_server
   ```

2. **Create systemd service** (`/etc/systemd/system/rust-api.service`):
   ```ini
   [Unit]
   Description=i-miss-rust API Server
   After=network.target

   [Service]
   Type=simple
   User=www-data
   WorkingDirectory=/var/www/i-miss-rust
   Environment="RUST_LOG=info"
   EnvironmentFile=/var/www/i-miss-rust/.env
   ExecStart=/var/www/i-miss-rust/target/release/api_server
   Restart=always

   [Install]
   WantedBy=multi-user.target
   ```

3. **Start and enable**:
   ```bash
   sudo systemctl daemon-reload
   sudo systemctl start rust-api
   sudo systemctl enable rust-api
   ```

4. **Configure nginx reverse proxy**:
   ```nginx
   server {
       listen 80;
       server_name rust-api.yourdomain.com;

       location / {
           proxy_pass http://localhost:8080;
           proxy_set_header Host $host;
           proxy_set_header X-Real-IP $remote_addr;
           proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
           proxy_set_header X-Forwarded-Proto $scheme;
       }
   }
   ```

5. **Add SSL with certbot**:
   ```bash
   sudo certbot --nginx -d rust-api.yourdomain.com
   ```

### Laravel Configuration

Update production `.env`:
```bash
RUST_API_URL=https://rust-api.yourdomain.com
RUST_API_SECRET=<your-production-secret>
RUST_API_TIMEOUT=60
```

## Next Steps

1. ✅ API server implemented
2. ✅ Laravel HTTP client created
3. ⏳ Implement actual ingestion trigger logic
4. ⏳ Add real-time price query endpoints
5. ⏳ Set up monitoring and metrics
6. ⏳ Add rate limiting
7. ⏳ Implement caching layer
8. ⏳ Create Postman/Insomnia collection
9. ⏳ Write integration tests
10. ⏳ Deploy to production
