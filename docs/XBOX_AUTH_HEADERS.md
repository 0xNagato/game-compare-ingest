# Xbox Live Authentication & Headers Guide

## Overview

This guide explains how Xbox Live API authentication works in the i-miss-rust codebase and how environment variables are mapped to HTTP headers for Xbox Live service calls.

## Architecture

The Xbox authentication system consists of three main components:

1. **XAL (Xbox Authentication Library)** - Handles SISU flow for interactive authentication
2. **Azure OAuth** - Alternative app-only authentication using client credentials
3. **Headers Module** - Constructs proper Xbox Live API headers from tokens and environment config

## Authentication Flows

### 1. SISU Flow (Interactive)

```
┌──────────────┐
│ User Browser │
└───────┬──────┘
        │ 1. Opens OAuth URL
        ▼
┌──────────────────┐
│ Microsoft OAuth  │
│ (login.live.com) │
└───────┬──────────┘
        │ 2. Returns auth code
        ▼
┌──────────────────┐
│ XAL Crate        │
│ (xbl_auth.rs)    │
└───────┬──────────┘
        │ 3. Exchanges for XSTS token
        ▼
┌──────────────────┐
│ .xbox_tokens.json│
│ (cached tokens)  │
└──────────────────┘
```

**Setup:**

```bash
cargo run --bin xbox_auth_setup
```

This creates `.xbox_tokens.json` containing:

- User token
- XSTS token (for relying party `http://xboxlive.com`)
- Refresh token

**Environment Variables:**

- `XBOX_CLIENT_ID` - MSA application ID (required)

### 2. Azure OAuth Flow (App-Only)

```
┌──────────────────┐
│ Azure Token      │
│ Endpoint         │
└───────┬──────────┘
        │ Client credentials grant
        ▼
┌──────────────────┐
│ Access Token     │
│ (JWT)            │
└───────┬──────────┘
        │ scope: https://onestore.microsoft.com/.default
        ▼
┌──────────────────┐
│ Display Catalog  │
│ API              │
└──────────────────┘
```

**Environment Variables:**

- `AZURE_TENANT_ID` - Azure AD tenant
- `AZURE_CLIENT_ID` - Azure app registration ID
- `AZURE_CLIENT_SECRET` - App secret (alternative to cert)
- `AZURE_CLIENT_CERT_PATH` - Certificate path (alternative to secret)
- `AZURE_CLIENT_KEY_PATH` - Private key path (with cert)

**Note:** Azure OAuth provides a simpler non-interactive flow but requires an Azure app registration with access to the Display Catalog API.

## Environment Variables → HTTP Headers Mapping

### Core Environment Variables

| Variable                   | Purpose                     | Example Value                            | Used In                             |
| -------------------------- | --------------------------- | ---------------------------------------- | ----------------------------------- |
| `XBOX_CLIENT_ID`           | MSA application ID          | `104c7f45-b8b4-403f-955b-597b93e0b199`   | SISU authentication                 |
| `XBOX_SANDBOX_ID`          | Sandbox identifier          | `MCCQXF.58`                              | Optional X-XBL-Sandbox-ID header    |
| `XBOX_WEB_SERVICE_ID`      | Service configuration ID    | `fa95cb55-f503-4cd0-9d24-6ff90ec2e258`   | Optional X-XBL-WebService-ID header |
| `XBOX_PACKAGE_FAMILY_NAME` | App package family          | `Microsoft.GamingServices_8wekyb3d8bbwe` | Internal tracking                   |
| `XBOX_PACKAGE_SID`         | Package security identifier | Various                                  | Internal tracking                   |
| `XBOX_APP_URI`             | App URI for token audience  | `ms-xbl-...`                             | Token validation                    |

### HTTP Headers Generated

#### Required Headers (Always Sent)

1. **Authorization**

   - Format: `XSTSv3 t=<token>`
   - Source: XSTS token from `.xbox_tokens.json` (SISU) or Azure OAuth
   - Example: `Authorization: XSTSv3 t=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...`

2. **x-xbl-contract-version**

   - Format: Integer as string
   - Source: Hard-coded constant (API-specific)
   - Values:
     - Display Catalog API: `"2"`
     - EDS (Entertainment Discovery): `"3"`
     - Collections API: `"2"`
     - Profile API: `"2"`
   - Example: `x-xbl-contract-version: 2`

3. **MS-CV** (Correlation Vector)
   - Format: `<base>.<counter>`
   - Source: Auto-generated per provider instance
   - Purpose: Request tracking and debugging
   - Example: `MS-CV: 18446744073709551615.4294967295.0`

#### Optional Context Headers

These headers are sent when the corresponding environment variable is set:

1. **X-XBL-Sandbox-ID**

   - Source: `XBOX_SANDBOX_ID` env var
   - Example: `X-XBL-Sandbox-ID: MCCQXF.58`
   - Purpose: Sandbox isolation (dev/retail environments)

2. **X-XBL-WebService-ID**
   - Source: `XBOX_WEB_SERVICE_ID` env var
   - Example: `X-XBL-WebService-ID: fa95cb55-f503-4cd0-9d24-6ff90ec2e258`
   - Purpose: Service configuration routing

## Code Structure

### headers.rs

```rust
pub mod contract_versions {
    pub const DISPLAY_CATALOG: &str = "2";
    pub const EDS: &str = "3";
    // ... more versions
}

pub struct CorrelationVector {
    base: String,
    counter: u32,
}

pub struct XboxLiveConfig {
    pub client_id: Option<String>,
    pub sandbox_id: Option<String>,
    // ... more fields
}

pub fn build_xbox_live_headers(
    xsts_token: &str,
    contract_version: &str,
    cv: &str,
    config: &XboxLiveConfig,
) -> Result<HeaderMap>
```

### provider.rs

```rust
pub struct XboxProvider {
    client: Client,
    auth: Option<XboxAuth>,
    correlation_vector: Arc<Mutex<CorrelationVector>>,
    xbox_config: XboxLiveConfig,
}

impl XboxProvider {
    async fn build_request(&self, url: &str, opts: &XboxOptions) -> Result<RequestBuilder> {
        // 1. Get auth token (XSTS or Azure)
        let token = self.auth_token().await?;

        // 2. Generate next correlation vector
        let cv = self.correlation_vector.lock().await.next();

        // 3. Build headers with proper contract version
        let headers = build_xbox_live_headers(
            &token,
            contract_versions::DISPLAY_CATALOG,
            &cv,
            &self.xbox_config,
        )?;

        // 4. Create request with all headers
        Ok(self.client.get(url).headers(headers))
    }
}
```

## Example Request

```http
GET /v7.0/productFamilies/Games/products?market=US HTTP/1.1
Host: displaycatalog.mp.microsoft.com
Authorization: XSTSv3 t=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
x-xbl-contract-version: 2
MS-CV: 18446744073709551615.4294967295.0
X-XBL-Sandbox-ID: MCCQXF.58
```

## Token Lifecycle

### SISU Tokens (.xbox_tokens.json)

```json
{
  "user_token": {
    "token": "...",
    "issued": "2025-01-01T00:00:00Z",
    "expires": "2025-01-02T00:00:00Z"
  },
  "xsts_token": {
    "token": "...",
    "userhash": "...",
    "issued": "2025-01-01T00:00:00Z",
    "expires": "2025-01-01T12:00:00Z"
  }
}
```

**Automatic Refresh:**

- Tokens are refreshed when they expire
- Refresh token is used to get new XSTS token
- No user intervention needed after initial setup

### Azure Tokens (In-Memory Cache)

```rust
struct CachedToken {
    token: String,
    expires_at: Instant,
}
```

**Automatic Refresh:**

- Token cached until expiration
- New token requested via client credentials grant
- 5-minute safety margin before expiration

## Rate Limiting

Environment variables for API throttling:

```bash
XBOX_RPS=5                    # Requests per second
XBOX_REQS_PER_MIN=20         # Requests per minute (alternative)
XBOX_MAX_RETRIES=10          # Max retry attempts
XBOX_BACKOFF_MS=1000         # Initial backoff in milliseconds
```

Implemented in `provider.rs`:

```rust
fn pace_ms() -> Option<u64> {
    // Returns milliseconds to wait between requests
}

fn retry_policy() -> (u32, u64) {
    // Returns (max_retries, backoff_ms)
}
```

## Troubleshooting

### "Xbox authentication token not available"

**Cause:** No auth token configured

**Solutions:**

1. SISU flow: Run `cargo run --bin xbox_auth_setup`
2. Azure flow: Set `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, `AZURE_TENANT_ID`

### "Failed to build Xbox Live headers"

**Cause:** Invalid header values from environment

**Solutions:**

1. Check `XBOX_SANDBOX_ID` format (e.g., `RETAIL.0` or `MCCQXF.58`)
2. Verify `XBOX_WEB_SERVICE_ID` is a valid GUID

### 401 Unauthorized

**Causes:**

1. Expired XSTS token
2. Wrong relying party (should be `http://xboxlive.com`)
3. Wrong contract version

**Solutions:**

1. Re-run `xbox_auth_setup` to refresh tokens
2. Check `.xbox_tokens.json` for expiry dates
3. Verify contract version matches API endpoint

### 429 Too Many Requests

**Cause:** Rate limit exceeded

**Solutions:**

1. Reduce `XBOX_RPS` value
2. Increase `XBOX_BACKOFF_MS` value
3. Check for multiple concurrent ingestion processes

## References

- [Microsoft Xbox Live API](https://github.com/microsoft/xbox-live-api)
- [XAL Crate Documentation](https://docs.rs/xal/)
- [Display Catalog API](https://displaycatalog.mp.microsoft.com/)
- [Azure OAuth 2.0](https://learn.microsoft.com/en-us/azure/active-directory/develop/v2-oauth2-client-creds-grant-flow)

## Testing

Run tests for headers module:

```bash
cargo test --lib xbox::headers
```

Test authentication flow:

```bash
# Interactive SISU
cargo run --bin xbox_auth_setup

# Test API call with authentication
RUST_LOG=debug cargo run --bin gc ingest run --only xbox
```

Verify headers in logs:

```
DEBUG i_miss_rust::database_ops::xbox::headers: Built Xbox Live headers: Authorization=XSTSv3 t=eyJhbGciOi..., contract_version=2, cv=18446744073709551615.4294967295.0
```
