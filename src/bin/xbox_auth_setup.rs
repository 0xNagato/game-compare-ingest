/// Xbox Live Authentication Setup
///
/// Device code flow authentication for Xbox Live.
/// Run this once to perform headless authentication and store tokens.
///
/// Usage:
///   cargo run --bin xbox_auth_setup
///
/// Requirements:
///   - Browser access (can be on phone, tablet, or any device)
///
/// How it works:
///   1. This program displays a URL and code
///   2. You visit the URL on ANY device (phone, tablet, browser)
///   3. You sign in with your Microsoft/Xbox account
///   4. The program automatically completes once you authenticate
///   5. Tokens are stored to `XBOX_TOKEN_FILE` (if set) or `.xbox_tokens.json`
///
/// Notes:
///   - If you see "token store already exists", re-auth with `XBOX_AUTH_SETUP_FORCE=1`.
///   - Some Xbox Store / catalog endpoints may additionally require environment
///     variables like `XBOX_CLIENT_ID` (see `src/database_ops/xbox/headers.rs`).
///
/// After successful authentication, tokens can be used by the Xbox ingestion provider.

use anyhow::Result;
use dotenv::dotenv;
use i_miss_rust::database_ops::xbox::xbl_auth::{device_code_authentication, save_token_store, token_file_path};
use std::path::Path;
use tracing::{error, info};
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<()> {
    // Load environment variables early so `.env` can provide RUST_LOG and XBOX_TOKEN_FILE.
    dotenv().ok();

    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))
        )
        .init();

    let token_file = token_file_path();
    let token_path = Path::new(&token_file);
    let force = std::env::var("XBOX_AUTH_SETUP_FORCE")
        .ok()
        .is_some_and(|v| v == "1" || v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("yes"));

    if token_path.exists() && !force {
        info!(
            token_file = %token_file,
            "xbox_auth_setup: token store already exists; skipping interactive device-code auth"
        );
        info!(
            "xbox_auth_setup: to re-authenticate, set XBOX_AUTH_SETUP_FORCE=1 and re-run"
        );
        info!(
            "xbox_auth_setup: to validate current tokens, run: cargo run --bin xbox_xsts_smoke_test"
        );
        return Ok(());
    }

    info!("Xbox Live Authentication Setup - Device Code Flow");
    info!("==================================================");
    info!("");
    info!("This tool uses OAuth 2.0 Device Authorization Grant (RFC 8628)");
    info!("for headless authentication. You can complete authentication");
    info!("from ANY device - phone, tablet, or browser.");
    info!("");

    // Perform device code authentication
    match device_code_authentication().await {
        Ok(token_store) => {
            info!("");
            info!("✓ Authentication successful!");
            info!("");

            // Save tokens
            save_token_store(&token_store).await?;

            info!(token_file = %token_file, "✓ Tokens saved");
            info!("");
            info!("You can now run Xbox ingestion:");
            info!("  cargo run --bin ingest run --only microsoft_store");
            info!("");

            Ok(())
        }
        Err(e) => {
            error!("");
            error!("✗ Authentication failed: {}", e);
            error!("");
            error!("Common issues:");
            error!("  - You did not complete the browser/device step");
            error!("  - Network connectivity issues");
            error!("  - Device code expired (codes expire after ~15 minutes)");
            error!("");
            error!("To retry: Run this command again to get a new device code");
            error!("");

            Err(e)
        }
    }
}
