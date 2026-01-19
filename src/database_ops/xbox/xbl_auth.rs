/// Xbox Live Authentication using xal crate
///
/// This module handles Xbox Live authentication for server-side ingestion.
///
/// Flow:
/// 1. Initial setup: Run authentication binary to perform device code auth
///    - User visits URL on any device (phone, browser, etc.)
///    - User enters displayed code
///    - Program polls for completion (no manual URL pasting needed)
/// 2. Tokens stored to .xbox_tokens.json
/// 3. Subsequent runs: Auto-refresh tokens from storage
/// 4. Use XSTS token for Xbox API calls

use anyhow::{Context, Result};
use std::path::Path;
use tracing::{info, warn, debug, error};
use xal::{AccessTokenPrefix, Flows, TokenStore, XalAuthenticator};

/// Token storage file path (default).
///
/// Defaults to `.xbox_tokens.json` in the current working directory.
/// Override this at runtime with `XBOX_TOKEN_FILE=/absolute/or/relative/path/to/file.json`.
const TOKEN_FILE_DEFAULT: &str = ".xbox_tokens.json";

/// Resolve the token store path.
///
/// Defaults to [`TOKEN_FILE_DEFAULT`], but can be overridden at runtime with
/// `XBOX_TOKEN_FILE=/absolute/or/relative/path/to/file.json`.
pub fn token_file_path() -> String {
    std::env::var("XBOX_TOKEN_FILE").unwrap_or_else(|_| TOKEN_FILE_DEFAULT.to_string())
}

/// XSTS relying party for Xbox Live API access.
const XSTS_RELYING_PARTY_XBOXLIVE: &str = "http://xboxlive.com";

async fn authorize_xbox_live_traditional(
    authenticator: &mut XalAuthenticator,
    live_tokens: xal::response::WindowsLiveTokens,
) -> Result<TokenStore> {
    // Different clients/accounts sometimes require different access token prefixes
    // for the XASU request. We'll try the common variants.
    let try_prefixes = [AccessTokenPrefix::D, AccessTokenPrefix::T];

    let mut last_err: Option<anyhow::Error> = None;
    for (idx, prefix) in try_prefixes.iter().cloned().enumerate() {
        debug!(
            "xbl_auth: attempting traditional Xbox Live authorization with prefix={:?}",
            prefix
        );

        match Flows::xbox_live_authorization_traditional_flow(
            authenticator,
            live_tokens.clone(),
            XSTS_RELYING_PARTY_XBOXLIVE.to_string(),
            prefix.clone(),
            false,
        )
        .await
        {
            Ok(store) => return Ok(store),
            Err(e) => {
                // This is expected in some environments: the same account can succeed
                // with one prefix but fail with another. Only warn if all prefixes fail.
                if idx + 1 < try_prefixes.len() {
                    info!(
                        "xbl_auth: traditional authorization failed with prefix={:?}; trying next prefix",
                        prefix
                    );
                    debug!("xbl_auth: prefix={:?} error: {}", prefix, e);
                } else {
                    warn!(
                        "xbl_auth: traditional authorization failed with prefix={:?} (last prefix)",
                        prefix
                    );
                    debug!("xbl_auth: last prefix={:?} error: {}", prefix, e);
                }

                last_err = Some(anyhow::Error::new(e).context(format!(
                    "xbl_auth: traditional authorization failed with prefix={:?}",
                    prefix
                )));
            }
        }
    }

    Err(last_err.unwrap_or_else(|| {
        anyhow::anyhow!("xbl_auth: traditional authorization failed for all prefixes")
    }))
}

/// Get a valid XSTS token for Xbox API calls
///
/// This function:
/// 1. Checks if tokens exist and are valid
/// 2. Refreshes if needed
/// 3. Returns authorization header value for API authentication
pub async fn get_xsts_token() -> Result<String> {
    let token_file = token_file_path();
    let token_path = Path::new(&token_file);

    // Try to refresh tokens from file
    if token_path.exists() {
        debug!("xbl_auth: attempting to refresh tokens from file");

        match Flows::try_refresh_live_tokens_from_file(&token_file).await {
            Ok((mut authenticator, ts)) => {
                info!("xbl_auth: tokens refreshed successfully, obtaining XSTS");

                // Perform traditional Xbox Live authorization flow to get XSTS token.
                // This avoids SISU, whose response shape appears to have changed.
                let mut token_store =
                    authorize_xbox_live_traditional(&mut authenticator, ts.live_token).await?;

                token_store.update_timestamp();
                token_store.save_to_file(&token_file)?;

                let xsts_token = token_store
                    .authorization_token
                    .as_ref()
                    .context("xbl_auth: traditional flow did not return an XSTS token")?;

                info!("xbl_auth: XSTS token obtained and saved");
                return Ok(xsts_token.authorization_header_value());
            }
            Err(e) => {
                warn!("xbl_auth: token refresh failed: {}", e);
                // Fall through to request new authentication
            }
        }
    }

    // No valid tokens, need device code authentication
    anyhow::bail!(
        "No valid Xbox Live tokens found. Please run the authentication setup:\n\
         cargo run --bin xbox_auth_setup\n\n\
         This will use device code authentication (no browser pasting required).\n\
         Tokens will be stored to {}",
        token_file
    );
}

/// Perform device code authentication for headless environments
///
/// This implements RFC 8628 Device Code Flow:
/// 1. Display a short code to the user (e.g., "HKLP8KX3")
/// 2. User visits URL on any device (phone, browser, etc.)
/// 3. User enters the code to authenticate
/// 4. Program polls for completion (no manual URL pasting needed)
///
/// After successful Windows Live authentication, performs the traditional Xbox Live
/// authorization flow to obtain all Xbox tokens (device, user, optional title, XSTS)
/// required for API calls.
pub async fn device_code_authentication() -> Result<TokenStore> {
    info!("xbl_auth: starting Xbox Live authentication with device code flow");

    let mut authenticator = XalAuthenticator::default();

    // Step 1: Initiate device code authentication
    info!("xbl_auth: initiating device code authentication");
    let device_code_response = authenticator
        .initiate_device_code_auth()
        .await
        .context("Failed to initiate device code authentication")?;

    // Step 2: Display the code to the user
    let user_code = device_code_response.user_code().secret();
    let verification_uri_str = device_code_response.verification_uri().url().as_str();
    let verification_uri_complete_str = device_code_response.verification_uri_complete()
        .map(|u| u.secret());

    println!("\n╔══════════════════════════════════════════════════════════════╗");
    println!("║                                                              ║");
    println!("║           Xbox Live Device Code Authentication               ║");
    println!("║                                                              ║");
    println!("╠══════════════════════════════════════════════════════════════╣");
    println!("║                                                              ║");
    println!("║  Please visit ONE of the following URLs on ANY device:      ║");
    println!("║                                                              ║");
    if let Some(complete_uri) = verification_uri_complete_str {
        println!("║  Quick Link (auto-fills code):                              ║");
        println!("║  {:<60} ║", complete_uri);
        println!("║                                                              ║");
        println!("║  OR                                                          ║");
        println!("║                                                              ║");
    }
    println!("║  Manual Entry:                                               ║");
    println!("║  {:<60} ║", verification_uri_str);
    println!("║                                                              ║");
    println!("║  And enter this code:                                        ║");
    println!("║                                                              ║");
    println!("║              CODE: {:<6}                                   ║", user_code);
    println!("║                                                              ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    info!(
        "xbl_auth: device code authentication initiated - user_code: {}, verification_uri: {}",
        user_code, verification_uri_str
    );
    info!("xbl_auth: polling for device code authorization...");

    // Step 3: Poll for completion
    let live_token = authenticator
        .poll_device_code_auth(&device_code_response, tokio::time::sleep)
        .await
        .context("Device code authentication polling failed")?;

    info!("xbl_auth: device code authentication successful, Windows Live token obtained");

    // Step 4: Perform Xbox Live authorization flow (non-SISU)
    info!("xbl_auth: starting Xbox Live traditional authorization");
    let mut token_store = match authorize_xbox_live_traditional(&mut authenticator, live_token).await {
        Ok(store) => store,
        Err(e) => {
            // Log the full error chain with all details
            error!("xbl_auth: Xbox Live authorization failed with error: {:?}", e);
            error!("xbl_auth: Error display: {}", e);
            if let Some(source) = e.source() {
                error!("xbl_auth: Error source: {}", source);
            }
            return Err(e).context("Xbox Live authorization failed");
        }
    };

    token_store.update_timestamp();

    info!("xbl_auth: all tokens obtained successfully");

    Ok(token_store)
}

/// Save token store after initial authentication
pub async fn save_token_store(store: &TokenStore) -> Result<()> {
    let token_file = token_file_path();
    store.save_to_file(&token_file)?;
    info!("xbl_auth: tokens saved to {}", token_file);
    Ok(())
}
