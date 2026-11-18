/// Xbox Live XSTS smoke test
///
/// This binary validates that:
/// - an XSTS Authorization header can be obtained from the saved TokenStore
/// - the XSTS token is accepted by a basic Xbox Live endpoint
///
/// Usage:
///   cargo run --bin xbox_xsts_smoke_test
///
/// Prerequisite:
///   cargo run --bin xbox_auth_setup

use anyhow::{Context, Result};
use dotenv::dotenv;
use i_miss_rust::database_ops::xbox::xbl_auth::get_xsts_token;
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, AUTHORIZATION};
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    dotenv().ok();

    info!("xbox_xsts_smoke_test: obtaining XSTS authorization header");
    let authorization_value = get_xsts_token().await?;

    // IMPORTANT: never log the auth header value; it contains sensitive tokens.
    info!("xbox_xsts_smoke_test: got XSTS header (redacted)");

    let mut headers = HeaderMap::new();
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&authorization_value)
            .context("xbox_xsts_smoke_test: invalid XSTS authorization header value")?,
    );
    headers.insert("x-xbl-contract-version", HeaderValue::from_static("2"));
    headers.insert(ACCEPT, HeaderValue::from_static("application/json"));

    // A simple endpoint that should succeed with a valid XSTS token.
    let url = "https://profile.xboxlive.com/users/me/profile/settings?settings=Gamertag";

    let client = reqwest::Client::builder()
        .user_agent("i-miss-rust/xbox_xsts_smoke_test")
        .build()
        .context("xbox_xsts_smoke_test: failed to build HTTP client")?;

    info!(%url, "xbox_xsts_smoke_test: calling Xbox Live profile endpoint");
    let resp = client
        .get(url)
        .headers(headers)
        .send()
        .await
        .context("xbox_xsts_smoke_test: request failed")?;

    let status = resp.status();
    let body = resp
        .text()
        .await
        .context("xbox_xsts_smoke_test: failed to read response body")?;

    if status.is_success() {
        // Try to parse out the gamertag (best-effort). Even if parsing fails,
        // a 200 here is still strong evidence that auth works.
        let maybe_gamertag = serde_json::from_str::<serde_json::Value>(&body)
            .ok()
            .and_then(|v| {
                v.get("profileUsers")
                    .and_then(|p| p.as_array())
                    .and_then(|arr| arr.first())
                    .and_then(|first| first.get("settings"))
                    .and_then(|s| s.as_array())
                    .and_then(|arr| {
                        arr.iter().find_map(|entry| {
                            let id = entry.get("id")?.as_str()?;
                            if id != "Gamertag" {
                                return None;
                            }
                            entry.get("value")?.as_str().map(|s| s.to_string())
                        })
                    })
            });

        if let Some(gt) = maybe_gamertag {
            info!(gamertag = %gt, "xbox_xsts_smoke_test: success");
        } else {
            info!(
                body_len = body.len(),
                "xbox_xsts_smoke_test: success (could not parse gamertag from JSON; body redacted)"
            );
        }

        return Ok(());
    }

    // Non-2xx: print a short excerpt for debugging (avoid dumping huge bodies).
    let excerpt: String = body.chars().take(800).collect();
    warn!(
        status = %status,
        body_excerpt = %excerpt,
        "xbox_xsts_smoke_test: Xbox Live endpoint returned non-success"
    );

    anyhow::bail!("xbox_xsts_smoke_test: request failed with status {}", status);
}
