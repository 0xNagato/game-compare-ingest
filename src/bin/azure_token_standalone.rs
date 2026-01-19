/*!
Standalone Azure Token Tester

Purpose:
- Request an app-only (client_credentials) token from Entra ID v2
- Print a safe summary (no full token by default)
- Decode JWT header/payload (no signature verification) to confirm audience/tenant
- Optionally probe a real URL with the Bearer token to produce an end-to-end "proof"

This binary is intentionally lightweight for debugging auth issues.
*/

use anyhow::{Context, Result};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use psstore_client::get_env;
use serde::Deserialize;
use serde_json::Value as JsonValue;

use reqwest::Client;
use std::time::Duration as StdDuration;

#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    expires_in: i64,
    token_type: String,
}

fn get_env_opt(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn try_decode_jwt_parts(token: &str) -> Result<Option<(JsonValue, JsonValue)>> {
    let mut parts = token.split('.');
    let Some(h) = parts.next() else {
        return Ok(None);
    };
    let Some(p) = parts.next() else {
        return Ok(None);
    };
    let Some(_s) = parts.next() else {
        return Ok(None);
    };
    // If there are more than 3 segments, it's not a standard JWT.
    if parts.next().is_some() {
        return Ok(None);
    }

    let hdr_bytes = URL_SAFE_NO_PAD
        .decode(h)
        .context("base64url decode JWT header")?;
    let payload_bytes = URL_SAFE_NO_PAD
        .decode(p)
        .context("base64url decode JWT payload")?;

    let hdr: JsonValue = serde_json::from_slice(&hdr_bytes).context("parse JWT header JSON")?;
    let claims: JsonValue =
        serde_json::from_slice(&payload_bytes).context("parse JWT payload JSON")?;
    Ok(Some((hdr, claims)))
}

fn print_jwt_identity_hint(hdr: &JsonValue, claims: &JsonValue) {
    let kid = hdr.get("kid").and_then(|v| v.as_str()).unwrap_or("<none>");
    let alg = hdr.get("alg").and_then(|v| v.as_str()).unwrap_or("<none>");

    let aud = claims
        .get("aud")
        .and_then(|v| v.as_str())
        .unwrap_or("<none>");
    let appid = claims
        .get("appid")
        .and_then(|v| v.as_str())
        .unwrap_or("<none>");
    let tid = claims
        .get("tid")
        .and_then(|v| v.as_str())
        .unwrap_or("<none>");
    let iss = claims
        .get("iss")
        .and_then(|v| v.as_str())
        .unwrap_or("<none>");

    println!("JWT header: alg={}, kid={}", alg, kid);
    println!("JWT claims: aud={}, appid={}, tid={}", aud, appid, tid);
    println!("JWT claims: iss={}", iss);

    if let Some(roles) = claims.get("roles") {
        println!("JWT roles: {}", roles);
    }
    if let Some(scp) = claims.get("scp") {
        println!("JWT scp: {}", scp);
    }
}

async fn probe_resource(access_token: &str, probe_url: &str) -> Result<()> {
    // Safe by default: status + basic headers.
    // Set AZURE_PROBE_PRINT_BODY=1 if you need to see response body.
    let client = Client::builder()
        .connect_timeout(StdDuration::from_secs(15))
        .timeout(StdDuration::from_secs(45))
        .build()
        .context("build reqwest client")?;

    let resp = client
        .get(probe_url)
        .bearer_auth(access_token)
        .send()
        .await?;
    let status = resp.status();
    let headers = resp.headers().clone();
    let ct = headers
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("<unknown>");
    let cl = headers
        .get(reqwest::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("<unknown>");

    println!(
        "Probe: GET {} -> {} (content-type={}, content-length={})",
        probe_url, status, ct, cl
    );

    if get_env_opt("AZURE_PROBE_PRINT_BODY").as_deref() == Some("1") {
        let body = resp.text().await.unwrap_or_default();
        let max = get_env_opt("AZURE_PROBE_BODY_MAX")
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(800);
        let snippet: String = body.chars().take(max).collect();
        println!("Probe body (first {} chars):\n{}", max, snippet);
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    tracing::info!("initialize tracing");

    println!("\n🔐 Azure/Entra ID OAuth Token Tester");
    println!("═══════════════════════════════════════════════════════════\n");

    let tenant_id = get_env("AZURE_TENANT_ID");
    let client_id = get_env("AZURE_CLIENT_ID");
    let client_secret = get_env("AZURE_CLIENT_SECRET");
    let scope = get_env_opt("AZURE_SCOPE")
        .unwrap_or_else(|| "https://xboxstore.microsoft.com/.default".to_string());

    // Optional override for authority path. Useful for testing:
    // - tenant id (recommended)
    // - organizations
    // - common (not recommended for app-only)
    let tenant_id = get_env_opt("AZURE_AUTHORITY_PATH").unwrap_or_else(|| tenant_id.clone());

    println!("📋 Configuration:");
    println!("  Authority: {}", tenant_id);
    println!("  Client ID: {}", client_id);
    println!("  Scope: {}\n", scope);

    println!("🔄 Requesting token...");

    let url = format!(
        "https://login.microsoftonline.com/{}/oauth2/v2.0/token",
        tenant_id
    );
    

    let params = [
        ("client_id", client_id.as_str()),
        ("client_secret", client_secret.as_str()),
        ("grant_type", "client_credentials"),
        ("scope", scope.as_str()),
    ];

    let client = reqwest::Client::new();
    let response = client
        .post(&url)
        .form(&params)
        .send()
        .await
        .context("Failed to send token request")?;

    if !response.status().is_success() {
        let status = response.status();
        let error_text = response.text().await.unwrap_or_default();
        anyhow::bail!("Token request failed: {} - {}", status, error_text);
    }

    let token_data = response
        .json::<TokenResponse>()
        .await
        .context("Failed to parse token response")?;

    println!("\n✅ SUCCESS!");
    println!("═══════════════════════════════════════════════════════════");
    println!("Token Type: {}", token_data.token_type);
    println!(
        "Expires In: {} seconds ({} minutes)",
        token_data.expires_in,
        token_data.expires_in / 60
    );

    let mode = get_env_opt("AZURE_TOKEN_OUTPUT_MODE").unwrap_or_else(|| "summary".into());
    match mode.as_str() {
        "raw" => {
            // WARNING: this prints the full token. Use only when intentionally exporting.
            println!("{}", token_data.access_token);
        }
        "export" => {
            println!("ACCESS_TOKEN={}", token_data.access_token);
        }
        _ => {
            println!("Token length: {} chars", token_data.access_token.len());

            if get_env_opt("AZURE_PRINT_TOKEN_PREVIEW").as_deref() == Some("1") {
                println!("\nToken preview (first 80 chars):");
                println!(
                    "{}...",
                    &token_data.access_token[..80.min(token_data.access_token.len())]
                );
            }

            if let Some((hdr, claims)) = try_decode_jwt_parts(&token_data.access_token)? {
                print_jwt_identity_hint(&hdr, &claims);
            } else {
                println!("Token is not a JWT (no '.' segments); cannot decode claims.");
            }

            if let Some(probe_url) = get_env_opt("AZURE_PROBE_URL") {
                probe_resource(&token_data.access_token, &probe_url).await?;
            }
        }
    }
    println!("═══════════════════════════════════════════════════════════");

    Ok(())
}
