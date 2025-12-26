use anyhow::{Context, Result, anyhow};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::{Duration, Utc};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use pem::parse as parse_pem;
// rand currently unused (removed previous helper); keep if future entropy needed
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use std::{env, fs};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    iss: String,
    sub: String,
    aud: String,
    jti: String,
    nbf: i64,
    iat: i64,
    exp: i64,
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    expires_in: i64,
    token_type: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    i_miss_rust::util::env::bootstrap_cli("azure_token");
    dotenv::dotenv().ok();

    let tenant_id = env::var("AZURE_TENANT_ID").context("AZURE_TENANT_ID not set")?;
    let client_id = env::var("AZURE_CLIENT_ID").context("AZURE_CLIENT_ID not set")?;
    let scope = env::var("AZURE_SCOPE")
        .unwrap_or_else(|_| "https://displaycatalog.mp.microsoft.com/.default".to_string());
    // Authority override: consumers | organizations | common | <tenant_id>
    let authority = env::var("AZURE_AUTHORITY_PATH").unwrap_or_else(|_| tenant_id.clone());

    // Prefer certificate-based auth if key+cert envs exist; else fall back to client secret.
    let cert_path =
        env::var("AZURE_CLIENT_CERT_PATH").unwrap_or_else(|_| "./example.crt".to_string());
    let key_path =
        env::var("AZURE_CLIENT_KEY_PATH").unwrap_or_else(|_| "./example.key".to_string());
    let mode = env::var("AZURE_AUTH_MODE").unwrap_or_else(|_| "auto".to_string());

    let files_present = fs::metadata(&cert_path).is_ok() && fs::metadata(&key_path).is_ok();
    let use_cert = match mode.as_str() {
        "certificate" => true,
        "secret" => false,
        _ => files_present,
    };
    if use_cert {
        println!(
            "Using certificate-based client assertion (authority: {}, cert: {}, key: {})",
            authority, cert_path, key_path
        );
        let assertion = build_client_assertion(&authority, &client_id, &cert_path, &key_path)?;
        let token = fetch_token_with_assertion(&authority, &client_id, &scope, &assertion).await?;
        print_token_summary(&token)?;
    } else {
        println!("Using client secret mode (authority: {})", authority);
        let client_secret =
            env::var("AZURE_CLIENT_SECRET").context("AZURE_CLIENT_SECRET not set")?;
        if client_secret.contains("BEGIN CERTIFICATE") || client_secret.contains("MIID") {
            return Err(anyhow!(
                "AZURE_CLIENT_SECRET looks like a certificate, not a secret. Create a portal client secret and paste its plaintext value."
            ));
        }
        let token = fetch_token_with_secret(&authority, &client_id, &client_secret, &scope).await?;
        print_token_summary(&token)?;
    }

    Ok(())
}

fn build_client_assertion(
    authority: &str,
    client_id: &str,
    cert_path: &str,
    key_path: &str,
) -> Result<String> {
    // Read and parse certificate for x5t = base64url(SHA1(der))
    let cert_pem =
        fs::read_to_string(cert_path).with_context(|| format!("reading cert {cert_path}"))?;
    let pem = parse_pem(cert_pem.as_bytes()).context("parse PEM cert")?;
    let der = pem.contents();
    let thumb_sha1 = Sha1::digest(&der);
    let x5t = URL_SAFE_NO_PAD.encode(thumb_sha1);

    let mut header = Header::new(Algorithm::RS256);
    header.x5t = Some(x5t);

    // Claims per AAD client assertion
    let now = Utc::now();
    let claims = Claims {
        iss: client_id.to_string(),
        sub: client_id.to_string(),
        aud: format!("https://login.microsoftonline.com/{authority}/v2.0"),
        jti: Uuid::new_v4().to_string(),
        nbf: now.timestamp(),
        iat: now.timestamp(),
        exp: (now + Duration::minutes(5)).timestamp(),
    };

    let key_pem = fs::read(key_path).with_context(|| format!("reading key {key_path}"))?;
    let enc_key = EncodingKey::from_rsa_pem(&key_pem)
        .context("EncodingKey::from_rsa_pem (is it an RSA private key?)")?;
    let jwt = encode(&header, &claims, &enc_key).context("encode JWT")?;
    Ok(jwt)
}

async fn fetch_token_with_assertion(
    authority: &str,
    client_id: &str,
    scope: &str,
    client_assertion: &str,
) -> Result<TokenResponse> {
    let url = format!("https://login.microsoftonline.com/{authority}/oauth2/v2.0/token");
    let form = [
        ("client_id", client_id),
        ("grant_type", "client_credentials"),
        ("scope", scope),
        (
            "client_assertion_type",
            "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
        ),
        ("client_assertion", client_assertion),
    ];
    let resp = Client::new().post(url).form(&form).send().await?;
    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(anyhow!("token endpoint error: {} - {}", status, text));
    }
    let tr = resp.json::<TokenResponse>().await?;
    Ok(tr)
}

async fn fetch_token_with_secret(
    authority: &str,
    client_id: &str,
    client_secret: &str,
    scope: &str,
) -> Result<TokenResponse> {
    let url = format!("https://login.microsoftonline.com/{authority}/oauth2/v2.0/token");
    let form = [
        ("client_id", client_id),
        ("client_secret", client_secret),
        ("grant_type", "client_credentials"),
        ("scope", scope),
    ];
    let resp = Client::new().post(url).form(&form).send().await?;
    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(anyhow!("token endpoint error: {} - {}", status, text));
    }
    let tr = resp.json::<TokenResponse>().await?;
    Ok(tr)
}

fn print_token_summary(token: &TokenResponse) -> Result<()> {
    let preview: String = token.access_token.chars().take(20).collect();
    let mode = std::env::var("AZURE_TOKEN_OUTPUT_MODE").unwrap_or_else(|_| "summary".into());
    match mode.as_str() {
        "raw" => println!("{}", token.access_token),
        "export" => println!("ACCESS_TOKEN={}", token.access_token),
        _ => println!(
            "Got token: type={}, expires_in={}s, preview={}…",
            token.token_type, token.expires_in, preview
        ),
    }
    Ok(())
}
