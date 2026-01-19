use anyhow::{anyhow, Context, Result};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use chrono::{Duration, Utc};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use pem::parse as parse_pem;
// rand currently unused (removed previous helper); keep if future entropy needed
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sha1::{Digest, Sha1};
use std::collections::HashMap;
use std::time::Duration as StdDuration;
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

#[derive(Debug, Deserialize)]
struct AadErrorResponse {
    error: Option<String>,
    error_description: Option<String>,
    error_codes: Option<Vec<i64>>,
    timestamp: Option<String>,
    trace_id: Option<String>,
    correlation_id: Option<String>,
    error_uri: Option<String>,
    suberror: Option<String>,
    // AAD returns this as a JSON-encoded string when present.
    claims: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    i_miss_rust::util::env::bootstrap_cli("azure_token");
    dotenv::dotenv().ok();

    let tenant_id = env::var("AZURE_TENANT_ID").context("AZURE_TENANT_ID not set")?;
    let client_id = env::var("AZURE_CLIENT_ID").context("AZURE_CLIENT_ID not set")?;
    // IMPORTANT: Display Catalog calls are to displaycatalog.mp.microsoft.com, but the token
    // audience must be `https://onestore.microsoft.com` to authorize pricing/availability.
    let scope = env::var("AZURE_SCOPE")
        .unwrap_or_else(|_| "https://onestore.microsoft.com/.default".to_string());
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
        print_token_summary(&token, &scope).await?;
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
        print_token_summary(&token, &scope).await?;
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
    let mut form: HashMap<&str, String> = HashMap::new();
    form.insert("client_id", client_id.to_string());
    form.insert("grant_type", "client_credentials".to_string());
    form.insert("scope", scope.to_string());
    form.insert(
        "client_assertion_type",
        "urn:ietf:params:oauth:client-assertion-type:jwt-bearer".to_string(),
    );
    form.insert("client_assertion", client_assertion.to_string());

    // If a CA claims challenge is provided, allow explicitly passing it through.
    // This is off by default because it can be confusing; enable via env.
    if let Ok(claims) = std::env::var("AZURE_TOKEN_CLAIMS") {
        if !claims.trim().is_empty() {
            form.insert("claims", claims);
        }
    }

    let client = Client::builder()
        .connect_timeout(StdDuration::from_secs(15))
        .timeout(StdDuration::from_secs(45))
        .build()
        .context("build reqwest client")?;

    let resp = client.post(url).form(&form).send().await?;
    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(anyhow!(format_aad_token_error(status.as_u16(), &text)));
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
    let mut form: HashMap<&str, String> = HashMap::new();
    form.insert("client_id", client_id.to_string());
    form.insert("client_secret", client_secret.to_string());
    form.insert("grant_type", "client_credentials".to_string());
    form.insert("scope", scope.to_string());

    // If a CA claims challenge is provided, allow explicitly passing it through.
    // This is off by default because it can be confusing; enable via env.
    if let Ok(claims) = std::env::var("AZURE_TOKEN_CLAIMS") {
        if !claims.trim().is_empty() {
            form.insert("claims", claims);
        }
    }

    let client = Client::builder()
        .connect_timeout(StdDuration::from_secs(15))
        .timeout(StdDuration::from_secs(45))
        .build()
        .context("build reqwest client")?;

    let resp = client.post(url).form(&form).send().await?;
    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(anyhow!(format_aad_token_error(status.as_u16(), &text)));
    }
    let tr = resp.json::<TokenResponse>().await?;
    Ok(tr)
}

fn format_aad_token_error(status_code: u16, body: &str) -> String {
    // The AAD token endpoint usually returns JSON errors.
    // Keep the original body available, but try to add a crisp explanation.
    let parsed: Option<AadErrorResponse> = serde_json::from_str(body).ok();
    let Some(err) = parsed else {
        return format!("token endpoint error: {status_code} - {body}");
    };

    let mut msg = String::new();
    msg.push_str(&format!("token endpoint error: {status_code}"));

    if let Some(code) = err
        .error_codes
        .as_ref()
        .and_then(|codes| codes.first().copied())
    {
        msg.push_str(&format!(" (AAD error code {code})"));
    }
    msg.push_str("\n");

    if let Some(desc) = err.error_description.as_deref() {
        msg.push_str(desc);
        msg.push('\n');
    } else if let Some(e) = err.error.as_deref() {
        msg.push_str(e);
        msg.push('\n');
    }

    if let (Some(tid), Some(cid)) = (err.trace_id.as_deref(), err.correlation_id.as_deref()) {
        msg.push_str(&format!("trace_id={tid} correlation_id={cid}\n"));
    } else {
        if let Some(tid) = err.trace_id.as_deref() {
            msg.push_str(&format!("trace_id={tid}\n"));
        }
        if let Some(cid) = err.correlation_id.as_deref() {
            msg.push_str(&format!("correlation_id={cid}\n"));
        }
    }
    if let Some(ts) = err.timestamp.as_deref() {
        msg.push_str(&format!("timestamp={ts}\n"));
    }

    // Extra guidance for the most common blocking scenarios we keep hitting.
    if err
        .error_codes
        .as_ref()
        .is_some_and(|codes| codes.contains(&53003))
    {
        msg.push_str(
            "\nThis is a Conditional Access (AADSTS53003) block. For app-only (client_credentials) flows, this usually means a tenant-level Conditional Access policy (often 'workload identities'), tenant restrictions, or app management policy is blocking token issuance for this service principal.\n",
        );

        if let Some(claims_str) = err.claims.as_deref() {
            if let Ok(claims_json) = serde_json::from_str::<JsonValue>(claims_str) {
                if let Some(values) = claims_json
                    .pointer("/access_token/capolids/values")
                    .and_then(|v| v.as_array())
                {
                    let ids: Vec<&str> = values.iter().filter_map(|v| v.as_str()).collect();
                    if !ids.is_empty() {
                        msg.push_str("Conditional Access policy ids (capolids):\n");
                        for id in ids {
                            msg.push_str(&format!("- {id}\n"));
                        }
                    }
                }
            }

            msg.push_str(
                "If your admin asks for the exact claims challenge, it is present in the error response. You can retry by setting AZURE_TOKEN_CLAIMS to that JSON string (rarely helps, but it's the supported pattern for claims challenges).\n",
            );
        }
    }

    // Fall back to the raw body at the end for forensics.
    msg.push_str("\nRaw error body:\n");
    msg.push_str(body);
    msg
}

async fn print_token_summary(token: &TokenResponse, requested_scope: &str) -> Result<()> {
    let mode = std::env::var("AZURE_TOKEN_OUTPUT_MODE").unwrap_or_else(|_| "summary".into());
    match mode.as_str() {
        "raw" => println!("{}", token.access_token),
        "export" => println!("ACCESS_TOKEN={}", token.access_token),
        _ => {
            println!("Requested scope: {}", requested_scope);
            println!(
                "Got token: type={}, expires_in={}s, token_len={}",
                token.token_type,
                token.expires_in,
                token.access_token.len()
            );
            if let Some((hdr, claims)) = try_decode_jwt_parts(&token.access_token)? {
                print_jwt_identity_hint(&hdr, &claims);
            } else {
                println!("Token is not a JWT (no '.' segments); cannot decode claims.");
            }

            if let Ok(probe_url) = std::env::var("AZURE_PROBE_URL") {
                probe_resource(&token.access_token, &probe_url).await?;
            }
        }
    }
    Ok(())
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
    // Keep this probe minimal and safe by default: status + headers.
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

    if std::env::var("AZURE_PROBE_PRINT_BODY").ok().as_deref() == Some("1") {
        let body = resp.text().await.unwrap_or_default();
        let max = std::env::var("AZURE_PROBE_BODY_MAX")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(800);
        let snippet: String = body.chars().take(max).collect();
        println!("Probe body (first {} chars):\n{}", max, snippet);
    }

    Ok(())
}
