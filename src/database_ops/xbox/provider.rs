use crate::database_ops::db::{Db, PriceRow};
use crate::database_ops::ingest_providers::{
    PostIngestSummary, ProviderEntityCache, ensure_country, ensure_currency,
    ensure_national_jurisdiction, ensure_platform, ensure_provider, ensure_provider_media_links,
    ensure_provider_media_links_with_meta, ensure_retailer, ingest_prices, ingest_run_finish,
    ingest_run_start, link_provider_offer, upsert_game_media,
};
use anyhow::{Context, Result, anyhow};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::{Duration as ChronoDuration, NaiveDate, NaiveDateTime, Utc};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use pem::parse as parse_pem;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha1::{Digest, Sha1};
use sqlx::Row;
use sqlx::types::Json;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
use tracing::{info, warn};
use uuid::Uuid;

/// Minimal Microsoft Store (Display Catalog) ingestion for specified product IDs.
pub struct XboxProvider {
    client: Client,
    auth: Option<XboxAuth>,
}

#[derive(Clone)]
enum XboxAuth {
    Static(String),
    Azure(AzureTokenManager),
}

#[derive(Clone)]
struct AzureTokenManager {
    client: Client,
    authority: String,
    scope: String,
    client_id: String,
    credential: AzureCredential,
    cache: Arc<Mutex<Option<CachedToken>>>,
}

#[derive(Clone)]
enum AzureCredential {
    ClientSecret { secret: String },
    ClientAssertion { cert_path: String, key_path: String },
}

#[derive(Clone)]
struct CachedToken {
    token: String,
    expires_at: Instant,
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    expires_in: i64,
    #[allow(dead_code)]
    token_type: String,
}

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

impl XboxAuth {
    fn from_env(client: &Client) -> Result<Option<Self>> {
        let direct = std::env::var("XBOX_ACCESS_TOKEN")
            .or_else(|_| std::env::var("AZURE_ACCESS_TOKEN"))
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        if let Some(token) = direct {
            return Ok(Some(Self::Static(token)));
        }

        let client_secret = std::env::var("AZURE_CLIENT_SECRET").ok();
        let cert_path = std::env::var("AZURE_CLIENT_CERT_PATH").ok();
        let key_path = std::env::var("AZURE_CLIENT_KEY_PATH").ok();
        let has_any = client_secret.is_some() || (cert_path.is_some() && key_path.is_some());
        if !has_any {
            return Ok(None);
        }

        let client_id = std::env::var("AZURE_CLIENT_ID")
            .context("AZURE_CLIENT_ID is required when configuring Azure auth")?;
        if client_id.trim().is_empty() {
            return Err(anyhow!("AZURE_CLIENT_ID cannot be blank"));
        }
        let scope = std::env::var("AZURE_SCOPE")
            .unwrap_or_else(|_| "https://displaycatalog.mp.microsoft.com/.default".to_string());
        let authority = std::env::var("AZURE_AUTHORITY_PATH")
            .ok()
            .filter(|s| !s.is_empty())
            .or_else(|| std::env::var("AZURE_TENANT_ID").ok())
            .unwrap_or_else(|| "organizations".to_string());

        if let Some(secret) = client_secret {
            if secret.trim().is_empty() {
                return Err(anyhow!("AZURE_CLIENT_SECRET is present but empty"));
            }
            let manager = AzureTokenManager::new(
                client.clone(),
                authority,
                scope,
                client_id,
                AzureCredential::ClientSecret { secret },
            );
            return Ok(Some(Self::Azure(manager)));
        }

        let cert_path = cert_path.context(
            "AZURE_CLIENT_CERT_PATH must be set when using certificate-based Azure auth",
        )?;
        let key_path = key_path
            .context("AZURE_CLIENT_KEY_PATH must be set when using certificate-based Azure auth")?;
        let manager = AzureTokenManager::new(
            client.clone(),
            authority,
            scope,
            client_id,
            AzureCredential::ClientAssertion {
                cert_path,
                key_path,
            },
        );
        Ok(Some(Self::Azure(manager)))
    }
}

impl AzureTokenManager {
    fn new(
        client: Client,
        authority: String,
        scope: String,
        client_id: String,
        credential: AzureCredential,
    ) -> Self {
        Self {
            client,
            authority,
            scope,
            client_id,
            credential,
            cache: Arc::new(Mutex::new(None)),
        }
    }

    async fn token(&self) -> Result<String> {
        {
            let guard = self.cache.lock().await;
            if let Some(cached) = guard.as_ref() {
                if cached.expires_at > Instant::now() {
                    return Ok(cached.token.clone());
                }
            }
        }
        let fresh = self.fetch_token().await?;
        let mut guard = self.cache.lock().await;
        let token = fresh.token.clone();
        *guard = Some(fresh);
        Ok(token)
    }

    async fn fetch_token(&self) -> Result<CachedToken> {
        let url = format!(
            "https://login.microsoftonline.com/{}/oauth2/v2.0/token",
            self.authority
        );
        let resp = match &self.credential {
            AzureCredential::ClientSecret { secret } => {
                let form = [
                    ("client_id", self.client_id.as_str()),
                    ("client_secret", secret.as_str()),
                    ("grant_type", "client_credentials"),
                    ("scope", self.scope.as_str()),
                ];
                self.client.post(&url).form(&form).send().await?
            }
            AzureCredential::ClientAssertion {
                cert_path,
                key_path,
            } => {
                let assertion = self.build_client_assertion(cert_path, key_path)?;
                let form = [
                    ("client_id", self.client_id.as_str()),
                    ("grant_type", "client_credentials"),
                    ("scope", self.scope.as_str()),
                    (
                        "client_assertion_type",
                        "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
                    ),
                    ("client_assertion", assertion.as_str()),
                ];
                self.client.post(&url).form(&form).send().await?
            }
        };
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!("azure token endpoint error: {} - {}", status, body));
        }
        let token = resp.json::<TokenResponse>().await?;
        let ttl = (if token.expires_in > 180 {
            token.expires_in - 60
        } else {
            token.expires_in.max(60)
        }) as u64;
        Ok(CachedToken {
            token: token.access_token,
            expires_at: Instant::now() + std::time::Duration::from_secs(ttl),
        })
    }

    fn build_client_assertion(&self, cert_path: &str, key_path: &str) -> Result<String> {
        let cert_pem = fs::read_to_string(cert_path)
            .with_context(|| format!("reading azure cert {cert_path}"))?;
        let pem = parse_pem(cert_pem.as_bytes()).context("parse Azure certificate PEM")?;
        let der = pem.contents();
        let thumb = Sha1::digest(&der);
        let x5t = URL_SAFE_NO_PAD.encode(thumb);

        let mut header = Header::new(Algorithm::RS256);
        header.x5t = Some(x5t);
        let now = Utc::now();
        let claims = Claims {
            iss: self.client_id.clone(),
            sub: self.client_id.clone(),
            aud: format!("https://login.microsoftonline.com/{}/v2.0", self.authority),
            jti: Uuid::new_v4().to_string(),
            nbf: now.timestamp(),
            iat: now.timestamp(),
            exp: (now + ChronoDuration::minutes(5)).timestamp(),
        };
        let key_pem =
            fs::read(key_path).with_context(|| format!("reading azure key {key_path}"))?;
        let encoding_key = EncodingKey::from_rsa_pem(&key_pem)
            .context("Azure client key must be an RSA private key")?;
        let jwt = encode(&header, &claims, &encoding_key)?;
        Ok(jwt)
    }
}

#[derive(Debug, Clone)]
pub struct XboxOptions {
    pub market: String,           // e.g., "US"
    pub language: String,         // e.g., "en-US"
    pub ms_cv: String,            // MS-CV header value
    pub product_ids: Vec<String>, // list of bigIds
}

impl Default for XboxOptions {
    fn default() -> Self {
        Self {
            market: std::env::var("XBOX_MARKET").unwrap_or_else(|_| "US".to_string()),
            language: std::env::var("XBOX_LANGUAGE").unwrap_or_else(|_| "en-US".to_string()),
            ms_cv: std::env::var("XBOX_MS_CV").unwrap_or_else(|_| "DGU1mcuYo0WMMp+".to_string()),
            product_ids: load_product_ids_from_env(),
        }
    }
}

impl XboxProvider {
    pub fn new() -> Result<Self> {
        let client = Client::new();
        let auth = XboxAuth::from_env(&client)?;
        Ok(Self { client, auth })
    }

    async fn build_request(
        &self,
        url: &str,
        opts: &XboxOptions,
    ) -> Result<reqwest::RequestBuilder> {
        let mut req = self.client.get(url).header("MS-CV", &opts.ms_cv);
        if let Some(token) = self.auth_token().await? {
            req = req.bearer_auth(token);
        }
        Ok(req)
    }

    async fn auth_token(&self) -> Result<Option<String>> {
        match &self.auth {
            Some(XboxAuth::Static(token)) => Ok(Some(token.clone())),
            Some(XboxAuth::Azure(manager)) => Ok(Some(manager.token().await?)),
            None => Ok(None),
        }
    }

    fn pace_ms() -> Option<u64> {
        // Prefer RPM over RPS; fall back to None (no pacing)
        if let Ok(v) = std::env::var("XBOX_REQS_PER_MIN") {
            if let Ok(rpm) = v.parse::<u32>() {
                if rpm > 0 {
                    return Some(60_000u64 / (rpm as u64));
                }
            }
        }
        if let Ok(v) = std::env::var("XBOX_RPS") {
            if let Ok(rps) = v.parse::<f32>() {
                if rps > 0.0 {
                    return Some((1000.0 / rps) as u64);
                }
            }
        }
        None
    }

    fn retry_policy() -> (u32, u64) {
        let max_retries: u32 = std::env::var("XBOX_MAX_RETRIES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(3);
        let backoff_ms: u64 = std::env::var("XBOX_BACKOFF_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1000);
        (max_retries, backoff_ms)
    }

    /// Dry-run: fetch and dump Microsoft DC JSON without touching the database.
    /// Returns the number of products observed across all chunks.
    pub async fn run_dump_only(&self, opts: XboxOptions) -> Result<usize> {
        if opts.product_ids.is_empty() {
            warn!("XBOX_PRODUCT_IDS empty; dry-run has nothing to fetch");
            return Ok(0);
        }

        let dump_json = std::env::var("XBOX_DUMP_JSON")
            .map(|v| (v == "1" || v.eq_ignore_ascii_case("true")))
            .unwrap_or(false);
        let dump_stdout = std::env::var("XBOX_DUMP_STDOUT")
            .map(|v| (v == "1" || v.eq_ignore_ascii_case("true")))
            .unwrap_or(false);
        let dump_dir =
            std::env::var("XBOX_DUMP_DIR").unwrap_or_else(|_| "exports/xbox".to_string());
        let run_dir: Option<PathBuf> = if dump_json {
            let ts = chrono::Utc::now().format("%Y%m%d_%H%M%S");
            let dir = PathBuf::from(dump_dir).join(format!("run_{}", ts));
            let _ = fs::create_dir_all(&dir);
            Some(dir)
        } else {
            None
        };
        let mut ndjson_file: Option<std::fs::File> = if let Some(dir) = &run_dir {
            let path = dir.join("logs.ndjson");
            OpenOptions::new().create(true).append(true).open(path).ok()
        } else {
            None
        };

        let mut total_products = 0usize;
        let mut had_error = false;
        let chunk_size: usize = std::env::var("XBOX_CHUNK_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(30)
            .max(1);
        for (chunk_idx, chunk) in opts.product_ids.chunks(chunk_size).enumerate() {
            let url = format!(
                "https://displaycatalog.mp.microsoft.com/v7.0/products?bigIds={}&market={}&languages={}",
                chunk.join(","),
                opts.market,
                opts.language
            );
            let (max_retries, backoff_ms) = Self::retry_policy();
            let pace_ms = Self::pace_ms();
            // do request with retry policy
            let mut tries: u32 = 0;
            let resp = loop {
                tries += 1;
                if let Some(ms) = pace_ms {
                    tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
                }
                let request = self.build_request(&url, &opts).await?;
                match request.send().await {
                    Ok(r) => {
                        if r.status().as_u16() == 429 {
                            let mut sleep_ms: u64 = std::env::var("XBOX_CHUNK_SLEEP_MS")
                                .ok()
                                .and_then(|s| s.parse().ok())
                                .unwrap_or(backoff_ms);
                            if let Some(ra) = r
                                .headers()
                                .get("Retry-After")
                                .and_then(|h| h.to_str().ok())
                                .and_then(|s| s.parse::<u64>().ok())
                            {
                                sleep_ms = (ra * 1000).max(sleep_ms);
                            }
                            warn!(chunk=%chunk_idx, sleep_ms, "xbox 429 throttled; sleeping before retry (dry-run)");
                            tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;
                            if tries <= max_retries {
                                continue;
                            }
                            break r;
                        } else {
                            break r;
                        }
                    }
                    Err(e) => {
                        if tries <= max_retries {
                            tokio::time::sleep(std::time::Duration::from_millis(
                                backoff_ms.saturating_mul(tries as u64),
                            ))
                            .await;
                            continue;
                        }
                        had_error = true;
                        tracing::error!(error=%e, url, "xbox http error (dry-run)");
                        if let Some(f) = ndjson_file.as_mut() {
                            let _ = writeln!(
                                f,
                                "{}",
                                serde_json::json!({"ts": chrono::Utc::now(), "chunk": chunk_idx, "status":"http_error", "error": e.to_string(), "ids": chunk})
                            );
                        }
                        continue;
                    }
                }
            };
            let resp = match resp.error_for_status() {
                Ok(ok) => ok,
                Err(e) => {
                    had_error = true;
                    tracing::error!(error=%e, url, "xbox non-success status (dry-run)");
                    if let Some(f) = ndjson_file.as_mut() {
                        let _ = writeln!(
                            f,
                            "{}",
                            serde_json::json!({"ts": chrono::Utc::now(), "chunk": chunk_idx, "status":"bad_status", "error": e.to_string(), "ids": chunk})
                        );
                    }
                    continue;
                }
            };
            let body: Value = match resp.json().await {
                Ok(b) => b,
                Err(e) => {
                    had_error = true;
                    tracing::error!(error=%e, url, "xbox json decode failed (dry-run)");
                    if let Some(f) = ndjson_file.as_mut() {
                        let _ = writeln!(
                            f,
                            "{}",
                            serde_json::json!({"ts": chrono::Utc::now(), "chunk": chunk_idx, "status":"json_error", "error": e.to_string(), "ids": chunk})
                        );
                    }
                    continue;
                }
            };
            if dump_stdout {
                println!("===== xbox chunk {} ({} ids) =====", chunk_idx, chunk.len());
                match serde_json::to_string_pretty(&body) {
                    Ok(pretty) => println!("{}", pretty),
                    Err(_) => println!("{}", body),
                }
            }
            if let Some(dir) = &run_dir {
                let first = chunk.get(0).cloned().unwrap_or_else(|| "chunk".to_string());
                let path = dir.join(format!("chunk_{:04}_{}.json", chunk_idx, first));
                if let Ok(mut f) = OpenOptions::new().create(true).write(true).open(path) {
                    let _ = write!(
                        f,
                        "{}",
                        serde_json::to_string_pretty(&body).unwrap_or_else(|_| "{}".into())
                    );
                }
            }
            let products = body
                .get("Products")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            total_products += products.len();
            if let Some(f) = ndjson_file.as_mut() {
                let _ = writeln!(
                    f,
                    "{}",
                    serde_json::json!({
                        "ts": chrono::Utc::now(),
                        "chunk": chunk_idx,
                        "status": "ok",
                        "ids": chunk,
                        "products": products.len(),
                    })
                );
            }
        }
        info!(products=total_products, status = if had_error { "partial" } else { "ok" }, market=%opts.market, language=%opts.language, "xbox dry-run complete");
        Ok(total_products)
    }

    /// Execute one pass of fetching & ingesting prices.
    pub async fn run_once(&self, db: &Db, opts: XboxOptions) -> Result<usize> {
        if opts.product_ids.is_empty() {
            warn!("XBOX_PRODUCT_IDS empty; skipping");
            return Ok(0);
        }

        // Ensure base domain entities
        let provider_id =
            ensure_provider(db, "microsoft_store", "storefront", Some("xbox-store")).await?;
        let retailer_id = ensure_retailer(db, "Microsoft", Some("microsoft")).await?;
        let (cur_code, cur_name) = currency_for_market(&opts.market);
        let mu = currency_minor_unit(cur_code);
        let currency_id = ensure_currency(db, cur_code, cur_name, mu).await?;
        let country_code2 = opts.market.to_ascii_uppercase();
        let country_id = ensure_country(db, &country_code2, &country_code2, currency_id).await?;
        let juris_id = ensure_national_jurisdiction(db, country_id).await?;
        let mut entity_cache = ProviderEntityCache::new(db.clone());

        // ingest run start
        let run_id = ingest_run_start(
            db,
            provider_id,
            Some(&opts.market),
            Some(serde_json::json!({"language": opts.language})),
        )
        .await?;

        let mut post_summary = PostIngestSummary::default();

        // Optional JSON dumping
        let dump_json = std::env::var("XBOX_DUMP_JSON")
            .map(|v| (v == "1" || v.eq_ignore_ascii_case("true")))
            .unwrap_or(false);
        let dump_dir =
            std::env::var("XBOX_DUMP_DIR").unwrap_or_else(|_| "exports/xbox".to_string());
        let run_dir: Option<PathBuf> = if dump_json {
            let ts = chrono::Utc::now().format("%Y%m%d_%H%M%S");
            let dir = PathBuf::from(dump_dir).join(format!("run_{}", ts));
            let _ = fs::create_dir_all(&dir);
            Some(dir)
        } else {
            None
        };
        let mut ndjson_file: Option<std::fs::File> = if let Some(dir) = &run_dir {
            let path = dir.join("logs.ndjson");
            OpenOptions::new().create(true).append(true).open(path).ok()
        } else {
            None
        };

        let mut total_products = 0usize;
        let mut prices_written: i64 = 0;
        let mut had_error = false;
        let chunk_size: usize = std::env::var("XBOX_CHUNK_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(30)
            .max(1);
        for (chunk_idx, chunk) in opts.product_ids.chunks(chunk_size).enumerate() {
            let url = format!(
                "https://displaycatalog.mp.microsoft.com/v7.0/products?bigIds={}&market={}&languages={}",
                chunk.join(","),
                opts.market,
                opts.language
            );
            let (max_retries, backoff_ms) = Self::retry_policy();
            let pace_ms = Self::pace_ms();
            let mut tries: u32 = 0;
            let resp = loop {
                tries += 1;
                if let Some(ms) = pace_ms {
                    tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
                }
                let request = self.build_request(&url, &opts).await?;
                match request.send().await {
                    Ok(r) => {
                        if r.status().as_u16() == 429 {
                            let mut sleep_ms: u64 = std::env::var("XBOX_CHUNK_SLEEP_MS")
                                .ok()
                                .and_then(|s| s.parse().ok())
                                .unwrap_or(backoff_ms);
                            if let Some(ra) = r
                                .headers()
                                .get("Retry-After")
                                .and_then(|h| h.to_str().ok())
                                .and_then(|s| s.parse::<u64>().ok())
                            {
                                sleep_ms = (ra * 1000).max(sleep_ms);
                            }
                            warn!(chunk=%chunk_idx, sleep_ms, "xbox 429 throttled; sleeping before retry");
                            tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;
                            if tries <= max_retries {
                                continue;
                            }
                            break r;
                        } else {
                            break r;
                        }
                    }
                    Err(e) => {
                        if tries <= max_retries {
                            tokio::time::sleep(std::time::Duration::from_millis(
                                backoff_ms.saturating_mul(tries as u64),
                            ))
                            .await;
                            continue;
                        }
                        had_error = true;
                        tracing::error!(error=%e, url, "xbox http error");
                        if let Some(f) = ndjson_file.as_mut() {
                            let _ = writeln!(
                                f,
                                "{}",
                                serde_json::json!({"ts": chrono::Utc::now(), "chunk": chunk_idx, "status":"http_error", "error": e.to_string(), "ids": chunk})
                            );
                        }
                        continue;
                    }
                }
            };
            let resp = match resp.error_for_status() {
                Ok(ok) => ok,
                Err(e) => {
                    had_error = true;
                    tracing::error!(error=%e, url, "xbox non-success status");
                    if let Some(f) = ndjson_file.as_mut() {
                        let _ = writeln!(
                            f,
                            "{}",
                            serde_json::json!({"ts": chrono::Utc::now(), "chunk": chunk_idx, "status":"bad_status", "error": e.to_string(), "ids": chunk})
                        );
                    }
                    continue;
                }
            };
            let body: Value = match resp.json().await {
                Ok(b) => b,
                Err(e) => {
                    had_error = true;
                    tracing::error!(error=%e, url, "xbox json decode failed");
                    if let Some(f) = ndjson_file.as_mut() {
                        let _ = writeln!(
                            f,
                            "{}",
                            serde_json::json!({"ts": chrono::Utc::now(), "chunk": chunk_idx, "status":"json_error", "error": e.to_string(), "ids": chunk})
                        );
                    }
                    continue;
                }
            };
            if let Some(dir) = &run_dir {
                // Save raw JSON for this chunk
                let first = chunk.get(0).cloned().unwrap_or_else(|| "chunk".to_string());
                let path = dir.join(format!("chunk_{:04}_{}.json", chunk_idx, first));
                if let Ok(mut f) = OpenOptions::new().create(true).write(true).open(path) {
                    let _ = write!(
                        f,
                        "{}",
                        serde_json::to_string_pretty(&body).unwrap_or_else(|_| "{}".into())
                    );
                }
            }
            let products = body
                .get("Products")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            if products.is_empty() {
                warn!(chunk=%chunk_idx, ids=?chunk, market=%opts.market, language=%opts.language, "xbox products array empty for chunk (possible invalid bigIds)");
            }

            let mut price_rows: Vec<PriceRow> = Vec::new();
            for p in &products {
                let pid = p.get("ProductId").and_then(|v| v.as_str()).unwrap_or("");
                if pid.is_empty() {
                    continue;
                }
                let localized = p
                    .get("LocalizedProperties")
                    .and_then(|v| v.as_array())
                    .and_then(|arr| arr.get(0));
                let title = localized
                    .and_then(|lp| {
                        lp.get("ProductTitle")
                            .or_else(|| lp.get("Title"))
                            .and_then(|v| v.as_str())
                    })
                    .unwrap_or(pid);
                let synopsis = localized.and_then(|lp| {
                    first_clean_string(&[
                        lp.get("ShortDescription").and_then(|v| v.as_str()),
                        lp.get("Description").and_then(|v| v.as_str()),
                        lp.get("ProductDescription").and_then(|v| v.as_str()),
                    ])
                });
                let publisher = localized
                    .and_then(|lp| lp.get("PublisherName").and_then(|v| v.as_str()))
                    .map(|s| s.to_string());
                let mut genres: Vec<String> = Vec::new();
                if let Some(props) = p.get("Properties") {
                    if let Some(cat) = props.get("Category").and_then(|v| v.as_str()) {
                        if !cat.trim().is_empty() {
                            genres.push(cat.trim().to_string());
                        }
                    }
                    if let Some(subcat) = props.get("SubCategory") {
                        match subcat {
                            Value::String(s) => {
                                if !s.trim().is_empty() {
                                    genres.push(s.trim().to_string());
                                }
                            }
                            Value::Array(arr) => {
                                for val in arr {
                                    if let Some(s) = val.as_str() {
                                        if !s.trim().is_empty() {
                                            genres.push(s.trim().to_string());
                                        }
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    if let Some(cat_arr) = props.get("Categories").and_then(|v| v.as_array()) {
                        for val in cat_arr {
                            if let Some(s) = val.as_str() {
                                if !s.trim().is_empty() {
                                    genres.push(s.trim().to_string());
                                }
                            }
                        }
                    }
                }
                if let Some(mps) = p.get("MarketProperties").and_then(|v| v.as_array()) {
                    for mp in mps {
                        if let Some(gen_arr) = mp.get("Genres").and_then(|v| v.as_array()) {
                            for g in gen_arr {
                                if let Some(s) = g.as_str() {
                                    if !s.trim().is_empty() {
                                        genres.push(s.trim().to_string());
                                    }
                                }
                            }
                        }
                    }
                }
                genres.sort();
                genres.dedup();
                let mut content_ratings: Vec<String> = Vec::new();
                if let Some(mps) = p.get("MarketProperties").and_then(|v| v.as_array()) {
                    for mp in mps {
                        if let Some(crs) = mp.get("ContentRatings").and_then(|v| v.as_array()) {
                            for cr in crs {
                                if let Some(id) = cr.get("RatingId").and_then(|v| v.as_str()) {
                                    content_ratings.push(id.to_string());
                                }
                            }
                        }
                    }
                }
                content_ratings.sort();
                content_ratings.dedup();
                let release_date = p
                    .get("MarketProperties")
                    .and_then(|v| v.as_array())
                    .and_then(|arr| arr.get(0))
                    .and_then(|mp| mp.get("OriginalReleaseDate"))
                    .and_then(|v| v.as_str())
                    .and_then(parse_original_release_date);
                let slug = slugify(title);
                let product_id = entity_cache
                    .ensure_product_named("software", &slug, title)
                    .await?;
                entity_cache.ensure_software_row(product_id).await?;
                let title_id = entity_cache
                    .ensure_video_game_title(product_id, title, Some(&slug))
                    .await?;
                // generic platform stub
                let platform_id = ensure_platform(db, "XBOX", Some("xbox")).await?;
                let vg_id = entity_cache
                    .ensure_video_game(title_id, platform_id, None)
                    .await?;
                let sellable_id = entity_cache.ensure_sellable("software", product_id).await?;
                let offer_id = entity_cache
                    .ensure_offer(sellable_id, retailer_id, Some(pid))
                    .await?;
                let oj_id = entity_cache
                    .ensure_offer_jurisdiction(offer_id, juris_id, currency_id)
                    .await?;
                let provider_item_id = entity_cache
                    .ensure_provider_item(provider_id, pid, Some(p.clone()), true)
                    .await?;
                post_summary.record_provider_item(provider_item_id);
                link_provider_offer(db, provider_item_id, offer_id, Some(0.85)).await?;
                {
                    let synopsis_ref = synopsis.as_deref();
                    let genre_opt: Option<&Vec<String>> = if genres.is_empty() {
                        None
                    } else {
                        Some(&genres)
                    };
                    let metadata_patch = if content_ratings.is_empty() && publisher.is_none() {
                        None
                    } else {
                        Some(json!({
                            "xbox": {
                                "content_ratings": content_ratings.clone(),
                                "publisher": publisher.clone(),
                            }
                        }))
                    };
                    if synopsis_ref.is_some()
                        || genre_opt.is_some()
                        || metadata_patch.is_some()
                        || release_date.is_some()
                    {
                        let _ = sqlx
                            ::query(
                                "UPDATE video_games
                             SET synopsis = CASE WHEN $1::text IS NOT NULL AND (synopsis IS NULL OR length(synopsis) < length($1)) THEN $1 ELSE synopsis END,
                                 genres = CASE WHEN $2::text[] IS NOT NULL AND array_length($2,1) > 0 THEN $2 ELSE genres END,
                                 metadata = CASE WHEN $3::jsonb IS NOT NULL THEN COALESCE(metadata, '{}'::jsonb) || $3 ELSE metadata END,
                                 release_date = COALESCE(release_date, $4)
                             WHERE id = $5"
                            )
                            .persistent(false)
                            .bind(synopsis_ref)
                            .bind(genre_opt)
                            .bind(metadata_patch.clone().map(Json))
                            .bind(release_date)
                            .bind(vg_id)
                            .execute(&db.pool).await;
                    }
                }
                let mut final_price_minor: Option<i64> = None;
                {
                    // Minimal media enrichment (cover) if image exists in payload (placeholder logic)
                    if let Some(images) = p
                        .get("LocalizedProperties")
                        .and_then(|v| v.as_array())
                        .and_then(|arr| arr.get(0))
                        .and_then(|lp| lp.get("Images"))
                        .and_then(|imgs| imgs.as_array())
                    {
                        if let Some(first_img) = images
                            .get(0)
                            .and_then(|im| im.get("Uri"))
                            .and_then(|u| u.as_str())
                        {
                            // Use fine-grained media_type 'cover' (rather than generic 'image') for semantic labeling; role remains 'cover'
                            let tuples = vec![(
                                first_img.to_string(),
                                Some("cover".into()),
                                Some("cover".into()),
                                Some(title.to_string()),
                            )];
                            let meta = serde_json::json!({"source":"xbox","market":opts.market});
                            if let Err(err) = ensure_provider_media_links_with_meta(
                                db,
                                provider_item_id,
                                Some(vg_id),
                                &tuples,
                                "xbox",
                                Some(meta),
                            )
                            .await
                            {
                                if err.to_string().contains("media_kind") {
                                    let simple_urls: Vec<String> =
                                        tuples.iter().map(|(u, _, _, _)| u.clone()).collect();
                                    let _ = ensure_provider_media_links(
                                        db,
                                        provider_item_id,
                                        &simple_urls,
                                    )
                                    .await;
                                } else {
                                    return Err(err);
                                }
                            }
                            let pdata = serde_json::json!({"market":opts.market});
                            let _ = upsert_game_media(
                                db, vg_id, "xbox", first_img, "cover", first_img, pdata,
                            )
                            .await;
                        }
                    } else {
                        // Fallback placeholder cover
                        let placeholder = format!("https://example.com/xbox/{}.jpg", pid);
                        let tuples = vec![(
                            placeholder.clone(),
                            Some("cover".into()),
                            Some("cover".into()),
                            Some(title.to_string()),
                        )];
                        let meta =
                            serde_json::json!({"source":"xbox","market":opts.market,"stub":true});
                        if let Err(err) = ensure_provider_media_links_with_meta(
                            db,
                            provider_item_id,
                            Some(vg_id),
                            &tuples,
                            "xbox",
                            Some(meta),
                        )
                        .await
                        {
                            if err.to_string().contains("media_kind") {
                                let simple_urls: Vec<String> =
                                    tuples.iter().map(|(u, _, _, _)| u.clone()).collect();
                                let _ =
                                    ensure_provider_media_links(db, provider_item_id, &simple_urls)
                                        .await;
                            } else {
                                return Err(err);
                            }
                        }
                        let pdata = serde_json::json!({"market":opts.market,"stub":true});
                        let _ = upsert_game_media(
                            db,
                            vg_id,
                            "xbox",
                            &placeholder,
                            "cover",
                            &placeholder,
                            pdata,
                        )
                        .await;
                    }
                }

                if let Some((amount_minor, tax_inclusive)) = find_price_minor(&p, mu) {
                    final_price_minor = Some(amount_minor);
                    price_rows.push(PriceRow {
                        offer_jurisdiction_id: oj_id,
                        provider_item_id: Some(provider_item_id),
                        recorded_at: Utc::now(),
                        amount_minor,
                        tax_inclusive,
                        fx_minor_per_unit: None,
                        btc_sats_per_unit: None,
                        meta: serde_json::json!({"src":"xbox","market":opts.market,"lang":opts.language}),
                    });
                }
                total_products += 1;

                info!(
                    xbox_product = %pid,
                    title = %title,
                    synopsis = synopsis.as_deref().unwrap_or(""),
                    genres = ?genres,
                    content_ratings = ?content_ratings,
                    final_price_minor = final_price_minor,
                    release_date = ?release_date,
                    "xbox product metadata captured"
                );
            }

            if !price_rows.is_empty() {
                let batch = std::mem::take(&mut price_rows);
                let batch_len = batch.len();
                match ingest_prices(db, batch).await {
                    Ok(res) => {
                        prices_written += res.current_updates.len() as i64;
                        post_summary.record_batch(batch_len, &res);
                    }
                    Err(e) => {
                        had_error = true;
                        tracing::error!(error=%e, "xbox ingest_prices failed");
                    }
                }
            }
            if let Some(f) = ndjson_file.as_mut() {
                let _ = writeln!(
                    f,
                    "{}",
                    serde_json::json!({
                        "ts": chrono::Utc::now(),
                        "chunk": chunk_idx,
                        "status": "ok",
                        "ids": chunk,
                        "products": products.len(),
                        "prices_written": prices_written,
                    })
                );
            }
        }
        let status = if had_error {
            if total_products == 0 {
                "error"
            } else {
                "partial"
            }
        } else {
            "ok"
        };
        if let Err(e) = ingest_run_finish(
            db,
            run_id,
            status,
            total_products as i64,
            prices_written,
            None,
        )
        .await
        {
            tracing::error!(error=%e, run_id, "ingest_run_finish failed");
        }
        info!(count=total_products, market=%opts.market, language=%opts.language, prices_written, status, "xbox: processed products");
        // Diagnostics: media classification summary (kind distribution + samples)
        // Safe best-effort logging; failures are non-fatal.
        if
            let Ok(media_counts) = sqlx
                ::query(
                    "SELECT kind::text, COUNT(*)::bigint AS cnt FROM provider_media_links WHERE provider_item_id IN (SELECT id FROM provider_items WHERE provider_id=$1) GROUP BY kind ORDER BY cnt DESC"
                )
                .bind(provider_id)
                .fetch_all(&db.pool).await
        {
            for row in media_counts {
                let kind: Option<String> = row.try_get("kind").ok();
                let cnt: i64 = row.try_get("cnt").unwrap_or(0);
                info!(media_kind=?kind.unwrap_or_else(||"<null>".into()), count=cnt, "xbox media kind count");
            }
        }
        if
            let Ok(samples) = sqlx
                ::query(
                    "SELECT kind::text, title, url FROM provider_media_links WHERE provider_item_id IN (SELECT id FROM provider_items WHERE provider_id=$1) ORDER BY id DESC LIMIT 5"
                )
                .bind(provider_id)
                .fetch_all(&db.pool).await
        {
            for r in samples {
                let kind: Option<String> = r.try_get("kind").ok();
                let title: Option<String> = r.try_get("title").ok();
                let url: String = r.try_get("url").unwrap_or_default();
                info!(media_kind=?kind.unwrap_or_else(||"<null>".into()), media_title=?title.unwrap_or_default(), url=%url, "xbox media sample");
            }
        }
        post_summary.verify(db, provider_id).await?;
        info!(
            provider_id,
            price_rows = post_summary.total_price_rows_written,
            provider_items = post_summary.provider_item_ids.len(),
            offer_jurisdictions = post_summary.offer_jurisdiction_ids.len(),
            "xbox provider verification complete"
        );
        Ok(total_products)
    }
}

pub fn parse_product_ids(input: &str) -> Vec<String> {
    input
        .split(|c: char| (c == '\n' || c == '\r' || c == ',' || c.is_whitespace()))
        .map(|s| s.trim())
        .filter(|s| !s.is_empty() && !s.starts_with('#'))
        .map(|s| s.to_string())
        .collect()
}

fn load_product_ids_from_env() -> Vec<String> {
    if let Ok(path) = std::env::var("XBOX_PRODUCT_IDS_FILE") {
        if let Ok(contents) = std::fs::read_to_string(&path) {
            let parsed = parse_product_ids(&contents);
            if !parsed.is_empty() {
                return parsed;
            }
        }
    }
    std::env::var("XBOX_PRODUCT_IDS")
        .ok()
        .map(|s| parse_product_ids(&s))
        .unwrap_or_default()
}

fn currency_minor_unit(code: &str) -> i16 {
    match code.to_ascii_uppercase().as_str() {
        "JPY" | "KRW" | "VND" | "CLP" | "ISK" | "HUF" => 0,
        "BHD" | "IQD" | "KWD" | "JOD" | "OMR" | "TND" => 3,
        _ => 2,
    }
}

fn currency_for_market(market: &str) -> (&'static str, &'static str) {
    match market.to_ascii_uppercase().as_str() {
        "US" | "CA" | "AU" | "NZ" => ("USD", "US Dollar"),
        "GB" => ("GBP", "British Pound"),
        "DE" | "FR" | "ES" | "IT" | "NL" | "BE" | "PT" | "IE" | "FI" | "GR" | "AT" | "LU"
        | "SI" | "SK" | "LV" | "LT" | "EE" | "MT" | "CY" => ("EUR", "Euro"),
        "JP" => ("JPY", "Japanese Yen"),
        "KR" => ("KRW", "South Korean Won"),
        _ => ("USD", "US Dollar"),
    }
}

fn slugify(s: &str) -> String {
    s.to_lowercase()
        .replace(|c: char| !c.is_ascii_alphanumeric(), "-")
        .trim_matches('-')
        .to_string()
}

fn find_price_minor(v: &Value, minor_unit: i16) -> Option<(i64, bool)> {
    if let Some(arr) = v.get("DisplaySkuAvailabilities").and_then(|a| a.as_array()) {
        for dsa in arr {
            if let Some(lp_arr) = dsa
                .get("Sku")
                .and_then(|s| s.get("LocalizedProperties"))
                .and_then(|x| x.as_array())
            {
                for lp in lp_arr {
                    if let Some(p) = lp.get("Price") {
                        if let Some((amt, ti)) = extract_common_price(p, minor_unit) {
                            return Some((amt, ti));
                        }
                    }
                }
            }
            if let Some(omd_price) = dsa.get("OrderManagementData").and_then(|o| o.get("Price")) {
                if let Some((amt, ti)) = extract_common_price(omd_price, minor_unit) {
                    return Some((amt, ti));
                }
            }
        }
    }
    extract_common_price(v, minor_unit)
}

fn extract_common_price(p: &Value, minor_unit: i16) -> Option<(i64, bool)> {
    let candidates = ["ListPrice", "MSRP", "Amount", "CurrentPrice", "Price"];
    for k in candidates {
        if let Some(n) = p.get(k) {
            if let Some(f) = n.as_f64() {
                return Some(((f * (10f64).powi(minor_unit as i32)).round() as i64, true));
            }
            if let Some(s) = n.as_str() {
                if let Ok(f) = s.parse::<f64>() {
                    return Some(((f * (10f64).powi(minor_unit as i32)).round() as i64, true));
                }
            }
        }
    }
    None
}

fn first_clean_string(candidates: &[Option<&str>]) -> Option<String> {
    for cand in candidates {
        if let Some(val) = cand {
            let cleaned = strip_html_tags(val);
            if !cleaned.is_empty() {
                return Some(cleaned);
            }
        }
    }
    None
}

fn strip_html_tags(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut in_tag = false;
    let mut prev_space = false;
    for ch in input.chars() {
        match ch {
            '<' => {
                in_tag = true;
            }
            '>' => {
                in_tag = false;
            }
            c => {
                if !in_tag {
                    let mapped = if c.is_whitespace() { ' ' } else { c };
                    if mapped == ' ' {
                        if !prev_space {
                            out.push(' ');
                            prev_space = true;
                        }
                    } else {
                        out.push(mapped);
                        prev_space = false;
                    }
                }
            }
        }
    }
    out.trim().to_string()
}

fn parse_original_release_date(raw: &str) -> Option<NaiveDate> {
    const FORMATS: [&str; 2] = ["%Y-%m-%dT%H:%M:%S%.fZ", "%Y-%m-%dT%H:%M:%S%.f"];
    for fmt in FORMATS {
        if let Ok(dt) = NaiveDateTime::parse_from_str(raw, fmt) {
            return Some(dt.date());
        }
    }
    if let Ok(date) = NaiveDate::parse_from_str(raw, "%Y-%m-%d") {
        return Some(date);
    }
    None
}

pub async fn run_from_env(db: &Db) -> Result<()> {
    let prov = XboxProvider::new()?;
    let opts = XboxOptions::default();
    // Dry-run option: skip database writes and only dump JSON if XBOX_DRY_RUN=1
    if std::env::var("XBOX_DRY_RUN").ok().as_deref() == Some("1") {
        let _ = prov.run_dump_only(opts).await?;
    } else {
        let _ = prov.run_once(db, opts).await?;
    }
    Ok(())
}
