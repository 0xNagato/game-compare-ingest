//! Copy media rows from a local SQLite database into Postgres public.game_media using COPY.
//!
//! - Reads from SQLite table `game_media` (generic), selecting a subset of columns.
//! - Streams rows into a temporary table via Postgres COPY FROM STDIN (CSV),
//!   then performs an upsert into public.game_media to remain idempotent.
//!
//! Environment:
//! - SQLITE_PATH or MEDIA_SQLITE_PATH: absolute/relative path to the SQLite database
//! - DATABASE_URL/SUPABASE_DB_URL/SUPABASE_DB_SESSION_URL: Postgres URL (resolved via util::env)
//!
//! Notes:
//! - Expects FK video_game_id values to already exist in Postgres.
//! - Upserts on (video_game_id, source, external_id) — external_id should uniquely identify
//!   the media item for that source (commonly the media URL itself).

use anyhow::{anyhow, bail, Context, Result};
use bytes::Bytes;
use futures::SinkExt;
use rusqlite::Connection;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::client::WebPkiServerVerifier;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, RootCertStore};
use rustls_native_certs::load_native_certs;
use std::collections::HashSet;
use std::sync::Arc;
use tokio_postgres::Client;
use tokio_postgres::NoTls;
use tokio_postgres_rustls::MakeRustlsConnect;
use webpki_roots::TLS_SERVER_ROOTS;

/// See `src/bin/migrate_one.rs`: Postgres `sslmode=require` means "encrypt" but does not
/// mandate verifying that the server cert chains to a trusted CA.
#[derive(Debug)]
struct InsecureSslModeRequireVerifier {
    inner: Arc<dyn ServerCertVerifier>,
}

impl ServerCertVerifier for InsecureSslModeRequireVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

/// Return true if an env var (case-insensitive) is set to a truthy value.
fn env_flag(name: &str) -> bool {
    std::env::var(name)
        .ok()
        .map(|v| match v.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => true,
            _ => false,
        })
        .unwrap_or(false)
}

/// Parse a positive usize from env, returning default if unset or invalid.
fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}

/// Apply aggressive performance PRAGMAs to the SQLite connection if requested.
fn apply_sqlite_perf_pragmas(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
PRAGMA journal_mode = OFF;       -- disable journaling for speed (acceptable for read-only export)
PRAGMA synchronous = OFF;        -- do not wait for disk sync
PRAGMA temp_store = MEMORY;      -- temp structures in RAM
PRAGMA mmap_size = 3000000000;   -- allow large mmap (best effort)
PRAGMA cache_size = 200000;      -- negative => KB units, here rely on default positive semantics
PRAGMA busy_timeout = 3000;      -- small wait if locked
"#,
    )?;
    Ok(())
}

/// Apply fast ingest Postgres session settings when FAST_INGEST=1.
async fn apply_fast_ingest_session(pg: &Client) -> Result<()> {
    pg.batch_execute(
        r#"
SET LOCAL synchronous_commit = OFF;        -- group commit, faster
SET LOCAL idle_in_transaction_session_timeout = '30s';
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '300s';
SET LOCAL timezone = 'UTC';
SET LOCAL jit = OFF;                       -- disable JIT for bulk INSERT/COPY
"#,
    )
    .await?;
    Ok(())
}

fn table_exists(conn: &Connection, table: &str) -> Result<bool> {
    let mut stmt = conn.prepare(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND lower(name) = lower(?) LIMIT 1",
    )?;
    let mut rows = stmt.query([table])?;
    Ok(rows.next()?.is_some())
}

fn sqlite_columns(conn: &Connection, table: &str) -> Result<HashSet<String>> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let mut rows = stmt.query([])?;
    let mut cols = HashSet::new();
    while let Some(row) = rows.next()? {
        let name: String = row.get(1)?;
        cols.insert(name.to_lowercase());
    }
    Ok(cols)
}

fn sqlite_literal(value: &str) -> String {
    let escaped = value.replace('\'', "''");
    format!("'{escaped}'")
}

fn select_expr(
    columns: &HashSet<String>,
    alias: &str,
    candidates: &[&str],
    field_alias: &str,
    default_expr: &str,
) -> String {
    for candidate in candidates {
        let lowered = candidate.to_lowercase();
        if columns.contains(&lowered) {
            return format!("{alias}.{candidate} AS {field_alias}");
        }
    }
    format!("{default_expr} AS {field_alias}")
}

fn select_expr_required(
    table: &str,
    columns: &HashSet<String>,
    alias: &str,
    candidates: &[&str],
    field_alias: &str,
) -> Result<String> {
    for candidate in candidates {
        let lowered = candidate.to_lowercase();
        if columns.contains(&lowered) {
            return Ok(format!("{alias}.{candidate} AS {field_alias}"));
        }
    }
    bail!(
        "SQLite table '{table}' is missing required column for '{field_alias}' (candidates: {candidates:?})"
    );
}

fn video_game_id_expr(
    conn: &Connection,
    table: &str,
    alias: &str,
    columns: &HashSet<String>,
) -> Result<String> {
    if columns.contains("video_game_id") {
        Ok(format!("{alias}.video_game_id"))
    } else if columns.contains("game_id") {
        Ok(format!("{alias}.game_id AS video_game_id"))
    } else if columns.contains("product_id") {
        let has_video_games = table_exists(conn, "video_games")?;
        let has_titles = table_exists(conn, "video_game_titles")?;
        if has_video_games && has_titles {
            let vgt_cols = sqlite_columns(conn, "video_game_titles")?;
            if vgt_cols.contains("product_id") {
                Ok(format!(
                    "(SELECT vg.id FROM video_games vg JOIN video_game_titles vgt ON vg.title_id = vgt.id WHERE vgt.product_id = {alias}.product_id ORDER BY vg.id LIMIT 1) AS video_game_id"
                ))
            } else {
                // Fallback: check if video_games has product_id directly
                let vg_cols = sqlite_columns(conn, "video_games")?;
                if vg_cols.contains("product_id") {
                    Ok(format!(
                        "(SELECT vg.id FROM video_games vg WHERE vg.product_id = {alias}.product_id ORDER BY vg.id LIMIT 1) AS video_game_id"
                    ))
                } else {
                    bail!("Neither video_game_titles nor video_games has product_id column");
                }
            }
        } else {
            bail!(
                "table '{table}' lacks video_game_id and cannot derive it without video_games/video_game_titles"
            );
        }
    } else {
        bail!("table '{table}' does not expose a usable video_game_id column");
    }
}

fn build_media_select(
    conn: &Connection,
    table: &str,
    alias: &str,
    fallback_literal: &str,
) -> Result<String> {
    let columns = sqlite_columns(conn, table)?;
    let fallback_external = sqlite_literal(fallback_literal);
    let video_game = video_game_id_expr(conn, table, alias, &columns)?;
    let source = select_expr(
        &columns,
        alias,
        &["source", "provider", "origin"],
        "source",
        "'legacy'",
    );
    let external_id = select_expr(
        &columns,
        alias,
        &["external_id", "media_id", "slug"],
        "external_id",
        &fallback_external,
    );
    let media_type = select_expr(
        &columns,
        alias,
        &["media_type", "type", "content_type"],
        "media_type",
        "'image'",
    );
    let raw_kind = select_expr(&columns, alias, &["kind", "role", "label"], "kind", "''");
    let title = select_expr(
        &columns,
        alias,
        &["title", "name", "caption"],
        "title",
        "''",
    );
    let url = select_expr_required(
        table,
        &columns,
        alias,
        &["url", "media_url", "asset_url"],
        "url",
    )?;
    let original_url = select_expr(
        &columns,
        alias,
        &["original_url", "original", "full_url"],
        "original_url",
        "''",
    );
    let thumbnail_url = select_expr(
        &columns,
        alias,
        &["thumbnail_url", "thumb", "thumb_url", "preview_url"],
        "thumbnail_url",
        "''",
    );
    let stream_url = select_expr(
        &columns,
        alias,
        &["stream_url", "video_url", "mp4_url"],
        "stream_url",
        "''",
    );
    let poster_url = select_expr(
        &columns,
        alias,
        &["poster_url", "cover_url"],
        "poster_url",
        "''",
    );
    let provider_data_text = select_expr(
        &columns,
        alias,
        &["provider_data", "metadata", "payload"],
        "provider_data_text",
        "'{}'",
    );

    let order_expr = if columns.contains("id") {
        format!("{alias}.id")
    } else {
        format!("{alias}.rowid")
    };

    let select_parts = vec![
        video_game,
        source,
        external_id,
        media_type,
        raw_kind,
        title,
        url,
        original_url,
        thumbnail_url,
        stream_url,
        poster_url,
        provider_data_text,
    ];

    Ok(format!(
        "SELECT\n  {}\nFROM {table} {alias}\nORDER BY {order}",
        select_parts.join(",\n  "),
        order = order_expr
    ))
}

fn build_product_media_select(conn: &Connection, fallback_literal: &str) -> Result<String> {
    build_media_select(conn, "product_media", "pm", fallback_literal)
}

fn build_game_media_select(conn: &Connection, fallback_literal: &str) -> Result<String> {
    build_media_select(conn, "game_media", "gm", fallback_literal)
}

fn normalize_source(source: &str) -> String {
    let trimmed = source.trim();
    if trimmed.is_empty() {
        "legacy".to_string()
    } else {
        trimmed.to_lowercase()
    }
}

fn guess_media_type_from_url(url: &str) -> &'static str {
    let lower = url.to_ascii_lowercase();
    if lower.ends_with(".mp4")
        || lower.ends_with(".webm")
        || lower.ends_with(".mov")
        || lower.contains("/video/")
        || lower.contains("youtube.com")
        || lower.contains("vimeo.com")
        || lower.contains("vulcan.dl")
    {
        "video"
    } else if lower.ends_with(".mp3") || lower.ends_with(".ogg") || lower.ends_with(".wav") {
        "audio"
    } else {
        "image"
    }
}

fn normalize_media_type(raw: &str, url: &str) -> String {
    let trimmed = raw.trim().to_ascii_lowercase();
    if trimmed.is_empty() {
        guess_media_type_from_url(url).to_string()
    } else if matches!(trimmed.as_str(), "img" | "picture" | "image" | "cover" | "artwork" | "thumbnail" | "thumb" | "banner" | "hero" | "background") {
        "image".to_string()
    } else if matches!(trimmed.as_str(), "vid" | "trailer" | "clip" | "video" | "vulcan.dl" | "preview" | "youtube" | "vimeo" | "podcast") {
        "video".to_string()
    } else if matches!(trimmed.as_str(), "audio" | "music") {
        "audio".to_string()
    } else {
        trimmed
    }
}

fn classify_media_kind(raw_kind: &str, media_type: &str, url: &str) -> String {
    let trimmed = raw_kind.trim();
    if !trimmed.is_empty() {
        return trimmed.to_lowercase();
    }
    if media_type == "video" {
        let lower = url.to_ascii_lowercase();
        if lower.contains("trailer") {
            "trailer".to_string()
        } else if lower.contains("gameplay") {
            "gameplay".to_string()
        } else {
            "video".to_string()
        }
    } else if media_type == "audio" {
        "audio".to_string()
    } else {
        "image".to_string()
    }
}

fn normalize_title(title: &str, media_type: &str) -> String {
    let trimmed = title.trim();
    if trimmed.is_empty() {
        format!("Legacy {media_type}")
    } else {
        trimmed.to_string()
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    i_miss_rust::util::env::bootstrap_cli("copy_sqlite_media");

    let sqlite_path = std::env::var("SQLITE_PATH")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .or_else(|| std::env::var("MEDIA_SQLITE_PATH").ok())
        .or_else(|| std::env::args().nth(1))
        .unwrap_or_else(|| "database.sqlite".to_string());

    // Connect SQLite (blocking)
    let conn = Connection::open(&sqlite_path)
        .with_context(|| format!("connect sqlite at {}", sqlite_path))?;

    if env_flag("SQLITE_PERF") {
        apply_sqlite_perf_pragmas(&conn)?;
        println!("copy_sqlite_media: applied SQLITE_PERF pragmas");
    }

    let has_product_media = table_exists(&conn, "product_media")?;
    let has_game_media = table_exists(&conn, "game_media")?;
    if !has_product_media && !has_game_media {
        return Err(anyhow!(
            "No usable SQLite media tables found (expected 'product_media' or 'game_media')."
        ));
    }
    let use_product_media = has_product_media;
    println!(
        "copy_sqlite_media: reading media rows from {}",
        if use_product_media {
            "product_media"
        } else {
            "game_media"
        }
    );

    // Resolve Postgres URL. Prefer explicit IPv6 DSN if present.
    let pg_url = if let Some(v) = i_miss_rust::util::env::ipv6_db_url() {
        println!("copy_sqlite_media: using IPv6 Postgres DSN for connection");
        v
    } else {
        i_miss_rust::util::env::db_url_prefer_session()?
    };
    let pg_client = connect_postgres_auto(&pg_url).await?;

    if env_flag("FAST_INGEST") {
        apply_fast_ingest_session(&pg_client).await?;
        println!("copy_sqlite_media: FAST_INGEST session settings applied");
    }

    // Ensure we write to public schema but keep extensions visible
    pg_client
        .batch_execute("SET search_path TO public, extensions, ext;")
        .await
        .context("set search_path")?;

    // Create a temp staging table compatible with COPY CSV
    pg_client
        .batch_execute(
            r#"
CREATE TEMP TABLE IF NOT EXISTS _gm_copy (
  video_game_id bigint NOT NULL,
  source        text   NOT NULL,
  external_id   text   NOT NULL,
  media_type    text   NOT NULL,
  kind          text,
  title         text,
  url           text   NOT NULL,
  original_url  text,
  thumbnail_url text,
  stream_url    text,
  poster_url    text,
    provider_data_text text
);
"#,
        )
        .await?;

    // Start COPY
    let copy_stmt = "COPY _gm_copy (video_game_id, source, external_id, media_type, kind, title, url, original_url, thumbnail_url, stream_url, poster_url, provider_data_text) FROM STDIN WITH (FORMAT csv)";
    let sink = pg_client.copy_in(copy_stmt).await?;
    tokio::pin!(sink);

    // CSV buffer
    let mut writer = csv::WriterBuilder::new()
        .has_headers(false)
        .from_writer(vec![]);

    let fallback_literal =
        std::env::var("MEDIA_EXTERNAL_ID_FALLBACK").unwrap_or_else(|_| "missing".to_string());
    let select_sql = if use_product_media {
        build_product_media_select(&conn, &fallback_literal)?
    } else {
        build_game_media_select(&conn, &fallback_literal)?
    };
    let mut stmt = conn.prepare(&select_sql)?;
    let mut rows = stmt.query([])?;

    let batch_size = env_usize("MEDIA_BATCH_SIZE", env_usize("BATCH_FLUSH_SIZE", 5_000));
    let progress_every = env_usize("MEDIA_PROGRESS_EVERY", 25_000);
    let limit = env_usize("MEDIA_LIMIT", 0); // 0 = no limit
    let dry_run = env_flag("DRY_RUN") || env_flag("MEDIA_DRY_RUN");

    let mut buffered: usize = 0;
    let mut copied: usize = 0;
    let mut total_read: usize = 0;
    let mut skipped_external_id: usize = 0;
    let mut skipped_url: usize = 0;
    let mut skipped_duplicate_url: usize = 0;
    let skip_empty_external =
        env_flag("MEDIA_SKIP_EMPTY_EXTERNAL_ID") || env_flag("SKIP_EMPTY_EXTERNAL_ID");
    // Default behavior: require URL to be present. Opt-in MEDIA_ALLOW_EMPTY_URL=1 to bypass skip.
    let allow_empty_url = env_flag("MEDIA_ALLOW_EMPTY_URL") || env_flag("ALLOW_EMPTY_URL");
    // Dedupe within a single run. Key is (video_game_id, url_key) so the same URL can
    // still be associated to multiple games.
    let mut seen_urls: HashSet<String> = HashSet::new();
    while let Some(row) = rows.next()? {
        let video_game_id: i64 = row.get(0)?;
        let source: String = row.get(1)?; // may be constant 'legacy'
        let external_id: String = row.get(2)?; // may be url or ''
        let media_type: String = row.get(3)?; // may derive from kind
        let raw_kind: String = row.get(4)?;
        let title: String = row.get(5)?;
        let url: String = row.get(6)?; // may be '' if absent
        let original_url: String = row.get(7)?;
        let thumbnail_url: String = row.get(8)?;
        let stream_url: String = row.get(9)?;
        let poster_url: String = row.get(10)?;
        let provider_data_text: String = row.get(11)?;

        let normalized_url = url.trim().to_string();
        let normalized_source = normalize_source(&source);
        let normalized_media_type = normalize_media_type(&media_type, &normalized_url);
        let normalized_kind =
            classify_media_kind(raw_kind.trim(), &normalized_media_type, &normalized_url);
        let final_title = normalize_title(&title, &normalized_media_type);
        let final_external_id = external_id.trim().to_string();

        // Optionally skip if after fallback external_id still empty (unlikely due to literal) but guard anyway.
        if skip_empty_external && final_external_id.is_empty() {
            skipped_external_id += 1;
        } else if !allow_empty_url && normalized_url.is_empty() {
            skipped_url += 1;
        } else {
            let mut url_key = normalized_url.clone();
            if url_key.is_empty() {
                url_key = original_url.trim().to_string();
            }
            let dedupe_key = format!("{}|{}", video_game_id, url_key);
            if !url_key.is_empty() && !seen_urls.insert(dedupe_key) {
                skipped_duplicate_url += 1;
                total_read += 1;
                if limit > 0 && total_read >= limit {
                    println!("copy_sqlite_media: hit MEDIA_LIMIT={limit}, stopping early");
                    break;
                }
                continue;
            }
            writer.write_record([
                video_game_id.to_string(),
                normalized_source.to_string(),
                final_external_id,
                normalized_media_type.to_string(),
                normalized_kind.to_string(),
                final_title,
                normalized_url,
                original_url,
                thumbnail_url,
                stream_url,
                poster_url,
                provider_data_text,
            ])?;

            buffered += 1;
        }
        total_read += 1;
        if limit > 0 && total_read >= limit {
            println!("copy_sqlite_media: hit MEDIA_LIMIT={limit}, stopping early");
            break;
        }

        if buffered >= batch_size {
            let buf = writer.into_inner()?;
            sink.send(Bytes::from(buf)).await?;
            writer = csv::WriterBuilder::new()
                .has_headers(false)
                .from_writer(vec![]);
            copied += buffered;
            buffered = 0;
            if copied % progress_every == 0 {
                println!(
                    "copy_sqlite_media: progress copied={} rows (read={})",
                    copied, total_read
                );
            }
        }
    }

    // Flush remainder
    let buf = writer.into_inner()?;
    if !buf.is_empty() {
        sink.send(Bytes::from(buf)).await?;
        copied += buffered;
    }
    sink.close().await?;

    if dry_run {
        println!(
                        "copy_sqlite_media: DRY_RUN set, skipping upsert into canonical_media. Staged rows: {}",
            copied
        );
    } else {
        // Upsert into public.canonical_media
        let upsert_sql = r#"
INSERT INTO canonical_media (
  url,
  url_hash,
  metadata
)
SELECT
  c.url,
    canonical_media_url_hash(c.video_game_id::text || '|' || c.url),
  (
    COALESCE(NULLIF(c.provider_data_text, ''), '{}')::jsonb ||
    jsonb_strip_nulls(jsonb_build_object(
            'video_game_source_id', vgs_match.id,
      'video_game_id', c.video_game_id,
      'source', c.source,
      'media_type', c.media_type,
      'title', c.title,
      'external_id', c.external_id,
      'kind', NULLIF(c.kind, ''),
      'original_url', NULLIF(c.original_url, ''),
      'thumbnail_url', NULLIF(c.thumbnail_url, ''),
      'stream_url', NULLIF(c.stream_url, ''),
      'poster_url', NULLIF(c.poster_url, '')
    ))
  )
FROM _gm_copy c
LEFT JOIN LATERAL (
    SELECT vgs.id
    FROM video_game_sources vgs
    WHERE vgs.video_game_id = c.video_game_id
        AND vgs.provider = c.source
    ORDER BY vgs.id
    LIMIT 1
) vgs_match ON true
ON CONFLICT (url_hash) DO UPDATE SET
  metadata = canonical_media.metadata || EXCLUDED.metadata
"#;
                let affected = pg_client
                        .execute(upsert_sql, &[])
                        .await
                        .context("upsert into canonical_media")?;
        println!(
                        "copy_sqlite_media: staged {} rows (skipped_external_id={}, skipped_url={}, skipped_duplicate_url={}); canonical_media upsert affected {} row(s)",
                        copied, skipped_external_id, skipped_url, skipped_duplicate_url, affected
        );
    }

    // Cleanup temp table
    pg_client
        .batch_execute("DROP TABLE IF EXISTS _gm_copy;")
        .await
        .ok();

    Ok(())
}


/// Connect to Postgres using TLS by default, but honor sslmode overrides.
///
/// Selection rules:
/// - If the URL contains `sslmode=...`, it is authoritative.
/// - Otherwise, if env PG_SSLMODE/DB_SSLMODE is set, use that.
/// - If sslmode is "disable", use plaintext (NoTls).
/// - If sslmode is "require"/"verify-full" or unspecified, use Rustls TLS with system roots.
/// - If sslmode is "prefer", try TLS first; on TLS handshake failure and local host, fall back to NoTls.
async fn connect_postgres_auto(url: &str) -> Result<Client> {
    fn sslmode_from_querystring(url: &str) -> Option<String> {
        url.splitn(2, '?').nth(1).and_then(|qs| {
            qs.split('&').find_map(|kv| {
                let mut it = kv.splitn(2, '=');
                match (it.next(), it.next()) {
                    (Some(k), Some(v)) if k.eq_ignore_ascii_case("sslmode") => {
                        Some(v.to_lowercase())
                    }
                    _ => None,
                }
            })
        })
    }

    let sslmode_url = sslmode_from_querystring(url);
    let sslmode_env = std::env::var("PG_SSLMODE")
        .ok()
        .or_else(|| std::env::var("DB_SSLMODE").ok())
        .map(|s| s.to_lowercase())
        .filter(|s| !s.trim().is_empty());

    // Choose effective sslmode; precedence: querystring > env > default
    let mut sslmode = match (sslmode_url.as_deref(), sslmode_env.as_deref()) {
        (Some(url_mode), Some(env_mode)) if url_mode != env_mode => {
            // Avoid printing the DSN (it contains secrets); only log the modes.
            eprintln!(
                "copy_sqlite_media: sslmode differs between DSN ({}) and env ({}); honoring DSN",
                url_mode, env_mode
            );
            url_mode.to_string()
        }
        (Some(url_mode), _) => url_mode.to_string(),
        (None, Some(env_mode)) => env_mode.to_string(),
        (None, None) => String::new(),
    };
    if sslmode.is_empty() {
        // Heuristic default: require for remote (e.g., Supabase), prefer for localhost
        if url.contains("localhost") || url.contains("127.0.0.1") || url.contains("://0.0.0.0") {
            sslmode = "prefer".to_string();
        } else {
            sslmode = "require".to_string();
        }
    }

    // Helper to open a TLS connection and spawn the connection task
    async fn connect_tls(url: &str, sslmode: &str) -> Result<Client> {
        let mut roots = RootCertStore::empty();
        let native = load_native_certs();
        for cert in native.certs {
            let _ = roots.add(cert);
        }
        roots.extend(TLS_SERVER_ROOTS.iter().cloned());

        let verify_server_cert = matches!(sslmode, "verify-ca" | "verify-full");
        let config = if verify_server_cert {
            ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth()
        } else {
            let inner: Arc<dyn ServerCertVerifier> =
                WebPkiServerVerifier::builder(roots.into()).build()?;
            ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(InsecureSslModeRequireVerifier {
                    inner,
                }))
                .with_no_client_auth()
        };
        let tls = MakeRustlsConnect::new(config);
        let (client, conn) = tokio_postgres::connect(url, tls).await?;
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                eprintln!("postgres connection error: {e}");
            }
        });
        Ok(client)
    }

    // Helper to open a plaintext connection and spawn the connection task
    async fn connect_notls(url: &str) -> Result<Client> {
        let (client, conn) = tokio_postgres::connect(url, NoTls).await?;
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                eprintln!("postgres connection error: {e}");
            }
        });
        Ok(client)
    }

    let host_is_local =
        url.contains("localhost") || url.contains("127.0.0.1") || url.contains("://0.0.0.0");

    let client = match sslmode.as_str() {
        "disable" => {
            println!("copy_sqlite_media: sslmode=disable (NoTLS)");
            connect_notls(url).await?
        }
        "prefer" => {
            // Try TLS first, then fallback if local
            match connect_tls(url, &sslmode).await {
                Ok(c) => {
                    println!("copy_sqlite_media: sslmode=prefer → using TLS");
                    c
                }
                Err(e) if host_is_local => {
                    eprintln!(
                        "copy_sqlite_media: TLS failed in prefer mode on local host, falling back to NoTLS: {e}"
                    );
                    connect_notls(url).await?
                }
                Err(e) => {
                    return Err(anyhow!(
                        "TLS connection failed (sslmode=prefer, non-local): {e}"
                    ));
                }
            }
        }
        // treat require/verify-full/verify-ca as require TLS
        _ => {
            println!("copy_sqlite_media: sslmode={}, using TLS", sslmode);
            connect_tls(url, &sslmode).await?
        }
    };
    Ok(client)
}
