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

use anyhow::{Context, Result, anyhow, bail};
use bytes::Bytes;
use futures::SinkExt;
use rusqlite::Connection;
use rustls::{ClientConfig, RootCertStore};
use rustls_native_certs::load_native_certs;
use std::collections::HashSet;
use tokio_postgres::Client;
use tokio_postgres::NoTls;
use tokio_postgres_rustls::MakeRustlsConnect;

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
            Ok(format!(
                "(SELECT vg.id FROM video_games vg JOIN video_game_titles vgt ON vg.title_id = vgt.id WHERE vgt.product_id = {alias}.product_id ORDER BY vg.id LIMIT 1) AS video_game_id"
            ))
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
    } else if matches!(trimmed.as_str(), "img" | "picture" | "image") {
        "image".to_string()
    } else if matches!(trimmed.as_str(), "vid" | "movie" | "clip" | "video") {
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

    // Ensure we write to public schema
    pg_client
        .batch_execute("SET search_path TO public;")
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
            if !url_key.is_empty() && !seen_urls.insert(url_key.clone()) {
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
            "copy_sqlite_media: DRY_RUN set, skipping upsert into game_media. Staged rows: {}",
            copied
        );
    } else {
        // Upsert into public.game_media
        let upsert_sql = r#"
INSERT INTO public.game_media (
  video_game_id, source, external_id, media_type, kind, title, url,
  original_url, thumbnail_url, stream_url, poster_url, provider_data
)
SELECT
  video_game_id,
  source,
  external_id,
  media_type,
  NULLIF(kind, '') as kind,
  NULLIF(title, '') as title,
  url,
  NULLIF(original_url, ''),
  NULLIF(thumbnail_url, ''),
  NULLIF(stream_url, ''),
  NULLIF(poster_url, ''),
  CASE WHEN provider_data_text IS NULL OR provider_data_text = '' THEN NULL ELSE provider_data_text::jsonb END
FROM _gm_copy
ON CONFLICT (video_game_id, source, external_id) DO UPDATE SET
  media_type    = EXCLUDED.media_type,
  kind          = COALESCE(EXCLUDED.kind, game_media.kind),
  title         = COALESCE(EXCLUDED.title, game_media.title),
  url           = EXCLUDED.url,
  original_url  = COALESCE(EXCLUDED.original_url, game_media.original_url),
  thumbnail_url = COALESCE(EXCLUDED.thumbnail_url, game_media.thumbnail_url),
  stream_url    = COALESCE(EXCLUDED.stream_url, game_media.stream_url),
  poster_url    = COALESCE(EXCLUDED.poster_url, game_media.poster_url),
  provider_data = COALESCE(EXCLUDED.provider_data, game_media.provider_data)
"#;
        pg_client
            .batch_execute(upsert_sql)
            .await
            .context("upsert into game_media")?;
        sync_provider_media_links(&pg_client).await?;
    }

    // Cleanup temp table
    pg_client
        .batch_execute("DROP TABLE IF EXISTS _gm_copy;")
        .await
        .ok();

    println!(
        "copy_sqlite_media: copied {} rows (skipped_external_id={}, skipped_url={}, skipped_duplicate_url={}) into temp table and upserted into game_media",
        copied, skipped_external_id, skipped_url, skipped_duplicate_url
    );
    Ok(())
}

async fn sync_provider_media_links(pg_client: &Client) -> Result<()> {
    let sql = r#"
WITH candidate AS (
    SELECT DISTINCT
        video_game_id,
        NULLIF(source,'') AS source,
        NULLIF(external_id,'') AS external_id,
        NULLIF(media_type,'') AS media_type,
        NULLIF(kind,'') AS kind,
        NULLIF(title,'') AS title,
        NULLIF(url,'') AS url,
        NULLIF(provider_data_text,'') AS provider_data_text
    FROM _gm_copy
), provider_ready AS (
    SELECT
        c.video_game_id,
        c.source,
        c.external_id,
        COALESCE(c.media_type, 'image') AS media_type,
        c.kind,
        c.title,
        c.url,
        COALESCE(NULLIF(c.provider_data_text,''), '{}')::jsonb AS provider_data,
        p.id AS provider_id,
        ('media:' || COALESCE(NULLIF(c.url,''), COALESCE(c.external_id, 'missing'))) AS item_external_id
    FROM candidate c
    JOIN providers p ON lower(p.slug) = lower(c.source)
    WHERE c.url IS NOT NULL AND c.url <> ''
), upsert_items AS (
    INSERT INTO provider_items (provider_id, external_id, kind, metadata)
    SELECT
        provider_id,
        item_external_id,
        COALESCE(kind, media_type),
        provider_data || jsonb_build_object('source', source, 'url', url)
    FROM provider_ready
    ON CONFLICT (provider_id, external_id) DO UPDATE
        SET kind = COALESCE(EXCLUDED.kind, provider_items.kind),
            metadata = COALESCE(provider_items.metadata, EXCLUDED.metadata)
    RETURNING provider_id, external_id, id
)
INSERT INTO provider_media_links (provider_item_id, media_id, url, video_game_id, media_type, title, role, source, metadata)
SELECT DISTINCT
    pi.id,
    gm.id,
    pr.url,
    pr.video_game_id,
    pr.media_type,
    pr.title,
    pr.kind,
    pr.source,
    pr.provider_data || jsonb_build_object('import', 'copy_sqlite_media')
FROM provider_ready pr
JOIN provider_items pi ON pi.provider_id = pr.provider_id AND pi.external_id = pr.item_external_id
LEFT JOIN game_media gm ON gm.video_game_id = pr.video_game_id AND gm.url = pr.url
ON CONFLICT (provider_item_id, url) DO UPDATE
    SET media_id = COALESCE(provider_media_links.media_id, EXCLUDED.media_id),
        video_game_id = COALESCE(provider_media_links.video_game_id, EXCLUDED.video_game_id),
        metadata = provider_media_links.metadata || EXCLUDED.metadata,
        title = COALESCE(provider_media_links.title, EXCLUDED.title),
        role = COALESCE(provider_media_links.role, EXCLUDED.role),
        media_type = COALESCE(provider_media_links.media_type, EXCLUDED.media_type),
        source = COALESCE(provider_media_links.source, EXCLUDED.source);
"#;
    pg_client
        .batch_execute(sql)
        .await
        .context("sync provider media links")?;
    Ok(())
}

/// Connect to Postgres using TLS by default, but honor sslmode overrides.
///
/// Selection rules:
/// - If env PG_SSLMODE/DB_SSLMODE is "disable" (case-insensitive) OR the URL has `?sslmode=disable`, use plaintext (NoTls).
/// - If sslmode is "require"/"verify-full" or unspecified, use Rustls TLS with system roots.
/// - If sslmode is "prefer", try TLS first; on TLS handshake failure and local host, fall back to NoTls.
async fn connect_postgres_auto(url: &str) -> Result<Client> {
    let sslmode_env = std::env::var("PG_SSLMODE")
        .ok()
        .or_else(|| std::env::var("DB_SSLMODE").ok())
        .unwrap_or_default()
        .to_lowercase();

    let sslmode_qs = url
        .splitn(2, '?')
        .nth(1)
        .and_then(|qs| {
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
        .unwrap_or_default();

    // Choose effective sslmode; precedence: env > querystring > default
    let mut sslmode = if !sslmode_env.is_empty() {
        sslmode_env
    } else {
        sslmode_qs
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
    async fn connect_tls(url: &str) -> Result<Client> {
        let mut roots = RootCertStore::empty();
        let native = load_native_certs();
        for cert in native.certs {
            let _ = roots.add(cert);
        }
        let config = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
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
            println!("copy_sqlite_media: PG_SSLMODE=disable (NoTLS)");
            connect_notls(url).await?
        }
        "prefer" => {
            // Try TLS first, then fallback if local
            match connect_tls(url).await {
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
            connect_tls(url).await?
        }
    };
    Ok(client)
}
