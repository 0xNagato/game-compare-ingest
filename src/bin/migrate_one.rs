use anyhow::{Context, Result};
use i_miss_rust::util::env::{self, db_url};
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use std::{fs, path::Path};
use tokio_postgres::{Client, NoTls};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    // Logging
    env::bootstrap_cli("migrate_one");
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let database_url = db_url().context(
        "no database URL env vars set (SUPABASE_DB_SESSION_URL | SUPABASE_DB_URL | DATABASE_URL)",
    )?;
    let mig_file = std::env::var("MIG_FILE")
        .context("MIG_FILE must be set to a .sql file path under ./migrations")?;
    let p = Path::new(&mig_file);
    if !p.exists() {
        anyhow::bail!("migration file does not exist: {mig_file}");
    }
    let raw = fs::read_to_string(p)
        .with_context(|| format!("failed to read migration file: {mig_file}"))?;

    let client = connect_postgres_auto(&database_url)
        .await
        .context("tokio-postgres connect failed")?;

    // Split into statements while preserving DO $$ ... $$ blocks.
    let statements = split_sql(&raw);
    println!("applying {} statements from {}", statements.len(), mig_file);
    let print_statements = std::env::var("MIG_PRINT_STATEMENTS")
        .ok()
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false);
    if print_statements {
        for (i, s) in statements.iter().enumerate() {
            println!("--- statement {} ---\n{}\n------------------", i + 1, s);
        }
    }
    for (i, stmt) in statements.iter().enumerate() {
        if stmt.trim().is_empty() {
            continue;
        }
        client.simple_query(stmt).await.with_context(|| {
            format!(
                "failed statement {} in {}\n\nSTATEMENT:\n{}",
                i + 1,
                mig_file,
                stmt
            )
        })?;
        if (i + 1) % 10 == 0 || i + 1 == statements.len() {
            println!("  progress: {}/{}", i + 1, statements.len());
        }
    }
    println!("applied: {mig_file}");
    Ok(())
}

/// Connect to Postgres using TLS by default.
///
/// `tokio_postgres::connect(url, NoTls)` will fail with:
/// "error performing TLS handshake" → "no TLS implementation configured"
/// when the URL requires TLS (e.g., `sslmode=require` on Supabase).
///
/// Selection rules (minimal, drift-tolerant):
/// - If the URL contains `sslmode=...`, it is authoritative.
/// - Otherwise, if env PG_SSLMODE/DB_SSLMODE is set, use that.
/// - If the effective sslmode is "disable", use NoTls; otherwise use rustls TLS.
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

    // Precedence: explicit DSN sslmode > env sslmode > default (TLS).
    let sslmode = match (sslmode_url.as_deref(), sslmode_env.as_deref()) {
        (Some(url_mode), Some(env_mode)) if url_mode != env_mode => {
            tracing::warn!(
                target = "db",
                url_sslmode = url_mode,
                env_sslmode = env_mode,
                "sslmode differs between DSN and env; honoring DSN"
            );
            url_mode.to_string()
        }
        (Some(url_mode), _) => url_mode.to_string(),
        (None, Some(env_mode)) => env_mode.to_string(),
        (None, None) => String::new(),
    };

    if sslmode == "disable" {
        let (client, connection) = tokio_postgres::connect(url, NoTls).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("postgres connection error: {e}");
            }
        });
        return Ok(client);
    }

    let verify_server_cert = matches!(sslmode.as_str(), "verify-ca" | "verify-full");
    if !verify_server_cert {
        tracing::warn!(
            target = "db",
            sslmode = %sslmode,
            "sslmode does not require CA verification; using native-tls with relaxed verification (encryption only)"
        );
    }

    let mut builder = TlsConnector::builder();
    if !verify_server_cert {
        builder.danger_accept_invalid_certs(true);
        builder.danger_accept_invalid_hostnames(true);
    }
    let connector = builder.build()?;
    let tls = MakeTlsConnector::new(connector);
    let (client, connection) = tokio_postgres::connect(url, tls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("postgres connection error: {e}");
        }
    });
    Ok(client)
}

// Robust SQL splitter for Postgres migrations:
// - Preserves DO $$ ... $$ and dollar-quoted function bodies intact
// - Handles single-quoted string literals with escaped ''
// - Ignores semicolons inside line (--) and block (/* ... */) comments
fn split_sql(input: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_single = false;
    let mut in_line_comment = false;
    let mut in_block_comment = false;
    let mut dollar_tag: Option<String> = None;

    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let ch = bytes[i] as char;

        // Handle end of line comment
        if in_line_comment {
            cur.push(ch);
            if ch == '\n' {
                in_line_comment = false;
            }
            i += 1;
            continue;
        }
        // Handle end of block comment
        if in_block_comment {
            // Look for */
            if ch == '*' && i + 1 < bytes.len() && (bytes[i + 1] as char) == '/' {
                cur.push('*');
                cur.push('/');
                i += 2;
                in_block_comment = false;
                continue;
            }
            cur.push(ch);
            i += 1;
            continue;
        }

        // Detect start of comments when not in quotes/dollar
        if !in_single && dollar_tag.is_none() {
            // Line comment --
            if ch == '-' && i + 1 < bytes.len() && (bytes[i + 1] as char) == '-' {
                cur.push('-');
                cur.push('-');
                i += 2;
                in_line_comment = true;
                continue;
            }
            // Block comment /* */
            if ch == '/' && i + 1 < bytes.len() && (bytes[i + 1] as char) == '*' {
                cur.push('/');
                cur.push('*');
                i += 2;
                in_block_comment = true;
                continue;
            }
        }

        // Detect start of dollar-quoted tag when not in single-quote
        if !in_single && dollar_tag.is_none() && ch == '$' {
            // Capture $tag$ or $$
            let mut j = i + 1;
            while j < bytes.len() {
                let cj = bytes[j] as char;
                if cj == '$' {
                    break;
                }
                if cj.is_ascii_alphanumeric() || cj == '_' {
                    j += 1;
                    continue;
                }
                break;
            }
            if j < bytes.len() && (bytes[j] as char) == '$' {
                let tag = &input[i..=j];
                dollar_tag = Some(tag.to_string());
                cur.push_str(tag);
                i = j + 1;
                continue;
            }
        }

        // Inside dollar-quoted body: copy verbatim until closing tag
        if let Some(tag) = &dollar_tag {
            if ch == '$' && input[i..].starts_with(tag) {
                cur.push_str(tag);
                i += tag.len();
                dollar_tag = None;
                continue;
            }
            cur.push(ch);
            i += 1;
            continue;
        }

        // Handle single-quoted string with escaped ''
        if ch == '\'' {
            cur.push(ch);
            if in_single {
                // If next char is also ', it's an escape, consume both without toggling
                if i + 1 < bytes.len() && (bytes[i + 1] as char) == '\'' {
                    cur.push('\'');
                    i += 2;
                    continue;
                } else {
                    in_single = false;
                    i += 1;
                    continue;
                }
            } else {
                in_single = true;
                i += 1;
                continue;
            }
        }

        // Split on semicolon only when not inside any quoted/comment context
        if ch == ';' && !in_single && dollar_tag.is_none() {
            let trimmed = cur.trim();
            if !trimmed.is_empty() {
                out.push(trimmed.to_string());
            }
            cur.clear();
            i += 1;
            continue;
        }

        // Default: append and advance
        cur.push(ch);
        i += 1;
    }

    let trimmed = cur.trim();
    if !trimmed.is_empty() {
        out.push(trimmed.to_string());
    }
    out
}
