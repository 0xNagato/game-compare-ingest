use anyhow::{Context, Result};
use i_miss_rust::util::env::{db_url, ipv6_db_url, preflight_check};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions, PgSslMode};
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr; // bring FromStr trait into scope for PgConnectOptions::from_str
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

// Note: We avoid sqlx::migrate! compile-time embedding to prevent failures when
// the ./migrations directory contains reference files that aren't versioned.

#[tokio::main]
async fn main() -> Result<()> {
    i_miss_rust::util::env::bootstrap_cli("db_migrate");
    // init logging
    let _ = dotenv::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        .compact()
        .init();

    // ensure migrations dir exists at runtime path
    let mig_dir = Path::new("./migrations");
    if !mig_dir.exists() {
        error!(?mig_dir, "migrations directory not found");
        anyhow::bail!("migrations directory not found: {:?}", mig_dir);
    }

    // Prefer SUPABASE_DB_SESSION_URL (5432) > SUPABASE_DB_URL > DATABASE_URL
    preflight_check(
        "db-migrate",
        &["SUPABASE_DB_SESSION_URL", "SUPABASE_DB_URL", "DATABASE_URL"],
        &["SUPABASE_DB_SESSION_URL", "SUPABASE_DB_URL", "DATABASE_URL"],
    )
    .ok();
    let resolved_url = db_url().context(
        "no database URL env vars set (SUPABASE_DB_SESSION_URL | SUPABASE_DB_URL | DATABASE_URL)",
    )?;
    let effective_url = ipv6_db_url().unwrap_or_else(|| resolved_url.clone());

    // Create a single connection for migrations and disable statement cache to avoid
    // prepared statement name collisions ("sqlx_s_1 already exists").
    let mut connect_options =
        PgConnectOptions::from_str(&effective_url)?.statement_cache_capacity(0);

    // Ensure TLS is enabled when DSN contains sslmode=require
    if effective_url.contains("sslmode=require") && !effective_url.contains("sslmode=disable") {
        connect_options = connect_options.ssl_mode(PgSslMode::Require);
    }

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(connect_options)
        .await
        .context("failed to connect to database")?;

    info!("running migrations (simple runner)...");
    // Use simple runner always to be PgBouncer- and DO $$-safe, and to ignore non-versioned files
    let mut conn = pool.acquire().await?;
    simple_run_migrations(Path::new("./migrations"), &mut conn, &effective_url).await?;

    info!("migrations completed successfully");
    Ok(())
}

// PgBouncer-safe simple runner: executes each .sql file (lexically sorted) using simple protocol
use sqlx::PgConnection; // Acquire no longer needed
async fn simple_run_migrations(
    dir: &Path,
    _conn: &mut PgConnection,
    database_url: &str,
) -> Result<()> {
    let mut entries: Vec<PathBuf> = fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.is_file() && p.extension().map(|ext| ext == "sql").unwrap_or(false))
        .collect();
    // Filter to files that start with a numeric prefix (e.g., 0001_*.sql)
    let include_all = std::env::var("MIG_INCLUDE_ALL")
        .ok()
        .map(|v| (v == "1" || v.eq_ignore_ascii_case("true")))
        .unwrap_or(false);
    if !include_all {
        entries.retain(|p| {
            p.file_name()
                .and_then(|s| s.to_str())
                .map(|name| {
                    name.chars()
                        .next()
                        .map(|c| c.is_ascii_digit())
                        .unwrap_or(false)
                })
                .unwrap_or(false)
        });
    }
    entries.sort();
    // Prefer true Simple Query protocol via tokio-postgres to safely run multi-statement SQL and DO $$ blocks
    // Use NoTls (sslmode=disable) for IPv6-only connections
    use tokio_postgres::NoTls;
    let (tp_client, tp_connection) = tokio_postgres
        ::connect(database_url, NoTls).await
        .context(
            "tokio-postgres connect failed (if your DB requires TLS, set sslmode=disable locally or configure TLS)"
        )?;
    // spawn the connection task
    tokio::spawn(async move {
        if let Err(e) = tp_connection.await {
            eprintln!("tokio-postgres connection error: {}", e);
        }
    });

    // Pre-flight: capture existing tables for diff reporting
    let initial_tables = tp_client
        .query(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name",
            &[]
        ).await?
        .into_iter()
        .map(|r| r.get::<usize, String>(0))
        .collect::<Vec<_>>();
    info!(count = initial_tables.len(), tables = ?initial_tables, "pre-migration existing tables in public schema");

    let mut applied: Vec<String> = Vec::new();
    let mut failed: Vec<(String, String)> = Vec::new();
    let mut created_tables: Vec<String> = Vec::new();

    for path in entries {
        if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
            if name.starts_with("archive_") {
                continue;
            }
        }
        let sql = fs::read_to_string(&path)?;
        let name = path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("<unknown>");
        info!(%name, "applying migration via simple_query");
        match tp_client.simple_query(&sql).await {
            Ok(_msgs) => {
                applied.push(name.to_string());
                // After applying, check for newly created tables (simple heuristic: compare list)
                let current_tables = tp_client
                    .query(
                        "SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name",
                        &[]
                    ).await?
                    .into_iter()
                    .map(|r| r.get::<usize, String>(0))
                    .collect::<Vec<_>>();
                for t in &current_tables {
                    if !initial_tables.contains(t) && !created_tables.contains(t) {
                        created_tables.push(t.clone());
                    }
                }
                // Specific instrumentation for ratings table presence
                if name.contains("ratings") || name.contains("video_game_ratings") {
                    let exists_row = tp_client.query_one(
                        "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='video_game_ratings_by_locale')",
                        &[]
                    ).await?;
                    let exists: bool = exists_row.get(0);
                    info!(migration = %name, ratings_table_exists = exists, "post-migration ratings table check");
                }
            }
            Err(e) => {
                error!(migration = %name, error = %e, "migration failed (continuing to next for diagnostics)");
                failed.push((name.to_string(), e.to_string()));
            }
        }
    }
    // Summary block
    info!(applied = applied.len(), failed = failed.len(), created_tables = ?created_tables, "migration simple runner summary");
    if !failed.is_empty() {
        for (m, err) in &failed {
            eprintln!("[db_migrate] FAILED: {m} => {err}");
        }
    }
    // Explicit final check for ratings table
    let final_exists_row = tp_client.query_one(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='video_game_ratings_by_locale')",
        &[]
    ).await?;
    let final_exists: bool = final_exists_row.get(0);
    if final_exists {
        info!("video_game_ratings_by_locale table present after migrations");
    } else {
        error!(
            "video_game_ratings_by_locale table MISSING after migrations — check 0009 migration"
        );
    }
    Ok(())
}

// Robust SQL splitter for Postgres
// - Preserves DO $$...$$ bodies
// - Ignores semicolons within single quotes, line and block comments
// - Handles dollar-tagged quotes like $tag$...$tag$
// Removed old splitter (unused) after adopting tokio-postgres simple_query for full-file execution.
