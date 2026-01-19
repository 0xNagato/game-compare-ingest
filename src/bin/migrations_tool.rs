//! Advanced migrations management binary.
//! Features:
//! - List migration files (numeric prefix only by default)
//! - Apply all or a range (--from/--to) with optional dry-run
//! - Apply single file (--file)
//! - Ensure `_sqlx_migrations` checksum column exists
//! - Verify core policies (--verify-policies)
//! - Verify schema tables (--verify-schema)
//! Uses simple_query (tokio-postgres) to remain PgBouncer + DO $$ safe.

use anyhow::{anyhow, Context, Result};
use i_miss_rust::util::env::{db_url_prefer_session, preflight_check};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};
use tokio_postgres::NoTls;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    i_miss_rust::util::env::bootstrap_cli("migrations_tool");
    let _ = dotenv::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        .compact()
        .init();

    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut flags = Flags::default();
    parse_flags(&args, &mut flags)?;

    preflight_check(
        "migrations_tool",
        &["SUPABASE_DB_SESSION_URL", "SUPABASE_DB_URL", "DATABASE_URL"],
        &["SUPABASE_DB_SESSION_URL", "SUPABASE_DB_URL", "DATABASE_URL"],
    )
    .ok();
    let database_url = db_url_prefer_session()?;

    let mig_dir = Path::new("./migrations");
    if !mig_dir.exists() {
        return Err(anyhow!("migrations directory missing"));
    }

    let entries = collect_migrations(mig_dir, flags.include_all)?;
    if flags.list {
        for m in &entries {
            println!("{}", m.file_name().unwrap().to_string_lossy());
        }
        return Ok(());
    }

    let (client, connection) = tokio_postgres::connect(&database_url, NoTls)
        .await
        .context("connect failed")?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });

    ensure_sqlx_checksum(&client).await?;

    // Apply only unmigrated (pending) migrations if requested
    if flags.apply_unmigrated {
        let pending = pending_migrations(&client, &entries).await?;
        info!(count = pending.len(), "pending (unmigrated) files to apply");
        apply_and_record(&client, pending, flags.dry_run, flags.fail_fast).await?;
        post_verify(&client, &flags).await?;
        return Ok(());
    }

    // Single file override
    if let Some(file) = flags.single_file.as_ref() {
        let path = mig_dir.join(file);
        if !path.exists() {
            return Err(anyhow!("specified file not found: {}", file));
        }
        apply_file(&client, &path, flags.dry_run).await?;
        post_verify(&client, &flags).await?;
        return Ok(());
    }

    let selected = select_range(entries, flags.from.as_deref(), flags.to.as_deref())?;
    info!(count=selected.len(), from=?flags.from, to=?flags.to, dry_run=flags.dry_run, "applying migration set");

    apply_and_record(&client, selected, flags.dry_run, flags.fail_fast).await?;

    post_verify(&client, &flags).await?;
    info!("migrations completed cleanly");
    Ok(())
}

#[derive(Default)]
struct Flags {
    list: bool,
    include_all: bool,
    dry_run: bool,
    fail_fast: bool,
    from: Option<String>,
    to: Option<String>,
    single_file: Option<String>,
    verify_policies: bool,
    verify_schema: bool,
    apply_unmigrated: bool,
}

fn parse_flags(args: &[String], f: &mut Flags) -> Result<()> {
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--list" => {
                f.list = true;
            }
            "--all" => {
                f.include_all = true;
            }
            "--dry-run" => {
                f.dry_run = true;
            }
            "--fail-fast" => {
                f.fail_fast = true;
            }
            "--from" => {
                i += 1;
                f.from = args.get(i).cloned();
                if f.from.is_none() {
                    return Err(anyhow!("--from requires value"));
                }
            }
            "--to" => {
                i += 1;
                f.to = args.get(i).cloned();
                if f.to.is_none() {
                    return Err(anyhow!("--to requires value"));
                }
            }
            "--file" => {
                i += 1;
                f.single_file = args.get(i).cloned();
                if f.single_file.is_none() {
                    return Err(anyhow!("--file requires value"));
                }
            }
            "--verify-policies" => {
                f.verify_policies = true;
            }
            "--verify-schema" => {
                f.verify_schema = true;
            }
            "--pending" | "--apply-unmigrated" => {
                f.apply_unmigrated = true;
            }
            other => {
                return Err(anyhow!("unrecognized flag: {}", other));
            }
        }
        i += 1;
    }
    Ok(())
}

fn collect_migrations(dir: &Path, include_all: bool) -> Result<Vec<PathBuf>> {
    let mut files: Vec<PathBuf> = fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.is_file() && p.extension().map(|e| e == "sql").unwrap_or(false))
        .collect();
    if !include_all {
        files.retain(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .map(|s| {
                    s.chars()
                        .next()
                        .map(|c| c.is_ascii_digit())
                        .unwrap_or(false)
                })
                .unwrap_or(false)
        });
    }
    files.sort();
    Ok(files)
}

fn select_range(all: Vec<PathBuf>, from: Option<&str>, to: Option<&str>) -> Result<Vec<PathBuf>> {
    if from.is_none() && to.is_none() {
        return Ok(all);
    }
    let mut out = Vec::new();
    for p in all {
        let name = p.file_name().and_then(|s| s.to_str()).unwrap_or("");
        if let Some(f) = from {
            if name < f {
                continue;
            }
        }
        if let Some(t) = to {
            if name > t {
                continue;
            }
        }
        out.push(p);
    }
    Ok(out)
}

async fn apply_file(client: &tokio_postgres::Client, path: &Path, dry: bool) -> Result<()> {
    let name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("<unknown>");
    let sql = fs::read_to_string(path)?;
    if dry {
        info!(file = name, bytes = sql.len(), "dry-run skip apply");
        return Ok(());
    }
    info!(file = name, "applying migration");
    // Execute entire file via simple_query (handles multi-stmt & DO $$)
    match client.simple_query(&sql).await {
        Ok(_msgs) => {
            info!(file = name, "applied");
            Ok(())
        }
        Err(e) => Err(anyhow!("{} failed: {}", name, e)),
    }
}

fn parse_version_and_description(path: &Path) -> Option<(i64, String)> {
    let name = path.file_name().and_then(|s| s.to_str())?;
    let mut parts = name.splitn(2, '_');
    let ver_str = parts.next()?;
    let rest = parts.next().unwrap_or("");
    if let Ok(v) = ver_str.parse::<i64>() {
        return Some((v, rest.trim_end_matches(".sql").to_string()));
    }
    None
}

async fn pending_migrations(
    client: &tokio_postgres::Client,
    files: &[PathBuf],
) -> Result<Vec<PathBuf>> {
    // Cast version to text to neutralize legacy column type differences and avoid driver deserialization panics.
    let rows = client
        .query(
            "SELECT (version::text) AS vtxt FROM public._sqlx_migrations WHERE success",
            &[],
        )
        .await?;
    let mut existing: std::collections::HashSet<i64> = std::collections::HashSet::new();
    for r in rows {
        let s: String = r.get(0);
        match s.trim().parse::<i64>() {
            Ok(v) => {
                existing.insert(v);
            }
            Err(e) => {
                warn!(raw_version=?s, error=?e, "unparsable version in _sqlx_migrations; skipping row");
            }
        }
    }
    let mut pending = Vec::new();
    for f in files {
        if let Some((ver, _desc)) = parse_version_and_description(f) {
            if !existing.contains(&ver) {
                pending.push(f.clone());
            }
        }
    }
    Ok(pending)
}

async fn apply_and_record(
    client: &tokio_postgres::Client,
    files: Vec<PathBuf>,
    dry: bool,
    fail_fast: bool,
) -> Result<()> {
    let mut failures: Vec<(String, String)> = Vec::new();
    for path in files {
        let name = path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("<unknown>");
        let (version, description) = match parse_version_and_description(&path) {
            Some(v) => v,
            None => {
                warn!(file = name, "skipping file without numeric prefix");
                continue;
            }
        };
        let sql = std::fs::read_to_string(&path)?;
        let checksum = Sha256::digest(sql.as_bytes());
        if dry {
            info!(
                file = name,
                version,
                bytes = sql.len(),
                "dry-run skip apply"
            );
            continue;
        }
        info!(file = name, version, "applying migration");
        let mut success = true;
        if let Err(e) = client.simple_query(&sql).await {
            let es = e.to_string();
            // Treat duplicate object as success (idempotent)
            if es.contains("duplicate object") || es.contains("already exists") {
                warn!(file=name, error=?es, "idempotent duplicate; marking success");
            } else {
                error!(file=name, error=?es, "migration failed");
                success = false;
                failures.push((name.to_string(), es));
                if fail_fast {
                    break;
                }
            }
        }
        record_migration(client, version, &description, success, &checksum).await?;
    }
    if !failures.is_empty() {
        return Err(anyhow!("migration failures encountered: {:?}", failures));
    }
    Ok(())
}

async fn record_migration(
    client: &tokio_postgres::Client,
    version: i64,
    description: &str,
    success: bool,
    checksum: &[u8],
) -> Result<()> {
    client.execute(
        "INSERT INTO public._sqlx_migrations (version, description, success, checksum) VALUES ($1,$2,$3,$4)
         ON CONFLICT (version) DO UPDATE SET success=EXCLUDED.success, checksum=EXCLUDED.checksum",
        &[&version, &description, &success, &checksum]
    ).await?;
    Ok(())
}

async fn ensure_sqlx_checksum(client: &tokio_postgres::Client) -> Result<()> {
    // Cast to text to avoid driver-specific OID/name deserialization issues
    let row = client
        .query_one("SELECT to_regclass('_sqlx_migrations')::text", &[])
        .await?;
    let reg: Option<String> = row.get(0);
    if reg.is_none() {
        info!("creating _sqlx_migrations table (missing)");
        client.simple_query(
            "CREATE TABLE IF NOT EXISTS public._sqlx_migrations (version BIGINT PRIMARY KEY, description TEXT NOT NULL, installed_on TIMESTAMPTZ NOT NULL DEFAULT now(), success BOOLEAN NOT NULL, checksum BYTEA)"
        ).await?;
        return Ok(());
    }
    let col_row = client.query(
        "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='_sqlx_migrations'",
        &[]
    ).await?;
    let mut has_checksum = false;
    for r in col_row {
        let c: String = r.get(0);
        if c == "checksum" {
            has_checksum = true;
            break;
        }
    }
    if !has_checksum {
        info!("adding checksum column to _sqlx_migrations");
        client
            .simple_query(
                "ALTER TABLE public._sqlx_migrations ADD COLUMN IF NOT EXISTS checksum BYTEA",
            )
            .await?;
    }
    Ok(())
}

async fn post_verify(client: &tokio_postgres::Client, flags: &Flags) -> Result<()> {
    if flags.verify_schema {
        verify_schema_tables(client).await?;
    }
    if flags.verify_policies {
        verify_policies(client).await?;
    }
    Ok(())
}

async fn verify_schema_tables(client: &tokio_postgres::Client) -> Result<()> {
    let expected = [
        "products",
        "software",
        "hardware",
        "video_game_titles",
        "video_games",
        "platforms",
        "countries",
        "currencies",
        "jurisdictions",
        "tax_rules",
        "sellables",
        "retailers",
        "offers",
        "offer_jurisdictions",
        "prices",
        "current_price",
        "users",
        "alerts",
    ];
    let rows = client
        .query(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='public'",
            &[],
        )
        .await?;
    let mut present = std::collections::HashSet::new();
    for r in rows {
        let t: String = r.get(0);
        present.insert(t);
    }
    let mut missing = Vec::new();
    for t in expected {
        if !present.contains(t) {
            missing.push(t);
        }
    }
    if missing.is_empty() {
        info!("schema verify ok (all expected tables present)");
    } else {
        warn!(missing=?missing, "schema verify missing tables");
    }
    Ok(())
}

async fn verify_policies(client: &tokio_postgres::Client) -> Result<()> {
    let expected = [
        "retailer_providers_select_for_members",
        "retailer_providers_no_insert_for_authenticated",
        "retailer_providers_no_update_for_authenticated",
        "retailer_providers_no_delete_for_authenticated",
        "currencies_select",
        "countries_select",
        "jurisdictions_select",
        "tax_rules_select",
    ];
    let rows = client
        .query(
            "SELECT policyname FROM pg_policies WHERE schemaname='public'",
            &[],
        )
        .await?;
    let mut present = std::collections::HashSet::new();
    for r in rows {
        let p: String = r.get(0);
        present.insert(p);
    }
    let mut missing = Vec::new();
    for p in expected {
        if !present.contains(p) {
            missing.push(p);
        }
    }
    if missing.is_empty() {
        info!("policy verify ok (all expected policies present)");
    } else {
        warn!(missing=?missing, "policy verify missing policies");
    }
    Ok(())
}
