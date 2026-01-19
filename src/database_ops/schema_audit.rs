use std::fmt::Write as _;
use std::io::{self, Write};

use anyhow::{Context, Result};
use sqlx::pool::PoolConnection;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Pool, Postgres, Row};
use tracing::warn;

use crate::util::env as envutil;

type PgConn = PoolConnection<Postgres>;

#[derive(Debug, Clone, Default)]
pub struct DbSchemaAuditConfig {
    pub database_url: Option<String>,
    pub table_filter: Option<Vec<String>>,
    pub max_connections: Option<u32>,
}

pub async fn run(cfg: DbSchemaAuditConfig) -> Result<()> {
    envutil::init_env();
    init_tracing();

    safe_println("[schema_audit] starting");
    let raw_url = if let Some(url) = cfg.database_url.clone() {
        url
    } else {
        envutil::db_url_prefer_session()?
    };
    if raw_url.contains(":5432") {
        warn!("using session pooler DB URL for schema audit");
    }

    let connect_opts = raw_url
        .parse::<PgConnectOptions>()?
        .statement_cache_capacity(0);

    let max_conns = cfg
        .max_connections
        .or_else(|| envutil::env_opt("SCHEMA_AUDIT_MAX_CONNS").and_then(|v| v.parse::<u32>().ok()))
        .unwrap_or(4);

    let pool: Pool<Postgres> = PgPoolOptions::new()
        .max_connections(max_conns)
        .after_connect(|conn, _| {
            Box::pin(async move {
                let _ = sqlx::query("SET application_name = 'schema_audit'")
                    .persistent(false)
                    .execute(conn)
                    .await;
                Ok(())
            })
        })
        .connect_with(connect_opts)
        .await?;

    let filter = if let Some(f) = cfg.table_filter.clone() {
        Some(f)
    } else {
        envutil::env_opt("TABLE_FILTER").map(|raw| {
            raw.split(',')
                .map(|t| t.trim().to_ascii_lowercase())
                .filter(|t| !t.is_empty())
                .collect::<Vec<_>>()
        })
    };

    let mut tables = vec![
        "products",
        "software",
        "hardware",
        "video_game_titles",
        "video_games",
        "sellables",
        "providers",
        "provider_items",
        "provider_offers",
        "vg_source_media_links",
        "game_media",
        "offers",
        "offer_jurisdictions",
        "prices",
        "current_price",
        "alerts",
        "countries",
        "jurisdictions",
        "tax_rules",
        "platforms",
        "users",
    ];
    if let Some(f) = &filter {
        tables.retain(|t| f.contains(&t.to_string()));
    }

    let mut out = String::new();
    writeln!(
        out,
        "SCHEMA AUDIT (public) — total tables: {}",
        tables.len()
    )
    .ok();

    for table in tables {
        let mut conn = pool.acquire().await?;

        let exists: Option<String> = sqlx::query_scalar("SELECT to_regclass($1)::text")
            .bind(format!("public.{table}"))
            .persistent(false)
            .fetch_optional(conn.as_mut())
            .await?;

        if exists.is_none() {
            writeln!(out, "\n=== {table} ===\n  (missing: not present in schema)").ok();
            safe_println(&out);
            continue;
        }

        writeln!(out, "\n=== {table} ===").ok();
        emit_columns(&mut out, table, &mut conn).await?;
        emit_pk(&mut out, table, &mut conn).await?;
        emit_unique_constraints(&mut out, table, &mut conn).await?;
        emit_unique_indexes(&mut out, table, &mut conn).await?;
        emit_foreign_keys_out(&mut out, table, &mut conn).await?;
        emit_foreign_keys_in(&mut out, table, &mut conn).await?;
        emit_checks(&mut out, table, &mut conn).await?;

        safe_println(&out);
    }

    std::fs::write("schema_audit.out", &out).context("failed to write schema_audit.out")?;
    safe_println(&out);
    Ok(())
}

async fn emit_columns(out: &mut String, table: &str, conn: &mut PgConn) -> Result<()> {
    let rows = sqlx
        ::query(
            "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema='public' AND table_name=$1 ORDER BY ordinal_position"
        )
        .bind(table)
        .persistent(false)
        .fetch_all(conn.as_mut()).await?;

    writeln!(out, "Columns (name:type:null):").ok();
    for row in rows {
        let name: String = row.get("column_name");
        let dt: String = row.get("data_type");
        let nul: String = row.get("is_nullable");
        writeln!(
            out,
            "  {name}:{dt}:{}",
            if nul == "YES" { "null" } else { "not-null" }
        )
        .ok();
    }
    Ok(())
}

async fn emit_pk(out: &mut String, table: &str, conn: &mut PgConn) -> Result<()> {
    let rows = sqlx::query(
        r#"SELECT c.conname, pg_get_constraintdef(c.oid) AS def
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname='public' AND t.relname=$1 AND c.contype='p'"#,
    )
    .bind(table)
    .persistent(false)
    .fetch_all(conn.as_mut())
    .await?;

    if rows.is_empty() {
        writeln!(out, "Primary Key: (none)").ok();
    } else {
        for row in rows {
            let name: String = row.get("conname");
            let def: String = row.get("def");
            writeln!(out, "Primary Key: {name} {def}").ok();
        }
    }
    Ok(())
}

async fn emit_unique_constraints(out: &mut String, table: &str, conn: &mut PgConn) -> Result<()> {
    let rows = sqlx::query(
        r#"SELECT c.conname, pg_get_constraintdef(c.oid) AS def
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname='public' AND t.relname=$1 AND c.contype='u'"#,
    )
    .bind(table)
    .persistent(false)
    .fetch_all(conn.as_mut())
    .await?;

    if rows.is_empty() {
        writeln!(out, "Unique Constraints: (none)").ok();
    } else {
        writeln!(out, "Unique Constraints:").ok();
        for row in rows {
            let name: String = row.get("conname");
            let def: String = row.get("def");
            writeln!(out, "  {name} {def}").ok();
        }
    }
    Ok(())
}

async fn emit_unique_indexes(out: &mut String, table: &str, conn: &mut PgConn) -> Result<()> {
    let rows = sqlx::query(
        r#"SELECT indexname, indexdef
        FROM pg_indexes
        WHERE schemaname='public' AND tablename=$1 AND indexdef ILIKE '%UNIQUE%'
        ORDER BY indexname"#,
    )
    .bind(table)
    .persistent(false)
    .fetch_all(conn.as_mut())
    .await?;

    if rows.is_empty() {
        writeln!(out, "Unique Indexes: (none)").ok();
    } else {
        writeln!(out, "Unique Indexes:").ok();
        for row in rows {
            let name: String = row.get("indexname");
            let def: String = row.get("indexdef");
            writeln!(out, "  {name}: {def}").ok();
        }
    }
    Ok(())
}

async fn emit_foreign_keys_out(out: &mut String, table: &str, conn: &mut PgConn) -> Result<()> {
    let rows = sqlx::query(
        r#"SELECT c.conname, pg_get_constraintdef(c.oid) AS def
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname='public' AND t.relname=$1 AND c.contype='f'"#,
    )
    .bind(table)
    .persistent(false)
    .fetch_all(conn.as_mut())
    .await?;

    if rows.is_empty() {
        writeln!(out, "Foreign Keys (outgoing): (none)").ok();
    } else {
        writeln!(out, "Foreign Keys (outgoing):").ok();
        for row in rows {
            let name: String = row.get("conname");
            let def: String = row.get("def");
            writeln!(out, "  {name} {def}").ok();
        }
    }
    Ok(())
}

async fn emit_foreign_keys_in(out: &mut String, table: &str, conn: &mut PgConn) -> Result<()> {
    let rows = sqlx::query(
        r#"SELECT c.conname, pg_get_constraintdef(c.oid) AS def, rel.relname AS referencing_table
        FROM pg_constraint c
        JOIN pg_class rel ON rel.oid = c.conrelid
        JOIN pg_namespace nrel ON nrel.oid = rel.relnamespace
        WHERE c.contype='f' AND c.confrelid = $1::regclass AND nrel.nspname='public'"#,
    )
    .bind(format!("public.{table}"))
    .persistent(false)
    .fetch_all(conn.as_mut())
    .await?;

    if rows.is_empty() {
        writeln!(out, "Foreign Keys (incoming): (none)").ok();
    } else {
        writeln!(out, "Foreign Keys (incoming):").ok();
        for row in rows {
            let referencing: String = row.get("referencing_table");
            let name: String = row.get("conname");
            let def: String = row.get("def");
            writeln!(out, "  {referencing}: {name} {def}").ok();
        }
    }
    Ok(())
}

async fn emit_checks(out: &mut String, table: &str, conn: &mut PgConn) -> Result<()> {
    let rows = sqlx::query(
        r#"SELECT c.conname, pg_get_constraintdef(c.oid) AS def
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname='public' AND t.relname=$1 AND c.contype='c'"#,
    )
    .bind(table)
    .persistent(false)
    .fetch_all(conn.as_mut())
    .await?;

    if rows.is_empty() {
        writeln!(out, "Check Constraints: (none)").ok();
    } else {
        writeln!(out, "Check Constraints:").ok();
        for row in rows {
            let name: String = row.get("conname");
            let def: String = row.get("def");
            writeln!(out, "  {name} {def}").ok();
        }
    }
    Ok(())
}

fn init_tracing() {
    if !tracing::dispatcher::has_been_set() {
        let filter = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into());
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_target(false)
            .compact()
            .init();
    }
}

fn safe_println(s: &str) {
    let mut stdout = io::stdout();
    let _ = writeln!(stdout, "{}", s);
}
