use anyhow::{anyhow, bail, Context, Result};
use sqlx::postgres::PgPoolOptions;
use sqlx::{migrate::Migrator, PgPool};
use std::{env, fs, path::Path};
use tracing::{error, info, Level};
use tracing_subscriber::EnvFilter;

static MIGRATIONS: Migrator = sqlx::migrate!("./migrations");

#[tokio::main]
async fn main() -> Result<()> {
    i_miss_rust::util::env::bootstrap_cli("db_reset_migrate");
    // Logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive(Level::INFO.into()))
        .try_init();

    let db_url = env::var("DATABASE_URL")
        .context("DATABASE_URL must be set (Supabase service role URL recommended)")?;
    let confirm = env::var("I_UNDERSTAND").unwrap_or_default();

    if confirm.to_ascii_lowercase() != "drop" {
        bail!("Refusing to drop schema. Set I_UNDERSTAND=drop to proceed.");
    }

    // Optional: allow pointing to a different migrations dir via env (used only when MIG_FILE is not set)
    let mig_dir = env::var("MIGRATIONS_DIR").unwrap_or_else(|_| "./migrations".to_string());
    if !Path::new(&mig_dir).exists() {
        bail!("Migrations directory not found: {}", mig_dir);
    }

    // Optional: single-file consolidated migration
    let mig_file = env::var("MIG_FILE").ok();

    info!("connecting url {:?}", &db_url);
    let mut builder = PgPoolOptions::new().max_connections(5);
    // Allow disabling statement cache for PgBouncer transaction mode
    if env::var("DISABLE_STMT_CACHE").unwrap_or_default() == "1" {
        info!("statement_cache" = "disabled");
    }
    let pool = builder
        .connect(&db_url)
        .await
        .context("Failed to connect to database")?;

    // Drop views first (including materialized), then tables (including partitioned parents)
    info!("step" = "dropping views (public)");
    drop_views(&pool).await?;

    info!("step" = "dropping tables (public)");
    drop_tables(&pool).await?;

    // Clear prepared statements between phases to avoid name collisions in PgBouncer
    if let Err(e) = sqlx::query("DEALLOCATE ALL;").execute(&pool).await {
        error!("deallocate_all_error" = %e);
    }

    info!("step" = "dropping enum types (public)");
    drop_enum_types(&pool).await?;

    // Drop the sqlx migrations table to force fresh migration run
    info!("step" = "clearing sqlx migration state");
    // First try to drop the table entirely
    if let Err(e) = sqlx::query("DROP TABLE IF EXISTS _sqlx_migrations CASCADE;")
        .execute(&pool)
        .await
    {
        // If drop fails, try to truncate it instead
        if let Err(e2) = sqlx::query("TRUNCATE TABLE _sqlx_migrations CASCADE;")
            .execute(&pool)
            .await
        {
            // If both fail, try to delete all rows
            if let Err(e3) = sqlx::query("DELETE FROM _sqlx_migrations;")
                .execute(&pool)
                .await
            {
                error!("clear_migrations_table_error" = %e, "truncate_error" = %e2, "delete_error" = %e3);
            } else {
                info!("cleared _sqlx_migrations via DELETE");
            }
        } else {
            info!("cleared _sqlx_migrations via TRUNCATE");
        }
    } else {
        info!("dropped _sqlx_migrations table");
    }

    // Clear requested schemas (ext and cg_pngr_tm), then recreate ext
    info!("step" = "dropping schemas ext, cg_pngr_tm (if exist)");
    drop_extra_schemas(&pool).await?;
    info!("step" = "recreating ext schema");
    sqlx::query("CREATE SCHEMA IF NOT EXISTS ext;")
        .execute(&pool)
        .await
        .context("failed to create ext schema")?;

    // Apply either MIG_FILE (single consolidated file) or the directory migrator
    if let Some(file) = mig_file {
        let p = Path::new(&file);
        if !p.exists() {
            bail!("MIG_FILE does not exist: {}", file);
        }
        info!("step" = "applying single migration file", "file" = %file);
        let raw =
            fs::read_to_string(p).with_context(|| format!("failed to read MIG_FILE: {}", file))?;
        let statements = split_sql(&raw);
        for (i, stmt) in statements.iter().enumerate() {
            let s = stmt.trim();
            if s.is_empty() {
                continue;
            }
            if let Err(e) = sqlx::query(s).execute(&pool).await {
                error!("apply_stmt_error" = %e, "i" = i + 1, "stmt" = %s);
                return Err(anyhow!("failed at statement {} in {}: {}", i + 1, file, e));
            }
            if (i + 1) % 10 == 0 || i + 1 == statements.len() {
                info!("progress" = format!("{}/{}", i + 1, statements.len()));
            }
        }
        info!(
            "status" = "ok",
            "message" = "Reset + single-file apply complete"
        );
    } else {
        info!("step" = "running migrations", "dir" = %mig_dir);
        MIGRATIONS.run(&pool).await.context("Migrations failed")?;
        info!(
            "status" = "ok",
            "message" = "Reset + directory migrations complete"
        );
    }
    Ok(())
}

async fn drop_views(pool: &PgPool) -> Result<()> {
    // relkind 'v' = view, 'm' = materialized view
    let stmts: Vec<(String,)> = sqlx::query_as(
        r#"
        SELECT format('DROP VIEW IF EXISTS %I.%I CASCADE;', n.nspname, c.relname)
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relkind IN ('v','m')
        ORDER BY 1;
        "#,
    )
    .fetch_all(pool)
    .await?;

    for (ddl,) in stmts {
        if let Err(e) = sqlx::query(&ddl).execute(pool).await {
            error!("drop_view_error" = %e, "ddl" = %ddl);
        }
    }
    Ok(())
}

async fn drop_tables(pool: &PgPool) -> Result<()> {
    // relkind 'r' = ordinary table, 'p' = partitioned table
    let stmts: Vec<(String,)> = sqlx::query_as(
        r#"
                WITH partitions AS (
                    SELECT inhrelid::regclass AS child, inhparent::regclass AS parent
                    FROM pg_inherits
                )
                SELECT format('DROP TABLE IF EXISTS %I.%I CASCADE;', n.nspname, c.relname)
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                LEFT JOIN partitions p ON p.child = c.oid
                WHERE n.nspname = 'public'
                    AND c.relkind IN ('r','p')
                ORDER BY (p.child IS NOT NULL) DESC, 1;
                "#,
    )
    .fetch_all(pool)
    .await?;

    for (ddl,) in stmts {
        if let Err(e) = sqlx::query(&ddl).execute(pool).await {
            error!("drop_table_error" = %e, "ddl" = %ddl);
        }
    }
    Ok(())
}

async fn drop_enum_types(pool: &PgPool) -> Result<()> {
    // Drop public enum types after tables are gone
    let stmts: Vec<(String,)> = sqlx::query_as(
        r#"
        SELECT format('DROP TYPE IF EXISTS %I.%I CASCADE;', n.nspname, t.typname)
        FROM pg_type t
        JOIN pg_namespace n ON n.oid = t.typnamespace
        WHERE n.nspname = 'public' AND t.typtype = 'e'
        ORDER BY 1;
        "#,
    )
    .fetch_all(pool)
    .await?;

    for (ddl,) in stmts {
        if let Err(e) = sqlx::query(&ddl).execute(pool).await {
            error!("drop_type_error" = %e, "ddl" = %ddl);
        }
    }
    Ok(())
}

async fn drop_extra_schemas(pool: &PgPool) -> Result<()> {
    // Drop ext and cg_pngr_tm schemas if they exist (CASCADE)
    let schemas = ["ext", "cg_pngr_tm"];
    for s in schemas.iter() {
        let ddl = format!("DROP SCHEMA IF EXISTS {} CASCADE;", s);
        if let Err(e) = sqlx::query(&ddl).execute(pool).await {
            error!("drop_schema_error" = %e, "ddl" = %ddl);
        }
    }
    Ok(())
}

fn redact(url: &str) -> String {
    // Best-effort hide password/token
    if let Some(idx) = url.find("@") {
        let (left, right) = url.split_at(idx);
        let left = left.replace(&left, "***:***");
        format!("{}{}", left, right)
    } else {
        "***".into()
    }
}

// Copy of the robust SQL splitter from migrate_one.rs (kept local to avoid new module wiring)
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

        if in_line_comment {
            cur.push(ch);
            if ch == '\n' {
                in_line_comment = false;
            }
            i += 1;
            continue;
        }
        if in_block_comment {
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

        if !in_single && dollar_tag.is_none() {
            if ch == '-' && i + 1 < bytes.len() && (bytes[i + 1] as char) == '-' {
                cur.push('-');
                cur.push('-');
                i += 2;
                in_line_comment = true;
                continue;
            }
            if ch == '/' && i + 1 < bytes.len() && (bytes[i + 1] as char) == '*' {
                cur.push('/');
                cur.push('*');
                i += 2;
                in_block_comment = true;
                continue;
            }
        }

        if !in_single && dollar_tag.is_none() && ch == '$' {
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

        if ch == '\'' {
            cur.push(ch);
            if in_single {
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

        if ch == ';' && !in_single && dollar_tag.is_none() {
            let trimmed = cur.trim();
            if !trimmed.is_empty() {
                out.push(trimmed.to_string());
            }
            cur.clear();
            i += 1;
            continue;
        }

        cur.push(ch);
        i += 1;
    }
    let trimmed = cur.trim();
    if !trimmed.is_empty() {
        out.push(trimmed.to_string());
    }
    out
}
