use anyhow::{Context, Result};
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions, PgSslMode},
    Row,
};
use std::str::FromStr;

#[tokio::main]
async fn main() -> Result<()> {
    i_miss_rust::util::env::bootstrap_cli("rls_check");
    let _ = dotenv::dotenv();
    let database_url = std::env::var("DATABASE_URL").context("DATABASE_URL not set")?;
    // Disable statement cache for PgBouncer compatibility
    let mut connect_opts = PgConnectOptions::from_str(&database_url)?.statement_cache_capacity(0);

    // Ensure TLS is enabled when DSN contains sslmode=require
    if database_url.contains("sslmode=require") && !database_url.contains("sslmode=disable") {
        connect_opts = connect_opts.ssl_mode(PgSslMode::Require);
    }

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(connect_opts)
        .await?;

    println!("== RLS Table Flags ==");
    let rows = sqlx::query(
        r#"SELECT c.relname, c.relrowsecurity, c.relforcerowsecurity
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname='gamecompare'
              AND c.relkind='r'
              AND c.relname = ANY($1)"#,
    )
    .bind(&[
        "users",
        "alerts",
        "offers",
        "offer_jurisdictions",
        "current_price",
    ])
    .persistent(false)
    .fetch_all(&pool)
    .await?;
    for r in rows {
        let name: String = r.try_get(0)?;
        let rls: bool = r.try_get(1)?;
        let force: bool = r.try_get(2)?;
        println!("table={name} rls={rls} force={force}");
    }

    println!("\n== Policies ==");
    let policies = sqlx::query(
        r#"SELECT p.polname, c.relname
            FROM pg_policy p
            JOIN pg_class c ON c.oid = p.polrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname='gamecompare'
              AND c.relname = ANY($1)
            ORDER BY c.relname, p.polname"#,
    )
    .bind(&[
        "users",
        "alerts",
        "offers",
        "offer_jurisdictions",
        "current_price",
    ])
    .persistent(false)
    .fetch_all(&pool)
    .await?;
    for p in policies {
        let pname: String = p.try_get(0)?;
        let tname: String = p.try_get(1)?;
        println!("policy={pname} table={tname}");
    }

    println!("\nRLS verification complete.");
    Ok(())
}
