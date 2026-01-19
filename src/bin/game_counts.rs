use anyhow::{Context, Result};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions, PgSslMode};
use std::str::FromStr;

#[tokio::main]
async fn main() -> Result<()> {
    i_miss_rust::util::env::bootstrap_cli("game_counts");

    let _ = dotenv::dotenv();
    // Prefer the project's resolver so we support SUPABASE_IPV6_DB + hostaddr overrides.
    let db_url = i_miss_rust::util::env::db_url().context("missing database URL env vars")?;
    let mut opts = PgConnectOptions::from_str(&db_url)?.statement_cache_capacity(0);

    // Ensure TLS is enabled when DSN contains sslmode=require
    if db_url.contains("sslmode=require") && !db_url.contains("sslmode=disable") {
        opts = opts.ssl_mode(PgSslMode::Require);
    }

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(opts)
        .await?;

    async fn count_table(pool: &sqlx::PgPool, table: &str) -> Result<Option<i64>> {
        let exists: Option<String> = sqlx::query_scalar("SELECT to_regclass($1)::text")
            .bind(table)
            .persistent(false)
            .fetch_one(pool)
            .await?;
        if exists.is_none() {
            return Ok(None);
        }

        // Safe because callers only pass hard-coded identifiers.
        let sql = format!("SELECT count(*) FROM {table}");
        let n: i64 = sqlx::query_scalar(&sql)
            .persistent(false)
            .fetch_one(pool)
            .await?;
        Ok(Some(n))
    }

    for table in [
        "public.video_game_titles",
        "public.video_games",
        // Laravel schema targets
        "public.sku_regions",
        "public.region_prices",
    ] {
        match count_table(&pool, table).await? {
            Some(n) => println!("{table}={n}"),
            None => println!("{table}=<missing>"),
        }
    }
    Ok(())
}
