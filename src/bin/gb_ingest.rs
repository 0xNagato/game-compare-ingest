use anyhow::{Context, Result};
use chrono::Utc;
use i_miss_rust::database_ops::{db::Db, giantbomb::ingest::ingest_from_file};
use i_miss_rust::util::env;
use sqlx::{postgres::PgPoolOptions, Row};

#[tokio::main]
async fn main() -> Result<()> {
    env::bootstrap_cli("gb_ingest");

    // 1) Show which URL we actually used.
    //    (env var order preserved, but we print a redacted version so you know host/port/db/user)
    let db_url = std::env::var("SUPABASE_DB_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .context("SUPABASE_DB_URL or DATABASE_URL must be set")?;

    let redacted = redact_url(&db_url);
    eprintln!("Connecting to: {}", redacted);

    // Optional: if you suspect pooler issues, try disabling the statement cache
    // let mut opts: sqlx::postgres::PgConnectOptions = db_url.parse()?;
    // opts = opts.statement_cache_capacity(0);
    // let pool = PgPoolOptions::new().max_connections(10).connect_with(opts).await?;

    // Your normal Db wrapper
    let db = Db::connect(&db_url, 10).await?;

    // 2) Print server identity to catch wrong target DB/port
    let meta = sqlx::query(
        r#"SELECT
               current_database() as db,
               current_user as usr,
               inet_server_addr()::text as host,
               inet_server_port() as port,
               version() as ver"#,
    )
    .fetch_one(&db.pool)
    .await?;
    eprintln!(
        "Connected → db={} user={} host={} port={} | {}",
        meta.get::<String, _>("db"),
        meta.get::<String, _>("usr"),
        meta.get::<String, _>("host"),
        meta.get::<i32, _>("port"),
        meta.get::<String, _>("ver")
            .split('\n')
            .next()
            .unwrap_or("")
    );

    // 3) Run your ingest
    let path = std::env::var("GIANT_BOMB_FILE")
        .unwrap_or_else(|_| "keep/giant_bomb_games_detialed.json".into());
    let limit = std::env::var("GB_LIMIT")
        .ok()
        .and_then(|s| s.parse::<usize>().ok());

    let started = Utc::now();
    let ingested = ingest_from_file(&db, &path, limit).await?;
    eprintln!("ingest_from_file returned processed={}", ingested);

    // 4) Verify a table that should have changed (replace table/conditions)
    //    If your ingestor upserts into, say, `games`, count rows updated recently.
    //    Adjust the WHERE to match your schema (timestamps, etc.).
    let check = sqlx::query(
        r#"
        SELECT COUNT(*)::bigint AS n
        FROM video_games
        WHERE updated_at >= $1
        "#,
    )
    .bind(started - chrono::Duration::seconds(20))
    .fetch_one(&db.pool)
    .await;

    match check {
        Ok(row) => println!("rows updated since start ≈ {}", row.get::<i64, _>("n")),
        Err(e) => println!("(skip) couldn't check `games` table: {e}"),
    }

    // 5) Canary write to prove we can actually persist
    //    Create a tiny audit table once in your schema and reuse it forever.
    //    (If you don't want DDL here, pre-create `ingest_canary(note text, t timestamptz default now())`.)
    let _ = sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS ingest_canary(
               id bigserial primary key,
               note text,
               t timestamptz NOT NULL DEFAULT now()
           )"#,
    )
    .execute(&db.pool)
    .await?;

    let note = format!("canary at {}", Utc::now());
    let res = sqlx::query("INSERT INTO ingest_canary(note) VALUES($1)")
        .bind(&note)
        .execute(&db.pool)
        .await?;

    eprintln!("canary insert rows_affected={}", res.rows_affected());

    let last: (String,) = sqlx::query_as("SELECT note FROM ingest_canary ORDER BY id DESC LIMIT 1")
        .fetch_one(&db.pool)
        .await?;
    eprintln!("canary last note='{}'", last.0);

    Ok(())
}

fn redact_url(url: &str) -> String {
    // crude redactor: hides password, preserves user@host:port/db and query
    match url.split_once("://") {
        Some((scheme, rest)) => {
            // rest like user:pass@host:port/db?params
            if let Some((creds, tail)) = rest.split_once('@') {
                let user = creds.split(':').next().unwrap_or("?");
                format!("{scheme}://{}:****@{tail}", user)
            } else {
                format!("{scheme}://{rest}")
            }
        }
        None => "<invalid-url>".into(),
    }
}
