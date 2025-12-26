use anyhow::Result;
use i_miss_rust::util::env;
use tokio_postgres::NoTls;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    env::bootstrap_cli("prices_drop_trigger");
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
    let db_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let (client, connection) = tokio_postgres::connect(&db_url, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });
    // Execute each DO block separately via simple_query (no prepared stmt multi-command limitation)
    let blocks = [
        r#"DO $$ BEGIN
          IF EXISTS (
            SELECT 1 FROM pg_trigger t
            JOIN pg_class c ON t.tgrelid = c.oid
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE c.relname = 'prices' AND n.nspname='public' AND t.tgname = 'prices_partition') THEN
              EXECUTE 'DROP TRIGGER prices_partition ON public.prices';
          END IF;
        END $$;"#,
        r#"DO $$ BEGIN
          IF EXISTS (
            SELECT 1 FROM pg_proc p
            JOIN pg_namespace n ON p.pronamespace = n.oid
            WHERE p.proname = 'prices_partition_insert_trigger' AND n.nspname = 'public') THEN
              EXECUTE 'DROP FUNCTION public.prices_partition_insert_trigger()';
          END IF;
        END $$;"#,
    ];
    for sql in blocks {
        client.simple_query(sql).await?;
    }
    println!("Dropped legacy prices partition trigger + function if they existed");
    Ok(())
}
