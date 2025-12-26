use sqlx::PgPool;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    i_miss_rust::util::env::bootstrap_cli("test_write");
    dotenv::dotenv().ok();
    let mut url = std::env::var("SUPABASE_DB_URL").or_else(|_| std::env::var("DATABASE_URL"))?;

    // Force disable prepared statements via connection string
    if !url.contains("?") {
        url.push_str("?statement_cache_capacity=0");
    } else if !url.contains("statement_cache_capacity") {
        url.push_str("&statement_cache_capacity=0");
    }

    println!("🔗 Connecting (no prepared statements via URL)...");
    let pool = PgPool::connect(&url).await?;

    println!("✅ Connected");

    // Clear any leftover prepared statements from previous connections
    println!("🧹 Clearing prepared statements...");
    let _ = sqlx::query("DEALLOCATE ALL").execute(&pool).await;
    println!("   Done");

    let mut tx = pool.begin().await?;

    println!("📝 Creating test table...");
    sqlx::query("CREATE TABLE IF NOT EXISTS public.test_write(id serial primary key, name text)")
        .execute(&mut *tx)
        .await?;

    println!("➕ Inserting row...");
    sqlx::query("INSERT INTO public.test_write(name) VALUES($1)")
        .bind("hello from rust")
        .execute(&mut *tx)
        .await?;

    println!("💾 Committing...");
    tx.commit().await?;

    println!("📖 Reading back...");
    let rec: (i32, String) =
        sqlx::query_as("SELECT id, name FROM public.test_write ORDER BY id DESC LIMIT 1")
            .fetch_one(&pool)
            .await?;

    println!("✨ Latest row = {:?}", rec);

    Ok(())
}
