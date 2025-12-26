// src/bin/verify_platform_hardware.rs
use anyhow::{Context, Result};
use i_miss_rust::database_ops::{db::Db, platform_hardware::fetch_platform_hardware_map};
use i_miss_rust::util::env::{self, db_url};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    env::bootstrap_cli("verify_platform_hardware");
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let database_url = db_url().context(
        "no database URL env vars set (SUPABASE_DB_SESSION_URL | SUPABASE_DB_URL | DATABASE_URL)",
    )?;

    let db = Db::connect_no_migrate(&database_url, 5).await?;

    // Check mapping table
    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM public.platform_hardware_map")
        .fetch_one(&db.pool)
        .await?;

    println!("Total mappings in platform_hardware_map: {}", count.0);

    // Fetch via helper
    let mappings = fetch_platform_hardware_map(&db).await?;

    println!("\nPlatform -> Hardware mappings (first 10):");
    for (i, m) in mappings.iter().take(10).enumerate() {
        println!(
            "  {}. platform_id={} code={:?} name={:?} -> hardware_product_id={:?} mapped_at={:?}",
            i + 1,
            m.platform_id,
            m.platform_code,
            m.platform_name,
            m.hardware_product_id,
            m.mapped_at
        );
    }

    // Check view
    let view_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM public.platform_hardware_view")
        .fetch_one(&db.pool)
        .await?;

    println!("\nTotal rows in platform_hardware_view: {}", view_count.0);

    // Check trigger function exists
    let trigger_exists: (bool,) = sqlx::query_as(
        "SELECT EXISTS(SELECT 1 FROM pg_proc WHERE proname = 'tg_sync_platform_hardware_map')",
    )
    .fetch_one(&db.pool)
    .await?;

    println!("Trigger function exists: {}", trigger_exists.0);

    // Check trigger exists on game_consoles
    let trigger_attached: (bool,) = sqlx::query_as(
        "SELECT EXISTS(
            SELECT 1 FROM pg_trigger t 
            JOIN pg_class c ON t.tgrelid = c.oid
            WHERE t.tgname = 'tr_game_consoles_sync_platform_hardware_map' 
              AND c.relname = 'game_consoles'
        )",
    )
    .fetch_one(&db.pool)
    .await?;

    println!("Trigger attached to game_consoles: {}", trigger_attached.0);

    // Check platforms count
    let platforms_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM public.platforms")
        .fetch_one(&db.pool)
        .await?;

    println!("\nTotal platforms: {}", platforms_count.0);

    // Check hardware count
    let hardware_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM public.hardware")
        .fetch_one(&db.pool)
        .await?;

    println!("Total hardware products: {}", hardware_count.0);

    // Check game_consoles count and platform_id population
    let consoles_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM public.game_consoles")
        .fetch_one(&db.pool)
        .await?;

    let consoles_with_platform: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM public.game_consoles WHERE platform_id IS NOT NULL")
            .fetch_one(&db.pool)
            .await?;

    println!(
        "Total game_consoles: {} (with platform_id: {})",
        consoles_count.0, consoles_with_platform.0
    );

    Ok(())
}
