use anyhow::{anyhow, Result};
use i_miss_rust::database_ops::{
    db::Db,
    search::{search_games, search_match_both},
};
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    i_miss_rust::util::env::bootstrap_cli("search");
    let database_url = env::var("SUPABASE_DB_URL")
        .or_else(|_| env::var("DATABASE_URL"))
        .map_err(|_| anyhow!("DB_URL or DATABASE_URL not set"))?;
    let q = env::args()
        .nth(1)
        .ok_or_else(|| anyhow!("usage: search <query> [limit]"))?;
    let limit: i64 = env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(25);
    let db = Db::connect(&database_url, 5).await?;

    let (sellables, provider_items) = search_match_both(&db, &q, limit).await?;

    println!("Sellables ({}):", sellables.len());
    for s in &sellables {
        println!("  [{}] {}", s.id, s.title);
    }
    println!("Provider Items ({}):", provider_items.len());
    for p in &provider_items {
        println!("  [{}] {} (ext: {:?})", p.id, p.title, p.extra);
    }

    // Unified view if needed
    let unified = search_games(&db, &q, limit).await?;
    println!("\nUnified hits ({}):", unified.len());
    for h in unified {
        println!("  {}:{} -> {}", h.kind, h.id, h.title);
    }
    Ok(())
}
