use anyhow::Result;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use std::str::FromStr;

#[tokio::main]
async fn main() -> Result<()> {
    i_miss_rust::util::env::bootstrap_cli("game_counts");
    let db_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let opts = PgConnectOptions::from_str(&db_url)?.statement_cache_capacity(0);
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(opts)
        .await?;
    let titles: i64 = sqlx::query_scalar("SELECT count(*) FROM public.video_game_titles")
        .persistent(false)
        .fetch_one(&pool)
        .await?;
    let games: i64 = sqlx::query_scalar("SELECT count(*) FROM public.video_games")
        .persistent(false)
        .fetch_one(&pool)
        .await?;
    println!("video_game_titles={titles}");
    println!("video_games={games}");
    Ok(())
}
