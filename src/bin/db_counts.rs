use anyhow::Result;
use i_miss_rust::cli::db_counts::{run, DbCountsConfig};

#[tokio::main]
async fn main() -> Result<()> {
    i_miss_rust::util::env::bootstrap_cli("db_counts");

    run(DbCountsConfig::default()).await
}
