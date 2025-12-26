use anyhow::Result;
use i_miss_rust::database_ops::giantbomb::price_guide;
use i_miss_rust::util::env as env_util;

#[tokio::main]
async fn main() -> Result<()> {
    env_util::bootstrap_cli("import_price_guide_fast");
    price_guide::run_import(true).await
}
