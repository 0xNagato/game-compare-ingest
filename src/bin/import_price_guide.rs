use anyhow::Result;
use i_miss_rust::database_ops::giantbomb::price_guide;
use i_miss_rust::util::env;

#[tokio::main]
async fn main() -> Result<()> {
    env::bootstrap_cli("import_price_guide");
    price_guide::run_import(false).await
}
