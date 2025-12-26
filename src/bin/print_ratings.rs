use anyhow::Result;
// Delegate to modular giantbomb utility
use i_miss_rust::database_ops::giantbomb::ratings;
use i_miss_rust::util::env;

fn main() -> Result<()> {
    env::bootstrap_cli("print_ratings");
    ratings::print_from_env()
}
