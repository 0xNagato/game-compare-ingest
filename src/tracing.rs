use tracing_subscriber::{EnvFilter, fmt::SubscriberBuilder};

/// Sets up the global tracing subscriber with a fmt formatter and env filter.
///
/// The caller provides a fallback filter string that is used when `RUST_LOG` is
/// not set. This helper ensures all binaries share the same formatting rules.
pub fn init_tracing(default_filter: &str) -> Result<(), anyhow::Error> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_filter));

    SubscriberBuilder::default()
        .with_env_filter(filter)
        .with_target(true)
        .with_line_number(true)
        .with_file(true)
        .try_init()
        .map_err(|e| anyhow::anyhow!("failed to initialize tracing: {}", e))
}
