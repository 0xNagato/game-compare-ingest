pub mod provider;

use crate::database_ops::db::Db;
use anyhow::Result;

/// Placeholder orchestrated sync for Nexarda.
/// For full ingestion, use provider::NexardaProvider::ingest_to_db with configured options
/// or ingest_catalogue_file for offline catalogue imports.
pub async fn sync(_db: &Db) -> Result<()> {
    // TODO: wire real-time Nexarda deals ingestion based on configured products/options
    Ok(())
}
