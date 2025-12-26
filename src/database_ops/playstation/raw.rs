use anyhow::Result;
use psstore_client::{PsConfig, PsStoreClient};

// Prints the raw JSON for a page of categoryGridRetrieve
pub async fn print_from_env() -> Result<()> {
    let locale = std::env::var("PS_LOCALE").unwrap_or_else(|_| "en-us".into());
    let cat_ps5 = std::env::var("PS_CATEGORY")
        .unwrap_or_else(|_| "4cbf39e2-5749-4970-ba81-93a489e4570c".into());
    let _sha = std::env::var("PS_HASH").unwrap_or_else(|_| {
        "9845afc0dbaab4965f6563fffc703f588c8e76792000e8610843b8d3ee9c4c09".into()
    });
    let size: u32 = std::env::var("RAW_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);
    let offset: u32 = std::env::var("RAW_OFFSET")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    let cfg = PsConfig {
        locales: vec![locale.clone()],
        rps: 3,
        retry_attempts: 5,
        retry_base_delay_ms: 2000,
        ..PsConfig::default()
    };
    let client = PsStoreClient::new(cfg);

    let vars = serde_json::json!({
        "id": cat_ps5,
        "pageArgs": { "size": size, "offset": offset },
        "sortBy": serde_json::Value::Null,
        "filterBy": [],
        "facetOptions": []
    });

    let raw = client
        .op_get("categoryGridRetrieve", &vars, Some(&locale))
        .await?;
    println!("{}", serde_json::to_string_pretty(&raw)?);
    Ok(())
}
