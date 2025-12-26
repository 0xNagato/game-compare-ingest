use anyhow::Result;
use psstore_client::{PsConfig, PsStoreClient};
use serde_json::Value;
use std::env;

pub async fn run_from_env() -> Result<()> {
    dotenv::dotenv().ok();
    let locale = env::var("PS_LOCALE").unwrap_or_else(|_| "en-us".into());
    let cat =
        env::var("PS_CATEGORY").unwrap_or_else(|_| "4cbf39e2-5749-4970-ba81-93a489e4570c".into());
    let _sha = env::var("PS_HASH").unwrap_or_else(|_| {
        "9845afc0dbaab4965f6563fffc703f588c8e76792000e8610843b8d3ee9c4c09".into()
    });
    let size: u32 = env::var("PS_PAGE_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100);
    let offset: u32 = env::var("PS_OFFSET")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);

    let cfg = PsConfig {
        locales: vec![locale.clone()],
        rps: 3,
        retry_attempts: 3,
        retry_base_delay_ms: 1000,
        ..PsConfig::default()
    };
    let client = PsStoreClient::new(cfg);
    let vars = serde_json::json!({
        "id": cat,
        "pageArgs": {"size": size, "offset": offset },
        "sortBy": serde_json::Value::Null,
        "filterBy": [],
        "facetOptions": []
    });
    let raw = client
        .op_get("categoryGridRetrieve", &vars, Some(&locale))
        .await?;
    let mut matches: Vec<String> = Vec::new();
    scan_value(&raw, &mut matches);
    matches.sort();
    matches.dedup();
    eprintln!("Found {} distinct raw genre strings/labels", matches.len());
    for m in &matches {
        eprintln!("genre_val: {}", m);
    }
    let summaries = psstore_client::extract_genre_facet(&raw);
    if !summaries.is_empty() {
        eprintln!("Facet productGenres ({} entries):", summaries.len());
        for g in summaries.iter().take(10) {
            eprintln!("  {} (key={}, count={})", g.display_name, g.key, g.count);
        }
    }
    Ok(())
}

fn scan_value(v: &Value, out: &mut Vec<String>) {
    match v {
        Value::Object(o) => {
            for (k, val) in o {
                let kl = k.to_lowercase();
                if kl.contains("genre") {
                    collect_strings(val, out);
                }
                scan_value(val, out);
            }
        }
        Value::Array(arr) => {
            for el in arr {
                scan_value(el, out);
            }
        }
        _ => {}
    }
}

fn collect_strings(v: &Value, out: &mut Vec<String>) {
    match v {
        Value::String(s) => {
            if !s.is_empty() {
                out.push(s.to_string());
            }
        }
        Value::Array(arr) => {
            for el in arr {
                collect_strings(el, out);
            }
        }
        Value::Object(o) => {
            for (_k, vv) in o {
                collect_strings(vv, out);
            }
        }
        _ => {}
    }
}
