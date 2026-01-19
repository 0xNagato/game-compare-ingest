use anyhow::Result;
use psstore_client::{PsConfig, PsStoreClient};

pub async fn run_from_env() -> Result<()> {
    let _sha = std::env::var("PS_HASH").unwrap_or_else(|_| {
        "9845afc0dbaab4965f6563fffc703f588c8e76792000e8610843b8d3ee9c4c09".into()
    });
    let locale = std::env::var("PS_LOCALE").unwrap_or_else(|_| "en-us".into());
    let cat_ps4 = std::env::var("PS4_CATEGORY")
        .unwrap_or_else(|_| "44d8bb20-653e-431e-8ad0-c0a365f68d2f".into());
    let cat_ps5 = std::env::var("PS5_CATEGORY")
        .unwrap_or_else(|_| "4cbf39e2-5749-4970-ba81-93a489e4570c".into());

    let cfg = PsConfig {
        locales: vec![locale.clone()],
        rps: 3,
        retry_attempts: 5,
        retry_base_delay_ms: 2000,
        ..PsConfig::default()
    };
    let client = PsStoreClient::new(cfg);

    println!("PS4 Games ({}):", &locale);
    match client
        .category_grid_retrieve_sorted(&locale, &cat_ps4, 24, 0, "productReleaseDate", false)
        .await
    {
        Ok(mut list) => {
            if list.is_empty() {
                if let Ok(fallback) = client
                    .category_grid_retrieve(&locale, &cat_ps4, 24, 0)
                    .await
                {
                    list = fallback;
                }
            }
            for h in list.into_iter().take(10) {
                println!(
                    "  product={:?} concept={:?} name={:?}",
                    h.product_id, h.concept_id, h.name
                );
            }
        }
        Err(e) => eprintln!("  error: {e}"),
    }

    let vars = serde_json::json!({
        "id": cat_ps4,
        "pageArgs": { "size": 5, "offset": 0 },
        "sortBy": serde_json::Value::Null,
        "filterBy": [],
        "facetOptions": []
    });
    match client
        .op_get("categoryGridRetrieve", &vars, Some(&locale))
        .await
    {
        Ok(v) => {
            println!(
                "\nPS4 raw snippet:\n{}",
                serde_json::to_string_pretty(&v).unwrap_or_default()
            );
        }
        Err(e) => eprintln!("\nraw error: {e}"),
    }

    println!("\nPS5 Games ({}):", &locale);
    match client
        .category_grid_retrieve_sorted(&locale, &cat_ps5, 24, 0, "productReleaseDate", false)
        .await
    {
        Ok(mut list) => {
            if list.is_empty() {
                if let Ok(fallback) = client
                    .category_grid_retrieve(&locale, &cat_ps5, 24, 0)
                    .await
                {
                    list = fallback;
                }
            }
            for h in list.into_iter().take(10) {
                println!(
                    "  product={:?} \n \n concept={:?} \n \n name={:?}",
                    h.product_id, h.concept_id, h.name
                );
            }
        }
        Err(e) => eprintln!("  error: {e}"),
    }

    Ok(())
}
