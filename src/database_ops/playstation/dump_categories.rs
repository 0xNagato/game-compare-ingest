use anyhow::{bail, Context, Result};
use serde_json::Value;
use std::collections::HashMap;
use std::{fs, path::PathBuf};

#[derive(Debug, Clone)]
pub struct CategoryRow {
    pub name: String,
    pub id: String,
    pub sha256: String,
}

pub fn run_from_env() -> Result<Vec<CategoryRow>> {
    let path = std::env::var("PS_API_JSON_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("psstore_client/psstore.api.json"));
    let data = fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
    let v: Value = serde_json::from_str(&data).context("parsing psstore.api.json")?;

    let mut vars_map: HashMap<String, String> = HashMap::new();
    if let Some(vars) = v.get("variable").and_then(|x| x.as_array()) {
        for var in vars {
            if let (Some(k), Some(val)) = (
                var.get("key").and_then(|x| x.as_str()),
                var.get("value").and_then(|x| x.as_str()),
            ) {
                vars_map.insert(k.to_string(), val.to_string());
            }
        }
    }

    let mut rows: Vec<CategoryRow> = Vec::new();
    if let Some(items) = v.get("item").and_then(|x| x.as_array()) {
        for folder in items {
            if let Some(subs) = folder.get("item").and_then(|x| x.as_array()) {
                for sub in subs {
                    if sub.get("name").and_then(|n| n.as_str()).unwrap_or("") != "Product list" {
                        continue;
                    }
                    if let Some(cat_items) = sub.get("item").and_then(|x| x.as_array()) {
                        for cat in cat_items {
                            let name = cat
                                .get("name")
                                .and_then(|n| n.as_str())
                                .unwrap_or("")
                                .to_string();
                            let mut id: Option<String> = None;
                            let mut sha: Option<String> = None;
                            if let Some(req) = cat.get("request") {
                                if let Some(url) = req.get("url") {
                                    if let Some(query) = url.get("query").and_then(|q| q.as_array())
                                    {
                                        for qkv in query {
                                            let key = qkv
                                                .get("key")
                                                .and_then(|s| s.as_str())
                                                .unwrap_or("");
                                            let val = qkv
                                                .get("value")
                                                .and_then(|s| s.as_str())
                                                .unwrap_or("");
                                            match key {
                                                "variables" => {
                                                    if let Ok(j) =
                                                        serde_json::from_str::<Value>(val)
                                                    {
                                                        if let Some(cid) =
                                                            j.get("id").and_then(|s| s.as_str())
                                                        {
                                                            id = Some(cid.to_string());
                                                        }
                                                    }
                                                }
                                                "extensions" => {
                                                    if let Ok(j) =
                                                        serde_json::from_str::<Value>(val)
                                                    {
                                                        if let Some(hash) = j
                                                            .get("persistedQuery")
                                                            .and_then(|pq| pq.get("sha256Hash"))
                                                            .and_then(|s| s.as_str())
                                                        {
                                                            sha = Some(hash.to_string());
                                                        }
                                                    }
                                                }
                                                _ => {}
                                            }
                                        }
                                    }
                                }
                            }
                            if let (Some(id), Some(mut sha)) = (id, sha) {
                                if sha.starts_with("{{") && sha.ends_with("}}") {
                                    let key = sha.trim_start_matches("{{").trim_end_matches("}}");
                                    if let Some(resolved) = vars_map.get(key) {
                                        sha = resolved.clone();
                                    }
                                }
                                rows.push(CategoryRow {
                                    name,
                                    id,
                                    sha256: sha,
                                });
                            }
                        }
                    }
                }
            }
        }
    }

    if rows.is_empty() {
        bail!("no categories found in {}", path.display());
    }
    Ok(rows)
}

pub fn write_csv(rows: &[CategoryRow]) -> Result<()> {
    let out_path = std::env::var("PS_CATEGORIES_CSV").unwrap_or_else(|_| "categories.csv".into());
    let mut wtr =
        csv::Writer::from_path(&out_path).with_context(|| format!("open {}", out_path))?;
    wtr.write_record(["name", "id", "sha256Hash"]).ok();
    for r in rows {
        wtr.write_record([r.name.as_str(), r.id.as_str(), r.sha256.as_str()])
            .ok();
    }
    wtr.flush().ok();
    println!("name,id,sha256Hash");
    for r in rows {
        println!("{} , {} , {}", r.name, r.id, r.sha256);
    }
    eprintln!("Wrote {} categories to {}", rows.len(), out_path);
    Ok(())
}
