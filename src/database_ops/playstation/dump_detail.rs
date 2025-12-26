use serde_json::Value;
use std::env;
use std::path::PathBuf;

/// Options to control PlayStation product detail dump
#[derive(Debug, Default, Clone)]
pub struct DumpDetailOptions {
    pub product_id: Option<String>,
    pub locale: Option<String>,
    pub out_path: Option<PathBuf>,
}

/// Result of a product detail dump
#[derive(Debug, Clone)]
pub struct DumpDetailResult {
    pub product_id: String,
    pub locale: String,
    pub detail: Value,
    pub ps_long_description: Option<String>,
    pub heuristic_description: Option<String>,
    pub wrote_path: Option<PathBuf>,
}

/// Execute the detail retrieval and optional write, returning parsed descriptions
pub async fn run(opts: DumpDetailOptions) -> anyhow::Result<DumpDetailResult> {
    let locale = resolve_locale(opts.locale);
    let client = psstore_client::PsStoreClient::new(psstore_client::PsConfig::default());

    let product_id = match opts.product_id.or_else(|| env::var("PRODUCT_ID").ok()) {
        Some(pid) => pid,
        None => find_first_ps5_product_id(&client, &locale).await?,
    };

    let detail = match client.product_detail_raw(&locale, &product_id).await {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(locale=%locale, product_id=%product_id, error=%e, "ps dump_detail: detail fetch failed; continuing with empty detail");
            Value::Null
        }
    };

    let wrote_path = if let Some(path) = opts.out_path {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).ok();
        }
        let pretty = serde_json::to_string_pretty(&detail)?;
        std::fs::write(&path, pretty.as_bytes())?;
        Some(path)
    } else {
        None
    };

    let node = detail.get("data").and_then(|d| d.get("metGetProductById"));
    let ps_long_description = find_ps_long_description(node);
    let heuristic_description = extract_summary_text(node);

    Ok(DumpDetailResult {
        product_id,
        locale,
        detail,
        ps_long_description,
        heuristic_description,
        wrote_path,
    })
}

fn resolve_locale(locale_arg: Option<String>) -> String {
    locale_arg
        .or_else(|| env::var("LOCALE").ok())
        .or_else(|| {
            env::var("PS_STORE_REGIONS").ok().and_then(|s| {
                s.split(|c: char| (c == ' ' || c == ','))
                    .find(|v| !v.is_empty())
                    .map(|v| v.to_lowercase())
            })
        })
        .unwrap_or_else(|| "en-us".to_string())
}

async fn find_first_ps5_product_id(
    client: &psstore_client::PsStoreClient,
    locale: &str,
) -> anyhow::Result<String> {
    let cat_ps5 =
        env::var("PS5_CATEGORY").unwrap_or_else(|_| "4cbf39e2-5749-4970-ba81-93a489e4570c".into());
    let items = client
        .category_grid_retrieve(locale, &cat_ps5, 1, 0)
        .await?;
    let Some(first) = items.into_iter().next() else {
        anyhow::bail!("No items returned for category {cat_ps5} locale {locale}");
    };
    if let Some(pid) = first.product_id {
        Ok(pid)
    } else {
        anyhow::bail!("First item missing product_id; cannot fetch detail")
    }
}

/// Heuristic extraction of a summary-like text by scanning description-ish fields
pub fn extract_summary_text(node: Option<&Value>) -> Option<String> {
    let Some(val) = node else {
        return None;
    };
    let mut best: Option<String> = None;

    fn clean(s: &str) -> String {
        // Strip simple HTML tags; collapse whitespace
        let mut out = String::with_capacity(s.len());
        let mut in_tag = false;
        let mut prev_space = false;
        for ch in s.chars() {
            match ch {
                '<' => {
                    in_tag = true;
                }
                '>' => {
                    in_tag = false;
                }
                c if !in_tag => {
                    let m = if c.is_whitespace() { ' ' } else { c };
                    if m == ' ' {
                        if !prev_space {
                            out.push(' ');
                            prev_space = true;
                        }
                    } else {
                        out.push(m);
                        prev_space = false;
                    }
                }
                _ => {}
            }
        }
        out.trim().to_string()
    }

    fn consider(v: &Value, best: &mut Option<String>) {
        match v {
            Value::String(s) => {
                let t = clean(s);
                if t.is_empty() {
                    return;
                }
                match best {
                    Some(b) => {
                        if t.len() > b.len() {
                            *b = t;
                        }
                    }
                    None => {
                        *best = Some(t);
                    }
                }
            }
            Value::Array(arr) => {
                for x in arr {
                    consider(x, best);
                }
            }
            Value::Object(obj) => {
                for (_k, vv) in obj {
                    consider(vv, best);
                }
            }
            _ => {}
        }
    }

    fn walk(k: &str, v: &Value, best: &mut Option<String>) {
        let kl = k.to_ascii_lowercase();
        let looks_like_desc = kl.contains("description")
            || kl.contains("summary")
            || kl.contains("synopsis")
            || kl.contains("about")
            || kl.contains("overview");
        if looks_like_desc {
            consider(v, best);
        }
        match v {
            Value::Object(obj) => {
                for (kk, vv) in obj {
                    walk(kk, vv, best);
                }
            }
            Value::Array(arr) => {
                for vv in arr {
                    walk("", vv, best);
                }
            }
            _ => {}
        }
    }

    if let Some(obj) = val.as_object() {
        for (k, v) in obj {
            walk(k, v, &mut best);
        }
    }
    best
}

/// Prefer PlayStation specific Description(type LONG).value text
pub fn find_ps_long_description(node: Option<&Value>) -> Option<String> {
    fn clean(s: &str) -> String {
        let mut out = String::with_capacity(s.len());
        let mut in_tag = false;
        let mut prev_space = false;
        for ch in s.chars() {
            match ch {
                '<' => {
                    in_tag = true;
                }
                '>' => {
                    in_tag = false;
                }
                c if !in_tag => {
                    let m = if c.is_whitespace() { ' ' } else { c };
                    if m == ' ' {
                        if !prev_space {
                            out.push(' ');
                            prev_space = true;
                        }
                    } else {
                        out.push(m);
                        prev_space = false;
                    }
                }
                _ => {}
            }
        }
        out.trim().to_string()
    }
    fn search(v: &Value) -> Option<&str> {
        match v {
            Value::Object(obj) => {
                let tn = obj
                    .get("__typename")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_ascii_lowercase();
                let ty = obj
                    .get("type")
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_ascii_lowercase();
                if tn == "description" && ty == "long" {
                    if let Some(Value::String(s)) = obj.get("value") {
                        return Some(s.as_str());
                    }
                }
                for (_k, vv) in obj {
                    if let Some(s) = search(vv) {
                        return Some(s);
                    }
                }
                None
            }
            Value::Array(arr) => {
                for el in arr {
                    if let Some(s) = search(el) {
                        return Some(s);
                    }
                }
                None
            }
            _ => None,
        }
    }
    let Some(v) = node else {
        return None;
    };
    let raw = search(v)?;
    Some(clean(raw))
}
