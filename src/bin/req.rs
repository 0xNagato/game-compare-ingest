use anyhow::{anyhow, Context, Result};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::env;
use std::fs;
use std::time::Duration;

#[derive(Debug, Deserialize)]
struct RequestFile {
    #[serde(default)]
    variables: HashMap<String, String>,
    requests: Vec<RequestEntry>,
}

#[derive(Debug, Deserialize)]
struct RequestEntry {
    name: String,
    method: String,
    url: String,
    #[serde(default)]
    headers: HashMap<String, String>,
    #[serde(default)]
    query: Value,
    #[serde(default)]
    body: Value,
    #[serde(default)]
    description: Option<String>,
}

fn replace_vars(input: &str, vars: &HashMap<String, String>) -> String {
    // Replace occurrences of {{KEY}} with vars[KEY]
    let mut out = String::with_capacity(input.len());
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'{' && i + 3 < bytes.len() && bytes[i + 1] == b'{' {
            // find closing }}
            if let Some(end) = input[i + 2..].find("}}") {
                let key = &input[i + 2..i + 2 + end];
                if let Some(val) = vars.get(key) {
                    out.push_str(val);
                } else {
                    // keep original if not found
                    out.push_str("{{");
                    out.push_str(key);
                    out.push_str("}}");
                }
                i += 2 + end + 2; // {{ key }}
                continue;
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

fn parse_cli() -> Result<(String, String, HashMap<String, String>, bool, u64)> {
    let mut args = env::args().skip(1);
    let file = args.next().ok_or_else(|| {
        anyhow!(
            "usage: req <file.json> <request-name> [--var KEY=VALUE ...] [--raw] [--timeout SECS]"
        )
    })?;
    let name = args.next().ok_or_else(|| {
        anyhow!(
            "usage: req <file.json> <request-name> [--var KEY=VALUE ...] [--raw] [--timeout SECS]"
        )
    })?;

    let mut vars = HashMap::new();
    let mut raw = false;
    let mut timeout_secs: u64 = 30;

    let mut rest: Vec<String> = args.collect();
    let mut i = 0;
    while i < rest.len() {
        match rest[i].as_str() {
            "--raw" => {
                raw = true;
                i += 1;
            }
            "--timeout" => {
                if i + 1 >= rest.len() {
                    return Err(anyhow!("--timeout requires SECS"));
                }
                timeout_secs = rest[i + 1].parse().context("invalid --timeout value")?;
                i += 2;
            }
            "--var" => {
                if i + 1 >= rest.len() {
                    return Err(anyhow!("--var requires KEY=VALUE"));
                }
                if let Some((k, v)) = rest[i + 1].split_once('=') {
                    vars.insert(k.to_string(), v.to_string());
                } else {
                    return Err(anyhow!("--var must be KEY=VALUE"));
                }
                i += 2;
            }
            other => {
                // allow shorthand KEY=VALUE without --var
                if let Some((k, v)) = other.split_once('=') {
                    vars.insert(k.to_string(), v.to_string());
                    i += 1;
                } else {
                    return Err(anyhow!("unknown arg: {}", other));
                }
            }
        }
    }

    Ok((file, name, vars, raw, timeout_secs))
}

#[tokio::main]
async fn main() -> Result<()> {
    i_miss_rust::util::env::bootstrap_cli("req");
    let (file, name, cli_vars, raw, timeout_secs) = parse_cli()?;
    let data = fs::read_to_string(&file).with_context(|| format!("reading {}", file))?;
    let mut reqfile: RequestFile =
        serde_json::from_str(&data).with_context(|| "parsing json file")?;

    // Merge variables: file < env < CLI
    // Allow env to override any key present in file.variables
    for (k, v) in std::env::vars() {
        if reqfile.variables.contains_key(&k) {
            reqfile.variables.insert(k, v);
        }
    }
    for (k, v) in cli_vars {
        reqfile.variables.insert(k, v);
    }

    // Find request by name
    let entry = reqfile
        .requests
        .iter()
        .find(|r| r.name == name)
        .ok_or_else(|| anyhow!("request not found: {}", name))?;

    // Prepare URL with substitution
    let url_str = replace_vars(&entry.url, &reqfile.variables);

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(timeout_secs))
        .build()?;

    // Build headers
    let mut headers = HeaderMap::new();
    for (k, v) in &entry.headers {
        let name = HeaderName::from_bytes(k.as_bytes())?;
        let val = HeaderValue::from_str(v)?;
        headers.insert(name, val);
    }

    // Build request
    let method = entry.method.to_uppercase();
    let mut req = match method.as_str() {
        "GET" => client.get(&url_str),
        "POST" => client.post(&url_str),
        "PUT" => client.put(&url_str),
        "PATCH" => client.patch(&url_str),
        "DELETE" => client.delete(&url_str),
        other => {
            return Err(anyhow!("unsupported method: {}", other));
        }
    };

    // Apply headers
    if !headers.is_empty() {
        req = req.headers(headers);
    }

    // Apply query if object
    if let Value::Object(map) = &entry.query {
        let mut pairs: Vec<(String, String)> = Vec::with_capacity(map.len());
        for (k, v) in map {
            let s = match v {
                Value::Null => String::new(),
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            pairs.push((k.clone(), s));
        }
        req = req.query(&pairs);
    }

    // Apply JSON body if present and method allows body
    let has_body_method = matches!(method.as_str(), "POST" | "PUT" | "PATCH" | "DELETE");
    if has_body_method && !entry.body.is_null() {
        req = req.json(&entry.body);
    }

    let resp = req.send().await;
    let resp = match resp {
        Ok(r) => r,
        Err(e) => {
            println!("{}", json!({"ok": false, "error": e.to_string()}));
            std::process::exit(1);
        }
    };

    let status = resp.status();
    let content_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("")
        .to_string();

    let bytes = resp.bytes().await?;

    if !status.is_success() {
        eprintln!("status: {}", status);
        if content_type.contains("application/json") {
            match serde_json::from_slice::<Value>(&bytes) {
                Ok(v) => println!("{}", serde_json::to_string_pretty(&v)?),
                Err(_) => println!("{}", String::from_utf8_lossy(&bytes)),
            }
        } else {
            println!("{}", String::from_utf8_lossy(&bytes));
        }
        std::process::exit(1);
    }

    if raw {
        // Print raw body
        print!("{}", String::from_utf8_lossy(&bytes));
        return Ok(());
    }

    if content_type.contains("application/json") {
        match serde_json::from_slice::<Value>(&bytes) {
            Ok(v) => println!("{}", serde_json::to_string_pretty(&v)?),
            Err(_) => println!("{}", String::from_utf8_lossy(&bytes)),
        }
    } else {
        println!("{}", String::from_utf8_lossy(&bytes));
    }

    Ok(())
}
