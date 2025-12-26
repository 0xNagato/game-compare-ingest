use std::{collections::HashMap, env, fs, path::Path};

fn parse_env_lines(contents: &str) -> Vec<(usize, String, String)> {
    let mut out = Vec::new();
    for (idx, raw) in contents.lines().enumerate() {
        let line_no = idx + 1;
        let mut line = raw.trim().to_string();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(rest) = line.strip_prefix("export ") {
            line = rest.trim().to_string();
        }
        // Find the first '=' only
        let eq = match line.find('=') {
            Some(i) => i,
            None => {
                continue;
            }
        };
        let key = line[..eq].trim().to_string();
        let mut val = line[eq + 1..].trim().to_string();
        // Strip surrounding quotes if present
        if (val.starts_with('"') && val.ends_with('"'))
            || (val.starts_with('\'') && val.ends_with('\''))
        {
            val = val[1..val.len() - 1].to_string();
        } else {
            // Remove inline comments for unquoted values: split on ' #' (space then #) or just first # after a space
            if let Some(hash_pos) = val.find('#') {
                // Only treat as comment if there is whitespace before '#'
                let prefix = &val[..hash_pos];
                if prefix.ends_with(' ') || prefix.ends_with('\t') {
                    val = prefix.trim_end().to_string();
                }
            }
        }
        if key.is_empty() {
            continue;
        }
        out.push((line_no, key, val));
    }
    out
}

fn is_likely_postgres_dsn(v: &str) -> bool {
    v.starts_with("postgres://") || v.starts_with("postgresql://")
}

fn main() {
    i_miss_rust::util::env::bootstrap_cli("validate_env");
    // Optional arg: path to .env (default ".env")
    let path = env::args().nth(1).unwrap_or_else(|| ".env".to_string());
    if !Path::new(&path).exists() {
        eprintln!("No .env found at {}", path);
        std::process::exit(2);
    }
    let contents = match fs::read_to_string(&path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to read {}: {}", path, e);
            std::process::exit(2);
        }
    };

    let entries = parse_env_lines(&contents);
    let mut first_seen: HashMap<String, (usize, String)> = HashMap::new();
    let mut duplicates: Vec<(String, usize, String, usize, String)> = Vec::new();

    for (line, key, val) in entries.iter().cloned() {
        if let Some((first_line, first_val)) = first_seen.get(&key).cloned() {
            duplicates.push((key.clone(), first_line, first_val, line, val));
        } else {
            first_seen.insert(key, (line, val));
        }
    }

    // Validation checks
    let mut has_errors = false;

    // 1) Duplicates
    if !duplicates.is_empty() {
        println!(
            "[WARN] Duplicate keys found (dotenv is first-value-wins; later values are ignored):"
        );
        for (key, l1, v1, l2, v2) in &duplicates {
            let conflict = if v1 == v2 { "same" } else { "different" };
            println!(
                "  - {}: line {}='{}' vs line {}='{}' ({} values)",
                key, l1, v1, l2, v2, conflict
            );
        }
    }

    // 2) DSN sanity: prefer SUPABASE_DB_URL, then DB_URL, then DATABASE_URL
    let dsn_key = if first_seen.contains_key("SUPABASE_DB_URL") {
        "SUPABASE_DB_URL"
    } else if first_seen.contains_key("DB_URL") {
        "DB_URL"
    } else if first_seen.contains_key("DATABASE_URL") {
        "DATABASE_URL"
    } else {
        ""
    };
    if dsn_key.is_empty() {
        eprintln!(
            "[ERROR] Missing SUPABASE_DB_URL (or DB_URL/DATABASE_URL) — set to your Postgres DSN, e.g., postgres://user:pass@host:port/db"
        );
        has_errors = true;
    } else {
        let (line, val) = first_seen.get(dsn_key).unwrap();
        if !is_likely_postgres_dsn(val) {
            eprintln!(
                "[ERROR] {} at line {} is not a Postgres DSN (expected postgres://…): '{}'",
                dsn_key, line, val
            );
            has_errors = true;
        } else {
            println!("[OK] Using {} from line {}", dsn_key, line);
        }
        // Inform if multiple DSN-like keys present
        let mut present_keys = Vec::new();
        for k in ["SUPABASE_DB_URL", "DB_URL", "DATABASE_URL"] {
            if first_seen.contains_key(k) {
                present_keys.push(k);
            }
        }
        if present_keys.len() > 1 {
            println!(
                "[INFO] Multiple DSN envs present: {:?} (priority: SUPABASE_DB_URL > DB_URL > DATABASE_URL).",
                present_keys
            );
        }
    }

    // 3) DATABASE_URL presence (informational)
    if let Some((line, _)) = first_seen.get("DATABASE_URL") {
        println!(
            "[INFO] DATABASE_URL is set (line {}), code prefers SUPABASE_DB_URL if present.",
            line
        );
    }

    if has_errors {
        println!("Validation: FAIL");
        std::process::exit(1);
    } else {
        println!("Validation: PASS");
        std::process::exit(0);
    }
}
