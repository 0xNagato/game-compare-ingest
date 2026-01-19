//! Environment helpers: centralized dotenv loading and ergonomic getters.
//! Call `init_env()` once early in each binary (or rely on lazy Once).
use std::str::FromStr;
use std::sync::Once;
use tracing::{info, warn};

static INIT: Once = Once::new();

/// Load .env and apply perf-oriented defaults exactly once.
/// Safe to call many times.
pub fn init_env() {
    INIT.call_once(|| {
        // 1) Load .env if present
        let _ = dotenv::dotenv();
        // Note: We intentionally avoid mutating process env at runtime.
        // Any perf-related DB options (e.g., PGOPTIONS) and logging levels
        // should be provided by the caller/.env. Connection-level tuning is
        // handled where we construct connect options.
    });
}

/// Common bootstrap for CLI binaries:
///   * initialize dotenv/env once
///   * default to IPV6_DIRECT=1 unless explicitly disabled via DISABLE_IPV6_BOOTSTRAP
///   * log whether an IPv6-specific DSN is available
pub fn bootstrap_cli(bin_name: &str) {
    init_env();

    if env_flag("DISABLE_IPV6_BOOTSTRAP", false) {
        info!(
            target = "bootstrap",
            bin = bin_name,
            "IPv6 bootstrap disabled via DISABLE_IPV6_BOOTSTRAP"
        );
    } else if !env_flag("IPV6_DIRECT", false) {
        unsafe {
            std::env::set_var("IPV6_DIRECT", "1");
        }
        info!(
            target = "bootstrap",
            bin = bin_name,
            "IPV6_DIRECT not set; defaulting to 1 for CLI bin"
        );
    }

    if ipv6_db_url().is_some() {
        info!(target = "bootstrap", bin = bin_name, "IPv6 DSN detected");
    } else {
        warn!(
            target = "bootstrap",
            bin = bin_name,
            "no IPv6 DSN configured; falling back to generic DATABASE_URL variants"
        );
    }
}

/// Get required env var; error if missing.
pub fn env_req(key: &str) -> anyhow::Result<String> {
    init_env();
    std::env::var(key).map_err(|_| anyhow::anyhow!("missing env var {key}"))
}

/// Get optional env var (None if unset or empty).
pub fn env_opt(key: &str) -> Option<String> {
    init_env();
    match std::env::var(key) {
        Ok(v) if !v.trim().is_empty() => Some(v),
        _ => None,
    }
}

/// Get parsed value with default fallback.
pub fn env_parse<T>(key: &str, default: T) -> T
where
    T: FromStr + Clone,
{
    init_env();
    match std::env::var(key) {
        Ok(raw) => raw.parse::<T>().unwrap_or(default),
        Err(_) => default,
    }
}

/// Boolean flag; accepts 1/true/on/yes (case-insensitive) as true.
pub fn env_flag(key: &str, default: bool) -> bool {
    init_env();
    match std::env::var(key) {
        Ok(raw) => {
            let v = raw.trim().to_ascii_lowercase();
            matches!(v.as_str(), "1" | "true" | "on" | "yes")
        }
        Err(_) => default,
    }
}

/// Optional parsed value.
pub fn env_parse_opt<T>(key: &str) -> Option<T>
where
    T: FromStr,
{
    init_env();
    std::env::var(key).ok().and_then(|s| s.parse().ok())
}

/// Composed database URL (tries specific -> generic). Returns first found.
pub fn db_url() -> anyhow::Result<String> {
    init_env();
    // Primary: always prefer explicit IPv6 DSN if provided.
    if let Some(v) = ipv6_db_url() {
        // If a regular Supabase URL is also present, build a hybrid DSN that
        // uses the canonical hostname for TLS/SNI while forcing the IPv6
        // address via hostaddr= to avoid rustls DNS name issues.
        if let Some(hostname_url) = env_opt("SUPABASE_DB_URL") {
            if let Some(hybrid) = build_hybrid_ipv6_dsn(&v, &hostname_url) {
                info!(
                    target = "env",
                    "using IPv6 DSN with hostaddr override + hostname for TLS"
                );
                return Ok(hybrid);
            }
        }

        info!(target = "env", "using SUPABASE_IPV6_DB / V6_* composed DSN");
        return Ok(v);
    }

    // Next: respect explicit preferences to avoid PgBouncer when asked.
    let no_pgbouncer = env_flag("NO_PGBOUNCER", false) || env_flag("IPV6_DIRECT", false);
    if no_pgbouncer || env_flag("PREFER_DIRECT_DB", false) {
        for k in [
            "SUPABASE_DB_URL",
            "SUPABASE_DB_SESSION_URL",
            "DATABASE_URL",
            "DB_URL",
        ] {
            if let Some(v) = env_opt(k) {
                return Ok(v);
            }
        }
    } else {
        // Default ordering: session/pooler first, then direct.
        for k in [
            "SUPABASE_DB_SESSION_URL",
            "DATABASE_URL",
            "SUPABASE_DB_URL",
            "DB_URL",
        ] {
            if let Some(v) = env_opt(k) {
                return Ok(v);
            }
        }
    }

    Err(anyhow::anyhow!("no database URL env vars set"))
}

/// Same as `db_url()` but auto-swaps Supabase transaction pooler 6543→5432 (session pooler)
/// to avoid prepared-statement/timeout issues. Safe no-op for non-Supabase URLs.
pub fn db_url_prefer_session() -> anyhow::Result<String> {
    let raw = db_url()?;
    // If the caller explicitly wants to avoid PgBouncer (e.g., using IPv6 direct),
    // do not rewrite pooler URLs.
    if env_flag("DISABLE_SESSION_SWAP", false)
        || env_flag("NO_PGBOUNCER", false)
        || env_flag("IPV6_DIRECT", false)
    {
        // Caller explicitly wants the URL as-is (e.g., direct IPv6 host)
        Ok(raw)
    } else {
        Ok(prefer_session_mode(&raw))
    }
}

/// If the URL looks like Supabase's transaction pooler (port 6543),
/// prefer the session pooler (5432) automatically to avoid prepare/timeout issues.
pub fn prefer_session_mode(url: &str) -> String {
    if url.contains("pooler.supabase.com:6543") {
        // Keep a single log line at info so users can tell it happened.
        tracing::warn!(
            "detected Supabase transaction pooler (:6543); switching to :5432 (session)"
        );
        url.replace("pooler.supabase.com:6543", "pooler.supabase.com:5432")
    } else {
        url.to_string()
    }
}

/// Explicit IPv6 DSN override if provided via SUPABASE_IPV6_DB or the V6_* components.
pub fn ipv6_db_url() -> Option<String> {
    if let Some(v) = env_opt("SUPABASE_IPV6_DB") {
        return Some(v);
    }
    build_ipv6_dsn_from_components()
}

fn build_ipv6_dsn_from_components() -> Option<String> {
    let host = env_opt("V6_HOST")?;
    let user = env_opt("V6_USER")?;
    let password = env_opt("V6_PASSWORD").or_else(|| env_opt("POSTGRES_PASSWORD"))?;
    let database = env_opt("V6_DATABASE").or_else(|| env_opt("POSTGRES_DB"))?;
    let port = env_opt("V6_PORT").unwrap_or_else(|| "5432".into());

    // IMPORTANT:
    // - The password may contain reserved URL characters (e.g. '?' / '!' / '@').
    // - sqlx / url parsing requires these to be percent-encoded in the DSN.
    // - Build via `url::Url` so username/password are encoded safely.
    // - Supabase Postgres requires TLS; default to sslmode=require for composed DSNs.
    let host_trimmed = host
        .trim()
        .trim_matches(|c| c == '[' || c == ']')
        .to_string();
    let port_u16: u16 = port.parse::<u16>().unwrap_or(5432);

    let mut out = url::Url::parse("postgresql://localhost").ok()?;
    out.set_username(&user).ok()?;
    out.set_password(Some(&password)).ok()?;
    out.set_host(Some(&host_trimmed)).ok()?;
    out.set_port(Some(port_u16)).ok()?;
    out.set_path(&format!("/{database}"));
    out.query_pairs_mut().append_pair("sslmode", "require");

    Some(out.to_string())
}

/// Build a DSN that connects via the given IPv6 address (hostaddr) but uses the
/// canonical hostname from another DSN for TLS/SNI. Falls back to None if parsing fails.
fn build_hybrid_ipv6_dsn(ipv6_dsn: &str, hostname_dsn: &str) -> Option<String> {
    let ipv6_url = url::Url::parse(ipv6_dsn).ok()?;
    let hostname_url = url::Url::parse(hostname_dsn).ok()?;

    let ip_host = ipv6_url.host_str()?.trim_matches(['[', ']']);
    let host = hostname_url.host_str()?;
    let port = ipv6_url.port_or_known_default().unwrap_or(5432);
    let db = ipv6_url.path().trim_start_matches('/');

    let user = ipv6_url.username();
    // Percent-encode user/pass safely via Url builder instead of manual string concatenation
    let mut out = url::Url::parse("postgresql://localhost").ok()?;
    out.set_username(user).ok()?;
    if let Some(pass) = ipv6_url.password() {
        out.set_password(Some(pass)).ok()?;
    }
    out.set_host(Some(host)).ok()?;
    out.set_port(Some(port)).ok()?;
    out.set_path(&format!("/{db}"));
    // Force TLS but allow hostaddr to steer the TCP connection toward IPv6
    out.query_pairs_mut()
        .append_pair("sslmode", "require")
        .append_pair("hostaddr", ip_host);

    Some(out.to_string())
}

fn normalize_ipv6_host(raw: &str) -> String {
    let trimmed = raw.trim().trim_matches(|c| (c == '[' || c == ']'));
    if trimmed.contains(':') {
        format!("[{trimmed}]")
    } else {
        trimmed.to_string()
    }
}

fn redact_value(key: &str, val: &str) -> String {
    let k = key.to_ascii_uppercase();
    if k.contains("PASSWORD")
        || k.contains("SECRET")
        || k.contains("KEY")
        || k.contains("TOKEN")
        || k.contains("COOKIE")
    {
        return "***".to_string();
    }

    // Trim and normalize whitespace so we don't accidentally log credentials
    // when values contain newlines (e.g., copy/paste env mistakes).
    let val_trim = val.trim();

    // Always redact postgres DSNs even if the key isn't obviously sensitive
    // (e.g., SUPABASE_IPV6_DB).
    if let Ok(mut u) = url::Url::parse(val_trim) {
        let scheme = u.scheme().to_ascii_lowercase();
        if scheme == "postgres" || scheme == "postgresql" {
            let _ = u.set_username("***");
            let _ = u.set_password(Some("***"));
            return u.to_string();
        }
    }

    if k.contains("URL") || k.contains("DSN") {
        // Fallback: best-effort string redaction for postgres URLs.
        if val_trim.starts_with("postgres://") || val_trim.starts_with("postgresql://") {
            if let Some(proto) = val_trim.find("//") {
                if let Some(at) = val_trim[proto + 2..].find('@') {
                    let host_part = &val_trim[proto + 2 + at + 1..];
                    return format!("{}***:{}", &val_trim[..proto + 2], host_part);
                }
            }
            return "postgres://***".to_string();
        }
    }

    val_trim.to_string()
}

/// Validate required keys and log a consolidated, redacted snapshot of configuration.
/// Returns error if any required key is missing.
pub fn preflight_check(title: &str, required: &[&str], also_log: &[&str]) -> anyhow::Result<()> {
    init_env();
    let mut missing: Vec<&str> = Vec::new();
    for &k in required {
        if env_opt(k).is_none() {
            missing.push(k);
        }
    }
    let mut snapshot: Vec<(String, String)> = Vec::new();
    for &k in also_log {
        let v = env_opt(k).unwrap_or_default();
        snapshot.push((k.to_string(), redact_value(k, &v)));
    }
    info!(target = "preflight", title, snapshot = ?snapshot, "configuration snapshot");
    if !missing.is_empty() {
        return Err(anyhow::anyhow!(format!(
            "missing required env: {:?}",
            missing
        )));
    }
    Ok(())
}
