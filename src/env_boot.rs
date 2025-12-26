use dotenv::dotenv;

/// Load .env from current working directory; if missing, try the project root.
pub fn ensure_dotenv() {
    if dotenv().is_ok() {
        return;
    }
    // Fallback to Cargo project root
    let root = env!("CARGO_MANIFEST_DIR");
    let candidate = format!("{}/.env", root);
    let _ = dotenv::from_filename(candidate);
}
