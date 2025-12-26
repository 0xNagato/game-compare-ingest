use anyhow::Result;
use i_miss_rust::database_ops::db::Db;
use i_miss_rust::util::env;
use sqlx::Row;
use tracing::info;

// Restores deleted platform duplicates from platforms_backup using platforms_dedupe_map.
// Only run if a rollback is required; assumes no conflicting canonical_code uniqueness during restore.
#[tokio::main]
async fn main() -> Result<()> {
    env::bootstrap_cli("platforms_restore");

    let pg_url = std::env::var("DATABASE_URL")
        .or_else(|_| std::env::var("SUPABASE_DB_URL"))?
        .to_string();
    let max_conns = std::env::var("DB_MAX_CONNS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8);
    let db = Db::connect(&pg_url, max_conns).await?;

    // Ensure required tables exist.
    for tbl in ["platforms_backup", "platforms_dedupe_map"] {
        let exists: Option<String> = sqlx
            ::query_scalar(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_name=$1"
            )
            .persistent(false)
            .bind(tbl)
            .fetch_optional(&db.pool).await?;
        if exists.is_none() {
            anyhow::bail!("Required table '{}' not found; cannot restore", tbl);
        }
    }

    // Find duplicates previously mapped (dupe_id present, platform row missing now)
    let rows = sqlx
        ::query(
            "SELECT m.dupe_id, b.name, b.code, b.canonical_code FROM platforms_dedupe_map m JOIN platforms_backup b ON b.original_id = m.dupe_id LEFT JOIN platforms p ON p.id = m.dupe_id WHERE p.id IS NULL"
        )
        .persistent(false)
        .fetch_all(&db.pool).await?;

    if rows.is_empty() {
        info!("No missing duplicate platforms to restore");
        return Ok(());
    }

    use sqlx::QueryBuilder;
    let mut qb =
        QueryBuilder::new("INSERT INTO public.platforms (id, name, code, canonical_code) VALUES ");
    let mut sep = qb.separated(", ");
    for r in rows.iter() {
        let id: i64 = r.get("dupe_id");
        let name: String = r.get("name");
        let code: Option<String> = r.try_get("code").ok();
        let canonical: Option<String> = r.try_get("canonical_code").ok();
        sep.push("(")
            .push_bind(id)
            .push(", ")
            .push_bind(name)
            .push(", ")
            .push_bind(code)
            .push(", ")
            .push_bind(canonical)
            .push(")");
    }
    qb.push(" ON CONFLICT (id) DO NOTHING");
    qb.build().execute(&db.pool).await?;
    info!(restored = rows.len(), "platform duplicates restored");
    Ok(())
}
