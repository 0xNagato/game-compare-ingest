use anyhow::Result;
use i_miss_rust::database_ops::db::Db;
use i_miss_rust::util::env;
use sqlx::Row;
use std::collections::HashMap;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    env::bootstrap_cli("platforms_dedupe");
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        .init();

    let pg_url = std::env::var("DATABASE_URL")
        .or_else(|_| std::env::var("SUPABASE_DB_URL"))
        .expect("DATABASE_URL or SUPABASE_DB_URL required");
    let max_conns = std::env::var("DB_MAX_CONNS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8);
    let db = Db::connect(&pg_url, max_conns).await?;

    let dry_run = std::env::var("DRY_RUN")
        .map(|v| (v == "1" || v.eq_ignore_ascii_case("true")))
        .unwrap_or(false);

    ensure_canonical_column(&db).await?;
    maybe_backup_platforms(&db).await?; // create snapshot before destructive ops

    // Load platforms
    let rows = sqlx::query("SELECT id, name, code, canonical_code FROM public.platforms")
        .persistent(false)
        .fetch_all(&db.pool)
        .await?;

    #[derive(Debug, Clone)]
    struct PRow {
        id: i64,
        name: String,
        code: Option<String>,
        canonical: String,
    }
    let mut groups: HashMap<String, Vec<PRow>> = HashMap::new();
    for r in rows {
        let id: i64 = r.get("id");
        let name: String = r.get("name");
        let code: Option<String> = r.try_get("code").ok();
        let canonical: Option<String> = r.try_get("canonical_code").ok();
        let canonical = canonical.unwrap_or_else(|| canonicalize(&code.as_ref().unwrap_or(&name)));
        groups.entry(canonical.clone()).or_default().push(PRow {
            id,
            name,
            code,
            canonical,
        });
    }

    let mut total_dupe_sets = 0usize;
    let mut total_rows_removed = 0usize;
    for (canon, mut vecs) in groups {
        if vecs.len() <= 1 {
            continue;
        }
        total_dupe_sets += 1;
        // Select canonical row: prefer one with a short code (2-8 chars) else lowest id.
        vecs.sort_by_key(|p| (p.code.as_ref().map(|c| c.len()).unwrap_or(999), p.id));
        let canonical_row = vecs.remove(0);
        let duplicates = vecs; // remaining
        info!(canonical_id=canonical_row.id, %canon, dup_count=duplicates.len(), "dedupe set identified");
        for dup in duplicates {
            total_rows_removed += 1;
            if dry_run {
                info!(dup_id=dup.id, keep_id=canonical_row.id, name=%dup.name, "dry-run: would merge & remove duplicate platform");
                continue;
            }
            // Repoint foreign keys (video_games)
            let updated =
                sqlx::query("UPDATE public.video_games SET platform_id=$1 WHERE platform_id=$2")
                    .persistent(false)
                    .bind(canonical_row.id)
                    .bind(dup.id)
                    .execute(&db.pool)
                    .await?;
            info!(
                rows_updated = updated.rows_affected(),
                dup_id = dup.id,
                keep_id = canonical_row.id,
                "repointed video_games"
            );
            // Delete duplicate
            let deleted = sqlx::query("DELETE FROM public.platforms WHERE id=$1")
                .persistent(false)
                .bind(dup.id)
                .execute(&db.pool)
                .await?;
            if deleted.rows_affected() == 0 {
                warn!(dup_id = dup.id, "expected platform delete affected 1 row");
            }
        }
    }

    if dry_run {
        info!(
            dupe_sets = total_dupe_sets,
            rows_would_remove = total_rows_removed,
            "dry-run complete (no changes applied)"
        );
    } else {
        info!(
            dupe_sets = total_dupe_sets,
            rows_removed = total_rows_removed,
            "platform dedupe complete"
        );
        // Attempt to add unique index (will fail if still duplicates). Ignore errors silently.
        let _ = sqlx
            ::query(
                "CREATE UNIQUE INDEX IF NOT EXISTS platforms_canonical_code_uq ON public.platforms (canonical_code)"
            )
            .persistent(false)
            .execute(&db.pool).await;
        let _ = sqlx::query("ANALYZE public.platforms")
            .persistent(false)
            .execute(&db.pool)
            .await;
    }
    Ok(())
}

async fn ensure_canonical_column(db: &Db) -> Result<()> {
    let exists: Option<String> = sqlx
        ::query_scalar(
            "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='platforms' AND column_name='canonical_code'"
        )
        .persistent(false)
        .fetch_optional(&db.pool).await?;
    if exists.is_none() {
        let _ = sqlx::query(
            "ALTER TABLE public.platforms ADD COLUMN IF NOT EXISTS canonical_code text",
        )
        .persistent(false)
        .execute(&db.pool)
        .await;
    }
    let _ = sqlx
        ::query(
            "UPDATE public.platforms SET canonical_code = lower(regexp_replace(coalesce(code,name),'[^a-z0-9]','','g')) WHERE canonical_code IS NULL"
        )
        .persistent(false)
        .execute(&db.pool).await?;
    Ok(())
}

fn canonicalize(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        }
    }
    out
}

// Backup existing platform rows into platforms_backup (idempotent).
async fn maybe_backup_platforms(db: &Db) -> Result<()> {
    let do_backup = std::env::var("PLATFORMS_BACKUP")
        .map(|v| (v == "1" || v.eq_ignore_ascii_case("true")))
        .unwrap_or(true); // default on for safety
    if !do_backup {
        return Ok(());
    }
    // Ensure backup table exists (migration should have created it; fallback if not).
    let _ = sqlx
        ::query(
            "CREATE TABLE IF NOT EXISTS public.platforms_backup (backup_id bigserial PRIMARY KEY, original_id bigint NOT NULL, name text NOT NULL, code text, canonical_code text, backed_up_at timestamptz NOT NULL DEFAULT now())"
        )
        .persistent(false)
        .execute(&db.pool).await;
    let _ = sqlx
        ::query(
            "CREATE UNIQUE INDEX IF NOT EXISTS platforms_backup_original_id_uq ON public.platforms_backup(original_id)"
        )
        .persistent(false)
        .execute(&db.pool).await;
    // Insert missing snapshots
    let inserted = sqlx
        ::query(
            "INSERT INTO public.platforms_backup(original_id,name,code,canonical_code) SELECT p.id,p.name,p.code,p.canonical_code FROM public.platforms p LEFT JOIN public.platforms_backup b ON b.original_id=p.id WHERE b.original_id IS NULL"
        )
        .persistent(false)
        .execute(&db.pool).await?
        .rows_affected();
    tracing::info!(inserted, "platform backup snapshot applied");
    Ok(())
}
