use anyhow::{Context, Result};
use i_miss_rust::database_ops::db::Db; // for existing helpers only (reuse pool)
use i_miss_rust::database_ops::ingest_providers::{
    ensure_provider_item, ensure_provider_media_links_with_meta,
};
use i_miss_rust::util::env as env_util;
use serde_json::Value;
use sqlx::Row;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use std::{env, str::FromStr};

#[tokio::main]
async fn main() -> Result<()> {
    env_util::bootstrap_cli("media_provider_backfill");
    let db_url = env::var("SUPABASE_DB_URL")
        .or_else(|_| env::var("DATABASE_URL"))
        .or_else(|_| env::var("DB_URL"))?;
    let connect_opts = PgConnectOptions::from_str(&db_url)?.statement_cache_capacity(0);
    let pool = PgPoolOptions::new()
        .max_connections(6)
        .connect_with(connect_opts)
        .await?;
    let db = Db { pool }; // minimal wrapper (Db::connect_no_migrate already performed earlier in import flow; we only need pool here)

    // Limit rows processed (default 500)
    let limit: i64 = env::var("BACKFILL_LIMIT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(500);

    // Select game_media rows that have a known provider (source matches providers.slug) but no provider_media_links entry
    // Reconstruct provider_item and provider_media_links idempotently.
    let rows = sqlx
        ::query(
            r#"SELECT gm.id, gm.video_game_id, gm.source::text AS source, gm.external_id, gm.url, gm.kind, gm.meta
            FROM public.game_media gm
            JOIN public.providers p ON p.slug = lower(gm.source::text)
            LEFT JOIN public.provider_items pi
               ON pi.provider_id = p.id AND pi.external_id = 'media:' || COALESCE(gm.external_id, gm.url)
            LEFT JOIN public.provider_media_links pml
               ON pml.provider_item_id = pi.id AND pml.url = gm.url
            WHERE gm.source IS NOT NULL AND gm.source <> '' AND pml.id IS NULL
            LIMIT $1"#
        )
        .bind(limit)
        .persistent(false)
        .fetch_all(&db.pool).await?;

    if rows.is_empty() {
        println!("no media backfill gaps (0 rows)");
        return Ok(());
    }

    println!(
        "backfilling provider_media_links for {} media rows (limit={})",
        rows.len(),
        limit
    );
    let mut processed = 0usize;
    let mut errors = 0usize;
    for r in rows {
        let gm_id: i64 = r.get("id");
        let vg_id: i64 = r.get("video_game_id");
        let source: String = r.get("source");
        let external_id: Option<String> = r.try_get("external_id").ok();
        let url: String = r.get("url");
        let kind: Option<String> = r.try_get("kind").ok();
        let meta_val: Option<Value> = r.try_get::<Option<Value>, _>("meta").ok().flatten();

        // Provider id lookup
        let provider_id: i64 =
            match sqlx::query_scalar::<_, i64>("SELECT id FROM public.providers WHERE slug=$1")
                .bind(&source)
                .persistent(false)
                .fetch_optional(&db.pool)
                .await?
            {
                Some(id) => id,
                None => {
                    errors += 1;
                    continue;
                }
            };

        let pi_external = format!(
            "media:{}",
            external_id.clone().unwrap_or_else(|| url.clone())
        );
        let provider_item_id =
            match ensure_provider_item(&db, provider_id, &pi_external, None).await {
                Ok(id) => id,
                Err(e) => {
                    eprintln!("error ensure_provider_item gm_id={gm_id}: {e}");
                    errors += 1;
                    continue;
                }
            };
        let url_kind_pair = (url.clone(), kind.clone());
        let meta_clone = meta_val.clone();
        if let Err(e) = ensure_provider_media_links_with_meta(
            &db,
            provider_item_id,
            Some(vg_id),
            &[(url_kind_pair.0.clone(), url_kind_pair.1.clone(), None, None)],
            &source,
            meta_clone,
        )
        .await
        {
            eprintln!("error ensure_provider_media_links gm_id={gm_id}: {e}");
            errors += 1;
            continue;
        }
        processed += 1;
    }
    println!("media provider backfill complete: processed={processed}, errors={errors}");
    Ok(())
}
