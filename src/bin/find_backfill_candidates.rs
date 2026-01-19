use anyhow::{Context, Result};
use chrono::Datelike;
use i_miss_rust::database_ops::db::Db;
use i_miss_rust::util::env;
use serde_json::{json, Value};
use sqlx::{Execute, Postgres, QueryBuilder, Row};
use tracing::{debug, info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    env::bootstrap_cli("find_backfill_candidates");
    let db_url = std::env::var("DATABASE_URL").or_else(|_| std::env::var("SUPABASE_DB_URL"))?;
    let db = Db::connect(
        &db_url,
        std::env::var("DB_MAX_CONNS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(150),
    )
    .await?;

    let year_min: i32 = std::env::var("YEAR_MIN")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2020);
    let year_max: i32 = std::env::var("YEAR_MAX")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2025);
    let limit: i64 = std::env::var("BACKFILL_LIMIT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000);

    info!(
        year_min,
        year_max, limit, "backfill: starting candidate scan"
    );
    info!(
        "remember: backfill scan (window {year_min}-{year_max} inclusive) — evaluating media & price coverage"
    );

    // Optional feature flags / filters
    let require_both_missing = env_bool("BACKFILL_REQUIRE_BOTH", false); // if true, candidate must miss BOTH media AND current_price
    let category_filter = std::env::var("BACKFILL_CATEGORY")
        .ok()
        .filter(|s| !s.is_empty());
    let outfile = std::env::var("BACKFILL_OUTFILE")
        .ok()
        .filter(|s| !s.is_empty());
    let order_mode =
        std::env::var("BACKFILL_ORDER_MODE").unwrap_or_else(|_| "priority".to_string()); // priority | release | alpha

    info!(
        require_both_missing,
        category_filter = category_filter.as_deref().unwrap_or("<none>"),
        outfile = outfile.as_deref().unwrap_or("<stdout>"),
        order_mode = %order_mode,
        max_conns = std::env::var("DB_MAX_CONNS").ok(),
        "backfill: environment configuration"
    );

    // Build dynamic query safely
    let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(
        r#"SELECT
                    vgt.product_id AS product_id,
                    p.slug AS product_slug,
                    p.name AS product_name,
                    vgt.id AS title_id,
                    vgt.title AS title,
                    vg.id AS video_game_id,
                    vg.slug AS video_game_slug,
                    vg.platform_id AS platform_id,
                    pf.name AS platform_name,
                    s.id AS sellable_id,
                    vg.sellable_id AS video_game_sellable_id,
                    vg.release_date AS release_date,
                    COALESCE(media.media_count, 0)::bigint AS media_count,
                    COALESCE(curr.current_price_count, 0)::bigint AS current_price_count,
                    COALESCE(hist.historical_price_count, 0)::bigint AS historical_price_count
                FROM public.video_games vg
                JOIN public.video_game_titles vgt ON vgt.id = vg.title_id
                LEFT JOIN public.products p ON p.id = vgt.product_id
                LEFT JOIN public.sellables s ON s.software_title_id = vgt.id
                LEFT JOIN public.platforms pf ON pf.id = vg.platform_id
                LEFT JOIN LATERAL (
                    SELECT COUNT(*)::bigint AS media_count
                    FROM public.game_media gm
                    WHERE gm.video_game_id = vg.id
                ) media ON TRUE
                LEFT JOIN LATERAL (
                    SELECT COUNT(*)::bigint AS current_price_count
                    FROM public.current_price cp
                    JOIN public.offer_jurisdictions oj ON oj.id = cp.offer_jurisdiction_id
                    JOIN public.offers o ON o.id = oj.offer_id
                    WHERE s.id IS NOT NULL
                        AND o.sellable_id = s.id
                ) curr ON TRUE
                LEFT JOIN LATERAL (
                    SELECT COUNT(*)::bigint AS historical_price_count
                    FROM public.prices pr
                    JOIN public.offer_jurisdictions oj2 ON oj2.id = pr.offer_jurisdiction_id
                    JOIN public.offers o2 ON o2.id = oj2.offer_id
                    WHERE s.id IS NOT NULL
                        AND o2.sellable_id = s.id
                ) hist ON TRUE
                WHERE vgt.title IS NOT NULL
                    AND (vg.release_date IS NULL OR EXTRACT(YEAR FROM vg.release_date)::int BETWEEN "#,
    );
    qb.push_bind(year_min)
        .push(" AND ")
        .push_bind(year_max)
        .push(")");

    if let Some(cat) = &category_filter {
        qb.push(" AND p.category = ").push_bind(cat);
    }

    // Missing criteria
    if require_both_missing {
        qb.push(
            " AND (COALESCE(media.media_count, 0) = 0 AND COALESCE(curr.current_price_count, 0) = 0)"
        );
    } else {
        qb.push(
            " AND (COALESCE(media.media_count, 0) = 0 OR COALESCE(curr.current_price_count, 0) = 0)"
        );
    }

    // Ordering strategy
    match order_mode.as_str() {
        "release" => {
            qb.push(" ORDER BY vg.release_date NULLS LAST, vgt.title ");
        }
        "alpha" => {
            qb.push(" ORDER BY vgt.title, vg.release_date NULLS LAST ");
        }
        _ => {
            // priority: candidates missing both first, then missing one; then most recent
            qb.push(
                " ORDER BY (
                    ((COALESCE(media.media_count, 0) = 0 AND COALESCE(curr.current_price_count, 0) = 0))::int DESC,
                    vg.release_date DESC NULLS LAST,
                    vgt.title
                )"
            );
        }
    }
    qb.push(" LIMIT ").push_bind(limit);

    let query = qb.build();
    debug!(sql = %query.sql(), "backfill: executing candidate query");
    let rows = query
        .fetch_all(&db.pool)
        .await
        .context("backfill query failed")?;

    info!(result_count = rows.len(), "backfill: query executed");

    // Query: find video_games whose canonical title is missing media records or price coverage
    if rows.is_empty() {
        info!(limit, "backfill: no candidates found");
        println!("backfill: no candidates found (limit={limit})");
        return Ok(());
    }

    // Optional file output
    let mut file_writer: Option<std::fs::File> = match &outfile {
        Some(path) => match std::fs::File::create(path) {
            Ok(f) => {
                info!(path = %path, "backfill: writing candidates to file");
                println!("writing candidates to {path}");
                Some(f)
            }
            Err(e) => {
                warn!(path = %path, error = %e, "backfill: failed to create output file");
                eprintln!("failed to create BACKFILL_OUTFILE={path}: {e}");
                None
            }
        },
        None => None,
    };

    let total = rows.len();
    for r in rows {
        let product_id = r.try_get::<Option<i64>, _>("product_id").ok().flatten();
        let product_slug = r
            .try_get::<Option<String>, _>("product_slug")
            .ok()
            .flatten();
        let product_name = r
            .try_get::<Option<String>, _>("product_name")
            .ok()
            .flatten();
        let title_id: i64 = r.get("title_id");
        let title_value = r.try_get::<Option<String>, _>("title").ok().flatten();
        let video_game_slug = r
            .try_get::<Option<String>, _>("video_game_slug")
            .ok()
            .flatten();
        let platform_id = r.try_get::<Option<i64>, _>("platform_id").ok().flatten();
        let platform_name = r
            .try_get::<Option<String>, _>("platform_name")
            .ok()
            .flatten();
        let sellable_id = r.try_get::<Option<i64>, _>("sellable_id").ok().flatten();
        let vg_sellable_id = r
            .try_get::<Option<i64>, _>("video_game_sellable_id")
            .ok()
            .flatten();
        let release_year = r
            .try_get::<Option<chrono::NaiveDate>, _>("release_date")
            .ok()
            .flatten()
            .map(|d| d.year())
            .unwrap_or(0);
        let media_count: i64 = r.try_get::<i64, _>("media_count").unwrap_or(0);
        let current_price_count: i64 = r.try_get::<i64, _>("current_price_count").unwrap_or(0);
        let historical_price_count: i64 =
            r.try_get::<i64, _>("historical_price_count").unwrap_or(0);
        let missing_media = media_count == 0;
        let missing_current_price = current_price_count == 0;
        let missing_hist = historical_price_count == 0;
        let priority =
            (missing_media as i32) + (missing_current_price as i32) + (missing_hist as i32);
        let obj = json!({
            "product_id": product_id,
            "slug": product_slug.clone(),
            "name": product_name.clone(),
            "product_slug": product_slug,
            "product_name": product_name,
            "title_id": title_id,
            "title": title_value,
            "video_game_id": r.get::<i64, _>("video_game_id"),
            "video_game_slug": video_game_slug,
            "platform_id": platform_id,
            "platform_name": platform_name,
            "sellable_id": sellable_id,
            "video_game_sellable_id": vg_sellable_id,
            "release_year": if release_year == 0 { Value::Null } else { json!(release_year) },
            "media_count": media_count,
            "current_price_count": current_price_count,
            "historical_price_count": historical_price_count,
            "missing_media": missing_media,
            "missing_current_price": missing_current_price,
            "missing_historical_prices": missing_hist,
            "priority_score": priority
        });
        debug!(
            sellable_id,
            title_id,
            video_game_slug = video_game_slug.as_deref().unwrap_or("<none>"),
            platform_id,
            platform_name = platform_name.as_deref().unwrap_or("<none>"),
            missing_media,
            missing_current_price,
            missing_hist,
            priority,
            "backfill: candidate emitted"
        );
        println!("backfill-candidate: {}", obj);
        if let Some(f) = file_writer.as_mut() {
            use std::io::Write;
            let _ = writeln!(f, "{}", obj);
        }
    }

    info!(total_candidates = total, limit, "backfill: scan complete");
    println!("summary: emitted {} candidates (limit={limit})", total);

    Ok(())
}

fn env_bool(k: &str, default: bool) -> bool {
    std::env::var(k)
        .ok()
        .map(|v| (v.eq_ignore_ascii_case("true") || v == "1"))
        .unwrap_or(default)
}
