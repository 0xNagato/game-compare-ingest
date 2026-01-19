use anyhow::Result;
use serde_json::json;
use sqlx::Row;
use tracing::info;

use crate::database_ops::db::Db;
use crate::util::env as env_util;

#[derive(Debug, Clone, Default)]
pub struct DbMissingStatsConfig {
    /// Optional override for the Postgres connection string.
    pub database_url: Option<String>,
}

pub async fn run(cfg: DbMissingStatsConfig) -> Result<()> {
    env_util::init_env();

    let database_url = if let Some(url) = cfg.database_url.clone() {
        url
    } else {
        env_util::db_url()?
    };
    let db = Db::connect(&database_url, 5).await?;

    let missing_price: i64 = sqlx::query(
        r#"
        SELECT COUNT(*)::BIGINT FROM public.video_games vg
        WHERE NOT EXISTS (
            SELECT 1 FROM public.products p
              JOIN public.sellables s ON s.product_id = p.id
              JOIN public.offers o ON o.sellable_id = s.id
              JOIN public.offer_jurisdictions oj ON oj.offer_id = o.id
              JOIN public.current_price cp ON cp.offer_jurisdiction_id = oj.id
            WHERE p.id = vg.title_id
        )
    "#,
    )
    .persistent(false)
    .fetch_one(&db.pool)
    .await?
    .get(0);

    let no_provider_items: i64 = sqlx::query(
        r#"
        SELECT COUNT(*)::BIGINT FROM public.products p
        WHERE NOT EXISTS (
            SELECT 1 FROM public.sellables s
              JOIN public.offers o ON o.sellable_id = s.id
              JOIN public.provider_offers po ON po.offer_id = o.id
            WHERE s.product_id = p.id
        )
    "#,
    )
    .persistent(false)
    .fetch_one(&db.pool)
    .await?
    .get(0);

    let no_ratings: i64 = sqlx::query(
        r#"
        SELECT COUNT(*)::BIGINT FROM public.video_games vg
        WHERE vg.average_rating IS NULL
    "#,
    )
    .persistent(false)
    .fetch_one(&db.pool)
    .await?
    .get(0);

    let out = json!({
        "missing_current_price_video_games": missing_price,
        "products_without_provider_items": no_provider_items,
        "video_games_without_ratings": no_ratings,
    });
    println!("{}", serde_json::to_string_pretty(&out)?);
    info!("db_missing_stats done");
    Ok(())
}
