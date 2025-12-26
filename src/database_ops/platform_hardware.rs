use anyhow::Result;
use chrono::Utc;
use sqlx::FromRow;

use crate::database_ops::db::Db;

#[derive(Debug, FromRow)]
pub struct PlatformHardwareMapping {
    pub platform_id: i64,
    pub platform_code: Option<String>,
    pub platform_name: Option<String>,
    pub hardware_product_id: Option<i64>,
    pub mapped_at: Option<chrono::DateTime<Utc>>,
}

/// Convenience helper to fetch the platform -> hardware mapping.
/// Returns one row per platform (left join); hardware_product_id is None when no mapping exists.
pub async fn fetch_platform_hardware_map(db: &Db) -> Result<Vec<PlatformHardwareMapping>> {
    let q = r#"
      SELECT p.id AS platform_id,
             p.code AS platform_code,
             p.name AS platform_name,
             phm.hardware_product_id AS hardware_product_id,
             phm.created_at AS mapped_at
      FROM public.platforms p
      LEFT JOIN public.platform_hardware_map phm ON phm.platform_id = p.id
      ORDER BY p.id
    "#;

    let rows = sqlx::query_as::<_, PlatformHardwareMapping>(q)
        .fetch_all(&db.pool)
        .await?;

    Ok(rows)
}
