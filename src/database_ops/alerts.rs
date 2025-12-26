use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::Row;
use tracing::instrument;

use crate::database_ops::db::Db;

#[derive(Debug)]
pub struct AlertTrigger {
    pub alert_id: i64,
    pub user_id: i64,
    pub offer_jurisdiction_id: i64,
    pub threshold_minor: i64,
    pub current_amount_minor: i64,
    pub op: String,
    pub triggered_at: DateTime<Utc>,
}

#[instrument(skip(db, offer_jurisdiction_ids))]
pub async fn evaluate_alerts(db: &Db, offer_jurisdiction_ids: &[i64]) -> Result<Vec<AlertTrigger>> {
    if offer_jurisdiction_ids.is_empty() {
        return Ok(vec![]);
    }
    let rows = sqlx::query(r#"
        SELECT a.id, a.user_id, a.offer_jurisdiction_id, a.threshold_minor, a.op, cp.amount_minor, cp.recorded_at
        FROM public.alerts a
        JOIN public.current_price cp ON cp.offer_jurisdiction_id = a.offer_jurisdiction_id
        WHERE a.active = true AND a.offer_jurisdiction_id = ANY($1)
    "#)
        .bind(offer_jurisdiction_ids)
        .fetch_all(&db.pool).await?;

    let mut triggers = Vec::new();
    for r in rows {
        let threshold: i64 = r.get("threshold_minor");
        let current: i64 = r.get("amount_minor");
        let op: String = r.get("op");
        // cmp_op enum is defined as ('above','below'); keep backward-compatible shorthands.
        let passes = match op.to_ascii_lowercase().as_str() {
            "below" | "lt" => current < threshold,
            "lte" => current <= threshold,
            "above" | "gt" => current > threshold,
            "gte" => current >= threshold,
            _ => false,
        };
        if passes {
            triggers.push(AlertTrigger {
                alert_id: r.get("id"),
                user_id: r.get("user_id"),
                offer_jurisdiction_id: r.get("offer_jurisdiction_id"),
                threshold_minor: threshold,
                current_amount_minor: current,
                op,
                triggered_at: r.get("recorded_at"),
            });
        }
    }
    Ok(triggers)
}
