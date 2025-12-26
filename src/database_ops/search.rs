use crate::database_ops::db::Db;
use anyhow::Result;
use sqlx::Row;

#[derive(Debug, Clone)]
pub struct SearchHit {
    pub kind: String, // "sellable" | "provider_item"
    pub id: i64,
    pub title: String,
    pub extra: Option<String>, // external id or retailer
}

pub async fn search_games(db: &Db, q: &str, limit: i64) -> Result<Vec<SearchHit>> {
    let pattern = format!("%{}%", q);

    // Sellables by title
    let sellables = sqlx::query(
        r#"
        SELECT 'sellable' AS kind, s.id, s.title, NULL::text AS extra
    FROM sellables s
        WHERE s.title ILIKE $1
        ORDER BY similarity(s.title, $2) DESC
        LIMIT $3
        "#,
    )
    .bind(&pattern)
    .bind(q)
    .bind(limit)
    .fetch_all(&db.pool)
    .await?;

    // Provider items by external id or payload->name
    let provider_items = sqlx::query(
        r#"
        SELECT 'provider_item' AS kind, pi.id, COALESCE(pi.payload->>'name', pi.external_item_id) AS title, pi.external_item_id AS extra
    FROM provider_items pi
        WHERE pi.external_item_id ILIKE $1 OR (pi.payload->>'name') ILIKE $1
        ORDER BY GREATEST(similarity(pi.external_item_id, $2), similarity(COALESCE(pi.payload->>'name',''), $2)) DESC
        LIMIT $3
        "#,
    )
    .bind(&pattern)
    .bind(q)
    .bind(limit)
    .fetch_all(&db.pool)
    .await?;

    let mut out = Vec::with_capacity(sellables.len() + provider_items.len());
    for r in sellables {
        out.push(SearchHit {
            kind: "sellable".into(),
            id: r.get("id"),
            title: r.get("title"),
            extra: None,
        });
    }
    for r in provider_items {
        out.push(SearchHit {
            kind: "provider_item".into(),
            id: r.get("id"),
            title: r.get("title"),
            extra: r.try_get("extra").ok(),
        });
    }
    Ok(out)
}

pub async fn search_match_both(
    db: &Db,
    q: &str,
    limit: i64,
) -> Result<(Vec<SearchHit>, Vec<SearchHit>)> {
    // Return both lists so callers can cross-reference
    let hits = search_games(db, q, limit).await?;
    let sellables: Vec<SearchHit> = hits
        .iter()
        .cloned()
        .filter(|h| h.kind == "sellable")
        .collect();
    let provider_items: Vec<SearchHit> = hits
        .into_iter()
        .filter(|h| h.kind == "provider_item")
        .collect();
    Ok((sellables, provider_items))
}
