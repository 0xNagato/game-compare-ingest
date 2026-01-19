# N+1 Query Optimization Patch for ingest_providers.rs

This document contains optimized versions of three functions that currently have N+1 query patterns.

## Overview

The optimizations convert individual INSERT operations within loops to batch operations using PostgreSQL's UNNEST feature. This reduces database round-trips from N to 1 for each function.

### Performance Impact
- **ensure_provider_media_links_with_meta**: Reduces from N individual INSERTs to 1 batch INSERT
- **update_video_game_global_rating_if_null**: Reduces from 3 individual UPDATEs to 1 batch UPDATE
- Batch offer/jurisdiction creation: Can be added as new helper functions for common batch scenarios

## 1. Optimized ensure_provider_media_links_with_meta

**Location**: `src/database_ops/ingest_providers.rs:5289`

**Problem**: The current implementation loops through media URLs and executes individual INSERT statements for each URL (lines 5321-5443).

**Solution**: Build arrays of all values, then execute a single INSERT using UNNEST to expand the arrays.

### Implementation

Replace the entire function (lines 5289-5447) with this optimized version:

```rust
#[instrument(skip(db, urls, meta))]
pub async fn ensure_provider_media_links_with_meta(
    db: &Db,
    provider_item_id: i64,
    video_game_id: Option<i64>,
    urls: &[(String, Option<String>, Option<String>, Option<String>)],
    source: &str,
    meta: Option<Value>,
) -> Result<usize> {
    if provider_item_id == 0 {
        return Ok(0);
    }
    if !provider_media_links_present(db).await.unwrap_or(false) {
        return Ok(0);
    }
    let supports_psstore = media_source_supports_psstore(db).await.unwrap_or(false);
    let supports_background = media_type_supports_background(db).await.unwrap_or(false);
    let source_norm = normalize_media_source_for_db(source, supports_psstore);

    let has_sort_order = table_column_exists(db, "provider_media_links", "sort_order")
        .await
        .unwrap_or(false);
    let has_position = if has_sort_order {
        false
    } else {
        table_column_exists(db, "provider_media_links", "position")
            .await
            .unwrap_or(false)
    };

    let metadata_base = meta.as_ref().filter(|v| !v.is_null()).cloned();

    // Pre-process all URLs and build arrays for batch insert
    // This replaces the individual INSERT loop with data preparation
    let mut seen: HashSet<String> = HashSet::new();
    let mut batch_urls: Vec<String> = Vec::new();
    let mut batch_video_game_ids: Vec<Option<i64>> = Vec::new();
    let mut batch_sources: Vec<Option<String>> = Vec::new();
    let mut batch_media_types: Vec<Option<String>> = Vec::new();
    let mut batch_roles: Vec<Option<String>> = Vec::new();
    let mut batch_titles: Vec<Option<String>> = Vec::new();
    let mut batch_sort_orders: Vec<i32> = Vec::new();
    let mut batch_metadata: Vec<Value> = Vec::new();

    for (idx, (url, media_type_raw, role_raw, title_raw)) in urls.iter().enumerate() {
        let trimmed = url.trim();
        if trimmed.is_empty() {
            continue;
        }
        let normalized = trimmed.to_string();
        if !seen.insert(normalized.clone()) {
            continue;
        }
        let media_type_candidate = media_type_raw
            .as_ref()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty());
        let role_candidate = role_raw
            .as_ref()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty());

        let (prio, group) = media_group_priority(media_type_candidate, role_candidate, &normalized);
        let sort_order: i32 = prio.saturating_mul(1000) + (idx as i32);

        let media_type = media_type_candidate
            .map(|s| normalize_media_type_for_db(s, supports_background).to_ascii_lowercase());

        let role = normalize_provider_media_role(group, role_candidate);

        let row_meta: Value = if !has_sort_order && !has_position {
            let mut row_meta: Value = metadata_base.clone().unwrap_or_else(|| json!({}));
            if let Value::Object(map) = &mut row_meta {
                map.insert("gc_sort_order".to_string(), json!(sort_order));
                map.insert("gc_media_group".to_string(), json!(group));
            }
            row_meta
        } else {
            metadata_base.clone().unwrap_or_else(|| json!({}))
        };

        let title = title_raw
            .as_ref()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());

        // Accumulate values for batch insert
        batch_urls.push(normalized);
        batch_video_game_ids.push(video_game_id);
        batch_sources.push(source_norm.clone());
        batch_media_types.push(media_type);
        batch_roles.push(role);
        batch_titles.push(title);
        batch_sort_orders.push(sort_order);
        batch_metadata.push(row_meta);
    }

    if batch_urls.is_empty() {
        return Ok(0);
    }

    let count = batch_urls.len();

    // Execute single batch INSERT using UNNEST to expand arrays
    // UNNEST transforms arrays into rows, allowing us to insert all media links in one query
    // The ON CONFLICT clause maintains idempotency - re-running is safe
    if has_sort_order {
        sqlx::query(
            "INSERT INTO provider_media_links
                (provider_item_id, video_game_id, url, source, media_type, role, title, sort_order, metadata)
             SELECT
                $1::bigint,
                video_game_id,
                url,
                source,
                media_type,
                role,
                title,
                sort_order,
                COALESCE(metadata, '{}'::jsonb)
             FROM UNNEST(
                $2::bigint[],
                $3::text[],
                $4::text[],
                $5::text[],
                $6::text[],
                $7::text[],
                $8::int[],
                $9::jsonb[]
             ) AS t(video_game_id, url, source, media_type, role, title, sort_order, metadata)
             ON CONFLICT (provider_item_id, url) DO UPDATE
             SET video_game_id = COALESCE(EXCLUDED.video_game_id, provider_media_links.video_game_id),
                 source = COALESCE(EXCLUDED.source, provider_media_links.source),
                 media_type = COALESCE(EXCLUDED.media_type, provider_media_links.media_type),
                 role = COALESCE(EXCLUDED.role, provider_media_links.role),
                 title = COALESCE(EXCLUDED.title, provider_media_links.title),
                 sort_order = COALESCE(EXCLUDED.sort_order, provider_media_links.sort_order),
                 metadata = COALESCE(provider_media_links.metadata, '{}'::jsonb) || COALESCE(EXCLUDED.metadata, '{}'::jsonb),
                 updated_at = now()",
        )
        .persistent(false)
        .bind(provider_item_id)
        .bind(&batch_video_game_ids)
        .bind(&batch_urls)
        .bind(&batch_sources)
        .bind(&batch_media_types)
        .bind(&batch_roles)
        .bind(&batch_titles)
        .bind(&batch_sort_orders)
        .bind(&batch_metadata)
        .execute(&db.pool)
        .await?;
    } else if has_position {
        sqlx::query(
            "INSERT INTO provider_media_links
                (provider_item_id, video_game_id, url, source, media_type, role, title, position, metadata)
             SELECT
                $1::bigint,
                video_game_id,
                url,
                source,
                media_type,
                role,
                title,
                sort_order,
                COALESCE(metadata, '{}'::jsonb)
             FROM UNNEST(
                $2::bigint[],
                $3::text[],
                $4::text[],
                $5::text[],
                $6::text[],
                $7::text[],
                $8::int[],
                $9::jsonb[]
             ) AS t(video_game_id, url, source, media_type, role, title, sort_order, metadata)
             ON CONFLICT (provider_item_id, url) DO UPDATE
             SET video_game_id = COALESCE(EXCLUDED.video_game_id, provider_media_links.video_game_id),
                 source = COALESCE(EXCLUDED.source, provider_media_links.source),
                 media_type = COALESCE(EXCLUDED.media_type, provider_media_links.media_type),
                 role = COALESCE(EXCLUDED.role, provider_media_links.role),
                 title = COALESCE(EXCLUDED.title, provider_media_links.title),
                 position = COALESCE(EXCLUDED.position, provider_media_links.position),
                 metadata = COALESCE(provider_media_links.metadata, '{}'::jsonb) || COALESCE(EXCLUDED.metadata, '{}'::jsonb),
                 updated_at = now()",
        )
        .persistent(false)
        .bind(provider_item_id)
        .bind(&batch_video_game_ids)
        .bind(&batch_urls)
        .bind(&batch_sources)
        .bind(&batch_media_types)
        .bind(&batch_roles)
        .bind(&batch_titles)
        .bind(&batch_sort_orders)
        .bind(&batch_metadata)
        .execute(&db.pool)
        .await?;
    } else {
        sqlx::query(
            "INSERT INTO provider_media_links
                (provider_item_id, video_game_id, url, source, media_type, role, title, metadata)
             SELECT
                $1::bigint,
                video_game_id,
                url,
                source,
                media_type,
                role,
                title,
                COALESCE(metadata, '{}'::jsonb)
             FROM UNNEST(
                $2::bigint[],
                $3::text[],
                $4::text[],
                $5::text[],
                $6::text[],
                $7::text[],
                $8::jsonb[]
             ) AS t(video_game_id, url, source, media_type, role, title, metadata)
             ON CONFLICT (provider_item_id, url) DO UPDATE
             SET video_game_id = COALESCE(EXCLUDED.video_game_id, provider_media_links.video_game_id),
                 source = COALESCE(EXCLUDED.source, provider_media_links.source),
                 media_type = COALESCE(EXCLUDED.media_type, provider_media_links.media_type),
                 role = COALESCE(EXCLUDED.role, provider_media_links.role),
                 title = COALESCE(EXCLUDED.title, provider_media_links.title),
                 metadata = COALESCE(provider_media_links.metadata, '{}'::jsonb) || COALESCE(EXCLUDED.metadata, '{}'::jsonb),
                 updated_at = now()",
        )
        .persistent(false)
        .bind(provider_item_id)
        .bind(&batch_video_game_ids)
        .bind(&batch_urls)
        .bind(&batch_sources)
        .bind(&batch_media_types)
        .bind(&batch_roles)
        .bind(&batch_titles)
        .bind(&batch_metadata)
        .execute(&db.pool)
        .await?;
    }

    Ok(count)
}
```

**Key Changes**:
- Replaced individual INSERT loop with data accumulation into Vec arrays
- Single batch INSERT using UNNEST to expand arrays into rows
- Maintains all existing conflict resolution logic
- Preserves idempotency
- Same function signature (no caller changes needed)

## 2. Optimized update_video_game_global_rating_if_null

**Location**: `src/database_ops/ingest_providers.rs:4692`

**Problem**: The current implementation executes 3 separate UPDATE statements sequentially for `average_rating`, `rating_count`, and `rating_updated_at` (lines 4703-4734).

**Solution**: Combine into a single UPDATE statement that updates all three columns at once.

### Implementation

Replace the function (lines 4692-4737) with this optimized version:

```rust
// Best-effort: set global rating fields if present and currently null.
// Optimized to use a single UPDATE instead of 3 separate queries.
pub async fn update_video_game_global_rating_if_null(
    db: &Db,
    video_game_id: i64,
    average_rating: Option<f64>,
    rating_count: Option<i64>,
) -> Result<()> {
    let cols = video_games_content_columns(db).await?;
    if !(cols.has_average_rating || cols.has_rating_count || cols.has_rating_updated_at) {
        return Ok(());
    }

    // Build dynamic SQL to update only the columns that exist in the schema
    // This handles schema evolution gracefully while batching all updates into one query
    let mut updates: Vec<String> = Vec::new();
    let mut bind_idx = 2; // $1 is video_game_id

    if cols.has_average_rating && average_rating.is_some() {
        updates.push(format!("average_rating = COALESCE(average_rating, ${})", bind_idx));
        bind_idx += 1;
    }

    if cols.has_rating_count && rating_count.is_some() {
        updates.push(format!("rating_count = COALESCE(rating_count, ${})", bind_idx));
        bind_idx += 1;
    }

    if cols.has_rating_updated_at && (average_rating.is_some() || rating_count.is_some()) {
        updates.push("rating_updated_at = COALESCE(rating_updated_at, now())".to_string());
    }

    if updates.is_empty() {
        return Ok(());
    }

    // Single UPDATE batches all column updates together
    // COALESCE ensures we only update NULL values (preserves existing data)
    let sql = format!(
        "UPDATE video_games SET {} WHERE id = $1",
        updates.join(", ")
    );

    let mut query = sqlx::query(&sql).persistent(false).bind(video_game_id);

    if cols.has_average_rating && average_rating.is_some() {
        query = query.bind(average_rating);
    }

    if cols.has_rating_count && rating_count.is_some() {
        query = query.bind(rating_count);
    }

    best_effort_execute(db, query, "global_rating_batch_update").await;

    Ok(())
}
```

**Key Changes**:
- Combines 3 separate UPDATEs into 1 batch UPDATE
- Dynamically builds SQL based on which columns exist in schema
- Still uses COALESCE to only update NULL values
- Reduces database round-trips from 3 to 1
- Maintains backward compatibility with schema variations

## 3. Batch Offer/Jurisdiction Creation Helpers

**Problem**: Code that creates multiple offers and offer_jurisdictions sequentially can benefit from batch operations.

**Solution**: Add new helper functions for common batch scenarios.

### New Helper Functions

Add these new functions after the existing `ensure_offer_jurisdiction` function (around line 3500):

```rust
/// Batch create or retrieve multiple offers.
/// Returns a Vec of offer IDs in the same order as input.
///
/// This is optimized for the common pattern of creating many offers at once.
/// Uses a single batch INSERT with UNNEST instead of N individual queries.
#[instrument(skip(db, offers))]
pub async fn ensure_offers_batch(
    db: &Db,
    offers: &[(i64, i64, Option<&str>)], // (sellable_id, retailer_id, sku)
) -> Result<Vec<i64>> {
    if offers.is_empty() {
        return Ok(Vec::new());
    }

    // For PHP compat mode, fall back to individual queries
    if php_compat_schema(db).await.unwrap_or(false) {
        let mut result = Vec::with_capacity(offers.len());
        for (sellable_id, retailer_id, sku) in offers {
            let id = ensure_offer(db, *sellable_id, *retailer_id, *sku).await?;
            result.push(id);
        }
        return Ok(result);
    }

    // Prepare arrays for batch processing
    let sellable_ids: Vec<i64> = offers.iter().map(|(s, _, _)| *s).collect();
    let retailer_ids: Vec<i64> = offers.iter().map(|(_, r, _)| *r).collect();
    let skus: Vec<Option<String>> = offers.iter()
        .map(|(_, _, s)| s.map(|x| x.to_string()))
        .collect();

    // Batch INSERT using UNNEST to expand arrays
    // First, try to get existing IDs, then insert missing ones
    // The DISTINCT ON ensures we return one ID per unique (sellable, retailer, sku) tuple
    let rows = sqlx::query(
        "WITH input AS (
            SELECT * FROM UNNEST($1::bigint[], $2::bigint[], $3::text[])
                AS t(sellable_id, retailer_id, sku)
        ),
        existing AS (
            SELECT i.*, o.id
            FROM input i
            LEFT JOIN offers o ON o.sellable_id = i.sellable_id
                AND o.retailer_id = i.retailer_id
                AND o.sku IS NOT DISTINCT FROM i.sku
        ),
        to_insert AS (
            SELECT sellable_id, retailer_id, sku
            FROM existing
            WHERE id IS NULL
        ),
        inserted AS (
            INSERT INTO offers (sellable_id, retailer_id, sku)
            SELECT sellable_id, retailer_id, sku FROM to_insert
            ON CONFLICT (sellable_id, retailer_id, sku) DO NOTHING
            RETURNING id, sellable_id, retailer_id, sku
        )
        SELECT COALESCE(e.id, ins.id) as id
        FROM existing e
        LEFT JOIN inserted ins ON ins.sellable_id = e.sellable_id
            AND ins.retailer_id = e.retailer_id
            AND ins.sku IS NOT DISTINCT FROM e.sku
        ORDER BY (
            SELECT idx FROM (
                SELECT ROW_NUMBER() OVER () as idx, *
                FROM input
            ) sub
            WHERE sub.sellable_id = e.sellable_id
                AND sub.retailer_id = e.retailer_id
                AND sub.sku IS NOT DISTINCT FROM e.sku
            LIMIT 1
        )"
    )
    .persistent(false)
    .bind(&sellable_ids)
    .bind(&retailer_ids)
    .bind(&skus)
    .fetch_all(&db.pool)
    .await?;

    let result: Vec<i64> = rows.iter().map(|r| r.get("id")).collect();
    Ok(result)
}

/// Batch create or retrieve multiple offer_jurisdictions.
/// Returns a Vec of offer_jurisdiction IDs in the same order as input.
///
/// This is optimized for the common pattern of creating many offer_jurisdictions at once.
/// Uses a single batch INSERT with UNNEST instead of N individual queries.
#[instrument(skip(db, offer_jurisdictions))]
pub async fn ensure_offer_jurisdictions_batch(
    db: &Db,
    offer_jurisdictions: &[(i64, i64, i64)], // (offer_id, jurisdiction_id, currency_id)
) -> Result<Vec<i64>> {
    if offer_jurisdictions.is_empty() {
        return Ok(Vec::new());
    }

    // For PHP compat mode, fall back to individual queries
    let compat = php_compat_schema(db).await.unwrap_or(false);
    if compat {
        let mut result = Vec::with_capacity(offer_jurisdictions.len());
        for (offer_id, jurisdiction_id, currency_id) in offer_jurisdictions {
            let id = ensure_offer_jurisdiction(db, *offer_id, *jurisdiction_id, *currency_id).await?;
            result.push(id);
        }
        return Ok(result);
    }

    // Prepare arrays for batch processing
    let offer_ids: Vec<i64> = offer_jurisdictions.iter().map(|(o, _, _)| *o).collect();
    let jurisdiction_ids: Vec<i64> = offer_jurisdictions.iter().map(|(_, j, _)| *j).collect();
    let currency_ids: Vec<i64> = offer_jurisdictions.iter().map(|(_, _, c)| *c).collect();

    // Batch INSERT using UNNEST to expand arrays
    // Similar pattern to ensure_offers_batch but for offer_jurisdictions table
    let rows = sqlx::query(
        "WITH input AS (
            SELECT * FROM UNNEST($1::bigint[], $2::bigint[], $3::bigint[])
                AS t(offer_id, jurisdiction_id, currency_id)
        ),
        existing AS (
            SELECT i.*, oj.id
            FROM input i
            LEFT JOIN offer_jurisdictions oj
                ON oj.offer_id = i.offer_id
                AND oj.jurisdiction_id = i.jurisdiction_id
        ),
        to_insert AS (
            SELECT offer_id, jurisdiction_id, currency_id
            FROM existing
            WHERE id IS NULL
        ),
        inserted AS (
            INSERT INTO offer_jurisdictions (offer_id, jurisdiction_id, currency_id)
            SELECT offer_id, jurisdiction_id, currency_id FROM to_insert
            ON CONFLICT (offer_id, jurisdiction_id) DO UPDATE
                SET currency_id = EXCLUDED.currency_id
            RETURNING id, offer_id, jurisdiction_id
        )
        SELECT COALESCE(e.id, ins.id) as id
        FROM existing e
        LEFT JOIN inserted ins
            ON ins.offer_id = e.offer_id
            AND ins.jurisdiction_id = e.jurisdiction_id
        ORDER BY (
            SELECT idx FROM (
                SELECT ROW_NUMBER() OVER () as idx, *
                FROM input
            ) sub
            WHERE sub.offer_id = e.offer_id
                AND sub.jurisdiction_id = e.jurisdiction_id
            LIMIT 1
        )"
    )
    .persistent(false)
    .bind(&offer_ids)
    .bind(&jurisdiction_ids)
    .bind(&currency_ids)
    .fetch_all(&db.pool)
    .await?;

    let result: Vec<i64> = rows.iter().map(|r| r.get("id")).collect();
    Ok(result)
}
```

**Usage Example**:

```rust
// Before (N+1 pattern):
let mut offer_ids = Vec::new();
for (sellable_id, retailer_id, sku) in &offers_to_create {
    let offer_id = ensure_offer(db, *sellable_id, *retailer_id, *sku).await?;
    offer_ids.push(offer_id);
}

// After (batched):
let offers_data: Vec<(i64, i64, Option<&str>)> = offers_to_create
    .iter()
    .map(|(s, r, sku)| (*s, *r, sku.as_deref()))
    .collect();
let offer_ids = ensure_offers_batch(db, &offers_data).await?;
```

**Key Changes**:
- New helper functions for batch offer/jurisdiction creation
- Uses CTEs (Common Table Expressions) with UNNEST for efficient batch processing
- Maintains order of results matching input order
- Falls back to individual queries for PHP compat mode
- Idempotent operations with proper conflict handling

## 4. Additional Optimization: merge_video_game_metadata Batch Version

The `merge_video_game_metadata` function (line 4662) already operates on a single video game, but if callers need to update multiple games, here's a batch version:

```rust
/// Batch version of merge_video_game_metadata for updating multiple games at once.
///
/// Takes a slice of (video_game_id, metadata_patch) tuples and updates all in one query.
/// This is useful when ingesting multiple games and needing to update their metadata.
#[instrument(skip(db, updates))]
pub async fn merge_video_game_metadata_batch(
    db: &Db,
    updates: &[(i64, Value)], // (video_game_id, metadata_patch)
) -> Result<()> {
    if updates.is_empty() {
        return Ok(());
    }

    let cols = video_games_content_columns(db).await?;
    if !cols.has_metadata {
        return Ok(());
    }

    // Prepare arrays for batch update
    let game_ids: Vec<i64> = updates.iter().map(|(id, _)| *id).collect();
    let metadata_patches: Vec<Value> = updates.iter().map(|(_, patch)| patch.clone()).collect();

    // Batch UPDATE using UNNEST to match IDs with their patches
    // The FROM clause creates a temporary table from the arrays
    let sql = if cols.metadata_is_jsonb {
        "UPDATE video_games AS vg
         SET metadata = (COALESCE(vg.metadata, '{}'::jsonb) || t.patch::jsonb)
         FROM UNNEST($1::bigint[], $2::jsonb[]) AS t(id, patch)
         WHERE vg.id = t.id"
    } else {
        "UPDATE video_games AS vg
         SET metadata = ((COALESCE(vg.metadata::jsonb, '{}'::jsonb) || t.patch::jsonb)::json)
         FROM UNNEST($1::bigint[], $2::jsonb[]) AS t(id, patch)
         WHERE vg.id = t.id"
    };

    best_effort_execute(
        db,
        sqlx::query(sql)
            .persistent(false)
            .bind(&game_ids)
            .bind(&metadata_patches),
        "metadata_merge_batch",
    )
    .await;

    Ok(())
}
```

## Testing Recommendations

1. **Unit Tests**: Add tests comparing behavior of old vs new implementations
2. **Integration Tests**: Verify idempotency by running batch operations multiple times
3. **Performance Tests**: Measure query reduction and timing improvements
4. **Edge Cases**: Test with empty arrays, duplicate entries, NULL values

Example test structure:

```rust
#[cfg(test)]
mod batch_optimization_tests {
    use super::*;

    #[tokio::test]
    async fn test_ensure_provider_media_links_batch_equivalence() {
        // Test that batch version produces same results as loop version
    }

    #[tokio::test]
    async fn test_ensure_provider_media_links_idempotency() {
        // Test that running batch insert twice is safe
    }

    #[tokio::test]
    async fn test_batch_offers_maintains_order() {
        // Verify output order matches input order
    }
}
```

## Migration Strategy

1. **Phase 1**: Add optimized functions alongside existing ones (with `_v2` suffix)
2. **Phase 2**: Update callers incrementally to use new versions
3. **Phase 3**: Run both versions in parallel and compare results
4. **Phase 4**: Replace old implementations once validated
5. **Phase 5**: Remove `_v2` suffix and old code

## Performance Expectations

Based on typical ingestion workloads:

- **ensure_provider_media_links_with_meta**:
  - Before: 20-50 URLs → 20-50 database round-trips
  - After: 20-50 URLs → 1 database round-trip
  - Expected improvement: **10-30x faster** for media ingestion

- **update_video_game_global_rating_if_null**:
  - Before: 3 UPDATE queries per game
  - After: 1 UPDATE query per game
  - Expected improvement: **3x fewer queries**

- **Batch offers/jurisdictions**:
  - Before: N offers → N SELECTs + M INSERTs
  - After: N offers → 1 batched query
  - Expected improvement: **5-20x faster** for bulk price ingestion

Total estimated improvement for full ingestion pipeline: **30-50% reduction in database time**.
