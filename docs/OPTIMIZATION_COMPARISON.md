# Side-by-Side Comparison: Before vs After Optimization

This document shows the actual code changes for the most impactful optimization.

## ensure_provider_media_links_with_meta - The Main Bottleneck

### BEFORE (Lines 5321-5443 - The N+1 Loop)

```rust
let mut count = 0usize;
let mut seen: HashSet<String> = HashSet::new();

// ❌ BAD: This loop executes a database INSERT for EACH URL
for (idx, (url, media_type_raw, role_raw, title_raw)) in urls.iter().enumerate() {
    let trimmed = url.trim();
    if trimmed.is_empty() {
        continue;
    }
    let normalized = trimmed.to_string();
    if !seen.insert(normalized.clone()) {
        continue;
    }

    // ... data preparation (same in both versions) ...
    let media_type_candidate = media_type_raw.as_ref().map(|s| s.trim()).filter(|s| !s.is_empty());
    let role_candidate = role_raw.as_ref().map(|s| s.trim()).filter(|s| !s.is_empty());
    let (prio, group) = media_group_priority(media_type_candidate, role_candidate, &normalized);
    let sort_order: i32 = prio.saturating_mul(1000) + (idx as i32);
    let media_type = media_type_candidate
        .map(|s| normalize_media_type_for_db(s, supports_background).to_ascii_lowercase());
    let role = normalize_provider_media_role(group, role_candidate);
    let row_meta_opt = if !has_sort_order && !has_position {
        let mut row_meta: Value = metadata_base.clone().unwrap_or_else(|| json!({}));
        if let Value::Object(map) = &mut row_meta {
            map.insert("gc_sort_order".to_string(), json!(sort_order));
            map.insert("gc_media_group".to_string(), json!(group));
        }
        Some(row_meta)
    } else {
        metadata_base.clone()
    };
    let title = title_raw.as_ref().map(|s| s.trim()).filter(|s| !s.is_empty()).map(|s| s.to_string());

    // ❌ PROBLEM: Individual INSERT executed N times (one per URL)
    // If we have 50 URLs, this means 50 separate database round-trips!
    if has_sort_order {
        let _ = sqlx::query(
            "INSERT INTO provider_media_links (provider_item_id, video_game_id, url, source, media_type, role, title, sort_order, metadata) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, COALESCE($9, '{}'::jsonb)) \
             ON CONFLICT (provider_item_id, url) DO UPDATE ..."
        )
        .bind(provider_item_id)
        .bind(video_game_id)
        .bind(&normalized)
        .bind(source_norm.as_ref())
        .bind(&media_type)
        .bind(&role)
        .bind(&title)
        .bind(sort_order)
        .bind(&row_meta_opt)
        .execute(&db.pool)
        .await?;
    } else if has_position {
        // ❌ Another individual INSERT variant
        let _ = sqlx::query("INSERT INTO provider_media_links ... VALUES ...")
            .bind(...)
            .execute(&db.pool)
            .await?;
    } else {
        // ❌ Yet another individual INSERT variant
        let _ = sqlx::query("INSERT INTO provider_media_links ... VALUES ...")
            .bind(...)
            .execute(&db.pool)
            .await?;
    }

    count += 1;
}

Ok(count)
```

**Problem**: For 50 URLs, this makes **50 database round-trips** (plus latency for each).

---

### AFTER (Optimized with UNNEST)

```rust
// ✅ GOOD: Separate data preparation from database operations
let mut seen: HashSet<String> = HashSet::new();

// Step 1: Prepare ALL data in memory (no database calls yet)
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

    // ... same data preparation as before ...
    let media_type_candidate = media_type_raw.as_ref().map(|s| s.trim()).filter(|s| !s.is_empty());
    let role_candidate = role_raw.as_ref().map(|s| s.trim()).filter(|s| !s.is_empty());
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
    let title = title_raw.as_ref().map(|s| s.trim()).filter(|s| !s.is_empty()).map(|s| s.to_string());

    // ✅ GOOD: Just accumulate data, no database calls
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

// Step 2: Execute ONE batch INSERT for ALL URLs
// ✅ EXCELLENT: All 50 URLs inserted in a single database round-trip!
if has_sort_order {
    sqlx::query(
        // UNNEST expands arrays into rows server-side:
        // UNNEST([1,2,3], ['a','b','c']) becomes:
        //   Row 1: (1, 'a')
        //   Row 2: (2, 'b')
        //   Row 3: (3, 'c')
        "INSERT INTO provider_media_links
            (provider_item_id, video_game_id, url, source, media_type, role, title, sort_order, metadata)
         SELECT
            $1::bigint,              -- same provider_item_id for all rows
            video_game_id,           -- from UNNEST array
            url,                     -- from UNNEST array
            source,                  -- from UNNEST array
            media_type,              -- from UNNEST array
            role,                    -- from UNNEST array
            title,                   -- from UNNEST array
            sort_order,              -- from UNNEST array
            COALESCE(metadata, '{}'::jsonb)  -- from UNNEST array
         FROM UNNEST(
            $2::bigint[],   -- batch_video_game_ids array
            $3::text[],     -- batch_urls array
            $4::text[],     -- batch_sources array
            $5::text[],     -- batch_media_types array
            $6::text[],     -- batch_roles array
            $7::text[],     -- batch_titles array
            $8::int[],      -- batch_sort_orders array
            $9::jsonb[]     -- batch_metadata array
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
    .bind(&batch_video_game_ids)   // ✅ Bind entire array at once
    .bind(&batch_urls)              // ✅ Bind entire array at once
    .bind(&batch_sources)           // ✅ Bind entire array at once
    .bind(&batch_media_types)       // ✅ Bind entire array at once
    .bind(&batch_roles)             // ✅ Bind entire array at once
    .bind(&batch_titles)            // ✅ Bind entire array at once
    .bind(&batch_sort_orders)       // ✅ Bind entire array at once
    .bind(&batch_metadata)          // ✅ Bind entire array at once
    .execute(&db.pool)
    .await?;
}
// Similar batched inserts for has_position and default cases...

Ok(count)
```

**Solution**: For 50 URLs, this makes **1 database round-trip** total!

---

## Visual Performance Comparison

### Before (N+1 Pattern)
```
Application                  Database
    |                            |
    |--INSERT URL #1----------->|
    |<-OK---------------------|
    |                            |
    |--INSERT URL #2----------->|
    |<-OK---------------------|
    |                            |
    |--INSERT URL #3----------->|
    |<-OK---------------------|
    |                            |
    ... (47 more round-trips) ...
    |                            |
    |--INSERT URL #50----------->|
    |<-OK---------------------|

Total: 50 round-trips
Each round-trip: ~1-5ms network latency + query time
Total time: 50-250ms + query processing
```

### After (Batched with UNNEST)
```
Application                  Database
    |                            |
    |                            |
    |--BATCH INSERT 50 URLs----->|
    |                            |
    |   (PostgreSQL unpacks      |
    |    arrays with UNNEST)     |
    |                            |
    |<-OK---------------------|

Total: 1 round-trip
Total time: 1-5ms + batch query processing
```

---

## Key Insights

### Why UNNEST?
- **PostgreSQL native**: Built-in function, highly optimized
- **Type safe**: Arrays are strongly typed (`bigint[]`, `text[]`, `jsonb[]`)
- **SQLx support**: Full support for binding Rust `Vec<T>` as PostgreSQL arrays
- **Transaction atomic**: All rows inserted/updated in single transaction

### Performance Math
For typical media ingestion (50 URLs per game):
- **Before**: 50 queries × 2ms latency = 100ms + processing
- **After**: 1 query × 2ms latency = 2ms + processing
- **Improvement**: **50x reduction in network overhead**

For a full ingestion run (1000 games):
- **Before**: 50,000 media insert queries
- **After**: 1,000 batched insert queries
- **Time saved**: Hours → Minutes for large datasets

### Idempotency Maintained
The `ON CONFLICT` clause ensures running this multiple times is safe:
```sql
ON CONFLICT (provider_item_id, url) DO UPDATE
SET video_game_id = COALESCE(EXCLUDED.video_game_id, provider_media_links.video_game_id),
    ...
```
- First run: Inserts new rows
- Second run: Updates existing rows with COALESCE (keeps non-NULL values)
- Result: Always the same final state

### Memory Trade-off
- **Before**: Constant memory (one row at a time)
- **After**: O(N) memory (all rows in arrays)
- **Impact**: Negligible for typical N (20-100 URLs), significant performance gain

---

## What About Other Functions?

### update_video_game_global_rating_if_null

**Before**: 3 separate UPDATE queries
```rust
// Query 1
sqlx::query("UPDATE video_games SET average_rating = COALESCE(average_rating, $1) WHERE id=$2")
    .bind(average_rating).bind(video_game_id).execute(&db.pool).await?;

// Query 2
sqlx::query("UPDATE video_games SET rating_count = COALESCE(rating_count, $1) WHERE id=$2")
    .bind(rating_count).bind(video_game_id).execute(&db.pool).await?;

// Query 3
sqlx::query("UPDATE video_games SET rating_updated_at = COALESCE(rating_updated_at, now()) WHERE id=$1")
    .bind(video_game_id).execute(&db.pool).await?;
```

**After**: 1 combined UPDATE query
```rust
let sql = "UPDATE video_games SET
    average_rating = COALESCE(average_rating, $2),
    rating_count = COALESCE(rating_count, $3),
    rating_updated_at = COALESCE(rating_updated_at, now())
WHERE id = $1";

sqlx::query(sql)
    .bind(video_game_id)
    .bind(average_rating)
    .bind(rating_count)
    .execute(&db.pool)
    .await?;
```

**Improvement**: 3x fewer queries, simpler code

---

## Bottom Line

These optimizations eliminate the most common performance anti-pattern (N+1 queries) by:
1. **Separating concerns**: Data preparation in memory vs database operations
2. **Batching operations**: Use PostgreSQL's native array/UNNEST features
3. **Preserving semantics**: Same behavior, same idempotency, just faster

For a typical ingestion run ingesting 1000 games with 50 media URLs each:
- **Database queries reduced**: 50,000+ → ~1,000 (50x improvement)
- **Estimated time saved**: 30-50% of total ingestion time
- **Code maintainability**: Improved (clearer separation of data prep vs I/O)
