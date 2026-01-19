# Ingestion Analysis Findings

## Summary
Analysis of the gc binary and database schema alignment reveals two critical issues:
1. **`ensure_video_game()` missing column population** - Only 3 of 19 columns are filled during insert
2. **GiantBomb image URL variants not stored** - Only stores ONE URL instead of all three available resolutions

---

## Issue 1: video_games Missing Column Population

### Current State
**Function:** `src/database_ops/ingest_providers.rs:2372` - `ensure_video_game()`

**Currently Inserted:**
- `title_id` ✓
- `platform_id` ✓
- `edition` ✓ (optional)

**Missing Columns:**
- `created_at` ❌ (has DEFAULT now() in schema, but should be explicit)
- `updated_at` ❌ (has DEFAULT now() in schema, but should be explicit)
- `slug` ❌ (nullable, should derive from title if available)
- `release_date` ❌ (nullable, should populate from provider data)
- `metadata` ❌ (nullable, should store provider metadata)
- `display_title` ❌ (nullable, should populate from title)
- `developer` ❌ (nullable, should populate from provider data)
- `region_codes` ❌ (nullable, should populate from provider data)
- `genres` ❌ (nullable, should populate from provider data)
- `synopsis` ❌ (nullable, should populate from provider data)
- `rating` ❌ (has DEFAULT 0)
- `popularity_score` ❌ (has DEFAULT 0)
- `average_rating` (not in all schemas, check dynamic)
- `rating_count` (not in all schemas, check dynamic)
- `rating_updated_at` (not in all schemas, check dynamic)
- `sellable_id` (not in all schemas, check dynamic)

### Reference Implementation
**Function:** `src/database_ops/ingest_providers.rs:4005` - `ensure_video_game_for_product()`

This function shows the CORRECT pattern:
1. Dynamic column detection via `video_games_laravel_columns()` (line 4041)
2. Builds column list dynamically (lines 4085-4095)
3. Adds created_at/updated_at when columns exist
4. Uses prepared statement with VALUES placeholders

```rust
if cols.has_created_at {
    columns.push("created_at");
    values.push("now()".to_string());
}
if cols.has_updated_at {
    columns.push("updated_at");
    values.push("now()".to_string());
}
```

### Schema References

**Unified Schema** (`migrations/0001_full_consolidated_schema.sql:200-256`):
```sql
CREATE TABLE IF NOT EXISTS video_games (
  id bigserial PRIMARY KEY,
  title_id bigint NOT NULL,
  platform_id bigint NOT NULL REFERENCES platforms(id),
  edition text,
  sellable_id bigint,
  slug citext UNIQUE,
  release_date date,
  metadata jsonb,
  average_rating real,
  rating_count bigint,
  rating_updated_at timestamptz,
  genres text[],
  display_title text,
  developer text,
  region_codes text[],
  popularity_score numeric NOT NULL DEFAULT 0,
  rating numeric NOT NULL DEFAULT 0,
  synopsis text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
```

**Laravel Schema** (`~/Documents/game-compare/storage/logs/postgres-schema.sql`):
- Different columns: product_id, title, genre, normalized_title, external_ids, external_links, platform_codes, title_keywords, payload_hash, last_synced_at
- Common columns: release_date, developer, metadata, created_at, updated_at, slug, region_codes, genres

---

## Issue 2: GiantBomb Image URL Variants Not Stored

### Current State
**Files:**
- `src/database_ops/giantbomb/ingest.rs:271-296` (gb_game_from_api)
- `src/database_ops/giantbomb/ingest.rs:606-631` (ensure_detail)

**GiantBomb Data Structure** (`src/database_ops/giantbomb/collector.rs:320-322`):
```rust
struct GbImage {
    original_url: Option<String>,  // Highest quality
    super_url: Option<String>,     // Medium quality
    small_url: Option<String>,     // Low quality/thumbnail
}
```

**Current Behavior:**
```rust
// Fallback chain - selects ONLY ONE URL
if let Some(url) = img
    .get("original_url")
    .and_then(|u| u.as_str())
    .or_else(|| img.get("super_url").and_then(|u| u.as_str()))
    .or_else(|| img.get("small_url").and_then(|u| u.as_str()))
{
    // Only stores the single selected URL
    upsert_game_media(db, vg_id, "giant_bomb", url, "screenshot", url,
        serde_json::json!({"size":"orig|super|small"})).await;
}
```

**Problem:**
1. Only stores ONE URL (best available) instead of all three
2. Provider data contains useless `{"size":"orig|super|small"}` string instead of actual URLs
3. Frontend (Laravel) expects all three variants for responsive images

### Database Schema Support

**Table:** `game_media` (`migrations/0472_game_media.sql` + `migrations/0492_game_media_url_columns.sql`)

**Columns:**
- `url` text NOT NULL (primary/required URL)
- `original_url` text (highest quality - maps to GiantBomb's original_url)
- `thumbnail_url` text (smallest - maps to GiantBomb's small_url)
- `stream_url` text (for videos)
- `poster_url` text (video preview frames)

**Note:** GiantBomb's `super_url` (medium quality) doesn't have a direct column mapping. Could use:
- Store as `original_url` if original not available
- Store in `provider_data` JSON as `{"super_url": "..."}`

### Batch Function Already Supports This

**Function:** `src/database_ops/ingest_providers.rs:1943` - `upsert_game_media_batch()`

Lines 2008-2050 show the batch function ALREADY extracts URL variants from provider_data:
```rust
let (original_url, thumbnail_url, stream_url, poster_url) = {
    if let Some(obj) = pdata.as_object() {
        let orig = grab(obj, &["original_url", "url", "high_url", "hd_url", "background"]);
        let thumb = grab(obj, &["thumbnail_url", "thumb", "thumbnail", "small_url", "image_thumb"]);
        let stream = grab(obj, &["stream_url", "video_url", "m3u8", "playable_url"]);
        let poster = grab(obj, &["poster_url", "preview_url", "image"]);
        (orig, thumb, stream, poster)
    } else {
        (Some(*url), None, None, None)
    }
};
```

**Issue:** GiantBomb uses the single `upsert_game_media()` function which doesn't extract variants.

### Laravel Frontend Expectations

**File:** `~/Documents/game-compare/app/Models/Product.php:142-153`

Shows image priority order and confirms need for multiple sizes:
- cover/box_art (priority 400)
- background/keyart (priority 350)
- artwork (priority 300)
- screenshot (priority 200)
- icon/thumb (priority 150)

**File:** `~/Documents/game-compare/app/Services/Catalogue/PriceCrossReferencer.php`

Confirms Laravel expects: `image_super_url`, `image_original_url`, `image_small_url` fields
Fallback chain: super_url → original_url → small_url

---

## Recommended Fixes

### Fix 1: Update ensure_video_game()

**Location:** `src/database_ops/ingest_providers.rs:2372`

**Approach:**
1. Add dynamic column detection (similar to lines 4041-4056)
2. Accept additional optional parameters: `slug`, `release_date`, `metadata`, etc.
3. Build dynamic INSERT statement with detected columns
4. Always include created_at/updated_at when available

**Signature Change:**
```rust
pub async fn ensure_video_game(
    db: &Db,
    title_id: i64,
    platform_id: i64,
    edition: Option<&str>,
    // New optional parameters from provider data:
    slug: Option<&str>,
    release_date: Option<&str>,
    display_title: Option<&str>,
    developer: Option<&str>,
    synopsis: Option<&str>,
    metadata: Option<&Value>,
    region_codes: Option<&[String]>,
    genres: Option<&[String]>,
) -> Result<i64>
```

### Fix 2: Update GiantBomb Ingestion

**Location:**
- `src/database_ops/giantbomb/ingest.rs:271-296`
- `src/database_ops/giantbomb/ingest.rs:606-631`

**Option A: Use Batch Function**
Switch from `upsert_game_media()` to `upsert_game_media_batch()` which already extracts variants

**Option B: Fix provider_data JSON**
Pass all three URLs in structured JSON:
```rust
let provider_data = serde_json::json!({
    "original_url": img.get("original_url").and_then(|u| u.as_str()),
    "super_url": img.get("super_url").and_then(|u| u.as_str()),
    "small_url": img.get("small_url").and_then(|u| u.as_str()),
    "source": "giant_bomb",
    "kind": "screenshot"
});
```

**Option C: Enhance Single Function**
Modify `upsert_game_media()` to accept optional URL parameters:
```rust
pub async fn upsert_game_media(
    db: &Db,
    video_game_id: i64,
    source: &str,
    external_id: &str,
    media_type: &str,
    url: &str,
    provider_data: Value,
    // New optional parameters:
    original_url: Option<&str>,
    thumbnail_url: Option<&str>,
) -> Result<()>
```

**Recommendation:** Option B (fix provider_data) is least invasive and uses existing infrastructure.

---

## Impact Assessment

### video_games Missing Columns
**Severity:** High
- Timestamps missing breaks Laravel's expectation (created_at/updated_at standard in Eloquent)
- Missing metadata prevents frontend from displaying rich information
- Missing slug prevents SEO-friendly URLs
- Missing release_date affects sorting and filtering

### GiantBomb Image URLs
**Severity:** Medium
- Frontend can't serve responsive images (no small/medium/large variants)
- Bandwidth waste (serving full resolution to mobile users)
- User experience degradation (slower page loads)
- Provider data (`{"size":"orig|super|small"}`) is useless placeholder

---

## Testing Plan

### After Fix 1 (ensure_video_game):
1. Run gc binary with GiantBomb ingestion
2. Check `SELECT * FROM video_games ORDER BY id DESC LIMIT 5`
3. Verify all columns populated (or NULL where appropriate)
4. Confirm created_at/updated_at are set

### After Fix 2 (GiantBomb URLs):
1. Ingest a game with multiple screenshot sizes
2. Check `SELECT url, original_url, thumbnail_url, provider_data FROM game_media WHERE source = 'giant_bomb' LIMIT 5`
3. Verify all three URL columns populated when available
4. Verify provider_data contains structured JSON (not placeholder string)

---

## Files Requiring Changes

1. **src/database_ops/ingest_providers.rs**
   - Line 2372: `ensure_video_game()` - add column detection and optional parameters
   - Line 4702: `upsert_game_media()` - optionally enhance to accept URL parameters

2. **src/database_ops/giantbomb/ingest.rs**
   - Line 271-296: `gb_game_from_api()` - extract all three URL variants
   - Line 606-631: `ensure_detail()` - extract all three URL variants

3. **Callers of ensure_video_game()** (find with grep):
   - Update call sites to pass additional optional data when available
   - PlayStation, Xbox, Steam, IGDB ingestion may also need updates
