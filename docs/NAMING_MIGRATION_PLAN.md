# Provider → video_game_source Naming Migration Plan

## Problem Statement

The codebase currently uses inconsistent terminology mixing "provider" and "video_game_source".
This causes confusion and breaks Laravel compatibility.

## Required Changes

### Terminology Standard
- ❌ **OLD**: "provider", "provider_item", "provider_media_links"
- ✅ **NEW**: "video_game_source" (or "vg_source"), "source_item", "vg_source_media_links"

---

## Phase 1: Database Schema Migration

### 1.1 Rename Table: `provider_media_links` → `vg_source_media_links`

**Migration File**: `migrations/XXXX_rename_provider_media_links_to_vg_source.sql`

```sql
-- Rename table
ALTER TABLE IF EXISTS provider_media_links
    RENAME TO vg_source_media_links;

-- Rename indexes
ALTER INDEX IF EXISTS provider_media_links_item_idx
    RENAME TO vg_source_media_links_item_idx;

ALTER INDEX IF EXISTS uq_provider_media_links_item_url
    RENAME TO uq_vg_source_media_links_item_url;

ALTER INDEX IF EXISTS provider_media_links_video_game_idx
    RENAME TO vg_source_media_links_video_game_idx;

ALTER INDEX IF EXISTS provider_media_links_type_role_idx
    RENAME TO vg_source_media_links_type_role_idx;

ALTER INDEX IF EXISTS provider_media_links_source_idx
    RENAME TO vg_source_media_links_source_idx;

ALTER INDEX IF EXISTS idx_provider_media_links_vg_type
    RENAME TO idx_vg_source_media_links_vg_type;

-- Update comments
COMMENT ON TABLE vg_source_media_links IS
    'Links between video_game_sources and media (images/videos). Replaces provider_media_links.';
```

### 1.2 Consider Consolidation

**Current State**: TWO overlapping systems
1. `providers` + `provider_items` + `provider_media_links` (old unified)
2. `video_game_sources` (new)

**Decision Needed**:
- Keep both for backward compatibility?
- OR migrate all to `video_game_sources`?

**Recommendation**: Deprecate old `providers` tables in favor of `video_game_sources`

---

## Phase 2: Function Renaming

### File: `src/database_ops/ingest_providers.rs`

#### 2.1 Primary Source Functions

```rust
// RENAME: Line 663
// FROM:
pub async fn ensure_provider(db: &Db, name: &str, kind: &str, slug: Option<&str>) -> Result<i64>

// TO:
pub async fn ensure_video_game_source_legacy(
    db: &Db,
    name: &str,
    kind: &str,
    slug: Option<&str>
) -> Result<i64>
// Mark as deprecated, keep for backward compat
```

```rust
// RENAME: Line 745
// FROM:
pub async fn ensure_game_provider(db: &Db, name: &str, _kind: &str, slug: Option<&str>) -> Result<i64>

// TO:
// DELETE - duplicate of above
```

```rust
// KEEP: Line 1684 (already correct)
pub async fn ensure_video_game_source(db: &Db, provider_key: &str, display_name: &str) -> Result<i64>
// This is the PRIMARY function to use
```

#### 2.2 Media Link Functions

```rust
// RENAME: Line 2453
// FROM:
pub async fn ensure_provider_media_link(db: &Db, provider_item_id: i64, url: &str) -> Result<i64>

// TO:
pub async fn ensure_vg_source_media_link(db: &Db, source_item_id: i64, url: &str) -> Result<i64> {
    // ... update table name in query ...
}
```

```rust
// RENAME: Line 5758
// FROM:
pub async fn ensure_provider_media_links_with_meta(
    db: &Db,
    provider_item_id: i64,
    media_tuples: &[(String, Option<String>, Option<String>, Option<String>)],
    vg_id: Option<i64>,
) -> Result<()>

// TO:
pub async fn ensure_vg_source_media_links_with_meta(
    db: &Db,
    source_item_id: i64,
    media_tuples: &[(String, Option<String>, Option<String>, Option<String>)],
    vg_id: Option<i64>,
) -> Result<()> {
    // Update table name from provider_media_links to vg_source_media_links
}
```

```rust
// RENAME: Line 6006
// FROM:
pub async fn ensure_provider_media_links(
    db: &Db,
    provider_item_id: i64,
    urls: &[String],
) -> Result<()>

// TO:
pub async fn ensure_vg_source_media_links(
    db: &Db,
    source_item_id: i64,
    urls: &[String],
) -> Result<()> {
    // Update table name
}
```

---

## Phase 3: Update All Provider Implementations

### Files to Update (14 files):

1. `src/database_ops/playstation/prices.rs`
2. `src/database_ops/playstation/ingest_demo.rs`
3. `src/database_ops/steam/provider.rs`
4. `src/database_ops/xbox/provider.rs`
5. `src/database_ops/nexarda/provider.rs`
6. `src/database_ops/igdb/ingest.rs`
7. `src/database_ops/giantbomb/ingest.rs`
8. `src/database_ops/rawg/mod.rs`
9. `src/database_ops/tgdb/mod.rs`
10. `src/database_ops/itad/mod.rs`
11. `src/bin/import_sqlite.rs`
12. `src/bin/ingest_itad.rs`
13. `src/bin/gc.rs`

### Example Changes:

```rust
// BEFORE:
use crate::database_ops::ingest_providers::{
    ensure_provider,
    ensure_provider_media_links_with_meta,
};

let provider_id = ensure_provider(&db, "PlayStation Store", "store", Some("ps-store")).await?;
ensure_provider_media_links_with_meta(&db, provider_item_id, &media_tuples, Some(vg_id)).await?;

// AFTER:
use crate::database_ops::ingest_providers::{
    ensure_video_game_source,
    ensure_vg_source_media_links_with_meta,
};

let source_id = ensure_video_game_source(&db, "ps-store", "PlayStation Store").await?;
ensure_vg_source_media_links_with_meta(&db, source_item_id, &media_tuples, Some(vg_id)).await?;
```

---

## Phase 4: Update ProviderEntityCache

### File: `src/database_ops/ingest_providers.rs`

```rust
// Current struct (around line 1165):
pub struct ProviderEntityCache {
    pub db: Db,
    // ... fields ...
}

impl ProviderEntityCache {
    // RENAME all methods:
    // ensure_provider_item() → ensure_source_item()
    // Variables: provider_id → source_id
}
```

---

## Phase 5: Ensure Uniqueness Constraints

### Check Existing Constraints

```sql
-- Check video_game_sources uniqueness
SELECT
    conname,
    pg_get_constraintdef(c.oid)
FROM pg_constraint c
JOIN pg_class t ON c.conrelid = t.oid
WHERE t.relname = 'video_game_sources'
  AND c.contype IN ('u', 'p');
```

### Add Missing Constraints if Needed

```sql
-- Ensure unique video_game_sources
ALTER TABLE video_game_sources
    ADD CONSTRAINT uq_video_game_sources_key
    UNIQUE (provider_key);

-- Ensure unique source items per source
-- (if provider_items table used)
ALTER TABLE provider_items
    ADD CONSTRAINT uq_provider_items_provider_external
    UNIQUE (provider_id, external_id);
```

---

## Phase 6: Update Laravel Compatibility

### File: `~/Documents/game-compare/app/Services/Media/Providers/*`

Ensure Laravel code uses consistent naming:
- Model: `VideoGameSource` (not `Provider`)
- Table: `video_game_sources`
- Relations: `vg_source_media_links`

---

## Implementation Order

### Week 1: Preparation
1. Create database migration for table rename
2. Run migration on dev database
3. Test that Laravel can still read data

### Week 2: Function Renaming
4. Rename functions in `ingest_providers.rs`
5. Add deprecation warnings to old functions
6. Keep old functions as aliases (temporary)

### Week 3: Provider Updates
7. Update all 14 provider files to use new functions
8. Test each provider individually
9. Verify data writes to correct tables

### Week 4: Cleanup
10. Remove deprecated function aliases
11. Update all documentation
12. Final integration test

---

## Testing Checklist

- [ ] Database migration runs successfully
- [ ] All indexes renamed correctly
- [ ] Laravel can read from `vg_source_media_links`
- [ ] Rust providers can write to `vg_source_media_links`
- [ ] `ensure_video_game_source()` creates unique sources
- [ ] No duplicate source entries created
- [ ] Media links properly reference video_game_id
- [ ] All 14 provider files compile
- [ ] Integration test: full ingestion run

---

## Rollback Plan

If issues arise:

```sql
-- Rollback table rename
ALTER TABLE vg_source_media_links
    RENAME TO provider_media_links;

-- Rollback indexes (repeat for all)
ALTER INDEX vg_source_media_links_item_idx
    RENAME TO provider_media_links_item_idx;
```

---

## Success Criteria

1. ✅ No references to "provider_media_links" in Rust code
2. ✅ All functions use "video_game_source" or "vg_source" terminology
3. ✅ Database tables use "vg_source_*" naming
4. ✅ Laravel and Rust both use same table names
5. ✅ Unique constraints prevent duplicate sources
6. ✅ All providers successfully ingest data
7. ✅ No breaking changes to existing data

---

**Status**: Ready for implementation
**Estimated Effort**: 4 weeks
**Risk Level**: Medium (requires coordination between Rust and Laravel)
