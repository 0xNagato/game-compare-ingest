# Naming Standardization: provider → video_game_source

**Status**: Database migrations complete, code changes pending
**Date**: 2025-12-27

## Executive Summary

All references to "provider" terminology must be changed to "video_game_source" (or "vg_source") throughout the codebase to maintain consistency with the database schema.

---

## Database Schema (COMPLETED ✅)

### Tables Created

1. **`video_game_sources`** - Primary source metadata table
   - Primary key: `id` (bigserial)
   - Foreign key: `video_game_id` → `video_games(id)` ON DELETE SET NULL
   - Unique constraint: `provider_key` (NOT NULL, unique index)
   - Columns: `provider_key`, `display_name`, `provider_game_id`, `provider_slug`, `payload`, `links`, `media`, `metadata`, `synced_at`, `created_at`, `updated_at`
   - **Relationship**: Many sources can point to ONE canonical video_game (many-to-one)

2. **`vg_source_media_links`** - Media URLs linked to sources
   - Primary key: `id` (bigserial)
   - Foreign key: `video_game_source_id` → `video_game_sources(id)` ON DELETE CASCADE
   - Foreign key: `video_game_id` → `video_games(id)` ON DELETE SET NULL
   - Unique index: `(video_game_source_id, url)`
   - Columns: `media_id`, `url`, `media_type`, `title`, `role`, `source`, `metadata`
   - CHECK: `media_id IS NOT NULL OR url IS NOT NULL`

### Tables REMOVED (Deprecated)

- ❌ `providers` - Removed (violates naming standard)
- ❌ `provider_items` - Removed (unnecessary intermediary)

### Migration Files

- **0529**: Creates `video_game_sources` and `vg_source_media_links` tables
- **0530**: Adds NOT NULL constraint to `provider_key`

---

## Design Decision: Game is Parent ✅

**Confirmed with user**: Keep current design where `video_games` is the parent.

```
video_games (id=1, title="Elden Ring")
  ↑
  ├── video_game_sources (id=1, provider_key="steam", video_game_id=1)
  ├── video_game_sources (id=2, provider_key="ps-store", video_game_id=1)
  ├── video_game_sources (id=3, provider_key="igdb", video_game_id=1)
  └── video_game_sources (id=4, provider_key="xbox", video_game_id=1)
```

**Why this design?**
- One canonical game can have data from MULTIPLE sources
- Each source maintains its own external metadata
- No duplication - clean many-to-one relationship
- Allows aggregating/merging data from multiple sources

---

## Code Changes Required (PENDING)

### Phase 1: Function Renaming in `src/database_ops/ingest_providers.rs`

#### Functions to Rename

| Old Name | New Name | Line | Status |
|----------|----------|------|--------|
| `ensure_provider_media_links_with_meta()` | `ensure_vg_source_media_links_with_meta()` | ~5758 | ❌ Pending |
| `ensure_provider_media_links()` | `ensure_vg_source_media_links()` | ~6006 | ❌ Pending |
| `ensure_provider()` | `ensure_video_game_source_legacy()` + deprecate | ~663 | ❌ Pending |
| `ensure_game_provider()` | DELETE (duplicate) | ~745 | ❌ Pending |

#### Functions CORRECT (Keep As-Is)

- ✅ `ensure_video_game_source()` - Line ~1684 (already correct)

### Phase 2: Update All Provider Implementations (14 files)

Files requiring updates:
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
14. Additional provider binaries

**Required changes in each file:**
- Update function calls: `ensure_provider_media_links()` → `ensure_vg_source_media_links()`
- Update function calls: `ensure_provider_media_links_with_meta()` → `ensure_vg_source_media_links_with_meta()`
- Update table name in queries: `provider_media_links` → `vg_source_media_links`
- Update column name: `provider_item_id` → `video_game_source_id`

### Phase 3: Update ProviderEntityCache

**File**: `src/database_ops/ingest_providers.rs` (around line 1165)

```rust
// Current (WRONG):
pub struct ProviderEntityCache {
    pub db: Db,
    // ... fields with provider terminology
}

impl ProviderEntityCache {
    pub async fn ensure_provider_item(...) -> Result<i64> {
        // uses provider_id variable
    }
}

// Target (CORRECT):
pub struct VideoGameSourceCache {
    pub db: Db,
    // ... fields with video_game_source terminology
}

impl VideoGameSourceCache {
    pub async fn ensure_source_item(...) -> Result<i64> {
        // uses video_game_source_id variable
    }
}
```

---

## Terminology Standard

### ❌ DEPRECATED (Do Not Use)

- "provider" (use "video_game_source")
- "provider_item" (use "video_game_source")
- "provider_media_links" (use "vg_source_media_links")
- `provider_item_id` column (use `video_game_source_id`)

### ✅ CORRECT (Use These)

- "video_game_source" or "vg_source"
- "video_game_sources" table
- "vg_source_media_links" table
- `video_game_source_id` column for foreign keys
- `ensure_video_game_source()` function
- `ensure_vg_source_media_links()` function

---

## Common Provider Keys

These are the canonical `provider_key` values used in `video_game_sources`:

- `"ps-store"` - PlayStation Store
- `"steam"` - Steam
- `"microsoft-store"` - Xbox/Microsoft Store
- `"igdb"` - Internet Game Database
- `"giant-bomb"` - GiantBomb
- `"rawg"` - RAWG
- `"tgdb"` - TheGamesDB
- `"nexarda"` - Nexarda
- `"itad"` - IsThereAnyDeal

---

## Testing Checklist

After code changes:

- [ ] All 14 provider files compile successfully
- [ ] `cargo check` passes
- [ ] Database queries use `vg_source_media_links` table name
- [ ] Foreign key references use `video_game_source_id` column
- [ ] No references to `provider_media_links` remain in code
- [ ] No references to `provider_item_id` remain in queries
- [ ] Integration test: Full ingestion run completes
- [ ] Verify data writes to correct tables
- [ ] Check that media links properly reference `video_game_sources.id`

---

## Success Criteria

1. ✅ Database schema uses `vg_source_*` naming
2. ✅ `video_game_sources` table exists and is unique by `provider_key`
3. ✅ No `providers` or `provider_items` tables exist
4. ❌ All Rust functions use `video_game_source` or `vg_source` terminology (PENDING)
5. ❌ All queries reference `vg_source_media_links` table (PENDING)
6. ❌ All foreign keys use `video_game_source_id` column (PENDING)
7. ❌ All 14 provider implementations updated (PENDING)

---

## Rollback Plan

If issues arise, run:

```sql
DROP TABLE IF EXISTS vg_source_media_links CASCADE;
DROP TABLE IF EXISTS video_game_sources CASCADE;
```

Then restore from backup or re-run old migrations.

---

**Next Steps**: Begin Phase 1 function renaming in `ingest_providers.rs`
