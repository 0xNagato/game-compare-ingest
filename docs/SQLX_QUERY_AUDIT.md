# sqlx Query Pattern Audit & Migration Plan

## Executive Summary

**Current State**: 100% of SQL queries use `sqlx::query()` runtime prepared statements  
**Target State**: Migrate to `sqlx::query!()` compile-time validated macros  
**Total Files**: 4 files with query usage  
**Total Occurrences**: ~60 `sqlx::query()` calls  
**Estimated Effort**: 4-6 hours  

---

## Why Migrate?

### `sqlx::query()` (Current - Runtime)
❌ **No compile-time SQL validation**  
❌ **No type checking**  
❌ **Runtime binding errors**  
❌ **Requires `.persistent(false)` workarounds**  
❌ **Manual row extraction with `.get()`**  

### `sqlx::query!()` (Target - Compile-time)
✅ **SQL validated at compile time**  
✅ **Full Rust type inference**  
✅ **Catches typos/schema drift before runtime**  
✅ **Auto-generated struct fields**  
✅ **Better IDE autocomplete**  

---

## Conversion Priority Matrix

| File | Lines | Priority | Reason |
|------|-------|----------|--------|
| `ingest_providers.rs` | ~30 | 🔴 **CRITICAL** | Core ingestion logic, high QPS |
| `gb_ingest_debug.rs` | ~20 | 🟡 **HIGH** | Active development, frequent changes |
| `search.rs` | ~2 | 🟢 **MEDIUM** | Read path, less critical |
| `test_write.rs` | ~3 | 🔵 **LOW** | Test file, dynamic SQL acceptable |

---

## File-by-File Breakdown

### 1️⃣ `/src/database_ops/ingest_providers.rs` (30 queries)

**Impact**: Core provider ingestion pipeline  
**Risk**: HIGH - production write path  

#### Conversion Candidates

```rust
// ❌ BEFORE (line 16)
let rec = sqlx::query("SELECT id FROM public.providers WHERE name = $1")
    .persistent(false)
    .bind(&name)
    .fetch_optional(pool)
    .await?;
let provider_id: i64 = rec.unwrap().get(0);

// ✅ AFTER
let rec = sqlx::query!(
    "SELECT id FROM public.providers WHERE name = $1",
    &name
)
.fetch_optional(pool)
.await?;
let provider_id: i64 = rec.unwrap().id;
```

#### Full Conversion List (30 occurrences)

| Line | Query | Conversion |
|------|-------|------------|
| 16 | `SELECT id FROM providers WHERE name = $1` | ✅ Simple macro |
| 21 | `INSERT INTO providers ... RETURNING id` | ✅ Simple macro |
| 32 | `SELECT id FROM retailers WHERE name = $1` | ✅ Simple macro |
| 37 | `INSERT INTO retailers ... RETURNING id` | ✅ Simple macro |
| 49 | `SELECT id FROM products WHERE slug = $1` | ✅ Simple macro |
| 54 | `INSERT INTO products ... RETURNING id` | ✅ Simple macro |
| 64 | `SELECT id FROM sellables WHERE software_title_id = $1` | ✅ Simple macro |
| 69 | `INSERT INTO sellables ... RETURNING id` | ✅ Simple macro |
| 80 | `SELECT id FROM offers WHERE ... AND sku = $3` | ✅ Simple macro |
| 83 | `SELECT id FROM offers WHERE ... AND sku IS NULL` | ✅ Simple macro |
| 88 | `INSERT INTO offers ... RETURNING id` | ✅ Simple macro |
| 96 | `SELECT id FROM offer_jurisdictions ...` | ✅ Simple macro |
| 99 | `INSERT INTO offer_jurisdictions ...` | ✅ Simple macro |
| 107 | `SELECT id FROM provider_items ...` | ✅ Simple macro |
| 110 | `INSERT INTO provider_items ...` | ✅ Simple macro |
| 118 | `SELECT id FROM provider_offers ...` | ✅ Simple macro |
| 121 | `INSERT INTO provider_offers ...` | ✅ Simple macro |
| 136 | `INSERT INTO prices ... (bulk)` | ⚠️ Dynamic - use `query_as!` |
| 207 | `SELECT id FROM platforms WHERE name = $1` | ✅ Simple macro |

**Estimated Time**: 2-3 hours  
**Benefit**: Catch schema drift, eliminate `.persistent(false)` hacks  

---

### 2️⃣ `/src/bin/gb_ingest_debug.rs` (20 queries)

**Impact**: GiantBomb ingestion debugging  
**Risk**: MEDIUM - active development file  

#### Pattern: Repetitive upsert chains

All 20 queries follow this pattern:
```rust
// ❌ BEFORE
let provider_id: i64 = match sqlx::query("SELECT id FROM public.providers WHERE name = $1")
    .bind(&provider_name)
    .fetch_optional(&pool)
    .await?
{
    Some(row) => row.get(0),
    None => {
    let row = sqlx::query("INSERT INTO public.providers ...")
            .bind(&provider_name)
            .fetch_one(&pool)
            .await?;
        row.get(0)
    }
};

// ✅ AFTER
let provider_id: i64 = match sqlx::query_scalar!(
    "SELECT id FROM public.providers WHERE name = $1",
    &provider_name
)
.fetch_optional(&pool)
.await?
{
    Some(id) => id,
    None => sqlx::query_scalar!(
    "INSERT INTO public.providers ... RETURNING id",
        &provider_name
    )
    .fetch_one(&pool)
    .await?
};
```

**Estimated Time**: 1.5-2 hours  
**Benefit**: Type safety in debug pipeline  

---

### 3️⃣ `/src/database_ops/search.rs` (2 queries)

**Impact**: Read path search queries  
**Risk**: LOW - read-only  

```rust
// Line 17: Sellables search
let sellables = sqlx::query("SELECT id, ... FROM public.sellables ...")
    .fetch_all(pool)
    .await?;

// Line 33: Provider items search
let provider_items = sqlx::query("SELECT id, ... FROM public.provider_items ...")
    .fetch_all(pool)
    .await?;
```

**Conversion**: Use `query_as!` with custom structs  
**Estimated Time**: 30 minutes  

---

### 4️⃣ `/src/bin/test_write.rs` (3 queries)

**Impact**: Test file only  
**Risk**: NONE  

```rust
// Line 23: Admin cleanup
let _ = sqlx::query("DEALLOCATE ALL").execute(&pool).await;

// Line 29: Dynamic table creation
sqlx::query("CREATE TABLE IF NOT EXISTS ...")

// Line 33: Test insert
sqlx::query("INSERT INTO public.test_write(name) VALUES($1)")
```

**Decision**: ⚠️ **SKIP** - Test file, dynamic SQL acceptable  

---

## Migration Strategy

### Phase 1: Core Ingestion (Week 1)
1. Convert `ingest_providers.rs` (30 queries) → **2-3 hours**
2. Run `cargo sqlx prepare` to generate metadata
3. Full integration test suite
4. Deploy to staging

### Phase 2: Debug Tools (Week 2)
1. Convert `gb_ingest_debug.rs` (20 queries) → **1.5-2 hours**
2. Verify GiantBomb pipeline
3. Update docs

### Phase 3: Read Path (Week 2)
1. Convert `search.rs` (2 queries) → **30 minutes**
2. Benchmark before/after
3. Document query patterns

### Phase 4: Enforcement (Week 2)
1. Add pre-commit hook → **30 minutes**
2. Update CONTRIBUTING.md → **15 minutes**
3. Team training → **1 hour**

**Total Estimated Time**: 4-6 hours + 1 hour training  

---

## Conversion Recipe

### Step-by-Step Template

```rust
// 1️⃣ SIMPLE SELECT (single column)
// BEFORE
let id: i64 = sqlx::query("SELECT id FROM table WHERE x = $1")
    .bind(&val)
    .fetch_one(pool)
    .await?
    .get(0);

// AFTER
let id: i64 = sqlx::query_scalar!(
    "SELECT id FROM table WHERE x = $1",
    &val
)
.fetch_one(pool)
.await?;

// 2️⃣ SELECT (multiple columns)
// BEFORE
let row = sqlx::query("SELECT id, name, created_at FROM table")
    .fetch_one(pool)
    .await?;
let id: i64 = row.get("id");
let name: String = row.get("name");

// AFTER
let row = sqlx::query!(
    "SELECT id, name, created_at FROM table"
)
.fetch_one(pool)
.await?;
// Auto-generated fields: row.id, row.name, row.created_at

// 3️⃣ INSERT RETURNING
// BEFORE
let id: i64 = sqlx::query("INSERT INTO table(x) VALUES($1) RETURNING id")
    .bind(&val)
    .fetch_one(pool)
    .await?
    .get(0);

// AFTER
let id: i64 = sqlx::query_scalar!(
    "INSERT INTO table(x) VALUES($1) RETURNING id",
    &val
)
.fetch_one(pool)
.await?;

// 4️⃣ BULK INSERT (dynamic)
// Keep as query() - dynamic SQL is acceptable here
let mut query_builder = sqlx::QueryBuilder::new(
    "INSERT INTO prices(offer_jurisdiction_id, recorded_at, amount_minor)"
);
query_builder.push_values(rows, |mut b, row| {
    b.push_bind(row.offer_jurisdiction_id)
     .push_bind(row.recorded_at)
     .push_bind(row.amount_minor);
});
query_builder.build().execute(pool).await?;
```

---

## Pre-Commit Hook (Enforcement)

**File**: `.git/hooks/pre-commit`

```bash
#!/bin/bash
# Prevent runtime query() usage in production code

# Allow query() in test files and dynamic SQL contexts
EXCLUDED_PATTERNS="test_|query_builder|QueryBuilder"

# Search for sqlx::query( in staged files
VIOLATIONS=$(git diff --cached --name-only --diff-filter=ACM | \
  grep '\.rs$' | \
  xargs grep -n 'sqlx::query(' | \
  grep -v -E "$EXCLUDED_PATTERNS" || true)

if [ ! -z "$VIOLATIONS" ]; then
  echo "❌ ERROR: Found sqlx::query() in production code"
  echo ""
  echo "Use sqlx::query!() macro instead for compile-time validation:"
  echo "$VIOLATIONS"
  echo ""
  echo "See docs/SQLX_QUERY_AUDIT.md for migration guide"
  echo ""
  echo "To bypass (discouraged): git commit --no-verify"
  exit 1
fi

echo "✅ No sqlx::query() violations found"
exit 0
```

**Installation**:
```bash
chmod +x .git/hooks/pre-commit
```

---

## Acceptance Tests

### Test 1: Compile-Time Type Safety
```rust
// This should FAIL at compile time:
let id = sqlx::query!("SELECT id FROM non_existent_table")
    .fetch_one(&pool)
    .await?;
// ERROR: relation "non_existent_table" does not exist
```

### Test 2: Schema Drift Detection
```rust
// If column renamed in DB, compile fails:
let name = sqlx::query!("SELECT old_column_name FROM table")
    .fetch_one(&pool)
    .await?;
// ERROR: column "old_column_name" does not exist
```

### Test 3: Zero Runtime Surprises
```rust
// Field access is validated:
let row = sqlx::query!("SELECT id, name FROM table").fetch_one(&pool).await?;
println!("{}", row.id);    // ✅ Works
println!("{}", row.slug);  // ❌ Compile error: no field `slug`
```

---

## Rollback Plan

If issues arise during migration:

1. **Immediate**: Revert file via git
   ```bash
   git checkout HEAD -- src/database_ops/ingest_providers.rs
   ```

2. **Prepare Metadata**: Delete sqlx-data.json if corrupted
   ```bash
   rm -rf .sqlx/
   cargo sqlx prepare --database-url=$DATABASE_URL
   ```

3. **Bypass Hook**: Emergency commits
   ```bash
   git commit --no-verify
   ```

---

## Success Metrics

- ✅ Zero `sqlx::query()` in `src/database_ops/` (excluding dynamic SQL)
- ✅ Pre-commit hook catches violations
- ✅ All queries validated against live schema
- ✅ CI builds with offline mode (`--offline`)
- ✅ Team trained on query! syntax

---

## FAQ

**Q: When is `query()` acceptable?**  
A: Dynamic SQL (QueryBuilder, runtime table names), test files, admin scripts

**Q: How do I handle optional columns?**  
A: `query!` returns `Option<T>` for nullable columns automatically

**Q: What if my query is complex?**  
A: Split into smaller queries, or use CTEs. If truly dynamic, document why `query()` is needed.

**Q: How often to run `cargo sqlx prepare`?**  
A: After every schema change. Add to migration workflow.

---

## References

- [sqlx Macro Docs](https://docs.rs/sqlx/latest/sqlx/macro.query.html)
- [Compile-Time Verification](https://github.com/launchbadge/sqlx/blob/main/FAQ.md#how-can-i-do-a-select--query)
- [Offline Mode Setup](https://github.com/launchbadge/sqlx/blob/main/sqlx-cli/README.md#enable-building-in-offline-mode-with-query)

---

**Last Updated**: 2025-11-10  
**Status**: Ready for Phase 1 execution  
**Owner**: Supervisor Agent → Rust Dev Agent
