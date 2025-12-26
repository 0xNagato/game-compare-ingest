# Contributing Guidelines

## SQL Query Standards

### ✅ REQUIRED: Use `sqlx::query!()` Macro

All SQL queries **MUST** use the compile-time validated `query!()` macro family:

```rust
// ✅ CORRECT: Compile-time validated
let id = sqlx::query_scalar!(
    "SELECT id FROM providers WHERE name = $1",
    &name
)
.fetch_one(&pool)
.await?;

// ❌ FORBIDDEN: Runtime prepared statements
let id: i64 = sqlx::query("SELECT id FROM providers WHERE name = $1")
    .bind(&name)
    .fetch_one(&pool)
    .await?
    .get(0);
```

### Why?

| `query!()` Macro | `query()` Runtime |
|------------------|-------------------|
| ✅ SQL validated at compile time | ❌ Errors only at runtime |
| ✅ Type safety guaranteed | ❌ Manual type casting |
| ✅ Auto-generated struct fields | ❌ Manual `.get()` extraction |
| ✅ IDE autocomplete | ❌ No autocomplete |
| ✅ Catches schema drift | ❌ Silent failures |

### Macro Family

```rust
// Single scalar value (id, count, bool)
let count: i64 = sqlx::query_scalar!(
    "SELECT COUNT(*) FROM table"
).fetch_one(&pool).await?;

// Single row, multiple columns
let row = sqlx::query!(
    "SELECT id, name, created_at FROM table WHERE id = $1",
    entity_id
).fetch_one(&pool).await?;
// Auto-fields: row.id, row.name, row.created_at

// Multiple rows
let rows = sqlx::query_as!(
    MyStruct,
    "SELECT id, name FROM table"
).fetch_all(&pool).await?;
```

### Exceptions (Dynamic SQL)

`query()` is **acceptable** only for:
- Dynamic table/column names (QueryBuilder)
- Bulk inserts with variable row counts
- Test utilities

Mark exceptions with comment:
```rust
// DYNAMIC SQL: Table name from user input
let table = format!("prices_{}", partition_suffix);
let rows = sqlx::query(&format!("SELECT * FROM {}", table))
    .fetch_all(&pool)
    .await?;
```

### Setup

1. **Install sqlx-cli**:
   ```bash
   cargo install sqlx-cli --no-default-features --features postgres
   ```

2. **Prepare metadata** (run after schema changes):
   ```bash
   export DATABASE_URL="postgresql://user:pass@localhost:5432/db"
   cargo sqlx prepare
   ```

3. **Install pre-commit hook**:
   ```bash
   chmod +x .git/hooks/pre-commit
   ```

### Pre-Commit Hook

The hook **automatically rejects** commits with `sqlx::query()` in production code:

```
❌ ERROR: Found sqlx::query() in production code

src/database_ops/ingest_providers.rs:
16:    let rec = sqlx::query("SELECT id FROM ...")

📚 Migration guide: docs/SQLX_QUERY_AUDIT.md
```

Bypass (discouraged):
```bash
git commit --no-verify
```

### Migration Guide

See **[docs/SQLX_QUERY_AUDIT.md](docs/SQLX_QUERY_AUDIT.md)** for:
- File-by-file conversion checklist
- Before/after examples
- Common patterns
- Troubleshooting

### Testing

Before submitting PR:

```bash
# Validate all queries against live DB
cargo sqlx prepare --check

# Run tests
cargo test

# Check pre-commit hook
git add .
git commit -m "test" --dry-run
```

### CI/CD

GitHub Actions validates queries in offline mode:
```yaml
- run: cargo sqlx prepare --check
- run: cargo build --offline
```

---

## Database Standards

### Schema Qualification

Use the public schema (no prefix needed):
```sql
-- ✅ CORRECT
SELECT id FROM providers WHERE name = $1

-- ❌ WRONG
SELECT id FROM providers WHERE name = $1
```

### Migrations

- **Idempotent**: Safe to re-run
- **Reversible**: Document rollback steps
- **Partitioned**: Monthly for `prices` table
- **Concurrent indexes**: Use `CONCURRENTLY` for production

See [migrations/README.md](migrations/README.md)

---

## Code Style

### Rust

- **Format**: `cargo fmt`
- **Lint**: `cargo clippy -- -D warnings`
- **Test**: `cargo test --all-features`

### SQL

- **Lowercase keywords**: `select`, `from`, `where`
- **Indentation**: 2 spaces
- **Comments**: Explain WHY, not WHAT

---

## Pull Request Checklist

- [ ] All queries use `query!()` macro
- [ ] `cargo sqlx prepare` run and committed
- [ ] Tests pass (`cargo test`)
- [ ] Pre-commit hook passes
- [ ] Schema changes documented in migration comments
- [ ] Performance impact assessed (EXPLAIN ANALYZE)

---

**Questions?** See [docs/SQLX_QUERY_AUDIT.md](docs/SQLX_QUERY_AUDIT.md) or ask in #dev-rust
