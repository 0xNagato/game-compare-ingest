# Performance Recommendations (GameCompare Ingestion & Dedupe)

## SQLite → Postgres Import

| Area            | Recommendation                                           | Rationale                                                       |
| --------------- | -------------------------------------------------------- | --------------------------------------------------------------- |
| Journal / Sync  | `journal_mode=OFF`, `synchronous=OFF` (read-only import) | Eliminates fsync & rollback journal I/O for static source DB.   |
| Temp Structures | `temp_store=MEMORY`                                      | Keeps sort / temp B-Trees off disk.                             |
| Cache Size      | `cache_size=-160000` (≈160MB)                            | Larger page cache to reduce file reads; negative uses KB units. |
| MMap            | `mmap_size=536870912` (512MB)                            | Memory-mapped pages accelerate sequential scans.                |
| Page Size       | `page_size=4096` (if database rebuilt)                   | Larger pages reduce b-tree depth and I/O ops.                   |
| Safe Mode       | Set `SQLITE_PERF_SAFE=1` for conservative run            | Keeps durability if source may still be written elsewhere.      |

## Platform Dedupe Strategy

1. Snapshot original rows (`platforms_backup`).
2. Compute canonical key: strip non-alphanumeric, lowercase.
3. Merge duplicates selecting smallest id or best short code.
4. Repoint dependent FKs (`video_games.platform_id`).
5. Delete duplicate rows; record mapping in `platforms_dedupe_map`.
6. Enforce `UNIQUE (canonical_code)` to prevent reintroduction.
7. For rollback, use `platforms_restore` binary to rehydrate.

## Media Ingestion Optimizations

| Technique           | Implementation                        | Benefit                                              |
| ------------------- | ------------------------------------- | ---------------------------------------------------- |
| In-batch URL de-dup | Lowercase + trim set before queries   | Prevents redundant SELECT/INSERT same batch.         |
| Bulk INSERT         | `QueryBuilder` multi-row VALUES       | Reduces round trips and statement planning overhead. |
| Conditional UPDATE  | Skip updates unless metadata provided | Avoids unnecessary writes.                           |
| Conflict Handling   | `ON CONFLICT DO NOTHING` for inserts  | Safe under concurrent ingestion workers.             |
| URL Normalization   | Case-insensitive + trimming           | Consistent canonical comparisons across providers.   |
| Future (LRU)        | Shared Redis / in-memory LRU          | Skip cross-run duplicates (not yet implemented).     |

## Pricing / Time-Series Considerations

| Item              | Recommendation                         | Rationale                                                |
| ----------------- | -------------------------------------- | -------------------------------------------------------- |
| Partition Horizon | Pre-create +6 months                   | Prevent insert-time DDL; stable ingest latency.          |
| Parent Indexes    | Define on ONLY parent                  | Auto-cloned to partitions; keeps consistent query plans. |
| Recent Queries    | Use `current_price` for latest lookups | Avoid scanning partitions for common UX paths.           |
| FX Enrichment     | Batch currency lookups                 | Minimize repetitive per-row queries.                     |
| Guard Provider    | Compare against reference (e.g. Itad)  | Detect anomalous price spikes.                           |

## Post-Dedupe Verification Steps

Run:

```bash
cargo run --bin verify_platforms
```

Confirm:

- No duplicate canonical groups.
- All `video_games.platform_id` resolve.

## Benchmark Harness Usage

```bash
# Default tables & 3 iterations
cargo run --bin bench_db

# Custom tables & iterations
BENCH_TABLES="products,platforms" BENCH_ITER=5 cargo run --bin bench_db
```

## Future Enhancements

| Enhancement                  | Impact                                         |
| ---------------------------- | ---------------------------------------------- |
| Redis-backed media LRU       | Cross-process duplicate avoidance.             |
| Prepared Statement Telemetry | Detect accidental persistent statements early. |
| Partition Stats Dashboard    | Identify skew & prune strategy.                |
| Adaptive Batch Sizing        | Tune batch size based on latency target.       |

## Operational Guardrails

- Always run platform dedupe with backups enabled first (`PLATFORMS_BACKUP=1`).
- Keep `AUTO_MIGRATE` disabled for production ingest binaries; apply migrations separately to control lock windows.
- Avoid `journal_mode=OFF` if any SQLite writes might occur during import.
- Set `SQLITE_PERF=0` for correctness-first investigations.
