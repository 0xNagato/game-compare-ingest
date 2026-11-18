# Contributing

## Building

The PlayStation Store client is a separate, private crate. `Cargo.toml` expects it at `./psstore_client`, so the
crate only builds with it checked out next to `src/`.

```bash
cargo build --release
cargo fmt --check
cargo clippy
cargo test
```

One-off probes and experiments live in `examples/` and run with `cargo run --example <name>`. Long-running workers
and operational tools live in `src/bin/`.

## SQL

- Queries go through `sqlx` with bound parameters: `sqlx::query("... WHERE name = $1").bind(&name)`. Never format a
  value into the SQL string.
- New code may use the compile-time checked `sqlx::query!()` family. It needs `DATABASE_URL` at build time, so keep
  it to code that is built against a live schema.
- Tables live in the `public` schema; no schema prefix is needed.

## Migrations

- Add migrations to `migrations/` with the next free number.
- Make them safe to re-run (`CREATE ... IF NOT EXISTS`, `ON CONFLICT DO NOTHING`).
- Create indexes on large tables `CONCURRENTLY`.
- The `prices` table is partitioned by month; new partitions follow the existing naming.

## Pull requests

- `cargo fmt --check`, `cargo clippy` and `cargo test` pass.
- Schema changes come with a migration and a note on how to roll it back.
- The `no-sprawl` workflow fails a pull request that adds or renames files unless its description contains a
  `Justification:` line saying why the files are needed.

Questions: open an issue on this repository.
