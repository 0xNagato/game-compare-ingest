#!/usr/bin/env bash
# create_indexes.sh
# Usage: DATABASE_URL=postgres://user:pass@host:5432/dbname ./scripts/create_indexes.sh
set -euo pipefail

if [ -z "${DATABASE_URL:-}" ]; then
  echo "Please set DATABASE_URL environment variable (psql will use it)."
  exit 1
fi

run_index() {
  local sql="$1"
  echo "Running: $sql"
  if ! PGPASSWORD="${PGPASSWORD:-}" psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "$sql"; then
    echo "Index creation failed for: $sql"
    return 1
  fi
  return 0
}

# Only include indexes for tables known in this schema. The ingestion_jobs table is not present.
idxs=(
  "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_provider_items_needs_process ON public.provider_items (provider_id, id) WHERE (attributes IS NULL OR last_seen_at IS NULL) AND locked_by IS NULL;"
  "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_provider_offers_needs_process ON public.provider_offers (provider_id, id) WHERE (is_active IS NULL) AND locked_by IS NULL;"
  "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_provider_items_locked_at ON public.provider_items (locked_at) WHERE locked_at IS NOT NULL;"
)

for sql in "${idxs[@]}"; do
  if ! run_index "$sql"; then
    echo "Error creating index. Exiting."
    exit 2
  fi
done

echo "All indexes created (or already exist)."
