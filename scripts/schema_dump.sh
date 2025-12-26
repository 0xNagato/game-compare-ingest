#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUTPUT_PATH="${OUTPUT_PATH:-${ROOT_DIR}/docs/schema_reference.sql}"
DATABASE_URL="${DATABASE_URL:-}"

if [[ -z "${DATABASE_URL}" ]]; then
  echo "[schema-dump] DATABASE_URL must be set" >&2
  exit 1
fi

I_UNDERSTAND="${I_UNDERSTAND:-drop}"
if [[ "${I_UNDERSTAND}" != "drop" ]]; then
  echo "[schema-dump] I_UNDERSTAND=drop is required to allow destructive reset" >&2
  exit 1
fi

if ! command -v pg_dump >/dev/null 2>&1; then
  echo "[schema-dump] pg_dump is required" >&2
  exit 1
fi

MIG_FILE_DEFAULT="${ROOT_DIR}/migrations/0001_full_consolidated_schema.sql"
MIG_FILE="${MIG_FILE:-${MIG_FILE_DEFAULT}}"
if [[ ! -f "${MIG_FILE}" ]]; then
  echo "[schema-dump] migration file not found: ${MIG_FILE}" >&2
  exit 1
fi

echo "[schema-dump] resetting database and applying migrations via ${MIG_FILE}"
I_UNDERSTAND=drop DATABASE_URL="${DATABASE_URL}" MIG_FILE="${MIG_FILE}" cargo run --quiet --bin db_reset_migrate >/tmp/schema_dump.log 2>&1 || {
  cat /tmp/schema_dump.log >&2
  exit 1
}

TMP_FILE="$(mktemp)"
echo "[schema-dump] dumping schema to ${OUTPUT_PATH}"
pg_dump --schema-only --no-owner --no-privileges "${DATABASE_URL}" \
  | sed '/^SET \(idle\|statement\)_timeout/d' \
  | sed '/^SET search_path/d' > "${TMP_FILE}"

mv "${TMP_FILE}" "${OUTPUT_PATH}"

if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "[schema-dump] diff against committed schema (if any)"
  git status --short -- "${OUTPUT_PATH}"
fi

echo "[schema-dump] complete"
