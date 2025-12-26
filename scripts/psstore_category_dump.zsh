#!/usr/bin/env zsh
# Fetch all pages of PS Store categoryGridRetrieve for a given category ID
# and merge them into a single JSON file to avoid repeated live calls.
#
# Requirements: curl, jq
#
# Usage:
#   ./scripts/psstore_category_dump.zsh <CATEGORY_ID> [LOCALE] [OUTFILE]
#
# Examples:
#   # Dump en-US category 4cbf39e2-... to ps_category_en-US.json
#   ./scripts/psstore_category_dump.zsh 4cbf39e2-5749-4970-ba81-93a489e4570c en-US ps_category_en-US.json
#
#   # Dump for en-GB using defaults (outfile auto-named)
#   ./scripts/psstore_category_dump.zsh 4cbf39e2-5749-4970-ba81-93a489e4570c en-GB
#
# Notes:
# - The GraphQL endpoint uses a persisted query hash seen in logs; update HASH if Sony changes it.
# - This script paginates by 100 until an empty result set appears or MAX_PAGES is reached.

set -euo pipefail

if ! command -v jq >/dev/null 2>&1; then
  echo "Error: jq is required (brew install jq)" >&2
  exit 1
fi

CATEGORY_ID=${1:-}
if [[ -z "$CATEGORY_ID" ]]; then
  echo "Usage: $0 <CATEGORY_ID> [LOCALE] [OUTFILE]" >&2
  exit 1
fi
LOCALE=${2:-en-US}
OUTFILE=${3:-ps_category_${LOCALE}.json}

# Tuning
SIZE=${SIZE:-100}
MAX_PAGES=${MAX_PAGES:-200}   # 200 * 100 = 20,000 items max
SLEEP_MS=${SLEEP_MS:-250}     # polite delay between requests

BASE_URL="https://web.np.playstation.com/api/graphql/v1/op"
OPERATION="categoryGridRetrieve"
# Persisted query hash observed in logs; change if the API updates the hash
# Prefer PSSTORE_SHA256 override if exported in your shell
HASH=${HASH:-${PSSTORE_SHA256:-9845afc0dbaab4965f6563fffc703f588c8e76792000e8610843b8d3ee9c4c09}}

# Temp directory for pages
TMP_DIR=$(mktemp -d -t ps_category_pages.XXXXXXXX)
trap 'rm -rf "$TMP_DIR"' EXIT

echo "Fetching $OPERATION pages for category=$CATEGORY_ID locale=$LOCALE -> $OUTFILE" >&2

page=0
offset=0
total_items=0

locale_lower=${LOCALE:l}
accept_lang="${LOCALE};q=0.9"
referer="https://store.playstation.com/${locale_lower}/pages/latest"

while (( page < MAX_PAGES )); do
  # Build variables JSON then URL-encode safely
  vars=$(jq -nc --arg id "$CATEGORY_ID" --argjson offset $offset --argjson size $SIZE '{facetOptions: [], filterBy: [], id: $id, pageArgs: {offset: $offset, size: $size}, sortBy: {isAscending: false, name: "releaseDate"}}')
  vars_enc=$(python3 -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.stdin.read(), safe=""))' <<< "$vars")
  ext=$(jq -nc --arg hash "$HASH" '{persistedQuery:{version:1, sha256Hash:$hash}}')
  ext_enc=$(python3 -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.stdin.read(), safe=""))' <<< "$ext")

  url="$BASE_URL?operationName=$OPERATION&variables=$vars_enc&extensions=$ext_enc"
  out_page="$TMP_DIR/page_${page}.json"

  # Perform request
  http_code=$(curl -sS \
    -H "Accept: application/json" \
    -H "Accept-Language: $accept_lang" \
    -H "X-PSN-Store-Locale-Override: $LOCALE" \
    -H "X-PSN-Store-Front: $locale_lower" \
    -H "x-apollo-operation-name: $OPERATION" \
    -H "apollo-require-preflight: true" \
    -H "Origin: https://store.playstation.com" \
    -H "Referer: $referer" \
    -H "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0 Safari/537.36" \
    -o "$out_page" -w "%{http_code}" \
    "$url")

  if [[ "$http_code" != "200" ]]; then
    echo "WARN: HTTP $http_code at offset=$offset; stopping." >&2
    break
  fi

  # Check for GraphQL errors and row count
  if jq -e '.errors and (.errors | length > 0)' "$out_page" >/dev/null 2>&1; then
    echo "WARN: GraphQL errors at offset=$offset; body: $(jq -c '.errors' "$out_page")" >&2
    # Retry once after small delay (simple strategy)
    sleep 0.5
    http_code=$(curl -sS \
      -H "Accept: application/json" \
      -H "Accept-Language: $accept_lang" \
      -H "X-PSN-Store-Locale-Override: $LOCALE" \
      -H "X-PSN-Store-Front: $locale_lower" \
      -H "x-apollo-operation-name: $OPERATION" \
      -H "apollo-require-preflight: true" \
      -H "Origin: https://store.playstation.com" \
      -H "Referer: $referer" \
      -H "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0 Safari/537.36" \
      -o "$out_page" -w "%{http_code}" \
      "$url")
    if [[ "$http_code" != "200" ]]; then
      echo "WARN: HTTP $http_code again; stopping." >&2
      break
    fi
  fi

  count=$(jq -r '(.data.categoryGridRetrieve?.products? // .data.categoryGridRetrieve?.results? // []) | length' "$out_page")
  if [[ "$count" == "" ]]; then count=0; fi
  echo "page=$page offset=$offset items=$count" >&2
  if (( count == 0 )); then
    break
  fi

  total_items=$(( total_items + count ))
  page=$(( page + 1 ))
  offset=$(( offset + SIZE ))
  # polite delay
  python3 - <<PY >/dev/null 2>&1
import time
ms=$SLEEP_MS
if ms>0:
    time.sleep(ms/1000.0)
PY

done

# Merge all pages into a single JSON file with a combined results array
# We try both fields Sony used historically: products[] or results[]
jq -s '{data:{categoryGridRetrieve:{results: (map(.data.categoryGridRetrieve | .products // .results // []) | add)}}}' \
  "$TMP_DIR"/page_*.json > "$OUTFILE"

echo "Wrote $OUTFILE with total_items=$total_items" >&2
