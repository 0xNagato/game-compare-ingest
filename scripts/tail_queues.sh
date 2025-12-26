#!/usr/bin/env bash
# Tail PGMQ queue metrics for all running ingest workers.
# Usage: ./scripts/tail_queues.sh [interval_seconds]
# Defaults to 5s refresh.

set -euo pipefail
INTERVAL="${1:-5}"

# Define worker name→port map (adjust if ports change)
QUEUES=(
  "default_ingest:9025"
  "psstore_ingest:9081"
  "igdb_catalog:9082"
  "gb_catalog:9083"
  "steam_ingest:9084"
  "itad_pricing:9085"
  "nexarda_ingest:9086"
  "xbox_ingest:9087"
)

function fetch_metrics() {
  local name="$1"; local port="$2";
  # /api/pgmq_metrics returns row_to_json(metrics)
  local json
  if ! json=$(curl -s --max-time 2 "http://127.0.0.1:${port}/api/pgmq_metrics" 2>/dev/null); then
    echo "$(date +%H:%M:%S) ${name} (port ${port}) ERROR: unreachable"; return
  fi
  # Expect JSON like: {"queue_name":"default_ingest","visible":12,"delayed":0,"buried":0,"total_messages":1482}
  local visible delayed buried total
  visible=$(echo "$json" | jq -r '.visible // .ready // 0')
  delayed=$(echo "$json" | jq -r '.delayed // 0')
  buried=$(echo "$json" | jq -r '.buried // 0')
  total=$(echo "$json" | jq -r '.total_messages // .total // 0')
  printf "%s %-16s v:%-6s d:%-4s b:%-4s total:%-7s\n" "$(date +%H:%M:%S)" "$name" "$visible" "$delayed" "$buried" "$total"
}

while true; do
  echo "---------------- $(date) ----------------"
  for q in "${QUEUES[@]}"; do
    IFS=':' read -r name port <<<"$q"
    fetch_metrics "$name" "$port"
  done | sort
  sleep "$INTERVAL"
  echo
done
