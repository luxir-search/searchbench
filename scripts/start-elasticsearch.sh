#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/engine-common.sh"
VERSION=$(python3 -c 'import json; print(json.load(open("'"$ROOT"'/engines/versions.json"))["elasticsearch"]["version"])')
HOME_DIR="$ROOT/engines/elasticsearch-$VERSION"
PIDFILE="$ROOT/run/elasticsearch.pid"
SEARCHBENCH_DATA=$(engine_data_dir elasticsearch)
export SEARCHBENCH_DATA
mkdir -p "$ROOT/run" "$SEARCHBENCH_DATA" "$ROOT/logs/elasticsearch"
echo "elasticsearch dataset: $SEARCHBENCH_DATASET" >&2
[[ -x "$HOME_DIR/bin/elasticsearch" ]] || "$ROOT/engines/download.sh" elasticsearch
[[ ! -f "$PIDFILE" ]] || { echo "Elasticsearch pidfile already exists: $PIDFILE" >&2; exit 1; }
raise_nofile_limit
export SEARCHBENCH_ROOT="$ROOT"
prepare_rest_config elasticsearch "$HOME_DIR"
export ES_PATH_CONF="$ROOT/run/config/elasticsearch"
# Redirect all daemon fds: an inherited pipe fd otherwise never reaches EOF
# and deadlocks callers like `quick.sh | tail`.
taskset -c "$SERVER_CORES" "$HOME_DIR/bin/elasticsearch" -p "$PIDFILE" -d \
  ${ES_EXTRA_ARGS:-} \
  < /dev/null >> "$ROOT/logs/elasticsearch.out" 2>&1
# Same async-pidfile race as OpenSearch: wait for a non-empty pidfile.
for _ in $(seq 1 30); do [[ -s "$PIDFILE" ]] && break; sleep 1; done
[[ -s "$PIDFILE" ]] || { echo "pidfile never appeared: $PIDFILE" >&2; exit 1; }
PID=$(cat "$PIDFILE")
if ! wait_ready 'http://127.0.0.1:9202/_cluster/health?wait_for_status=green&timeout=1s' "$PID"; then
  tail -100 "$ROOT/logs/elasticsearch/searchbench-elasticsearch.log" 2>/dev/null || true
  kill "$PID" 2>/dev/null || true
  rm -f "$PIDFILE"
  exit 1
fi
apply_rest_query_cache elasticsearch 9202 || { kill "$PID" 2>/dev/null; rm -f "$PIDFILE"; exit 1; }
echo "$PID"
