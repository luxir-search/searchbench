#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/engine-common.sh"
VERSION=$(python3 -c 'import json; print(json.load(open("'"$ROOT"'/engines/versions.json"))["opensearch"]["version"])')
HOME_DIR="$ROOT/engines/opensearch-$VERSION"
PIDFILE="$ROOT/run/opensearch.pid"
SEARCHBENCH_DATA=$(engine_data_dir opensearch)
export SEARCHBENCH_DATA
mkdir -p "$ROOT/run" "$SEARCHBENCH_DATA" "$ROOT/logs/opensearch"
echo "opensearch dataset: $SEARCHBENCH_DATASET" >&2
[[ -x "$HOME_DIR/bin/opensearch" ]] || "$ROOT/engines/download.sh" opensearch
[[ ! -f "$PIDFILE" ]] || { echo "OpenSearch pidfile already exists: $PIDFILE" >&2; exit 1; }
raise_nofile_limit
export SEARCHBENCH_ROOT="$ROOT"
prepare_rest_config opensearch "$HOME_DIR"
export OPENSEARCH_PATH_CONF="$ROOT/run/config/opensearch"
# Redirect all daemon fds: an inherited pipe fd otherwise never reaches EOF
# and deadlocks callers like `quick.sh | tail`.
taskset -c "$SERVER_CORES" "$HOME_DIR/bin/opensearch" -p "$PIDFILE" -d \
  ${OPENSEARCH_EXTRA_ARGS:-} \
  < /dev/null >> "$ROOT/logs/opensearch.out" 2>&1
# -d daemonizes and writes the pidfile asynchronously; reading it immediately
# races an empty file (empty PID -> instant bogus "exited before ready" and an
# orphaned daemon).  Wait for a non-empty pidfile first.
for _ in $(seq 1 30); do [[ -s "$PIDFILE" ]] && break; sleep 1; done
[[ -s "$PIDFILE" ]] || { echo "pidfile never appeared: $PIDFILE" >&2; exit 1; }
PID=$(cat "$PIDFILE")
if ! wait_ready 'http://127.0.0.1:9201/_cluster/health?wait_for_status=green&timeout=1s' "$PID"; then
  tail -100 "$ROOT/logs/opensearch/searchbench-opensearch.log" 2>/dev/null || true
  kill "$PID" 2>/dev/null || true
  rm -f "$PIDFILE"
  exit 1
fi
apply_rest_query_cache opensearch 9201 || { kill "$PID" 2>/dev/null; rm -f "$PIDFILE"; exit 1; }
echo "$PID"
