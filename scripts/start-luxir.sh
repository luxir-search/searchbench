#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/engine-common.sh"
LUXIR_REPO=${LUXIR_REPO:-$ROOT/../luxir}
BIN=${LUXIR_BIN:-$LUXIR_REPO/build/gcc-release/bin/luxir}
PIDFILE="$ROOT/run/luxir.pid"
DATA_DIR=$(engine_data_dir luxir)
mkdir -p "$ROOT/run" "$DATA_DIR" "$ROOT/logs"
[[ -x "$BIN" ]] || { echo "missing release binary: $BIN" >&2; exit 1; }
echo "luxir dataset: $SEARCHBENCH_DATASET" >&2
[[ ! -f "$PIDFILE" ]] || { echo "Luxir pidfile already exists: $PIDFILE" >&2; exit 1; }
raise_nofile_limit
# Same posture variable the REST start scripts consume (engine-common.sh):
# off = no query cache, so repeated identical queries measure execution.
CACHE_ARGS=""
[[ "${SEARCHBENCH_QUERY_CACHE:-}" == off ]] && CACHE_ARGS="--query-cache-bytes=0"
taskset -c "$SERVER_CORES" "$BIN" \
  --log-level=warn \
  --server.http.port=9400 \
  --server.grpc.port=9401 \
  --store.backend=fs \
  --store.data-dir="$DATA_DIR" \
  --store.checked-dir.sync=off \
  $CACHE_ARGS ${LUXIR_EXTRA_ARGS:-} \
  >"$ROOT/logs/luxir.log" 2>&1 &
PID=$!
echo "$PID" > "$PIDFILE"
if ! wait_ready 'http://127.0.0.1:9400/health' "$PID"; then
  tail -100 "$ROOT/logs/luxir.log" 2>/dev/null || true
  kill "$PID" 2>/dev/null || true
  rm -f "$PIDFILE"
  exit 1
fi
echo "$PID"
