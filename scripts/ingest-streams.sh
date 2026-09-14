#!/usr/bin/env bash
# Ingest-parallelism sweep: feed the same corpus once per client-concurrency
# level and report documents/second. Each cell gets a fresh server and an empty
# data directory, so nothing carries over between them.
#
# The dimension is whatever the engine's feeder calls a channel: concurrent
# /update streams for luxir, concurrent _bulk connections for the REST engines.
#
# Indexes land under SEARCHBENCH_DATA_ROOT (default /tmp/searchbench-ingest):
# every cell is refed from scratch and discarded, so there is nothing to keep
# and no reason to write it to the SSD.
#
# usage: INGEST_ENGINE=elasticsearch INGEST_CORPUS=/tmp/corpus.ndjson \
#          scripts/ingest-streams.sh [streams...]
set -euo pipefail
export SEARCHBENCH_DATA_ROOT=${SEARCHBENCH_DATA_ROOT:-/tmp/searchbench-ingest}
source "$(dirname "$0")/engine-common.sh"

ENGINE=${INGEST_ENGINE:-luxir}
CORPUS=${INGEST_CORPUS:-$STANDARD_CORPUS}
OUT=${INGEST_OUT:-$ROOT/results/ingest-streams-$ENGINE}
STREAMS=("$@")
[[ ${#STREAMS[@]} -gt 0 ]] || STREAMS=(1 2 4 8 16 32)

[[ -f "$CORPUS" ]] || { echo "missing corpus: $CORPUS" >&2; exit 1; }
PORT=$(engine_port "$ENGINE")
mkdir -p "$OUT"

for streams in "${STREAMS[@]}"; do
  export SEARCHBENCH_DATASET="ingest-streams-$streams"
  dir=$(engine_data_dir "$ENGINE")
  rm -rf "$dir"
  "$ROOT/scripts/stop-$ENGINE.sh" >/dev/null 2>&1 || true
  "$ROOT/scripts/start-$ENGINE.sh" >/dev/null
  status=0
  if [[ "$ENGINE" == luxir ]]; then
    python3 "$ROOT/python/feed_luxir.py" "$CORPUS" --streams "$streams" \
      > "$OUT/streams-$streams.json" || status=$?
  else
    python3 "$ROOT/python/feed_rest.py" "$ENGINE" "$CORPUS" --port "$PORT" \
      --clients "$streams" ${FEED_EXTRA_ARGS:-} \
      > "$OUT/streams-$streams.json" || status=$?
  fi
  du -sb "$dir" > "$OUT/streams-$streams.bytes" 2>/dev/null || true
  "$ROOT/scripts/stop-$ENGINE.sh" >/dev/null
  rm -rf "$dir"
  [[ $status == 0 ]] || { echo "feed failed at streams=$streams" >&2; exit "$status"; }
  python3 - "$OUT/streams-$streams.json" "$streams" <<'PY'
import json, sys
feed = json.load(open(sys.argv[1]))
found = feed.get("count", (feed.get("count_response") or {}).get("found"))
print(f"streams={sys.argv[2]:>3}  {feed['elapsed_s']:8.1f}s  "
      f"{feed['docs_per_s']:>10,.0f} docs/s  {found:>10,} docs")
PY
done
