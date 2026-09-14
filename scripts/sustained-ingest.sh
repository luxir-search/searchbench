#!/usr/bin/env bash
# Sustained-ingest lane: 2 back-to-back feeds of the 10M corpus (20M adds) in
# one server session at a small budget, merges on.  Per-pass stream time and
# segment counts show whether merging keeps up DURING ingest; commit tail last.
set -euo pipefail
export SEARCHBENCH_DATA_ROOT=${SEARCHBENCH_DATA_ROOT:-/tmp/searchbench-ingest}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
source "$ROOT/scripts/engine-common.sh"
CORPUS=${CORPUS:-$ROOT/corpus/corpus-10m-searchbench.ndjson}
OUT=${OUT:-/tmp/sustained-ingest}; STREAMS=${STREAMS:-8}
LABEL=${LABEL:-run}; PASSES=${PASSES:-2}
INV=${INV:-1024}; BUDGET=${BUDGET:-2048}
mkdir -p "$OUT"
cputime() { awk '{print ($14+$15)/'"$(getconf CLK_TCK)"'}' /proc/$1/stat; }
segs() { curl -s "http://127.0.0.1:9400/collections/searchbench/_stats?segments=true" \
  | python3 -c 'import json,sys;d=json.load(sys.stdin);print(len(d["collections"][0]["shards"][0]["index"]["segments"]))' 2>/dev/null || echo "?"; }

cell="$LABEL-inv$INV-b$BUDGET"
export SEARCHBENCH_DATASET="sust-$cell"
dir=$(engine_data_dir luxir); rm -rf "$dir"
"$ROOT/scripts/stop-luxir.sh" >/dev/null 2>&1 || true
export LUXIR_EXTRA_ARGS="--indexing.max-inverter-ram-mb=$INV --indexing.max-ram-mb=$BUDGET --indexing.merge-factor=10"
pid=$("$ROOT/scripts/start-luxir.sh")
( while [[ -d /proc/$pid ]]; do awk '/^VmRSS/{print $2}' /proc/$pid/status; sleep 0.5; done > "$OUT/$cell.rss" 2>/dev/null ) &
sampler=$!
declare -a PT PC PS
for p in $(seq 1 $PASSES); do
  c0=$(cputime $pid); t0=$(date +%s.%N)
  python3 "$ROOT/python/feed_luxir.py" "$CORPUS" --streams "$STREAMS" --no-commit > "$OUT/$cell.pass$p.json"
  c1=$(cputime $pid); t1=$(date +%s.%N)
  PT[$p]=$(echo "$t1 $t0" | awk '{printf "%.1f", $1-$2}')
  PC[$p]=$(echo "$c1 $c0 $t1 $t0" | awk '{printf "%.1f", ($1-$2)/($3-$4)}')
  PS[$p]=$(segs)
done
c0=$(cputime $pid); t0=$(date +%s.%N)
curl -s -X POST "http://127.0.0.1:9400/collections/searchbench/_update" \
  -H 'Content-Type: application/x-ndjson' \
  --data-binary '{"_end_":{"commit":{"wait_for_merges":true}}}' > "$OUT/$cell.commit"
c1=$(cputime $pid); t1=$(date +%s.%N)
CT=$(echo "$t1 $t0" | awk '{printf "%.1f", $1-$2}')
CC=$(echo "$c1 $c0 $t1 $t0" | awk '{printf "%.1f", ($1-$2)/($3-$4)}')
FS=$(segs)
kill $sampler 2>/dev/null || true
"$ROOT/scripts/stop-luxir.sh" >/dev/null; rm -rf "$dir"
peak=$(python3 -c "import pathlib;v=[int(x) for x in pathlib.Path('$OUT/$cell.rss').read_text().split()];print(f'{max(v)/1048576:.1f}' if v else 'nan')")
line="$cell "
for p in $(seq 1 $PASSES); do line+=" pass$p ${PT[$p]}s (${PC[$p]}c) segs=${PS[$p]} |"; done
line+=" commit ${CT}s (${CC}c) final=$FS peakRSS=${peak}G"
echo "$line"
