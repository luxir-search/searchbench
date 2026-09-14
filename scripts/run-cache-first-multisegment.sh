#!/usr/bin/env bash
# Deterministic tiered topology cache-first gate: changing-query / stable-filter
# cells plus representative COUNT controls. The topology has its own data
# directory (python/datasets.py), so it neither displaces the standard index
# nor is rebuilt by a run that finds it already fed.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
PY="$ROOT/python"
source "$ROOT/scripts/engine-common.sh"

TOPOLOGY=tiered-45
CORPUS="$ROOT/corpus/corpus-10m-searchbench.ndjson"
QUERIES="$ROOT/queries/luceneutil/queries.txt"
ENGINES=luxir
DURATION=5
REPETITIONS=1
QUERY_VARIANTS=
TASK_SPEC=
OUTDIR=

DEFAULT_TASKS=(
  FILTERED_RANGE_90_AND_HIGH_MED_TOP_10
  FILTERED_RANGE_90_AND_HIGH_MED_COUNT
  FILTERED_RANGE_1_AND_HIGH_MED_TOP_10
  FILTERED_RANGE_1_AND_HIGH_MED_COUNT
  HIGH_TERM_COUNT
  AND_HIGH_MED_COUNT
  HIGH_SLOPPY_PHRASE_COUNT
)

usage() {
  echo "usage: $0 [-T tiered-45|tiered-5]" \
       "[-e luxir,opensearch,elasticsearch] [-t task,task]" \
       "[-d seconds] [-r 1] [-v changing-query-count] [-o results-dir]" >&2
  exit 2
}

while getopts "T:e:t:d:r:v:o:" opt; do
  case $opt in
    T) TOPOLOGY=$OPTARG ;;
    e) ENGINES=$OPTARG ;;
    t) TASK_SPEC=$OPTARG ;;
    d) DURATION=$OPTARG ;;
    r) REPETITIONS=$OPTARG ;;
    v) QUERY_VARIANTS=$OPTARG ;;
    o) OUTDIR=$OPTARG ;;
    *) usage ;;
  esac
done
shift $((OPTIND - 1))
[[ $# == 0 ]] || usage
case "$TOPOLOGY" in
  tiered-45|tiered-5) ;;
  *) echo "unsupported campaign topology: $TOPOLOGY" >&2; exit 2 ;;
esac
OUTDIR=${OUTDIR:-$ROOT/results/cache-first-$TOPOLOGY}
DATASET=$(dataset_name "$CORPUS" "$TOPOLOGY") || exit 2
SEARCHBENCH_DATASET=$DATASET
[[ "$REPETITIONS" == 1 ]] || {
  echo "the changing-query lane requires exactly one repetition" >&2
  exit 2
}
[[ -z "$QUERY_VARIANTS" || "$QUERY_VARIANTS" =~ ^[1-9][0-9]*$ ]] || {
  echo "changing-query-count must be a positive integer" >&2
  exit 2
}

IFS=',' read -r -a ENGINE_LIST <<< "$ENGINES"
for engine in "${ENGINE_LIST[@]}"; do
  case "$engine" in
    luxir|opensearch|elasticsearch) ;;
    *) echo "unknown engine: $engine" >&2; exit 2 ;;
  esac
done
if [[ -n "$TASK_SPEC" ]]; then
  IFS=',' read -r -a TASKS <<< "$TASK_SPEC"
else
  TASKS=("${DEFAULT_TASKS[@]}")
fi
(( ${#TASKS[@]} > 0 )) || usage

ACTIVE_ENGINE=

stop_selected_engine() {
  local engine=$1 pidfile
  pidfile=$(engine_pidfile "$engine")
  if [[ -f "$pidfile" ]]; then
    "$ROOT/scripts/stop-$engine.sh"
  fi
}

on_exit() {
  local status=$?
  trap - EXIT INT TERM
  set +e
  # Leaving a measured server up after a failed or interrupted run would hand
  # the next campaign a warm process it never started.
  [[ -z "$ACTIVE_ENGINE" ]] || stop_selected_engine "$ACTIVE_ENGINE"
  exit "$status"
}
trap on_exit EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

"$ROOT/scripts/prepare-corpus.sh" standard
[[ -s "$CORPUS" && -s "$CORPUS.sha256" ]] || {
  echo "missing prepared standard corpus or digest" >&2
  exit 1
}
EXPECTED_CORPUS_SHA256=$(awk 'NR == 1 {print $1}' "$CORPUS.sha256")
ACTUAL_CORPUS_SHA256=$(sha256sum "$CORPUS" | awk '{print $1}')
[[ "$ACTUAL_CORPUS_SHA256" == "$EXPECTED_CORPUS_SHA256" ]] || {
  echo "standard corpus SHA-256 mismatch" >&2
  exit 1
}
CORPUS_SHA256=$ACTUAL_CORPUS_SHA256
CORPUS_DOCS=$(wc -l < "$CORPUS")
read -r TOPOLOGY_DOCS EXPECTED_SEGMENTS <<< "$(PYTHONPATH="$PY" python3 -c \
  'import sys; from topologies import resolve_topology; t=resolve_topology(sys.argv[1]); print(t.documents, t.segment_count)' \
  "$TOPOLOGY")"
[[ "$CORPUS_DOCS" == "$TOPOLOGY_DOCS" ]] || {
  echo "$TOPOLOGY requires $TOPOLOGY_DOCS documents, corpus has $CORPUS_DOCS" >&2
  exit 1
}

ensure_driver
archive_existing "$OUTDIR"
mkdir -p "$OUTDIR"

run_cell() {
  local engine=$1 port=$2 pid=$3 task=$4 task_variants
  local extra=()
  echo "=== $engine $TOPOLOGY $task"
  if [[ "$task" == FILTERED_* ]]; then
    task_variants=$QUERY_VARIANTS
    if [[ -z "$task_variants" ]]; then
      task_variants=$(PYTHONPATH="$PY" python3 -c \
        "from presets import resolve; print(resolve('$task', 'exact')['query_variants'])")
      extra=(--max-requests "$task_variants")
    else
      extra=(--set "query_variants=$task_variants" \
             --max-requests "$task_variants")
    fi
  fi
  "$ROOT/scripts/run-driver.sh" "$engine" "$task" --lane exact --port "$port" \
    --server-pid "$pid" --duration "$DURATION" --repetitions "$REPETITIONS" \
    --warmup-seconds 0 --expected-segments "$EXPECTED_SEGMENTS" \
    --topology-name "$TOPOLOGY" --corpus "$CORPUS" --queries "$QUERIES" \
    --label "cache-first-$TOPOLOGY" --output "$OUTDIR/$engine-$task.json" \
    "${extra[@]}"
  if [[ "$task" == FILTERED_* ]]; then
    python3 - "$OUTDIR/$engine-$task.json" "$task_variants" "$DURATION" <<'PY'
import json
import sys

result = json.load(open(sys.argv[1], encoding="utf-8"))
requests = result["aggregate"]["requests"]
elapsed = result["aggregate"]["elapsed_s"]
limit = int(sys.argv[2])
duration = float(sys.argv[3])
if requests >= limit:
    raise SystemExit(
        f"changing-query pool exhausted after {requests} requests (limit {limit})")
if duration and elapsed < duration * 0.99:
    raise SystemExit(
        f"measured replay ended after {elapsed:.3f}s, expected {duration:.3f}s")
PY
  fi
}

run_engine() {
  local engine=$1 port pid task artifact
  port=$(engine_port "$engine")
  stop_selected_engine "$engine"
  LUXIR_EXTRA_ARGS=${MULTISEG_LUXIR_EXTRA_ARGS:-} \
    ensure_dataset "$engine" "$CORPUS" "$TOPOLOGY" "$CORPUS_SHA256" "$CORPUS_DOCS"
  for artifact in feed.json topology.json; do
    cp "$(dataset_artifact "$engine" "$DATASET" "$artifact")" \
       "$OUTDIR/$engine-${artifact%.json}.json"
  done

  ACTIVE_ENGINE=$engine
  "$ROOT/scripts/start-$engine.sh" >/dev/null
  pid=$(cat "$(engine_pidfile "$engine")")
  verify_count "$engine" "$port" "$CORPUS_DOCS"

  for task in "${TASKS[@]}"; do
    run_cell "$engine" "$port" "$pid" "$task"
  done
  stop_selected_engine "$engine"
  ACTIVE_ENGINE=
}

for engine in "${ENGINE_LIST[@]}"; do
  run_engine "$engine"
done

python3 "$PY/report.py" "$OUTDIR" \
  --title "Cache-first stable-filter gate: $TOPOLOGY ($ENGINES)" \
  --output "$OUTDIR/REPORT.md"
echo "cache-first multi-segment campaign complete: $OUTDIR"
