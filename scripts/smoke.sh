#!/usr/bin/env bash
set -uo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
PY="$ROOT/python"
source "$ROOT/scripts/engine-common.sh"
DEFAULT_CORPUS="$ROOT/corpus/corpus-100k-searchbench.ndjson"
CORPUS=${SMOKE_CORPUS:-$DEFAULT_CORPUS}
ENGINES=${SMOKE_ENGINES:-luxir}
DEFAULT_TASKS=(HIGH_TERM_TOP_10 MED_TERM_TOP_10 LOW_TERM_TOP_10
               AND_HIGH_HIGH_TOP_10 AND_HIGH_MED_TOP_10 AND_HIGH_LOW_TOP_10
               OR_HIGH_HIGH_TOP_10 OR_HIGH_MED_TOP_10 OR_HIGH_LOW_TOP_10
               HIGH_PHRASE_TOP_10 MED_PHRASE_TOP_10 LOW_PHRASE_TOP_10
               HIGH_SLOPPY_PHRASE_TOP_10 MED_SLOPPY_PHRASE_TOP_10
               LOW_SLOPPY_PHRASE_TOP_10 COUNT FACET_10 FACET_1K FACET_HC
               FACET_DATE FACET_MULTI DEEP_COLLECT)
if [[ -n ${SMOKE_TASKS:-} ]]; then
  read -ra TASKS <<< "${SMOKE_TASKS//,/ }"
else
  TASKS=("${DEFAULT_TASKS[@]}")
fi
LANES=${SMOKE_LANES:-exact,skip}
REPETITIONS=${SMOKE_REPETITIONS:-1}
MAX_QUERIES=${SMOKE_MAX_QUERIES:-3}
CONCURRENCY=${SMOKE_CONCURRENCY:-2}
mkdir -p "$ROOT/corpus"
if [[ "$CORPUS" == "$DEFAULT_CORPUS" ]]; then
  "$ROOT/scripts/prepare-corpus.sh" smoke
fi
[[ -s "$CORPUS" ]] || { echo "missing smoke corpus: $CORPUS" >&2; exit 1; }
ensure_driver || exit 1
# The acceptance corpus has its own data directory: a smoke run no longer
# displaces the campaign index it shares a collection name with.
SEARCHBENCH_DATASET=$(dataset_name "$CORPUS" as-fed) || exit 1
archive_existing "$ROOT/results/smoke-exact"
archive_existing "$ROOT/results/smoke-skip"
mkdir -p "$ROOT/results/smoke-exact" "$ROOT/results/smoke-skip"

record_engine_failure() {
  local engine=$1 message=$2 lane task
  IFS=, read -ra lane_list <<< "$LANES"
  for lane in "${lane_list[@]}"; do
    for task in "${TASKS[@]}"; do
      python3 "$PY/failure_result.py" "$engine" "$task" "$lane" "$message" \
        "$ROOT/results/smoke-$lane/$engine-$task.json" --corpus "$CORPUS"
    done
  done
}

run_tasks() {
  local engine=$1 pid=$2 port=$3 lane task
  IFS=, read -ra lane_list <<< "$LANES"
  for lane in "${lane_list[@]}"; do
    for task in "${TASKS[@]}"; do
      echo "$engine $lane $task"
      "$ROOT/scripts/run-driver.sh" "$engine" "$task" --lane "$lane" --port "$port" \
        --server-pid "$pid" --repetitions "$REPETITIONS" --max-queries "$MAX_QUERIES" \
        --concurrency "$CONCURRENCY" --label smoke \
        --output "$ROOT/results/smoke-$lane/$engine-$task.json" \
        || python3 "$PY/failure_result.py" "$engine" "$task" "$lane" \
          "driver/task failure" "$ROOT/results/smoke-$lane/$engine-$task.json" \
          --corpus "$CORPUS"
    done
  done
}

run_luxir() {
  [[ ! -f "$ROOT/run/luxir.pid" ]] || "$ROOT/scripts/stop-luxir.sh" || return 1
  archive_engine_data luxir
  "$ROOT/scripts/start-luxir.sh" || {
    record_engine_failure luxir "server failed to start; see logs/luxir.log"
    return 1
  }
  local pid
  pid=$(cat "$ROOT/run/luxir.pid")
  python3 "$PY/feed_luxir.py" "$CORPUS" --collection searchbench \
    > "$ROOT/results/luxir-feed.json" || {
      record_engine_failure luxir "feed failed; see results/luxir-feed.json"
      "$ROOT/scripts/stop-luxir.sh"
      return 1
    }
  run_tasks luxir "$pid" 9400
  "$ROOT/scripts/stop-luxir.sh"
}

run_rest() {
  local engine=$1 port binary version home pid
  if [[ "$engine" == opensearch ]]; then
    port=9201; binary=opensearch
  else
    port=9202; binary=elasticsearch
  fi
  version=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))[sys.argv[2]]["version"])' \
    "$ROOT/engines/versions.json" "$engine")
  home="$ROOT/engines/$engine-$version"
  [[ -x "$home/bin/$binary" ]] || "$ROOT/engines/download.sh" "$engine" || {
    record_engine_failure "$engine" "official artifact download failed"
    return 1
  }
  [[ ! -f "$ROOT/run/$engine.pid" ]] || "$ROOT/scripts/stop-$engine.sh" || return 1
  archive_engine_data "$engine"
  "$ROOT/scripts/start-$engine.sh" || {
    record_engine_failure "$engine" "server failed to start; see logs"
    return 1
  }
  pid=$(cat "$ROOT/run/$engine.pid")
  python3 "$PY/feed_rest.py" "$engine" "$CORPUS" --port "$port" --index searchbench \
    > "$ROOT/results/$engine-feed.json" || {
      record_engine_failure "$engine" "feed failed; see results/$engine-feed.json"
      "$ROOT/scripts/stop-$engine.sh"
      return 1
    }
  run_tasks "$engine" "$pid" "$port"
  "$ROOT/scripts/stop-$engine.sh"
}

for engine in ${ENGINES//,/ }; do
  case "$engine" in
    luxir) run_luxir || true ;;
    opensearch|elasticsearch) run_rest "$engine" || true ;;
    *) echo "unknown engine: $engine" >&2; exit 2 ;;
  esac
done
python3 "$PY/report.py" "$ROOT/results/smoke-exact" \
  --title "Searchbench smoke report" --output "$ROOT/results/smoke-exact/REPORT.md"
