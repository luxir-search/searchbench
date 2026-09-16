#!/usr/bin/env bash
# Standard-corpus baseline: feed each engine, then run identical task cells.
#
# BASELINE_VARIANTS holds space-separated variant specs (python/variants.py),
# "-" being the baseline; every task cell runs once per variant. Variants that
# share a server posture run inside one server session; each distinct server
# posture is its own start/stop. The index is fed at most once per engine:
# an existing index is reused whenever its recorded corpus hash and layout
# version match. Set BASELINE_REFEED=1 to force a fresh feed (e.g. to time
# indexing itself).
#
# BASELINE_TOPOLOGY names a declared serving layout (python/topologies.py) to
# construct and assert around every cell, instead of the default force merge
# to one segment. Corpus and layout together name the data directory the run
# feeds and serves (python/datasets.py), so topologies coexist on disk and
# each is reusable: the same board can be run over one segment and over
# tiered-45 without either feed displacing the other.
set -uo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
PY="$ROOT/python"
CORPUS=${BASELINE_CORPUS:-$ROOT/corpus/corpus-10m-searchbench.ndjson}
DEFAULT_CORPUS="$ROOT/corpus/corpus-10m-searchbench.ndjson"
QUERIES=${BASELINE_QUERIES:-$ROOT/queries/luceneutil/queries.txt}
LANE=${BASELINE_LANE:-exact}
DURATION=${BASELINE_DURATION:-5}
REPETITIONS=${BASELINE_REPETITIONS:-2}
LABEL=${BASELINE_LABEL:-baseline}
TITLE=${BASELINE_TITLE:-Searchbench baseline: $LANE}
ENGINES=${BASELINE_ENGINES:-luxir}
VARIANTS=${BASELINE_VARIANTS:--}
DEFAULT_TASKS=$(python3 -c "import sys; sys.path.insert(0, '$PY'); \
from presets import FULL_TEXT_TASKS; print(' '.join(FULL_TEXT_TASKS))")
TASKS=(${BASELINE_TASKS:-$DEFAULT_TASKS FACET_10 FACET_1K FACET_HC FACET_DATE FACET_MULTI DEEP_COLLECT})
OUTDIR=${BASELINE_OUTDIR:-$ROOT/results/baseline-$LANE}
if [[ "$CORPUS" == "$DEFAULT_CORPUS" ]]; then
  "$ROOT/scripts/prepare-corpus.sh" standard
fi
[[ -s "$CORPUS" ]] || { echo "missing corpus: $CORPUS" >&2; exit 1; }

source "$ROOT/scripts/engine-common.sh"
ensure_driver || exit 1
TOPOLOGY=${BASELINE_TOPOLOGY:-}
LAYOUT=${TOPOLOGY:-merged}
DATASET=$(dataset_name "$CORPUS" "$LAYOUT") || exit 1
if [[ -n "$TOPOLOGY" ]]; then
  CELL_TOPOLOGY_ARGS=(--topology-name "$TOPOLOGY")
else
  CELL_TOPOLOGY_ARGS=(--expected-segments 1)
fi
mapfile -t POSTURES < <(python3 "$PY/variants.py" postures $VARIANTS)
[[ ${#POSTURES[@]} -gt 0 ]] || { echo "invalid BASELINE_VARIANTS: $VARIANTS" >&2; exit 1; }
if [[ ${BASELINE_APPEND_RESULTS:-0} != 1 ]]; then
  archive_existing "$OUTDIR"
fi
mkdir -p "$OUTDIR"
CORPUS_DOCS=$(wc -l < "$CORPUS")
CORPUS_SHA256=$(corpus_sha256 "$CORPUS") || exit 1
set +e

run_cells() {
  local engine=$1 pid=$2 port=$3 specs=$4 task spec out
  for task in "${TASKS[@]}"; do
    for spec in $specs; do
      out="$OUTDIR/$(python3 "$PY/variants.py" filename "$engine" "$task" "$spec")"
      echo "=== $engine $LANE $task ${spec#-}"
      "$ROOT/scripts/run-driver.sh" "$engine" "$task" --lane "$LANE" --port "$port" \
        --server-pid "$pid" --duration "$DURATION" --repetitions "$REPETITIONS" \
        "${CELL_TOPOLOGY_ARGS[@]}" --variant "$spec" \
        --corpus "$CORPUS" --queries "$QUERIES" --label "$LABEL" \
        --output "$out" \
        || echo "FAILED: $engine $task ${spec#-}" >&2
    done
  done
}

run_engine() {
  local engine=$1 port pid posture posture_env posture_specs artifact
  port=$(engine_port "$engine") || return 1
  [[ ! -f "$(engine_pidfile "$engine")" ]] || "$ROOT/scripts/stop-$engine.sh" || return 1
  DATASET_REFEED=${BASELINE_REFEED:-0} \
    ensure_dataset "$engine" "$CORPUS" "$LAYOUT" "$CORPUS_SHA256" "$CORPUS_DOCS" \
    || return 1
  for artifact in feed.json topology.json; do
    [[ -s "$(dataset_artifact "$engine" "$DATASET" "$artifact")" ]] \
      && cp "$(dataset_artifact "$engine" "$DATASET" "$artifact")" \
            "$OUTDIR/$engine-${artifact%.json}.json"
  done
  for posture in "${POSTURES[@]}"; do
    posture_env=${posture%%|*}
    posture_specs=${posture#*|}
    env $posture_env SEARCHBENCH_DATASET="$DATASET" \
      "$ROOT/scripts/start-$engine.sh" || return 1
    pid=$(cat "$(engine_pidfile "$engine")")
    verify_count "$engine" "$port" "$CORPUS_DOCS" \
      || { "$ROOT/scripts/stop-$engine.sh"; return 1; }
    # Assert the declared shape once per session rather than discovering a
    # collapsed topology one failed cell at a time: an engine that merges the
    # constructed tiers when it reopens the index fails every cell it serves.
    if [[ -n "$TOPOLOGY" ]]; then
      python3 "$PY/verify_topology.py" "$engine" "$TOPOLOGY" --port "$port" \
        > "$OUTDIR/$engine-served-topology.json" \
        || { "$ROOT/scripts/stop-$engine.sh"; return 1; }
    fi
    run_cells "$engine" "$pid" "$port" "$posture_specs"
    record_health_control "$engine" "$pid" "$port" "$OUTDIR" "${posture_specs%% *}" \
      || echo "FAILED: $engine health control" >&2
    "$ROOT/scripts/stop-$engine.sh"
  done
}

for engine in $ENGINES; do
  case "$engine" in
    luxir|opensearch|elasticsearch)
      run_engine "$engine" || echo "ENGINE FAILED: $engine" >&2 ;;
    *) echo "unknown engine: $engine" >&2 ;;
  esac
done
python3 "$PY/report.py" "$OUTDIR" --title "$TITLE" \
  --output "$OUTDIR/REPORT.md"
echo "baseline campaign complete: $OUTDIR"
