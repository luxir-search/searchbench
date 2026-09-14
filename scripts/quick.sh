#!/usr/bin/env bash
# Fast iteration checks against EXISTING indexes: warmup + one short rep per
# cell, compact table with delta vs the previous quick run. Engines are
# started if not running and LEFT RUNNING for the next iteration.
#
# Usage: quick.sh [-e engines] [-t tasks] [-d seconds] [-l lane] [-T topology]
#                 [-v variant]...
#   quick.sh                          # luxir, luceneutil-taxonomy full-text cells
#   quick.sh -t FACET_10,FACET_HC     # two tasks (~1 min)
#   quick.sh -e luxir,opensearch -t TOP_10
#   quick.sh -t FACET_10 -v facet_limit=100       # ad-hoc param variation
#   quick.sh -t HIGH_TERM_TOP_10 -v concurrency=1 -v - -v concurrency=16
# Each -v is one variant spec (python/variants.py, comma-separated key=value,
# "-" = baseline); every named cell runs once per variant into its own
# slug-suffixed result file. The delta column compares against the previous
# quick run of the same cell and flags param drift. Server-posture keys (heap)
# need a restart and belong to run-baseline.sh, not here.
#
# Numbers are for ITERATION, not record: medians are trustworthy for big
# effects; JVM cells wobble 10-20% run-to-run; p99 at 5s is noise.
set -uo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
source "$ROOT/scripts/engine-common.sh"
set +e

ENGINES="luxir"
TASKS=$(python3 -c "import sys; sys.path.insert(0, '$ROOT/python'); \
from presets import FULL_TEXT_TASKS; print(','.join(FULL_TEXT_TASKS))")
DURATION=5
LANE=exact
# -T names a declared topology (python/topologies.py) to iterate against: the
# run then serves that topology's own data directory and asserts its complete
# shape around every cell. Default: the force-merged standard index.
TOPOLOGY=""
VARIANTS=()
while getopts "e:t:d:l:T:v:" opt; do
  case $opt in
    e) ENGINES=$OPTARG ;;
    t) TASKS=$OPTARG ;;
    d) DURATION=$OPTARG ;;
    l) LANE=$OPTARG ;;
    T) TOPOLOGY=$OPTARG ;;
    v) VARIANTS+=("$OPTARG") ;;
    *) exit 2 ;;
  esac
done
[[ ${#VARIANTS[@]} -gt 0 ]] || VARIANTS=("-")
for spec in "${VARIANTS[@]}"; do
  server_env=$(python3 "$ROOT/python/variants.py" server-env "$spec") || exit 2
  [[ -z "$server_env" ]] || {
    echo "variant '$spec' changes server posture ($server_env); use run-baseline.sh" >&2
    exit 2
  }
done

CORPUS=${BASELINE_CORPUS:-$ROOT/corpus/corpus-10m-searchbench.ndjson}
[[ -s "$CORPUS" ]] || {
  echo "missing standard corpus/index; run scripts/run-baseline.sh first" >&2
  exit 1
}
ensure_driver || exit 1
CORPUS_DOCS=$(wc -l < "$CORPUS")
CORPUS_SHA256=$(corpus_sha256 "$CORPUS") || exit 1
SEARCHBENCH_DATASET=$(dataset_name "$CORPUS" "${TOPOLOGY:-merged}") || exit 2
if [[ -n "$TOPOLOGY" ]]; then
  CELL_TOPOLOGY_ARGS=(--topology-name "$TOPOLOGY")
else
  CELL_TOPOLOGY_ARGS=(--expected-segments 1)
fi
OUTDIR="$ROOT/results/quick"
mkdir -p "$OUTDIR"

for engine in ${ENGINES//,/ }; do
  port=$(engine_port "$engine") || continue
  ensure_running "$engine" || continue
  verify_count "$engine" "$port" "$CORPUS_DOCS" || continue
  verify_indexed_corpus "$engine" "$SEARCHBENCH_DATASET" "$CORPUS_SHA256" || continue
  pid=$(cat "$(engine_pidfile "$engine")")
  for task in ${TASKS//,/ }; do
    for spec in "${VARIANTS[@]}"; do
      if [[ "$task" == MIX ]] \
          && [[ -n "$(python3 "$ROOT/python/variants.py" param-keys "$spec")" ]]; then
        echo "skipping MIX for '$spec': it takes no parameter overrides" >&2
        continue
      fi
      out="$OUTDIR/$(python3 "$ROOT/python/variants.py" filename "$engine" "$task" "$spec")"
      [[ -f "$out" ]] && mv "$out" "$out.prev"
      "$ROOT/scripts/run-driver.sh" "$engine" "$task" --lane "$LANE" --port "$port" \
        --server-pid "$pid" --duration "$DURATION" --repetitions 1 \
        "${CELL_TOPOLOGY_ARGS[@]}" --variant "$spec" \
        --corpus "$CORPUS" --label quick --output "$out" >/dev/null \
        || echo "FAILED: $engine $task ${spec#-}" >&2
    done
  done
done

python3 - "$OUTDIR" "$ENGINES" "$TASKS" "$DURATION" "${VARIANTS[@]}" <<'EOF'
import json, os, sys
sys.path.insert(0, os.path.join(os.path.dirname(sys.argv[1]), "..", "python"))
from variants import filename, parse_variant, slug
outdir, engines, tasks, duration = sys.argv[1], sys.argv[2].split(","), sys.argv[3].split(","), sys.argv[4]
specs = sys.argv[5:]
show_variant = any(slug(parse_variant(spec)) for spec in specs)
variant_header = f"{'variant':20}" if show_variant else ""
print(f"\n{'task':32}{'engine':14}{variant_header}{'p50ms':>8}{'p90ms':>8}{'qps':>9}{'err':>5}{'vs prev':>9}")
for task in tasks:
    for engine in engines:
        for spec in specs:
            label = slug(parse_variant(spec)) or "baseline"
            variant_text = f"{label:20}" if show_variant else ""
            path = f"{outdir}/{filename(engine, task, parse_variant(spec))}"
            if not os.path.exists(path):
                print(f"{task:32}{engine:14}{variant_text}  (missing)")
                continue
            doc = json.load(open(path))
            a = doc["aggregate"]
            delta = ""
            if os.path.exists(path + ".prev"):
                prev = json.load(open(path + ".prev"))
                if prev.get("param_hash") != doc.get("param_hash"):
                    delta = "params!"
                elif prev["aggregate"]["qps"]:
                    delta = f"{100*(a['qps']-prev['aggregate']['qps'])/prev['aggregate']['qps']:+8.1f}%"
            print(f"{task:32}{engine:14}{variant_text}{a['latency_ms']['p50']:8.2f}{a['latency_ms']['p90']:8.2f}"
                  f"{a['qps']:9.0f}{a['errors']:5}{delta:>9}")
print(f"\nquick mode: 1 warmup(validated) + 1x{duration}s rep - iteration numbers, not record")
EOF
