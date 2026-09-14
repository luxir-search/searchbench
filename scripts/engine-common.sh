#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
SERVER_CORES=${SERVER_CORES:-$(python3 "$ROOT/python/cpu_layout.py" server)}
START_TIMEOUT=${START_TIMEOUT:-420}
# schema.py owns the physical field/ingest compatibility boundary used by both
# results and reusable on-disk index markers.
index_layout_version() {
  PYTHONPATH="$ROOT/python" python3 -c \
    "from schema import index_layout_version; print(index_layout_version('$1'))"
}

raise_nofile_limit() {
  local requested=${SEARCHBENCH_NOFILE:-65536}
  [[ "$requested" =~ ^[1-9][0-9]*$ ]] || {
    echo "SEARCHBENCH_NOFILE must be a positive integer: $requested" >&2
    return 1
  }
  (( requested >= 65536 )) || {
    echo "SEARCHBENCH_NOFILE must be at least 65536: $requested" >&2
    return 1
  }
  ulimit -Sn "$requested" || {
    echo "cannot raise the open-file soft limit to $requested" >&2
    echo "hard limit: $(ulimit -Hn)" >&2
    return 1
  }
}

ensure_driver() {
  local binary=${SEARCHBENCH_DRIVER:-$ROOT/driver/build/bench_replay}
  [[ -x "$binary" ]] || "$ROOT/scripts/build-driver.sh"
}

corpus_sha256() {
  local corpus=$1 sidecar="$1.sha256" digest computed=0
  if [[ -s "$sidecar" && "$sidecar" -nt "$corpus" ]]; then
    read -r digest _ < "$sidecar"
  else
    digest=$(sha256sum "$corpus" | awk '{print $1}')
    computed=1
  fi
  [[ "$digest" =~ ^[0-9a-f]{64}$ ]] || {
    echo "invalid corpus SHA-256 for $corpus" >&2
    return 1
  }
  if [[ "$computed" == 1 ]]; then
    printf '%s\n' "$digest" > "$sidecar"
  fi
  echo "$digest"
}

# One data directory per fed index shape (python/datasets.py): an engine opens
# every index in its data directory, so the standard campaign, a constructed
# multi-segment topology, the smoke corpus and the facet corpus each get their
# own directory instead of displacing one another inside a shared one. All of
# a dataset's state - the index, its corpus identity, the feed record, and the
# verified topology - is named after the dataset and lives beside it.
dataset_name() {
  PYTHONPATH="$ROOT/python" python3 "$ROOT/python/datasets.py" "$@"
}

STANDARD_CORPUS="$ROOT/corpus/corpus-10m-searchbench.ndjson"
# Exported so a campaign that selects a dataset keeps it across the start
# scripts it invokes; unset means the standard force-merged 10M campaign.
export SEARCHBENCH_DATASET=${SEARCHBENCH_DATASET:-$(dataset_name "$STANDARD_CORPUS")}

# Root of every engine's on-disk index state. Overridable so a throwaway
# experiment (ingest sweeps, anything refed per run) can land on tmpfs instead
# of writing tens of GiB to the SSD; the default keeps campaign datasets on
# durable storage, where reuse across runs is the point.
export SEARCHBENCH_DATA_ROOT=${SEARCHBENCH_DATA_ROOT:-$ROOT/data}

engine_data_dir() {
  echo "$SEARCHBENCH_DATA_ROOT/$1/${2:-$SEARCHBENCH_DATASET}"
}

dataset_artifact() {
  echo "$(engine_data_dir "$1" "${2:-}").$3"
}

index_corpus_marker() {
  dataset_artifact "$1" "${2:-}" identity
}

record_indexed_corpus() {
  local marker
  marker=$(index_corpus_marker "$1" "$2")
  mkdir -p "$(dirname "$marker")"
  printf '%s %s\n' "$3" "$(index_layout_version "$1")" > "$marker"
}

verify_indexed_corpus() {
  local marker actual recorded_layout
  marker=$(index_corpus_marker "$1" "$2")
  [[ -s "$marker" ]] || {
    echo "$1/$2 has no recorded corpus identity" >&2
    return 1
  }
  read -r actual recorded_layout _ < "$marker"
  [[ "$actual" == "$3" ]] || {
    echo "$1/$2 was built from a different corpus" >&2
    return 1
  }
  local expected_layout
  expected_layout=$(index_layout_version "$1")
  [[ "$recorded_layout" == "$expected_layout" ]] || {
    echo "$1/$2 uses stale index layout ${recorded_layout:-unknown}; expected $expected_layout" >&2
    return 1
  }
}

archive_engine_data() {
  local engine=$1 dataset=${2:-$SEARCHBENCH_DATASET} artifact
  archive_existing "$(engine_data_dir "$engine" "$dataset")"
  for artifact in identity feed.json topology.json; do
    archive_existing "$(dataset_artifact "$engine" "$dataset" "$artifact")"
  done
}

# Ingest posture for constructing a declared topology: one serialized stream
# owns each largest-first range, and the high merge factor keeps background
# policy merges out of the explicit range boundaries. The inverter RAM cap is
# luxir's own ceiling (an inverter pool addresses at most 4 GiB), so a range
# larger than that still auto-flushes into pieces the range-boundary commit
# force-merges back together. Feed-only - a measured session never runs with
# these.
luxir_ingest_args() {
  echo "--indexing.max-inverter-ram-mb=3814 --indexing.max-inverter-docs=5000000" \
       "--indexing.merge-factor=64 --indexing.max-ram-mb=0"
}

# Feed <engine>'s <dataset> unless its recorded identity already matches the
# corpus. Construction runs in its own server session, so ingest posture never
# leaks into a measured one and a reused dataset is served by exactly the
# posture a freshly fed one is. DATASET_REFEED=1 rebuilds regardless.
#
# usage: ensure_dataset <engine> <corpus> <layout> <corpus-sha256> <documents>
ensure_dataset() {
  local engine=$1 corpus=$2 layout=$3 sha=$4 documents=$5
  local collection=${DATASET_COLLECTION:-searchbench}
  local dataset port dir status=0
  dataset=$(dataset_name "$corpus" "$layout") || return 1
  port=$(engine_port "$engine") || return 1
  dir=$(engine_data_dir "$engine" "$dataset")
  if [[ ${DATASET_REFEED:-0} != 1 && -n "$(ls -A "$dir" 2>/dev/null)" ]] \
      && verify_indexed_corpus "$engine" "$dataset" "$sha"; then
    echo "reusing $engine dataset $dataset"
    return 0
  fi
  [[ ! -f "$(engine_pidfile "$engine")" ]] || "$ROOT/scripts/stop-$engine.sh" || return 1
  archive_engine_data "$engine" "$dataset"
  local -a start_env=(SEARCHBENCH_DATASET="$dataset")
  case "$layout" in
    merged|as-fed) ;;
    *) [[ "$engine" != luxir ]] || start_env+=(
         LUXIR_EXTRA_ARGS="$(luxir_ingest_args) ${LUXIR_EXTRA_ARGS:-}") ;;
  esac
  env "${start_env[@]}" "$ROOT/scripts/start-$engine.sh" >/dev/null || return 1
  feed_dataset "$engine" "$port" "$corpus" "$layout" "$collection" "$dataset" \
    || status=1
  if [[ $status == 0 ]]; then
    verify_count "$engine" "$port" "$documents" "$collection" || status=1
  fi
  if [[ $status == 0 ]]; then
    record_indexed_corpus "$engine" "$dataset" "$sha" || status=1
  fi
  "$ROOT/scripts/stop-$engine.sh" || status=1
  return "$status"
}

feed_dataset() {
  local engine=$1 port=$2 corpus=$3 layout=$4 collection=$5 dataset=$6
  local -a topology=()
  case "$layout" in
    merged|as-fed) ;;
    *) topology=(--topology "$layout") ;;
  esac
  if [[ "$engine" == luxir ]]; then
    python3 "$ROOT/python/feed_luxir.py" "$corpus" --collection "$collection" \
      "${topology[@]}" > "$(dataset_artifact "$engine" "$dataset" feed.json)" || return 1
  else
    python3 "$ROOT/python/feed_rest.py" "$engine" "$corpus" --port "$port" \
      --index "$collection" "${topology[@]}" \
      > "$(dataset_artifact "$engine" "$dataset" feed.json)" || return 1
  fi
  if [[ "$layout" == merged ]]; then
    force_merge_one "$engine" "$port" "$collection" || return 1
  fi
  if [[ ${#topology[@]} -gt 0 ]]; then
    python3 "$ROOT/python/verify_topology.py" "$engine" "$layout" --port "$port" \
      --collection "$collection" \
      > "$(dataset_artifact "$engine" "$dataset" topology.json)" || return 1
  fi
}

wait_ready() {
  local url=$1 pid=$2 deadline=$((SECONDS + START_TIMEOUT))
  while (( SECONDS < deadline )); do
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "server exited before becoming ready" >&2
      return 1
    fi
    if curl --fail --silent "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "timed out after ${START_TIMEOUT}s waiting for $url" >&2
  return 1
}

engine_port() {
  case "$1" in
    luxir) echo 9400 ;;
    opensearch) echo 9201 ;;
    elasticsearch) echo 9202 ;;
    *) echo "unknown engine: $1" >&2; return 1 ;;
  esac
}

engine_process() {
  case "$1" in
    luxir|opensearch|elasticsearch) echo "$1" ;;
    *) echo "unknown engine: $1" >&2; return 1 ;;
  esac
}

engine_pidfile() {
  echo "$ROOT/run/$(engine_process "$1").pid"
}

archive_existing() {
  local path=$1 destination=${2:-} archived
  if [[ -n "$destination" ]]; then
    printf -v "$destination" '%s' ""
  fi
  [[ -e "$path" ]] || return 0
  archived="$path.previous-$(date -u +%Y%m%dT%H%M%SZ)-$$"
  mv "$path" "$archived"
  if [[ -n "$destination" ]]; then
    printf -v "$destination" '%s' "$archived"
  fi
  echo "archived $path as $archived"
}

prepare_rest_config() {
  local engine=$1 home=$2 config source stamp
  config="$ROOT/run/config/$engine"
  source=$(readlink -f "$home")
  stamp="$config/.searchbench-source"
  if [[ ! -f "$stamp" || "$(cat "$stamp")" != "$source" ]]; then
    archive_existing "$config"
    mkdir -p "$config"
    cp -a "$home/config/." "$config/"
    printf '%s\n' "$source" > "$stamp"
  fi
  cp "$ROOT/config/$engine/$engine.yml" "$config/$engine.yml"
  mkdir -p "$config/jvm.options.d"
  cp "$ROOT/config/$engine/searchbench.options" \
    "$config/jvm.options.d/searchbench.options"
  # Server-posture variant: jvm.options.d files apply lexicographically, so a
  # zz- overlay's heap flags land after (and override) searchbench.options.
  local overlay="$config/jvm.options.d/zz-searchbench-variant.options"
  if [[ -n "${SEARCHBENCH_HEAP:-}" ]]; then
    printf -- '-Xms%s\n-Xmx%s\n' "$SEARCHBENCH_HEAP" "$SEARCHBENCH_HEAP" > "$overlay"
  else
    rm -f "$overlay"
  fi
}

ensure_running() {
  local engine=$1 pidfile
  pidfile=$(engine_pidfile "$engine")
  if [[ -f "$pidfile" ]] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
    return 0
  fi
  [[ ! -e "$pidfile" ]] || archive_existing "$pidfile"
  echo "starting $engine (left running; use scripts/stop-$engine.sh to stop)"
  "$ROOT/scripts/start-$engine.sh" >/dev/null
}

served_count() {
  local engine=$1 port=$2 collection=${3:-searchbench}
  if [[ "$engine" == luxir ]]; then
    curl -s -X POST "localhost:$port/collections/$collection/_search" \
      -d '{"query":{"all":true},"limit":0,"get_number":true}' \
      | python3 -c 'import json,sys; print(json.load(sys.stdin).get("found",-1))'
  else
    curl -s "localhost:$port/$collection/_count" \
      | python3 -c 'import json,sys; print(json.load(sys.stdin).get("count",-1))'
  fi
}

verify_count() {
  local got
  got=$(served_count "$1" "$2" "${4:-searchbench}")
  if [[ "$got" != "$3" ]]; then
    echo "$1 serves $got docs but corpus has $3; index is stale or partial" >&2
    return 1
  fi
}

force_merge_one() {
  local engine=$1 port=$2 collection=${3:-searchbench} response
  if [[ "$engine" == luxir ]]; then
    response=$(curl --fail --silent --show-error -X POST \
      -H 'Content-Type: application/json' \
      "localhost:$port/collections/$collection/_update" \
      -d '{"commit":{"max_segments":1,"wait_for_merges":true}}') || return 1
    python3 -c 'import json,sys
value=json.load(sys.stdin)
if value.get("status") not in (None, "ok") or value.get("error"):
    raise SystemExit(f"Luxir force merge failed: {value}")' <<< "$response"
  else
    response=$(curl --fail --silent --show-error -X POST \
      "localhost:$port/$collection/_forcemerge?max_num_segments=1") || return 1
    python3 -c 'import json,sys
value=json.load(sys.stdin)
if value.get("_shards", {}).get("failed", 0):
    raise SystemExit(f"REST force merge failed: {value}")' <<< "$response"
    curl --fail --silent --show-error -X POST \
      "localhost:$port/$collection/_refresh" >/dev/null
  fi
}

wait_cool() {
  local target=${1:-78} timeout=${2:-120} t sensor
  local deadline=$((SECONDS + timeout))
  for sensor in /sys/class/hwmon/hwmon*; do
    [[ "$(cat "$sensor/name" 2>/dev/null)" == k10temp ]] || continue
    while (( SECONDS < deadline )); do
      t=$(awk '{printf "%d", $1/1000}' "$sensor/temp1_input")
      (( t <= target )) && return 0
      sleep 0.5
    done
    echo "Tctl still ${t}C after ${timeout}s; proceeding" >&2
    return 0
  done
}

stop_pidfile() {
  local pidfile=$1 pid
  [[ -f "$pidfile" ]] || return 0
  pid=$(cat "$pidfile")
  if kill -0 "$pid" 2>/dev/null; then
    kill "$pid"
    for _ in $(seq 1 120); do
      kill -0 "$pid" 2>/dev/null || break
      sleep 0.5
    done
    if kill -0 "$pid" 2>/dev/null; then
      echo "process $pid ignored SIGTERM for 60s; sending SIGKILL" >&2
      kill -9 "$pid" 2>/dev/null
    fi
  fi
  archive_existing "$pidfile"
}

# Query-cache posture for REST engines. SEARCHBENCH_QUERY_CACHE=off|on flips
# the Lucene query cache (index.queries.cache.enabled) on the served index; it
# is a static index setting, so the index is closed around the update. A
# missing index (fresh feed session) is skipped: the posture is applied by the
# serving session's start, after the dataset exists. Luxir consumes the same
# variable in start-luxir.sh (--query-cache-bytes=0).
apply_rest_query_cache() {
  local engine=$1 port=$2 collection=${DATASET_COLLECTION:-searchbench} enabled
  # The setting persists in the index, so unset restores the engine default
  # (enabled) rather than inheriting whatever the previous board left behind.
  case "${SEARCHBENCH_QUERY_CACHE:-on}" in
    off) enabled=false ;;
    on) enabled=true ;;
    *) echo "SEARCHBENCH_QUERY_CACHE must be 'off' or 'on'" >&2; return 1 ;;
  esac
  local base="http://127.0.0.1:$port/$collection"
  if ! curl -sf "$base" > /dev/null 2>&1; then
    echo "$engine: no index $collection yet; query-cache posture deferred" >&2
    return 0
  fi
  local current
  current=$(curl -sf "$base/_settings?filter_path=*.settings.index.queries.cache.enabled"     | grep -o '"enabled":"[a-z]*"' | grep -o 'true\|false' || echo true)
  [[ "$current" == "$enabled" ]] && return 0
  curl -sf -X POST "$base/_close" > /dev/null || return 1
  curl -sf -X PUT "$base/_settings" -H 'Content-Type: application/json' \
    -d "{\"index.queries.cache.enabled\": $enabled}" > /dev/null || return 1
  curl -sf -X POST "$base/_open" > /dev/null || return 1
  curl -sf "http://127.0.0.1:$port/_cluster/health/$collection?wait_for_status=yellow&timeout=30s" > /dev/null || return 1
  echo "$engine: index.queries.cache.enabled=$enabled" >&2
}
