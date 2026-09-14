#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
MANIFEST="$ROOT/corpora/wikipedia/manifest.json"
CACHE="$ROOT/corpus/cache"
TRANSFORM="$ROOT/python/corpus_transform.py"

manifest_value() {
  python3 - "$MANIFEST" "$1" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as source:
    value = json.load(source)
for component in sys.argv[2].split("."):
    value = value[component]
print(value)
PY
}

SOURCE_URL=$(manifest_value source_url)
SOURCE_FILE=$(manifest_value source_file)
SOURCE_FORMAT=$(manifest_value source_format)
SOURCE_BYTES=$(manifest_value source_bytes)
SOURCE_SHA256=$(manifest_value source_sha256)
SOURCE_GIB=$(python3 - "$SOURCE_BYTES" <<'PY'
import sys

print(f"{int(sys.argv[1]) / 2**30:.2f} GiB")
PY
)
SMOKE_DOCUMENTS=$(manifest_value lanes.smoke.documents)
SMOKE_VARIANT=$(manifest_value lanes.smoke.variant)
STANDARD_DOCUMENTS=$(manifest_value lanes.standard.documents)
STANDARD_VARIANT=$(manifest_value lanes.standard.variant)
FACET_DOCUMENTS=$(manifest_value lanes.facet.documents)
FACET_VARIANT=$(manifest_value lanes.facet.variant)
SCALE_DOCUMENTS=$(manifest_value lanes.scale.documents)
SCALE_VARIANT=$(manifest_value lanes.scale.variant)
ARCHIVE="$CACHE/$SOURCE_FILE"

verify_file() {
  local path=$1 actual_bytes actual_sha
  actual_bytes=$(stat -c '%s' "$path")
  [[ "$actual_bytes" == "$SOURCE_BYTES" ]] || {
    echo "Wikipedia archive size mismatch: expected $SOURCE_BYTES, got $actual_bytes" >&2
    return 1
  }
  actual_sha=$(sha256sum "$path" | awk '{print $1}')
  [[ "$actual_sha" == "$SOURCE_SHA256" ]] || {
    echo "Wikipedia archive SHA-256 mismatch" >&2
    echo "expected: $SOURCE_SHA256" >&2
    echo "actual:   $actual_sha" >&2
    return 1
  }
}

verify_archive() {
  local stamp="$ARCHIVE.sha256-ok"
  if [[ -s "$ARCHIVE" && -s "$stamp" && "$stamp" -nt "$ARCHIVE" ]] \
      && [[ "$(cat "$stamp")" == "$SOURCE_SHA256" ]]; then
    return
  fi
  verify_file "$ARCHIVE"
  printf '%s\n' "$SOURCE_SHA256" > "$stamp"
}

download_archive() {
  local partial="$ARCHIVE.part"
  mkdir -p "$CACHE"
  if [[ ! -s "$ARCHIVE" ]]; then
    echo "downloading pinned luceneutil Wikipedia corpus ($SOURCE_GIB)"
    curl --fail --location --retry 3 --retry-all-errors --continue-at - \
      --output "$partial" "$SOURCE_URL"
    verify_file "$partial"
    mv "$partial" "$ARCHIVE"
  fi
  verify_archive
}

prepare_output() {
  local name=$1 variant=$2 limit=$3 expected=$4
  local output="$ROOT/corpus/$name"
  local report="$output.cardinalities.json"
  local digest="$output.sha256"
  local output_part="$output.part"
  local report_part="$report.part"
  local digest_part="$digest.part"
  local actual_docs actual_sha

  if [[ -s "$output" && -s "$report" && -s "$digest" \
      && "$output" -nt "$ARCHIVE" && "$output" -nt "$TRANSFORM" \
      && "$output" -nt "$MANIFEST" ]]; then
    echo "corpus ready: $output"
    return
  fi

  echo "preparing $name"
  rm -f "$output_part" "$report_part" "$digest_part"
  local -a args=(--source-format "$SOURCE_FORMAT" --variant "$variant"
                 --cardinality-report "$report_part")
  [[ "$limit" == 0 ]] || args+=(--limit "$limit")
  if ! python3 "$TRANSFORM" "${args[@]}" "$ARCHIVE" > "$output_part"; then
    rm -f "$output_part" "$report_part" "$digest_part"
    return 1
  fi
  actual_docs=$(wc -l < "$output_part")
  if [[ "$actual_docs" != "$expected" ]]; then
    echo "corpus document-count mismatch: expected $expected, got $actual_docs" >&2
    rm -f "$output_part" "$report_part" "$digest_part"
    return 1
  fi
  actual_sha=$(python3 - "$report_part" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as source:
    print(json.load(source)["corpus_sha256"])
PY
)
  printf '%s\n' "$actual_sha" > "$digest_part"
  mv "$output_part" "$output"
  mv "$report_part" "$report"
  mv "$digest_part" "$digest"
  echo "corpus ready: $output ($actual_docs documents)"
}

usage() {
  echo "usage: $0 [smoke|standard|scale|facet|all]" >&2
  exit 2
}

mode=${1:-smoke}
[[ $# -le 1 ]] || usage
case "$mode" in
  smoke|standard|scale|facet|all) ;;
  *) usage ;;
esac

download_archive
case "$mode" in
  smoke)
    prepare_output corpus-100k-searchbench.ndjson "$SMOKE_VARIANT" \
      "$SMOKE_DOCUMENTS" "$SMOKE_DOCUMENTS"
    ;;
  standard)
    prepare_output corpus-10m-searchbench.ndjson "$STANDARD_VARIANT" \
      "$STANDARD_DOCUMENTS" "$STANDARD_DOCUMENTS"
    ;;
  scale)
    prepare_output corpus-33m-searchbench.ndjson "$SCALE_VARIANT" \
      "$SCALE_DOCUMENTS" "$SCALE_DOCUMENTS"
    ;;
  facet)
    prepare_output corpus-10m-facet.ndjson "$FACET_VARIANT" \
      "$FACET_DOCUMENTS" "$FACET_DOCUMENTS"
    ;;
  all)
    prepare_output corpus-100k-searchbench.ndjson "$SMOKE_VARIANT" \
      "$SMOKE_DOCUMENTS" "$SMOKE_DOCUMENTS"
    prepare_output corpus-10m-searchbench.ndjson "$STANDARD_VARIANT" \
      "$STANDARD_DOCUMENTS" "$STANDARD_DOCUMENTS"
    prepare_output corpus-10m-facet.ndjson "$FACET_VARIANT" \
      "$FACET_DOCUMENTS" "$FACET_DOCUMENTS"
    prepare_output corpus-33m-searchbench.ndjson "$SCALE_VARIANT" \
      "$SCALE_DOCUMENTS" "$SCALE_DOCUMENTS"
    ;;
esac
