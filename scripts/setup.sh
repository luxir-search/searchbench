#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
CORPUS_MODE=smoke
REFERENCES=0

usage() {
  echo "usage: $0 [--smoke|--standard|--scale|--all] [--references]"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --smoke) CORPUS_MODE=smoke ;;
    --standard) CORPUS_MODE=standard ;;
    --scale) CORPUS_MODE=scale ;;
    --all) CORPUS_MODE=all ;;
    --references) REFERENCES=1 ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; exit 2 ;;
  esac
  shift
done

LUXIR_REPO=${LUXIR_REPO:-$ROOT/../luxir}
LUXIR_BIN=${LUXIR_BIN:-$LUXIR_REPO/build/gcc-release/bin/luxir}
[[ -x "$LUXIR_BIN" ]] || {
  echo "missing built Luxir server: $LUXIR_BIN" >&2
  echo "set LUXIR_BIN to an executable Luxir server" >&2
  exit 1
}

"$ROOT/scripts/build-driver.sh"
"$ROOT/scripts/prepare-corpus.sh" "$CORPUS_MODE"
if [[ "$REFERENCES" == 1 ]]; then
  "$ROOT/engines/download.sh"
fi

echo "Searchbench setup complete"
echo "Luxir: $LUXIR_BIN"
