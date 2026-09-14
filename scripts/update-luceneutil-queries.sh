#!/usr/bin/env bash
# Re-derive the checked-in primary query source from the pinned luceneutil task file.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
COMMIT=3cfd163094feda7ef5ab14d89a095e4e523c33f2
SOURCE_SHA256=e8166233f415b9bb735618f3a5f5205e25cf1d6b86d1e6647176288b05d0271a
URL="https://raw.githubusercontent.com/mikemccand/luceneutil/$COMMIT/tasks/wikimedium.10M.tasks"
TMPDIR_QUERY=$(mktemp -d)
trap 'rm -rf "$TMPDIR_QUERY"' EXIT
SOURCE="$TMPDIR_QUERY/wikimedium.10M.tasks"

curl -fsSL "$URL" -o "$SOURCE"
echo "$SOURCE_SHA256  $SOURCE" | sha256sum --check --status || {
  echo "luceneutil task source checksum mismatch" >&2
  exit 1
}
python3 "$ROOT/python/import_luceneutil_queries.py" "$SOURCE" \
  "$ROOT/queries/luceneutil/queries-all.txt" --limit-per-class 50
