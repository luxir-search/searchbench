#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
ENGINE_DIR="$ROOT/engines"
CACHE_DIR="$ENGINE_DIR/cache"
VERSIONS="$ENGINE_DIR/versions.json"
mkdir -p "$CACHE_DIR"

read_version() {
  python3 - "$VERSIONS" "$1" "$2" <<'PY'
import json, sys
with open(sys.argv[1], encoding="utf-8") as source:
    print(json.load(source)[sys.argv[2]][sys.argv[3]])
PY
}

download() {
  local url=$1 out=$2
  if [[ -s "$out" ]]; then
    return
  fi
  local partial="$out.part"
  curl --fail --location --retry 3 --continue-at - --output "$partial" "$url"
  mv "$partial" "$out"
}

install_opensearch() {
  local version url signature_url key_url fingerprint archive key target gnupg actual
  version=$(read_version opensearch version)
  url=$(read_version opensearch url)
  signature_url=$(read_version opensearch signature_url)
  key_url=$(read_version opensearch signing_key_url)
  fingerprint=$(read_version opensearch signing_key_fingerprint)
  archive="$CACHE_DIR/${url##*/}"
  key="$CACHE_DIR/opensearch-release.pgp"
  gnupg="$CACHE_DIR/opensearch-gnupg"
  target="$ENGINE_DIR/opensearch-$version"

  download "$url" "$archive"
  download "$signature_url" "$archive.sig"
  download "$key_url" "$key"
  command -v gpg >/dev/null || { echo "gpg is required to verify OpenSearch" >&2; return 1; }
  mkdir -p "$gnupg"
  chmod 700 "$gnupg"
  GNUPGHOME="$gnupg" gpg --batch --import "$key" >/dev/null 2>&1
  actual=$(GNUPGHOME="$gnupg" gpg --batch --with-colons --fingerprint \
    | awk -F: '$1=="fpr" {print $10; exit}')
  [[ "$actual" == "$fingerprint" ]] \
    || { echo "OpenSearch signing-key fingerprint mismatch" >&2; return 1; }
  GNUPGHOME="$gnupg" gpg --batch --verify "$archive.sig" "$archive"
  (cd "$CACHE_DIR" && sha512sum "${archive##*/}" \
    > "${archive##*/}.sha512.local")
  if [[ ! -x "$target/bin/opensearch" ]]; then
    tar -xzf "$archive" -C "$ENGINE_DIR"
  fi
  test -x "$target/bin/opensearch"
}

install_elasticsearch() {
  local version url checksum_url archive checksum target
  version=$(read_version elasticsearch version)
  url=$(read_version elasticsearch url)
  checksum_url=$(read_version elasticsearch checksum_url)
  archive="$CACHE_DIR/${url##*/}"
  checksum="$archive.sha512"
  target="$ENGINE_DIR/elasticsearch-$version"

  download "$url" "$archive"
  download "$checksum_url" "$checksum"
  (cd "$CACHE_DIR" && sha512sum -c "$(basename "$checksum")")
  if [[ ! -x "$target/bin/elasticsearch" ]]; then
    tar -xzf "$archive" -C "$ENGINE_DIR"
  fi
  test -x "$target/bin/elasticsearch"
}

case "${1:-all}" in
  opensearch) install_opensearch ;;
  elasticsearch) install_elasticsearch ;;
  all) install_opensearch; install_elasticsearch ;;
  *) echo "usage: $0 [all|opensearch|elasticsearch]" >&2; exit 2 ;;
esac
