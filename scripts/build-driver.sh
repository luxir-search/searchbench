#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
BUILD_DIR=${SEARCHBENCH_DRIVER_BUILD:-$ROOT/driver/build}
CMAKE_ARGS=(-DCMAKE_BUILD_TYPE=Release)
LUXIR_REPO=${LUXIR_REPO:-$ROOT/../luxir}
LUXIR_BIN=${LUXIR_BIN:-$LUXIR_REPO/build/gcc-release/bin/luxir}
LUXIR_BUILD=$(dirname "$(dirname "$LUXIR_BIN")")
LUXIR_CACHE="$LUXIR_BUILD/CMakeCache.txt"
TOOLCHAIN=${CMAKE_TOOLCHAIN_FILE:-}

if [[ -z "$TOOLCHAIN" && -n ${VCPKG_ROOT:-} ]]; then
  TOOLCHAIN="$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake"
fi
if [[ -z "$TOOLCHAIN" && -f "$LUXIR_CACHE" ]]; then
  TOOLCHAIN=$(awk -F= '$1 == "CMAKE_TOOLCHAIN_FILE:FILEPATH" {print $2; exit}' "$LUXIR_CACHE")
fi
if [[ -z "$TOOLCHAIN" && -f /opt/vcpkg/scripts/buildsystems/vcpkg.cmake ]]; then
  TOOLCHAIN=/opt/vcpkg/scripts/buildsystems/vcpkg.cmake
fi
[[ -z "$TOOLCHAIN" ]] || CMAKE_ARGS+=("-DCMAKE_TOOLCHAIN_FILE=$TOOLCHAIN")

cmake -S "$ROOT/driver" -B "$BUILD_DIR" "${CMAKE_ARGS[@]}"
cmake --build "$BUILD_DIR" -j
test -x "$BUILD_DIR/bench_replay"
