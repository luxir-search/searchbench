# Native replay driver

`bench_replay` is the engine-neutral HTTP load loop used after Python has
resolved and validated a workload. It replays pre-serialized HTTP/1.1 requests,
records HDR latency histograms and response failures, and samples server CPU
time around each repetition.

Build:

```bash
cmake -S driver -B driver/build \
  -DCMAKE_TOOLCHAIN_FILE="$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake"
cmake --build driver/build
```

From the repository root, `scripts/build-driver.sh` performs the same build and
uses `CMAKE_TOOLCHAIN_FILE` or `VCPKG_ROOT` when set.

The Python driver writes the workload file and invokes this binary. Run
`python/driver.py --help` rather than constructing the binary protocol by hand.

Vector workloads use SBWL version 2: after each version-1 request record,
a uint32 little-endian length and that many expected-response marker bytes
follow. Replay requires that marker after stripping JSON whitespace outside
strings. The vector adapter supplies the complete prevalidated docs array,
so every timed response must return the same ordered ids used for recall.
Version 1 remains unchanged. vector_tool is a separate preparation helper;
it performs float32 JSON serialization and exhaustive IP top-k without HTTP.
