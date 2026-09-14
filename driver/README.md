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
