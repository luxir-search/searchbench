#!/usr/bin/env python3
"""Shared validation, native load replay, sampling, and JSON reporting core."""

import argparse
import asyncio
from collections import Counter
from dataclasses import replace
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import statistics
import struct
import subprocess
import tempfile
import time

from adapters import DEFAULT_PORTS, ENGINES, make_adapter
from cpu_layout import current_cpu_sets, format_cpu_list
from network_namespace import network_namespace
from presets import TASKS, comparable_hash, param_hash, parse_override, resolve
from query_source import read_id_batches, searchbench_queries
from request_capture import capture_request
from results_io import write_context
from sampler import ProcSampler
from schema import (ANALYZER_POSTURE, BODY_FIELD, IDENTITY_POSTURE,
                    index_layout_version)
from topologies import TOPOLOGIES, assert_topology, resolve_topology
from topology_probe import assert_quiescent_topology, index_topology
from variants import client_settings, param_settings, parse_variant

SEARCHBENCH_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LUXIR_REPO = SEARCHBENCH_ROOT.parent / "luxir"
# The validated warmup pass runs at this fixed client concurrency for every
# cell, whatever the cell's own concurrency is.
WARMUP_CONCURRENCY = 8
DEFAULT_SERVER_CPUS, DEFAULT_CLIENT_CPUS = current_cpu_sets()
DEFAULT_SERVER_CORES = format_cpu_list(DEFAULT_SERVER_CPUS)
DEFAULT_CLIENT_CORES = format_cpu_list(DEFAULT_CLIENT_CPUS)

STARTUP_ENV_NAMES = frozenset({
    "SEARCHBENCH_ROOT", "ES_PATH_CONF", "OPENSEARCH_PATH_CONF",
    "JAVA_HOME", "ES_JAVA_HOME", "OPENSEARCH_JAVA_HOME",
    "JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS", "_JAVA_OPTIONS",
    "ES_JAVA_OPTS", "OPENSEARCH_JAVA_OPTS",
    "LD_PRELOAD", "LD_LIBRARY_PATH", "GLIBC_TUNABLES",
    "MALLOC_ARENA_MAX", "MALLOC_CONF",
    "LUXIR_BIN", "LUXIR_REPO", "LUXIR_EXTRA_ARGS",
    "ES_EXTRA_ARGS", "OPENSEARCH_EXTRA_ARGS", "SEARCHBENCH_HEAP",
})
STARTUP_ENV_PREFIXES = ("OMP_", "TBB_")


def parse_cores(spec):
    result = set()
    for part in spec.split(","):
        bounds = part.split("-", 1)
        if len(bounds) == 1:
            result.add(int(bounds[0]))
        else:
            result.update(range(int(bounds[0]), int(bounds[1]) + 1))
    return result


def make_workload(task, params, pools, max_queries, adapter, corpus=None):
    """Workload entries are (resolved_params, query_item)."""
    def pool_for(name, preset_params):
        pool = pools["count"] if preset_params["limit"] == 0 else pools["top"]
        query_class = preset_params.get("query_class")
        if query_class is not None:
            pool = [item for item in pool if item.query_class == query_class]
        if max_queries:
            pool = pool[:max_queries]
        if not pool:
            raise RuntimeError(
                f"query source produced no queries for class {query_class or 'all'}")
        return pool

    if params["shape"] == "get":
        entry = dict(params, name=task)
        items = read_id_batches(corpus, params["batch_size"], max_queries)
        return [(entry, item) for item in items]
    if params["shape"] != "mix":
        entry = dict(params, name=task)
        pool = pool_for(task, params)
        variants = params.get("query_variants", 0)
        if not isinstance(variants, int) or variants < 0:
            raise ValueError("query_variants must be a non-negative integer")
        if variants:
            prefix = task.lower()
            pool = [replace(pool[index % len(pool)],
                            nonce=f"__searchbench_nonce_{prefix}_{index:08x}__")
                    for index in range(variants)]
        return [(entry, item) for item in pool]
    workload = []
    slots = [(name, dict(resolve(name, params["count_mode"]), name=name))
             for name in params["slots"]]
    base = pool_for(task, {"limit": 10})
    for query_index in range(len(base)):
        for name, slot_params in slots:
            slot_pool = pool_for(name, slot_params)
            workload.append((slot_params, slot_pool[query_index % len(slot_pool)]))
    return workload


async def run_fixed(host, port, timeout, concurrency, adapter, workload, validate):
    """One pass over the workload. With validate=True this is the untimed
    validation phase: every response is fully parsed, payloads are asserted
    per task, and total-hit counts are collected for cross-engine agreement."""
    if not validate:
        raise ValueError("run_fixed is reserved for the validation pass")
    queue = asyncio.Queue()
    for sequence, work in enumerate(workload):
        queue.put_nowait((sequence, work))
    latencies = []
    errors = []
    counts = {}

    async def worker():
        connection = adapter.connect(host, port, timeout)
        while True:
            try:
                sequence, (entry, item) = queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            started = time.perf_counter_ns()
            try:
                # Keep request-build failures as normal per-item errors rather
                # than crashing the whole validation pass.
                request = adapter.build(entry, item)
                raw = await connection.request(request)
                count = adapter.validate(entry, raw, item)
                if count is not None:
                    counts[f"{entry['name']}\t{item.text}"] = count
                latencies.append((time.perf_counter_ns() - started) / 1_000_000.0)
            except Exception as error:
                errors.append({"sequence": sequence, "task": entry["name"], "query": item.text,
                               "error": repr(error)})
            finally:
                queue.task_done()
        await connection.close()

    started = time.perf_counter()
    await asyncio.gather(*(worker() for _ in range(min(concurrency, len(workload)))))
    return latencies, errors, counts, time.perf_counter() - started


def materialize_workload(workload, adapter, host, port, blob_path):
    """Write fully formed wire requests to an mmap-friendly SBWL blob.

    The blob holds opaque length-prefixed HTTP/1.1 requests.
    """
    labels = []
    bucket_ids = {}
    for entry, _item in workload:
        label = entry["name"]
        if label not in bucket_ids:
            encoded = label.encode("utf-8")
            if len(encoded) > 0xffff:
                raise ValueError(f"bucket label is too long: {label!r}")
            bucket_ids[label] = len(labels)
            labels.append(label)
    if not labels or len(labels) > 0xffff:
        raise ValueError("workload must have 1..65535 distinct buckets")
    if not workload or len(workload) > 0xffffffff:
        raise ValueError("workload must have 1..4294967295 records")

    representative_requests = {}
    with open(blob_path, "wb") as destination:
        destination.write(struct.pack("<4sIIH", b"SBWL", 1, len(workload), len(labels)))
        for label in labels:
            encoded = label.encode("utf-8")
            destination.write(struct.pack("<H", len(encoded)))
            destination.write(encoded)
        for entry, item in workload:
            request = adapter.build(entry, item)
            wire = adapter.wire_bytes(request, host, port)
            if len(wire) > 0xffffffff:
                raise ValueError("serialized request exceeds 4 GiB")
            if entry["name"] not in representative_requests:
                representative_requests[entry["name"]] = capture_request(
                    request, wire, entry)
            destination.write(struct.pack("<IH", len(wire), bucket_ids[entry["name"]]))
            destination.write(wire)
    return str(blob_path), labels, representative_requests


def latency_ms(latency_us):
    return {name: None if value is None else value / 1000.0
            for name, value in latency_us.items()}


def replay_metrics(raw, elapsed):
    requests = raw["requests"]
    return {
        "requests": requests,
        "elapsed_s": elapsed,
        "qps": requests / elapsed if elapsed else 0.0,
        "latency_ms": latency_ms(raw["latency_us"]),
        "p99_claim_valid": elapsed >= 60.0,
        "errors": raw["errors"],
    }


def add_server_cpu(result, cpu_seconds):
    if result["requests"]:
        result["server_cpu"] = {
            "cpu_s": round(cpu_seconds, 3),
            "cores_busy": round(cpu_seconds / result["elapsed_s"], 2),
            "cpu_ms_per_request": round(
                cpu_seconds * 1000.0 / result["requests"], 3),
        }


def add_client_cpu(result, cpu_seconds):
    """Replay-process CPU: near-saturated replay threads mean a client-bound
    cell whose throughput ceiling is the client, not the engine."""
    if cpu_seconds is not None and result["elapsed_s"]:
        result["client_cpu"] = {
            "cpu_s": round(cpu_seconds, 3),
            "cores_busy": round(cpu_seconds / result["elapsed_s"], 2),
        }


def run_replay(root, args, adapter, shape, blob_path, driver_output):
    binary = Path(os.environ.get(
        "SEARCHBENCH_DRIVER", root / "driver" / "build" / "bench_replay"))
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise RuntimeError(
            f"C++ replay driver is not executable: {binary}; build driver/bench_replay first")
    command = [
        str(binary),
        "--workload", str(blob_path),
        "--host", args.host,
        "--port", str(args.port),
        "--concurrency", str(args.concurrency),
        "--threads", str(args.threads),
        "--client-cores", args.client_cores,
        "--duration", str(args.duration),
        "--repetitions", str(args.repetitions),
        "--order", args.order,
        "--warmup-seconds", str(args.warmup_seconds),
        "--max-requests", str(args.max_requests),
        "--warmup-requests", str(args.warmup_requests),
        "--timeout", str(args.timeout),
        "--server-pid", str(args.server_pid),
        "--out", str(driver_output),
    ] + adapter.replay_args()
    if args.seed is not None:
        command += ["--seed", str(args.seed)]
    for kind, marker in adapter.fail_rules(shape):
        try:
            text = marker.decode("ascii")
        except UnicodeDecodeError as error:
            raise ValueError("driver fail markers must be ASCII") from error
        command += ["--fail", f"{kind}:{text}"]
    completed = subprocess.run(command, text=True, capture_output=True, check=False)
    if completed.returncode not in (0, 1):
        raise RuntimeError(
            f"bench_replay exited {completed.returncode}: "
            f"{completed.stderr[-2000:] or completed.stdout[-2000:]}")
    try:
        result = json.loads(Path(driver_output).read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise RuntimeError(
            f"bench_replay produced no readable result: {error}; "
            f"stderr={completed.stderr[-1000:]!r}") from error
    if result.get("schema") != "sbdriver-1":
        raise RuntimeError(f"unexpected replay schema: {result.get('schema')!r}")
    return result


def file_sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def resolve_running_binary(pidfile=None):
    if pidfile is None:
        root = Path(__file__).resolve().parents[1]
        pidfile = root / "run" / "luxir.pid"
    try:
        pid = int(Path(pidfile).read_text(encoding="utf-8").strip())
        if pid <= 0:
            return None
        os.kill(pid, 0)
        proc_exe = f"/proc/{pid}/exe"
        binary = os.readlink(proc_exe)
        binary_sha256 = file_sha256(proc_exe)
    except (OSError, UnicodeError, ValueError):
        return None
    suffix = " (deleted)"
    if binary.endswith(suffix):
        binary = binary[:-len(suffix)]
    return binary, binary_sha256


def git_head():
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=os.environ.get("LUXIR_REPO", DEFAULT_LUXIR_REPO), text=True).strip()
    except Exception:
        return None


def git_head_at(directory):
    try:
        return subprocess.check_output(
            ["git", "-C", directory, "rev-parse", "HEAD"], text=True).strip()
    except Exception:
        return None


def git_dirty(directory):
    try:
        return bool(subprocess.check_output(
            ["git", "-C", directory, "status", "--porcelain"], text=True).strip())
    except Exception:
        return False


def read_text(path):
    try:
        return Path(path).read_text(encoding="utf-8").strip()
    except OSError:
        return None


def corpus_config(path):
    corpus = Path(path)
    result = {"path": str(corpus), "exists": corpus.exists()}
    if corpus.exists():
        stat = corpus.stat()
        result.update({"bytes": stat.st_size, "mtime_ns": stat.st_mtime_ns})
        checksum = read_text(str(corpus) + ".sha256")
        result["sha256"] = checksum.split()[0] if checksum else None
        cardinalities = Path(str(corpus) + ".cardinalities.json")
        if cardinalities.exists():
            try:
                report = json.loads(cardinalities.read_text(encoding="utf-8"))
                if checksum and report.get("corpus_sha256") != checksum.split()[0]:
                    result["facet_cardinalities_error"] = "corpus SHA-256 differs"
                else:
                    # Embedded whole (once per run directory, in the context
                    # file): referencing the corpus-tree sidecar instead would
                    # break run-dir self-containment for archives and
                    # cross-box copies.
                    result["facet_cardinalities"] = report
            except (OSError, ValueError) as error:
                result["facet_cardinalities_error"] = repr(error)
    return result


def source_file_config(path):
    source = Path(path)
    return {"path": str(source), "bytes": source.stat().st_size,
            "sha256": file_sha256(source)}


def host_config(server_pid=None):
    governors = set()
    for path in Path("/sys/devices/system/cpu").glob("cpu*/cpufreq/scaling_governor"):
        value = read_text(path)
        if value:
            governors.add(value)
    swaps = read_text("/proc/swaps") or ""
    namespace = network_namespace()
    return {"hostname": platform.node(), "platform": platform.platform(),
            "machine": platform.machine(), "python": platform.python_version(),
            "cpu_count": os.cpu_count(), "cpu_governors": sorted(governors),
            "swap_enabled": len(swaps.splitlines()) > 1, "proc_swaps": swaps.splitlines(),
            "network": {
                "mode": (os.environ.get("SEARCHBENCH_NETWORK_MODE", "unmanaged")
                         if os.environ.get("SEARCHBENCH_NETWORK_NAMESPACE")
                         == namespace else "unmanaged"),
                "client_namespace": namespace,
                "server_namespace": (network_namespace(server_pid)
                                     if server_pid is not None else None),
            }}


def process_startup_config(engine, pid):
    """Capture the effective live process rather than rebuilding a launch command."""
    proc = Path(f"/proc/{pid}")
    raw_cmdline = (proc / "cmdline").read_bytes()
    argv = [part.decode("utf-8", "replace")
            for part in raw_cmdline.split(b"\0") if part]
    if not argv:
        raise RuntimeError(f"server process {pid} has an empty command line")

    raw_environment = (proc / "environ").read_bytes()
    full_environment = {}
    for entry in raw_environment.split(b"\0"):
        key, separator, value = entry.partition(b"=")
        if not separator:
            continue
        full_environment[key.decode("utf-8", "replace")] = value.decode(
            "utf-8", "replace")
    environment = {
        key: value for key, value in sorted(full_environment.items())
        if key in STARTUP_ENV_NAMES or key.startswith(STARTUP_ENV_PREFIXES)
    }

    overlays = []
    if engine in ("opensearch", "elasticsearch"):
        env_name = "OPENSEARCH_PATH_CONF" if engine == "opensearch" else "ES_PATH_CONF"
        config_dir = full_environment.get(env_name)
        if config_dir:
            for relative in (f"{engine}.yml", "jvm.options.d/searchbench.options"):
                path = Path(config_dir) / relative
                if not path.is_file():
                    continue
                raw = path.read_bytes()
                overlays.append({
                    "path": str(path), "relative_path": relative,
                    "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest(),
                    "content": raw.decode("utf-8"),
                })

    return {
        "source": f"/proc/{pid}",
        "pid": pid,
        "executable": os.readlink(proc / "exe"),
        "working_directory": os.readlink(proc / "cwd"),
        "argv": argv,
        "cmdline_sha256": hashlib.sha256(raw_cmdline).hexdigest(),
        "environment": environment,
        "config_overlays": overlays,
    }


def server_config(engine, versions, args):
    common = {"host": args.host, "port": args.port, "shards": 1, "replicas": 0,
              "network_host": "127.0.0.1", "security_enabled": False,
              "server_cores": args.server_cores,
              "index_layout_version": index_layout_version(engine),
              "full_text": {"field": BODY_FIELD, **ANALYZER_POSTURE[engine]},
              "identity": IDENTITY_POSTURE[engine]}
    if engine == "luxir":
        configured = os.environ.get("LUXIR_BIN")
        luxir_repo = Path(os.environ.get("LUXIR_REPO", DEFAULT_LUXIR_REPO))
        resolved = resolve_running_binary()
        if resolved is None:
            binary = (configured if configured is not None
                      else str(luxir_repo / "build" / "gcc-release" / "bin" / "luxir"))
            version = git_head()
            version_dirty = git_dirty(luxir_repo)
            binary_sha256 = file_sha256(binary) if os.path.exists(binary) else None
            binary_provenance = "env"
        else:
            binary, binary_sha256 = resolved
            directory = os.path.dirname(binary)
            # An out-of-repo binary has no version; do not guess LUXIR_REPO's
            # HEAD and stamp independent A/B arms with the same version.
            version = git_head_at(directory)
            version_dirty = git_dirty(directory) if version is not None else None
            binary_provenance = "proc"
        common.update({"version": version, "version_dirty": version_dirty,
                       "binary": binary, "binary_sha256": binary_sha256,
                       "binary_provenance": binary_provenance,
                       "store_backend": "fs", "grpc_port": 9401,
                       "http_threads": "auto", "grpc_threads": "auto"})
        if (resolved is not None and configured is not None
                and os.path.realpath(configured) != os.path.realpath(binary)):
            common["binary_configured"] = configured
    else:
        common.update({"version": versions[engine]["version"],
                       "store_backend": "lucene",
                       "distribution": versions[engine]["distribution"],
                       "license": versions[engine]["license"],
                       "transport_port": 9301 if engine == "opensearch" else 9302,
                       "node_count": 1})
    try:
        with open(f"/proc/{args.server_pid}/status", encoding="ascii") as status:
            for line in status:
                if line.startswith("Cpus_allowed_list:"):
                    common["observed_cpus_allowed_list"] = line.split(":", 1)[1].strip()
                    break
    except OSError:
        common["observed_cpus_allowed_list"] = None
    try:
        with open(f"/proc/{args.server_pid}/limits", encoding="ascii") as limits:
            for line in limits:
                if line.startswith("Max open files"):
                    fields = line.split()
                    common["nofile_soft"] = int(fields[3])
                    common["nofile_hard"] = int(fields[4])
                    break
    except (OSError, ValueError, IndexError):
        common["nofile_soft"] = None
        common["nofile_hard"] = None
    common["startup"] = process_startup_config(engine, args.server_pid)
    if engine != "luxir":
        # Observed heap, not asserted: the last -Xms/-Xmx on the live JVM's
        # command line wins, which is also how jvm.options.d overlays land.
        argv = common["startup"].get("argv") or ()
        for prefix, key in (("-Xms", "heap_min"), ("-Xmx", "heap_max")):
            values = [word[len(prefix):] for word in argv if word.startswith(prefix)]
            common[key] = values[-1] if values else None
    return common


async def async_main(args):
    root = Path(__file__).resolve().parents[1]
    with open(root / "engines" / "versions.json", encoding="utf-8") as source:
        versions = json.load(source)
    requested_cores = parse_cores(args.client_cores)
    allowed_before = os.sched_getaffinity(0)
    if not requested_cores <= allowed_before:
        raise RuntimeError(f"client cores {sorted(requested_cores)} not allowed by {sorted(allowed_before)}")
    os.sched_setaffinity(0, requested_cores)
    # Variant task-parameter keys and --set share one override path; --set
    # wins on conflict so a variant sweep can still be probed ad hoc.
    overrides = {**param_settings(args.variant),
                 **dict(parse_override(text) for text in args.set or ())}
    params = resolve(args.task, args.lane, overrides)
    if args.mix_slots:
        if args.task != "MIX":
            raise ValueError("--mix-slots is only valid with the MIX task")
        slots = [slot.strip() for slot in args.mix_slots.split(",") if slot.strip()]
        if not slots or any(slot not in TASKS or slot == "MIX" for slot in slots):
            raise ValueError("--mix-slots must name one or more non-MIX presets")
        params["slots"] = slots
    pools = None if params["shape"] == "get" else searchbench_queries(args.queries)
    adapter = make_adapter(args.engine, args.collection)
    workload = make_workload(
        args.task, params, pools, args.max_queries, adapter, args.corpus)
    declared_topology = (resolve_topology(args.topology_name)
                         if args.topology_name else None)

    # One complete warmup pass, excluded from both timing and memory peaks.
    # This doubles as the validation phase: full parse, per-task payload
    # assertions, and count collection for cross-engine agreement. It always
    # runs at the standard concurrency, independent of the cell's client-count
    # variant: client count is pure client posture (server threading never
    # changes with it), so a c=1 cell warms the same server state as any other
    # and must not pay a sequential warmup for it.
    warmup = await run_fixed(args.host, args.port, args.timeout, WARMUP_CONCURRENCY,
                             adapter, workload, True)
    if warmup[1]:
        raise RuntimeError(f"warmup/validation failed: {warmup[1][:3]}")
    count_samples = warmup[2] if args.lane == "exact" else {}

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    timeline = str(output.with_suffix(".memory.csv"))
    with tempfile.TemporaryDirectory(prefix="searchbench-replay-") as temp_dir:
        blob_path, bucket_labels, representative_requests = materialize_workload(
            workload, adapter, args.host, args.port, Path(temp_dir) / "workload.sbwl")
        driver_output = Path(temp_dir) / "result.json"
        # This is the serving-reader topology immediately around the measured
        # interval. A benchmark over a changing segment set is not one stable
        # workload, even when both endpoint samples happen to have the same
        # scalar segment count.
        topology_before = index_topology(adapter, args.host, args.port, args.timeout)
        assert_quiescent_topology(topology_before, args.expected_segments)
        if declared_topology is not None:
            assert_topology(declared_topology, topology_before)
        sampler = ProcSampler(args.server_pid, timeline)
        sampler.start()
        try:
            replay = run_replay(
                root, args, adapter, params["shape"], blob_path, driver_output)
        finally:
            memory = sampler.stop()
        topology_after = index_topology(adapter, args.host, args.port, args.timeout)

    expected_buckets = {str(index): label for index, label in enumerate(bucket_labels)}
    if replay["workload"]["buckets"] != expected_buckets:
        raise RuntimeError("bench_replay returned bucket labels that differ from the workload")
    repetitions = []
    for raw_rep in replay["repetitions"]:
        elapsed = raw_rep["elapsed_s"]
        rep = {"repetition": raw_rep["repetition"],
               **replay_metrics(raw_rep["overall"], elapsed),
               "per_bucket": {
                   label: replay_metrics(bucket, elapsed)
                   for label, bucket in raw_rep["per_bucket"].items()}}
        # Server CPU charged to this rep (utime+stime across all threads):
        # cpu_ms_per_request measures base-strategy work independent of
        # intra-request parallelism and client ceilings; cores_busy is the
        # parallelism axis. Includes server background work (merges, GC).
        add_server_cpu(rep, raw_rep["server_cpu_s"])
        add_client_cpu(rep, raw_rep.get("client_cpu_s"))
        repetitions.append(rep)

    measured_elapsed = sum(rep["elapsed_s"] for rep in repetitions)
    aggregate = replay_metrics(replay["aggregate"]["overall"], measured_elapsed)
    aggregate["per_bucket"] = {
        label: replay_metrics(bucket, measured_elapsed)
        for label, bucket in replay["aggregate"]["per_bucket"].items()}
    rep_qps = [rep["qps"] for rep in repetitions]
    aggregate["repetition_qps_median"] = statistics.median(rep_qps) if rep_qps else None
    aggregate["repetition_qps_min"] = min(rep_qps) if rep_qps else None
    aggregate["repetition_qps_max"] = max(rep_qps) if rep_qps else None
    cpu_reps = [rep["server_cpu"] for rep in repetitions if "server_cpu" in rep]
    if cpu_reps:
        aggregate["server_cpu"] = {
            "cores_busy": round(statistics.median(r["cores_busy"] for r in cpu_reps), 2),
            "cpu_ms_per_request": round(
                statistics.median(r["cpu_ms_per_request"] for r in cpu_reps), 3)}
    client_reps = [rep["client_cpu"] for rep in repetitions if "client_cpu" in rep]
    if client_reps:
        aggregate["client_cpu"] = {
            "cores_busy": round(
                statistics.median(r["cores_busy"] for r in client_reps), 2)}
    all_errors = list(replay["errors"])
    topology_stable = topology_before == topology_after
    if not topology_stable:
        all_errors.append({"type": "index_topology_changed",
                           "before_segment_count": topology_before["segment_count"],
                           "after_segment_count": topology_after["segment_count"]})
    if (args.expected_segments is not None
            and topology_after["segment_count"] != args.expected_segments):
        all_errors.append({"type": "unexpected_segment_count",
                           "expected": args.expected_segments,
                           "actual": topology_after["segment_count"]})
    if topology_after.get("active_merges", 0):
        all_errors.append({"type": "active_merges_after_measurement",
                           "active_merges": topology_after["active_merges"]})
    if declared_topology is not None:
        try:
            assert_topology(declared_topology, topology_after)
        except RuntimeError as error:
            all_errors.append({"type": "declared_topology_mismatch",
                               "detail": str(error)})
    hash_params = params
    if args.order != "seq" or args.seed is not None:
        hash_params = dict(params, replay_order=args.order, replay_seed=args.seed)

    engine_config = server_config(args.engine, versions, args)
    startup = engine_config.get("startup")
    if startup:
        # pid/source are per-observation noise (the report's startup identity
        # already excludes them); the context must hash identically across
        # every cell of one unchanged server.
        engine_config = dict(engine_config, startup={
            key: value for key, value in startup.items()
            if key not in ("pid", "source")})
    # Everything run-constant goes into one immutable content-addressed file
    # per (engine, observed environment). The driver writes it, not the
    # orchestrator: the content is derived from what this process observed
    # (/proc capture, live topology), so a second context file for one engine
    # in one directory is the drift alarm, not bookkeeping.
    context = {
        "label": args.label,
        "lane": args.lane,
        # Measured, not asserted: OS concurrent segment search can make even
        # single-shard terms aggs approximate. 0 = exact; None = engine
        # reports no bound (luxir counts exactly).
        "facet_accuracy": {"max_doc_count_error_upper_bound": adapter.facet_error},
        "inert_params": sorted(adapter.inert_params),
        "corpus": corpus_config(args.corpus),
        "core_split": {"server": args.server_cores, "client": args.client_cores,
                       "client_allowed_before": sorted(allowed_before),
                       "client_allowed_after": sorted(os.sched_getaffinity(0))},
        "engine_config": engine_config,
        "host": host_config(args.server_pid),
        # Recorded as run environment whether or not this task's requests read
        # it (get/match-all cells), so one run yields one context.
        "source_files": {"queries": source_file_config(args.queries)},
        "index_topology": {
            "name": args.topology_name,
            "expected_segments": args.expected_segments,
            "declared_segment_docs": (
                list(declared_topology.segment_docs)
                if declared_topology is not None else None),
            "before": topology_before,
        },
    }
    context_name = write_context(output.parent, args.engine, context)

    result = {
        "schema_version": 8,
        "context": context_name,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "engine": args.engine,
        "task_class": args.task,
        # The declared deviations from the campaign baseline (variants.py).
        # The cell's identity axis for report pivoting; empty = baseline.
        "variant": args.variant,
        "task_parameters": params,
        "query_taxonomy": {
            "source": "Lucene benchmark workload query tags",
            "classes": sorted({item.query_class for _entry, item in workload}),
            "counts": {
                query_class: sum(item.query_class == query_class for _entry, item in workload)
                for query_class in sorted({item.query_class for _entry, item in workload})
            },
            "tag_counts": dict(sorted(Counter(
                tag for _entry, item in workload for tag in item.tags).items())),
        },
        "param_overrides": overrides or None,
        "param_hash": param_hash(hash_params),
        # The exact identity of this run is param_hash; comparability is
        # comparable_hash, which drops the parameters this engine's adapter
        # provably never reads, so a parameter only one engine implements
        # cannot split byte-identical measurements. Both are sanity checks:
        # report tooling compares recorded values, not hashes.
        "comparable_hash": comparable_hash(hash_params, adapter.inert_params),
        "run_config": {"warmup_passes": 1,
                       "warmup_concurrency": WARMUP_CONCURRENCY,
                       "repetitions": args.repetitions,
                       "duration_per_repetition_s": args.duration,
                       # A capped repetition is neither "the whole workload"
                       # nor open-ended, so record the cap that shaped it.
                       "requests_per_repetition": (
                           args.max_requests or None if args.duration
                           else len(workload)),
                       "max_requests": args.max_requests or None,
                       "warmup_requests": args.warmup_requests or None,
                       "concurrency": args.concurrency, "timeout_s": args.timeout,
                       "max_source_queries": args.max_queries,
                       "replay_driver": "bench_replay",
                       "replay_threads": args.threads,
                       "replay_order": args.order, "replay_seed": args.seed,
                       "replay_warmup_s": args.warmup_seconds},
        # The canonical serving snapshot lives in the context; the passing
        # cell records the live-compared verdict, and only the rare unstable
        # cell inlines both samples (it fails anyway, verbosity is fine).
        "index_topology": ({"stable": True} if topology_stable else
                           {"stable": False, "before": topology_before,
                            "after": topology_after}),
        # Warmup doubles as the validation phase (full parse + per-task payload
        # assertions); measured reps check status/shape markers only.
        "warmup": {"requests": len(warmup[0]), "elapsed_s": warmup[3], "errors": 0,
                   "payloads_validated": True},
        "repetitions": repetitions,
        "aggregate": aggregate,
        "memory": memory,
        # First request per replay bucket, captured from the exact serialized
        # bytes consumed by bench_replay. Reports render this artifact rather
        # than rebuilding a request through whatever adapter code exists later.
        "representative_requests": representative_requests,
        "count_samples": count_samples,
        "errors": all_errors[:100],
    }
    with open(output, "w", encoding="utf-8") as destination:
        json.dump(result, destination, indent=2, sort_keys=True)
        destination.write("\n")
    print(json.dumps({"output": str(output), "aggregate": aggregate, "memory": memory}, indent=2))
    return 1 if all_errors else 0


def main():
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser()
    parser.add_argument("engine", choices=ENGINES)
    parser.add_argument("task", choices=TASKS)
    parser.add_argument("--lane", choices=("exact", "skip"), required=True,
                        help="count_mode: exact totals vs each engine's shipping default")
    parser.add_argument("--set", action="append", metavar="KEY=VALUE",
                        help="override a preset parameter (repeatable); values parse as JSON "
                             "when possible. Overrides are recorded in param_overrides and "
                             "param_hash; report tooling surfaces undeclared differences.")
    parser.add_argument("--variant", default="",
                        help="declared deviations from the campaign baseline, as "
                             "comma-separated key=value (see variants.py). Client-layer "
                             "keys (concurrency, threads) override those flags; other "
                             "keys become preset overrides. '-' or empty = baseline.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int)
    parser.add_argument("--collection", default="searchbench")
    parser.add_argument("--server-pid", type=int, required=True)
    # SMT-clean split: the native replay loop plus sampler get one physical
    # core; servers get the rest of the process's allowed topology.
    parser.add_argument("--server-cores", default=DEFAULT_SERVER_CORES)
    parser.add_argument("--client-cores", default=DEFAULT_CLIENT_CORES)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--threads", type=int, default=1,
                        help="C++ replay event-loop threads (default: 1)")
    parser.add_argument("--repetitions", type=int, default=5)
    parser.add_argument("--duration", type=float, default=0.0)
    parser.add_argument("--order", choices=("seq", "shuffle"), default="seq")
    parser.add_argument("--seed", type=int,
                        help="required with --order shuffle; included in parameter identity")
    parser.add_argument("--warmup-seconds", type=float, default=1.0,
                        help="unrecorded C++ connection warmup after validation")
    parser.add_argument("--max-requests", type=int, default=0,
                        help="end each measured repetition after this many requests "
                             "(0 = unbounded); with --duration, whichever comes first")
    parser.add_argument("--warmup-requests", type=int, default=0,
                        help="bound the connection warmup by request count as well as "
                             "seconds; concurrency requests is one per socket")
    parser.add_argument("--mix-slots",
                        help="comma-separated non-MIX presets for a custom MIX composition")
    parser.add_argument("--max-queries", type=int, default=0)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--expected-segments", type=int,
                        help="require this many committed serving segments before and "
                             "after measurement")
    parser.add_argument("--topology-name", choices=tuple(TOPOLOGIES),
                        help="require and record this named complete segment layout")
    parser.add_argument(
        "--queries",
        default=str(root / "queries" / "luceneutil" / "queries.txt"))
    parser.add_argument("--corpus", default=str(root / "corpus" / "corpus-100k-searchbench.ndjson"))
    parser.add_argument("--label", default="baseline")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    try:
        args.variant = parse_variant(args.variant)
    except ValueError as error:
        parser.error(str(error))
    for key, value in client_settings(args.variant).items():
        setattr(args, key, value)
    if args.repetitions < 1:
        parser.error("--repetitions must be positive")
    if args.threads < 1 or args.threads > args.concurrency:
        parser.error("--threads must be in 1..concurrency")
    if args.order == "shuffle" and args.seed is None:
        parser.error("--seed is required with --order shuffle")
    if args.duration < 0 or args.warmup_seconds < 0:
        parser.error("--duration and --warmup-seconds must be non-negative")
    if args.expected_segments is not None and args.expected_segments < 1:
        parser.error("--expected-segments must be positive")
    if args.topology_name:
        declared = resolve_topology(args.topology_name)
        if (args.expected_segments is not None
                and args.expected_segments != declared.segment_count):
            parser.error(
                f"--expected-segments={args.expected_segments} conflicts with "
                f"{args.topology_name} ({declared.segment_count})")
        args.expected_segments = declared.segment_count
    args.port = args.port or DEFAULT_PORTS[args.engine]
    raise SystemExit(asyncio.run(async_main(args)))


if __name__ == "__main__":
    main()
