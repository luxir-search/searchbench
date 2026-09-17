#!/usr/bin/env python3
"""Render a compact cross-engine report from Searchbench result JSON.

Cells are keyed (engine, task, variant). When one variant is present the
report is a flat cross-engine table; when several are, the query tables pivot
on the variant axis. Configuration differences are never a reason to refuse
tabulation: declared differences (the variant) become columns, and undeclared
differences are surfaced as warnings from the recorded config itself.
"""

import argparse
from collections import defaultdict
import json
from pathlib import Path
import shlex
import sys

from adapters import ENGINES
from controls import health_control_lines
from presets import FULL_TEXT_TASKS, PRESETS, TASKS
from request_capture import captured_http_lines, request_shape_label
from results_io import load_result
import variants as variants_module

ROOT = Path(__file__).resolve().parents[1]

_MISSING = object()


def fmt(value, digits=2):
    return "N/A" if value is None else f"{value:.{digits}f}"


def fmt_bytes(value):
    if value is None:
        return "N/A"
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024 or unit == "TiB":
            return f"{value:.2f} {unit}" if unit != "B" else f"{value} B"
        value /= 1024


def fmt_memory_mib(memory, key):
    value = memory.get(key)
    return fmt(None if value is None else value / 1024)


def read_json(path):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def variant_key(variant):
    return json.dumps(variant or {}, sort_keys=True)


def variant_order_key(vkey):
    def rank(value):
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return (1, str(value))
        return (0, value)
    return [(key, rank(value)) for key, value in sorted(json.loads(vkey).items())]


def variant_label(vkey):
    return variants_module.slug(json.loads(vkey)) or "baseline"


def variant_description(vkey):
    variant = json.loads(vkey)
    if not variant:
        return "campaign defaults"
    return ", ".join(f"{key}={variant[key]}" for key in sorted(variant))


def load_results(directory):
    results = {}
    contexts = {}
    for path in Path(directory).glob("*.json"):
        try:
            value = load_result(path, contexts)
        except (OSError, json.JSONDecodeError) as error:
            # A dangling context reference must be loud, never a silently
            # emptier table row.
            if "-context-" not in path.name:
                print(f"skipping {path}: {error}", file=sys.stderr)
            continue
        engine = value.get("engine")
        task = value.get("task_class")
        if engine in ENGINES and task in PRESETS:
            results[(engine, task, variant_key(value.get("variant")))] = value
    return results


def ordered_variants(results):
    return sorted({vkey for (_engine, _task, vkey) in results}, key=variant_order_key)


def agreement(results, task, vkey):
    samples = [results.get((engine, task, vkey), {}).get("count_samples", {})
               for engine in ENGINES]
    if any(not sample for sample in samples):
        return 0, None
    common = set(samples[0])
    for sample in samples[1:]:
        common &= set(sample)
    mismatches = [key for key in sorted(common)
                  if len({sample[key] for sample in samples}) != 1]
    return len(common), mismatches


def ordered_tasks(results):
    present = {task for (_engine, task, _vkey) in results}
    order = FULL_TEXT_TASKS + tuple(task for task in TASKS if task not in FULL_TEXT_TASKS)
    return [task for task in order if task in present]


def representative_request_lines(results, observed):
    """Render actual first-per-bucket requests captured during serialization."""
    choices = {}
    vkeys = ordered_variants(results)
    for task in observed:
        for vkey in vkeys:
            for engine in ENGINES:
                result = results.get((engine, task, vkey), {})
                for bucket, capture in result.get("representative_requests", {}).items():
                    parameters = (capture.get("parameters")
                                  or result.get("task_parameters", {}))
                    label = request_shape_label(parameters)
                    choices.setdefault(label, (task, vkey, bucket))
    if not choices:
        return ["## Representative requests", "",
                "Representative requests were not captured in these legacy results."]

    lines = ["## Representative requests", "",
             "Each example is the first request of that shape actually serialized into "
             "the replay workload. The JSON body is pretty-printed for readability; "
             "the result JSON retains its original body, serialized headers, and the "
             "SHA-256 of the exact wire bytes."]
    for label, (task, vkey, bucket) in choices.items():
        source = f"`{task}`"
        if json.loads(vkey):
            source += f" (variant `{variant_label(vkey)}`)"
        lines += ["", f"### {label}", "", f"Source cell: {source}."]
        for engine in ENGINES:
            capture = (results.get((engine, task, vkey), {})
                       .get("representative_requests", {}).get(bucket))
            if capture is None:
                lines += ["", f"**{engine}:** not captured."]
                continue
            query = capture.get("query")
            query_text = f" Query: `{query}`." if query else ""
            lines += ["", f"**{engine}.**{query_text}", "", "```http",
                      *captured_http_lines(capture), "```", "",
                      f"Wire SHA-256: `{capture.get('wire_sha256', 'N/A')}`."]
    return lines


def corpus_lines(results):
    observed = [result for result in results.values() if result.get("corpus")]
    if not observed:
        return ["## Corpus and workload", "", "Corpus provenance was not recorded."]
    corpus = observed[0]["corpus"]
    cardinalities = corpus.get("facet_cardinalities", {})
    documents = cardinalities.get("documents")
    variant = cardinalities.get("variant")
    identity = (corpus.get("sha256"), documents, variant)
    inconsistent = any(
        (item["corpus"].get("sha256"),
         item["corpus"].get("facet_cardinalities", {}).get("documents"),
         item["corpus"].get("facet_cardinalities", {}).get("variant")) != identity
        for item in observed)
    path = Path(corpus.get("path", "unknown"))
    lines = ["## Corpus and workload", "",
             f"- Corpus: `{path.name}`, "
             f"{documents:,} documents" if documents is not None
             else f"- Corpus: `{path.name}`, document count not recorded"]
    lines[-1] += (f", {fmt_bytes(corpus.get('bytes'))}, variant `{variant}`, "
                  f"SHA-256 `{corpus.get('sha256')}`.")

    manifest = read_json(ROOT / "corpora" / "wikipedia" / "manifest.json")
    selection = read_json(ROOT / "queries" / "luceneutil" / "selection.json")
    source = read_json(ROOT / "queries" / "luceneutil" / "source.json")
    known_corpus = selection and any(
        lane.get("sha256") == corpus.get("sha256")
        for lane in selection.get("corpora", {}).values())
    if manifest and known_corpus:
        lines.append(
            f"- Corpus source: first {documents:,} line documents from "
            f"`{manifest['source_artifact']}` ({manifest['content_origin']}), "
            f"source SHA-256 `{manifest['source_sha256']}`.")

    query_files = [result.get("source_files", {}).get("queries")
                   for result in observed
                   if result.get("source_files", {}).get("queries")]
    if query_files:
        query_file = query_files[0]
        query_hash = query_file.get("sha256")
        query_count = (selection.get("selection", {}).get("queries")
                       if selection
                       and selection.get("selection", {}).get("sha256") == query_hash
                       else None)
        text = f"- Queries: `{Path(query_file.get('path', 'unknown')).name}`"
        if query_count is not None:
            text += f", {query_count:,} exact-agreement queries"
        text += f", SHA-256 `{query_hash}`"
        if source and query_count is not None:
            text += (f", derived from luceneutil `{source['file']}` at pinned commit "
                     f"`{source['commit']}`")
        lines.append(text + ".")
    if inconsistent:
        lines += ["", "> INVALID: result files do not all identify the same corpus."]
    return lines


def topology_snapshots(result):
    topology = result.get("index_topology")
    if not topology:
        return []
    return [sample for sample in (topology.get("before"), topology.get("after"))
            if sample]


def _distinct(engine_results, extract):
    values = sorted({extract(result) for result in engine_results
                     if extract(result) is not None}, key=repr)
    return ", ".join(str(value) for value in values) if values else "N/A"


def index_shape_lines(results):
    lines = ["## Index and run shape", "",
             "Segment topology is sampled immediately before and after every measured "
             "cell. A changed topology fails the cell.", "",
             "| Engine | Version | Documents | Index size | Shards x replicas | "
             "Segments observed | Topology | Concurrency | Heap |",
             "|---|---|---:|---:|---:|---|---|---:|---|"]
    stable_topologies = {}
    all_valid = True
    for engine in ENGINES:
        engine_results = [result for (cell_engine, _task, _vkey), result in results.items()
                          if cell_engine == engine]
        if not engine_results:
            continue
        first = engine_results[0]
        snapshots = [snapshot for result in engine_results
                     for snapshot in topology_snapshots(result)]
        if snapshots:
            encoded = {json.dumps(snapshot, sort_keys=True): snapshot
                       for snapshot in snapshots}
            counts = sorted({snapshot["segment_count"] for snapshot in snapshots})
            stable = (len(encoded) == 1
                      and all(result.get("index_topology", {}).get("stable") is True
                              for result in engine_results))
            status = "STABLE" if stable else "CHANGED"
            if stable:
                stable_topologies[engine] = next(iter(encoded.values()))
        else:
            legacy_counts = sorted({result.get("segments") for result in engine_results
                                    if result.get("segments") is not None})
            counts = legacy_counts
            status = "CHANGED (legacy count only)" if len(counts) > 1 else "NOT RECORDED"
            stable = False
        all_valid &= stable
        config = first.get("engine_config", {})
        corpus = first.get("corpus", {}).get("facet_cardinalities", {})
        segment_text = ", ".join(str(count) for count in counts) if counts else "N/A"
        # Serving size is only meaningful for one agreed topology: the sum of
        # per-segment on-disk bytes, the same accounting for every engine.
        size = stable_topologies.get(engine, {}).get("size_bytes")
        lines.append(
            f"| {engine} | {str(config.get('version', 'N/A'))[:12]} "
            f"| {corpus.get('documents', 'N/A')} | {fmt_bytes(size)} "
            f"| {config.get('shards', 'N/A')} x {config.get('replicas', 'N/A')} "
            f"| {segment_text} | {status} "
            f"| {_distinct(engine_results, lambda r: r.get('run_config', {}).get('concurrency'))} "
            f"| {_distinct(engine_results, lambda r: r.get('engine_config', {}).get('heap_max'))} |")

    if not all_valid:
        lines += ["",
                  "> INVALID QUERY BOARD: serving segment topology was not stable and "
                  "fully recorded for every engine. Results below must not be used as "
                  "a controlled cross-engine comparison."]
    if stable_topologies:
        lines += ["", "### Serving segments", "",
                  "| Engine | Shard | Segment | Max docs | Live docs | Deleted docs | Bytes |",
                  "|---|---:|---|---:|---:|---:|---:|"]
        for engine in ENGINES:
            topology = stable_topologies.get(engine)
            if not topology:
                continue
            for segment in topology["segments"]:
                lines.append(
                    f"| {engine} | {segment['shard']} | `{segment['id']}` "
                    f"| {segment['max_docs']:,} | {segment['live_docs']:,} "
                    f"| {segment['deleted_docs']:,} | {fmt_bytes(segment.get('size_bytes'))} |")
    return lines


def _startup_identity(startup):
    return json.dumps({key: value for key, value in startup.items()
                       if key not in ("pid", "source")}, sort_keys=True)


def _argv_lines(argv):
    if not argv:
        return ["N/A"]
    quoted = [shlex.quote(value) for value in argv]
    lines = [quoted[0]]
    for value in quoted[1:]:
        lines[-1] += " \\"
        lines.append(f"  {value}")
    return lines


def startup_lines(results):
    """Show the effective process and startup overlays captured from /proc."""
    lines = ["## Engine startup", "",
             "The effective live server process is captured from `/proc` during each "
             "cell. This shows the JVM after its launcher has expanded options; "
             "affinity and open-file limits are the observed process values."]
    for engine in ENGINES:
        engine_results = [result for (cell_engine, _task, _vkey), result in results.items()
                          if cell_engine == engine]
        if not engine_results:
            continue
        postures = {}
        for result in engine_results:
            config = result.get("engine_config", {})
            startup = config.get("startup")
            if startup:
                declared = variants_module.server_settings(result.get("variant") or {})
                postures.setdefault(_startup_identity(startup),
                                    (startup, config, declared))
        lines += ["", f"### {engine}", ""]
        if not postures:
            lines.append("Startup parameters were not captured in these legacy results.")
            continue
        # Distinct startups are expected when server-posture variants declare
        # them; only startups no variant accounts for are drift.
        declared_counts = defaultdict(int)
        for (_startup, _config, declared) in postures.values():
            declared_counts[json.dumps(declared, sort_keys=True)] += 1
        undeclared = sum(count - 1 for count in declared_counts.values())
        if undeclared:
            lines += [f"> WARNING: {undeclared + 1} distinct startup postures occur "
                      "without a declaring variant.", ""]
        for posture_index, (startup, config, declared) in enumerate(
                postures.values(), 1):
            if len(postures) > 1:
                label = variants_module.slug(declared) or "baseline"
                lines += [f"#### {label} ({posture_index})", ""]
            lines += [
                f"- Executable: `{startup.get('executable', 'N/A')}`",
                f"- Working directory: `{startup.get('working_directory', 'N/A')}`",
                f"- Observed CPU affinity: `{config.get('observed_cpus_allowed_list', 'N/A')}`",
                f"- Observed `nofile`: soft `{config.get('nofile_soft', 'N/A')}`, "
                f"hard `{config.get('nofile_hard', 'N/A')}`",
                f"- Exact cmdline SHA-256: `{startup.get('cmdline_sha256', 'N/A')}`",
                "", "<details>",
                f"<summary>Effective argv ({len(startup.get('argv', ())) } arguments)</summary>",
                "", "```sh", *_argv_lines(startup.get("argv", ())), "```", "",
                "</details>",
            ]
            environment = startup.get("environment", {})
            if environment:
                lines += ["", "Relevant environment:", "", "```text",
                          *(f"{key}={value}" for key, value in environment.items()),
                          "```"]
            for overlay in startup.get("config_overlays", ()):
                lines += ["", f"`{overlay['relative_path']}` "
                          f"(SHA-256 `{overlay['sha256']}`):", "", "```text",
                          overlay.get("content", "").rstrip("\n"), "```"]
    return lines


# run_config knobs a variant may declare, mapped to their recorded names.
CLIENT_KNOBS = {"concurrency": "concurrency", "threads": "replay_threads"}
# run_config knobs that are campaign posture and never variant-declared (yet).
CAMPAIGN_KNOBS = ("duration_per_repetition_s", "repetitions",
                  "replay_order", "replay_seed")


def undeclared_variation(results):
    """Differences the recorded configs show that no variant declares.

    This is the sanity check that replaced hash-gated tabulation: a hash can
    only refuse; the recorded values can say exactly what drifted.
    """
    notes = []
    residual = defaultdict(set)
    for (_engine, _task, vkey), result in results.items():
        declared = json.loads(vkey)
        run = result.get("run_config", {})
        for vname, rname in CLIENT_KNOBS.items():
            if vname not in declared:
                residual[rname].add(run.get(rname))
        for rname in CAMPAIGN_KNOBS:
            residual[rname].add(run.get(rname))
        # Namespace IDs are ephemeral; the network setup is the comparable
        # property. Historical results did not record it at all.
        residual["network_mode"].add(
            result.get("host", {}).get("network", {}).get("mode", "unrecorded"))
    for rname in sorted(residual):
        if len(residual[rname]) > 1:
            values = ", ".join(str(v) for v in sorted(residual[rname], key=repr))
            notes.append(f"`{rname}` varies without a declaring variant: {values}")

    for engine in ENGINES:
        heaps = {result.get("engine_config", {}).get("heap_max")
                 for (cell_engine, _task, vkey), result in results.items()
                 if cell_engine == engine and "heap" not in json.loads(vkey)}
        if len(heaps) > 1:
            values = ", ".join(str(v) for v in sorted(heaps, key=repr))
            notes.append(f"{engine} `heap_max` varies without a declaring variant: {values}")

    groups = defaultdict(list)
    for (engine, task, vkey), result in results.items():
        groups[(engine, task)].append((json.loads(vkey), result))
    for (engine, task), members in sorted(groups.items()):
        declared_params = set()
        for variant, _result in members:
            declared_params |= set(variants_module.param_settings(variant))
        projections = [
            {key: value for key, value in (result.get("task_parameters") or {}).items()
             if key not in declared_params}
            for _variant, result in members]
        baseline_projection = projections[0]
        differing = sorted({
            key for projection in projections[1:]
            for key in set(baseline_projection) | set(projection)
            if projection.get(key, _MISSING) != baseline_projection.get(key, _MISSING)})
        if differing:
            notes.append(f"{engine} `{task}`: parameters differ across cells "
                         f"without a declaring variant: {', '.join(differing)}")
    return notes


def _core_count(spec):
    if spec is None:
        return None
    try:
        count = 0
        for part in str(spec).split(","):
            bounds = part.split("-", 1)
            count += 1 if len(bounds) == 1 else int(bounds[1]) - int(bounds[0]) + 1
        return count or None
    except ValueError:
        return None


def client_saturation(results):
    """Cells whose replay threads ran near saturation: the recorded QPS is a
    client ceiling, not an engine measurement."""
    notes = []
    for (engine, task, vkey) in sorted(results):
        result = results[(engine, task, vkey)]
        busy = (result.get("aggregate", {}).get("client_cpu") or {}).get("cores_busy")
        if busy is None:
            continue
        threads = result.get("run_config", {}).get("replay_threads") or 1
        cores = _core_count(result.get("core_split", {}).get("client"))
        ceiling = min(threads, cores) if cores else threads
        if busy >= 0.85 * ceiling:
            label = f" @{variant_label(vkey)}" if json.loads(vkey) else ""
            notes.append(
                f"{engine} `{task}`{label}: replay client busy {busy} of "
                f"{ceiling} thread(s); the cell may be client-bound "
                f"(raise the `threads` variant or discount its QPS)")
    return notes


QUERY_TABLE_PREAMBLE = [
    "`Peak total RSS` includes resident file-backed mappings. `Peak anon RSS` "
    "approximates resident heap, allocator arenas, stacks, and other anonymous "
    "allocations."]


def flat_query_lines(results, observed, vkey, failures):
    lines = ["## Query results", "", *QUERY_TABLE_PREAMBLE, ""]
    if json.loads(vkey):
        lines[-1:] = ["", f"Every cell declares variant `{variant_label(vkey)}` "
                      f"({variant_description(vkey)}).", ""]
    lines += [
        "| Task | Engine | p50 ms | p90 ms | p99 ms | QPS | CPU ms/request | "
        "Peak total RSS MiB | Peak anon RSS MiB | Status |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---|"]
    for task in observed:
        for engine in ENGINES:
            result = results.get((engine, task, vkey))
            if not result or "aggregate" not in result:
                status = result.get("failure", "missing") if result else "missing"
                lines.append(
                    f"| {task} | {engine} | N/A | N/A | N/A | N/A | N/A | N/A | N/A | {status} |")
                failures[engine].add(status)
                continue
            aggregate = result["aggregate"]
            latency = aggregate["latency_ms"]
            memory = result.get("memory", {})
            server_cpu = aggregate.get("server_cpu", {})
            lines.append(
                f"| {task} | {engine} | {fmt(latency.get('p50'))} | "
                f"{fmt(latency.get('p90'))} | {fmt(latency.get('p99'))} | "
                f"{fmt(aggregate.get('qps'), 0)} | "
                f"{fmt(server_cpu.get('cpu_ms_per_request'), 3)} | "
                f"{fmt_memory_mib(memory, 'peak_vmrss_kb')} | "
                f"{fmt_memory_mib(memory, 'peak_rssanon_kb')} | "
                f"{'PASS' if not result.get('errors') else 'FAIL'} |")
    return lines


def pivot_query_lines(results, observed, vkeys, failures):
    """One column group per variant; the first variant is the ratio base."""
    labels = {vkey: variant_label(vkey) for vkey in vkeys}
    lines = ["## Variants", "",
             "Each declared variant is one column group in the tables below. "
             f"QPS ratios are relative to `{labels[vkeys[0]]}`.", ""]
    lines += [f"- `{labels[vkey]}`: {variant_description(vkey)}" for vkey in vkeys]

    def cells(extract, fmt_cell):
        rows = []
        for task in observed:
            for engine in ENGINES:
                if not any((engine, task, vkey) in results for vkey in vkeys):
                    continue
                row = [task, engine]
                base = None
                for vkey in vkeys:
                    result = results.get((engine, task, vkey))
                    if not result or "aggregate" not in result:
                        status = result.get("failure", "missing") if result else "missing"
                        failures[engine].add(status)
                        row += ["N/A"] * len(fmt_cell(None, None))
                        continue
                    if result.get("errors"):
                        failures[engine].add("errors recorded")
                    value = extract(result)
                    row += fmt_cell(value, base)
                    if base is None:
                        base = value
                rows.append(row)
        return rows

    def latency_cell(value, base):
        if value is None:
            return ["", "", ""]
        latency, qps, errored = value
        qps_text = fmt(qps, 0)
        if base is not None and base[1] and qps is not None:
            qps_text += f" ({qps / base[1]:.2f}x)"
        if errored:
            qps_text += " FAIL"
        return [fmt(latency.get("p50")), fmt(latency.get("p99")), qps_text]

    def cost_cell(value, _base):
        if value is None:
            return ["", "", ""]
        server_cpu, memory = value
        return [fmt(server_cpu.get("cpu_ms_per_request"), 3),
                fmt(server_cpu.get("cores_busy")),
                fmt_memory_mib(memory, "peak_rssanon_kb")]

    lines += ["", "### Latency and throughput", "",
              "| Task | Engine |" + "".join(
                  f" {labels[vkey]} p50 | {labels[vkey]} p99 | {labels[vkey]} QPS |"
                  for vkey in vkeys),
              "|---|---|" + "---:|" * (3 * len(vkeys))]
    for row in cells(lambda r: (r["aggregate"]["latency_ms"], r["aggregate"].get("qps"),
                                bool(r.get("errors"))),
                     latency_cell):
        lines.append("| " + " | ".join(row) + " |")

    lines += ["", "### Server cost", "",
              "`cores busy` is measured server CPU over wall clock: the "
              "parallelism the engine actually used, background work included.", "",
              "| Task | Engine |" + "".join(
                  f" {labels[vkey]} CPU ms/req | {labels[vkey]} cores busy "
                  f"| {labels[vkey]} anon MiB |"
                  for vkey in vkeys),
              "|---|---|" + "---:|" * (3 * len(vkeys))]
    for row in cells(lambda r: (r["aggregate"].get("server_cpu", {}), r.get("memory", {})),
                     cost_cell):
        lines.append("| " + " | ".join(row) + " |")
    return lines


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("directory")
    parser.add_argument("--output")
    parser.add_argument("--title", default="Searchbench report")
    args = parser.parse_args()
    results = load_results(args.directory)
    observed = ordered_tasks(results)
    vkeys = ordered_variants(results)
    failures = defaultdict(set)

    lines = [f"# {args.title}", "",
             "The same corpus, resolved task parameters, replay driver, and HTTP transport "
             "are used for Luxir, OpenSearch, and Elasticsearch.", ""]
    lines += corpus_lines(results)
    if any(engine == "luxir" for engine, _task, _variant in results):
        lines += [""] + health_control_lines(args.directory, queries=list(results.values()))
    drift = undeclared_variation(results)
    if drift:
        lines += ["", "## Undeclared variation", "",
                  "Recorded configuration differs between cells in ways no variant "
                  "declares. Rows sharing an undeclared difference are not one "
                  "controlled comparison."]
        lines += [""] + [f"> WARNING: {note}" for note in drift]
    saturated = client_saturation(results)
    if saturated:
        lines += ["", "## Client saturation", ""]
        lines += [f"> WARNING: {note}" for note in saturated]
    lines += [""] + index_shape_lines(results)
    lines += [""] + startup_lines(results)
    lines += [""]
    if len(vkeys) > 1:
        lines += pivot_query_lines(results, observed, vkeys, failures)
    else:
        single = vkeys[0] if vkeys else variant_key({})
        lines += flat_query_lines(results, observed, single, failures)

    lines += [""] + representative_request_lines(results, observed)

    lines += ["", "## Exact-count agreement", "",
              "Agreement compares every query key returned by all three engines. "
              "Normal baselines use the canonical exact-agreement set; query-selection "
              "campaigns intentionally run the unfiltered source and report its "
              "mismatches.", "",
              "| Task | Compared | Mismatches |",
              "|---|---:|---:|"]
    for task in observed:
        for vkey in vkeys:
            if not any((engine, task, vkey) in results for engine in ENGINES):
                continue
            compared, mismatches = agreement(results, task, vkey)
            name = task if len(vkeys) == 1 else f"{task} @{variant_label(vkey)}"
            lines.append(f"| {name} | {compared} | "
                         f"{'N/A' if mismatches is None else len(mismatches)} |")
            if mismatches:
                lines.append(f"\nMismatched keys for `{name}`: `{mismatches[:10]}`")

    cache_explicit = bool(results) and all(
        result.get("task_parameters", {}).get("request_cache") is False
        for result in results.values())
    cache_line = (
        "- The REST shard request cache is explicitly disabled in every cell."
        if cache_explicit else
        "- Legacy result: the REST shard request-cache switch was not explicit "
        "in every cell.")
    lines += ["", "## Posture", "",
              "- Full-text cells retain the workload query class in every result.",
              "- Exact and skip count modes are separate parameter identities.",
              cache_line,
              "- OpenSearch and Elasticsearch use bundled JDKs with fixed heaps.",
              "- A warmup pass fully validates payloads before measured replay."]
    if failures:
        lines += ["", "## Failures", ""]
        for engine in ENGINES:
            for failure in sorted(failures.get(engine, ())):
                lines.append(f"- {engine}: {failure}")

    text = "\n".join(lines) + "\n"
    if args.output:
        Path(args.output).write_text(text, encoding="utf-8")
    else:
        print(text, end="")


if __name__ == "__main__":
    main()
