#!/usr/bin/env python3
"""Run and render a fixed selectivity x field grid.

One GRID is one operation swept over the same domain matrix (presets.GRIDS):
`facet` counts buckets, `sort10`/`sort10k` rank by a field at two depths.
Rows, corpus, domains, warmup, probing and reporting are shared - the grids
differ only in which parameter the column axis writes and what a cell pins
around it, so a new grid is a spec entry rather than another runner.
"""

import argparse
import asyncio
import json
import os
from pathlib import Path
import subprocess
import sys

from adapters import ENGINES, facet_metrics, facet_order, make_adapter
from cpu_layout import current_cpu_sets, format_cpu_list
from controls import health_control_lines, record_health_control
from presets import (GRID_NAMES, GRID_SELECTIVITIES, GRIDS, NUMERIC_COLUMNS,
                     STRING_COLUMNS, designated_values, grid_columns, param_hash,
                     resolve, resolve_grid_cell)
from request_capture import captured_http_lines
from results_io import load_result
from schema import index_layout_version

DEFAULT_SERVER_CPUS, _DEFAULT_CLIENT_CPUS = current_cpu_sets()
DEFAULT_SERVER_CORES = format_cpu_list(DEFAULT_SERVER_CPUS)

# What a sort column means, and what a sort cell does and does not isolate.
SORT_NOTES = [
    "- **Column cardinality is tie density, not bucket count.** Every cell "
    "compares the same number of documents (the row's domain - field sort "
    "prunes nothing); what changes is how often a comparison wins. At 10 "
    "distinct values over 10M documents the queue fills with the smallest value "
    "immediately and nearly every later document loses on its first compare, "
    "while a high-cardinality uniform field keeps displacing it. The low "
    "columns are the cheap end of a sort, the opposite of what they are for a "
    "facet.",
    "- **The `n` columns are ints, and that is a different code path, not just "
    "a different cardinality.** Luxir compares raw int64 keys in bulk windows "
    "for numeric sorts and per-document ordinals (one virtual call each) for "
    "string sorts; Lucene can skip whole numeric blocks through points when no "
    "total is requested and has no equivalent for a keyword sort. Read "
    "string-vs-int as an engine-capability comparison, not a cardinality one, "
    "and check `total_hits` in the posture before doing so - it is what "
    "enables the Lucene skip.",
    "- **The deep grid measures sort plus id materialization; the shallow grid "
    "is the closer read on sorting alone.** At limit=10,000 both engines pay "
    "for 10,000 ids: luxir reads the id column (one ordinal lookup, one "
    "term-dictionary seek and one copy per hit) and streams the result in "
    "batches (`batch_size`, default 100, server cap 256); ES/OS read the "
    "custom `id` keyword's doc values. Same cell across the two grids brackets "
    "what depth "
    "costs; if a deep row looks wrong, check batching before blaming the sort.",
    "- No scores are computed anywhere in these grids: the domain is match_all "
    "and the sort is an explicit field, so every engine skips scoring.",
]

# How each grid describes itself in its rendered report. Prose only; the
# parameters live in presets.GRIDS.
def _facet_doc(payload):
    """Report prose for one member of the facet family."""
    return {
        "title": "Facet selectivity x cardinality grid" + (f" ({payload})" if payload else ""),
        "summary": "Cells are `QPS / p50 ms`; facet_limit is 10. Every grid in "
                   "the facet family sweeps the same domains, cardinalities and "
                   "posture, so the same cell of two of them differs only by the "
                   "per-bucket payload - the delta between them IS its cost.",
        "columns_are": "the faceted field",
        "notes": [
            "- The 100% row is an unfiltered domain. Read it separately from "
            "filtered rows because an engine may answer it from global field "
            "statistics rather than collecting matching documents.",
        ],
    }


GRID_DOC = {
    "facet": _facet_doc(None),
    "facet-metric": _facet_doc("metric"),
    "facet-metrics": _facet_doc("metrics"),
    "facet-metric-sort": _facet_doc("metric-sort"),
    "facet-metrics-sort": _facet_doc("metrics-sort"),
    "facet-selected": {
        "title": "Facet selectivity x cardinality grid (selected values pinned)",
        "summary": "Cells are `QPS / p50 ms`; facet_limit is 10, and every "
                   "request additionally selects the column field's designated "
                   "head and tail values (exact-count values the corpus "
                   "transform records per field). Read each cell against the "
                   "same cell of the plain facet grid - the delta section "
                   "below renders that read: selection refines sideways - "
                   "documents, never the facet's own counts - so the counting "
                   "work is identical and the delta between the two grids IS "
                   "the cost of selection: value-to-ord lookup, merging the "
                   "head pin into the natural page, appending the out-of-page "
                   "tail pin with its exact count.",
        "columns_are": "the faceted field",
        "notes": [
            "- **The reasonableness bar.** The legitimate extra work is a "
            "per-request constant (two dictionary lookups, two pinned "
            "buckets), so the delta vs the plain facet grid should be flat "
            "down the selectivity rows and near-flat across cardinality. A "
            "delta that grows with the row's domain means the engine is "
            "paying for the refined document result nothing in the request "
            "reads (limit=0, totals off).",
            "- The untimed 100% probe asserts exactness before any cell "
            "runs: each pinned bucket must carry its designated whole-corpus "
            "count and the refined total must equal their sum.",
            "- luxir-only: the REST adapters refuse facet_selected rather "
            "than emulate it (post_filter plus a filters agg would measure a "
            "different mechanism under the same cell name).",
        ],
    },
    "facet-top10": {
        "title": "Facet selectivity x cardinality grid (top 10 ids retrieved)",
        "summary": "Cells are `QPS / p50 ms`; facet_limit is 10 and the top 10 "
                   "matching ids are retrieved, so a cell is the plain facet "
                   "cell plus a live document result. The delta section against "
                   "the plain facet grid prices retrieval itself.",
        "columns_are": "the faceted field",
        "notes": [
            "- Exists as the base for facet-selected-top10: with a document "
            "result to refine, a facet selection cannot be elided, so that "
            "pair measures selection where the sideways machinery actually "
            "runs.",
        ],
    },
    "facet-selected-top10": {
        "title": "Facet selectivity x cardinality grid (selected, top 10 ids)",
        "summary": "Cells are `QPS / p50 ms`; the facet-selected request with "
                   "the top 10 matching ids retrieved. The document result "
                   "consumes the selection, so the refiner is live: the delta "
                   "section against facet-top10 is the cost of CONSUMED "
                   "selection - producing the refined domain plus pin "
                   "bookkeeping - where facet-selected's delta showed the "
                   "unconsumed (elided) posture.",
        "columns_are": "the faceted field",
        "notes": [
            "- The grid cycles identical requests, so selection-filter cache "
            "entries are hot after warmup: this measures the resident path. "
            "Cold-key cost (a value's first sighting) is invisible here by "
            "construction and needs a varying-selection workload.",
            "- luxir-only, same as facet-selected.",
        ],
    },
    "sort10": {
        "title": "Sort selectivity x field grid, top 10",
        "summary": "Cells are `QPS / p50 ms`; top 10 documents, ids only.",
        "columns_are": "the sort key",
        "notes": SORT_NOTES,
    },
    "sort10k": {
        "title": "Sort selectivity x field grid, top 10,000",
        "summary": "Cells are `QPS / p50 ms`; top 10,000 documents, ids only.",
        "columns_are": "the sort key",
        "notes": SORT_NOTES,
    },
}

NUMERIC_FIELDS = frozenset(field for _label, field, _n, _r in NUMERIC_COLUMNS)


def parse_engines(text):
    engines = text.split(",")
    unknown = [engine for engine in engines if engine not in ENGINES]
    if unknown:
        raise ValueError(f"unknown engines: {unknown}")
    return engines


def cell_path(outdir, engine, selectivity, cardinality, params):
    # No grid in the name: each grid owns its result tree (results/<grid>-grid),
    # so one grid's glob can never pick up another's cells.
    sel_slug = selectivity.replace(".", "p")
    card_slug = cardinality.lower()
    name = f"{engine}-sel{sel_slug}-card{card_slug}+{param_hash(params)}.json"
    return Path(outdir) / name


def report_name(grid):
    """Rendered report filename for a grid (FACET_GRID.md for `facet`)."""
    return f"{grid.upper().replace('-', '_')}_GRID.md"


def payload_summary(params):
    """One clause describing what a facet cell asks for per bucket."""
    metrics = facet_metrics(params)
    if not metrics:
        return "bucket counts only"
    listed = ", ".join(f'{metric["op"]}({metric["field"]})' for metric in metrics)
    order = facet_order(params)
    ordering = (f"buckets ordered by {order[0]} {order[1]}" if order
                else "buckets ordered by count desc")
    return f"{len(metrics)} per-bucket metric{'s' if len(metrics) > 1 else ''} ({listed}); {ordering}"


def load_cardinality_report(corpus, expected_documents=None):
    corpus = Path(corpus)
    path = Path(str(corpus) + ".cardinalities.json")
    if not path.exists():
        raise RuntimeError(
            f"missing {path}; re-transform with --cardinality-report {path}")
    report = json.loads(path.read_text(encoding="utf-8"))
    if (report.get("schema_version") != 3
            or report.get("variant") not in ("full", "facet")):
        raise RuntimeError(f"{path}: unsupported or non-grid corpus report")
    if expected_documents is not None and report.get("documents") != expected_documents:
        raise RuntimeError(
            f"{path} covers {report.get('documents')} docs, expected {expected_documents}")
    checksum_path = Path(str(corpus) + ".sha256")
    if not checksum_path.exists():
        raise RuntimeError(f"missing {checksum_path}; hash the transformed corpus first")
    checksum = checksum_path.read_text(encoding="utf-8").split()[0]
    if report.get("corpus_sha256") != checksum:
        raise RuntimeError(f"{path} does not describe {corpus} (SHA-256 differs)")
    # The whole string ladder, whichever grid is running: this is the corpus
    # identity check, not a per-grid one.
    for _label, field, nominal, _realized in STRING_COLUMNS:
        entry = report.get("fields", {}).get(field, {})
        if entry.get("nominal_cardinality") != nominal:
            raise RuntimeError(f"{path}: missing or invalid cardinality metadata for {field}")
        if entry.get("distribution") not in ("zipf", "uniform"):
            raise RuntimeError(f"{path}: {field} has unknown distribution")
        realized = entry.get("realized_cardinality")
        if not isinstance(realized, int) or not 0 < realized <= nominal:
            raise RuntimeError(f"{path}: invalid realized cardinality for {field}")
    for _label, field, _nominal, realized in NUMERIC_COLUMNS:
        if realized is not None:
            continue
        entry = report.get("fields", {}).get(field, {})
        reported = entry.get("realized_cardinality")
        if (entry.get("nominal_cardinality") != report["documents"]
                or reported != report["documents"]):
            raise RuntimeError(f"{path}: invalid realized cardinality for {field}")
    return report


def probe_request(adapter, grid, field, cardinality, report=None):
    """(params, request) for one column's pre-flight check; params is None for
    the adapter's own field probe, which validates itself."""
    probe = GRIDS[grid]["probe"]
    if probe == "field_dict":
        return None, adapter.build_field_probe(field)
    from query_source import QueryItem
    item = QueryItem("(match_all; grid cells carry no query text)", "union", (), "grid")
    # Totals on: a probe is untimed, so it can ask for the answer the cells
    # deliberately do not (see resolve_grid_cell). The sort probe also asks
    # for one document back, to prove the engine ranked by the field; the
    # selected-cell probe keeps the cell's own limit=0 shape - its evidence
    # is the pinned buckets and the refined total, not a document.
    params = dict(resolve_grid_cell(grid, "100", cardinality, "exact", report=report),
                  total_hits=True, name=f"{grid} probe {field}")
    if probe == "cell":
        params["limit"] = 1
    return params, adapter.build(params, item)


def probe_field(engine, host, port, collection, grid, field, cardinality, report=None):
    """Prove a column is usable by this grid before any of its cells run.

    Two instruments, one per grid kind, because "usable" is not the same
    property. A facet grid needs the field's values to be there, and each
    adapter's build_field_probe asks through the engine's native field-facet
    path so a stored column cannot cover for a missing indexed field. A
    sort grid needs the engine to accept the field as a sort KEY, which a
    facet probe does not test and which is the thing most likely to be
    missing - so it sends the cell's own request, shallow and with the total
    turned on, and requires documents and a non-zero count back.

    Both go through the adapter's own HTTP transport rather than a hand-rolled
    call, so validation and measured replay use identical framing.
    """
    adapter = make_adapter(engine, collection)
    params, request = probe_request(adapter, grid, field, cardinality, report)

    async def run():
        connection = adapter.connect(host, port, 120)
        try:
            return await connection.request(request)
        finally:
            await connection.close()

    try:
        raw = asyncio.run(run())
    except Exception as error:
        raise RuntimeError(f"{engine} probe for {field} failed: {error}") from error
    if params is None:
        return adapter.validate_field_probe(raw, field)
    count = adapter.validate(params, raw)
    if count is None or count <= 0:
        raise RuntimeError(f"{grid} probe {field}: index is empty or returned no count")
    labels = GRIDS[grid].get("selected")
    if labels:
        expected = {entry["value"]: entry["count"] for entry in
                    designated_values(report, field, labels).values()}
        adapter.validate_selected_probe(params, raw, expected)
    return count


def column_cardinality(column, report):
    """Realized distinct values for a column: the corpus report, or the value
    the column states for a field the report does not cover."""
    _label, field, _nominal, realized = column
    if realized is not None:
        return realized
    return report["fields"][field]["realized_cardinality"]


def warm_engine(engine, host, port, collection, grid, lane, max_parallel,
                selectivities, cardinalities, passes, concurrency, report=None):
    """Exercise every shape the leg will measure, before measuring any of it.

    Without this the first cell after an engine start is measurably
    pessimistic, and the grid is selectivity-major so it is always the same
    cell (100%, highest cardinality) that pays. Measured on a freshly started
    Elasticsearch: the unfiltered 2Mu cell ran 32.8 qps when it was the first
    shape the JVM saw and 37.6 when an earlier cell had run first (0.87x), and
    the filtered cell 36.1 against 38.7 (0.93x). Both shapes are FASTER second,
    so this is not one shape's profile crowding out another's - it is
    JVM-wide warmup (shared paths, allocation, GC ergonomics) that whichever
    cell goes first pays on everyone else's behalf.

    Bounded by PASSES over the shape list, not by seconds, so the warmup is
    proportional to the grid rather than to how slow the engine is: two passes
    over 42 shapes is 84 requests whether they take a second or a minute. It
    is also what makes coverage unconditional - no shape can be skipped
    because a budget ran out inside the slow ones.

    Not JVM-specific. Luxir also instantiates per-field structures on first use
    and faults in mmapped pages. Every engine gets the same pass.

    WHAT THIS COSTS, and why it is still right. Warming across shapes leaves a
    JVM's call sites polymorphic, and HotSpot does not walk that back: a site
    that specialized, deoptimized when a second shape arrived, and went generic
    stays generic. Measured on Elasticsearch at 1%/1M - a fresh JVM that saw
    ONLY that shape settled at 3,666 qps; after warming across all 42 shapes it
    settled at 3,309, a stable 0.90x rather than a still-climbing one.
    So this warmup does NOT make the JVMs look their best.
    It is still correct for this grid, because the grid measures 42 shapes in
    one process: every cell from roughly the second onward is already in the
    generic regime, and without this pass the only cell escaping it is the
    first one. The choice is between all cells consistent and one cell
    flattering. Consistency wins; the caveat is recorded in the rendered
    report, since it means a JVM number here is mixed-traffic steady state and
    understates a single-shape deployment by ~10%. It does not apply to the
    C++ engines, which have no equivalent to lose.

    Not recorded and not sampled - it runs before any cell's driver starts, so
    it cannot enter a measurement or a memory peak.
    """
    from query_source import QueryItem
    adapter = make_adapter(engine, collection)
    item = QueryItem("(match_all; grid cells carry no query text)", "union", (), "grid")
    shapes = []
    for selectivity, cardinality, _field in grid_cells(grid, selectivities, cardinalities):
        params = resolve_grid_cell(grid, selectivity, cardinality, lane, max_parallel,
                                   report=report)
        shapes.append(adapter.build(dict(params, name="warm"), item))
    if not shapes or passes <= 0:
        return 0

    async def run():
        # Every shape, `passes` times, shared out across the workers - a work
        # queue rather than a time budget. Bounding by passes instead of
        # seconds keeps the warmup proportional to the grid rather than to how
        # slow the engine is, which is what a dev-tier leg needs: two passes
        # over 42 shapes is 84 requests whether the engine serves them in a
        # second or a minute.
        queue = asyncio.Queue()
        for _ in range(passes):
            for index in range(len(shapes)):
                queue.put_nowait(index)
        sent = 0

        async def worker():
            nonlocal sent
            connection = adapter.connect(host, port, 120)
            try:
                while True:
                    try:
                        which = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                    await connection.request(shapes[which])
                    sent += 1
            finally:
                await connection.close()

        workers = max(1, min(concurrency, len(shapes)))
        await asyncio.gather(*(worker() for _ in range(workers)),
                             return_exceptions=True)
        return sent

    return asyncio.run(run())


def rendered_columns(grid, cardinalities=None):
    """Columns to render, in the requested order.

    Defaults to the grid's own column set; naming extra rungs with -c renders
    those instead, so a sub-grid over `100` (or `10K`) has somewhere to appear
    rather than measuring cells no table can show.
    """
    by_label = {column[0]: column for column in grid_columns(grid)}
    if cardinalities in (None, "all"):
        return list(GRIDS[grid]["columns"])
    labels = (cardinalities.split(",") if isinstance(cardinalities, str)
              else list(cardinalities))
    unknown = [label for label in labels if label not in by_label]
    if unknown:
        raise ValueError(f"unknown {grid} columns: {unknown}")
    return [by_label[label] for label in labels]


def grid_cells(grid, selectivities=None, cardinalities=None):
    """(selectivity, column_label, field) for the selected sub-grid."""
    sels = GRID_SELECTIVITIES if selectivities in (None, "all") else selectivities.split(",")
    unknown = [s for s in sels if s not in GRID_SELECTIVITIES]
    if unknown:
        raise ValueError(f"unknown selectivities: {unknown}")
    by_label = {label: field for label, field, _nominal, _realized in grid_columns(grid)}
    cards = ([label for label, _f, _n, _r in GRIDS[grid]["columns"]]
             if cardinalities in (None, "all") else cardinalities.split(","))
    unknown = [c for c in cards if c not in by_label]
    if unknown:
        raise ValueError(f"unknown {grid} columns: {unknown}")
    for selectivity in sels:
        for cardinality in cards:
            yield selectivity, cardinality, by_label[cardinality]


def archive_results(outdir, engines, grid, lane, selectivities=None, cardinalities=None,
                    max_parallel=1, report=None):
    for engine in engines:
        for selectivity, cardinality, _field in grid_cells(grid, selectivities, cardinalities):
            params = resolve_grid_cell(grid, selectivity, cardinality, lane, max_parallel,
                                       report=report)
            output = cell_path(outdir, engine, selectivity, cardinality, params)
            if output.exists():
                os.replace(output, Path(str(output) + ".prev"))


def grid_overrides(grid, params, lane):
    """`--set k=v` flags that turn the grid's base preset into this cell.

    Derived from resolve_grid_cell's own output rather than hand-listed. The
    two used to be maintained separately, so adding limit=0 and
    request_cache=false to the resolver left the driver still resolving the
    preset defaults: every cell then failed its parameter-identity check,
    which is the guard working, but only after a full grid had been launched.
    """
    base = resolve(GRIDS[grid]["task"], lane)
    flags = []
    for key in sorted(set(params) | set(base)):
        value = params.get(key)
        if value == base.get(key):
            continue
        # JSON so parse_override recovers the type; bare strings stay bare.
        flags += ["--set", f"{key}={value if isinstance(value, str) else json.dumps(value)}"]
    return flags


def run_engine(args):
    report = load_cardinality_report(args.corpus, args.documents)
    archive_results(args.outdir, [args.engine], args.grid, args.lane,
                    args.selectivities, args.cardinalities, args.max_parallel, report)
    # Every field this leg will sweep, not just one: doc count alone cannot
    # detect an index fed before a field existed, and one column's probe
    # cannot detect it for the other five.
    for _selectivity, cardinality, field in grid_cells(args.grid, "100", args.cardinalities):
        if field not in NUMERIC_FIELDS and report["fields"][field]["realized_cardinality"] <= 0:
            raise RuntimeError(f"cardinality report says {field} has no realized values")
        count = probe_field(args.engine, args.host, args.port, args.collection,
                            args.grid, field, cardinality, report)
        print(f"{args.engine}: {field} ({cardinality}) probe passed across {count} docs",
              flush=True)
    if args.warm_passes > 0:
        sent = warm_engine(args.engine, args.host, args.port, args.collection,
                           args.grid, args.lane, args.max_parallel, args.selectivities,
                           args.cardinalities, args.warm_passes, args.concurrency,
                           report)
        print(f"{args.engine}: warmed with {sent} requests over "
              f"{args.warm_passes} passes of every grid shape", flush=True)

    root = Path(__file__).resolve().parents[1]
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    failures = 0
    for selectivity, cardinality, _field in grid_cells(args.grid, args.selectivities,
                                                       args.cardinalities):
        params = resolve_grid_cell(args.grid, selectivity, cardinality, args.lane,
                                   args.max_parallel, report=report)
        output = cell_path(outdir, args.engine, selectivity, cardinality, params)
        command = [str(root / "scripts" / "run-driver.sh"), args.engine,
                   GRIDS[args.grid]["task"],
                   "--lane", args.lane, "--host", args.host, "--port", str(args.port),
                   "--collection", args.collection, "--server-pid", str(args.server_pid),
                   # Record the affinity the engine was ACTUALLY started under.
                   # Without this every cell stamps driver.py's default, so a
                   # CCD-pinned run is indistinguishable from a both-CCD one in
                   # its own result file - and they are not comparable: pinning
                   # the server to one CCD moved the unfiltered row 101-110k to
                   # 124-144k qps.
                   "--server-cores", args.server_cores,
                   "--duration", str(args.duration),
                   "--repetitions", str(args.repetitions),
                   # Grid requests are identical (match_all): a handful of
                   # warmup/validation requests proves as much as the full
                   # query pool would, and the pool-sized pass was the
                   # dominant grid cost on slow cells. 0 = full pool.
                   "--max-queries", str(args.max_source_queries),
                   # Identical requests preceded by a validation pass on the
                   # same connections leave nothing for a long warmup to warm;
                   # measured indistinguishable at 0.1s. See scripts/grid.sh.
                   "--warmup-seconds", str(args.replay_warmup),
                   "--max-requests", str(args.max_requests),
                   "--warmup-requests", str(args.warmup_requests),
                   "--concurrency", str(args.concurrency),
                   "--corpus", args.corpus, "--label", f"{args.grid}-grid",
                   "--output", str(output)] + grid_overrides(args.grid, params, args.lane)
        print(f"{args.engine} selectivity={selectivity}% column={cardinality}",
              flush=True)
        completed = subprocess.run(command, stdout=subprocess.DEVNULL, check=False)
        if completed.returncode:
            print(f"FAILED: {args.engine} {args.grid} {selectivity}% {cardinality}",
                  flush=True)
            failures += 1
        if output.exists():
            try:
                result = load_result(output)
            except (OSError, ValueError) as error:
                print(f"FAILED: unreadable result {output}: {error}", flush=True)
                failures += 1
            else:
                if (result.get("task_parameters") != params
                        or result.get("param_hash") != param_hash(params)):
                    print(f"FAILED: parameter identity mismatch in {output}", flush=True)
                    failures += 1
        else:
            failures += 1
    failures += record_health_control(args.engine, args.server_pid, args.port,
                                      args.outdir, server_cores=args.server_cores,
                                      host=args.host)
    return 1 if failures else 0


def load_prev_results(outdir, engines, grid, lane, report, max_parallel=1):
    """Previous generation (.prev) of each cell, same guards as load_results.
    The corpus-sha check matters: a lane switch archives the OTHER lane's
    cells, and a delta across corpora would be fiction."""
    corpus_sha256 = report["corpus_sha256"]
    results = {}
    for engine in engines:
        for selectivity, cardinality, _field in grid_cells(grid):
            params = resolve_grid_cell(grid, selectivity, cardinality, lane, max_parallel,
                                       report=report)
            path = Path(str(cell_path(outdir, engine, selectivity, cardinality, params)) + ".prev")
            if not path.exists():
                continue
            try:
                result = load_result(path)
            except (OSError, ValueError):
                continue
            if (result.get("task_parameters") == params
                    and result.get("engine_config", {}).get("index_layout_version")
                    == index_layout_version(engine)
                    and result.get("corpus", {}).get("sha256") == corpus_sha256):
                results[(engine, selectivity, cardinality)] = result
    return results


def engine_inert_params(engine):
    """Parameters this engine's adapter provably never reads."""
    try:
        return make_adapter(engine, "searchbench").inert_params
    except Exception:
        return frozenset()


def param_difference(recorded, expected, inert):
    """{key: (recorded, expected)} for parameters that shaped the request.

    Empty means the two cells ran the same request and are directly
    comparable, whatever their param_hash says.
    """
    if not isinstance(recorded, dict):
        return None
    keys = (set(recorded) | set(expected)) - set(inert)
    return {key: (recorded.get(key), expected.get(key))
            for key in sorted(keys) if recorded.get(key) != expected.get(key)}


def load_results(outdir, engines, grid, lane, report, max_parallel=1,
                 strict=False, cardinalities=None):
    """Newest usable cell per (engine, selectivity, cardinality).

    Cells are matched on the parameters that actually shaped the request, so a
    parameter an engine ignores can never hide its measurements: max_parallel
    is implemented only by the luxir adapter, and hashing it into everyone's
    identity used to split byte-identical REST runs into series that
    refused to be tabulated together.

    A cell that differs in a parameter the engine DOES read is still returned,
    tagged with `_param_diff`, and rendered as approximate rather than
    discarded. These runs cost real wall-clock time; silently dropping one
    because a label moved is how that time gets wasted. The exact parameters
    stay recorded in every result either way, and `strict=True` restores
    exact-match-only.
    """
    corpus_sha256 = report["corpus_sha256"]
    results = {}
    for engine in engines:
        inert = engine_inert_params(engine)
        for selectivity in GRID_SELECTIVITIES:
            sel_slug = selectivity.replace(".", "p")
            for cardinality, _field, _nominal, _realized in rendered_columns(
                    grid, cardinalities):
                params = resolve_grid_cell(grid, selectivity, cardinality, lane, max_parallel,
                                           report=report)
                pattern = f"{engine}-sel{sel_slug}-card{cardinality.lower()}+*.json"
                best = None
                for path in Path(outdir).glob(pattern):
                    try:
                        result = load_result(path)
                    except (OSError, ValueError):
                        continue
                    # Hard gates: a different engine, task or corpus is a
                    # different experiment, not a differently-labelled one.
                    if (result.get("engine") != engine
                            or result.get("task_class") != GRIDS[grid]["task"]
                            or result.get("engine_config", {}).get("index_layout_version")
                            != index_layout_version(engine)
                            or result.get("corpus", {}).get("sha256") != corpus_sha256):
                        continue
                    diff = param_difference(result.get("task_parameters"), params, inert)
                    if diff is None or (diff and strict):
                        continue
                    result["_param_diff"] = diff
                    # Prefer an exact request match; otherwise the newest cell.
                    key = (not diff, path.stat().st_mtime)
                    if best is None or key > best[0]:
                        best = (key, result)
                if best is not None:
                    results[(engine, selectivity, cardinality)] = best[1]
    return results


def cell_qps(results, engine, selectivity, cardinality):
    result = results.get((engine, selectivity, cardinality))
    aggregate = result.get("aggregate") if result else None
    if not aggregate or aggregate.get("errors"):
        return None
    return aggregate.get("qps")


def cell_cpu(results, engine, selectivity, cardinality):
    result = results.get((engine, selectivity, cardinality))
    aggregate = result.get("aggregate") if result else None
    if not aggregate or aggregate.get("errors"):
        return None
    return aggregate.get("server_cpu")


def request_shape_line(grid, params):
    """The parameters that decide what a cell measured, in the grid's own
    vocabulary. Every one of these has silently changed under a rendered
    report at least once, which is why it is printed rather than assumed."""
    shape = [f"`limit={params.get('limit')}` (documents returned per request"
             + (" - 0 means the grid measures the operation only)"
                if not params.get("limit") else ")")]
    if GRIDS[grid]["field_param"] == "facet_field":
        shape.append(f"`facet_limit={params.get('facet_limit')}`")
        shape.append(f"per-bucket payload {payload_summary(params)}")
        if params.get("facet_selected"):
            shape.append(f"`facet_selected={params.get('facet_selected')}` "
                         "(this cell's column; every column pins its own "
                         "designated values)")
    else:
        shape.append(f"`sort_dir={params.get('sort_dir')}`, "
                     f"`fields={params.get('fields')}`")
    shape += [f"`match_all={params.get('match_all')}`",
              f"`request_cache={params.get('request_cache')}`",
              f"`total_hits={params.get('total_hits', True)}` (whether a total "
              "match count is requested at all - false lets luxir prune and "
              "Elasticsearch terminate early or skip numeric-sort blocks, and "
              "drops the cross-engine count-agreement check with it)"]
    return "Request shape: " + ", ".join(shape) + "."


def posture_lines(grid, results, engines, lane, report):
    """The measurement posture, read back out of the cells themselves.

    A grid table is unreadable without it: segment count, client concurrency
    and the request-shape parameters decide what the numbers mean, and they
    have all silently changed under this report at least once. Values are
    collected per engine across its rendered cells, so a posture that was not
    uniform shows up as a list rather than a single value.
    """
    def spread(engine, get):
        seen = []
        for (cell_engine, _sel, _card), result in results.items():
            if cell_engine != engine:
                continue
            try:
                value = get(result)
            except Exception:
                value = None
            if value not in seen:
                seen.append(value)
        if not seen:
            return "-"
        return ", ".join("null" if v is None else str(v) for v in sorted(seen, key=str))

    # Best-effort: this is a report addendum, never a reason to fail a render.
    any_cell = next((r for r in results.values() if r.get("task_parameters")), None)
    if any_cell is None:
        return []
    params = any_cell["task_parameters"]
    corpus_name = Path(any_cell.get("corpus", {}).get("path", "?")).name
    lines = ["", "## Run posture", "",
             f"Corpus `{corpus_name}`, "
             f"{report.get('documents', 0):,} docs, count_mode `{lane}`. "
             + request_shape_line(grid, params), "",
             "| engine | version | segments | clients | max_parallel | duration x reps | cores server/client | store |",
             "|---|---|---:|---:|---:|---|---|---|"]
    for engine in engines:
        if not any(key[0] == engine for key in results):
            continue
        lines.append(
            f"| {engine} "
            f"| {spread(engine, lambda r: str(r['engine_config']['version'])[:12])} "
            f"| {spread(engine, lambda r: r['index_topology']['before']['segment_count'])} "
            f"| {spread(engine, lambda r: r['run_config']['concurrency'])} "
            f"| {spread(engine, lambda r: r['task_parameters']['max_parallel'])} "
            f"| {spread(engine, lambda r: r['run_config']['duration_per_repetition_s'])}s x "
            f"{spread(engine, lambda r: r['run_config']['repetitions'])} "
            f"| {spread(engine, lambda r: r['core_split']['server'])} / "
            f"{spread(engine, lambda r: r['core_split']['client'])} "
            f"| {spread(engine, lambda r: r['engine_config']['store_backend'])} |")
    lines += ["",
              "- `max_parallel` is translated into a request only by the luxir adapter. "
              "For both REST engines it is recorded but inert, which is why cells "
              "carrying different values still tabulate together (see comparable_hash).",
              "- Every cell records the complete serving topology before and after "
              "measurement, including live/deleted documents per segment. A topology "
              "change fails the cell.",
              *GRID_DOC[grid]["notes"],
              "- Luxir's `version` is a repository stamp, while `binary_sha256` is "
              "the identity of the executable that actually served the cell.",
              "- Multi-shape JVM runs intentionally measure a mixed steady state; "
              "single-shape deployments should use a separate process per shape.",
              "- Per-cell JSON carries the rest: heap, the affinity actually observed, "
              "corpus SHA-256, host and CPU governor."]
    return lines


def representative_request_lines(grid, results, engines, collection=None,
                                 cardinalities=None):
    """The actual bytes each engine was sent, for one cell.

    Captured while materializing the replay blob. A grid table hides everything
    about request shape - retrieval, count requests, cache switches - and each
    of those has silently changed what the numbers meant at least once.
    Printing one request makes that inspectable at a glance without asking the
    current adapter to reconstruct a historical request.
    """
    # A filtered cell shows the fuller shape (filter clause plus the operation);
    # fall back to whatever is present.
    labels = [label for label, _f, _n, _r in rendered_columns(grid, cardinalities)]
    preferred = [(sel, card) for sel in ("1", "99", "100") for card in labels]
    chosen = next((c for c in preferred
                   if any((e, c[0], c[1]) in results for e in engines)), None)
    if chosen is None:
        return []
    selectivity, cardinality = chosen
    lines = ["", "## Representative request", "",
             f"One cell - {selectivity}% selectivity, {cardinality} column - "
             "captured from the request serialized into each replay workload. "
             "JSON bodies are pretty-printed; exact wire hashes are shown.", ""]
    for engine in engines:
        result = results.get((engine, selectivity, cardinality))
        if result is None:
            continue
        captures = result.get("representative_requests", {})
        capture = captures.get(result.get("task_class"))
        if capture is None and captures:
            capture = next(iter(captures.values()))
        if capture is None:
            lines += [f"**{engine}**: not captured (legacy result).", ""]
            continue
        lines += [f"**{engine}**", "", "```http",
                  *captured_http_lines(capture), "```", "",
                  f"Wire SHA-256: `{capture.get('wire_sha256', 'N/A')}`.", ""]
    return lines


def delta_lines(grid, results, base_results, engines, cardinalities=None):
    """Per-cell server-work delta against the family's base grid.

    cpu_ms/request rather than qps: the work-done measure the report already
    uses for iteration deltas, immune to thermal state and client posture.
    Rendered only when the base grid has same-corpus results, so the family
    contract - same cell, payload-only difference, the delta IS the payload's
    cost - is one table instead of a two-report read.
    """
    base = GRIDS[grid].get("delta_base")
    if not base or not base_results:
        return []
    columns = rendered_columns(grid, cardinalities)
    headers = [column[0] for column in columns]
    sections = []
    for engine in engines:
        rows = []
        seen = False
        for selectivity in GRID_SELECTIVITIES:
            cells = []
            for cardinality, _field, _nominal, _realized in columns:
                cpu = cell_cpu(results, engine, selectivity, cardinality)
                base_cpu = cell_cpu(base_results, engine, selectivity, cardinality)
                if not cpu or not base_cpu or not base_cpu["cpu_ms_per_request"]:
                    cells.append("-")
                    continue
                seen = True
                this = cpu["cpu_ms_per_request"]
                that = base_cpu["cpu_ms_per_request"]
                approx = "~" if (
                    results[(engine, selectivity, cardinality)].get("_param_diff")
                    or base_results[(engine, selectivity, cardinality)].get("_param_diff")
                ) else ""
                cells.append(f"{approx}{this - that:+.1f} ({this / that:.1f}x)")
            rows.append(f"| {selectivity}% | " + " | ".join(cells) + " |")
        if seen:
            sections += ["", f"### {engine}", "",
                         "| Selectivity | " + " | ".join(headers) + " |",
                         "|---:|" + "|".join("---:" for _ in headers) + "|"] + rows
    if not sections:
        return []
    return ["", f"## Server work vs the `{base}` grid", "",
            f"cpu_ms/request, `{grid}` minus `{base}` (ratio in parens); "
            "positive = this grid's payload costs more. `~` marks a cell where "
            "either side ran with differing recorded parameters; `-` = a side "
            "is missing or carried no cpu sample."] + sections


def render_tables(grid, results, engines, lane, report, prev_results=None,
                  collection=None, cardinalities=None, base_results=None):
    prev_results = prev_results or {}
    doc = GRID_DOC[grid]
    columns = rendered_columns(grid, cardinalities)
    headers = []
    for column in columns:
        label, field = column[0], column[1]
        kind = "int" if field in NUMERIC_FIELDS else "str"
        headers.append(f"{label} ({kind}, R={column_cardinality(column, report):,})")
    lines = [f"# {doc['title']}", "",
             f"Lane: `{lane}`. {doc['summary']} Columns are {doc['columns_are']}: "
             "the label is nominal cardinality, and the header shows the field type "
             "and exact corpus-realized R. "
             "Luxir cells append QPS relative to elasticsearch and opensearch "
             "(`.. / ESx / OSx`, >1 = luxir faster), and when a same-corpus "
             "previous run exists, the change in server work "
             "(`+N%cpu` = more cpu_ms/request than the previous run).", "",
             "Every result filename and JSON carries the exact resolved param_hash. Cells "
             "are matched on the parameters that actually shaped the request, so a "
             "parameter an engine's adapter ignores (max_parallel, which only luxir "
             "translates) does not hide its measurements. Cells that differ in a "
             "parameter the engine does read are still shown, with a warning above."]
    approximate = {}
    for engine in engines:
        lines += ["", f"## {engine}", "",
                  "| Selectivity | " + " | ".join(headers) + " |",
                  "|---:|" + "|".join("---:" for _ in headers) + "|"]
        for selectivity in GRID_SELECTIVITIES:
            cells = []
            for cardinality, _field, _nominal, _realized in columns:
                result = results.get((engine, selectivity, cardinality))
                aggregate = result.get("aggregate") if result else None
                if not aggregate:
                    cells.append("missing")
                    continue
                if aggregate.get("errors"):
                    cells.append(f"ERROR ({aggregate['errors']})")
                    continue
                qps = aggregate.get("qps")
                p50 = aggregate.get("latency_ms", {}).get("p50")
                if qps is None or p50 is None:
                    cells.append("missing")
                    continue
                cell = f"{qps:.0f} / {p50:.2f}"
                if result.get("_param_diff"):
                    approximate.setdefault(engine, {}).update(result["_param_diff"])
                if engine == "luxir":
                    for peer in ("elasticsearch", "opensearch"):
                        peer_qps = cell_qps(results, peer, selectivity, cardinality)
                        cell += " / -" if not peer_qps else f" / {qps / peer_qps:.1f}x"
                    # Iteration delta: work-done vs this cell's previous run
                    # (cpu_ms/request - thermal-state-immune, unlike QPS).
                    cpu = cell_cpu(results, engine, selectivity, cardinality)
                    prev_cpu = cell_cpu(prev_results, engine, selectivity, cardinality)
                    if cpu and prev_cpu and prev_cpu["cpu_ms_per_request"]:
                        delta = (cpu["cpu_ms_per_request"] / prev_cpu["cpu_ms_per_request"] - 1) * 100
                        cell += f" / {delta:+.0f}%cpu"
                cells.append(cell)
            lines.append(f"| {selectivity}% | " + " | ".join(cells) + " |")

        cpu_rows = []
        engine_has_cpu = False
        for selectivity in GRID_SELECTIVITIES:
            cells = []
            for cardinality, _field, _nominal, _realized in columns:
                cpu = cell_cpu(results, engine, selectivity, cardinality)
                if not cpu:
                    cells.append("-")
                    continue
                engine_has_cpu = True
                cell = f"{cpu['cpu_ms_per_request']:.1f} ({cpu['cores_busy']:.1f})"
                if engine == "luxir":
                    for peer in ("elasticsearch", "opensearch"):
                        peer_cpu = cell_cpu(results, peer, selectivity, cardinality)
                        ok = peer_cpu and cpu["cpu_ms_per_request"]
                        cell += (f" / {peer_cpu['cpu_ms_per_request'] / cpu['cpu_ms_per_request']:.1f}x"
                                 if ok else " / -")
                cells.append(cell)
            cpu_rows.append(f"| {selectivity}% | " + " | ".join(cells) + " |")
        if engine_has_cpu:
            lines += ["", f"### {engine} server CPU: ms/request (cores busy); "
                      "luxir appends peer_cpu/luxir_cpu, >1 = luxir does less work", "",
                      "| Selectivity | " + " | ".join(headers) + " |",
                      "|---:|" + "|".join("---:" for _ in headers) + "|"] + cpu_rows
    lines += delta_lines(grid, results, base_results or {}, engines, cardinalities)
    lines += posture_lines(grid, results, engines, lane, report)
    lines += representative_request_lines(grid, results, engines, collection,
                                          cardinalities)
    if approximate:
        warning = ["", "> **WARNING: some cells ran with different parameters.**",
                   "> They are tabulated anyway - the measurement cost real time and is",
                   "> still informative - but treat them as ballpark, not as a like-for-like",
                   "> comparison. Differing parameters (recorded -> this table's value):"]
        for engine in sorted(approximate):
            differences = ", ".join(
                f"`{key}` {recorded!r} -> {expected!r}"
                for key, (recorded, expected) in sorted(approximate[engine].items()))
            warning.append(f"> - **{engine}**: {differences}")
        # Above the tables, and on stderr so it is not missed in a long report.
        first_table = next((index for index, line in enumerate(lines)
                            if line.startswith("## ")), len(lines))
        lines = lines[:first_table] + warning + lines[first_table:]
        print("\n".join(line.lstrip("> ") for line in warning if line),
              file=sys.stderr)
    return "\n".join(lines) + "\n"


def render(args):
    engines = parse_engines(args.engines)
    # Luxir relative columns need the peer cells even when only luxir renders.
    load_engines = sorted(set(engines) | ({"elasticsearch", "opensearch"}
                                          if "luxir" in engines else set()))
    report = load_cardinality_report(args.corpus, args.documents)
    results = load_results(args.outdir, load_engines, args.grid, args.lane,
                           report, args.max_parallel,
                           cardinalities=args.cardinalities)
    prev = load_prev_results(args.outdir, ["luxir"], args.grid, args.lane,
                             report, args.max_parallel)
    base = GRIDS[args.grid].get("delta_base")
    base_results = {}
    if base:
        base_outdir = Path(args.delta_outdir or Path(args.outdir).parent / f"{base}-grid")
        if base_outdir.is_dir():
            base_results = load_results(str(base_outdir), load_engines, base, args.lane,
                                        report, args.max_parallel,
                                        cardinalities=args.cardinalities)
    markdown = render_tables(args.grid, results, engines, args.lane, report, prev,
                             args.collection, args.cardinalities, base_results)
    controls = health_control_lines(args.outdir, expected="luxir" in engines,
                                    queries=list(results.values()))
    if controls:
        markdown += "\n" + "\n".join(controls) + "\n"
    output = Path(args.output or Path(args.outdir) / report_name(args.grid))
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(markdown, encoding="utf-8")
    print(markdown, end="")
    return 0


def validate_report(args):
    load_cardinality_report(args.corpus, args.documents)
    return 0


def prepare(args):
    report = load_cardinality_report(args.corpus, args.documents)
    archive_results(args.outdir, parse_engines(args.engines), args.grid, args.lane,
                    args.selectivities, args.cardinalities, args.max_parallel, report)
    return 0


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    run = subparsers.add_parser("run-engine")
    run.add_argument("engine", choices=ENGINES)
    run.add_argument("--grid", choices=GRID_NAMES, default="facet")
    run.add_argument("--host", default="127.0.0.1")
    run.add_argument("--port", type=int, required=True)
    run.add_argument("--collection", default="searchbench")
    run.add_argument("--server-pid", type=int, required=True)
    run.add_argument("--duration", type=float, default=5)
    run.add_argument("--lane", choices=("exact", "skip"), default="exact")
    run.add_argument("--corpus", required=True)
    run.add_argument("--documents", type=int, required=True)
    run.add_argument("--outdir", required=True)
    run.add_argument("--selectivities", default="all")
    run.add_argument("--cardinalities", default="all")
    run.add_argument("--max-source-queries", type=int, default=32)
    run.add_argument("--replay-warmup", type=float, default=0.1)
    run.add_argument("--warm-passes", type=int, default=2)
    run.add_argument("--max-requests", type=int, default=0)
    run.add_argument("--warmup-requests", type=int, default=0)
    run.add_argument("--repetitions", type=int, default=1)
    run.add_argument("--server-cores",
                     default=os.environ.get("SERVER_CORES", DEFAULT_SERVER_CORES))
    run.add_argument("--max-parallel", type=int, default=1)
    run.add_argument("--concurrency", type=int, default=8)
    run.set_defaults(func=run_engine)

    validate = subparsers.add_parser("validate-report")
    validate.add_argument("--corpus", required=True)
    validate.add_argument("--documents", type=int, required=True)
    validate.set_defaults(func=validate_report)

    prepare_parser = subparsers.add_parser("prepare")
    prepare_parser.add_argument("--engines", required=True)
    prepare_parser.add_argument("--corpus", required=True)
    prepare_parser.add_argument("--documents", type=int, required=True)
    prepare_parser.add_argument("--grid", choices=GRID_NAMES, default="facet")
    prepare_parser.add_argument("--lane", choices=("exact", "skip"), default="exact")
    prepare_parser.add_argument("--outdir", required=True)
    prepare_parser.add_argument("--selectivities", default="all")
    prepare_parser.add_argument("--cardinalities", default="all")
    prepare_parser.add_argument("--max-parallel", type=int, default=1)
    prepare_parser.set_defaults(func=prepare)

    report = subparsers.add_parser("render")
    report.add_argument("--engines", required=True)
    report.add_argument("--grid", choices=GRID_NAMES, default="facet")
    report.add_argument("--lane", choices=("exact", "skip"), default="exact")
    report.add_argument("--corpus", required=True)
    report.add_argument("--documents", type=int, required=True)
    report.add_argument("--outdir", required=True)
    report.add_argument("--output", help="default: <outdir>/<GRID>_GRID.md")
    report.add_argument("--max-parallel", type=int, default=1)
    report.add_argument("--collection", default=None,
                        help="collection used by the rendered result set")
    report.add_argument("--cardinalities", default="all",
                        help="columns to render; names the extra rungs too (e.g. 10,100,1K)")
    report.add_argument("--delta-outdir", default=None,
                        help="results tree of the grid's delta_base "
                             "(default: the sibling <base>-grid directory)")
    report.set_defaults(func=render)
    args = parser.parse_args()
    raise SystemExit(args.func(args))


if __name__ == "__main__":
    main()
