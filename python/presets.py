"""Task presets: named, frozen parameter sets over engine-neutral shapes.

A task SHAPE is the request template (top_docs, facet, nested_facet,
date_facet, multi, get, mix). A PRESET freezes a full parameter set over one shape and gives it the
name the regression grid tracks over time. Ad-hoc variations are the same
preset plus --set overrides or a declared variant (variants.py). param_hash
records the exact resolved identity in every result file as a sanity check;
report tooling pivots on declared differences and surfaces undeclared ones
from the recorded values rather than refusing to tabulate.

count_mode is a parameter like any other: "exact" (Luxir get_number:true /
REST track_total_hits:true) or "skip" (both engines' shipping default).
The historical name for it was "lane"; the driver's --lane flag still sets
it and result files still record it as "lane" for continuity.
"""

import hashlib
import json
import os

from query_source import (BENCHMARK_GAME_QUERY_CLASSES, PRIMARY_QUERY_CLASSES,
                          SUPPORTED_QUERY_CLASSES)

DATE_START_MS = 1_577_836_800_000
DATE_END_MS = 1_735_689_600_000
DATE_GAP_MS = 30 * 24 * 60 * 60 * 1000

# Per-bucket metrics for the `facet` shape (a bucket's `metrics` are computed
# over the documents in that bucket, unlike `multi`'s `stats`, which are one
# scalar each over the whole domain).
#
# The corpus carries exactly two numeric columns - price_i (0..100,000) and
# sort_i (full uint32 range) - so a three-metric request spans both rather than
# stacking three accumulators on one column: METRICS3 is two ops over price_i
# plus one over sort_i, which prices the second column read as well as the
# second accumulator. Adding a third numeric field would change the corpus
# hash and orphan every recorded cell, which is not worth a third column.
METRIC1 = [{"name": "price_avg", "op": "avg", "field": "price_i"}]
METRICS3 = [{"name": "price_avg", "op": "avg", "field": "price_i"},
            {"name": "price_max", "op": "max", "field": "price_i"},
            {"name": "sort_avg", "op": "avg", "field": "sort_i"}]

PRESETS = {
    "GET_1": {"shape": "get", "batch_size": 1, "fields": ["id", "price_i"]},
    "GET_10": {"shape": "get", "batch_size": 10, "fields": ["id", "price_i"]},
    "GET_100": {"shape": "get", "batch_size": 100, "fields": ["id", "price_i"]},
    # Ranking/retrieval and exact counting are distinct operations. COUNT owns
    # the total-hit work; asking for it here disables pruning and turns TOP_K
    # into an engine-dependent rank-plus-count compound benchmark.
    "TOP_10": {"shape": "top_docs", "limit": 10, "fields": ["id"],
               "total_hits": False},
    "TOP_100": {"shape": "top_docs", "limit": 100, "fields": ["id"],
                "total_hits": False},
    "COUNT": {"shape": "top_docs", "limit": 0, "fields": None},
    "FACET_10": {"shape": "facet", "limit": 10, "fields": ["id"],
                 "facet_field": "cat_s", "facet_limit": 10},
    "FACET_1K": {"shape": "facet", "limit": 10, "fields": ["id"],
                 "facet_field": "cat1k_s", "facet_limit": 10},
    "FACET_HC": {"shape": "facet", "limit": 10, "fields": ["id"],
                 "facet_field": "catid_s", "facet_limit": 1000},
    # Cardinality ladder: 10/1k are above; 100 and 10k/100k Zipf fields also
    # exist in the corpus (cat100_s, cat10k_s) for ad-hoc sweeps via
    # --set facet_field=...; the named upper-rung presets follow.
    "FACET_10K": {"shape": "facet", "limit": 10, "fields": ["id"],
                  "facet_field": "cat10k_s", "facet_limit": 1000},
    "FACET_100K": {"shape": "facet", "limit": 10, "fields": ["id"],
                   "facet_field": "cat100k_s", "facet_limit": 1000},
    "FACET_1M": {"shape": "facet", "limit": 10, "fields": ["id"],
                 "facet_field": "cat1m_s", "facet_limit": 10},
    "FACET_DATE": {"shape": "date_facet", "limit": 10, "fields": ["id"],
                   "date_field": "date_dt", "date_start_ms": DATE_START_MS,
                   "date_end_ms": DATE_END_MS, "date_gap_ms": DATE_GAP_MS,
                   "mincount": 1},
    # Per-bucket metrics over the same facet the count presets use. Four
    # shapes, deliberately paired: one metric vs three (does a second column
    # and a second accumulator cost what the first did), and count-ordered vs
    # metric-ordered. Ordering by a metric is the one that changes WHICH
    # buckets come back, so an engine cannot answer it from counts alone -
    # every bucket's metric has to exist before the top ten can be picked.
    "FACET_METRIC": {"shape": "facet", "limit": 10, "fields": ["id"],
                     "facet_field": "cat1m_s", "facet_limit": 10,
                     "metrics": METRIC1},
    "FACET_METRICS": {"shape": "facet", "limit": 10, "fields": ["id"],
                      "facet_field": "cat1m_s", "facet_limit": 10,
                      "metrics": METRICS3},
    "FACET_METRIC_SORT": {"shape": "facet", "limit": 10, "fields": ["id"],
                          "facet_field": "cat1m_s", "facet_limit": 10,
                          "metrics": METRIC1,
                          "facet_sort": "price_avg", "facet_sort_dir": "desc"},
    "FACET_METRICS_SORT": {"shape": "facet", "limit": 10, "fields": ["id"],
                           "facet_field": "cat1m_s", "facet_limit": 10,
                           "metrics": METRICS3,
                           "facet_sort": "price_avg", "facet_sort_dir": "desc"},
    "FACET_MULTI": {"shape": "multi", "limit": 10, "fields": ["id"],
                    "facets": [{"name": "cat10", "field": "cat_s", "limit": 10},
                               {"name": "cat1k", "field": "cat1k_s", "limit": 10}],
                    "stats": [{"name": "price_avg", "op": "avg", "field": "price_i"},
                              {"name": "price_min", "op": "min", "field": "price_i"},
                              {"name": "price_max", "op": "max", "field": "price_i"}]},
    # Nested terms facets: top 10 parent buckets, then top 10 child buckets
    # independently within each returned parent. These deliberately sample
    # both directions at the low and middle ranges plus two high-parent cases,
    # rather than expanding into a cardinality-pair matrix.
    "FACET_NESTED_10_100": {"shape": "nested_facet", "limit": 10, "fields": ["id"],
                            "facet_field": "cat_s", "facet_limit": 10,
                            "subfacet_field": "cat100_s", "subfacet_limit": 10},
    "FACET_NESTED_100_10": {"shape": "nested_facet", "limit": 10, "fields": ["id"],
                            "facet_field": "cat100_s", "facet_limit": 10,
                            "subfacet_field": "cat_s", "subfacet_limit": 10},
    "FACET_NESTED_1K_100K": {"shape": "nested_facet", "limit": 10, "fields": ["id"],
                              "facet_field": "cat1k_s", "facet_limit": 10,
                              "subfacet_field": "cat100k_s", "subfacet_limit": 10},
    "FACET_NESTED_100K_1K": {"shape": "nested_facet", "limit": 10, "fields": ["id"],
                              "facet_field": "cat100k_s", "facet_limit": 10,
                              "subfacet_field": "cat1k_s", "subfacet_limit": 10},
    "FACET_NESTED_2MU_1K": {"shape": "nested_facet", "limit": 10, "fields": ["id"],
                             "facet_field": "catu2m_s", "facet_limit": 10,
                             "subfacet_field": "cat1k_s", "subfacet_limit": 10},
    "FACET_NESTED_2MU_5M": {"shape": "nested_facet", "limit": 10, "fields": ["id"],
                             "facet_field": "catu2m_s", "facet_limit": 10,
                             "subfacet_field": "cat5m_s", "subfacet_limit": 10},
    "DEEP_COLLECT": {"shape": "top_docs", "limit": 10000, "fields": ["id"],
                     "sort_field": "price_i", "sort_dir": "asc",
                     "total_hits": False},
    # Sort grid base. Same shape as TOP_10 plus an explicit sort field, so a
    # cell measures ranking by a stored value rather than by score. The grid
    # overrides sort_field (its column axis) and limit (10 = shallow, 10000 =
    # deep); the preset stands alone for ad-hoc runs, e.g.
    # quick.sh -t SORT -v limit=10000,sort_field=cat1m_s.
    "SORT": {"shape": "top_docs", "limit": 10, "fields": ["id"],
             "sort_field": "price_i", "sort_dir": "asc", "total_hits": False},
    "MIX": {"shape": "mix",
            "slots": ("TOP_10",) * 7 + ("TOP_100",) * 3 + ("COUNT",) * 3 + (
                "FACET_10", "FACET_1K", "FACET_HC", "FACET_DATE", "FACET_MULTI") + (
                "DEEP_COLLECT",) * 2},
}

# Full-text results use luceneutil's operator by frequency classes as the
# primary axis. Keep the generic presets for mixed production workloads; these
# named cells prevent a frequent term from hiding a rare conjunction or phrase.
FULL_TEXT_TASKS = []
for query_class in PRIMARY_QUERY_CLASSES:
    prefix = query_class.upper()
    for base in ("TOP_10", "TOP_100", "COUNT"):
        name = f"{prefix}_{base}"
        PRESETS[name] = dict(PRESETS[base], query_class=query_class)
        FULL_TEXT_TASKS.append(name)
FULL_TEXT_TASKS = tuple(FULL_TEXT_TASKS)

# Search Benchmark Game queries are deliberately secondary and never enter the
# default baseline or its headline aggregates. They remain named presets so the
# retained workload is runnable without changing driver code.
BENCHMARK_GAME_TASKS = []
for query_class in BENCHMARK_GAME_QUERY_CLASSES:
    prefix = query_class.upper()
    for base in ("TOP_10", "TOP_100", "COUNT"):
        name = f"{prefix}_{base}"
        PRESETS[name] = dict(PRESETS[base], query_class=query_class)
        BENCHMARK_GAME_TASKS.append(name)
BENCHMARK_GAME_TASKS = tuple(BENCHMARK_GAME_TASKS)

# One intentionally narrow clause-cache campaign family. The source query
# still comes from the primary luceneutil AND_HIGH_MED pool, but every replay
# record gets a semantics-preserving nonce (driver.make_workload) so composite
# membership keys never repeat within the measured pass. The numeric range is
# column-scanned by Luxir and is therefore meaningful cache work, unlike a
# cheap exact term. Both sides of the scored-filter density route are explicit
# in the cell name and resolved parameters.
STABLE_FILTER_TASKS = []
for density, upper in (("90", 90_000), ("1", 1_000)):
    for base in ("TOP_10", "COUNT"):
        name = f"FILTERED_RANGE_{density}_AND_HIGH_MED_{base}"
        PRESETS[name] = dict(
            PRESETS[base],
            query_class="and_high_med",
            query_variants=200_000,
            filter_mode="boolean",
            filter_kind="numeric_range",
            filter_field="price_i",
            filter_gte=0,
            filter_lt=upper,
            filter_density_percent=float(density),
        )
        STABLE_FILTER_TASKS.append(name)
STABLE_FILTER_TASKS = tuple(STABLE_FILTER_TASKS)

# Controls use the same validation/replay path, but are not search workloads
# or cross-engine cells. Luxir's health endpoint does no index work.
CONTROL_PRESETS = {"HEALTH": {"shape": "health"}}
TASKS = tuple(PRESETS) + tuple(CONTROL_PRESETS)


def parse_override(text):
    key, _, value = text.partition("=")
    if not key or not _:
        raise ValueError(f"override must be key=value: {text!r}")
    try:
        return key, json.loads(value)
    except json.JSONDecodeError:
        return key, value


# Nested corpus selectivity tiers (corpus_transform.py SEL_TIERS): the value
# names the ~percentage of documents the filter matches.
SEL_FIELDS = {"99": "sel99_s", "90": "sel90_s", "50": "sel50_s",
              "10": "sel10_s", "1": "sel1_s", "0.1": "sel01_s",
              "0.01": "sel001_s"}

# Grid rows, shared by every grid: the domain is match_all narrowed by one
# selectivity-tier filter, so the operation under test is the only thing that
# changes across a row.
GRID_SELECTIVITIES = ("100", "99", "50", "10", "1", "0.1", "0.01")

# Grid columns: (label, field, nominal cardinality, realized cardinality).
#
# realized=None means the corpus cardinality report carries the exact realized
# value and the runner refuses to measure a corpus that does not describe the
# field - that check is what catches a stale index or a corpus fed before a
# field existed. A stated number is a field the transform's report does not
# cover (it reports the synthetic string ladder and sort_i): price_i is uniform
# 0..100000 by construction. sort_i is a bijective permutation of the source
# ordinal, so its realized cardinality is exactly the document count recorded
# by the corpus report.
#
# The string ladder mixes distributions deliberately. The high-realized rungs
# use Zipf-1.0 and uniform because Zipf-1.15 leaves much of a large nominal
# domain unrealized. Each corpus report records the exact realized counts.
STRING_COLUMNS = (
    ("2Mu", "catu2m_s", 2_000_000, None),
    ("5M", "cat5m_s", 5_000_000, None),
    ("1M", "cat1m_s", 1_000_000, None),
    ("100K", "cat100k_s", 100_000, None),
    ("10K", "cat10k_s", 10_000, None),
    ("1K", "cat1k_s", 1_000, None),
    ("100", "cat100_s", 100, None),
    ("10", "cat_s", 10, None),
)
# Numeric columns, sort grids only: an int column is a different engine path
# from a string one, not just a different cardinality. Luxir compares raw
# int64 keys in bulk windows for numerics and per-doc ordinals for strings;
# Lucene can skip whole numeric blocks via points when no total is requested
# and has no equivalent for a keyword sort.
NUMERIC_COLUMNS = (
    ("100Kn", "price_i", 100_001, 100_001),
    ("10Mn", "sort_i", 10_000_000, None),
)

_BY_LABEL = {column[0]: column for column in STRING_COLUMNS + NUMERIC_COLUMNS}


def _columns(*labels):
    return tuple(_BY_LABEL[label] for label in labels)


def resolve(task, count_mode, overrides=None):
    """Return the resolved parameter set for a preset (+ optional overrides).

    MIX takes no overrides (its slots are presets); other presets accept any
    key, including new ones an adapter understands. filter_sel=<tier> is
    sugar for filter_field/filter_value on the corpus selectivity fields and
    expands here so param_hash covers the canonical form.
    """
    if task not in PRESETS and task not in CONTROL_PRESETS:
        raise ValueError(f"unknown preset {task!r}")
    params = dict((PRESETS if task in PRESETS else CONTROL_PRESETS)[task],
                  count_mode=count_mode)
    if overrides:
        if params["shape"] == "mix":
            raise ValueError("MIX takes no overrides; vary its component presets instead")
        params.update(overrides)
    # Never benchmark the REST shard request cache by accident. It can return
    # an entire repeated response without executing the search. This is not
    # the Lucene query/filter cache, which remains enabled. Resolve the switch
    # here so every current and future measured search shape is safe by
    # default; explicit true remains available as an A/B override.
    if params["shape"] != "mix":
        params.setdefault("request_cache", False)
    if params["shape"] == "get":
        params.setdefault("get_impl", "boolean")
    query_class = params.get("query_class")
    if query_class is not None and query_class not in SUPPORTED_QUERY_CLASSES:
        raise ValueError(
            f"query_class must be one of {SUPPORTED_QUERY_CLASSES}, got {query_class!r}")
    sel = params.pop("filter_sel", None)
    if sel is not None:
        sel = str(sel)
        if sel not in SEL_FIELDS:
            raise ValueError(f"filter_sel must be one of {sorted(SEL_FIELDS)}, got {sel!r}")
        params.setdefault("filter_field", SEL_FIELDS[sel])
        params.setdefault("filter_value", "t")
    if params["shape"] == "mix":
        params["slots"] = list(params["slots"])
    # Per-bucket metrics belong to the facet shape alone, and a bucket order
    # can only name a metric the same request asks for. Checked here, at the
    # one place a parameter set is created, so no adapter can silently drop
    # either (an override that lands on the wrong shape is a typo, not a
    # request for degraded service).
    if params.get("metrics") and params["shape"] != "facet":
        raise ValueError(f"metrics are per-bucket and only apply to the facet "
                         f"shape, not {params['shape']!r}")
    if params.get("facet_selected") and params["shape"] != "facet":
        raise ValueError(f"facet_selected pins field-facet buckets and only "
                         f"applies to the facet shape, not {params['shape']!r}")
    if params.get("facet_sort"):
        names = {metric["name"] for metric in params.get("metrics") or ()}
        if params["facet_sort"] not in names:
            raise ValueError(f"facet_sort={params['facet_sort']!r} names no metric "
                             f"in this request ({sorted(names)})")
        if params.get("facet_sort_dir", "desc") not in ("asc", "desc"):
            raise ValueError(f"facet_sort_dir must be asc or desc, "
                             f"got {params['facet_sort_dir']!r}")
    return params


# A GRID is one operation swept over the same domain matrix: selectivity rows
# by field columns, match_all everywhere, one thing measured per cell.
#
# task        - the preset a cell resolves from (and the task_class recorded).
# field_param - the parameter the column axis writes; this is what the grid
#               sweeps and therefore what the grid is about.
# fixed       - the rest of the "measure one thing" posture for this grid.
# columns     - default column set; extra_columns are selectable with -c.
# probe       - what a column must prove before the leg measures it:
#               "field_dict" = the adapter's own field-is-indexed probe,
#               "cell" = the grid's own request, shallow and totals-on
#               (see grid.probe_field).
#
# Parameters only. How a grid is described in its rendered report lives with
# the renderer (grid.py GRID_DOC), and where its cells land is derived from
# the grid name (results/<grid>-grid) by scripts/grid.sh.
def _facet_grid(task, limit=0):
    """One member of the facet family: same domains, same columns, same
    facet_limit and posture, differing only in what its preset asks for per
    bucket. So the difference between two of these grids on the same cell IS
    the cost of the extra payload.

    limit=0 (default): no engine returns documents, so a cell is counting
    rather than retrieval. Retrieval is not free and it is not the same work
    everywhere - measured on ES at 1% selectivity / 1M cardinality, returning
    ten hits cost 0.59 of 2.90 cpu_ms per request, a fifth of the cell, purely
    to collect ids nothing in the grid reads. The -top10 members set limit=10
    deliberately: they measure the consuming posture, where the document
    result exists and a facet selection actually refines it. facet_limit=10
    keeps the cell about counting rather than bucket serialization.

    Six columns by choice: uniform-2M, zipf-1.0-5M, then the zipf-1.15 ladder.
    Keeps a default full grid at 6x7 and fast; the intermediate rungs stay
    selectable with -c. Exact realized cardinalities are report metadata.
    """
    return {"task": task, "field_param": "facet_field", "probe": "field_dict",
            "fixed": {"facet_limit": 10, "limit": limit},
            "columns": _columns("2Mu", "5M", "1M", "100K", "1K", "10"),
            "extra_columns": _columns("10K", "100")}


GRIDS = {
    "facet": _facet_grid("FACET_1M"),
    # The facet family's payload axis: per-bucket metrics, and for the -sort
    # pair a bucket order that names one of them - which changes WHICH ten
    # buckets come back, so no engine can answer from counts alone. Grid names
    # are part of result paths and report names; keep them stable.
    #
    # delta_base names the family member a payload grid is read against; the
    # renderer appends a per-cell server-work delta section against it when
    # that grid's results exist, so "the delta between them IS the cost" is a
    # rendered table rather than a manual two-report read.
    "facet-metric": dict(_facet_grid("FACET_METRIC"), delta_base="facet"),
    "facet-metrics": dict(_facet_grid("FACET_METRICS"), delta_base="facet"),
    "facet-metric-sort": dict(_facet_grid("FACET_METRIC_SORT"), delta_base="facet"),
    "facet-metrics-sort": dict(_facet_grid("FACET_METRICS_SORT"), delta_base="facet"),
    # The selection axis of the same family: the identical count-only cell
    # with the column field's designated head and tail values selected. The
    # values come from the corpus report (designate_values), so each column
    # pins one value guaranteed inside its natural page (head, the merge path)
    # and one outside it (tail, the append path) with exact known counts.
    # Selection refines sideways - documents, never the facet's own counts -
    # so the counting work is byte-identical to `facet` and the delta between
    # the two grids on a cell is the whole cost of selection.
    "facet-selected": dict(_facet_grid("FACET_1M"), probe="selected_cell",
                           selected=("head", "tail"), delta_base="facet"),
    # The consuming posture: same cells with the top 10 matching ids
    # retrieved, so the refined document result has a consumer and selection
    # cannot be elided. The unselected member prices retrieval itself (delta
    # vs facet); the selected member's delta against IT is the cost of
    # selection where the sideways machinery actually runs.
    "facet-top10": dict(_facet_grid("FACET_1M", limit=10), delta_base="facet"),
    "facet-selected-top10": dict(_facet_grid("FACET_1M", limit=10),
                                 probe="selected_cell",
                                 selected=("head", "tail"),
                                 delta_base="facet-top10"),
    # Sorting, isolated the same way: same corpus, same domains, same columns,
    # with the swept field used as the SORT key instead of the facet key. Two
    # depths, because the cost curve is not the same shape at both: a shallow
    # top-10 is dominated by the comparison per matching document, a deep
    # top-10K adds a 10,000-slot queue, its heap churn, and materializing
    # 10,000 ids.
    "sort10": {
        "task": "SORT",
        "field_param": "sort_field",
        "probe": "cell",
        # fields=["id"]: something has to come back, and the corpus id is the
        # cheapest comparable value every engine can return (ES/OS and luxir
        # all read their scalar column/doc values with source off). This is not
        # a stored-field retrieval benchmark.
        "fixed": {"limit": 10, "sort_dir": "asc", "fields": ["id"]},
        # Four string rungs spanning maximum ties to nearly none, plus both
        # int columns - for a sort the string/int split is a bigger difference
        # than any two string rungs. Every other rung stays available with -c.
        "columns": _columns("2Mu", "1M", "1K", "10", "100Kn", "10Mn"),
        "extra_columns": _columns("5M", "100K", "10K", "100"),
    },
    "sort10k": {
        "task": "SORT",
        "field_param": "sort_field",
        "probe": "cell",
        "fixed": {"limit": 10000, "sort_dir": "asc", "fields": ["id"]},
        "columns": _columns("2Mu", "1M", "1K", "10", "100Kn", "10Mn"),
        "extra_columns": _columns("5M", "100K", "10K", "100"),
    },
}

GRID_NAMES = tuple(GRIDS)


def grid_columns(grid):
    """Every column selectable for a grid, default set first."""
    spec = GRIDS[grid]
    return spec["columns"] + spec["extra_columns"]


def designated_values(report, field, labels):
    """{label: {"value", "count"}} for a field's designated corpus values.

    designate_values (corpus_transform.py) records these at transform time
    with exact whole-corpus counts. They are the only value vocabulary a grid
    may pin: any other choice would put a value with an unverifiable count
    into the experiment.
    """
    if report is None:
        raise ValueError("a selected grid resolves its pinned values from the "
                         "corpus cardinality report; pass report=")
    values = report.get("fields", {}).get(field, {}).get("values") or {}
    missing = [label for label in labels if label not in values]
    if missing:
        raise ValueError(f"corpus report designates no {missing} for {field}; "
                         "re-transform with a current corpus_transform.py")
    picked = {label: values[label] for label in labels}
    resolved = [entry["value"] for entry in picked.values()]
    if len(set(resolved)) != len(resolved):
        raise ValueError(f"{field}: designated {list(labels)} collapse onto "
                         f"the same value ({resolved})")
    return picked


def resolve_grid_cell(grid, selectivity, cardinality, count_mode="exact", max_parallel=1,
                      report=None):
    """Resolve one fixed-emission grid cell. Grids that pin selected values
    (spec key `selected`) need the corpus cardinality report to name them."""
    if grid not in GRIDS:
        raise ValueError(f"unknown grid {grid!r}")
    if selectivity not in GRID_SELECTIVITIES:
        raise ValueError(f"unknown grid selectivity {selectivity!r}")
    fields = {label: field for label, field, _nominal, _realized in grid_columns(grid)}
    if cardinality not in fields:
        raise ValueError(f"unknown {grid}-grid column {cardinality!r}")
    spec = GRIDS[grid]
    # match_all: the grid controls the DOMAIN exactly - selectivity is the
    # sel-tier filter over the whole corpus, never a full-text query's result
    # set (full-text domains belong to the workload presets).
    # max_parallel=1 (default): serial-everywhere strategy posture - ES is
    # serial natively, OS serial via the exact-count setting, luxir via the
    # request param (only the luxir adapter translates it; 1 = serial on
    # luxir's shared arena). 0 = luxir's serving default since 2026-09-01,
    # inline serial on the connection thread; -1 = the parallel lane (the
    # pre-flip 0). In params, so it is part of the comparability hash.
    #
    # request_cache=False is mandatory wherever limit=0, not optional: the
    # ES/OS shard request cache is enabled by default for size:0 requests only,
    # and a grid cycles identical match_all requests, so its hit rate goes to
    # ~100%. Left on, one facet cell measured 56,542 qps at 0.145
    # cpu_ms/request against 3,502 at 2.308 with it off - a 16x cache artifact.
    # It is pinned on every grid so the switch is recorded rather than assumed.
    #
    # total_hits=False: a cell that also demands an exact total is measuring
    # two things. Asking is not free - it disables luxir's pruning and
    # Elasticsearch's early termination, so the cell measures a different plan
    # than the operation alone would. It also decides how much a SORT cell can
    # skip: with no total requested, Lucene prunes a numeric sort through
    # points, while luxir visits every match either way (field sort turns
    # pruning off). That difference is a real engine property and this is the
    # posture that exposes it. The cost is the cross-engine count-agreement
    # check; run a totals-on pass (--set total_hits=true) when correctness
    # rather than speed is the question.
    overrides = {spec["field_param"]: fields[cardinality],
                 "match_all": True, "max_parallel": max_parallel,
                 "request_cache": False, "total_hits": False}
    overrides.update(spec["fixed"])
    if spec.get("selected"):
        # Resolved to concrete values here, so param_hash records the exact
        # request identity rather than a corpus-dependent label.
        overrides["facet_selected"] = [
            entry["value"] for entry in
            designated_values(report, fields[cardinality], spec["selected"]).values()]
    if selectivity != "100":
        overrides["filter_sel"] = selectivity
        # Optional query-driven posture: make the selectivity predicate the
        # main query instead of a cached/non-scoring filter. Omitted in the
        # default posture so its existing parameter identity stays stable.
        filter_mode = os.environ.get("GRID_FILTER_MODE", "top_docs")
        if filter_mode != "top_docs":
            overrides["filter_mode"] = filter_mode
    return resolve(spec["task"], count_mode, overrides)


def param_hash(params):
    """Stable short hash of a resolved parameter set (exact run identity)."""
    canon = json.dumps(params, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canon.encode()).hexdigest()[:12]


def comparable_hash(params, inert=()):
    """Hash of the parameters that actually shaped an engine's request.

    param_hash is the exact identity of a run and covers every parameter,
    including ones the engine in question ignores. That makes it the wrong key
    for "may these two cells appear in the same table": a parameter only one
    engine implements (max_parallel, which only the luxir adapter translates)
    otherwise splits every other engine's identical measurements into series
    that refuse to be tabulated together, and the only way back would be
    re-running cells to relabel byte-identical work.

    Each adapter declares the parameters it provably never reads
    (BaseAdapter.inert_params); dropping them here makes comparability mean
    "same request", which is what it always should have meant.
    """
    return param_hash({key: value for key, value in params.items()
                       if key not in set(inert)})
