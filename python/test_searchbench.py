from collections import Counter, defaultdict
import hashlib
import json
import lzma
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

from adapters import ENGINES, make_adapter
from corpus_transform import (LUCENEUTIL_HEADER, input_documents,
                              ordinal_sort_value, synthetic_fields, transform_doc)
from cpu_layout import choose_cpu_sets, format_cpu_list, parse_cpu_list
from datasets import dataset_name
from driver import (file_sha256, make_workload, materialize_workload,
                    resolve_running_binary, server_config)
from feed_luxir import ALLOW_DUPS_LINE, feed_topology, range_blocks, stream_records
from feed_rest import MAPPINGS, bulk_chunks, feed_topology as feed_rest_topology
from import_luceneutil_queries import (REGEX_TASKS, escape_pattern, escape_term,
                                       expression)
from presets import (BENCHMARK_GAME_TASKS, FULL_TEXT_TASKS, GRIDS, METRIC1,
                     PRESETS, STABLE_FILTER_TASKS, comparable_hash, param_hash, resolve,
                     resolve_grid_cell)
from query_source import (BENCHMARK_GAME_QUERY_CLASSES, DERIVED_QUERY_CLASSES,
                          LUCENEUTIL_CLASS_MAP, PRIMARY_QUERY_CLASSES,
                          QueryItem, classify, read_id_batches,
                          searchbench_queries)
from results_io import load_result, write_context
from report import (client_saturation, fmt, fmt_memory_mib, index_shape_lines,
                    ordered_tasks, ordered_variants, pivot_query_lines,
                    representative_request_lines, startup_lines,
                    undeclared_variation)
from schema import (IDENTITY_POSTURE, LUXIR_ID_SCHEMA, index_layout_version,
                    LUXIR_SCHEMA, REST_BODY_MAPPING, REST_ID_MAPPING)
from select_queries import select
from topologies import Topology, assert_topology, resolve_topology
from variants import (client_settings, filename, param_settings, parse_variant,
                      postures, server_env, slug)


class SearchbenchUnitTest(unittest.TestCase):
    def setUp(self):
        self.union = QueryItem("alpha beta", "union", ("union",), "test")
        self.intersection = QueryItem(
            "+alpha +beta", "intersection", ("intersection",), "test")
        self.ids = tuple(
            f"https://en.wikipedia.org/wiki?curid={48687903 + index}"
            for index in range(100))

    def get_item(self, batch_size):
        ids = self.ids[:batch_size]
        return QueryItem(ids[0], "get", (), "test", ids)

    def test_engine_scope_is_deliberately_small(self):
        self.assertEqual(ENGINES, ("luxir", "opensearch", "elasticsearch"))
        for engine in ENGINES:
            self.assertIsNotNone(make_adapter(engine, "searchbench"))
        with self.assertRaises(ValueError):
            make_adapter("unknown", "searchbench")

    def test_corpus_manifest_declares_prefix_lanes_and_variants(self):
        path = (Path(__file__).resolve().parents[1]
                / "corpora" / "wikipedia" / "manifest.json")
        manifest = json.loads(path.read_text(encoding="utf-8"))
        lanes = manifest["lanes"]
        self.assertEqual(
            [(lanes[name]["documents"], lanes[name]["variant"])
             for name in ("smoke", "standard", "scale")],
            [(100_000, "full"), (10_000_000, "full"),
             (33_332_620, "text")])
        self.assertEqual(lanes["facet"],
                         {"documents": 10_000_000, "variant": "facet"})
        self.assertEqual(lanes["scale"]["documents"],
                         manifest["source_documents"])

    def test_benchmark_game_query_taxonomy(self):
        examples = {
            "term": ("the", ("term",)),
            "intersection": ("+alpha +beta", ("intersection",)),
            "union": ("alpha beta", ("union",)),
            "phrase": ('"alpha beta"', ("phrase",)),
            "sloppy_phrase": ('"alpha beta"~2', ("sloppy_phrase",)),
            "negated": ("+python -snake", ("negated",)),
            "intersection_union": ("+climate policy", ("intersection_union",)),
            "boosted": ("climate^3 policy", ("boosted",)),
            "two_phase": ('+"the who" +uk', ("two-phase-critic",)),
        }
        self.assertEqual(set(examples), set(BENCHMARK_GAME_QUERY_CLASSES))
        for expected, (text, tags) in examples.items():
            self.assertEqual(classify(text, tags), expected)

    def test_full_text_presets_cross_luceneutil_taxonomy_with_operations(self):
        self.assertEqual(tuple(LUCENEUTIL_CLASS_MAP.values()) + DERIVED_QUERY_CLASSES,
                         PRIMARY_QUERY_CLASSES)
        self.assertEqual(len(FULL_TEXT_TASKS), len(PRIMARY_QUERY_CLASSES) * 3)
        for query_class in PRIMARY_QUERY_CLASSES:
            for operation in ("TOP_10", "TOP_100", "COUNT"):
                params = resolve(f"{query_class.upper()}_{operation}", "exact")
                self.assertEqual(params["query_class"], query_class)
                self.assertEqual(params["limit"], {"TOP_10": 10, "TOP_100": 100,
                                                   "COUNT": 0}[operation])
                self.assertIs(params["request_cache"], False)
                if operation.startswith("TOP_"):
                    self.assertIs(params["total_hits"], False)
        self.assertEqual(len(BENCHMARK_GAME_TASKS),
                         len(BENCHMARK_GAME_QUERY_CLASSES) * 3)
        self.assertTrue(set(FULL_TEXT_TASKS).isdisjoint(BENCHMARK_GAME_TASKS))

    def test_stable_filter_tasks_vary_composite_keys_but_not_source_semantics(self):
        self.assertEqual(STABLE_FILTER_TASKS, (
            "FILTERED_RANGE_90_AND_HIGH_MED_TOP_10",
            "FILTERED_RANGE_90_AND_HIGH_MED_COUNT",
            "FILTERED_RANGE_1_AND_HIGH_MED_TOP_10",
            "FILTERED_RANGE_1_AND_HIGH_MED_COUNT",
        ))
        source = QueryItem("+alpha +beta", "and_high_med", (), "test")
        pools = {"top": [source], "count": [source]}
        params = resolve(STABLE_FILTER_TASKS[0], "exact", {"query_variants": 3})
        self.assertEqual(params["filter_density_percent"], 90.0)
        self.assertEqual((params["filter_gte"], params["filter_lt"]), (0, 90_000))
        workload = make_workload(
            STABLE_FILTER_TASKS[0], params, pools, 0,
            make_adapter("luxir", "searchbench"))
        self.assertEqual(len(workload), 3)
        self.assertEqual({item.text for _entry, item in workload}, {source.text})
        nonces = [item.nonce for _entry, item in workload]
        self.assertEqual(len(set(nonces)), 3)
        self.assertTrue(all(nonce.startswith("__searchbench_nonce_filtered_range_90_")
                            for nonce in nonces))

        luxir = json.loads(make_adapter("luxir", "searchbench")
                           .build(*workload[0]).body)
        outer = luxir["query"]["boolean"]
        self.assertEqual(outer["filter"], [{"range": {
            "field": "price_i", "gte": 0, "lt": 90_000}}])
        inner = outer["required"][0]["boolean"]
        self.assertEqual(inner["min_match"], 1)
        self.assertEqual(inner["optional"][0], "body:(+alpha +beta)")
        self.assertEqual(inner["optional"][1], {
            "match": {"field": "id", "val": nonces[0]}})

        for engine in ("elasticsearch", "opensearch"):
            rest = json.loads(make_adapter(engine, "searchbench")
                              .build(*workload[0]).body)
            self.assertEqual(rest["query"]["bool"]["filter"], [{
                "range": {"price_i": {"gte": 0, "lt": 90_000}}}])
            self.assertEqual(
                rest["query"]["bool"]["must"][0]["bool"]
                ["minimum_should_match"], 1)
            self.assertIs(rest["track_total_hits"], False)
        self.assertTrue(all(resolve(task, "exact")["query_variants"] == 200_000
                            for task in STABLE_FILTER_TASKS))

    def test_query_source_preserves_tags_and_filters_workload(self):
        rows = [
            {"query": "the", "tags": ["term"]},
            {"query": "+alpha +beta", "tags": ["intersection", "global"]},
            {"query": '"alpha beta"~1', "tags": ["sloppy_phrase", "sloppy_phrase:slop_1"]},
        ]
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "queries.txt"
            path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
            pools = searchbench_queries(path)
        self.assertEqual([item.query_class for item in pools["top"]],
                         ["term", "intersection", "sloppy_phrase"])
        adapter = make_adapter("luxir", "searchbench")
        params = resolve("SLOPPY_PHRASE_TOP_10", "exact")
        workload = make_workload("SLOPPY_PHRASE_TOP_10", params, pools, 0, adapter)
        self.assertEqual([item.text for _entry, item in workload], ['"alpha beta"~1'])

    def test_primary_luceneutil_workload_covers_taxonomy(self):
        root = Path(__file__).resolve().parents[1] / "queries" / "luceneutil"
        source = searchbench_queries(root / "queries-all.txt")["top"]
        selected = searchbench_queries(root / "queries.txt")["top"]
        manifest = json.loads((root / "selection.json").read_text(encoding="utf-8"))
        source_counts = Counter(item.query_class for item in source)
        selected_counts = Counter(item.query_class for item in selected)
        self.assertEqual(len(source), 940)
        self.assertEqual(manifest["schema_version"], 2)
        # Admission is gated by cross-engine agreement on at least the
        # standard corpus; the scale lane is opt-in (SELECT_CORPORA).
        corpus_manifest = json.loads(
            (root.parents[1] / "corpora" / "wikipedia" / "manifest.json").read_text(encoding="utf-8"))
        self.assertIn("standard", manifest["corpora"])
        self.assertLessEqual(set(manifest["corpora"]),
                             set(corpus_manifest["lanes"]))
        self.assertEqual(manifest["selection"]["queries"], len(selected))
        self.assertTrue(all(source_counts[query_class]
                            for query_class in PRIMARY_QUERY_CLASSES))
        self.assertEqual(dict(sorted(selected_counts.items())),
                         manifest["selection"]["class_counts"])
        self.assertEqual(file_sha256(root / "queries.txt"),
                         manifest["selection"]["sha256"])
        # A query is adjudicated when the recorded selection either admitted or
        # rejected it. The selection may lag the source while new classes wait
        # for the next cross-corpus agreement run, but only by WHOLE classes:
        # a class with any adjudicated query must have every query adjudicated,
        # so nothing edited or added inside an admitted class can ride in
        # without re-selection, and queries.txt simply has no pending queries.
        selected_text = {item.text for item in selected}
        rejected_text = {entry["query"] for entry in manifest["rejected"]}
        source_text = {item.text for item in source}
        self.assertLessEqual(selected_text | rejected_text, source_text)
        self.assertEqual(manifest["source"]["queries"],
                         len(selected_text) + len(rejected_text))
        adjudicated_classes = set(manifest["source"]["class_counts"])
        pending = [item for item in source
                   if item.text not in selected_text
                   and item.text not in rejected_text]
        self.assertEqual({item.query_class for item in pending},
                         set(source_counts) - adjudicated_classes)
        if not pending:
            self.assertEqual(file_sha256(root / "queries-all.txt"),
                             manifest["source"]["sha256"])
            self.assertEqual(dict(sorted(source_counts.items())),
                             manifest["source"]["class_counts"])
        for entry in manifest["rejected"]:
            self.assertEqual(
                entry["mismatched_corpora"],
                [name for name, counts in entry["counts"].items()
                 if len(set(counts.values())) > 1])

    def test_luceneutil_source_is_pinned(self):
        root = Path(__file__).resolve().parents[1]
        source = json.loads(
            (root / "queries/luceneutil/source.json").read_text(encoding="utf-8"))
        self.assertEqual(source["commit"],
                         "3cfd163094feda7ef5ab14d89a095e4e523c33f2")
        self.assertEqual(source["sha256"],
                         "e8166233f415b9bb735618f3a5f5205e25cf1d6b86d1e6647176288b05d0271a")
        self.assertEqual(source["derivation"], {
            "ordering": "source order within the declared class order",
            "limit_per_class": 50,
            "term_translation": ("escape expression-parser metacharacters while "
                                 "preserving the task operator"),
            "derived_classes": ("wildcard_scan and wildcard_lead: LowTerm affixes "
                                "around a star, so scanned terms dominate matched "
                                "terms; regex: curated automaton-structure ladder"),
            "queries": 940,
        })

    def test_luceneutil_literal_terms_preserve_task_operator(self):
        self.assertEqual(escape_term("user:ed&&admin"), "user\\:ed\\&\\&admin")
        self.assertEqual(expression("AndHighLow", "+a +user:ed"),
                         "+a +user\\:ed")
        self.assertEqual(expression("OrHighLow", "been p.n.e"), "been p.n.e")
        self.assertEqual(expression("HighPhrase", '"of the"'), '"of the"')

    def test_multiterm_patterns_preserve_operators_and_stay_literal(self):
        self.assertEqual(expression("Wildcard", "th*e"), "th*e")
        self.assertEqual(expression("Prefix3", "200*"), "200*")
        self.assertEqual(escape_pattern("a:b*c?"), "a\\:b*c?")

    def test_regex_patterns_use_the_shared_engine_syntax(self):
        # Only operators every engine's regexp parser shares, and only the
        # lowercase alphanumeric literal space the analyzers emit.
        allowed = set("abcdefghijklmnopqrstuvwxyz0123456789()[]{}|,.*?+-")
        self.assertEqual(len({pattern for pattern, _intent in REGEX_TASKS}),
                         len(REGEX_TASKS))
        for pattern, intent in REGEX_TASKS:
            self.assertTrue(set(pattern) <= allowed, pattern)
            self.assertTrue(intent)

    def test_secondary_benchmark_game_workload_remains_runnable(self):
        root = Path(__file__).resolve().parents[1] / "queries" / "benchmark-game"
        source = searchbench_queries(root / "queries-all.txt")["top"]
        selected = searchbench_queries(root / "queries.txt")["top"]
        manifest = json.loads((root / "selection.json").read_text(encoding="utf-8"))
        self.assertEqual(len(source), 1010)
        self.assertEqual(len(selected), manifest["selection"]["queries"])
        self.assertEqual({item.query_class for item in source},
                         set(BENCHMARK_GAME_QUERY_CLASSES))

    def test_benchmark_game_source_is_pinned(self):
        root = Path(__file__).resolve().parents[1] / "queries" / "benchmark-game"
        source = json.loads((root / "source.json").read_text(encoding="utf-8"))
        self.assertEqual(source["commit"],
                         "ac0a1aa7ff1d9eb95562ad36a8229f28c83c11c1")
        # The upstream file is carried verbatim as the leading lines, so the
        # pin is checkable without a download.
        lines = (root / "queries-all.txt").read_text(
            encoding="utf-8").splitlines(keepends=True)
        upstream = "".join(lines[:source["queries"]]).encode("utf-8")
        self.assertEqual(len(upstream), source["bytes"])
        self.assertEqual(hashlib.sha256(upstream).hexdigest(), source["sha256"])
        additions = source["derivation"]["additions"]
        self.assertEqual(len(lines), source["queries"] + sum(additions.values()))
        self.assertEqual(len(lines), source["derivation"]["queries"])
        self.assertEqual(Counter(json.loads(line)["tags"][0]
                                 for line in lines[source["queries"]:]), additions)

    def test_query_selection_requires_exact_cross_engine_agreement(self):
        rows = [
            {"query": "alpha", "tags": ["term"]},
            {"query": "beta gamma", "tags": ["union"]},
        ]
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source = root / "queries-all.txt"
            source.write_text("".join(json.dumps(row) + "\n" for row in rows),
                              encoding="utf-8")
            result_paths = {}
            for engine, differing_count in (
                    ("luxir", 2), ("opensearch", 3), ("elasticsearch", 3)):
                result = {
                    "engine": engine,
                    "lane": "exact",
                    "task_class": "COUNT",
                    "source_files": {"queries": {"sha256": file_sha256(source)}},
                    "corpus": {"sha256": "corpus-hash",
                               "facet_cardinalities": {
                                   "documents": 10, "variant": "full"}},
                    "count_samples": {"COUNT\talpha": 1,
                                      "COUNT\tbeta gamma": differing_count},
                    "engine_config": {
                        "version": "test-version",
                        "full_text": ({"field": "body", "tokenizer": "unicode_word",
                                       "filters": ["lowercase"], "long_terms": "truncate"}
                                      if engine == "luxir" else
                                      {"field": "body", "analyzer": "standard",
                                       "search_analyzer": "standard"}),
                    },
                }
                path = root / f"{engine}-COUNT.json"
                path.write_text(json.dumps(result), encoding="utf-8")
                result_paths[engine] = path
            selected, manifest = select(source, {"standard": result_paths})
        self.assertEqual(selected, (json.dumps(rows[0]) + "\n").encode())
        self.assertEqual(manifest["selection"]["queries"], 1)
        self.assertEqual(manifest["rejected"], [{
            "query": "beta gamma", "class": "union",
            "mismatched_corpora": ["standard"],
            "counts": {"standard": {
                "luxir": 2, "opensearch": 3, "elasticsearch": 3}}}])

    def test_report_orders_taxonomy_and_formats_memory_units(self):
        results = {("luxir", FULL_TEXT_TASKS[-1], "{}"): {},
                   ("opensearch", FULL_TEXT_TASKS[0], "{}"): {}}
        self.assertEqual(ordered_tasks(results),
                         [FULL_TEXT_TASKS[0], FULL_TEXT_TASKS[-1]])
        self.assertEqual(fmt(9606104 / 1024), "9380.96")
        self.assertEqual(fmt_memory_mib({"peak_rssanon_kb": 18440},
                                        "peak_rssanon_kb"), "18.01")
        self.assertEqual(fmt_memory_mib({}, "peak_rssanon_kb"), "N/A")

    def test_representative_request_is_captured_from_replay_wire(self):
        adapter = make_adapter("luxir", "searchbench")
        entry = dict(resolve("TOP_10", "exact"), name="HIGH_TERM_TOP_10")
        workload = [(entry, self.union), (entry, self.intersection)]
        with tempfile.TemporaryDirectory() as temp_dir:
            blob, labels, captures = materialize_workload(
                workload, adapter, "127.0.0.1", 8080,
                Path(temp_dir) / "workload.sbwl")
            self.assertTrue(Path(blob).is_file())
        self.assertEqual(labels, ["HIGH_TERM_TOP_10"])
        capture = captures["HIGH_TERM_TOP_10"]
        expected = adapter.build(entry, self.union)
        wire = adapter.wire_bytes(expected, "127.0.0.1", 8080)
        self.assertEqual(capture["query"], "alpha beta")
        self.assertEqual(capture["body"], expected.body.decode())
        self.assertEqual(capture["wire_sha256"], hashlib.sha256(wire).hexdigest())
        self.assertEqual(capture["headers"][0],
                         {"name": "Host", "value": "127.0.0.1:8080"})

        result = {"representative_requests": captures,
                  "task_parameters": entry}
        text = "\n".join(representative_request_lines(
            {("luxir", "HIGH_TERM_TOP_10", "{}"): result}, ["HIGH_TERM_TOP_10"]))
        self.assertIn("### TOP_10", text)
        self.assertIn("POST /collections/searchbench/_search", text)
        self.assertIn('"limit": 10', text)
        self.assertIn(capture["wire_sha256"], text)

    def test_report_renders_stable_per_segment_document_shape(self):
        results = {}
        for index, engine in enumerate(ENGINES):
            topology = {
                "shard_count": 1, "segment_count": 2,
                "max_docs": 10, "live_docs": 9, "deleted_docs": 1,
                "size_bytes": 3072,
                "segments": [
                    {"id": f"{engine}-a", "shard": 0, "max_docs": 4,
                     "live_docs": 4, "deleted_docs": 0, "size_bytes": 1024},
                    {"id": f"{engine}-b", "shard": 0, "max_docs": 6,
                     "live_docs": 5, "deleted_docs": 1, "size_bytes": 2048},
                ],
            }
            results[(engine, "HIGH_TERM_COUNT", "{}")] = {
                "engine_config": {"version": str(index), "shards": 1,
                                  "replicas": 0, "heap_max": "8g"},
                "run_config": {"concurrency": 8},
                "corpus": {"facet_cardinalities": {"documents": 9}},
                "index_topology": {"stable": True, "before": topology,
                                   "after": topology},
            }
        text = "\n".join(index_shape_lines(results))
        self.assertIn("| luxir | 0 | 9 | 3.00 KiB | 1 x 0 | 2 | STABLE |", text)
        self.assertIn("| luxir | 0 | `luxir-a` | 4 | 4 | 0 | 1.00 KiB |", text)
        self.assertNotIn("INVALID QUERY BOARD", text)

    def test_report_renders_captured_effective_startup(self):
        startup = {
            "source": "/proc/123",
            "pid": 123,
            "executable": "/opt/bin/luxir",
            "working_directory": "/srv/searchbench",
            "argv": ["/opt/bin/luxir", "--server.http.port=9400"],
            "cmdline_sha256": "a" * 64,
            "environment": {"MALLOC_ARENA_MAX": "2"},
            "config_overlays": [],
        }
        results = {("luxir", "HIGH_TERM_COUNT", "{}"): {
            "engine_config": {
                "startup": startup,
                "observed_cpus_allowed_list": "0-14,16-30",
                "nofile_soft": 65536, "nofile_hard": 65536,
            }}}
        text = "\n".join(startup_lines(results))
        self.assertIn("/opt/bin/luxir", text)
        self.assertIn("--server.http.port=9400", text)
        self.assertIn("MALLOC_ARENA_MAX=2", text)
        self.assertIn("soft `65536`", text)

    def test_variant_layers_slugs_and_posture_grouping(self):
        variant = parse_variant("concurrency=16,threads=2,max_parallel=0")
        self.assertEqual(client_settings(variant), {"concurrency": 16, "threads": 2})
        self.assertEqual(param_settings(variant), {"max_parallel": 0})
        self.assertEqual(slug(variant), "c16+max_parallel=0+t2")
        # Explicit driver defaults normalize to the baseline cell.
        self.assertEqual(parse_variant("concurrency=8"), {})
        self.assertEqual(parse_variant("-"), {})
        self.assertEqual(filename("luxir", "TOP_10", parse_variant("concurrency=16")),
                         "luxir-TOP_10@c16.json")
        self.assertEqual(filename("luxir", "TOP_10", {}), "luxir-TOP_10.json")
        self.assertEqual(server_env(parse_variant("heap=2g")), "SEARCHBENCH_HEAP=2g")
        with self.assertRaises(ValueError):
            parse_variant("concurrency=0")
        with self.assertRaises(ValueError):
            parse_variant("concurrency=1,concurrency=2")
        # Baseline server posture runs first (feed/verify happen there);
        # specs normalizing to the same variant deduplicate.
        grouped = postures(["heap=2g,concurrency=1", "concurrency=16",
                            "concurrency=8", "-"])
        self.assertEqual(grouped, [("", ["concurrency=16", "concurrency=8"]),
                                   ("SEARCHBENCH_HEAP=2g", ["heap=2g,concurrency=1"])])

    @staticmethod
    def variant_cell(concurrency, qps, variant=None, facet_limit=10, heap="8g"):
        return {
            "variant": variant or {},
            "task_parameters": {"shape": "top_docs", "limit": 10,
                                "facet_limit": facet_limit},
            "run_config": {"concurrency": concurrency, "replay_threads": 1,
                           "duration_per_repetition_s": 5, "repetitions": 2,
                           "replay_order": "seq", "replay_seed": None},
            "engine_config": {"heap_max": heap},
            "aggregate": {"latency_ms": {"p50": 1.0, "p90": 1.5, "p99": 2.0},
                          "qps": qps, "errors": 0,
                          "server_cpu": {"cpu_ms_per_request": 0.5,
                                         "cores_busy": 4.0}},
            "memory": {"peak_rssanon_kb": 2048},
        }

    def test_report_pivots_on_declared_variants(self):
        sweep = {"concurrency": 16}
        results = {
            ("luxir", "HIGH_TERM_TOP_10", "{}"): self.variant_cell(8, 1000),
            ("luxir", "HIGH_TERM_TOP_10", json.dumps(sweep)):
                self.variant_cell(16, 1800, sweep),
        }
        vkeys = ordered_variants(results)
        self.assertEqual(vkeys, ["{}", '{"concurrency": 16}'])
        failures = defaultdict(set)
        text = "\n".join(pivot_query_lines(
            results, ["HIGH_TERM_TOP_10"], vkeys, failures))
        self.assertIn("- `baseline`: campaign defaults", text)
        self.assertIn("- `c16`: concurrency=16", text)
        self.assertIn("baseline p50 | baseline p99 | baseline QPS", text)
        self.assertIn("c16 p50 | c16 p99 | c16 QPS", text)
        self.assertIn("1800 (1.80x)", text)
        self.assertIn("c16 cores busy", text)
        self.assertFalse(failures)
        self.assertEqual(undeclared_variation(results), [])

    def test_report_surfaces_undeclared_variation(self):
        # Same cells, but nothing declares the concurrency difference, the
        # heap difference, or the parameter drift.
        results = {
            ("luxir", "HIGH_TERM_TOP_10", "{}"): self.variant_cell(8, 1000),
            ("luxir", "FACET_10", "{}"): self.variant_cell(16, 1800, heap="2g"),
            ("luxir", "FACET_10", '{"concurrency": 4}'):
                self.variant_cell(4, 900, {"concurrency": 4}, facet_limit=100),
        }
        notes = "\n".join(undeclared_variation(results))
        self.assertIn("`concurrency` varies without a declaring variant", notes)
        self.assertIn("`heap_max` varies without a declaring variant", notes)
        self.assertIn("facet_limit", notes)
        declared = {
            ("luxir", "FACET_10", "{}"): self.variant_cell(8, 1000),
            ("luxir", "FACET_10", '{"facet_limit": 100}'):
                self.variant_cell(8, 700, {"facet_limit": 100}, facet_limit=100),
        }
        self.assertEqual(undeclared_variation(declared), [])

    def test_report_flags_client_bound_cells(self):
        hot = self.variant_cell(16, 50000, {"concurrency": 16})
        hot["aggregate"]["client_cpu"] = {"cores_busy": 0.97}
        hot["core_split"] = {"client": "15,31"}
        cool = self.variant_cell(8, 12000)
        cool["aggregate"]["client_cpu"] = {"cores_busy": 0.41}
        cool["core_split"] = {"client": "15,31"}
        results = {
            ("luxir", "HIGH_TERM_COUNT", '{"concurrency": 16}'): hot,
            ("luxir", "HIGH_TERM_COUNT", "{}"): cool,
        }
        notes = client_saturation(results)
        self.assertEqual(len(notes), 1)
        self.assertIn("HIGH_TERM_COUNT", notes[0])
        self.assertIn("@c16", notes[0])
        self.assertIn("client-bound", notes[0])

    def test_run_context_write_and_load_round_trip(self):
        context = {"label": "baseline", "lane": "exact",
                   "corpus": {"sha256": "c" * 64},
                   "engine_config": {"version": "v1"},
                   "index_topology": {"name": None, "expected_segments": 1,
                                      "before": {"segment_count": 1}}}
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            name = write_context(root, "luxir", context)
            self.assertEqual(name, write_context(root, "luxir", context))
            self.assertEqual(len(list(root.glob("luxir-context-*.json"))), 1)
            # A different observed environment is a second file: the drift
            # alarm is the directory listing.
            other = write_context(root, "luxir",
                                  dict(context, engine_config={"version": "v2"}))
            self.assertNotEqual(name, other)
            task = {"schema_version": 8, "context": name, "engine": "luxir",
                    "task_class": "HIGH_TERM_COUNT", "variant": {},
                    "index_topology": {"stable": True},
                    "aggregate": {"qps": 1.0}}
            path = root / "luxir-HIGH_TERM_COUNT.json"
            path.write_text(json.dumps(task), encoding="utf-8")
            merged = load_result(path, cache={})
            self.assertEqual(merged["engine_config"], {"version": "v1"})
            self.assertEqual(merged["lane"], "exact")
            self.assertEqual(merged["index_topology"],
                             {"name": None, "expected_segments": 1,
                              "before": {"segment_count": 1}, "stable": True})
            self.assertEqual(merged["aggregate"], {"qps": 1.0})
            # Legacy files without a reference pass through unchanged.
            legacy = dict(task, schema_version=5)
            del legacy["context"]
            path.write_text(json.dumps(legacy), encoding="utf-8")
            self.assertEqual(load_result(path), legacy)
            # A dangling reference is loud, never a silently emptier result.
            path.write_text(json.dumps(dict(task, context="luxir-context-missing.json")),
                            encoding="utf-8")
            with self.assertRaises(OSError):
                load_result(path)

    def test_lucene_syntax_reaches_each_engines_parser(self):
        item = QueryItem('+"the who" +uk -radio^2', "boosted", ("boosted",), "test")
        params = resolve("BOOSTED_TOP_10", "exact")
        luxir = json.loads(make_adapter("luxir", "searchbench").build(params, item).body)
        self.assertEqual(
            luxir["query"],
            'body:(+"the who" +uk -radio^2)')
        for engine in ("opensearch", "elasticsearch"):
            body = json.loads(make_adapter(engine, "searchbench").build(params, item).body)
            self.assertEqual(body["query"], {"query_string": {
                "query": item.text, "default_field": "body", "default_operator": "OR"}})

    def test_luxir_search_uses_root_shorthand(self):
        adapter = make_adapter("luxir", "searchbench")
        item = QueryItem("the", "high_term", (), "test")
        for task, limit in (("HIGH_TERM_TOP_10", 10), ("HIGH_TERM_TOP_100", 100),
                            ("HIGH_TERM_COUNT", 0)):
            request = adapter.build(resolve(task, "exact"), item)
            expected = {"query": "body:(the)", "limit": limit}
            if limit:
                expected["fields"] = ["id"]
            else:
                expected["get_number"] = True
            self.assertEqual((request.method, request.path),
                             ("POST", "/collections/searchbench/_search"))
            self.assertEqual(json.loads(request.body), expected)
        probe = json.loads(adapter.build_field_probe("cat_s").body)
        self.assertEqual(probe["limit"], 0)
        self.assertEqual(probe["ops"], {
            "facet": {"field_facet": {"field": "cat_s", "limit": 1}}})

    def test_multiterm_classes_reach_native_query_types(self):
        cases = (
            ("wildcard", "th*e",
             "wildcard('th*e', field=body)",
             {"query_string": {"query": "th*e", "default_field": "body",
                               "default_operator": "OR"}}),
            ("wildcard_scan", "h*band",
             "wildcard('h*band', field=body)",
             {"query_string": {"query": "h*band", "default_field": "body",
                               "default_operator": "OR"}}),
            ("wildcard_lead", "*sband",
             "wildcard('*sband', field=body)",
             {"query_string": {"query": "*sband", "default_field": "body",
                               "default_operator": "OR"}}),
            ("regex", "(19|20)[0-9]{2}",
             "regex('(19|20)[0-9]{2}', field=body)",
             {"regexp": {"body": {"value": "(19|20)[0-9]{2}"}}}),
            ("prefix3", "mos*",
             "body:(mos*)",
             {"query_string": {"query": "mos*", "default_field": "body",
                               "default_operator": "OR"}}),
        )
        for query_class, text, luxir_query, rest_query in cases:
            item = QueryItem(text, query_class, (), "test")
            params = resolve(f"{query_class.upper()}_COUNT", "exact")
            luxir = json.loads(
                make_adapter("luxir", "searchbench").build(params, item).body)
            self.assertEqual(luxir["query"], luxir_query)
            for engine in ("opensearch", "elasticsearch"):
                body = json.loads(
                    make_adapter(engine, "searchbench").build(params, item).body)
                self.assertEqual(body["query"], rest_query)

    def test_full_text_analyzers_are_explicit(self):
        self.assertEqual(LUXIR_SCHEMA["fields"]["body"]["analyzer"], {
            "tokenizer": "unicode_word", "filters": ["lowercase"]})
        self.assertIs(MAPPINGS["mappings"]["properties"]["body"], REST_BODY_MAPPING)
        self.assertEqual(REST_BODY_MAPPING["analyzer"], "standard")
        self.assertEqual(REST_BODY_MAPPING["search_analyzer"], "standard")

    def test_identity_fields_are_indexed_and_column_backed(self):
        self.assertIs(LUXIR_SCHEMA["fields"]["id"], LUXIR_ID_SCHEMA)
        self.assertEqual(LUXIR_ID_SCHEMA,
                         {"type": "id", "index": "match", "column": True})
        self.assertIs(MAPPINGS["mappings"]["properties"]["id"], REST_ID_MAPPING)
        self.assertEqual(REST_ID_MAPPING,
                         {"type": "keyword", "index": True, "doc_values": True})
        self.assertEqual(IDENTITY_POSTURE["luxir"]["ingest_mode"], "allow_dups")
        self.assertEqual(IDENTITY_POSTURE["elasticsearch"]["internal_id"],
                         "auto_generated")

    def test_feeders_use_append_only_id_posture(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            corpus = Path(temp_dir) / "corpus.ndjson"
            corpus.write_text(json.dumps({"id": self.ids[0], "body": "alpha"}) + "\n",
                              encoding="utf-8")
            body = list(stream_records(corpus, 0, corpus.stat().st_size))
            bulk, count = next(bulk_chunks(corpus, "searchbench", 100))
        self.assertEqual(body[0], ALLOW_DUPS_LINE)
        self.assertEqual(body[-1], b'{"_end_":{}}\n')
        self.assertEqual(count, 1)
        metadata, document = [json.loads(line) for line in bulk.splitlines()]
        self.assertEqual(metadata, {"index": {"_index": "searchbench"}})
        self.assertNotIn("_id", metadata["index"])
        self.assertEqual(document["id"], self.ids[0])

    def test_stream_ranges_partition_the_corpus_exactly(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            corpus = Path(temp_dir) / "corpus.ndjson"
            corpus.write_bytes(b"".join(
                json.dumps({"id": str(index), "body": "x" * (index % 7)}).encode() + b"\n"
                for index in range(200)))
            size = corpus.stat().st_size
            for streams in (1, 2, 3, 16, 500):
                bounds = [size * i // streams for i in range(streams + 1)]
                fed = b"".join(b"".join(range_blocks(corpus, bounds[i], bounds[i + 1],
                                                     block_bytes=13))
                               for i in range(streams))
                self.assertEqual(fed, corpus.read_bytes(), f"streams={streams}")

    def test_all_non_mix_tasks_build_for_all_engines_and_lanes(self):
        for engine in ENGINES:
            adapter = make_adapter(engine, "searchbench")
            for lane in ("exact", "skip"):
                for task, preset in PRESETS.items():
                    if preset["shape"] == "mix":
                        continue
                    item = (self.get_item(preset["batch_size"])
                            if preset["shape"] == "get" else self.union)
                    request = adapter.build(resolve(task, lane), item)
                    if request.body:
                        json.loads(request.body)

    def test_lane_controls_are_never_mixed(self):
        for engine in ENGINES:
            adapter = make_adapter(engine, "searchbench")
            exact = json.loads(adapter.build(resolve("COUNT", "exact"), self.union).body)
            skip = json.loads(adapter.build(resolve("COUNT", "skip"), self.union).body)
            if engine == "luxir":
                self.assertTrue(exact["get_number"])
                self.assertNotIn("get_number", skip)
            else:
                self.assertTrue(exact["track_total_hits"])
                self.assertNotIn("track_total_hits", skip)

    def test_top_k_does_not_also_count_matches(self):
        for engine in ENGINES:
            body = json.loads(make_adapter(engine, "searchbench").build(
                resolve("TOP_10", "exact"), self.union).body)
            if engine == "luxir":
                self.assertNotIn("get_number", body)
            else:
                self.assertIs(body["track_total_hits"], False)

    def test_shard_request_cache_is_explicitly_off_by_default(self):
        for task, preset in PRESETS.items():
            if preset["shape"] != "mix":
                self.assertIs(resolve(task, "exact")["request_cache"], False)
        rest = make_adapter("elasticsearch", "searchbench")
        default = rest.build(resolve("TOP_10", "exact"), self.union)
        cached = rest.build(resolve("TOP_10", "exact", {"request_cache": True}),
                            self.union)
        self.assertTrue(default.path.endswith("?request_cache=false"))
        self.assertTrue(cached.path.endswith("?request_cache=true"))

    def test_get_presets_requests_and_validation(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            corpus = Path(temp_dir) / "corpus.ndjson"
            corpus.write_text("".join(json.dumps({"id": value}) + "\n"
                                      for value in self.ids[:25]), encoding="utf-8")
            items = read_id_batches(corpus, 10, max_queries=2)
        self.assertEqual(items[0].ids, self.ids[:10])
        self.assertEqual(items[1].ids, self.ids[10:20])

        single = resolve("GET_1", "skip")
        batch = resolve("GET_10", "skip")
        luxir = make_adapter("luxir", "searchbench")
        top = json.loads(luxir.build(batch, self.get_item(10)).body)
        self.assertEqual(top["limit"], 10)
        self.assertEqual(
            top["query"]["constant_score"]["query"]["boolean"]["min_match"], 1)

        rest = make_adapter("elasticsearch", "searchbench")
        request = rest.build(single, self.get_item(1))
        self.assertEqual((request.method, request.path),
                         ("POST", "/searchbench/_search?request_cache=false"))
        body = json.loads(request.body)
        self.assertEqual(body["query"], {"constant_score": {"filter": {"term": {
            "id": {"value": self.ids[0]}}}}})
        self.assertEqual(body["docvalue_fields"], ["id", "price_i"])
        self.assertIs(body["track_total_hits"], False)
        self.assertIs(body["_source"], False)
        request = rest.build(batch, self.get_item(10))
        body = json.loads(request.body)
        self.assertEqual(body["query"], {"constant_score": {"filter": {"terms": {
            "id": list(self.ids[:10])}}}})
        self.assertEqual(body["size"], 10)

        raw = json.dumps({"hits": {"hits": [{
            "_id": "generated-internal-id",
            "fields": {"id": [self.ids[0]], "price_i": [1]},
        }]}}).encode()
        self.assertIsNone(rest.validate(single, raw, self.get_item(1)))

        top_request = json.loads(rest.build(resolve("TOP_10", "skip"), self.union).body)
        self.assertEqual(top_request["docvalue_fields"], ["id"])

    def test_nested_facet_translation_and_validation(self):
        params = resolve("FACET_NESTED_10_100", "exact")
        luxir = json.loads(make_adapter("luxir", "searchbench").build(params, self.union).body)
        facet = luxir["ops"]["facet"]["field_facet"]
        self.assertEqual((facet["field"], facet["ops"]["subfacet"]["field_facet"]["field"]),
                         ("cat_s", "cat100_s"))
        rest = json.loads(
            make_adapter("opensearch", "searchbench").build(params, self.union).body)
        facet = rest["aggs"]["facet"]
        self.assertEqual((facet["terms"]["field"],
                          facet["aggs"]["subfacet"]["terms"]["field"]),
                         ("cat_s", "cat100_s"))

    def test_facet_grid_resolution_and_translation(self):
        params = resolve_grid_cell("facet", "99", "1M")
        self.assertEqual(params["filter_field"], "sel99_s")
        self.assertEqual(params["facet_field"], "cat1m_s")
        for engine in ENGINES:
            body = json.loads(make_adapter(engine, "searchbench").build(params, self.union).body)
            if engine == "luxir":
                top = body
                self.assertEqual(top["filter"][0]["match"]["field"], "sel99_s")
                self.assertEqual(top["ops"]["facet"]["field_facet"]["field"], "cat1m_s")
            else:
                self.assertEqual(body["query"]["bool"]["filter"][0]["term"]
                                 ["sel99_s"]["value"], "t")
                self.assertEqual(body["aggs"]["facet"]["terms"]["field"], "cat1m_s")

    def test_selected_grid_pins_designated_values(self):
        report = {"fields": {"cat1m_s": {"values": {
            "head": {"value": "cat1m-7", "count": 100},
            "tail": {"value": "cat1m-1", "count": 1}}}}}
        params = resolve_grid_cell("facet-selected", "99", "1M", report=report)
        self.assertEqual(params["facet_selected"], ["cat1m-7", "cat1m-1"])
        luxir = json.loads(make_adapter("luxir", "searchbench").build(params, self.union).body)
        facet = luxir["ops"]["facet"]["field_facet"]
        self.assertEqual(facet["selected"], ["cat1m-7", "cat1m-1"])
        for engine in ("elasticsearch", "opensearch"):
            with self.assertRaisesRegex(ValueError, "facet_selected is luxir-only"):
                make_adapter(engine, "searchbench").build(params, self.union)
        with self.assertRaisesRegex(ValueError, "cardinality report"):
            resolve_grid_cell("facet-selected", "99", "1M")
        with self.assertRaisesRegex(ValueError, "designates no"):
            resolve_grid_cell("facet-selected", "99", "1M",
                              report={"fields": {"cat1m_s": {"values": {}}}})
        with self.assertRaisesRegex(ValueError, "only applies to the facet"):
            resolve("SORT", "exact", {"facet_selected": ["x"]})

    def test_delta_section_renders_cpu_deltas_against_base_grid(self):
        from grid import delta_lines

        def cell(cpu, diff=None):
            result = {"aggregate": {"server_cpu": {"cpu_ms_per_request": cpu}}}
            if diff:
                result["_param_diff"] = diff
            return result

        key = ("luxir", "100", "1M")
        results = {key: cell(11.7)}
        base = {key: cell(0.4, diff={"limit": (0, 10)})}
        text = "\n".join(delta_lines("facet-selected", results, base, ["luxir"]))
        self.assertIn("Server work vs the `facet` grid", text)
        self.assertIn("~+11.3 (29.2x)", text)
        self.assertEqual([], delta_lines("facet", results, base, ["luxir"]))
        self.assertEqual([], delta_lines("facet-selected", results, {}, ["luxir"]))
        self.assertEqual([], delta_lines("facet-selected", results,
                                         {key: cell(0.4)}, ["opensearch"]))

    def test_luxir_validates_selected_pins_and_probe_counts(self):
        adapter = make_adapter("luxir", "searchbench")
        params = {"shape": "facet", "limit": 0, "count_mode": "exact",
                  "facet_selected": ["b", "zzz"], "name": "cell"}
        pinned = json.dumps({"docs": [], "found": 5, "ops": {"facet": {"buckets": [
            {"val": "a", "count": 3}, {"val": "b", "count": 2},
            {"val": "zzz", "count": 0}]}}})
        adapter.validate(params, pinned)
        dropped = json.dumps({"docs": [], "found": 5, "ops": {"facet": {"buckets": [
            {"val": "a", "count": 3}, {"val": "b", "count": 2}]}}})
        with self.assertRaisesRegex(RuntimeError, "'zzz' appears 0 times"):
            adapter.validate(params, dropped)

        probe = json.dumps({"docs": [], "found": 4, "ops": {"facet": {"buckets": [
            {"val": "head", "count": 3}, {"val": "tail", "count": 1}]}}})
        adapter.validate_selected_probe(params, probe, {"head": 3, "tail": 1})
        with self.assertRaisesRegex(RuntimeError, "corpus report says 2"):
            adapter.validate_selected_probe(params, probe, {"head": 2, "tail": 1})
        drifted = json.dumps({"docs": [], "found": 9, "ops": {"facet": {"buckets": [
            {"val": "head", "count": 3}, {"val": "tail", "count": 1}]}}})
        with self.assertRaisesRegex(RuntimeError, "refined total"):
            adapter.validate_selected_probe(params, drifted, {"head": 3, "tail": 1})

    def test_luxir_shorthand_response_ownership_and_streaming(self):
        adapter = make_adapter("luxir", "searchbench")
        params = resolve("GET_10", "skip")
        docs = [{"id": value, "price_i": 1} for value in self.ids[:10]]
        raw = "\n".join(json.dumps({"more": i == 0, "docs": batch,
                                    "ops": {"q": {"found": 999,
                                                   "docs": [{"id": "unrelated"}]}}})
                        for i, batch in enumerate((docs[:4], docs[4:])))
        for lane in ("exact", "skip"):
            self.assertIsNone(adapter.validate(resolve("GET_10", lane), raw, self.get_item(10)))
        self.assertIsNone(adapter.count(raw))
        with self.assertRaisesRegex(RuntimeError, "missing root docs"):
            adapter.validate(params, json.dumps({"ops": {"q": {"docs": docs}}}),
                             self.get_item(10))
        with self.assertRaisesRegex(RuntimeError, "response error"):
            adapter.validate(params, raw + '\n{"error":"late failure"}', self.get_item(10))
        with self.assertRaisesRegex(RuntimeError, "missing root docs"):
            adapter.validate(params, raw + '\n{"ops":{"q":{"docs":[]}}}', self.get_item(10))
        for invalid in ("", "{}", "[]", '{"docs":null}', '{"ops":{"q":{"docs":[]}}}'):
            with self.subTest(raw=invalid), self.assertRaises(RuntimeError):
                adapter.validate(resolve("COUNT", "skip"), invalid)

    def test_luxir_exact_counts_and_nested_children(self):
        adapter = make_adapter("luxir", "searchbench")
        params = resolve("FACET_NESTED_10_100", "exact")
        q = {"found": 1, "docs": [{"id": "0"}], "ops": {"facet": {"buckets": [
            {"val": "parent", "count": 1,
             "subfacet": {"buckets": [{"val": "child", "count": 1}]}}]}}}
        q["ops"]["q"] = {"found": 999, "ops": {"facet": {"buckets": []}}}
        raw = json.dumps(q)
        self.assertEqual(adapter.validate(params, raw), 1)
        self.assertEqual(adapter.count(raw), 1)
        del q["ops"]["facet"]["buckets"][0]["subfacet"]
        with self.assertRaisesRegex(RuntimeError, "missing nested facet"):
            adapter.validate(params, json.dumps(q))
        for total in (None, True, -1, 1.5, "1"):
            q = {"docs": []} if total is None else {"docs": [], "found": total}
            with self.subTest(total=total), self.assertRaisesRegex(RuntimeError, "found count"):
                adapter.validate(resolve("HIGH_TERM_COUNT", "exact"), json.dumps(q))
        with self.assertRaisesRegex(RuntimeError, "missing exact found count"):
            adapter.validate(resolve("HIGH_TERM_COUNT", "exact"),
                             '{"docs":[],"ops":{"q":{"found":1}}}')
        with self.assertRaisesRegex(RuntimeError, "inconsistent found count"):
            adapter.count('{"docs":[],"found":1}\n{"docs":[],"found":2}')
        self.assertEqual(adapter.count('{"docs":[],"found":1}\n{"docs":[]}'), 1)

    def test_query_driven_grid_moves_selectivity_into_main_query(self):
        with mock.patch.dict(os.environ, {"GRID_FILTER_MODE": "query"}):
            params = resolve_grid_cell("facet", "99", "1M")
        self.assertEqual(params["filter_mode"], "query")

        luxir = json.loads(make_adapter("luxir", "searchbench").build(params, self.union).body)
        top = luxir
        self.assertEqual(top["query"], {"match": {"field": "sel99_s", "val": "t"}})
        self.assertNotIn("filter", top)

        for engine in ("opensearch", "elasticsearch"):
            body = json.loads(make_adapter(engine, "searchbench").build(params, self.union).body)
            self.assertEqual(body["query"], {"term": {"sel99_s": {"value": "t"}}})

    def test_facet_metrics_translate_and_validate(self):
        params = resolve_grid_cell("facet-metric-sort", "99", "1M")
        luxir = json.loads(make_adapter("luxir", "searchbench").build(params, self.union).body)
        facet = luxir["ops"]["facet"]["field_facet"]
        self.assertEqual(facet["sort"], [{"expr": "price_avg", "dir": "desc"}])
        rest = json.loads(
            make_adapter("elasticsearch", "searchbench").build(params, self.union).body)
        self.assertEqual(rest["aggs"]["facet"]["terms"]["order"], {"price_avg": "desc"})
        with self.assertRaisesRegex(ValueError, "only apply to the facet shape"):
            resolve("FACET_DATE", "exact", {"metrics": METRIC1})

    def test_sort_grid_is_not_a_facet(self):
        params = resolve_grid_cell("sort10k", "99", "1M")
        self.assertEqual((params["sort_field"], params["limit"]), ("cat1m_s", 10000))
        self.assertFalse(params["total_hits"])
        for engine in ENGINES:
            body = json.loads(make_adapter(engine, "searchbench").build(params, self.union).body)
            if engine == "luxir":
                top = body
                self.assertEqual(top["sort"], [{"expr": "cat1m_s", "dir": "asc"}])
                self.assertNotIn("ops", top)
            else:
                self.assertEqual(body["sort"], [{"cat1m_s": "asc"}])
                self.assertIs(body["track_total_hits"], False)
                self.assertNotIn("aggs", body)

    def test_comparability_ignores_only_inert_parameters(self):
        serial = resolve_grid_cell("facet", "99", "1M", "exact", 1)
        inline = resolve_grid_cell("facet", "99", "1M", "exact", -1)
        self.assertNotEqual(param_hash(serial), param_hash(inline))
        for engine in ("opensearch", "elasticsearch"):
            inert = make_adapter(engine, "searchbench").inert_params
            self.assertIn("max_parallel", inert)
            self.assertEqual(comparable_hash(serial, inert), comparable_hash(inline, inert))
        luxir_inert = make_adapter("luxir", "searchbench").inert_params
        self.assertNotEqual(comparable_hash(serial, luxir_inert),
                            comparable_hash(inline, luxir_inert))

    def test_segment_topology_translation(self):
        luxir = make_adapter("luxir", "searchbench")
        self.assertIn("_stats?segments=true", luxir.build_topology_probe()[0].path)
        luxir_topology = luxir.parse_topology((json.dumps({
            "collections": [{"shards": [{"index": {
                "totals": {"committed_segments": 2},
                "segments": [
                    {"seg": "s07", "max_doc": 11, "live_docs": 10,
                     "deleted_docs": 1, "committed": True, "bytes": 4096},
                    {"seg": "s09", "max_doc": 5, "live_docs": 5,
                     "committed": True, "merging": False, "bytes": 512},
                ],
            }}]}]
        }).encode(),))
        self.assertEqual(luxir_topology["segment_count"], 2)
        self.assertEqual(luxir_topology["live_docs"], 15)
        self.assertEqual([segment["max_docs"]
                          for segment in luxir_topology["segments"]], [11, 5])
        self.assertEqual([segment["size_bytes"]
                          for segment in luxir_topology["segments"]], [4096, 512])
        self.assertEqual(luxir_topology["size_bytes"], 4608)

        rest = make_adapter("opensearch", "searchbench")
        probes = rest.build_topology_probe()
        self.assertIn("/_segments", probes[0].path)
        self.assertIn("_stats/merge", probes[1].path)
        rest_topology = rest.parse_topology((json.dumps({
            "indices": {"searchbench": {"shards": {"0": [{
                "routing": {"primary": True},
                "num_committed_segments": 1,
                "num_search_segments": 1,
                "segments": {"_a": {
                    "num_docs": 14, "deleted_docs": 2,
                    "size_in_bytes": 1234, "committed": True,
                    "search": True, "compound": False,
                }},
            }]}}}
        }).encode(), json.dumps({
            "indices": {"searchbench": {"primaries": {"merges": {"current": 0}}}}
        }).encode()))
        self.assertEqual(rest_topology["segment_count"], 1)
        self.assertEqual(rest_topology["max_docs"], 16)
        self.assertEqual(rest_topology["size_bytes"], 1234)
        self.assertEqual(rest_topology["active_merges"], 0)

    def test_named_topology_asserts_full_document_distribution(self):
        declared = resolve_topology("tiered-5")
        self.assertEqual(declared.segment_count, 5)
        self.assertEqual(declared.documents, 10_000_000)
        observed = {
            "shard_count": 1,
            "segment_count": 5,
            "max_docs": 10_000_000,
            "live_docs": 10_000_000,
            "deleted_docs": 0,
            "active_merges": 0,
            "segments": [
                {"id": str(index), "max_docs": docs, "live_docs": docs,
                 "deleted_docs": 0, "merging": False}
                for index, docs in enumerate(reversed(declared.segment_docs))
            ],
        }
        assert_topology(declared, observed)
        observed["segments"][0]["max_docs"] -= 1
        with self.assertRaisesRegex(RuntimeError, "segment documents"):
            assert_topology(declared, observed)

    def test_dataset_names_separate_every_fed_shape(self):
        standard = "corpus/corpus-10m-searchbench.ndjson"
        self.assertEqual(dataset_name(standard), "10m-searchbench-merged")
        self.assertEqual(dataset_name(standard, "tiered-45"),
                         "10m-searchbench-tiered-45")
        # Same corpus, three layouts, three directories; and a different
        # corpus never lands in any of them.
        self.assertEqual(
            len({dataset_name(standard), dataset_name(standard, "tiered-45"),
                 dataset_name(standard, "tiered-5"),
                 dataset_name("corpus/corpus-10m-facet.ndjson", "as-fed")}), 4)

    def test_dataset_name_rejects_an_undeclared_layout(self):
        with self.assertRaisesRegex(ValueError, "unknown index layout"):
            dataset_name("corpus/corpus-10m-searchbench.ndjson", "tiered-9")

    def test_tiered_45_is_five_full_tiered_merge_policy_bands(self):
        topology = resolve_topology("tiered-45")
        self.assertEqual(topology.segment_count, 45)
        self.assertEqual(topology.documents, 10_000_000)
        self.assertEqual(topology.segment_docs[:9],
                         (1_000_100,) + (1_000_000,) * 8)
        self.assertEqual(topology.segment_docs[9:18], (100_000,) * 9)
        self.assertEqual(topology.segment_docs[18:27], (10_000,) * 9)
        self.assertEqual(topology.segment_docs[27:36], (1_000,) * 9)
        self.assertEqual(topology.segment_docs[36:], (100,) * 9)
        self.assertIn("100-document", topology.description)

    def test_named_topology_prefixes_reuse_the_declared_vector(self):
        topology = resolve_topology("tiered-5")
        prefix = topology.after_ranges(3)
        self.assertEqual(prefix.segment_docs, topology.segment_docs[:3])
        self.assertEqual(prefix.documents, 8_500_000)
        with self.assertRaises(ValueError):
            topology.after_ranges(0)

    def test_named_topology_feed_commits_exact_streamed_ranges(self):
        topology = Topology("test-2", (2, 3), "test")
        with tempfile.TemporaryDirectory() as temp_dir:
            corpus = Path(temp_dir) / "corpus.ndjson"
            corpus.write_bytes(b"".join(
                json.dumps({"id": str(index)}).encode() + b"\n"
                for index in range(5)))
            args = SimpleNamespace(
                corpus=str(corpus), host="127.0.0.1", port=9400,
                collection="searchbench", optimize=False)
            bodies = []

            def consume(_host, _port, _method, _path, body, _content_type):
                bodies.append(b"".join(body))
                return b'{"status":"ok"}\n'

            observations = []
            for count in range(1, 3):
                docs = topology.segment_docs[:count]
                observations.append({
                    "shard_count": 1, "segment_count": count,
                    "max_docs": sum(docs), "live_docs": sum(docs),
                    "deleted_docs": 0, "active_merges": 0,
                    "segments": [
                        {"id": str(index), "max_docs": size,
                         "live_docs": size, "deleted_docs": 0,
                         "merging": False}
                        for index, size in enumerate(docs)
                    ],
                })
            with mock.patch("feed_luxir.stream_request", side_effect=consume):
                with mock.patch("feed_luxir.index_topology",
                                side_effect=observations):
                    responses, boundaries = feed_topology(args, topology)
        self.assertEqual(responses, [{"status": "ok"}, {"status": "ok"}])
        self.assertEqual([item["total_documents"] for item in boundaries], [2, 5])
        self.assertEqual([body.count(b'"id"') for body in bodies], [2, 3])
        self.assertTrue(all(body.startswith(ALLOW_DUPS_LINE) for body in bodies))
        self.assertTrue(all(b'"wait_for_merges":true' in body for body in bodies))
        self.assertIn(b'"max_segments":1', bodies[0])
        self.assertIn(b'"max_segments":2', bodies[1])

    def test_rest_named_topology_asserts_every_force_merge_boundary(self):
        topology = Topology("test-2", (2, 3), "test")
        observations = []
        for count in range(1, 3):
            docs = topology.segment_docs[:count]
            observations.append({
                "shard_count": 1,
                "segment_count": count,
                "max_docs": sum(docs),
                "live_docs": sum(docs),
                "deleted_docs": 0,
                "active_merges": 0,
                "segments": [
                    {"id": str(index), "max_docs": size, "live_docs": size,
                     "deleted_docs": 0, "merging": False}
                    for index, size in enumerate(docs)
                ],
            })
        with tempfile.TemporaryDirectory() as temp_dir:
            corpus = Path(temp_dir) / "corpus.ndjson"
            corpus.write_bytes(b"".join(
                json.dumps({"id": str(index)}).encode() + b"\n"
                for index in range(5)))
            args = SimpleNamespace(
                engine="elasticsearch", corpus=str(corpus), host="127.0.0.1",
                port=9202, index="searchbench", batch_docs=2, clients=1)
            sent = []

            def send_bulk(body):
                sent.append(body)
                return 1

            with mock.patch("feed_rest.request_json", return_value={
                    "_shards": {"failed": 0}}) as request:
                with mock.patch("feed_rest.index_topology",
                                side_effect=observations):
                    totals, boundaries = feed_rest_topology(
                        args, topology, send_bulk)

        self.assertEqual(totals["documents"], 5)
        self.assertEqual([item["total_documents"] for item in boundaries], [2, 5])
        paths = [call.args[3] for call in request.call_args_list]
        self.assertIn("/searchbench/_forcemerge?max_num_segments=1&flush=true", paths)
        self.assertIn("/searchbench/_forcemerge?max_num_segments=2&flush=true", paths)
        self.assertEqual(sum(body.count(b'"index"') for body in sent), 5)

    def test_synthetic_fields_are_deterministic_and_bounded(self):
        first = synthetic_fields("doc-1", "title")
        self.assertEqual(first, synthetic_fields("doc-1", "title"))
        self.assertNotEqual(first, synthetic_fields("doc-2", "title"))
        self.assertLessEqual(first["price_i"], 100000)

    def test_luceneutil_lzma_line_docs_preserve_raw_body_and_ordinals(self):
        rows = [LUCENEUTIL_HEADER + "\n",
                "Anarchism\t30-APR-2012 03:25:17.000\t"
                "{{Redirect}} Cafe C++ \u0130 DOG\tzh\n",
                "Anarchism\t30-APR-2012 03:25:17.000\tsecond chunk\tfoo\n"]
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "source.txt.lzma"
            with lzma.open(path, "wt", encoding="utf-8") as destination:
                destination.writelines(rows)
            source = list(input_documents([path], "luceneutil-line-docs"))
        self.assertEqual(source[0], {
            "id": "0", "title": "Anarchism",
            "body": "{{Redirect}} Cafe C++ \u0130 DOG",
            "sort_i": ordinal_sort_value(0),
        })
        self.assertEqual(source[1]["id"], "1")
        self.assertNotEqual(source[0]["sort_i"], source[1]["sort_i"])
        self.assertEqual(transform_doc(source[0])["body"],
                         "{{Redirect}} Cafe C++ \u0130 DOG")

    def test_full_text_corpus_requires_raw_body(self):
        with self.assertRaises(KeyError):
            transform_doc({"id": "doc-1", "text": "legacy text"})

    def test_facet_corpus_drops_only_full_text(self):
        source = {"id": "doc-1", "title": "One", "body": "alpha beta"}
        full = transform_doc(source)
        facet = transform_doc(source, "facet")
        self.assertNotIn("body", facet)
        self.assertEqual(facet, {key: value for key, value in full.items()
                                 if key != "body"})

    def test_text_corpus_drops_only_synthetic_fields(self):
        source = {"id": "doc-1", "title": "One", "body": "alpha beta"}
        self.assertEqual(transform_doc(source, "text"),
                         {"id": "doc-1", "body": "alpha beta"})

    def test_default_cpu_layout_reserves_two_physical_cores_for_client(self):
        siblings = ({cpu, cpu + 16} for cpu in range(16))
        server, client = choose_cpu_sets(set(range(32)), siblings)
        self.assertEqual(client, {14, 15, 30, 31})
        self.assertEqual(server, set(range(32)) - client)
        self.assertEqual(format_cpu_list(server), "0-13,16-29")
        self.assertEqual(parse_cpu_list("0-13,16-29"), server)

    def test_running_binary_provenance(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            pidfile = Path(temp_dir) / "luxir.pid"
            pidfile.write_text(str(os.getpid()), encoding="utf-8")
            binary, binary_sha256 = resolve_running_binary(pidfile)
        self.assertEqual(binary, os.readlink("/proc/self/exe"))
        self.assertEqual(binary_sha256, file_sha256(sys.executable))

        args = SimpleNamespace(host="127.0.0.1", port=9400, server_cores="0",
                               server_pid=os.getpid())
        with mock.patch("driver.resolve_running_binary", return_value=None), \
                mock.patch("driver.git_head", return_value="configured-head"), \
                mock.patch("driver.git_dirty", return_value=False), \
                mock.patch.dict(os.environ, {"LUXIR_BIN": sys.executable}):
            config = server_config("luxir", {}, args)
        self.assertEqual(config["binary_provenance"], "env")
        self.assertEqual(config["version"], "configured-head")
        self.assertEqual(config["index_layout_version"], index_layout_version("luxir"))
        self.assertEqual(config["full_text"], {
            "field": "body", "tokenizer": "unicode_word", "filters": ["lowercase"],
            "long_terms": "truncate"})
        self.assertEqual(config["identity"], IDENTITY_POSTURE["luxir"])
        self.assertGreaterEqual(config["nofile_soft"], 1)
        self.assertGreaterEqual(config["nofile_hard"], config["nofile_soft"])
        self.assertEqual(config["startup"]["pid"], os.getpid())
        self.assertEqual(config["startup"]["executable"], os.readlink("/proc/self/exe"))
        self.assertTrue(config["startup"]["argv"])
        self.assertEqual(len(config["startup"]["cmdline_sha256"]), 64)


if __name__ == "__main__":
    unittest.main()
