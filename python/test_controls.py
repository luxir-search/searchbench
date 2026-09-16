import copy
import json
import os
from pathlib import Path
import socket
import tempfile
import unittest

from adapters import make_adapter
from controls import health_control_lines, record_health_control
from driver import make_workload, materialize_workload
from presets import resolve
from report import load_results, ordered_variants


class HealthControlTest(unittest.TestCase):
    def test_health_request_and_validation_need_no_queries_or_index(self):
        adapter = make_adapter("luxir", "not-an-index")
        params = resolve("HEALTH", "exact")
        workload = make_workload("HEALTH", params, None, 0, adapter)
        self.assertEqual(len(workload), 1)
        entry, item = workload[0]
        request = adapter.build(entry, item)
        self.assertEqual((request.method, request.path, request.body), ("GET", "/health", b""))
        self.assertIsNone(adapter.validate(entry, b'{"status":"ok"}', item))
        for body in (b'{}', b'{"status":"failed"}', b'{"error":"broken"}'):
            with self.assertRaises(RuntimeError):
                adapter.validate(entry, body, item)
        self.assertIn(("absent", b'"status":"ok"'), adapter.fail_rules("health"))
        with tempfile.TemporaryDirectory() as tmp:
            path, labels, captures = materialize_workload(
                workload, adapter, "127.0.0.1", 9400, Path(tmp) / "workload.sbwl")
            self.assertIn(b"GET /health HTTP/1.1\r\n", Path(path).read_bytes())
            self.assertEqual(labels, ["HEALTH"])
            self.assertEqual(captures["HEALTH"]["body"], "")

    @staticmethod
    def health():
        return {
            "engine": "luxir", "task_class": "HEALTH",
            "variant": {"concurrency": 32, "threads": 4},
            "run_config": {"concurrency": 32, "replay_threads": 4,
                           "repetitions": 3, "duration_per_repetition_s": 10},
            "aggregate": {"qps": 499_990, "repetition_qps_median": 500_000,
                          "repetition_qps_min": 490_000, "repetition_qps_max": 510_000,
                          "client_cpu": {"cores_busy": 3.96}, "errors": 0},
            "core_split": {"client": "14-15,30-31", "server": "0-13,16-29"},
            "source_files": {"replay_driver": {"sha256": "d" * 64}},
            "host": {"platform": "test-kernel", "network": {"mode": "isolated-loopback"}},
            "server_session": {"pid": 123, "start_ticks": 100, "boot_id": "boot"},
            "engine_config": {"binary_sha256": "e" * 64},
            "created_at_utc": "2026-09-16T20:00:00+00:00",
            "errors": [],
        }

    def test_report_shows_control_without_adding_query_variants(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "controls").mkdir()
            (root / "controls/luxir-HEALTH@c32+t4.json").write_text(json.dumps(self.health()))
            query = {"engine": "luxir", "task_class": "HIGH_TERM_COUNT", "variant": {}}
            (root / "luxir-HIGH_TERM_COUNT.json").write_text(json.dumps(query))
            # Even a manually placed health result is not a search cell.
            (root / "luxir-HEALTH.json").write_text(json.dumps(self.health()))
            results = load_results(root)
            self.assertEqual(len(results), 1)
            self.assertEqual(ordered_variants(results), ["{}"])
            text = "\n".join(health_control_lines(root))
            self.assertIn("500,000", text)
            self.assertIn("490,000-510,000", text)
            self.assertIn("3.96", text)
            self.assertIn("3 x 10s", text)
            self.assertIn("isolated-loopback", text)
            self.assertIn("test-kernel", text)
            self.assertIn("d" * 64, text)
            self.assertIn("controls/luxir-HEALTH@c32+t4.json", text)

    def test_failed_and_missing_controls_do_not_report_valid_qps(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.assertIn("Not recorded", "\n".join(health_control_lines(root)))
            self.assertEqual(health_control_lines(root, expected=False), [])
            (root / "controls").mkdir()
            path = root / "controls/luxir-HEALTH.json"
            for field, value in (("errors", ["connection failed"]), ("failure", "driver failed")):
                failed = dict(self.health(), **{field: value})
                path.write_text(json.dumps(failed))
                text = "\n".join(health_control_lines(root))
                self.assertIn("FAIL", text)
                self.assertNotIn("500,000", text)
            bad_posture = self.health()
            bad_posture["run_config"]["replay_threads"] = 1
            path.write_text(json.dumps(bad_posture))
            self.assertIn("FAIL", "\n".join(health_control_lines(root)))

    def test_control_must_follow_and_match_reported_query_session(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "controls").mkdir()
            control = self.health()
            control["host"]["cpu_governors"] = ["performance"]
            query = copy.deepcopy(control)
            query.update(task_class="HIGH_TERM_COUNT", variant={},
                         created_at_utc="2026-09-16T19:00:00+00:00")
            path = root / "controls/luxir-HEALTH.json"
            path.write_text(json.dumps(control))
            self.assertIn("500,000", "\n".join(health_control_lines(root, queries=[query])))
            cases = [
                ("server_session", {"pid": 456, "start_ticks": 101, "boot_id": "boot"}),
                ("source_files", {"replay_driver": {"sha256": "f" * 64}}),
                ("created_at_utc", "2026-09-16T21:00:00+00:00"),
                ("host", {"platform": "new-kernel", "network": {"mode": "unmanaged"}}),
                ("core_split", {"client": "0", "server": "1"}),
            ]
            for key, value in cases:
                with self.subTest(key=key):
                    changed = dict(query, **{key: value})
                    text = "\n".join(health_control_lines(root, queries=[changed]))
                    self.assertIn("UNMATCHED", text)
                    self.assertNotIn("500,000", text)
            other_posture = dict(query, variant={"heap": "2g"})
            text = "\n".join(health_control_lines(root, queries=[query, other_posture]))
            self.assertIn("No health control recorded", text)
            self.assertIn("heap=2g", text)

    def test_failed_rerun_archives_old_success(self):
        # A bound, non-listening socket reliably rejects connection attempts.
        # Run the real command path; no server or replay process is launched.
        with tempfile.TemporaryDirectory() as tmp, socket.socket() as closed:
            closed.bind(("127.0.0.1", 0))
            root = Path(tmp)
            (root / "controls").mkdir()
            path = root / "controls/luxir-HEALTH@c32+t4.json"
            previous = json.dumps(self.health())
            path.write_text(previous)
            status = record_health_control("luxir", os.getpid(), closed.getsockname()[1], root)
            self.assertEqual(status, 1)
            self.assertEqual(path.with_suffix(".json.prev").read_text(), previous)
            self.assertIn("failure", json.loads(path.read_text()))
            text = "\n".join(health_control_lines(root))
            self.assertIn("FAIL", text)
            self.assertNotIn("500,000", text)


if __name__ == "__main__":
    unittest.main()
