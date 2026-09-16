"""Real-process checks for namespace isolation and campaign cleanup."""

import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest

from network_namespace import network_namespace
from report import undeclared_variation

ROOT = Path(__file__).resolve().parents[1]


class NetworkReportTest(unittest.TestCase):
    def test_compare_modes_not_namespace_ids(self):
        results = {
            (engine, "HIGH_TERM_COUNT", "{}"): {"host": {"network": {
                "mode": "isolated-loopback", "client_namespace": f"net:[{index}]",
                "server_namespace": f"net:[{index}]"}}}
            for index, engine in enumerate(("luxir", "elasticsearch"))}
        self.assertEqual(undeclared_variation(results), [])
        reference = results[("elasticsearch", "HIGH_TERM_COUNT", "{}")]
        reference["host"]["network"]["mode"] = "unmanaged"
        self.assertIn("network_mode", "\n".join(undeclared_variation(results)))
        reference.clear()
        warning = "\n".join(undeclared_variation(results))
        self.assertIn("network_mode", warning)
        self.assertIn("unrecorded", warning)


class NetworkNamespaceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not all(shutil.which(name) for name in ("unshare", "setpriv", "ip")):
            raise unittest.SkipTest("needs unshare, setpriv, and ip")
        check = subprocess.run(
            ["unshare", "--user", "--map-current-user", "--net", "true"],
            capture_output=True, text=True)
        if check.returncode:
            raise unittest.SkipTest(f"user/network namespaces unavailable: {check.stderr}")

    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        for relative in ("scripts/run-isolated.sh", "python/network_namespace.py"):
            target = self.root / relative
            target.parent.mkdir(exist_ok=True)
            shutil.copy2(ROOT / relative, target)
        (self.root / "run").mkdir()
        self.runner = str(self.root / "scripts/run-isolated.sh")

    def run_python(self, source, *args):
        return subprocess.run([self.runner, sys.executable, "-c", source, *args],
                              capture_output=True, text=True, timeout=30)

    def test_private_loopback_preserves_uid_and_drops_capabilities(self):
        # Bind the same port on the host and in the new namespace. Exchange
        # bytes inside it, while the host listener receives no connection.
        with socket.socket() as host:
            host.bind(("127.0.0.1", 0))
            host.listen()
            result = self.run_python('''
import json, os, socket
from pathlib import Path
with socket.socket() as server, socket.socket() as client:
    server.bind(("127.0.0.1", int(__import__("sys").argv[1])))
    server.listen()
    client.connect(server.getsockname())
    peer, _ = server.accept()
    with peer:
        client.sendall(b"hello")
        assert peer.recv(5) == b"hello"
caps = {line.split(":")[0]: int(line.split(":")[1], 16)
        for line in Path("/proc/self/status").read_text().splitlines()
        if line.startswith("Cap")}
print(json.dumps({"uid": os.getuid(), "gid": os.getgid(), "caps": caps,
                  "ns": os.readlink("/proc/self/ns/net"),
                  "mode": os.environ["SEARCHBENCH_NETWORK_MODE"]}))
''', str(host.getsockname()[1]))
            self.assertEqual(result.returncode, 0, result.stderr)
            record = json.loads(result.stdout)
            self.assertEqual((record["uid"], record["gid"]), (os.getuid(), os.getgid()))
            self.assertNotEqual(record["ns"], network_namespace())
            self.assertEqual(record["mode"], "isolated-loopback")
            for name in ("CapInh", "CapPrm", "CapEff", "CapAmb"):
                self.assertEqual(record["caps"][name], 0)
            host.settimeout(0.05)
            with self.assertRaises(socket.timeout):
                host.accept()

    def test_failure_stops_detached_engine_and_clears_pidfile(self):
        pidfile = self.root / "run/luxir.pid"
        result = self.run_python('''
import subprocess, sys
from pathlib import Path
child = subprocess.Popen(["sleep", "60"], start_new_session=True,
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
Path(sys.argv[1]).write_text(str(child.pid))
print(child.pid, flush=True)
sys.exit(23)
''', str(pidfile))
        self.assertEqual(result.returncode, 23, result.stderr)
        self.assertIsNone(network_namespace(int(result.stdout)))
        self.assertFalse(pidfile.exists())

    def test_uid_other_than_1000(self):
        result = subprocess.run(
            ["unshare", "--user", "--map-user=2345", "--map-group=2345",
             self.runner, sys.executable, "-c",
             "import os; print(os.getuid(), os.getgid())"],
            capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "2345 2345")

    def test_sigterm_forces_cleanup_of_uncooperative_child(self):
        ready = self.root / "ready"
        source = '''
import os, signal, sys, time
from pathlib import Path
signal.signal(signal.SIGTERM, signal.SIG_IGN)
Path(sys.argv[1]).write_text(str(os.getpid()))
time.sleep(60)
'''
        runner = subprocess.Popen([self.runner, sys.executable, "-c", source, str(ready)],
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            deadline = time.monotonic() + 10
            while not ready.exists() and runner.poll() is None and time.monotonic() < deadline:
                time.sleep(0.02)
            self.assertTrue(ready.exists())
            pid = int(ready.read_text())
            overlap = self.run_python('raise RuntimeError("must not run")')
            self.assertNotEqual(overlap.returncode, 0)
            self.assertIn("another isolated Searchbench command", overlap.stderr)
            self.assertIsNotNone(network_namespace(pid))
            runner.terminate()
            _, error = runner.communicate(timeout=20)
            self.assertEqual(runner.returncode, 128 + signal.SIGTERM, error)
            self.assertIsNone(network_namespace(pid))
        finally:
            if runner.poll() is None:
                runner.terminate()
                runner.communicate(timeout=20)

    def test_existing_pidfile_does_not_stop_host_process(self):
        with subprocess.Popen(["sleep", "60"]) as host:
            try:
                (self.root / "run/luxir.pid").write_text(str(host.pid))
                result = self.run_python('raise RuntimeError("must not run")')
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("stop the existing engine first", result.stderr)
                self.assertIsNone(host.poll())
                self.assertTrue((self.root / "run/luxir.pid").exists())
            finally:
                host.terminate()


if __name__ == "__main__":
    unittest.main()
