"""Memory samplers: /proc status for every engine, allocator statistics opt-in.

RssAnon is the cross-engine memory column. AllocatorSampler is a diagnostic
for comparing one engine's builds: it polls an engine's allocator statistics
(Luxir's `/_stats?memory=true` jemalloc block), which separate the live bytes
the engine allocated from freed pages awaiting decay. Each poll is server work
inside the measured interval, so it is off by default and both arms of an A/B
must use the same setting.
"""

import csv
import http.client
import json
import threading
import time

ALLOCATOR_FIELDS = ("allocated", "active", "resident")


class ProcSampler:
    def __init__(self, pid, output_path, interval=0.1):
        self.pid = pid
        self.output_path = output_path
        self.interval = interval
        self.stop_event = threading.Event()
        self.thread = None
        self.peak_vmrss_kb = 0
        self.peak_rssanon_kb = 0
        self.samples = 0
        self.error = None

    def start(self):
        self.thread = threading.Thread(target=self._run, name="proc-status-sampler", daemon=True)
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        if self.thread:
            self.thread.join()
        return {"pid": self.pid, "interval_ms": int(self.interval * 1000),
                "samples": self.samples, "peak_vmrss_kb": self.peak_vmrss_kb,
                "peak_rssanon_kb": self.peak_rssanon_kb, "timeline_csv": self.output_path,
                "error": self.error}

    def _run(self):
        started = time.monotonic()
        try:
            with open(self.output_path, "w", newline="", encoding="utf-8") as output:
                writer = csv.writer(output)
                writer.writerow(("elapsed_s", "epoch_s", "VmRSS_kb", "RssAnon_kb"))
                while not self.stop_event.is_set():
                    values = {}
                    with open(f"/proc/{self.pid}/status", encoding="ascii") as status:
                        for line in status:
                            if line.startswith(("VmRSS:", "RssAnon:")):
                                key, rest = line.split(":", 1)
                                values[key] = int(rest.split()[0])
                    vmrss = values.get("VmRSS", 0)
                    rssanon = values.get("RssAnon", 0)
                    self.peak_vmrss_kb = max(self.peak_vmrss_kb, vmrss)
                    self.peak_rssanon_kb = max(self.peak_rssanon_kb, rssanon)
                    self.samples += 1
                    writer.writerow((f"{time.monotonic() - started:.6f}", f"{time.time():.6f}", vmrss, rssanon))
                    output.flush()
                    self.stop_event.wait(self.interval)
        except Exception as error:
            self.error = repr(error)


class AllocatorSampler:
    """Polls `memory.jemalloc.<field>_bytes` from a JSON stats endpoint.

    A failed poll is counted and retried on a fresh connection; peaks are
    None when no poll succeeded."""

    def __init__(self, host, port, path, output_path, interval, timeout=1.0):
        self.host = host
        self.port = port
        self.path = path
        self.output_path = output_path
        self.interval = interval
        self.timeout = timeout
        self.stop_event = threading.Event()
        self.thread = None
        self.peaks_kb = dict.fromkeys(ALLOCATOR_FIELDS)
        self.samples = 0
        self.errors = 0
        self.last_error = None

    def start(self):
        self.thread = threading.Thread(target=self._run, name="allocator-sampler", daemon=True)
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        if self.thread:
            self.thread.join()
        return {"interval_ms": int(self.interval * 1000), "samples": self.samples,
                "errors": self.errors, "last_error": self.last_error,
                "timeline_csv": self.output_path,
                **{f"peak_{field}_kb": self.peaks_kb[field] for field in ALLOCATOR_FIELDS}}

    def _poll(self, connection):
        connection.request("GET", self.path)
        response = connection.getresponse()
        body = response.read()
        if response.status != 200:
            raise RuntimeError(f"HTTP {response.status}")
        jemalloc = json.loads(body).get("memory", {}).get("jemalloc")
        if jemalloc is None:
            raise RuntimeError("response has no memory.jemalloc section")
        return {field: int(jemalloc.get(f"{field}_bytes", 0)) // 1024
                for field in ALLOCATOR_FIELDS}

    def _run(self):
        started = time.monotonic()
        connection = None
        try:
            with open(self.output_path, "w", newline="", encoding="utf-8") as output:
                writer = csv.writer(output)
                writer.writerow(("elapsed_s", "epoch_s")
                                + tuple(f"jemalloc_{field}_kb" for field in ALLOCATOR_FIELDS))
                while not self.stop_event.is_set():
                    try:
                        if connection is None:
                            connection = http.client.HTTPConnection(
                                self.host, self.port, timeout=self.timeout)
                        sample = self._poll(connection)
                    except Exception as error:
                        self.errors += 1
                        self.last_error = repr(error)
                        if connection is not None:
                            connection.close()
                        connection = None
                    else:
                        for field, value in sample.items():
                            peak = self.peaks_kb[field]
                            self.peaks_kb[field] = value if peak is None else max(peak, value)
                        self.samples += 1
                        writer.writerow((f"{time.monotonic() - started:.6f}", f"{time.time():.6f}")
                                        + tuple(sample[field] for field in ALLOCATOR_FIELDS))
                        output.flush()
                    self.stop_event.wait(self.interval)
        except Exception as error:
            self.errors += 1
            self.last_error = repr(error)
        finally:
            if connection is not None:
                connection.close()
