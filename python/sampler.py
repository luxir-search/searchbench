"""100ms /proc status memory sampler."""

import csv
import threading
import time


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
