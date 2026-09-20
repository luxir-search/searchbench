"""Campaign controls measured through the ordinary native replay driver."""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import sys

from results_io import load_result
from variants import filename, parse_variant, server_settings, slug

ROOT = Path(__file__).resolve().parents[1]
HEALTH_CONCURRENCY = 32
HEALTH_THREADS = 4
HEALTH_DURATION = 10
HEALTH_REPETITIONS = 3


def record_health_control(engine, server_pid, port, outdir, variant="-",
                          server_cores=None, host="127.0.0.1"):
    if engine != "luxir":
        return 0
    posture = server_settings(parse_variant(variant))
    control_variant = dict(posture, concurrency=HEALTH_CONCURRENCY, threads=HEALTH_THREADS)
    spec = ",".join(f"{key}={json.dumps(value)}" for key, value in control_variant.items())
    output = Path(outdir) / "controls" / filename("luxir", "HEALTH", control_variant)
    output.parent.mkdir(parents=True, exist_ok=True)
    # A failed rerun must never leave a previous successful measurement in
    # the report. Context files are immutable and stay beside both generations.
    if output.exists():
        output.replace(output.with_suffix(".json.prev"))
    command = [str(ROOT / "scripts/run-driver.sh"), "luxir", "HEALTH", "--lane", "exact",
               "--host", host, "--port", str(port), "--server-pid", str(server_pid),
               "--variant", spec, "--duration", str(HEALTH_DURATION),
               "--repetitions", str(HEALTH_REPETITIONS), "--label", "health-control",
               "--output", str(output)]
    if server_cores is not None:
        command += ["--server-cores", server_cores]
    print("Luxir transport control: GET /health, c32/t4, 3 x 10s", flush=True)
    with output.with_suffix(".log").open("w") as log:
        completed = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT)
    if completed.returncode:
        if not output.exists():
            output.write_text(json.dumps({
                "engine": "luxir", "task_class": "HEALTH", "variant": control_variant,
                "created_at_utc": datetime.now(timezone.utc).isoformat(),
                "failure": f"Health control exited {completed.returncode}; see {output.stem}.log",
            }, indent=2) + "\n", encoding="utf-8")
        print(f"FAILED: Luxir health control; see {output.with_suffix('.log')}", file=sys.stderr)
        return 1
    print(f"Luxir health control saved: {output}", flush=True)
    return 0


def control_mismatches(control, queries):
    """A control belongs to the serving session and cells it actually followed."""
    posture = server_settings(control.get("variant", {}))
    peers = [query for query in queries if query.get("engine") == "luxir"
             and server_settings(query.get("variant", {})) == posture]
    if not peers:
        return ["no matching Luxir server variant in the report"]

    def identity(value):
        host = value.get("host", {})
        cores = value.get("core_split", {})
        return {
            "server session": value.get("server_session"),
            "Luxir binary": value.get("engine_config", {}).get("binary_sha256"),
            "replay binary": value.get("source_files", {}).get("replay_driver", {}).get("sha256"),
            "client CPUs": cores.get("client"), "server CPUs": cores.get("server"),
            "OS": host.get("platform"), "network": host.get("network"),
            "CPU governors": host.get("cpu_governors"),
        }

    actual = identity(control)
    differences = set()
    for query in peers:
        for name, expected in identity(query).items():
            if expected is None or actual[name] is None or expected != actual[name]:
                differences.add(f"{name} differs or is unrecorded")
        when = query.get("created_at_utc")
        if not when or not control.get("created_at_utc") or control["created_at_utc"] < when:
            differences.add("control does not follow every reported cell for this server variant")
    return sorted(differences)


def health_control_lines(directory, expected=True, queries=None):
    if not expected:
        return []
    lines = ["## Luxir transport control (c32/t4)", "",
             "`GET /health` uses the same native HTTP replay driver and client/server "
             "CPU sets as the search cells. It measures client, kernel/network, and "
             "Luxir HTTP handling without index work. QPS is the median of measured "
             "repetitions; search timings are not rescaled by this control.", ""]
    paths = sorted((Path(directory) / "controls").glob("luxir-HEALTH*.json"))
    if not paths:
        return lines + ["Not recorded for this run."]
    lines += ["| Server variant | Median QPS | Rep QPS min-max | Client cores busy | Reps x seconds | Network | Result |",
              "|---|---:|---:|---:|---|---|---|"]
    details = []
    measured_postures = set()
    for path in paths:
        # A path, not a link: the report is also rendered away from its directory.
        link = f"`controls/{path.name}`"
        try:
            value = load_result(path)
        except (OSError, ValueError) as error:
            lines.append(f"| unknown | FAIL | | | | | {link} |")
            details.append(f"> WARNING: Unreadable health control {path.name}: {error}")
            continue
        aggregate = value.get("aggregate", {})
        run = value.get("run_config", {})
        posture = slug(server_settings(value.get("variant", {}))) or "default"
        measured_postures.add(posture)
        host = value.get("host", {})
        failed = bool(value.get("failure") or value.get("errors") or aggregate.get("errors"))
        fixed = (run.get("concurrency") == HEALTH_CONCURRENCY
                 and run.get("replay_threads") == HEALTH_THREADS)
        qps = aggregate.get("repetition_qps_median")
        mismatches = (control_mismatches(value, queries)
                      if queries is not None and not failed and fixed and qps is not None else [])
        valid = not failed and fixed and qps is not None and not mismatches
        minimum = aggregate.get("repetition_qps_min")
        maximum = aggregate.get("repetition_qps_max")
        spread = f"{minimum:,.0f}-{maximum:,.0f}" if valid and minimum is not None and maximum is not None else ""
        cpu = aggregate.get("client_cpu", {}).get("cores_busy")
        cpu_text = f"{cpu:.2f}" if cpu is not None and valid else ""
        protocol = f"{run.get('repetitions', '?')} x {run.get('duration_per_repetition_s', '?')}s"
        network = host.get("network", {}).get("mode", "unrecorded")
        qps_text = f"{qps:,.0f}" if valid else "UNMATCHED" if mismatches else "FAIL"
        lines.append(f"| {posture} | {qps_text} | {spread} | {cpu_text} | {protocol} | {network} | {link} |")
        if mismatches:
            details.append(f"> WARNING: Health control {path.name}: {'; '.join(mismatches)}.")
        elif not valid:
            details.append(f"> WARNING: Health control {path.name} failed or lacks valid c32/t4 measurements.")
        cores = value.get("core_split", {})
        driver = value.get("source_files", {}).get("replay_driver", {})
        engine = value.get("engine_config", {})
        details += [f"- `{posture}` at {value.get('created_at_utc', 'unknown time')}: "
                    f"client CPUs `{cores.get('client', 'unknown')}`, "
                    f"server CPUs `{cores.get('server', 'unknown')}`; "
                    f"OS `{host.get('platform', 'unknown')}`.",
                    f"  Replay SHA-256 `{driver.get('sha256', 'unrecorded')}`; "
                    f"Luxir SHA-256 `{engine.get('binary_sha256', 'unrecorded')}`."]
    if queries is not None:
        needed = {slug(server_settings(query.get("variant", {}))) or "default"
                  for query in queries if query.get("engine") == "luxir"}
        for posture in sorted(needed - measured_postures):
            details.append(f"> WARNING: No health control recorded for Luxir server variant `{posture}`.")
    return lines + [""] + details


def main():
    parser = argparse.ArgumentParser(description="Record the fixed Luxir c32/t4 health control")
    parser.add_argument("engine")
    parser.add_argument("--server-pid", type=int, required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--outdir", required=True)
    parser.add_argument("--variant", default="-")
    parser.add_argument("--server-cores")
    args = parser.parse_args()
    return record_health_control(**vars(args))


if __name__ == "__main__":
    sys.exit(main())
