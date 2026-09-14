#!/usr/bin/env python3
"""Choose disjoint default server and client CPU sets from Linux topology."""

import argparse
import os
from pathlib import Path


def parse_cpu_list(spec):
    cpus = set()
    for part in spec.strip().split(","):
        if not part:
            continue
        bounds = part.split("-", 1)
        if len(bounds) == 1:
            cpus.add(int(bounds[0]))
        else:
            cpus.update(range(int(bounds[0]), int(bounds[1]) + 1))
    return cpus


def format_cpu_list(cpus):
    values = sorted(cpus)
    ranges = []
    start = previous = values[0]
    for value in values[1:]:
        if value == previous + 1:
            previous = value
            continue
        ranges.append(str(start) if start == previous else f"{start}-{previous}")
        start = previous = value
    ranges.append(str(start) if start == previous else f"{start}-{previous}")
    return ",".join(ranges)


def choose_cpu_sets(allowed, sibling_groups):
    allowed = set(allowed)
    if not allowed:
        raise ValueError("CPU affinity is empty")
    groups = {tuple(sorted(set(group) & allowed)) for group in sibling_groups}
    groups.discard(())
    covered = {cpu for group in groups for cpu in group}
    groups.update((cpu,) for cpu in allowed - covered)
    # Two physical cores (sibling groups) for the client: one core's worth of
    # replay threads saturates near 200k qps/thread, below what a served
    # engine can now sustain on the remaining cores.
    ordered = sorted(groups, key=lambda group: (max(group), len(group)), reverse=True)
    client = set()
    for group in ordered[:2]:
        client.update(group)
    server = allowed - client
    if not server:
        client = set(ordered[0])
        server = allowed - client
    if not server:
        return allowed, allowed
    return server, client


def current_cpu_sets():
    allowed = set(os.sched_getaffinity(0))
    groups = []
    for cpu in sorted(allowed):
        path = Path(f"/sys/devices/system/cpu/cpu{cpu}/topology/thread_siblings_list")
        try:
            groups.append(parse_cpu_list(path.read_text(encoding="utf-8")))
        except OSError:
            groups.append({cpu})
    return choose_cpu_sets(allowed, groups)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("role", choices=("server", "client"))
    args = parser.parse_args()
    server, client = current_cpu_sets()
    print(format_cpu_list(server if args.role == "server" else client))


if __name__ == "__main__":
    main()
