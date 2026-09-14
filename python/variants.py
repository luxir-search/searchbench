"""Variants: named deviations from a campaign's baseline cell.

A measured cell is (engine, task, variant). The variant is the experiment
dimension mechanism: a small key=value set, spec'd as comma-separated pairs
("concurrency=16", "concurrency=8,max_parallel=0"). "-" or the empty spec is
the baseline (no deviations). Values must not contain commas or spaces; for
anything richer, drive driver.py directly with --set.

Keys span three layers, resolved here so every caller agrees:
  - CLIENT_KEYS: replay-driver posture (concurrency, threads). Applied by
    driver.py; values equal to the driver default are dropped during parsing
    so "concurrency=8" and the baseline are the same cell.
  - SERVER_KEYS: server-start posture, exported as environment variables for
    the start scripts. A key an engine does not implement (heap on luxir) is
    inert there, mirroring inert task parameters; the captured /proc startup
    records what actually ran.
  - everything else: task-parameter overrides, applied through
    presets.resolve() exactly like --set.

Comparability policy: a variant is a declared difference. Report tooling
pivots on declared differences and warns about undeclared ones; nothing
refuses to tabulate. param_hash remains a recorded sanity check, not a gate.
"""

import json
import sys

from presets import parse_override

# key -> driver default (an explicit default is normalized away)
CLIENT_KEYS = {"concurrency": 8, "threads": 1}
# key -> environment variable the start scripts consume
SERVER_KEYS = {"heap": "SEARCHBENCH_HEAP"}
# slug abbreviations for the common ladder keys; other keys render key=value
SLUG_ABBREV = {"concurrency": "c", "threads": "t"}


def parse_variant(spec):
    """Parse a variant spec into its canonical dict. "" and "-" are baseline."""
    spec = (spec or "").strip()
    if spec in ("", "-"):
        return {}
    variant = {}
    for pair in spec.split(","):
        key, value = parse_override(pair.strip())
        if key in variant:
            raise ValueError(f"duplicate variant key {key!r} in {spec!r}")
        if key in CLIENT_KEYS:
            if not isinstance(value, int) or value < 1:
                raise ValueError(f"variant {key} must be a positive integer, got {value!r}")
            if value == CLIENT_KEYS[key]:
                continue
        variant[key] = value
    return variant


def client_settings(variant):
    return {key: value for key, value in variant.items() if key in CLIENT_KEYS}


def server_settings(variant):
    return {key: value for key, value in variant.items() if key in SERVER_KEYS}


def param_settings(variant):
    return {key: value for key, value in variant.items()
            if key not in CLIENT_KEYS and key not in SERVER_KEYS}


def slug(variant):
    """Deterministic human-readable slug; empty for the baseline."""
    parts = []
    for key in sorted(variant):
        value = json.dumps(variant[key]) if isinstance(variant[key], (list, dict)) \
            else str(variant[key])
        text = f"{SLUG_ABBREV[key]}{value}" if key in SLUG_ABBREV else f"{key}={value}"
        parts.append("".join(ch if ch.isalnum() or ch in "._=-" else "_"
                             for ch in text))
    return "+".join(parts)


def filename(engine, task, variant):
    tail = slug(variant)
    return f"{engine}-{task}@{tail}.json" if tail else f"{engine}-{task}.json"


def server_env(variant):
    """Space-joined NAME=value exports for the start scripts."""
    return " ".join(f"{SERVER_KEYS[key]}={value}"
                    for key, value in sorted(server_settings(variant).items()))


def postures(specs):
    """Group specs by server posture: [(env_string, [spec, ...]), ...].

    Baseline posture first, then sorted, so feeding and index verification
    happen under the standard server configuration. Specs that normalize to
    the same variant are deduplicated (first spelling wins).
    """
    groups = {}
    seen = set()
    for spec in specs:
        variant = parse_variant(spec)
        canon = json.dumps(variant, sort_keys=True)
        if canon in seen:
            print(f"duplicate variant {spec!r} ignored", file=sys.stderr)
            continue
        seen.add(canon)
        groups.setdefault(server_env(variant), []).append(spec)
    return sorted(groups.items(), key=lambda item: (item[0] != "", item[0]))


def main(argv):
    command, args = argv[0], argv[1:]
    if command == "slug":
        print(slug(parse_variant(args[0])))
    elif command == "filename":
        print(filename(args[0], args[1], parse_variant(args[2])))
    elif command == "server-env":
        print(server_env(parse_variant(args[0])))
    elif command == "param-keys":
        print(" ".join(sorted(param_settings(parse_variant(args[0])))))
    elif command == "postures":
        for env, specs in postures(args):
            print(f"{env}|{' '.join(specs)}")
    else:
        raise SystemExit(f"unknown variants command {command!r}")


if __name__ == "__main__":
    main(sys.argv[1:])
