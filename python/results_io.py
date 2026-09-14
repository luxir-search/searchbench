"""Result-file IO: content-addressed run-context files.

A task file records what a cell measured; the run environment it was measured
in - engine startup, host, core split, corpus, canonical serving topology -
lives once per result directory in an immutable content-addressed context
file (`<engine>-context-<hash12>.json`). The orchestrator cannot write that
file up front: its content is derived from what driver.py observes at cell
time (/proc capture, the live topology endpoint), so the driver hashes the
context it observed and writes the file only if that exact content is not
already present. Identical observations collapse to one file; a second
context file for one engine in one directory IS the drift alarm (rebuilt
binary, changed topology, altered startup), visible in a directory listing.

Context files are immutable and never rewritten; directory rotation must
archive or keep them alongside the task files that reference them (searchbench
archives whole directories, which preserves this invariant).

Loading folds the context back under the task keys, so every reader sees the
historical merged shape. Task files without a `context` key are
legacy-complete and pass through unchanged.
"""

import hashlib
import json
import os
from pathlib import Path
import tempfile

CONTEXT_HASH_CHARS = 12


def canonical_json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def context_filename(engine, context):
    digest = hashlib.sha256(canonical_json(context).encode()).hexdigest()
    return f"{engine}-context-{digest[:CONTEXT_HASH_CHARS]}.json"


def write_context(directory, engine, context):
    """Write the context file if this exact content is not already present.

    Idempotent and race-free without coordination: the name is derived from
    the content, and the write is temp-file + atomic rename, so concurrent
    cells observing the same environment produce one identical file.
    Returns the filename for the task file's `context` reference.
    """
    directory = Path(directory)
    name = context_filename(engine, context)
    path = directory / name
    if not path.exists():
        handle, temp = tempfile.mkstemp(dir=directory, prefix=".context-",
                                        suffix=".tmp")
        try:
            with os.fdopen(handle, "w", encoding="utf-8") as out:
                json.dump(context, out, indent=2, sort_keys=True)
                out.write("\n")
            os.replace(temp, path)
        except BaseException:
            try:
                os.unlink(temp)
            except OSError:
                pass
            raise
    return name


def load_result(path, cache=None):
    """Load a task file with its run context folded back in (task keys win).

    `cache` is an optional dict keyed by context filename for directory-wide
    loads. A missing or unreadable context file raises - a dangling reference
    must be loud, never a silently emptier result.
    """
    path = Path(path)
    doc = json.loads(path.read_text(encoding="utf-8"))
    name = doc.get("context")
    if not name:
        return doc
    context = cache.get(name) if cache is not None else None
    if context is None:
        context = json.loads((path.parent / name).read_text(encoding="utf-8"))
        if cache is not None:
            cache[name] = context
    merged = {**context, **{key: value for key, value in doc.items()
                            if key != "index_topology"}}
    # The canonical serving snapshot lives in the context; the task file
    # carries the stability verdict (plus both samples when unstable).
    merged["index_topology"] = {**context.get("index_topology", {}),
                                **doc.get("index_topology", {})}
    return merged
