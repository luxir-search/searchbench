#!/usr/bin/env python3
"""One engine data directory per fed index shape.

An engine opens every index it finds in its data directory at startup, so a
directory shared by several corpora - or by several segment layouts of one
corpus - makes every run carry the indexes it is not measuring, and any run
that wants a different shape has to displace the one already there. A dataset
names what a directory holds: the corpus, plus the layout its feed produced.
Unrelated shapes then coexist under data/<engine>/<dataset> and each stays
reusable.

The layout is part of the name because a different segment count is a
different index: the standard force-merged campaign and a constructed tiered
topology are built from the same corpus and cannot share a directory.
"""

import argparse
from pathlib import Path

from topologies import TOPOLOGIES

# Layouts that are not a declared topology: the campaign default, which force
# merges to a single segment, and whatever the feed itself left behind.
MERGED = "merged"
AS_FED = "as-fed"
LAYOUTS = (MERGED, AS_FED) + tuple(TOPOLOGIES)


def corpus_label(corpus):
    """Directory-safe short name of a prepared corpus file."""
    label = Path(corpus).name
    if label.endswith(".ndjson"):
        label = label[: -len(".ndjson")]
    if label.startswith("corpus-"):
        label = label[len("corpus-"):]
    if not label or label.startswith("."):
        raise ValueError(f"corpus path has no usable name: {corpus!r}")
    return label


def dataset_name(corpus, layout=MERGED):
    if layout not in LAYOUTS:
        raise ValueError(
            f"unknown index layout {layout!r}; expected one of {list(LAYOUTS)}")
    return f"{corpus_label(corpus)}-{layout}"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("corpus")
    parser.add_argument("layout", nargs="?", default=MERGED, choices=LAYOUTS)
    args = parser.parse_args()
    print(dataset_name(args.corpus, args.layout))


if __name__ == "__main__":
    main()
