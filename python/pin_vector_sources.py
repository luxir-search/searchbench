#!/usr/bin/env python3
"""Explicitly pin completed local vector downloads; never fetch source data."""
import argparse
import json
from pathlib import Path
from corpus_cache import sha256, verify_file
from vector_datasets import DATASETS, ROOT, dataset_manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dataset', required=True, choices=DATASETS)
    args = parser.parse_args()
    manifest = dataset_manifest(args.dataset)
    # Check completeness before hashing or changing any pin.
    for entry in manifest['files']:
        path = ROOT / 'corpus/cache' / entry['source_file']
        if not path.is_file() or Path(str(path) + '.part').exists():
            parser.error(f'source not complete: {path}')
    for entry in manifest['files']:
        path = ROOT / 'corpus/cache' / entry['source_file']
        if entry['source_bytes'] is None and entry['source_sha256'] is None:
            before = path.stat()
            digest = sha256(path)
            after = path.stat()
            if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
                raise RuntimeError(f'source changed while hashing: {path}')
            entry.update(source_bytes=after.st_size, source_sha256=digest)
            # The authoring hash already verified these stable bytes. Reuse it
            # through the normal verification stamp instead of hashing twice.
            Path(str(path) + '.sha256-ok').write_text(digest + '\n')
        verify_file(path, entry['source_bytes'], entry['source_sha256'])
    target = ROOT / 'corpora' / args.dataset / 'manifest.json'
    part = target.with_suffix('.json.part')
    part.write_text(json.dumps(manifest, indent=2) + '\n')
    part.replace(target)
    print(f'Pinned and verified {args.dataset}')


if __name__ == '__main__':
    main()
