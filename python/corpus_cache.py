"""Pinned local artifact verification, shared by corpus preparers."""
import hashlib
from pathlib import Path


def sha256(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as source:
        for block in iter(lambda: source.read(8 * 1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def verify_file(path, size, expected):
    path = Path(path)
    stamp = Path(str(path) + '.sha256-ok')
    actual_size = path.stat().st_size
    if actual_size != size:
        raise ValueError(f'{path}: size mismatch: expected {size}, got {actual_size}')
    if (stamp.exists() and stamp.stat().st_mtime_ns > path.stat().st_mtime_ns
            and stamp.read_text().strip() == expected):
        return
    actual = sha256(path)
    if actual != expected:
        raise ValueError(f'{path}: SHA-256 mismatch: expected {expected}, got {actual}')
    stamp.write_text(expected + '\n')
