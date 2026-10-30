"""Pinned streaming vector sources; normalized scratch is little-endian float32."""
import base64
import bz2
import csv
import hashlib
import json
import math
import struct
import subprocess
import tarfile
import tempfile
from pathlib import Path
from corpus_cache import sha256, verify_file

ROOT = Path(__file__).resolve().parents[1]
TOOL = ROOT / 'driver/build/vector_tool'

DATASETS = ('cohere', 'sift', 'gist', 'miracl768', 'nq1536')


def dataset_manifest(dataset):
    if dataset not in DATASETS:
        raise ValueError(f'unknown dataset: {dataset}')
    return json.loads((ROOT / 'corpora' / dataset / 'manifest.json').read_text())


def vector_rows(path, dims=1024, limit=None):
    if dims < 1 or Path(path).stat().st_size % (4 * dims):
        raise ValueError('invalid vector file shape')
    with open(path, 'rb') as source:
        i = 0
        while limit is None or i < limit:
            raw = source.read(dims * 4)
            if not raw:
                break
            yield struct.unpack('<' + 'f' * dims, raw)
            i += 1
        if limit is not None and i != limit:
            raise ValueError('short vector file')


def recall_at_k(ids, truth, k):
    if len(ids) != k or len(set(ids)) != k or len(truth) < k:
        raise ValueError('invalid recall operands')
    return len(set(ids) & set(truth[:k])) / k


def checked_vector(vector, dims, metric):
    if len(vector) != dims or any(not math.isfinite(v) for v in vector):
        raise ValueError('invalid vector dimensions/components')
    if metric == 'cosine' and not any(vector):
        raise ValueError('zero cosine vector')
    return struct.pack('<' + 'f' * dims, *vector)


def json_vectors(path, count, dims, metric, spec):
    opener = bz2.open if path.suffix == '.bz2' else open
    with opener(path, 'rt', encoding='utf-8') as source:
        for _ in range(count):
            line = source.readline()
            if not line:
                raise ValueError('short JSON vector source')
            value = json.loads(line)
            vector = value[spec['field']] if 'field' in spec else value
            encoding = spec['encoding']
            if encoding == 'base64-f32be':
                raw = base64.b64decode(vector, validate=True)
                if len(raw) != dims * 4:
                    raise ValueError('invalid base64 vector length')
                vector = struct.unpack('>' + 'f' * dims, raw)
            elif encoding != 'json-array':
                raise ValueError(f'unsupported JSON vector encoding: {encoding}')
            if spec.get('unit_norm'):
                norm = math.sqrt(sum(v*v for v in vector))
                if not .99 < norm < 1.01:
                    raise ValueError(f'vector norm {norm}; expected unit norm')
            yield checked_vector(vector, dims, metric)


def fvecs(source, count, dims):
    for _ in range(count):
        header = source.read(4)
        if len(header) != 4 or struct.unpack('<I', header)[0] != dims:
            raise ValueError('invalid fvecs dimension header')
        raw = source.read(dims * 4)
        if len(raw) != dims * 4:
            raise ValueError('short fvecs row')
        yield checked_vector(struct.unpack('<' + 'f' * dims, raw), dims, 'l2')


def preparation_identity(dataset, rows, nq=1000):
    return {'dataset': dataset, 'rows': rows, 'nq': nq,
            'manifest': dataset_manifest(dataset), 'transform': sha256(Path(__file__)),
            'tool_source': sha256(ROOT / 'driver/vector_tool.cpp'), 'tool_binary': sha256(TOOL)}


def prepared_directory(identity):
    key = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()[:20]
    return ROOT / 'corpus' / f"vector-{identity['dataset']}-{identity['rows']}-{key}"


def materialize_vectors(manifest, stage, rows, nq):
    """Use raw cache matrices in place; normalize other streams into staging."""
    cache = ROOT / 'corpus/cache'
    paths, archives = {}, {}
    for role, count in [('docs', rows), ('queries', nq)]:
        spec = manifest['vectors'][role]
        source = cache / spec['source_file']
        if spec['format'] == 'raw':
            if spec['encoding'] != 'f32le':
                raise ValueError('unsupported raw encoding')
            if source.stat().st_size % (manifest['dims'] * 4) or source.stat().st_size < count * manifest['dims'] * 4:
                raise ValueError('invalid raw vector shape')
            paths[role] = source
            continue
        path = stage / (role + '.vec')
        paths[role] = path
        if spec['format'] == 'tar-fvecs':
            archives.setdefault(source, []).append((spec['member'], path, count))
        elif spec['format'] == 'jsonl':
            with path.open('wb') as dest:
                dest.writelines(json_vectors(source, count, manifest['dims'], manifest['metric'], spec))
        else:
            raise ValueError(f"unsupported source format: {spec['format']}")
    for source, members in archives.items():
        pending = {name: (path, count) for name, path, count in members}
        with tarfile.open(source, 'r|gz') as archive:
            for member in archive:
                if member.name in pending:
                    path, count = pending.pop(member.name)
                    with archive.extractfile(member) as src, path.open('wb') as dest:
                        dest.writelines(fvecs(src, count, manifest['dims']))
                if not pending:
                    break
        if pending:
            raise ValueError(f'missing fvecs archive members: {list(pending)}')
    return paths['docs'], paths['queries']


def csv_fields(path, rows):
    with open(path, newline='', encoding='utf-8') as source:
        reader = csv.DictReader(source, strict=True)
        if reader.fieldnames != ['wiki_id', 'paragraph_count', 'paragraph_id', 'title', 'text', 'url']:
            raise ValueError('unexpected CSV header')
        for _ in range(rows):
            row = next(reader, None)
            if row is None or None in row or any(v is None for v in row.values()):
                raise ValueError('short or malformed CSV input')
            yield {k: row[k] for k in ('title', 'text', 'url')}


def transform(vec_path, output, rows, dims, csv_path=None):
    fields = csv_fields(csv_path, rows) if csv_path else ({} for _ in range(rows))
    with subprocess.Popen([str(TOOL), 'encode', str(vec_path), str(rows), str(dims)],
                          stdout=subprocess.PIPE, text=True) as encoder:
        try:
            with open(output, 'w', encoding='utf-8') as dest:
                for i in range(rows):
                    line = encoder.stdout.readline().strip()
                    if not line:
                        raise ValueError('short vector encoder output')
                    doc = {'id': str(i), **next(fields)}
                    dest.write(json.dumps(doc, ensure_ascii=True, separators=(',', ':'))[:-1]
                               + ',"embedding":' + line + '}\n')
            if encoder.wait():
                raise RuntimeError('vector encoding failed')
        finally:
            if encoder.poll() is None:
                encoder.kill()
            encoder.stdout.close()
            fields.close()


def prepare(rows, nq=1000, dataset='cohere'):
    if not 100 <= rows <= 1000000 or not 1 <= nq <= 1000:
        raise ValueError('unsupported row/query prefix')
    if not TOOL.exists() or TOOL.stat().st_mtime_ns < (ROOT / 'driver/vector_tool.cpp').stat().st_mtime_ns:
        raise RuntimeError('rebuild vector_tool')
    manifest = dataset_manifest(dataset)
    cache = ROOT / 'corpus/cache'
    for entry in manifest['files']:
        if entry['source_sha256'] is None or entry['source_bytes'] is None:
            raise ValueError(f"source not yet pinned: {entry['source_file']}")
        verify_file(cache / entry['source_file'], entry['source_bytes'], entry['source_sha256'])
    dims, metric = manifest['dims'], manifest['metric']
    identity = preparation_identity(dataset, rows, nq)
    target = prepared_directory(identity)
    if not target.exists():
        # Publish the corpus, queries, truth and checksums together, or nothing.
        with tempfile.TemporaryDirectory(prefix=target.name + '.part-', dir=target.parent) as tmp:
            stage = Path(tmp)
            docs, queries = materialize_vectors(manifest, stage, rows, nq)
            corpus = stage / f'corpus-{dataset}-{rows}.ndjson'
            extra = manifest.get('extra_csv')
            transform(docs, corpus, rows, dims, cache / extra if extra else None)
            digest = sha256(corpus)
            truth_identity = {'corpus_sha256': digest, 'query_sha256': sha256(queries),
                              'nq': nq, 'k': 100, 'metric': metric,
                              'tool_source': identity['tool_source'], 'tool_binary': identity['tool_binary']}
            result = subprocess.check_output([str(TOOL), 'truth', str(docs), str(rows), str(dims),
                                              str(queries), str(nq), '100', '16', metric], text=True)
            (stage / 'truth.json').write_text(json.dumps({'identity': truth_identity, 'neighbors': json.loads(result)}) + '\n')
            if docs.parent == stage:
                docs.unlink()
            checksums = {p.name: sha256(p) for p in stage.iterdir()}
            (stage / 'identity.json').write_text(json.dumps({'identity': identity, 'checksums': checksums}))
            stage.rename(target)
    metadata = json.loads((target / 'identity.json').read_text())
    if metadata['identity'] != identity:
        raise ValueError('prepared identity mismatch')
    for name, digest in metadata['checksums'].items():
        if sha256(target / name) != digest:
            raise ValueError(f'prepared hash mismatch: {name}')
    corpus = target / f'corpus-{dataset}-{rows}.ndjson'
    digest = metadata['checksums'][corpus.name]
    Path(str(corpus) + '.sha256').write_text(digest + '\n')
    truth = target / 'truth.json'
    spec = manifest['vectors']['queries']
    queries = cache / spec['source_file'] if spec['format'] == 'raw' else target / 'queries.vec'
    return corpus, digest, list(vector_rows(queries, dims, nq)), json.loads(truth.read_text()), truth
