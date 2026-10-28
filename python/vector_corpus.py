"""Cohere preparation using the shared verified cache and native numeric helper."""
import csv
import hashlib
import json
import struct
import subprocess
from pathlib import Path
from corpus_cache import sha256, verify_file

ROOT = Path(__file__).resolve().parents[1]
TOOL = ROOT / 'driver/build/vector_tool'
PREFIX = 'cohere-v3-wikipedia-en-scattered-1024d.'


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


def transform(csv_path, vec_path, output, rows, dims=1024):
    # to_chars(float) emits the shortest decimal that round-trips to float32.
    with subprocess.Popen([str(TOOL), 'encode', str(vec_path), str(rows), str(dims)],
                          stdout=subprocess.PIPE, text=True) as native:
        try:
            with open(csv_path, newline='', encoding='utf-8') as source, open(output, 'w', encoding='utf-8') as dest:
                reader = csv.DictReader(source, strict=True)
                if reader.fieldnames != ['wiki_id', 'paragraph_count', 'paragraph_id', 'title', 'text', 'url']:
                    raise ValueError('unexpected CSV header')
                for i in range(rows):
                    row = next(reader, None)
                    vector = native.stdout.readline().strip()
                    if row is None or None in row or any(v is None for v in row.values()) or not vector:
                        raise ValueError('short or malformed CSV/vector input')
                    doc = {'id': str(i), **{k: row[k] for k in ('title', 'text', 'url')}}
                    dest.write(json.dumps(doc, ensure_ascii=True, separators=(',', ':'))[:-1] + ',"embedding":' + vector + '}\n')
            if native.wait():
                raise RuntimeError('vector encoder failed')
        finally:
            if native.poll() is None:
                native.kill()
            native.stdout.close()


def prepare(rows, nq=1000):
    if not TOOL.exists() or TOOL.stat().st_mtime_ns < (ROOT / 'driver/vector_tool.cpp').stat().st_mtime_ns:
        raise RuntimeError('numeric helper missing or stale; rebuild vector_tool')
    manifest = json.loads((ROOT / 'corpora/cohere/manifest.json').read_text())
    cache = ROOT / 'corpus/cache'
    for entry in manifest['files']:
        verify_file(cache / entry['source_file'], entry['source_bytes'], entry['source_sha256'])
    docs = cache / (PREFIX + 'docs.first1M.vec')
    queries = cache / (PREFIX + 'queries.first200K.vec')
    corpus = ROOT / f'corpus/corpus-cohere-{rows}.ndjson'
    identity = {'rows': rows, 'dims': 1024, 'version': 1,
                'sources': manifest['files'], 'transform': sha256(Path(__file__)),
                'tool_source': sha256(ROOT / 'driver/vector_tool.cpp'), 'tool_binary': sha256(TOOL)}
    meta = Path(str(corpus) + '.json')
    if not corpus.exists() or not meta.exists() or json.loads(meta.read_text()).get('identity') != identity:
        part = Path(str(corpus) + '.part')
        transform(cache / (PREFIX + 'docs.first1M.csv'), docs, part, rows)
        part.replace(corpus)
        meta.write_text(json.dumps({'identity': identity, 'sha256': sha256(corpus)}, indent=2) + '\n')
    digest = json.loads(meta.read_text())['sha256']
    if sha256(corpus) != digest:
        raise ValueError('transformed corpus hash mismatch')
    Path(str(corpus) + '.sha256').write_text(digest + '\n')
    query_hash = manifest['files'][3]['source_sha256']
    truth_identity = {'corpus_sha256': digest, 'query_sha256': query_hash,
                      'nq': nq, 'k': 100, 'metric': 'ip', 'tool_source': identity['tool_source'], 'tool_binary': identity['tool_binary']}
    key = hashlib.sha256(json.dumps(truth_identity, sort_keys=True).encode()).hexdigest()[:20]
    truth_path = corpus.parent / f'cohere-truth-{key}.json'
    if not truth_path.exists():
        result = subprocess.check_output([str(TOOL), 'truth', str(docs), str(rows), '1024',
                                          str(queries), str(nq), '100', '16'], text=True)
        part = truth_path.with_suffix('.part')
        part.write_text(json.dumps({'identity': truth_identity, 'neighbors': json.loads(result)}) + '\n')
        part.replace(truth_path)
    truth = json.loads(truth_path.read_text())
    if truth['identity'] != truth_identity:
        raise ValueError('ground truth identity mismatch')
    return corpus, digest, list(vector_rows(queries, limit=nq)), truth, truth_path


def recall_at_k(ids, truth, k):
    if len(ids) != k or len(set(ids)) != k or len(truth) < k:
        raise ValueError('invalid recall operands')
    return len(set(ids) & set(truth[:k])) / k
