import base64
import io
import json
from pathlib import Path
import struct
import subprocess
import tempfile
import unittest
from unittest.mock import patch
import shutil
import vector_datasets as datasets
from corpus_cache import sha256
from vector_datasets import TOOL, checked_vector, fvecs, json_vectors
from schema import luxir_vector_schema


class DatasetTests(unittest.TestCase):
    def test_schema(self):
        for name, dims, metric in [('sift', 128, 'l2'), ('gist', 960, 'l2'),
                                   ('miracl768', 768, 'cosine'), ('nq1536', 1536, 'cosine')]:
            fields = luxir_vector_schema('rabitq2', name)['fields']
            self.assertEqual(set(fields), {'id', 'embedding'})
            self.assertEqual((fields['embedding']['dims'], fields['embedding']['metric']), (dims, metric))
        self.assertEqual(luxir_vector_schema('pq-m120', 'gist')['fields']['embedding']['ann'],
                         {'codec': 'pq', 'pq': {'m': 120}})
        for codec in ('pq-m0', 'pq-m119', 'pq-mx'):
            with self.assertRaises(ValueError):
                luxir_vector_schema(codec, 'gist')

    def test_parsers(self):
        raw = struct.pack('<I2fI2f', 2, 1, 2, 2, 3, 4)
        self.assertEqual(list(fvecs(io.BytesIO(raw), 1, 2)), [struct.pack('<2f', 1, 2)])
        with self.assertRaises(ValueError):
            list(fvecs(io.BytesIO(raw), 1, 3))
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / 'source.json'
            p.write_text(json.dumps({'emb': base64.b64encode(struct.pack('>2f', .6, .8)).decode()}) + '\ninvalid trailing data')
            result = list(json_vectors(p, 1, 2, 'cosine', {'field': 'emb', 'encoding': 'base64-f32be', 'unit_norm': True}))
            self.assertEqual(result, [struct.pack('<2f', .6, .8)])
        for v in ([0., 0.], [float('nan'), 1.]):
            with self.assertRaises(ValueError):
                checked_vector(v, 2, 'cosine')

    @unittest.skipUnless(TOOL.exists(), 'build vector_tool')
    def test_metrics_and_ties(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp)
            (p/'d').write_bytes(struct.pack('<8f', 1, 0, 2, 0, 0, 1, 2, 0))
            (p/'q').write_bytes(struct.pack('<2f', 1, 0))
            args = [str(TOOL), 'truth', str(p/'d'), '4', '2', str(p/'q'), '1', '4', '16']
            for metric, expected in [('ip', ['1','3','0','2']), ('l2', ['0','1','3','2']), ('cosine', ['0','1','3','2'])]:
                self.assertEqual(json.loads(subprocess.check_output(args + [metric])), [expected])
            (p/'d').write_bytes(bytes(32))
            self.assertNotEqual(subprocess.run(args + ['cosine'], capture_output=True).returncode, 0)

    @unittest.skipUnless(TOOL.exists(), 'build vector_tool')
    def test_unified_raw_preparation_and_reuse(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cache = root / 'corpus/cache'
            cache.mkdir(parents=True)
            (root / 'driver').mkdir()
            shutil.copy2(datasets.ROOT / 'driver/vector_tool.cpp', root / 'driver/vector_tool.cpp')
            docs, queries = cache / 'docs.vec', cache / 'queries.vec'
            docs.write_bytes(struct.pack('<200f', *(v for i in range(100) for v in (i, 1))))
            queries.write_bytes(struct.pack('<4f', 1, 0, 0, 1))
            text = cache / 'docs.csv'
            text.write_text('wiki_id,paragraph_count,paragraph_id,title,text,url\n' + 'x,1,1,title,body,url\n' * 100)
            manifest = {'dims': 2, 'metric': 'ip', 'extra_csv': text.name,
                        'files': [{'source_file': p.name, 'source_bytes': p.stat().st_size,
                                   'source_sha256': sha256(p)} for p in (docs, queries, text)],
                        'vectors': {role: {'source_file': p.name, 'format': 'raw', 'encoding': 'f32le'}
                                    for role, p in [('docs', docs), ('queries', queries)]}}
            with patch.object(datasets, 'ROOT', root), patch.object(datasets, 'dataset_manifest', return_value=manifest):
                with tempfile.TemporaryDirectory(dir=root) as staging:
                    stage = Path(staging)
                    self.assertEqual(datasets.materialize_vectors(manifest, stage, 100, 2), (docs, queries))
                    self.assertEqual(list(stage.iterdir()), [])
                corpus, digest, vectors, truth, truth_path = datasets.prepare(100, 2)
                self.assertEqual(truth['neighbors'][0][:3], ['99', '98', '97'])
                self.assertEqual(vectors, [(1, 0), (0, 1)])
                self.assertEqual(json.loads(corpus.read_text().splitlines()[0])['text'], 'body')
                self.assertFalse((corpus.parent / 'docs.vec').exists())
                self.assertFalse((corpus.parent / 'queries.vec').exists())
                self.assertTrue(docs.exists() and queries.exists())
                before = corpus.stat().st_mtime_ns
                self.assertEqual(datasets.prepare(100, 2)[1], digest)
                self.assertEqual(corpus.stat().st_mtime_ns, before)
                corpus.write_text('corrupt')
                with self.assertRaisesRegex(ValueError, 'hash mismatch'):
                    datasets.prepare(100, 2)
                # A failed new prefix never publishes a directory or leaves staging.
                with self.assertRaisesRegex(ValueError, 'raw vector shape'):
                    datasets.prepare(101, 2)
                self.assertFalse(datasets.prepared_directory(datasets.preparation_identity('cohere', 101, 2)).exists())
                self.assertEqual(list((root / 'corpus').glob('*.part-*')), [])

    def test_unit_norm_is_manifest_policy(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / 'q.json'
            p.write_text(json.dumps(base64.b64encode(struct.pack('>2f', 3, 4)).decode()) + '\n')
            spec = {'encoding': 'base64-f32be'}
            self.assertEqual(list(json_vectors(p, 1, 2, 'cosine', spec)), [struct.pack('<2f', 3, 4)])
            with self.assertRaisesRegex(ValueError, 'unit norm'):
                list(json_vectors(p, 1, 2, 'cosine', dict(spec, unit_norm=True)))
