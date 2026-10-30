import csv
import json
import struct
import subprocess
import tempfile
import os
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
import unittest
from pathlib import Path

from vector_datasets import TOOL, transform, vector_rows, recall_at_k
from schema import luxir_vector_schema
from adapters import LuxirVectorAdapter, make_adapter
from driver import materialize_workload
from corpus_cache import verify_file, sha256


class VectorTests(unittest.TestCase):
    def test_schema(self):
        for codec in ('pq', 'rabitq', 'rabitq4'):
            schema = luxir_vector_schema(codec)['fields']
            self.assertEqual(schema['embedding']['metric'], 'ip')
            self.assertEqual(schema['embedding']['dims'], 1024)
            self.assertEqual(schema['url']['index'], 'none')
        self.assertEqual(luxir_vector_schema('rabitq4')['fields']['embedding']['ann'],
                         {'codec': 'rabitq', 'rabitq': {'bits': 4}})
        for codec in ('rabitq0', 'rabitq10', 'rabitqx', 'hnsw'):
            with self.assertRaises(ValueError):
                luxir_vector_schema(codec)

    def test_reading(self):
        with tempfile.TemporaryDirectory() as directory:
            p = Path(directory) / 'v'
            p.write_bytes(struct.pack('<4f', 1, -2, .5, 0))
            self.assertEqual(list(vector_rows(p, 2)), [(1, -2), (.5, 0)])
            with self.assertRaises(ValueError):
                list(vector_rows(p, 3))
            with self.assertRaises(ValueError):
                list(vector_rows(p, 2, 3))

    @unittest.skipUnless(TOOL.exists(), 'build vector_tool for numeric tests')
    def test_transform(self):
        with tempfile.TemporaryDirectory() as directory:
            p = Path(directory)
            vec = p / 'v'
            values = (0.123456789, -1.23456789e-20, 0., -0.)
            raw = struct.pack('<4f', *values)
            vec.write_bytes(raw)
            text = 'comma, quote "yes"\nnext line\r\nend'
            with (p / 'c').open('w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['wiki_id', 'paragraph_count', 'paragraph_id', 'title', 'text', 'url'])
                writer.writerow(['x', 1, 1, 'title, "quoted"', text, 'u'])
                writer.writerow(['y', 1, 1, 't', 'body', 'v'])
            transform(vec, p / 'o', 2, 2, p / 'c')
            docs = [json.loads(line) for line in (p / 'o').read_text().splitlines()]
            self.assertEqual(docs[0]['text'], text)
            self.assertEqual(docs[0]['title'], 'title, "quoted"')
            self.assertEqual([d['id'] for d in docs], ['0', '1'])
            self.assertEqual(struct.pack('<4f', *(d for doc in docs for d in doc['embedding'])), raw)
            with self.assertRaises(ValueError):
                transform(vec, p / 'bad', 3, 2, p / 'c')

    @unittest.skipUnless(TOOL.exists(), 'build vector_tool for numeric tests')
    def test_truth(self):
        with tempfile.TemporaryDirectory() as directory:
            p = Path(directory)
            # IP, not cosine: the longer vector must win. Ties use ordinal.
            (p / 'd').write_bytes(struct.pack('<8f', 1, 0, 2, 0, 0, 1, 2, 0))
            (p / 'q').write_bytes(struct.pack('<4f', 1, 0, 0, 1))
            raw = subprocess.check_output([str(TOOL), 'truth', str(p / 'd'), '4', '2',
                                           str(p / 'q'), '2', '3', '2'])
            self.assertEqual(json.loads(raw), [['1', '3', '0'], ['2', '0', '1']])

    def test_verified_cache(self):
        with tempfile.TemporaryDirectory() as directory:
            p = Path(directory) / 'artifact'
            p.write_bytes(b'correct')
            digest = sha256(p)
            verify_file(p, 7, digest)
            verify_file(p, 7, digest)
            p.write_bytes(b'changed')
            with self.assertRaises(ValueError):
                verify_file(p, 7, digest)
            with self.assertRaises(ValueError):
                verify_file(p, 8, sha256(p))

    def test_large_native_request_and_expected_docs(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'w'
            adapter = LuxirVectorAdapter(make_adapter('luxir', 'test'))
            body = {'query': {'knn': {'field': 'embedding', 'query': [0.123456789] * 1024, 'k': 10}}}
            expected = '\"docs\":[{\"id\":\"1\"}]'
            materialize_workload([({'name': 'knn', 'expected': expected}, body)],
                                 adapter, '127.0.0.1', 9400, path)
            raw = path.read_bytes()
            magic, version, records, buckets = struct.unpack_from('<4sIIH', raw)
            self.assertEqual((magic, version, records, buckets), (b'SBWL', 2, 1, 1))
            label_size, = struct.unpack_from('<H', raw, 14)
            position = 16 + label_size
            length, bucket = struct.unpack_from('<IH', raw, position)
            self.assertGreater(length, 10000)
            wire = raw[position + 6:position + 6 + length]
            self.assertEqual(json.loads(wire.split(b'\r\n\r\n', 1)[1]), body)
            position += 6 + length
            expected_size, = struct.unpack_from('<I', raw, position)
            self.assertEqual(raw[position + 4:], expected.encode())
            self.assertEqual(expected_size, len(expected))

    @unittest.skipUnless((TOOL.parent / 'bench_replay').exists(), 'build replay for payload checks')
    def test_native_checks_each_expected_payload(self):
        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                self.rfile.read(int(self.headers['Content-Length']))
                body = b'{ "docs" : [ {"id": "1"} ] }'
                self.send_response(200)
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        server = HTTPServer(('127.0.0.1', 0), Handler)
        worker = threading.Thread(target=server.serve_forever)
        worker.start()
        try:
            with tempfile.TemporaryDirectory() as directory:
                p = Path(directory)
                adapter = LuxirVectorAdapter(make_adapter('luxir', 'test'))
                for doc_id, expected_errors in [('1', 0), ('2', 1)]:
                    expected = '\"docs\":[{\"id\":\"' + doc_id + '\"}]'
                    materialize_workload([({'name': 'knn', 'expected': expected}, {})],
                        adapter, '127.0.0.1', server.server_port, p / 'w')
                    command = [str(TOOL.parent / 'bench_replay'), '--workload', str(p / 'w'),
                        '--host', '127.0.0.1', '--port', str(server.server_port),
                        '--concurrency', '1', '--threads', '1', '--repetitions', '1',
                        '--timeout', '5', '--server-pid', str(os.getpid()),
                        '--max-requests', '1', '--warmup-requests', '0', '--warmup-seconds', '0',
                        '--client-cores', str(min(os.sched_getaffinity(0))), '--out', str(p / 'r')]
                    result = subprocess.run(command, capture_output=True, timeout=10)
                    self.assertEqual(result.returncode, expected_errors, result.stderr)
                    value = json.loads((p / 'r').read_text())
                    self.assertEqual(value['aggregate']['overall']['errors'], expected_errors)
        finally:
            server.shutdown()
            server.server_close()
            worker.join()

    def test_recall(self):
        self.assertEqual(recall_at_k(['1', '3'], ['1', '2', '3'], 2), .5)
        self.assertEqual(recall_at_k(['1', '2'], ['2', '1'], 2), 1)
        with self.assertRaises(ValueError):
            recall_at_k(['1', '1'], ['1', '2'], 2)
