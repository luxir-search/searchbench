#!/usr/bin/env python3
"""Vector campaign orchestration; numeric preparation and HTTP replay are native."""
from datetime import datetime, timezone
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import statistics
import subprocess
import time
from types import SimpleNamespace

from adapters import make_adapter, LuxirVectorAdapter
from controls import control_mismatches
from corpus_cache import sha256
from cpu_layout import current_cpu_sets, format_cpu_list
from datasets import dataset_name
from driver import (materialize_workload, run_replay, replay_metrics, host_config,
                    process_startup_config, source_file_config, server_session)
from http_sync import request_json
from report import render_vector_report
from results_io import load_result, write_context
from sampler import ProcSampler
from schema import luxir_vector_schema, index_layout_version
from topology_probe import index_topology, assert_quiescent_topology
from vector_corpus import ROOT, TOOL, prepare, recall_at_k


def save(path, value):
    Path(path).write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')


def cpu(pid):
    fields = Path(f'/proc/{pid}/stat').read_text().rpartition(')')[2].split()
    return (int(fields[11]) + int(fields[12])) / os.sysconf('SC_CLK_TCK')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', choices=('100k', '1m'), default='100k')
    parser.add_argument('--outdir', type=Path)
    parser.add_argument('--prepare', action='store_true')
    parser.add_argument('--smoke', action='store_true')
    parser.add_argument('--codec', default='pq', help='pq, rabitq or rabitqN (N bits per dimension)')
    parser.add_argument('--refeed', action='store_true')
    parser.add_argument('--duration', type=float, default=20)
    parser.add_argument('--repetitions', type=int, default=2)
    parser.add_argument('--nprobes', default='16,64,256')
    parser.add_argument('--refine', default='default',
                        help="refine_candidates per cell: default (Luxir's k-derived pool), k, or a count")
    parser.add_argument('--concurrency', default='1,8')
    args = parser.parse_args()
    if args.duration <= 0 or args.repetitions < 1:
        parser.error('--duration and --repetitions must be positive')
    try:
        luxir_vector_schema(args.codec)
    except ValueError as error:
        parser.error(str(error))
    refines = args.refine.split(',')
    if any(r not in ('default', 'k') and not (r.isdigit() and int(r) > 0) for r in refines):
        parser.error('--refine takes default, k or positive counts')
    concurrencies = [int(c) for c in args.concurrency.split(',')]
    if any(c < 1 for c in concurrencies):
        parser.error('--concurrency must be positive')
    if not args.prepare:
        if args.outdir is None:
            parser.error('--outdir is required for runs')
        if (ROOT / 'run/luxir.pid').exists():
            raise RuntimeError('stop the existing Luxir server first')
        if not os.access(os.environ.get('LUXIR_BIN', ''), os.X_OK):
            raise RuntimeError('LUXIR_BIN must name an executable')
        replay = Path(os.environ.get('SEARCHBENCH_DRIVER', ROOT / 'driver/build/bench_replay'))
        if not os.access(replay, os.X_OK):
            raise RuntimeError('build the native replay driver before running')
        def terminate(signum, frame):
            raise KeyboardInterrupt('terminated')
        signal.signal(signal.SIGTERM, terminate)
    rows = 10000 if args.smoke else {'100k': 100000, '1m': 1000000}[args.s]
    if not TOOL.exists():
        raise RuntimeError('build driver/build/vector_tool before preparation')
    corpus, digest, queries, truth, truth_path = prepare(rows)
    if args.prepare:
        print(f'Prepared {corpus}; truth {truth_path}')
        return
    out = args.outdir.resolve()
    out.mkdir(parents=True, exist_ok=False)
    binary = Path(os.environ['LUXIR_BIN']).resolve()
    binary_info = source_file_config(binary)
    identity = {'corpus_sha256': digest, 'binary_sha256': binary_info['sha256'],
                'schema': luxir_vector_schema(args.codec), 'layout': index_layout_version('luxir'),
                'feed_version': sha256(ROOT / 'python/feed_luxir.py')}
    tag = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()[:16]
    dataset = dataset_name(corpus) + '-' + args.codec + '-' + tag
    # The server's directory (engine-common.sh engine_data_dir), so a
    # SEARCHBENCH_DATA_ROOT on tmpfs holds the index and its identity marker.
    data = Path(os.environ.get('SEARCHBENCH_DATA_ROOT', ROOT / 'data')) / 'luxir' / dataset
    marker = data / 'vector-identity.json'
    if args.smoke and data.exists():
        raise RuntimeError('smoke scratch already exists')
    reuse = marker.exists() and json.loads(marker.read_text()) == identity and not args.refeed
    if not reuse and data.exists():
        # Preserve previous measurements and the old index.
        data.rename(data.with_name(data.name + f'.previous-{time.time_ns()}'))
    env = dict(os.environ, SEARCHBENCH_DATASET=dataset, SEARCHBENCH_QUERY_CACHE='off')
    pid = None
    try:
        started = subprocess.check_output([str(ROOT / 'scripts/start-luxir.sh')], env=env, text=True)
        pid = int(started.strip())
        base = make_adapter('luxir', 'searchbench')
        adapter = LuxirVectorAdapter(base)
        def request(method, suffix, body=None):
            return request_json('127.0.0.1', 9400, method, suffix, body, timeout=7200)
        def stats():
            return request('GET', '/_stats?segments=true')
        def measured(name, action):
            before = stats()
            sampler = ProcSampler(pid, str(out / (name + '.rss.csv')))
            sampler.start()
            begin, cpu_before = time.monotonic(), cpu(pid)
            try:
                value = action()
            finally:
                elapsed, used = time.monotonic() - begin, cpu(pid) - cpu_before
                memory = sampler.stop()
            result = {'elapsed_s': elapsed, 'server_cpu_s': used, 'memory': memory,
                      'stats_before': before, 'stats_after': stats(), 'response': value}
            save(out / (name + '.json'), result)
            return result
        feed = None
        if not reuse:
            def ingest():
                raw = subprocess.check_output(['python3', str(ROOT / 'python/feed_luxir.py'),
                       str(corpus), '--vector-codec', args.codec], text=True)
                value = json.loads(raw)
                if value['count_response'].get('found') != rows:
                    raise RuntimeError('feed count mismatch')
                return value
            feed = measured('feed', ingest)
            save(marker, identity)
        merge = measured('force-merge', lambda: request('POST', '/collections/searchbench/_update',
                         {'commit': {'max_segments': 1, 'wait_for_merges': True,
                                     'build_aux_indexes': ['vec.embedding']}}))
        if merge['response'].get('error'):
            raise RuntimeError(merge['response'])
        def posture():
            topology = index_topology(base, '127.0.0.1', 9400, 120)
            assert_quiescent_topology(topology, 1)
            if topology['segments'][0]['live_docs'] != rows:
                raise RuntimeError('serving document count mismatch')
            raw = request('GET', '/collections/searchbench/_stats?segments=true')
            segments = raw['collections'][0]['shards'][0]['index']['segments']
            if not any(a.get('name') == 'vec.embedding' for a in segments[0].get('overlays', [])):
                raise RuntimeError('one-segment index lacks vector overlay')
            return {'topology': topology, 'stats': raw, 'overlays': segments[0]['overlays']}
        layout = posture()
        server_cpus, client_cpus = current_cpu_sets()
        client_cores = os.environ.get('CLIENT_CORES', format_cpu_list(client_cpus))
        context = {'engine': 'luxir', 'workload': 'cohere-vector', 'functional_smoke': args.smoke,
                   'binary': binary_info, 'host': host_config(pid), 'session': server_session(pid),
                   'startup': process_startup_config('luxir', pid), 'identity': identity,
                   'index_topology': layout, 'ground_truth': source_file_config(truth_path),
                   'server_cores': os.environ.get('SERVER_CORES', format_cpu_list(server_cpus)),
                   'client_cores': client_cores, 'feed_reused': reuse}
        context.update({
            'server_session': server_session(pid),
            'engine_config': {'binary_sha256': binary_info['sha256']},
            'source_files': {'replay_driver': source_file_config(Path(os.environ.get(
                'SEARCHBENCH_DRIVER', ROOT / 'driver/build/bench_replay'))),
                'numeric_helper': source_file_config(TOOL)},
            'core_split': {'server': context['server_cores'], 'client': client_cores},
        })
        context_name = write_context(out, 'luxir', context)
        cells = []
        if args.smoke:
            exploration = []
            for probe in (4, 8, 16, 32, 64, 100):
                scores = []
                for q, vector in enumerate(queries[:100]):
                    value = request('POST', '/collections/searchbench/_search',
                        {'query': {'knn': {'field': 'embedding', 'query': vector, 'k': 10,
                                          'ivf': {'nprobe': probe}}}, 'limit': 10, 'fields': ['id']})
                    scores.append(recall_at_k([d['id'] for d in value['docs']], truth['neighbors'][q], 10))
                exploration.append({'nprobe': probe, 'recall_at_10': statistics.mean(scores)})
            save(out / 'probe-exploration.json', exploration)
            print('Untimed 10k probe exploration: ' + json.dumps(exploration), flush=True)
        probes = [None] + [int(p) for p in args.nprobes.split(',')]
        if any(p is not None and p < 1 for p in probes):
            raise ValueError('nprobe must be positive')
        configs = [(10, None, 'default', 1), (100, 64, 'default', 1)] if args.smoke else [
            (k, p, r, c) for k in (10, 100) for p in probes for r in refines for c in concurrencies
            if not (r.isdigit() and int(r) < k)]
        for k, probe, refine, concurrency in configs:
            refine_candidates = None if refine == 'default' else k if refine == 'k' else int(refine)
            name = (f'knn-k{k}-np{probe or "default"}'
                    + ('' if refine == 'default' else f'-rf{refine}') + f'-c{concurrency}')
            workload, recalls, returned = [], [], []
            posture()
            for q, vector in enumerate(queries):
                knn = {'field': 'embedding', 'query': vector, 'k': k}
                if probe is not None:
                    knn['ivf'] = {'nprobe': probe}
                if refine_candidates is not None:
                    knn['refine_candidates'] = refine_candidates
                body = {'query': {'knn': knn}, 'limit': k, 'fields': ['id']}
                value = request('POST', '/collections/searchbench/_search', body)
                if value.get('error') or value.get('warnings'):
                    raise RuntimeError(value)
                docs = value.get('docs', [])
                if any(set(doc) != {'id'} for doc in docs):
                    raise RuntimeError('expected ids only')
                ids = [doc['id'] for doc in docs]
                if any(not isinstance(i, str) or not i.isdigit() or int(i) >= rows for i in ids):
                    raise RuntimeError('invalid document id')
                recalls.append(recall_at_k(ids, truth['neighbors'][q], k))
                returned.append(ids)
                workload.append(({'name': name, 'expected':
                    ('"docs":' + json.dumps(docs, separators=(',', ':')))}, body))
            save(out / (name + '.validation.json'), {'recall': recalls, 'returned_ids': returned})
            blob = out / (name + '.sbwl')
            _, _, examples = materialize_workload(workload, adapter, '127.0.0.1', 9400, blob)
            replay_args = SimpleNamespace(host='127.0.0.1', port=9400, concurrency=concurrency,
                threads=1, client_cores=client_cores, duration=0 if args.smoke else args.duration,
                repetitions=1 if args.smoke else args.repetitions, order='seq',
                warmup_seconds=0 if args.smoke else 1, max_requests=1000 if args.smoke else 0,
                warmup_requests=0, timeout=120, server_pid=pid, seed=None)
            sampler = ProcSampler(pid, str(out / (name + '.rss.csv')))
            sampler.start()
            try:
                raw = run_replay(ROOT, replay_args, adapter, 'knn', blob, out / (name + '.native.json'))
            finally:
                memory = sampler.stop()
            blob.unlink()
            if raw['aggregate']['overall']['errors'] or raw['errors']:
                raise RuntimeError('native replay errors: ' + str(raw['errors']))
            after = posture()
            if (after['topology'] != layout['topology']
                    or after['overlays'] != layout['overlays']):
                raise RuntimeError('index changed during queries')
            elapsed = sum(rep['elapsed_s'] for rep in raw['repetitions'])
            cell = {'created_at_utc': datetime.now(timezone.utc).isoformat(),
                    'engine': 'luxir', 'variant': {}, 'name': name, 'k': k, 'nprobe': probe,
                    'refine_candidates': refine_candidates, 'concurrency': concurrency,
                    'recall_at_k': statistics.mean(recalls), 'context': context_name,
                    'metrics': replay_metrics(raw['aggregate']['overall'], elapsed),
                    'memory': memory, 'request_examples': examples, 'native': raw}
            save(out / (name + '.json'), cell)
            cells.append(cell)
            print(f'{name}: recall@{k}={cell["recall_at_k"]:.6f}; errors=0', flush=True)
        if args.smoke and cells[0]['recall_at_k'] <= .5:
            raise RuntimeError('smoke recall <= 0.5')
        if not args.smoke:
            subprocess.run(['bash', '-c', 'source scripts/engine-common.sh\nrecord_health_control luxir "$1" 9400 "$2"',
                            'vector-control', str(pid), str(out)], cwd=ROOT, env=env, check=True)
            controls = list((out / 'controls').glob('luxir-HEALTH*.json'))
            if len(controls) != 1:
                raise RuntimeError('missing health control')
            control = load_result(controls[0])
            peers = [{**context, **cell} for cell in cells]
            differences = control_mismatches(control, peers)
            if differences or not control.get('aggregate', {}).get('repetition_qps_median', 0):
                raise RuntimeError('invalid health control: ' + str(differences))
        shutil.copyfile(ROOT / 'logs/luxir.log', out / 'luxir.log')
        render_vector_report(out, cells, feed, merge, context)
        print('PASS: vector functional smoke' if args.smoke else 'PASS: vector campaign', flush=True)
    finally:
        if pid is not None:
            subprocess.run([str(ROOT / 'scripts/stop-luxir.sh')], env=env, check=True)
        if args.smoke and data.exists():
            shutil.rmtree(data)


if __name__ == '__main__':
    main()
