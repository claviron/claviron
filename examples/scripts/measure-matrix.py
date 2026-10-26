#!/usr/bin/env python3
"""Measure already-running example handlers through the loopback Edge listener."""
import argparse
import concurrent.futures
import datetime
import http.client
import json
import pathlib
import re
import subprocess
import time

CASES = {
    'edge-rust': ('edge.bench.test', '/bench'),
    'orbit-rust': ('rust.bench.test', '/bench'),
    'fastapi-rust': ('fastapi.bench.test', '/bench'),
    'fastapi-python': ('fastapi.bench.test', '/bench/python'),
    'octane-rust': ('octane.bench.test', '/bench'),
    'octane-laravel': ('octane.bench.test', '/bench/laravel'),
    'symfony-php': ('symfony.bench.test', '/bench'),
    'node-javascript': ('node.bench.test', '/bench/javascript'),
    'bun-javascript': ('bun.bench.test', '/bench/javascript'),
    'deno-javascript': ('deno.bench.test', '/bench/javascript'),
}


def probe(host, path, port):
    connection = http.client.HTTPConnection('127.0.0.1', port)
    try:
        connection.request('GET', path, headers={'Host': host})
        response = connection.getresponse()
        body = response.read()
        expected = b'ok\n' if path.endswith('/plain') else b'{"message":"ok"}'
        if response.status != 200 or body != expected:
            raise RuntimeError(f'{host}{path}: status={response.status}, body={body[:100]!r}')
        headers = dict(response.getheaders())
        return {key: value for key, value in headers.items()
                if key.lower().startswith(('x-', 'content-type'))}
    finally:
        connection.close()


def resources(roots):
    output = subprocess.check_output(
        ['ps', '-axo', 'pid=,ppid=,rss=,time=,comm='], text=True)
    rows = []
    for line in output.splitlines():
        fields = line.split(None, 4)
        if len(fields) == 5:
            rows.append(dict(zip(('pid', 'ppid', 'rss_kib', 'cpu_time', 'command'), fields)))
    selected = set(roots)
    while True:
        expanded = selected | {int(row['pid']) for row in rows if int(row['ppid']) in selected}
        if expanded == selected:
            break
        selected = expanded
    return [row for row in rows if int(row['pid']) in selected]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=pathlib.Path, required=True)
    parser.add_argument('--port', type=int, default=8080)
    parser.add_argument('--connections', type=int, required=True)
    parser.add_argument('--threads', type=int, required=True)
    parser.add_argument('--duration', required=True, help='h2load duration, e.g. 5m')
    parser.add_argument('--sample-seconds', type=float, required=True)
    parser.add_argument('--family', type=int, action='append', default=[], help='Owned master PID; descendants are included')
    parser.add_argument('--case', choices=CASES, action='append')
    parser.add_argument('--check-only', action='store_true')
    args = parser.parse_args()
    if min(args.connections, args.threads, args.sample_seconds) <= 0:
        parser.error('Connections, threads and sample interval must be positive')
    args.output.mkdir(parents=True, exist_ok=False)
    metadata = vars(args).copy()
    metadata['output'] = str(args.output)
    metadata['started_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    metadata['h2load'] = subprocess.check_output(['h2load', '--version'], text=True).strip()
    (args.output / 'metadata.json').write_text(json.dumps(metadata, indent=2) + '\n')
    summary = []
    for name in args.case or CASES:
        host, prefix = CASES[name]
        for workload in ('plain', 'json'):
            stem = f'{name}-{workload}'
            path = f'{prefix}/{workload}'
            # Separate fresh HTTP connections expose the responding lane/process headers.
            with concurrent.futures.ThreadPoolExecutor(max_workers=args.threads) as executor:
                responses = list(executor.map(
                    lambda _: probe(host, path, args.port), range(args.connections)))
            (args.output / f'{stem}.headers.json').write_text(json.dumps(responses, indent=2) + '\n')
            print(f'{stem}: exact response verified', flush=True)
            if args.check_only:
                continue
            command = ['h2load', '--h1', '-c', str(args.connections), '-t', str(args.threads),
                       '-m', '1', '-D', args.duration, '-H', f'host: {host}',
                       f'http://127.0.0.1:{args.port}{path}']
            samples = []
            started = time.monotonic()
            with (args.output / f'{stem}.txt').open('w') as output:
                process = subprocess.Popen(command, stdout=output, stderr=subprocess.STDOUT)
                while True:
                    samples.append({'elapsed_s': time.monotonic() - started,
                                    'processes': resources(args.family) if args.family else []})
                    try:
                        code = process.wait(timeout=args.sample_seconds)
                        break
                    except subprocess.TimeoutExpired:
                        pass
            samples.append({'elapsed_s': time.monotonic() - started,
                            'processes': resources(args.family) if args.family else []})
            (args.output / f'{stem}.resources.json').write_text(json.dumps(samples, indent=2) + '\n')
            result = (args.output / f'{stem}.txt').read_text()
            rate = re.search(r'finished in .*?, ([\d.]+) req/s', result)
            latency = re.search(r'^request\s*:\s*(.+)$', result, re.MULTILINE)
            percentiles = re.search(r'^\s*min\s+max\s+median\s+p95\s+p99', result, re.MULTILINE)
            row = {'case': stem, 'command': command, 'exit_code': code,
                   'requests_per_second': float(rate[1]) if rate else None,
                   'p99': latency[1].split()[4] if latency and percentiles else None,
                   'requests': next((line for line in result.splitlines() if line.startswith('requests:')), None),
                   'status_codes': next((line for line in result.splitlines() if line.startswith('status codes:')), None)}
            summary.append(row)
            (args.output / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
            print(json.dumps(row), flush=True)
            if code != 0 or rate is None:
                raise RuntimeError(f'{stem}: incomplete measurement; inspect raw output')


if __name__ == '__main__':
    main()
