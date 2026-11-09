#!/usr/bin/env python3
"""Extract and execute the real baseline find_ts_sync AST, without app imports."""
import argparse
import ast
import hashlib
import json
from pathlib import Path
import random
import subprocess

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / 'apps/proxy/utils.py'
SOURCE_SHA = 'ea821d26a5336eabaa5d09687c728cc89aa5b447d1024d025031e1ee8a792423'


def legacy():
    data = SOURCE.read_bytes()
    if hashlib.sha256(data).hexdigest() != SOURCE_SHA:
        raise RuntimeError('Sync helper source differs from pinned baseline')
    module = ast.parse(data, filename=str(SOURCE))
    selected = [node for node in module.body if
                isinstance(node, ast.FunctionDef) and node.name == 'find_ts_sync' or
                isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and
                    target.id in ('_TS_PACKET_SIZE', '_TS_SYNC_BYTE') for target in node.targets)]
    namespace = {}
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(SOURCE), 'exec'), namespace)
    return namespace['find_ts_sync']


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--seed', type=int, default=20261109)
    args = parser.parse_args()
    reference = legacy()
    rng = random.Random(args.seed)
    vectors = [b'', b'G', b'G' * 376, b'G' * 377, b'<html>error</html>' * 100]
    for size in (376, 377, 512, 1024, 4096):
        for offset in range(0, min(size, 400), 7):
            data = bytearray(b'X' * size)
            for index in (offset, offset + 188, offset + 376):
                if index < size:
                    data[index] = 0x47
            vectors.append(bytes(data))
    for _ in range(2000):
        data = bytearray(rng.randbytes(rng.randrange(0, 4096)))
        if len(data) > 376 and rng.randrange(2):
            offset = rng.randrange(len(data) - 376)
            data[offset] = data[offset + 188] = data[offset + 376] = 0x47
        vectors.append(bytes(data))
    expected = [reference(data) for data in vectors]
    process = subprocess.run(['cargo', 'run', '--manifest-path', str(ROOT / 'rust/Cargo.toml'),
                              '--locked', '--quiet', '-p', 'dispatcharr-stream-core',
                              '--example', 'ts_sync_parity'], input='\n'.join(data.hex() for data in vectors) + '\n',
                             text=True, capture_output=True, timeout=60, check=True)
    actual = [int(line) for line in process.stdout.splitlines()]
    if actual != expected:
        raise AssertionError('Rust sync offsets differ from the pinned Python helper')
    report = dict(scope='startup-sync-helper-only', source_sha256=SOURCE_SHA, passed=True,
                  vectors=len(vectors), seed=args.seed, application_performance_verified=False,
                  note='The current Python HTTP reader does not call this helper; bounded Rust guard is opt-in.')
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
