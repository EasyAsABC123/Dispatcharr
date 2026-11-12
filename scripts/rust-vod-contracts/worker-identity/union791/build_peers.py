"""Build exact reconstructed source before staging a NEW regression run.
Historical ELF hashes are not assumed reproducible across source paths/toolchains.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import selectors
import shutil
import signal
import subprocess
import time
from reconstruct import verify, INPUTS_SHA

HERE = Path(__file__).resolve().parent
WANTED = {'http', 'retained_reserve', 'lease_physical', 'idle_entry', 'generation_scope', 'generation_scope_physical', 'pending_follower_http', 'http_recovery'}


def compile_bounded(argv, cwd, env, output, timeout=600, limit=8388608):
    if not 0 < timeout <= 600 or type(limit) is not int or not 1 <= limit <= 8388608: raise ValueError('CompilerCollectorBounds')
    process = subprocess.Popen(argv, cwd=cwd, env=env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
    selector = selectors.DefaultSelector(); selector.register(process.stdout, selectors.EVENT_READ, 'stdout'); selector.register(process.stderr, selectors.EVENT_READ, 'stderr')
    paths = {name: Path(output) / ('cargo.' + name) for name in ('stdout', 'stderr')}
    streams = {name: path.open('xb') for name, path in paths.items()}; used = {'stdout': 0, 'stderr': 0}; deadline = time.monotonic() + timeout
    failed = False
    try:
        while selector.get_map():
            if time.monotonic() >= deadline: raise TimeoutError('Cargo600Seconds')
            for key, _ in selector.select(.05):
                data = os.read(key.fileobj.fileno(), 65536)
                if not data: selector.unregister(key.fileobj); continue
                used[key.data] += len(data)
                if used[key.data] > limit: raise ValueError('CargoOutputByteCapacity')
                streams[key.data].write(data)
        code = process.wait(timeout=max(.001, deadline - time.monotonic()))
        if code != 0: raise RuntimeError('CargoFailed:' + str(code))
    except BaseException:
        failed = True
        raise
    finally:
        if failed or process.poll() is None:
            # This is the session/process group created by this exact Popen.
            for sig in (signal.SIGTERM, signal.SIGKILL):
                try: os.killpg(process.pid, sig)
                except ProcessLookupError: pass
                try: process.wait(timeout=3)
                except subprocess.TimeoutExpired: pass
                until = time.monotonic() + 3
                while time.monotonic() < until:
                    try: os.killpg(process.pid, 0)
                    except ProcessLookupError: break
                    time.sleep(.01)
                else: continue
                break
            else: raise RuntimeError('OwnedCargoProcessGroupNotSettled')
        selector.close(); process.stdout.close(); process.stderr.close()
        for stream in streams.values(): stream.close()
        if process.poll() is None: raise RuntimeError('CargoPhysicalOwnerNotReaped')
    return paths


def build(source, output):
    source, output = Path(source).resolve(), Path(output).resolve()
    verify(source)
    if output.exists() or output.is_symlink(): raise ValueError('FreshReplayOutputRequired')
    output.mkdir(parents=True, exist_ok=False)
    argv = ['cargo', 'test', '--locked', '--manifest-path', str(source / 'rust/Cargo.toml'), '-p', 'dispatcharr-vod-http', '--all-targets', '--no-run', '--message-format=json']
    paths = compile_bounded(argv, source, {**os.environ, 'CARGO_TARGET_DIR': str(output / 'target')}, output)
    verify(source)  # Cargo must not silently change qualified inputs/lockfile.
    rows = [json.loads(line) for line in paths['stdout'].read_text().splitlines()]
    if not any(row.get('reason') == 'build-finished' and row.get('success') is True for row in rows): raise ValueError('SuccessfulCompilerCompletionRequired')
    binaries = {}
    for row in rows:
        target = row.get('target', {}); name = target.get('name')
        if row.get('reason') != 'compiler-artifact' or name not in WANTED or not row.get('executable'): continue
        expected = source / 'rust/crates/vod-http/tests' / (name + '.rs')
        if Path(target.get('src_path', '')).resolve() != expected or target.get('kind') != ['test']: raise ValueError('ExactPeerSourceTarget')
        executable = Path(row['executable']).resolve()
        if not executable.is_relative_to(output / 'target') or not executable.is_file(): raise ValueError('OwnedFreshCargoExecutable')
        binaries[name] = {'path': str(executable), 'sha256': hashlib.sha256(executable.read_bytes()).hexdigest(), 'bytes': executable.stat().st_size}
    if set(binaries) != WANTED: raise ValueError('AllEightPeersRequired')
    manifest = {'passed': True, 'source_map_sha256': INPUTS_SHA, 'copied_only_after_actual_compiler_exit0': True, 'binaries': binaries, 'scope': 'fresh compilation only; NEW95 run still required; no historical ELF identity claim'}
    (output / 'outside-peers.json').write_text(json.dumps(manifest, indent=2) + '\n')
    shutil.copyfile(HERE / 'source-inputs.json', output / 'source-inputs.json')
    shutil.copyfile(HERE / 'run_regressions.py', output / 'run_regressions.py')
    (output / 'compilation-receipt.json').write_text(json.dumps({'actual_exit_code': 0, 'source_revision': 'f0bfc2f200dcfd1f64ae32a9855ea250731ce67f', 'argv': argv, 'unique_target': str(output / 'target'), 'runtime_map_sha256': INPUTS_SHA, 'cargo_stdout_sha256': hashlib.sha256(paths['stdout'].read_bytes()).hexdigest(), 'regressions_executed': False}, indent=2) + '\n')
    return {'fresh_peers': 8, 'Source_or_Main_app_executed': False, 'regressions_executed': False}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True); parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(); print(json.dumps(build(args.source, args.output), indent=2))
