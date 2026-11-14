"""Owned finite-signature asset preparation; explicit container-only invocation.
Reuses bounded subreaper cleanup from the qualified Source crypto asset generator.
Original Dispatcharr authors retain application credit (ATTRIBUTION.md).
"""
import argparse
import ctypes
import shutil
import tempfile
import hashlib
import json
import os
import selectors
import resource
import re
import signal
import subprocess
import time
from pathlib import Path


def run(argv, output, serial, timeout=30, expected_exit=0):
    """Own the real child, wait, and pipes; bounded output, no shell or HOME changes."""
    start = time.monotonic()
    process = subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
    identity = Path('/proc', str(process.pid), 'stat').read_text().rsplit(')', 1)[1].split()[19]
    if not identity.isdecimal() or int(identity) <= 0:
        raise ValueError('asset child identity')
    selector = selectors.DefaultSelector()
    data = [bytearray(), bytearray()]
    eof = set()
    invalid = None
    forced = False
    try:
        for index, stream in enumerate((process.stdout, process.stderr)):
            selector.register(stream, selectors.EVENT_READ, index)
        while selector.get_map():
            if time.monotonic() - start > timeout:
                raise TimeoutError('asset child deadline')
            for key, _ in selector.select(.05):
                raw = os.read(key.fileobj.fileno(), 65536)
                if not raw:
                    eof.add(key.data)
                    selector.unregister(key.fileobj)
                    key.fileobj.close()
                    continue
                if sum(map(len, data)) + len(raw) > 262144:
                    raise ValueError('asset output capacity')
                data[key.data].extend(raw)
        process.wait(timeout=max(.01, timeout - (time.monotonic() - start)))
    except BaseException as exc:
        invalid = {'type': type(exc).__name__, 'message': str(exc)}
    finally:
        if process.poll() is None:
            forced = True
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=2)
        for stream in (process.stdout, process.stderr):
            if not stream.closed:
                stream.close()
        selector.close()
        row = {'argv': argv, 'pid': process.pid, 'observed_starttime': identity, 'wait': process.wait(), 'poll': process.poll(), 'EOF': sorted(eof), 'forced': forced, 'invalid': invalid, 'elapsed': time.monotonic() - start, 'expected_exit': expected_exit, 'PID_namespace': os.readlink('/proc/self/ns/pid')}
        (output / f'command-{serial:02d}.json').write_text(json.dumps(row, indent=2) + '\n')
        for tag, raw in zip(('stdout', 'stderr'), data):
            (output / f'command-{serial:02d}.{tag}').write_bytes(raw)
    if invalid or forced or eof != {0, 1} or row['wait'] != expected_exit or row['poll'] != expected_exit:
        raise ValueError('asset child actual wait/pipe failure')
    return bytes(data[0])


def fingerprints(raw):
    result = []
    for line in raw.decode('ascii').splitlines():
        values = line.split(':')
        if values[0] == 'fpr':
            if len(values) < 10 or re.fullmatch('[A-F0-9]{40}', values[9]) is None:
                raise ValueError('actual complete OpenPGP v4 fingerprint')
            result.append(values[9])
    if not result:
        raise ValueError('actual fingerprint missing')
    return result


def generate(output, definition):
    resource.setrlimit(resource.RLIMIT_FSIZE, (524288, 524288))
    output = Path(output).absolute()
    definition = Path(definition).absolute()
    output.mkdir(mode=0o755, exist_ok=False)
    receipts = output / 'receipts'
    receipts.mkdir(mode=0o755)
    # A short owned homedir keeps real GPG agent Unix paths below sun_path limits.
    home = Path(tempfile.mkdtemp(prefix='dispatch-plugin-sign-'))
    if ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno(), 'PR_SET_CHILD_SUBREAPER')
    known_descendants = {}
    descendant_reaps = []

    def observe_descendants():
        pending = [os.getpid()]
        observed = []
        while pending:
            parent = pending.pop()
            try:
                children = Path('/proc', str(parent), 'task', str(parent), 'children').read_text().split()
            except FileNotFoundError:
                children = []
            for raw_pid in children:
                pid = int(raw_pid)
                try:
                    start = Path('/proc', str(pid), 'stat').read_text().rsplit(')', 1)[1].split()[19]
                    argv = Path('/proc', str(pid), 'cmdline').read_bytes().split(b'\0')
                except FileNotFoundError:
                    continue
                if not start.isdecimal() or int(start) <= 0:
                    raise ValueError('actual descendant start identity')
                row = {'pid': pid, 'starttime': start, 'argv': [v.decode(errors='replace') for v in argv if v]}
                known_descendants[(pid, start)] = row
                observed.append(row)
                pending.append(pid)
        return observed
    serial = 0
    began = time.monotonic()
    report = {'passed': False, 'Source_executions': 0, 'network_calls': 0, 'commands': [], 'GPG_home': str(home), 'status': 'PARTIAL_ASSET_GENERATION', 'PID_namespace': os.readlink('/proc/self/ns/pid'), 'generator_UID': os.getuid(), 'actual_uid_map': Path('/proc/self/uid_map').read_text()}

    def command(argv, cleanup=False, expected_exit=0):
        nonlocal serial
        if serial >= (24 if cleanup else 23) or (not cleanup and time.monotonic() - began > 180):
            raise ValueError('finite asset command/lifetime capacity')
        serial += 1
        report['commands'].append(argv)
        try:
            return run(argv, receipts, serial, timeout=30 if cleanup else min(30, max(.01, 180 - (time.monotonic() - began))), expected_exit=expected_exit)
        finally:
            observe_descendants()

    def gpg(*args):
        return ['gpg', '--batch', '--no-tty', '--homedir', str(home), '--no-auto-key-retrieve', '--auto-key-locate', 'clear', *args]

    try:
        command(gpg('--pinentry-mode', 'loopback', '--passphrase', '', '--quick-generate-key', 'Owned Finite Manifest Fixture <owned-finite@plugin-fixture.test>', 'ed25519', 'sign', '0'))
        public = command(gpg('--armor', '--export', 'owned-finite@plugin-fixture.test'))
        (output / 'finite-public-key.asc').write_bytes(public)
        # Exact Original canonicalization expression, Python builtin numeric
        # parsing/spelling, insertion order and ASCII escaping; no Source API run.
        document = {'registry_name': 'Owned finite signing fixture', 'plugins': [], 'numeric': {'one': 1.0, 'minus_zero': -0.0, 'small': 1e-7, 'fixed': 1e-6, 'large': 1e20, 'exp': 1e21, 'subnormal': 5e-324, 'max': 1.7976931348623157e308, 'integer': 12345678901234567890123456789}, 'unicode': '\u96ea\U0001f600'}
        canonical = (json.dumps(document, separators=(',', ':')) + '\n').encode()
        (output / 'finite-canonical.json').write_bytes(canonical)
        command(gpg('--pinentry-mode', 'loopback', '--passphrase', '', '--armor', '--detach-sign', '--output', str(output / 'finite-signature.asc'), str(output / 'finite-canonical.json')))
        signature = (output / 'finite-signature.asc').read_text()
        (output / 'finite-valid.json').write_bytes(json.dumps({'manifest': document, 'signature': signature}, separators=(',', ':')).encode())
        changed = {**document, 'registry_name': 'Owned changed finite signing fixture'}
        (output / 'finite-changed-canonical.json').write_bytes((json.dumps(changed, separators=(',', ':')) + '\n').encode())
        (output / 'finite-invalid.json').write_bytes(json.dumps({'manifest': changed, 'signature': signature}, separators=(',', ':')).encode())
        report['fixture_fingerprints'] = fingerprints(command(gpg('--with-colons', '--fingerprint', '--list-keys', 'owned-finite@plugin-fixture.test')))
        command(gpg('--status-fd', '2', '--verify', str(output / 'finite-signature.asc'), str(output / 'finite-canonical.json')))
        if b'[GNUPG:] VALIDSIG ' not in (receipts / f'command-{serial:02d}.stderr').read_bytes():
            raise ValueError('Actual finite signature VALIDSIG required')
        command(gpg('--status-fd', '2', '--verify', str(output / 'finite-signature.asc'), str(output / 'finite-changed-canonical.json')), expected_exit=1)
        if b'[GNUPG:] BADSIG ' not in (receipts / f'command-{serial:02d}.stderr').read_bytes():
            raise ValueError('Actual finite changed signature BADSIG required')
        report['canonical_sha256'] = hashlib.sha256(canonical).hexdigest()
        report['scope'] = 'Python builtin Original canonical bytes + actual owned fixture GPG valid/invalid; no Source application/API replay'
        report['status'] = 'ASSET_COMMANDS_COMPLETED_PENDING_CHILD_CLEANUP'
    except BaseException as exc:
        report['error'] = {'type': type(exc).__name__, 'message': str(exc)}
    finally:
        try:
            command(['gpgconf', '--homedir', str(home), '--kill', 'all'], cleanup=True)
            # Actual descendant PID/generation absence must be independently collected
            # before executor admission. This generator never equates gpgconf0 with it.
            report['gpgconf_wait0'] = True
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                children = observe_descendants()
                try:
                    while True:
                        pid, status = os.waitpid(-1, os.WNOHANG)
                        if pid == 0:
                            break
                        descendant_reaps.append({'pid': pid, 'wait_status': status, 'exit_code': os.waitstatus_to_exitcode(status)})
                        report['descendant_reaps'] = list(descendant_reaps)
                        report['observed_descendants'] = list(known_descendants.values())
                        (output / 'asset-generation-report.json').write_text(json.dumps(report, indent=2) + '\n')
                        # These are adopted background helpers, not direct
                        # cryptographic commands. Retain their literal status;
                        # shutdown acceptance requires actual reaping and every
                        # known generation absent, never an invented exit0.
                except ChildProcessError:
                    pass
                if not observe_descendants():
                    break
                time.sleep(.05)
            if observe_descendants():
                raise ValueError('owned asset descendants remain after actual agent shutdown')
            report['descendant_reaps'] = descendant_reaps
            report['observed_descendants'] = list(known_descendants.values())
            report['known_descendant_absence'] = []
            for (pid, start), row in known_descendants.items():
                try:
                    current = Path('/proc', str(pid), 'stat').read_text().rsplit(')', 1)[1].split()[19]
                except FileNotFoundError:
                    current = None
                report['known_descendant_absence'].append({**row, 'current_starttime': current, 'absent': current != start})
            if not all(r['absent'] for r in report['known_descendant_absence']):
                raise ValueError('known asset descendant generation remains')
            shutil.rmtree(home)
            report['owned_temporary_home_absent'] = not home.exists()
        except BaseException as exc:
            report['cleanup_error'] = {'type': type(exc).__name__, 'message': str(exc)}
        report['direct_command_absence'] = []
        for path in receipts.glob('command-*.json'):
            row = json.loads(path.read_text())
            try:
                current = Path('/proc', str(row['pid']), 'stat').read_text().rsplit(')', 1)[1].split()[19]
            except FileNotFoundError:
                current = None
            report['direct_command_absence'].append({'pid': row['pid'], 'old_starttime': row['observed_starttime'], 'current_starttime': current, 'absent': current != row['observed_starttime']})
        if not all(row['absent'] for row in report['direct_command_absence']):
            report['cleanup_error'] = {'type': 'OwnershipError', 'message': 'known direct generation remains'}
        report['actual_command_count'] = serial
        report['elapsed_seconds'] = time.monotonic() - began
        report['asset_files'] = {p.name: {'bytes': p.stat().st_size, 'sha256': hashlib.sha256(p.read_bytes()).hexdigest()} for p in output.iterdir() if p.is_file()}
        report['passed'] = report.get('status') == 'ASSET_COMMANDS_COMPLETED_PENDING_CHILD_CLEANUP' and report.get('gpgconf_wait0') is True and report.get('owned_temporary_home_absent') is True and 'cleanup_error' not in report
        report['crypto_validation'] = 'Actual owned fixture CLI validation only; Source HTTPS/GPG outcomes remain UNMEASURED'
        (output / 'asset-generation-report.json').write_text(json.dumps(report, indent=2) + '\n')
        # Disposable fixture keys are readable through a private host parent; keep UID1000.
        for owned in output.rglob('*'):
            if owned.is_symlink():
                raise ValueError('no generated symlink permission rewrite')
            owned.chmod(0o755 if owned.is_dir() else 0o444)
        output.chmod(0o755)
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--execute-owned-container', action='store_true')
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    if not args.execute_owned_container:
        print(json.dumps({'state': 'not_run', 'key_generation': 0, 'application_runs': 0}))
        raise SystemExit(0)
    if args.output != Path('/evidence/finite-assets') or not Path('/owned-finite-context').is_file():
        raise SystemExit('Exact owned output/context required')
    context = json.loads(Path('/owned-finite-context').read_bytes())
    if context != {'owned_fixture': True, 'Original_application_runs': 0, 'public_contacts': 0}:
        raise SystemExit('Exact fixture context required')
    raise SystemExit(generate(args.output, Path('/tmp')))
