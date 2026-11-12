"""Pure actual-process witness parsing; no HOSTNAME or provider fallback."""
import hashlib
import re


def process_stat(raw):
    if type(raw) is not bytes or not 1 <= len(raw) <= 16384: raise ValueError('BoundedProcStat')
    end = raw.rfind(b')'); begin = raw.find(b'(')
    if not 0 < begin < end: raise ValueError('ProcStatShape')
    pid = int(raw[:begin].strip()); fields = raw[end + 2:].split()
    if len(fields) < 20 or not fields[19].isdigit() or not 0 < int(fields[19]) < 2**64 or not 0 < pid < 2**31: raise ValueError('ProcStatFields')
    return {'pid': pid, 'comm': raw[begin + 1:end].decode(), 'ppid': int(fields[1]), 'starttime': fields[19].decode()}


def kernel(hostname, stat, status, cgroup, expected_pid, expected_start=None, native=False):
    if type(hostname) is not bytes or len(hostname) > 256: raise ValueError('KernelHostnameBytes')
    name = hostname.decode('ascii').rstrip('\n')
    if not re.fullmatch('[A-Za-z0-9_.-]{1,64}', name): raise ValueError('ControlledASCIIKernelHostname')
    proc = process_stat(stat)
    if type(expected_pid) is not int or proc['pid'] != expected_pid or (expected_start is not None and proc['starttime'] != expected_start): raise ValueError('KnownActualApplicationProcess')
    if len(status) > 16384 or len(cgroup) > 16384: raise ValueError('ProcWitnessByteBounds')
    lines = dict(line.split(':', 1) for line in status.decode().splitlines() if ':' in line)
    uid = int(lines['Uid'].split()[0])
    if native and (expected_pid != 1 or uid != 65532 or proc['comm'] != 'dispatcharr-rs'): raise ValueError('NativeMainProcessUID')
    return {**proc, 'raw_witness': {'hostname_hex': hostname.hex(), 'stat_hex': stat.hex(), 'status_hex': status.hex(), 'cgroup_hex': cgroup.hex()}, 'identity_diagnostic_only': True, 'kernel_hostname': name, 'uid': uid, 'expected_worker_header': name + '-' + str(proc['pid']), 'cgroup_identity_sha256': hashlib.sha256(cgroup).hexdigest(), 'cgroup_usage_sampled': False, 'probe_process_identity_not_substituted': True}


def worker(headers):
    if type(headers) is not list or any(type(row) is not list or len(row) != 2 for row in headers): raise ValueError('RawHeaderPairs')
    values = [value for key, value in headers if key.lower() == 'x-worker-id']
    if len(values) > 1: raise ValueError('AmbiguousWorkerHeader')
    return None if not values else values[0]


def verify_worker(headers, witness):
    raw = witness.get('raw_witness', {})
    if set(raw) != {'hostname_hex','stat_hex','status_hex','cgroup_hex'}: raise ValueError('LiteralKernelWitnessRequired')
    derived = kernel(bytes.fromhex(raw['hostname_hex']), bytes.fromhex(raw['stat_hex']), bytes.fromhex(raw['status_hex']), bytes.fromhex(raw['cgroup_hex']), witness['pid'], witness['starttime'])
    if any(witness.get(key) != derived[key] for key in ['pid','comm','ppid','starttime','kernel_hostname','uid','expected_worker_header','cgroup_identity_sha256']): raise ValueError('KernelWitnessProjectionChanged')
    value = worker(headers)
    if value != witness['expected_worker_header'] or value == 'owned-provider-spoof' or value.startswith('owned-environment-spoof-'): raise ValueError('OwnKernelWorkerIdentityRequired')
    return value
