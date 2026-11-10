"""Source-only qualification guards and normalization; no workload execution.
Original Dispatcharr authors retain full application credit and AGPL licensing.
"""
import datetime
import hashlib
import json
from pathlib import Path
import acceptance
from plan import POLICY, TREE, ARCHIVE, digest, select_period


def authorize(path, manifest):
    """Qualification precedes plan sealing, so bind policy rather than plan SHA."""
    if (manifest.get('passed') is not True or manifest.get('source_tree_sha256') != TREE
            or manifest.get('source_archive_sha256') != ARCHIVE):
        raise ValueError('Passing exact configured source/archive manifest required')
    raw = Path(path).read_bytes()
    if len(raw) > 16384:
        raise ValueError('Quiet authorization exceeds16KiB')
    value = json.loads(raw)
    expiry = datetime.datetime.fromisoformat(value['expires_utc'])
    boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
    if (value.get('scope') != 'guide-source-qualification'
            or value.get('authorized') is not True
            or value.get('candidate_image') != manifest.get('image_id')
            or value.get('candidate_manifest_canonical_sha256') != digest(manifest)
            or value.get('policy_sha256') != digest(POLICY)
            or value.get('boot_id') != boot
            or expiry.tzinfo is None
            or datetime.datetime.now(datetime.timezone.utc) >= expiry):
        raise ValueError('Fresh matching Source-only quiet authorization required')
    return hashlib.sha256(raw).hexdigest()


def row(workload, period, phase, backend_counters):
    """Normalize explicit Source phase evidence; never accept candidate metrics."""
    if workload not in POLICY['workloads'] or period not in workload['period_candidates_seconds']:
        raise ValueError('Undeclared qualification workload/rate')
    if set(backend_counters) != {'backend', 'cpu_percent_mean', 'memory_fraction',
                                 'throttled_period_fraction'} or backend_counters['backend'] != 'source':
        raise ValueError('Only Source backend counters may select pacing')
    # Qualification probes have15 samples rather than measurement's20 minimum;
    # all correctness and delivery failures still invalidate qualification.
    failures = [reason for reason in acceptance.phase(phase)
                if reason != 'insufficient_samples']
    minimum = 1
    if phase['samples'] < minimum:
        failures.append('insufficient_qualification_samples')
    for key in ['cpu_percent_mean', 'memory_fraction', 'throttled_period_fraction']:
        acceptance.number(backend_counters[key])
    if backend_counters['memory_fraction'] > 1 or backend_counters['throttled_period_fraction'] > 1:
        raise ValueError('Qualification fraction exceeds one')
    return {'backend': 'source', 'period_seconds': period,
            'errors': phase['errors'] + len(failures),
            'correct_responses': not failures,
            'pacing_fraction': phase['pacing_fraction'],
            'cpu_percent_mean': backend_counters['cpu_percent_mean'],
            'memory_fraction': backend_counters['memory_fraction'],
            'throttled_period_fraction': backend_counters['throttled_period_fraction']}


def complete(qualification):
    if set(qualification) != {work['name'] for work in POLICY['workloads']}:
        raise ValueError('Incomplete Source workload grid')
    return {work['name']: select_period(work, qualification[work['name']])
            for work in POLICY['workloads']}
