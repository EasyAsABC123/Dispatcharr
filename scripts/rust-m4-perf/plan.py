#!/usr/bin/env python3
"""Frozen M4 experiment policy; application credit belongs to Dispatcharr authors."""
import hashlib
import json
import base64
import time
from pathlib import Path

FORMAT = 1


def validate_fixture_tokens(seed, required_seconds, now=None):
    """Check fixture lifetime only; actual Source/native authenticate signatures.

    A full matrix can outlast the production30-minute default. Never log tokens
    or silently change application auth configuration to hide fixture expiry.
    """
    now = time.time() if now is None else now
    tokens = [user['token'] for user in seed['users']] + [seed['observer_token']]
    if not seed['users']:
        raise ValueError('Synthetic viewers missing')
    remaining = []
    for token in tokens:
        try:
            parts = token.split('.')
            if len(parts) != 3:
                raise ValueError
            payload = parts[1].encode('ascii')
            claims = json.loads(base64.b64decode(payload+b'='*((-len(payload))%4),
                                                altchars=b'-_', validate=True))
            expiry = claims['exp']
            if type(expiry) is not int or expiry-now < required_seconds:
                raise ValueError
            remaining.append(expiry-now)
        except (ValueError, TypeError, KeyError, UnicodeError):
            raise ValueError('Synthetic token lifetime insufficient or malformed') from None
    return {'token_count':len(tokens), 'minimum_remaining_seconds':int(min(remaining)),
            'application_jwt_settings_modified':False}


PLAN = {
    'format_version': FORMAT,
    'scope': 'Sustained native XC VOD and archive catch-up playback subset; not full application acceptance',
    'workloads': [{'protocol': protocol, 'viewers': viewers} for protocol in ('vod', 'timeshift') for viewers in (1, 10)],
    'trials': 3,
    'orders': [['source', 'candidate'], ['candidate', 'source'], ['source', 'candidate']],
    'warmup_seconds': 15,
    'measurement_seconds': 60,
    'sample_seconds': 2,
    'prefix_bytes': 1024 * 1024,
    'decoder_seconds': 8,
    'read_timeout_seconds': 10,
    'fixture': {'duration_seconds': 900, 'video': 'H264320x18025fps', 'audio': 'AAC48kHz',
                'vod_bytes_per_second': 262144, 'timeshift_bytes_per_second': 500000,
                'origin_workers': 'ThreadingHTTPServer; no transcoding during trials'},
    'service_limits': {'infrastructure': {'memory_bytes': 1024**3, 'nano_cpus': 10**9},
                       'origin': {'memory_bytes': 1024**3, 'nano_cpus': 2 * 10**9},
                       'backend': {'memory_bytes': 512 * 1024**2, 'nano_cpus': 2 * 10**9}},
    'selected_services': {'source': ['infrastructure', 'origin', 'source'],
                          'candidate': ['infrastructure', 'origin', 'candidate']},
    'websocket_observers': 1,
    'provider_capacity': 128,
    'budgets': {'max_cpu_ratio': .80, 'max_memory_ratio': .95,
                'min_throughput_ratio': .95, 'max_first_byte_ratio': 1.10,
                'first_byte_floor_ms': 50, 'max_errors': 0,
                'min_samples': 20, 'min_viewer_rate_fraction': .90,
                'max_viewer_rate_fraction': 1.10},
    'reset': 'Both backends stopped; isolated Redis FLUSHDB; same DB fixture; fresh active backend. No reconnect between warmup and measurement.',
    'acceptance': 'All three pairs of each workload must pass all correctness and resource gates. Report medians additionally. No discarded trials or budget retuning.',
}


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def freeze(path):
    record = {'plan': PLAN, 'plan_sha256': digest(PLAN), 'measurements_started': False}
    Path(path).write_text(json.dumps(record, indent=2) + '\n')
    return record


def validate_plan(record):
    if record.get('plan_sha256') != digest(record.get('plan')) or record.get('plan') != PLAN:
        raise ValueError('Frozen plan differs from reviewed experiment policy')
    return record['plan']


def authorize_quiet(path, plan_sha256, image_id):
    """Explicit fresh root-issued authorization; time expiry is not authorization."""
    import datetime
    record = json.loads(Path(path).read_text())
    now = datetime.datetime.now(datetime.timezone.utc)
    expiry = datetime.datetime.fromisoformat(record['expires_utc'])
    if (record.get('scope') != 'm4-resource-measurement' or
            record.get('plan_sha256') != plan_sha256 or record.get('image_id') != image_id or
            record.get('authorized') is not True or expiry.tzinfo is None or now >= expiry):
        raise ValueError('Missing or expired coordinated quiet-window authorization')
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


if __name__ == '__main__':
    import argparse
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--freeze', type=Path, required=True)
    args = p.parse_args()
    print(json.dumps(freeze(args.freeze), indent=2))


def validate_limits(actual, plan):
    """Inspect observed cgroups, not just requested Compose configuration."""
    if set(actual) != {'infrastructure', 'origin', 'source', 'candidate'}:
        raise ValueError('All four explicitly isolated service cgroups are required')
    for service, observed in actual.items():
        expected = plan['service_limits']['backend' if service in ('source', 'candidate') else service]
        if observed != expected:
            raise ValueError('Observed service limits differ from frozen experiment plan')


def final_acceptance(pairs, plan):
    expected = {(work['protocol'], work['viewers'], trial)
                for work in plan['workloads'] for trial in range(1, plan['trials'] + 1)}
    observed = [(row['protocol'], row['viewers'], row['trial']) for row in pairs]
    return (len(observed) == len(expected) and set(observed) == expected and
            all(row.get('passed') is True for row in pairs))
