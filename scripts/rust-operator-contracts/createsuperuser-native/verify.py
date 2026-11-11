"""Verify scoped Original/native CLI observations. Original authors retain credit."""
import argparse
import datetime
import hashlib
import json
from pathlib import Path

def require(value, message):
    if not value:
        raise ValueError(message)

def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False)

def clock_rows(report):
    known = {}
    result = {}
    for case in report['cases']:
        for row in case['after']['users']:
            identity = row['id']
            require(type(identity) is int and identity > 0, 'user ID type')
            if identity not in known:
                require(all(identity != old['id'] for old in case['before']['users']), 'unknown existing user clock')
                value = datetime.datetime.fromisoformat(row['date_joined']).timestamp()
                require(case['start_epoch'] <= value <= case['end_epoch'], 'creation clock outside actual command')
                known[identity] = row['date_joined']
        for state in ['before', 'after']:
            rows = []
            for original in case[state]['users']:
                identity = original['id']
                require(identity in known and original['date_joined'] == known[identity], 'creation clock changed later')
                row = dict(original)
                row['date_joined'] = {'actual_created_user_clock_slot': identity}
                rows.append(row)
            result[(case['id'], state)] = rows
    require(len(known) == 10, 'expected ten actual creations')
    return result

def verify(source, native):
    for report, is_native in [(source, False), (native, True)]:
        require(report['passed'] is True and report['native_verified'] is is_native, 'scope/status mismatch')
        require(report['performance_verified'] is False, 'unexpected performance claim')
        require(len(report['cases']) == 22 and len(report['checks']) == 95, 'incomplete case/check matrix')
        require(all(c['passed'] is True for c in report['checks']), 'failed functional check')
        require(len({c['id'] for c in report['cases']}) == 22, 'duplicate case')
        for case in report['cases']:
            require(type(case['exit_code']) is int, 'exit-code type')
            for stream in ['stdout', 'stderr']:
                require(hashlib.sha256(case[stream].encode()).hexdigest() == case[stream + '_sha256'], 'transcript hash mismatch')
    require([c['id'] for c in source['cases']] == [c['id'] for c in native['cases']], 'case identity/order mismatch')
    a, b = clock_rows(source), clock_rows(native)
    comparisons = 0
    def equal(left, right, label):
        nonlocal comparisons
        require(canonical(left) == canonical(right), label)
        comparisons += 1
    for original, port in zip(source['cases'], native['cases']):
        for field in ['arguments', 'exit_code', 'events']:
            equal(original[field], port[field], 'command/event mismatch')
        for state in ['before', 'after']:
            equal(a[(original['id'], state)], b[(port['id'], state)], 'typed user field mismatch')
            equal(original[state]['user_sequence'], port[state]['user_sequence'], 'sequence mismatch')
    return {'passed': True, 'cases': 22, 'strict_typed_comparisons': comparisons,
            'actual_creation_clock_slots_per_backend': 10, 'full_CLI_byte_parity': False,
            'full_M8_or_package_or_performance_acceptance': False}

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--native', type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(verify(json.loads(args.source.read_text()), json.loads(args.native.read_text()))))

if __name__ == '__main__':
    main()
