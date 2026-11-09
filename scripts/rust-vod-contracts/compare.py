#!/usr/bin/env python3
"""Compare captured native wire behavior; never assert application parity/gains."""
import argparse
import json
from pathlib import Path


def events(report):
    if 'wire' in report:
        report = report['wire']
    return {event['case']: event for event in report['events']}


def semantic(value):
    if isinstance(value, dict):
        return {key: sorted(part.strip() for part in item.split(',')) if key.lower() == 'allow' and isinstance(item,str) else semantic(item) for key,item in value.items()}
    if isinstance(value,list):
        return [semantic(item) for item in value]
    return value


def compare(baseline, candidate):
    original, rewritten = events(baseline), events(candidate)
    differences = []
    for case in sorted(original.keys() | rewritten.keys()):
        if case not in original or case not in rewritten:
            differences.append({'case': case, 'field': 'case_present', 'baseline': case in original, 'candidate': case in rewritten})
            continue
        for field in ('status', 'bytes', 'body_sha256', 'headers', 'mint'):
            if semantic(original[case].get(field)) != semantic(rewritten[case].get(field)):
                differences.append({'case': case, 'field': field, 'baseline': original[case].get(field), 'candidate': rewritten[case].get(field)})
    return {'format_version': 1, 'scope': 'captured native XC VOD seven-case wire comparison only',
            'captured_wire_matches': not differences, 'differences': differences,
            'application_parity_verified': False, 'performance_verified': False,
            'remaining_gates': ['bound candidate build/source provenance', 'authenticated handoff and user/network controls',
                                'provider/cross-worker session lifecycle', 'full metadata/WebSocket side effects',
                                'decoded browser execution', 'resource/performance matrix']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline', required=True, type=Path)
    parser.add_argument('--candidate', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    report = compare(json.loads(args.baseline.read_text()), json.loads(args.candidate.read_text()))
    args.output.write_text(json.dumps(report, indent=2)+'\n')
    print(json.dumps({'captured_wire_matches': report['captured_wire_matches'], 'differences': len(report['differences'])}))
    raise SystemExit(0 if report['captured_wire_matches'] else 1)

if __name__ == '__main__':
    main()
