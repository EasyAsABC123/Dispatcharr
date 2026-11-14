"""One bounded safe-GET capability scan inside an already owned release fixture.
No Source replay, subprocess, stack startup or acceptance from component evidence.
"""
import argparse
import hashlib
import ipaddress
import json
import os
import re
import time
import urllib.parse
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent / "helpers"))
from absolute_http import Observation

CASES = [
    ('plugin-repositories', '/api/plugins/repos/', True, 200, 'application/json', list),
    ('plugin-catalogue', '/api/plugins/repos/available/', True, 200, 'application/json', dict),
    ('plugin-settings', '/api/plugins/repos/settings/', True, 200, 'application/json', dict),
    ('plugin-discovery', '/api/plugins/plugins/', True, 200, 'application/json', dict),
    ('backup-files', '/api/backups/', True, 200, 'application/json', list),
    ('backup-schedule', '/api/backups/schedule/', True, 200, 'application/json', dict),
    ('Django-admin-login', '/admin/login/', False, 200, 'text/html', None),
    ('HDHR-discover', '/hdhr/discover.json', True, 200, 'application/json', dict),
    ('HDHR-lineup', '/hdhr/lineup.json', True, 200, 'application/json', list),
    ('HDHR-lineup-status', '/hdhr/lineup_status.json', True, 200, 'application/json', dict),
]



def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base', required=True)
    parser.add_argument('--api-key-file', required=True)
    parser.add_argument('--owned-context', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    url = urllib.parse.urlsplit(args.base)
    if url.scheme != 'http' or url.username is not None or url.password is not None or url.path not in ('', '/') or url.query or url.fragment:
        raise ValueError('literal owned HTTP origin only')
    ip = ipaddress.ip_address(url.hostname)
    if url.hostname != '127.0.0.1' or type(url.port) is not int:
        raise ValueError('must run in attested owned Main peer namespace over numeric loopback')
    context = json.loads(Path(args.owned_context).read_text())
    if context.get('owned_disposable_fixture') is not True or context.get('base') != args.base or re.fullmatch('rust_milestone_[a-f0-9]{10}', context.get('database', '')) is None or context.get('production_Main_image_attested') is not True:
        raise ValueError('parent actual owned-DB/image/network attestation required')
    if re.fullmatch('sha256:[a-f0-9]{64}', context.get('image_id', '')) is None:
        raise ValueError('actual release image identity')
    key = Path(args.api_key_file).read_bytes()
    if len(key) > 1024 or not key or key.endswith(b'\n') or any(v < 33 or v > 126 for v in key):
        raise ValueError('literal owned disposable API key file')
    output = Path(args.output)
    output.mkdir(mode=0o700, exist_ok=False)
    report = {'capture_only': True, 'passed': False, 'capture_complete': False, 'qualification_scope': 'safe required-route capability presence only; NOT full behavior/Main acceptance', 'candidate_image': context['image_id'], 'owned_database': context['database'], 'API_key_sha256': hashlib.sha256(key).hexdigest(), 'requests': [], 'Source_calls': 0, 'actual_calls': 0, 'phase_started_epoch': time.time(), 'PID_namespace': os.readlink('/proc/self/ns/pid'), 'network_namespace': os.readlink('/proc/self/ns/net')}
    start = time.monotonic()
    for index, (name, path, auth, expected_status, media, shape) in enumerate(CASES, 1):
        if time.monotonic() - start > 60:
            break
        row = {'index': index, 'name': name, 'method': 'GET', 'path': path, 'authenticated': auth, 'expected_capability_status': expected_status, 'expected_media_type': media, 'began_epoch': time.time()}
        report['requests'].append(row)
        headers = {'Accept': 'text/html' if media == 'text/html' else 'application/json'}
        if auth:
            headers['X-API-Key'] = key.decode('ascii')
        observation = Observation(args.base, seconds=3)
        report['actual_calls'] += 1
        try:
            response = observation.request(path, headers, method='GET')
            raw = response.raw.read(1048577)
            if len(raw) > 1048576:
                prefix = raw[:1048576]
                (output / f'response-{index:02d}.prefix').write_bytes(prefix)
                row['partial_body'] = {'observed_bytes': len(raw), 'retained_bytes': len(prefix), 'retained_sha256': hashlib.sha256(prefix).hexdigest(), 'complete': False, 'full_body_size': 'UNMEASURED'}
                raise ValueError('1MiB response capacity; raw prefix retained is incomplete')
            (output / f'response-{index:02d}.body').write_bytes(raw)
            ct = response.headers.get('Content-Type', '')
            row.update(status=response.status_code, headers=list(response.headers.items()), body_bytes=len(raw), body_sha256=hashlib.sha256(raw).hexdigest(), response_complete=True)
            try:
                parsed = json.loads(raw)
            except (ValueError, UnicodeDecodeError):
                parsed = None
            row['parsed_JSON'] = parsed
            row['capability_present'] = response.status_code == expected_status and ct.split(';', 1)[0].strip().lower() == media and (shape is None or type(parsed) is shape)
            row['absolute_observation_completed'] = observation.complete()
            row['full_behavior_accepted'] = False
        except BaseException as error:
            row.update(error={'type': type(error).__name__, 'message': str(error)}, capability_present=False, response_complete=False)
        finally:
            try:
                row['actual_client_socket_retirement'] = observation.cancel()
            except BaseException as error:
                row.update(close_error={'type': type(error).__name__, 'message': str(error)}, capability_present=False, response_complete=False)
            row['completed_epoch'] = time.time()
            (output / 'capability-capture.json').write_text(json.dumps(report, indent=2, allow_nan=False) + '\n')
    report['capture_complete'] = report['actual_calls'] == len(CASES) and all(r.get('response_complete') is True for r in report['requests'])
    report['missing_capability_cells'] = [r['name'] for r in report['requests'] if r.get('capability_present') is not True]
    report['capability_scan_passed'] = report['capture_complete'] and not report['missing_capability_cells']
    report['elapsed_seconds'] = time.monotonic() - start
    report['passed'] = False  # This scanner never accepts full M4/M7/M8 behavior.
    (output / 'capability-capture.json').write_text(json.dumps(report, indent=2, allow_nan=False) + '\n')
    return 0 if report['capability_scan_passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
