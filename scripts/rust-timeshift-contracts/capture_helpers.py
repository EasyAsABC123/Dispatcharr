"""Capture pure helper behavior from mounted immutable original source.
All application credit belongs to the original Dispatcharr contributors.
"""
import hashlib
import json
from pathlib import Path
import sys
from datetime import datetime, timezone
sys.path.insert(0, '/app')
from apps.timeshift import helpers


def outcome(function, *args, **kwargs):
    try:
        result = function(*args, **kwargs)
        if isinstance(result, datetime):
            result = result.isoformat(timespec='seconds')
        return {'value': result}
    except Exception as error:
        return {'exception': type(error).__name__}


def main():
    cases = []
    timestamps = [None, '', '   ', 'bad', '2026-99-99:99-99',
        '2026-07-09:14-30', '2026-07-09:14-30-45', '2026-07-09_14-30',
        '2026-07-09:14:30', '2026-07-09:14:30:45', '2026-07-09 14:30:45',
        '2026-07-09:14-30:45', '2026-07-09T14:30:45Z',
        '2026-07-09T16:30:45+02:00', '2026-07-09T12:30:45-02:00',
        '2026-07-09T14:30:45.987654Z', '2026-07-09',
        '1720521000', '1720521000123', '123456789', '123456789012',
        '9999999999999', 1720521000, False]
    for value in timestamps:
        for name in ['normalize_catchup_timestamp_input', 'parse_catchup_timestamp',
                     'format_timestamp_as_colon_dash', 'format_timestamp_as_colon_seconds',
                     'format_timestamp_as_underscore', 'format_timestamp_as_sql_datetime']:
            cases.append({'function': name, 'input': value,
                          'output': outcome(getattr(helpers, name), value)})
    for value in [None, '', '  ', 0, -1, 1, 30, 475, 479, 480, 481, 999,
                  '2.5', 'abc', ' 42 ', True, '1_0', '+2', '-4']:
        cases.append({'function': 'client_duration_to_window', 'input': value,
                      'output': outcome(helpers.client_duration_to_window, value)})
    for zone in [None, '', 'UTC', 'Europe/Brussels', 'America/New_York', 'Asia/Kathmandu', 'not-a-zone']:
        for timestamp in ['2026-03-29T00:30:00Z', '2026-03-29T01:30:00Z',
                          '2026-11-01T05:30:00Z', '2026-11-01T06:30:00Z', 'bad']:
            cases.append({'function': 'convert_timestamp_to_provider_tz', 'input': [timestamp, zone],
                          'output': outcome(helpers.convert_timestamp_to_provider_tz, timestamp, zone)})
    # Explicit placeholder credentials: no account data is loaded or persisted.
    creds = helpers.TimeshiftCredentials('http://provider.invalid/base/', '<fixture-user>', '<fixture-password>')
    for timestamp in ['2026-07-09T14:30:45Z', '2026-07-09_14-30', 'bad']:
        cases.append({'function': 'build_timeshift_candidate_urls', 'input': [timestamp, 65],
                      'output': outcome(helpers.build_timeshift_candidate_urls, creds, '123', timestamp, 65)})
    for timestamp in ['2026-11-09T12:00:00Z', '2026-11-09T11:59:59Z', '2026-11-08T12:00:00Z',
                      '2026-11-08T11:59:59Z', '2026-11-10T12:00:00Z', 'bad']:
        cases.append({'function': 'programme_age_days', 'input': timestamp,
                      'output': outcome(helpers.programme_age_days, timestamp,
                                        now=datetime(2026, 11, 9, 12, tzinfo=timezone.utc))})
    report = {'format_version': 1, 'scope': 'original Python pure Timeshift helper characterization',
              'capture_complete': True, 'rust_parity_verified': False, 'performance_verified': False,
              'source_sha256': {name: hashlib.sha256((Path('/app')/name).read_bytes()).hexdigest()
                                for name in ['apps/timeshift/helpers.py', 'apps/timeshift/redis_keys.py']},
              'vector_count': len(cases), 'cases': cases}
    print(json.dumps(report, indent=2))

if __name__ == '__main__':
    main()
