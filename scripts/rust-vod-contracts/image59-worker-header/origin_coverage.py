"""Literal Origin coverage, independent of viewer/generator interpretation."""
import hashlib
import math


def validate(value, nonce, terminal=False):
    if value.get('invalid') != [] or value.get('nonce_sha256') != nonce: raise ValueError('OwnedOriginStatus')
    connections, requests = value.get('connections'), value.get('requests')
    if type(connections) is not list or type(requests) is not list or not connections or not requests or len(connections) > 128 or len(requests) > 64: raise ValueError('MandatoryOriginCoverage')
    by_id = {}
    for row in connections:
        key = row.get('id')
        if type(key) is not int or not 1 <= key <= 128 or key in by_id or type(row.get('socket_closed')) is not bool or type(row.get('requests')) is not int: raise ValueError('OriginConnectionIdentity')
        by_id[key] = row
    seen = set(); counts = {key: 0 for key in by_id}
    kinds = {'preflight_GET', 'stream_GET', 'media_HEAD', 'control', 'unknown'}
    for row in requests:
        key, connection = row.get('id'), row.get('connection')
        if type(key) is not int or not 1 <= key <= 64 or key in seen or type(connection) is not int or connection not in by_id or row.get('kind') not in kinds or row.get('method') not in ('GET', 'HEAD') or type(row.get('in_flight')) is not bool: raise ValueError('OriginRequestIdentity')
        seen.add(key); counts[connection] += 1
        for field in ('began',):
            if type(row.get(field)) not in (int, float) or not math.isfinite(row[field]) or row[field] <= 0: raise ValueError('OriginRequestClock')
        if type(row.get('bytes_written')) is not int or not 0 <= row['bytes_written'] <= 8388608: raise ValueError('OriginBodyCount')
    if any(by_id[key]['requests'] != count for key, count in counts.items()): raise ValueError('OriginConnectionRequestJoin')
    expected = {'accepted_TCP_total': len(connections), 'actual_open_TCP': sum(not row['socket_closed'] for row in connections), 'actual_stream_GET_inflight': sum(row['kind'] == 'stream_GET' and row['in_flight'] for row in requests), 'media_HEAD_total': sum(row['kind'] == 'media_HEAD' for row in requests), 'control_total': sum(row['kind'] == 'control' for row in requests), 'transient_TCP_without_request': sum(count == 0 for count in counts.values())}
    if any(type(value.get(key)) is not int or value[key] != count for key, count in expected.items()): raise ValueError('OriginLiteralCounterTotals')
    if terminal and (expected['actual_open_TCP'] != 0 or any(row['in_flight'] for row in requests)): raise ValueError('OriginPhysicalTerminalIncomplete')
    return {'literal_coverage_valid': True, **expected, 'TCP_is_not_viewer': True, 'client_to_origin_association_inferred': False}
