"""Actual original/native rapid-reclose HTTP and WebSocket timer witness.
Full application credit belongs to original Dispatcharr authors/contributors.
"""
import copy
import importlib.util
import json
import math
from pathlib import Path
import time
from urllib.error import HTTPError
from urllib.parse import quote, urlsplit
from urllib.request import Request, build_opener

spec = importlib.util.spec_from_file_location('rapid_normal_vod_ws', Path(__file__).with_name('vod_ws_probe.py'))
normal = importlib.util.module_from_spec(spec)
spec.loader.exec_module(normal)
KINDS = {'vod_started', 'vod_stopped', 'vod_stats'}


def capture(base, ws_base, token, control, error_path=None):
    address = urlsplit(ws_base)
    peer = None
    responses = []
    events = []
    sid = 'fixture-packaged-vod-rapid-reclose'
    report = {'scope': 'Actual HTTP rapid-reclose plus packaged WebSocket, audit and Redis cleanup',
              'capture_complete': False, 'events': events,
              'application_parity_verified': False, 'performance_verified': False}
    try:
        peer = normal.Client(address.hostname, address.port or 80, '/ws/?token='+quote(token, safe=''))
        if peer.status != 101 or peer.receive() != {'message': normal.INITIAL}:
            raise RuntimeError('Rapid observer upgrade/acknowledgement failed')
        mark = control('rapid-audit-before')['after']

        def opened():
            request = Request(base+'/proxy/vod/movie/a4d78cd1-bf9f-4a63-a746-f40566825cf9/'+sid,
                              headers={'User-Agent': 'packaged-rapid-reclose'})
            try:
                response = build_opener(normal.NoRedirect()).open(request, timeout=10)
            except HTTPError as error:
                response = error
            responses.append(response)
            if response.status != 200 or len(response.read(8192)) != 8192:
                raise RuntimeError('Rapid media response was incomplete')
            return response

        def received():
            message = peer.receive(5).get('message', {})
            if (message.get('data') or {}).get('type') in KINDS:
                events.append(message)

        first = opened()
        deadline = time.monotonic()+5
        while not {'vod_started', 'vod_stats'} <= {e['data']['type'] for e in events}:
            if time.monotonic() > deadline:
                raise RuntimeError('Rapid start deadline')
            received()
        began = time.monotonic()
        first.close()
        def idle():
            while time.monotonic()-began < .9:
                current = control('rapid-inspect', sid)
                if not current['exists']:
                    raise RuntimeError('Rapid session expired before reconnect witness')
                if current['active_streams'] == 0:
                    return
                time.sleep(.005)
            raise RuntimeError('Server did not acknowledge rapid close inside grace')
        # A closed client socket is not a synchronous generator close. Wait for
        # the actual server count to reach zero before reopening; overlapping
        # requests only schedule the last timer in the original implementation.
        idle()
        report['first_server_idle_seconds'] = time.monotonic()-began
        second = opened()
        second.close()
        idle()
        report['reclose_elapsed_seconds'] = time.monotonic()-began
        if report['reclose_elapsed_seconds'] >= 1:
            raise RuntimeError('Rapid closes missed the original one-second grace')
        deadline = time.monotonic()+5
        while len(events) < 6:
            if time.monotonic() > deadline:
                raise RuntimeError('Rapid stop deadline')
            received()
        extra = peer.optional(.25)
        if extra and (extra.get('message', {}).get('data') or {}).get('type') in KINDS:
            events.append(extra['message'])
        report['event_types'] = [e['data']['type'] for e in events]
        report['final'] = control('rapid-audit-after', str(mark)+':'+sid)
        report['capture_complete'] = True
        return report
    except Exception as error:
        report['error_type'] = type(error).__name__
        if error_path is not None:
            Path(error_path).write_text(json.dumps(report, indent=2)+'\n')
        raise
    finally:
        for response in responses:
            response.close()
        if peer is not None:
            peer.close()


def compare(source, candidate):
    differences = []

    def stable(snapshot):
        snapshot = copy.deepcopy(snapshot)
        snapshot.pop('timestamp', None)
        for group in snapshot['vod_connections']:
            for connection in group['connections']:
                for key in ['connected_at', 'last_activity', 'duration', 'bytes_sent']:
                    connection.pop(key, None)
        return snapshot

    starts = []
    for label, capture in [('source', source), ('candidate', candidate)]:
        if not capture.get('capture_complete') or not 0 <= capture.get('reclose_elapsed_seconds', 99) < 1:
            differences.append(label+': incomplete or missed grace')
        events = capture.get('events', [])
        kinds = [event.get('data', {}).get('type') for event in events]
        if len(kinds) != 6 or any(kinds.count(kind) != count for kind, count in [('vod_started', 1), ('vod_stopped', 2), ('vod_stats', 3)]):
            differences.append(label+': timer multiplicity')
        if kinds[:2] != ['vod_started', 'vod_stats']:
            differences.append(label+': first start/stat order')
        initial = None
        for event in events:
            if event.get('type') != 'update' or 'success' in event.get('data', {}):
                differences.append(label+': envelope')
            if event.get('data', {}).get('type') != 'vod_stats':
                continue
            try:
                stats = json.loads(event['data']['stats'])
                if set(stats) != {'vod_connections', 'total_connections', 'timestamp'} or not math.isfinite(stats['timestamp']):
                    raise ValueError('statistics envelope')
                connections = [conn for group in stats['vod_connections'] for conn in group['connections']]
                if len(connections) != stats['total_connections'] or stats['total_connections'] not in (0, 1):
                    raise ValueError('active count')
                for conn in connections:
                    if conn['bytes_sent'] < 0 or conn['bytes_sent'] % 819200:
                        raise ValueError('consumed units')
                    if any(not math.isfinite(float(conn[key])) for key in ['connected_at', 'last_activity']):
                        raise ValueError('connection time')
                if initial is None:
                    if stats['total_connections'] != 1:
                        raise ValueError('missing initial connection')
                    initial = stable(stats)
                elif stats['total_connections'] and stable(stats) != initial:
                    raise ValueError('changed stable connection')
            except (ValueError, TypeError, KeyError):
                differences.append(label+': invalid full statistics')
        starts.append(initial)
        final = capture.get('final', {})
        if final.get('remaining_metadata') is not False or final.get('provider_connections') != 0:
            differences.append(label+': Redis cleanup')
        if [row['event_type'] for row in final.get('audit', [])] != ['vod_start', 'vod_stop', 'vod_stop']:
            differences.append(label+': audit multiplicity/order')
    if starts[0] != starts[1]:
        differences.append('Initial full stable statistics differ')
    transitions = [[event['data'] for event in capture.get('events', []) if event.get('data', {}).get('type') != 'vod_stats']
                   for capture in [source, candidate]]
    if transitions[0] != transitions[1]:
        differences.append('Transition payloads differ')
    if source.get('final', {}).get('audit') != candidate.get('final', {}).get('audit'):
        differences.append('Full audit payloads differ')
    return {'captured_packaged_rapid_reclose_match': not differences, 'differences': differences,
            'normalizations': ['Independent legacy timer threads can interleave stop/stat pairs; exact event multiplicities and transition/audit payloads retained',
                               'Snapshot times/elapsed/progress validated; complete stable DTOs compared; stopping snapshot may retain the same active DTO or be empty'],
            'application_parity_verified': False, 'performance_verified': False}
