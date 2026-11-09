"""Reusable native session API source/candidate wire characterization."""
import json
import urllib.error
import urllib.request
import uuid


def fetch(base, path, *, body=None, token=None, method='POST'):
    headers = {'Content-Type': 'application/json'}
    if token:
        headers['Authorization'] = 'Bearer '+token
    request = urllib.request.Request(base+path, method=method, headers=headers,
                                     data=json.dumps(body).encode() if body is not None else None)
    try:
        response = urllib.request.urlopen(request, timeout=15)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        raw = response.read(65537)
        if len(raw) > 65536:
            raise ValueError('Session API response exceeds bounded probe')
        try:
            value = json.loads(raw) if raw else None
        except ValueError:
            value = {'non_json': True, 'bytes': len(raw)}
        return response.status, value, response.headers.get('Allow')


def capture(base, seed):
    events = []
    tokens = {role: values['token'] for role, values in seed['users'].items()}
    path = '/api/catchup/sessions/'
    valid = {'channel_uuid': seed['channel_uuid'], 'start': '2026-11-09T11:00:00Z', 'duration': 30}
    def record(name, route=path, body=None, role='owner', method='POST'):
        status, value, allow = fetch(base, route, body=body, token=tokens.get(role), method=method)
        captured = dict(value) if isinstance(value, dict) else value
        if isinstance(captured, dict) and 'session_id' in captured:
            captured['session_id'] = '<session>'
            captured['playback_url'] = captured['playback_url'].split('?')[0]+'?session_id=<session>'
            captured['expires_at'] = '<handshake-deadline>'
        events.append({'case': name, 'status': status, 'body': captured, 'allow': allow})
        return status, value
    record('missing-auth', body=valid, role=None)
    record('viewer-denied', body=valid, role='viewer')
    record('catchup-disabled-before-validation', body={}, role='disabled')
    record('required-fields', body={})
    for name, override in [('invalid-uuid', {'channel_uuid': 'bad'}), ('blank-start', {'start': ' '}),
        ('invalid-start', {'start': 'bad'}), ('duration-zero', {'duration': 0}),
        ('duration-over-cap', {'duration': 481}), ('duration-fraction', {'duration': '3.1'}),
        ('duration-null', {'duration': None}), ('unsupported-channel', {'channel_uuid': seed['unsupported_uuid']}),
        ('missing-channel', {'channel_uuid': str(uuid.UUID(int=1))})]:
        record(name, body={**valid, **override})
    status, session = record('create', body=valid)
    if status != 201:
        return {'scope': 'native catch-up session API characterization', 'capture_complete': False, 'events': events}
    session_path = path+session['session_id']+'/'
    record('position-without-active-playback', route=session_path+'position/', body={'position_secs': 3})
    record('position-missing-field', route=session_path+'position/', body={})
    record('position-negative', route=session_path+'position/', body={'position_secs': -1})
    record('position-over-cap', route=session_path+'position/', body={'position_secs': 28801})
    record('position-invalid-paused', route=session_path+'position/', body={'position_secs': 1, 'paused': 'bad'})
    record('other-owner-hidden', route=session_path, role='other', method='DELETE')
    record('revoke', route=session_path, method='DELETE')
    record('revoked-session-hidden', route=session_path, method='DELETE')
    record('native-playback-missing-auth', route='/proxy/catchup/'+seed['channel_uuid'], role=None, method='GET')
    record('native-playback-head', route='/proxy/catchup/'+seed['channel_uuid'], role=None, method='HEAD')
    return {'format_version': 1, 'scope': 'native catch-up session API source/candidate wire characterization',
            'capture_complete': len(events) == 24, 'application_parity_verified': False,
            'performance_verified': False, 'events': events}
