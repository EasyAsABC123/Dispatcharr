"""Exact Original owned TS detail and targeted stop controls."""
import hashlib
import json
from pathlib import Path
import re
import urllib.error
import urllib.request

AGENT = 'owned-ts-controls-'
MISSING = 'fb67be07-7323-4179-85de-ad24f471689c'
CLIENT = re.compile(r'client_[0-9]{13}_[0-9]{4}')
STOP_STATE = """local p='live:channel:'..ARGV[1];local c=ARGV[2];local f=ARGV[3];
return {redis.call('SISMEMBER',p..':clients',c),redis.call('SISMEMBER',p..':clients',f),
redis.call('EXISTS',p..':client:'..c..':stop'),redis.call('EXISTS',p..':client:'..f..':stop')}"""


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def require(condition, label):
    if not condition:
        raise RuntimeError(label)


def generation(pid):
    require(type(pid) is int and pid > 0, 'invalid owned role PID')
    try:
        return int(Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()[19])
    except FileNotFoundError:
        return None


def http(base, role, method, path, token=None, data=None):
    require(bool(re.fullmatch(r'http://127\.0\.0\.1:[0-9]+', base)), 'target must be a fixture loopback endpoint')
    headers = {'Accept': 'application/json'}
    if token:
        headers['Authorization'] = 'Bearer '+token
    if data is not None:
        headers['Content-Type'] = 'application/json'
        data = json.dumps(data, separators=(',', ':')).encode()
    request = urllib.request.Request(base+path, method=method, data=data, headers=headers)
    try:
        response = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect()).open(request, timeout=8)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        raw = response.read(128*1024+1)
        mime = response.headers.get_content_type()
        require(response.geturl() == base+path and mime == 'application/json' and len(raw) <= 128*1024,
                'unexpected response URL, body length or media type')
        return {'role':role, 'method':method, 'path':path, 'status':response.code, 'content_type':mime,
                'body_size':len(raw), 'body_sha256':hashlib.sha256(raw).hexdigest()}, json.loads(raw)


def capture(base, accounts, channel, profile, readers, wait, snapshot, origin, redis,
            origin_before, shared, origin_shared, processes, evidence, request=http):
    owner, follower = readers
    admin, standard = (accounts[role]['access'] for role in ('admin', 'standard'))
    require(channel != MISSING, 'missing UUID must differ from the owned fixture channel')
    require(shared and shared['state']=='active' and shared['owner']
            and shared['provider_slots']==1 and shared['clients']==2, 'two readers must share one active provider slot')
    require(origin_shared and origin_shared['active']==1 and origin_shared['connections']==origin_before['connections']+1,
            'owned origin must have exactly one upstream for both viewers')
    require(processes and len(processes)==1 and processes[0]['service']=='baseline', 'one owner FFmpeg process is required')
    require(all(r.snapshot()['media_bytes'] >= 750000 and r.snapshot()['http_status']==200
                and r.snapshot()['content_type']=='video/mp2t' and not r.snapshot()['ended']
                and not r.snapshot()['errors'] and not r.snapshot()['sync_errors'] for r in readers),
            'both owned readers must have valid open TS sessions')
    evidence.update(channel=channel, provider_profile=profile, route_replica='baseline', observations={})
    detail = '/proxy/ts/status/'+channel
    stop = '/proxy/ts/stop_client/'+channel

    def call(name, role, method, path, code, token=None, data=None, expected=None):
        observation, body = request(base, role, method, path, token, data)
        evidence['observations'][name] = observation
        require(observation['status']==code and (expected is None or body==expected), 'unexpected '+name+' response')
        if expected is not None:
            observation['body'] = body
        return body

    def status(name, labels):
        payload = call(name, 'admin', 'GET', detail, 200, admin)
        require(payload['channel_id']==channel and payload['state']=='active' and payload['client_count']==len(labels),
                'wrong channel identity, state, or detailed client count')
        rows = {row['user_agent']: {key:row[key] for key in ('client_id','worker_id','user_agent','output_format')}
                for row in payload['clients']}
        require(len(payload['clients'])==len(rows) and set(rows)=={AGENT+label for label in labels},
                'detailed Original status must identify the exact owned readers')
        require(all(CLIENT.fullmatch(row['client_id']) and row['worker_id'] not in ('','unknown')
                    and row['output_format']=='mpegts' for row in rows.values()),
                'invalid client, worker or output identity in detailed Original status')
        evidence['observations'][name]['body_projection'] = {
            'channel_id':channel, 'state':payload['state'], 'client_count':len(rows), 'clients':rows,
        }
        return rows

    call('detail_anonymous', 'anonymous', 'GET', detail, 401,
         expected={'detail':'Authentication credentials were not provided.'})
    forbidden = {'detail':'You do not have permission to perform this action.'}
    call('detail_standard', 'standard', 'GET', detail, 403, standard, expected=forbidden)
    call('detail_missing', 'admin', 'GET', '/proxy/ts/status/'+MISSING, 404, admin,
         expected={'error':f'Channel {MISSING} not found'})
    viewers = status('detail_active', ('owner', 'follower'))
    left, right = (viewers[AGENT+label] for label in ('owner', 'follower'))
    require(left['client_id']!=right['client_id'] and left['worker_id']!=right['worker_id'],
            'viewers must be distinct on different source worker identities')
    owner_id, target_id = left['client_id'], right['client_id']
    data = {'client_id':target_id}
    evidence['stop_request'] = data
    call('stop_standard', 'standard', 'POST', stop, 403, standard, data, expected=forbidden)
    require(redis(STOP_STATE, channel, owner_id, target_id)==[1,1,0,0]
            and not owner.snapshot()['ended'] and not follower.snapshot()['ended'],
            'denied stop changed the client set, markers or live response')
    evidence['denied_stop_leave_both_live'] = True
    call('stop_target', 'admin', 'POST', stop, 200, admin, data, expected={
        'message':'Client stop request processed', 'channel_id':channel,
        'client_id':target_id, 'locally_processed':False,
    })

    def follower_eof():
        row = follower.snapshot()
        require(not row['errors'] and not row['sync_errors'], 'target transport errored instead of closing cleanly')
        return row if row['ended'] else None

    target_eof = wait(follower_eof, 15, 'targeted viewer EOF')
    require(not target_eof['partial_bytes'], 'targeted EOF cut a transport packet')
    evidence['target_eof'] = target_eof
    peer_at_eof = owner.snapshot()['media_bytes']

    def peer_advanced():
        row = owner.snapshot()
        require(not row['ended'] and not row['errors'] and not row['sync_errors'], 'targeted stop affected the surviving viewer')
        return row if row['media_bytes'] >= peer_at_eof+300000 else None

    peer = wait(peer_advanced, 12, 'peer media advance following actual targeted EOF')
    evidence['peer_media_bytes_after_eof'] = peer['media_bytes']-peer_at_eof
    survivor = wait(lambda: snapshot(channel, profile)['clients']==1, 15, 'targeted client accounting retirement')
    require(survivor and status('detail_survivor', ('owner',))=={AGENT+'owner':left}, 'exact surviving viewer identity changed')
    counts, upstream = snapshot(channel, profile), origin()
    membership = redis(STOP_STATE, channel, owner_id, target_id)
    require(counts['owner']==shared['owner'] and counts['state']=='active' and counts['provider_slots']==1
            and counts['clients']==1 and membership[:3]==[1,0,0],
            'targeted stop changed owner, surviving Redis identity or provider accounting')
    require(upstream['active']==1 and upstream['connections']==origin_shared['connections'],
            'targeted stop restarted or duplicated upstream')
    require(not owner.snapshot()['ended'] and not owner.snapshot()['errors'], 'surviving response closed before final status')
    evidence.update(provider_slots=counts['provider_slots'], upstream_connections=upstream['connections'],
                    upstream_active=upstream['active'], client_memberships=membership[:2],
                    survivor_stop_marker_present=bool(membership[2]), passed=True)
    return evidence
