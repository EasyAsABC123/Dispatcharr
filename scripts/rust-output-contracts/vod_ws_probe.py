"""Actual original/native HTTP + packaged gateway VOD publication contracts.
Original Dispatcharr authors/contributors retain all application credit.
Tokens and credential-bearing URLs remain in memory; evidence is sanitized.
"""
import sys,time,json,socket,math,copy
from pathlib import Path
from urllib.request import Request,build_opener,HTTPRedirectHandler
from urllib.error import HTTPError
from urllib.parse import urlsplit,quote
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'rust-realtime-contracts'))
from ws_client import Client
INITIAL={'type':'connection_established','data':{'success':True,'message':'WebSocket connection established successfully'}}
KINDS={'vod_started','vod_stopped','vod_stats'}
class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self,*args):return None

def capture(base,ws_base,token,control,error_path=None):
    address=urlsplit(ws_base);peer=None;events=[];cases=[];responses=[]
    def received_until(kinds,seconds=5):
        found=[];deadline=time.monotonic()+seconds
        while time.monotonic()<deadline:
            value=peer.receive(max(.05,deadline-time.monotonic()))
            if 'closed' in value:raise RuntimeError('Media observer unexpectedly closed')
            message=value.get('message',{});kind=(message.get('data') or {}).get('type')
            if kind in KINDS:
                found.append(message);events.append(message)
                if set(kinds)<={item['data']['type'] for item in found}:return found
        raise RuntimeError('Media event deadline exceeded')
    def open_response(sid,failed=False):
        path='/proxy/vod/movie/'+('00000000-0000-0000-0000-000000000000' if failed else 'a4d78cd1-bf9f-4a63-a746-f40566825cf9')+'/'+sid
        try:value=build_opener(NoRedirect()).open(Request(base+path,headers={'User-Agent':'packaged-media-events'}),timeout=15)
        except HTTPError as error:value=error
        responses.append(value);return value
    def closed(sid):
        deadline=time.monotonic()+5
        while time.monotonic()<deadline:
            if not control('inspect',sid)['exists']:return
            time.sleep(.1)
        raise RuntimeError('Packaged VOD metadata did not clean up')
    try:
        peer=Client(address.hostname,address.port or 80,'/ws/?token='+quote(token,safe=''))
        if peer.status!=101:raise RuntimeError('Packaged media observer upgrade denied')
        if peer.receive()!= {'message':INITIAL}:raise RuntimeError('Unexpected original connection acknowledgement')
        sid='fixture-packaged-vod-disconnect';before=len(events)
        response=open_response(sid)
        if response.status!=200:raise RuntimeError('Packaged VOD response was not200')
        data=response.read(8192)
        if not data:raise RuntimeError('Packaged VOD delivered no payload')
        received_until({'vod_started','vod_stats'});response.close();received_until({'vod_stopped','vod_stats'});closed(sid)
        cases.append({'case':'disconnect','status':200,'payload_received':True,'event_types':[item['data']['type'] for item in events[before:]]})
        sid='fixture-packaged-vod-reconnect';before=len(events)
        first=open_response(sid);first.read(8192);received_until({'vod_started','vod_stats'});first.close()
        second=open_response(sid)
        if second.status!=200 or not second.read(8192):raise RuntimeError('Packaged VOD reconnect failed')
        # A reconnect inside the grace window suppresses the old stop/start.
        deadline=time.monotonic()+1.2
        while time.monotonic()<deadline:
            value=peer.optional(min(.15,max(.01,deadline-time.monotonic())))
            if value and (value.get('message',{}).get('data') or {}).get('type') in KINDS:raise RuntimeError('Active reconnect unexpectedly announced another start/stop')
        second.close();received_until({'vod_stopped','vod_stats'});closed(sid)
        cases.append({'case':'reconnect','status':200,'payload_received':True,'event_types':[item['data']['type'] for item in events[before:]]})
        sid='fixture-packaged-vod-missing';before=len(events);response=open_response(sid,failed=True);status=response.status;response.close()
        deadline=time.monotonic()+.3
        while time.monotonic()<deadline:
            value=peer.optional(max(.01,deadline-time.monotonic()))
            if value and (value.get('message',{}).get('data') or {}).get('type') in KINDS:raise RuntimeError('Missing content unexpectedly announced playback')
        cases.append({'case':'missing-content-no-event','status':status,'event_types':[]})
        for case in cases[:2]:
            if case['event_types']!=['vod_started','vod_stats','vod_stopped','vod_stats']:raise RuntimeError('Unexpected packaged event transition sequence')
        for event in events:
            if event.get('type')!='update' or 'success' in event['data']:raise RuntimeError('VOD envelope changed')
            if event['data']['type']=='vod_stats':
                stats=json.loads(event['data']['stats']);assert isinstance(stats['vod_connections'],list) and isinstance(stats['total_connections'],int) and isinstance(stats['timestamp'],(int,float))
        return {'format_version':1,'scope':'Actual packaged/source HTTP playback and WebSocket VOD transition/fullstats publication','capture_complete':len(cases)==3,'fixture_bytes_per_second':262144,'active_reconnect_hold_seconds':1.2,'cases':cases,'events':events,'secrets_not_persisted':True,'application_parity_verified':False,'performance_verified':False}
    except Exception as error:
        if error_path is not None:Path(error_path).write_text(json.dumps({'capture_complete':False,'cases':cases,'events':events,'error_type':type(error).__name__,'secrets_not_persisted':True},indent=2)+'\n')
        raise
    finally:
        for response in responses:response.close()
        if peer is not None:peer.close()

def compare(source,candidate):
    differences=[]
    if not source.get('capture_complete') or not candidate.get('capture_complete'):differences.append('Incomplete capture')
    if source.get('cases')!=candidate.get('cases'):differences.append('HTTP/transition cases differ')
    if len(source.get('events',[]))!=len(candidate.get('events',[])):differences.append('Event count differs')
    phase=None;prior=[None,None]
    def stable(value):
        value=copy.deepcopy(value);value.pop('timestamp',None)
        for group in value['vod_connections']:
            for conn in group['connections']:
                for key in ['connected_at','last_activity','duration','bytes_sent']:conn.pop(key,None)
        return value
    for left,right in zip(source['events'],candidate['events']):
        if left['data']['type']!=right['data']['type']:differences.append('Event order/type differs');continue
        kind=left['data']['type']
        if kind!='vod_stats':
            phase=kind
            if left!=right:differences.append('Playback transition fields differ')
            continue
        snapshots=[json.loads(left['data']['stats']),json.loads(right['data']['stats'])]
        # Same first-poll component tests separately assert exact byte samples.
        # Actual HTTP transport consumption races the background snapshot, so
        # validate consumed-byte units and every stable full DTO field here.
        for index,value in enumerate(snapshots):
            if set(value)!={'vod_connections','total_connections','timestamp'} or not math.isfinite(value['timestamp']):differences.append('Invalid full statistics envelope')
            connections=[conn for group in value['vod_connections'] for conn in group['connections']]
            if len(connections)!=value['total_connections']:differences.append('Invalid connection count')
            for conn in connections:
                if conn['bytes_sent']%819200:differences.append('Byte progress notoriginal consumed chunk milestone')
                for key in ['connected_at','last_activity']:
                    try:valid=math.isfinite(float(conn[key]))
                    except (ValueError,TypeError):valid=False
                    if not valid:differences.append('Invalid timestamp')
            if phase=='vod_started':
                if value['total_connections']!=1:differences.append('Started snapshot omitted active full connection')
                prior[index]=stable(value)
            elif phase=='vod_stopped':
                if value['total_connections'] not in (0,1):differences.append('Invalid stopping snapshot')
                if value['total_connections'] and stable(value)!=prior[index]:differences.append('Stopping snapshot changed stable connection fields')
        if phase=='vod_started' or all(value['vod_connections'] for value in snapshots):
            if stable(snapshots[0])!=stable(snapshots[1]):differences.append('Stable full statistics fields differ')
    return {'captured_packaged_vod_events_match':not differences,'differences':differences,'normalizations':['Own timestamps/elapsed/consumed progress validated; stable complete DTOs compared','Original stopping snapshot races metadata removal; empty/nonempty final snapshots permitted'],'application_parity_verified':False,'performance_verified':False}
