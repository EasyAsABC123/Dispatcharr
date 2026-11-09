"""Original Timeshift admin wire characterization; all upstream credit retained."""
import json
import time
from urllib.request import Request,build_opener
from urllib.error import HTTPError
from direct_probe import NoRedirect


def capture(base,seed,control,label,active=False):
    events=[];opener=build_opener(NoRedirect())
    tokens={role:user['token'] for role,user in seed['users'].items()}
    prefix='/proxy/catchup/'
    def record(case,route='stats/',*,role='owner',method='GET',body=None,raw=None,content_type='application/json'):
        headers={'Content-Type':content_type}
        if role in tokens:headers['Authorization']='Bearer '+tokens[role]
        data=raw if raw is not None else json.dumps(body).encode() if body is not None else None
        before=time.time()
        try:response=opener.open(Request(base+prefix+route,data=data,headers=headers,method=method),timeout=15)
        except HTTPError as error:response=error
        with response:
            payload=response.read(65537)
            if len(payload)>65536:raise RuntimeError('management probe exceeded body budget')
            try:value=json.loads(payload) if payload else None
            except ValueError:value={'non_json':True,'bytes':len(payload)}
            event={'case':case,'method':method,'status':response.status,'body':value,'headers':{key.lower():value for key,value in response.headers.items() if key.lower() in ('allow','www-authenticate','content-type','x-content-type-options')}}
            if isinstance(value,dict) and 'timestamp' in value:
                stamp=value['timestamp']
                event['timestamp_within_request']=isinstance(stamp,(int,float)) and before<=stamp<=time.time()
                value['timestamp']='<request-clock>'
            events.append(event)
        return value
    control('begin',label)
    try:
        for route in ['stats/','programs/','stop_client/']:
            record(route+'missing-auth',route,role=None,method='GET' if route=='stats/' else 'POST',body={} if route!='stats/' else None)
            record(route+'standard-user',route,role='other',method='GET' if route=='stats/' else 'POST',body={} if route!='stats/' else None)
        record('stats-empty')
        record('stats-post-method',method='POST',body={})
        record('stats-head',method='HEAD')
        record('stats-options',method='OPTIONS')
        for name,body in [('missing',{}),('null',{'sessions':None}),('object',{'sessions':{}}),('empty',{'sessions':[]}),('invalid-elements',{'sessions':[{}]})]:
            record('programs-'+name,'programs/',method='POST',body=body)
        record('programs-invalid-json','programs/',method='POST',raw=b'{')
        record('programs-get-method','programs/')
        record('stop-invalid-json','stop_client/',method='POST',raw=b'{')
        for name,body in [('missing',{}),('empty',{'session_id':''}),('unknown',{'session_id':'unknown-fixture'})]:record('stop-'+name,'stop_client/',method='POST',body=body)
        control('inactive',label);record('inactive-admin');control('active',label)
        control('deny-ui',label);record('ui-network-deny');control('allow-ui',label)
        control('seed',label)
        record('stats-populated')
        sessions=[{'session_id':'management-fixture','channel_uuid':seed['channel_uuid'],'programme_start':'2026-11-09T11:00:00Z'}]
        record('programs-paused-redis-playhead','programs/',method='POST',body={'sessions':sessions})
        record('programs-next-guide','programs/',method='POST',body={'sessions':[{**sessions[0],'position_secs':2730}]})
        record('programs-invalid-guide','programs/',method='POST',body={'sessions':[{**sessions[0],'programme_start':'bad'}]})
        record('programs-first50','programs/',method='POST',body={'sessions':[{**sessions[0],'session_id':f'fixture-{i}'} for i in range(51)]})
        record('stop-existing','stop_client/',method='POST',body={'session_id':'management-fixture'})
        side_effects=control('inspect',label)['cleanup']
        record('stats-after-stop')
        record('stop-again','stop_client/',method='POST',body={'session_id':'management-fixture'})
        playback=active_controls(base,seed,control,label,opener) if active else None
        return {'format_version':1,'scope':'Original Timeshift stats/programmes/admin-stop HTTP and synthetic Redis cleanup characterization','capture_complete':len(events)==31 and (not active or playback_complete(playback)),'events':events,'synthetic_stop_effects':side_effects,'active_and_idle_playback':playback,'application_parity_verified':False,'performance_verified':False}
    finally:control('restore',label)


def compare(source,candidate):
    differences=[]
    if not source.get('capture_complete') or not candidate.get('capture_complete'):differences.append({'case':'capture','detail':'incomplete'})
    for original,native in zip(source['events'],candidate['events']):
        for field in ['case','method','status','body']:
            if original.get(field)!=native.get(field):differences.append({'case':original['case'],'field':field,'source':original.get(field),'candidate':native.get(field)})
        def headers(event):
            result=dict(event['headers'])
            if 'allow' in result:result['allow']=sorted(item.strip() for item in result['allow'].split(','))
            return result
        if headers(original)!=headers(native):differences.append({'case':original['case'],'field':'headers','source':headers(original),'candidate':headers(native)})
        if 'timestamp_within_request' in original and not native.get('timestamp_within_request'):differences.append({'case':original['case'],'field':'timestamp','detail':'outside request bounds'})
    if source['synthetic_stop_effects']!=candidate['synthetic_stop_effects']:differences.append({'case':'stop-effects','source':source['synthetic_stop_effects'],'candidate':candidate['synthetic_stop_effects']})
    if source.get('active_and_idle_playback')!=candidate.get('active_and_idle_playback'):differences.append({'case':'real-playback-controls','source':source.get('active_and_idle_playback'),'candidate':candidate.get('active_and_idle_playback')})
    return {'captured_management_matches':not differences,'differences':differences,'normalizations':['Allow order as unordered original DRF method set','Only top-level timestamp replaced after verifying it lies within each actual request interval'],'application_parity_verified':False,'performance_verified':False}


def active_controls(base,seed,control,label,opener):
    import http.client
    import socket
    results=[]
    for mode in ['active','idle']:
        headers={'Authorization':'Bearer '+seed['users']['owner']['token'],'Content-Type':'application/json'}
        with opener.open(Request(base+'/api/catchup/sessions/',data=json.dumps({'channel_uuid':seed['channel_uuid'],'start':'2026-11-09T11:00:00Z','duration':30}).encode(),headers=headers),timeout=15) as response:
            if response.status!=201:raise RuntimeError('management active mint failed')
            session=json.load(response)
        response=opener.open(Request(base+session['playback_url'],headers={'Range':'bytes=0-'+('4999999' if mode=='active' else '255')}),timeout=15)
        initial=response.read(65536 if mode=='active' else 256)
        if response.status!=206 or len(initial)!=(65536 if mode=='active' else 256):raise RuntimeError('management active provider setup failed')
        if mode=='active':time.sleep(2.5)
        before=control('observe-active',label,session['session_id'])['state']
        if mode=='idle':
            deadline=time.monotonic()+5
            while before['profile_connections']!=0 and time.monotonic()<deadline:
                time.sleep(.1);before=control('observe-active',label,session['session_id'])['state']
        with opener.open(Request(base+'/proxy/catchup/stop_client/',data=json.dumps({'session_id':session['session_id']}).encode(),headers=headers),timeout=15) as stopped:
            body=json.load(stopped)
            if stopped.status!=200 or body.get('session_id')!=session['session_id']:raise RuntimeError('management stop request failed')
        began=time.monotonic();remaining=0;closed=mode=='idle'
        try:
            if mode=='active':
                response.fp.raw._sock.settimeout(3)
                while remaining<5_000_000:
                    chunk=response.read(65536)
                    if not chunk:closed=True;break
                    remaining+=len(chunk)
        except http.client.IncompleteRead as error:remaining+=len(error.partial);closed=True
        except (socket.timeout,TimeoutError):pass
        finally:response.close()
        first_state=control('observe-active',label,session['session_id'])['state']
        deadline=time.monotonic()+9
        while True:
            state=control('observe-active',label,session['session_id'])['state']
            if state['profile_connections']==0 and state['stats_client_count']==0 and not state['api_exists']:break
            if time.monotonic()>deadline:break
            time.sleep(.1)
        results.append({'mode':mode,'initial_media_correct':True,'before_provider_owned':before['profile_connections']==(1 if mode=='active' else 0),'before_api_exists':before['api_exists'],'before_stats_client_count':before['stats_client_count'],'closed_after_stop':closed,'stopped_before_complete_media':remaining+len(initial)<5_000_000 if mode=='active' else True,'immediate_after_stop':first_state,'after_grace':state})
    return results


def playback_complete(playback):
    if not isinstance(playback,list) or len(playback)!=2:return False
    for result in playback:
        if not all(result.get(key) is True for key in ['initial_media_correct','before_provider_owned','before_api_exists','closed_after_stop','stopped_before_complete_media']):return False
        if result.get('before_stats_client_count')!=1:return False
        state=result.get('after_grace',{})
        if state.get('api_exists') is not False or state.get('stats_client_count')!=0 or state.get('profile_connections')!=0:return False
    return True
