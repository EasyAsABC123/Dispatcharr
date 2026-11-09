"""Host-side native gateway assertions against actual original Django controls.

Original authors retain application credit. Control responses containing JWTs
remain in memory; returned evidence contains statuses and public fixture data.
"""
import time
import hashlib
import json
import socket
from urllib.parse import urlencode
from ws_client import Client

INITIAL={'type':'connection_established','data':{'success':True,'message':'WebSocket connection established successfully'}}
ADMIN_TYPES=['channel_stats','vod_stats','timeshift_stats','vod_started','vod_stopped']


def exercise(host,port,control,browser=None,second_port=None,diagnostics=None,source_live_stats=False,slow_value_bytes=1024*1024,live_stats_target=None,live_stats_peer=None):
    accounts=control({'op':'seed'})['accounts'];clients=[];cases=[]
    def connect(query,**kwargs):
        client=Client(host,port,'/ws/?'+urlencode(query),**kwargs);clients.append(client);return client
    def user(role):
        client=connect([('token',accounts[role]['access'])]);assert client.status==101
        assert client.receive()=={'message':INITIAL};return client
    def emit(message):assert control({'op':'emit','message':message})=={'sent':True}
    try:
        missing=control({'op':'token'})['access']
        auth=[('absent',[],None,403),('invalid',[('token','invalid')],None,403),
              ('refresh',[('token',accounts['admin']['refresh'])],None,403),
              ('inactive',[('token',accounts['inactive']['access'])],None,403),
              ('missing-user',[('token',missing)],None,403),
              ('apikey-query',[('token',accounts['admin']['api_key'])],None,403),
              ('header-only',[],{'Authorization':'Bearer '+accounts['admin']['access']},403),
              ('wrong-case',[('Token',accounts['admin']['access'])],None,403),
              ('invalid-first',[('token','invalid'),('token',accounts['admin']['access'])],None,403),
              ('blank-first',[('token',''),('token',accounts['admin']['access'])],None,101),
              ('valid-first',[('token',accounts['admin']['access']),('token','invalid')],None,101),
              ('query-with-invalid-header',[('token',accounts['admin']['access'])],{'Authorization':'invalid'},101)]
        for name,query,headers,expected in auth:
            client=connect(query,headers=headers);assert client.status==expected
            if expected==101:assert client.receive()=={'message':INITIAL}
            closed=client.close();cases.append({'case':'auth-'+name,'status':expected,'close':closed})
        for role in ['admin','standard','viewer']:
            client=user(role);cases.append({'case':'role-'+role,'close':client.close()})
        for origin in [None,'null','https://cross-origin.invalid']:
            client=connect([('token',accounts['standard']['access'])],origin=origin)
            assert client.status==101 and client.receive()=={'message':INITIAL}
            cases.append({'case':'origin','origin':origin,'close':client.close()})
        for value in [None,{},False,0,'',['invalid'],{'public_ip':'192.0.2.10','country_code':'ZZ','verified_at':1234.5},{'type':'fixture_type_override','verified_at':12}]:
            assert control({'op':'cache','value':value})=={'value':value}
            client=user('admin');extra=client.optional();expected=None
            if isinstance(value,dict) and value:expected={'message':{'data':{'type':'ip_lookup_complete',**{k:v for k,v in value.items() if k!='verified_at'}}}}
            assert extra==expected
            assert control({'op':'cache','value':value})=={'value':value}
            cases.append({'case':'cached-ip','input':value,'extra':extra,'close':client.close()})
        control({'op':'cache','value':None})
        admin=user('admin');standard=user('standard');viewer=user('viewer')
        for number,kind in enumerate(ADMIN_TYPES+['future_event',None]):
            event={'type':'update','data':{'type':kind,'fixture_probe':number}};emit(event)
            got=[client.optional(.3) for client in [admin,standard,viewer]]
            expected=[{'message':event}]*3
            if kind in ADMIN_TYPES:expected[1:]=[None,None]
            assert got==expected;cases.append({'case':'event-filter','kind':kind,'received':got})
        for flag in [True,1,'false',[False],False,0,'',[],{}]:
            event={'type':'update','data':{'type':'system_notification','notification':{'admin_only':flag}}};emit(event)
            got=[client.optional(.25) for client in [admin,standard,viewer]];expected=[{'message':event}]*3
            if flag:expected[1:]=[None,None]
            assert got==expected;cases.append({'case':'notification-filter','flag':flag,'received':got})
        for value in [None,False,0,'',[],{}]:
            event={'type':'update','data':value};emit(event)
            assert [client.receive() for client in [admin,standard,viewer]]==[{'message':event}]*3
            cases.append({'case':'falsy-update','data':value,'passed':True})
        control({'op':'user','role':'admin','fields':{'user_level':1,'is_active':False}})
        control({'op':'user','role':'standard','fields':{'user_level':10}})
        event={'type':'update','data':{'type':'channel_stats','fixture_probe':'captured-role'}};emit(event)
        assert admin.receive()=={'message':event} and standard.optional(.2) is None
        denied=connect([('token',accounts['admin']['access'])]);assert denied.status==403;denied.close()
        cases.append({'case':'captured-role-and-active','passed':True})
        for client in [admin,standard,viewer]:client.close()
        control({'op':'user','role':'admin','fields':{'user_level':10,'is_active':True}})
        control({'op':'user','role':'standard','fields':{'user_level':1}})
        short=control({'op':'token','user_id':accounts['admin']['id'],'seconds':2})['access']
        old=connect([('token',short)]);assert old.status==101 and old.receive()=={'message':INITIAL}
        time.sleep(2.2)
        event={'type':'update','data':{'type':'channel_stats','fixture_probe':'expired-token'}};emit(event)
        assert old.receive()=={'message':event}
        new=connect([('token',short)]);assert new.status==403;new.close();old.close()
        cases.append({'case':'token-expiry-after-handshake','old-stays-connected':True,'new-status':403})
        for name,opcode,payload in [('invalid-json',1,b'{'),('missing-type',1,b'{}'),('array',1,b'[]'),('binary',2,b'fixture')]:
            client=user('admin');client.send(opcode,payload);result=client.receive();assert result=={'closed':1011}
            cases.append({'case':'inbound-'+name,**result});client.close()
        client=user('standard');client.json({'type':'m3u_profile_test','url':'fixture','search':'f','replace':'x'});assert client.optional(.3) is None
        cases.append({'case':'standard-preview-ignored','close':client.close()})
        client=user('admin');client.send(9,b'fixture-ping');assert client.receive()=={'pong':b'fixture-ping'.hex()}
        cases.append({'case':'ping-pong','close':client.close()})
        for client in clients:client.close()
        large_client=connect([('token',accounts['admin']['access'])],max_message_bytes=8*1024*1024)
        assert large_client.status==101 and large_client.receive()=={'message':INITIAL}
        large={'type':'update','data':{'type':'fixture_large','value':'\x01'*(1024*1024)}}
        emit(large);large_client.send(9,b'during-large-message')
        received=[large_client.receive(5),large_client.receive(5)]
        assert {'message':large} in received
        assert {'pong':b'during-large-message'.hex()} in received
        cases.append({'case':'large-fragmented-json-with-ping','json_bytes':len(json.dumps(large,separators=(',',':')).encode()),
                      'value_sha256':hashlib.sha256(large['data']['value'].encode()).hexdigest(),'one_message':True,'pong_received':True})
        large_client.close()
        if browser:
            initial=browser.call({'op':'connect','url':'ws://'+host+':'+str(port)+'/ws/?'+urlencode([('token',accounts['admin']['access'])])})
            assert initial=={'initial':INITIAL}
            emit(large);observed=browser.call({'op':'observe'})
            assert observed['large']['messages']==1 and observed['large']['valueCharacters']==1024*1024
            assert observed['large']['firstCode']==1 and observed['large']['lastCode']==1
            closed=browser.call({'op':'disconnect'});assert closed=={'closed':{'code':1000,'clean':True}}
            cases.append({'case':'actual-Chromium-single-onmessage-for-6MiB-fragmented-JSON',**observed,**closed})
        if second_port is not None:
            peers=[]
            for index in range(100):
                endpoint=port if index%2==0 else second_port
                client=Client(host,endpoint,'/ws/?'+urlencode([('token',accounts['admin']['access'])]));clients.append(client)
                assert client.status==101 and client.receive()=={'message':INITIAL};peers.append(client)
            assert control({'op':'groups'})['members']==100
            event={'type':'update','data':{'type':'fixture_fanout','value':'東京é'*1024}};emit(event)
            for client in peers:assert client.receive()=={'message':event}
            if diagnostics:
                snapshots=diagnostics();assert [row['local_sockets'] for row in snapshots]==[50,50]
                assert all(row['socket_output_reserved_bytes']==50*65536 for row in snapshots)
            else:snapshots=None
            cases.append({'case':'100-sockets-two-native-gateways-Source-broadcast','messages':100,'diagnostics':snapshots})
            for client in peers[:50]:client.close()
            emit(event)
            for client in peers[50:]:assert client.receive()=={'message':event}
            for client in peers[50:]:client.close()
            cases.append({'case':'closing-half-preserves-other50','passed':True})
        slow=user('admin');slow.socket.setsockopt(socket.SOL_SOCKET,socket.SO_RCVBUF,4096)
        fast=connect([('token',accounts['admin']['access'])],max_message_bytes=24*1024*1024)
        assert fast.status==101 and fast.receive()=={'message':INITIAL}
        slow_event={'type':'update','data':{'type':'fixture_large','value':'\x01'*slow_value_bytes}}
        if diagnostics is None:assert control({'op':'groups'})['members']==2, 'expected exactly slow+fast members before backpressure'
        started=time.monotonic();emit(slow_event)
        assert fast.receive(5)=={'message':slow_event}
        fast_seconds=time.monotonic()-started
        if diagnostics:
            deadline=time.monotonic()+4
            while True:
                snapshot=diagnostics()[0]
                if snapshot['local_sockets']==1:break
                if time.monotonic()>deadline:raise AssertionError('slow socket exceeded send/cleanup deadline')
                time.sleep(.025)
            assert snapshot['socket_output_reserved_bytes']==65536
            cases.append({'case':'held-slow-reader-fast-reader-isolated','fast_delivery_seconds':fast_seconds,
                          'slow_socket_removed_seconds':time.monotonic()-started,'remaining_socket_output_bytes':snapshot['socket_output_reserved_bytes']})
        else:
            deadline=time.monotonic()+4
            while control({'op':'groups'})['members']!=1:
                if time.monotonic()>deadline:raise AssertionError('packaged slow socket membership cleanup deadline')
                time.sleep(.025)
            cases.append({'case':'held-slow-reader-fast-reader-isolated','fast_delivery_seconds':fast_seconds,
                          'slow_member_removed_seconds':time.monotonic()-started,'remaining_members':1,'event_value_bytes':slow_value_bytes,'json_bytes':len(json.dumps(slow_event,separators=(',',':')).encode()),
                          'internal_byte_metrics':'unexposed in app; identical gateway component budgets verified separately'})
        fast.close();slow.socket.close();slow.closed=True
        if source_live_stats:
            observer=user('admin');observed=[]
            def stats_until(predicate,seconds=8):
                deadline=time.monotonic()+seconds
                while time.monotonic()<deadline:
                    incoming=observer.optional(min(.5,deadline-time.monotonic()))
                    if incoming is None:continue
                    message=incoming['message'];data=message.get('data') or {}
                    if data.get('type')!='channel_stats':continue
                    assert message['type']=='update' and data['success'] is True and isinstance(data['stats'],str)
                    snapshot=json.loads(data['stats']);observed.append(snapshot)
                    if predicate(snapshot):return snapshot
                raise AssertionError('Source live stats event deadline')
            empty=control({'op':'status_get',**({'base_url':live_stats_target} if live_stats_target else {})});assert empty['status']==200
            stats_until(lambda value:value==empty['body'])
            opened=control({'op':'play_start',**({'base_url':live_stats_target} if live_stats_target else {})})
            channel=opened['channel_id']
            connected=stats_until(lambda value:any(row['channel_id']==channel and row['client_count']==1 for row in value['channels']))
            initial_channel=next(row for row in connected['channels'] if row['channel_id']==channel)
            assert initial_channel['state']=='connecting' and initial_channel['buffer_index']==0
            assert initial_channel['stream_profile']==str(opened['profile_id']) and 'total_bytes' not in initial_channel
            deadline=time.monotonic()+15
            while control({'op':'play_state'}).get('bytes',0)<65536:
                if time.monotonic()>deadline:raise AssertionError('Source media first bytes deadline')
                time.sleep(.1)
            active=control({'op':'status_get',**({'base_url':live_stats_target} if live_stats_target else {})});assert active['status']==200
            stats_until(lambda value:value==active['body'])
            peer_snapshot=None
            if live_stats_peer:
                peer_snapshot=control({'op':'status_get','base_url':live_stats_peer})
                assert peer_snapshot['status']==200
                remote=next(row for row in peer_snapshot['body']['channels'] if row['channel_id']==channel)
                assert remote['client_count']==1 and 'healthy' not in remote
                stats_until(lambda value:value==peer_snapshot['body'])
            stopped=control({'op':'play_stop'});assert stopped['stopped'] and stopped['bytes']>=65536 and stopped['media']['decoder_exit']==0 and stopped['media']['video_frames']>=25 and stopped['media']['audio_frames']>0
            removed=stats_until(lambda value:all(row.get('client_count')==0 for row in value['channels']))
            cases.append({'case':('native-live-client-add-remove-and-status-GET-producers' if live_stats_target else 'actual-Source-live-client-add-remove-and-status-GET-producers'),
                          'source_backend_only':live_stats_target is None,'native_backend':live_stats_target is not None,'native_gateway_delivery':True,'initial_get':empty['body'],
                          'connected':connected,'active_get':active['body'],'foreign_replica_get':peer_snapshot,'removed':removed,
                          'observed_snapshots':observed,'media_bytes':stopped['bytes'],'media':stopped['media']})
            observer.close()
        deadline=time.monotonic()+3
        while True:
            group=control({'op':'groups'})
            if group['members']==0:break
            if time.monotonic()>deadline:raise AssertionError('native exceptional socket members not cleaned')
            time.sleep(.02)
        return {'passed':True,'cases':cases,'case_count':len(cases),'group_members_after_close':group['members'],
                'scope':'native wire assertions using actual original Django auth/cache/events; no admin regex preview or browser proof'}
    except Exception as error:
        error.partial_wire_cases=cases
        raise
    finally:
        for client in clients:client.close()
