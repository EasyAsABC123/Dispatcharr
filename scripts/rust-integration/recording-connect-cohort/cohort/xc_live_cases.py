"""Additive callback in the existing Source/Native recording milestone cohort.
Genuinely NEW Source XC provider/task characterization, not replayed authority.
No apps on import and no nested fixture or scheduler.
"""
import hashlib,json,uuid,traceback
from pathlib import Path
CASE='xc-live';PORT=18096

def save(path,value):Path(path).write_text(json.dumps(value,indent=2,allow_nan=False)+'\n')
def control(fixture,path,value=None):
    script="import json,urllib.request;data="+repr(None if value is None else json.dumps(value).encode())+";req=urllib.request.Request('http://127.0.0.1:"+str(PORT)+"'+"+repr(path)+",data=data,headers={'Content-Type':'application/json'});r=urllib.request.build_opener(urllib.request.ProxyHandler({})).open(req,timeout=5);raw=r.read(1048577);assert len(raw)<=1048576;print(raw.decode())"
    return json.loads(fixture.python(script,seconds=10,secret=True))
def prepare_context(fixture):
    if fixture.case!=CASE:return
    user=uuid.uuid4().hex;password=uuid.uuid4().hex;guest='/shared/'+CASE
    media_target=fixture.sql("from apps.channels.models import ChannelStream;print(json.dumps(ChannelStream.objects.select_related('stream').filter(channel_id="+str(fixture.channel_id)+").order_by('order').first().stream.url))",secret=True)
    fixture.role('xc-live-origin',['/dispatcharrpy/bin/python','/normal-dvr-tools/xc_live_origin.py'],extra={'XC_LIVE_DATA':guest,'XC_LIVE_USERNAME':user,'XC_LIVE_PASSWORD':password,'XC_LIVE_MEDIA_TARGET':media_target})
    fixture.wait(lambda:(fixture.case_data/'xc-live-origin.ready').exists(),15,'actual-owned-XC-Origin-ready')
    # Actual settled setup, including Source's normal default-profile signal.
    # Periodic schedules are disabled explicitly in this dedicated context.
    code="from apps.m3u.models import M3UAccount;from django_celery_beat.models import PeriodicTask,PeriodicTasks;from core.models import CoreSettings;a=M3UAccount.objects.create(name='Owned normal XC',account_type='XC',server_url='http://127.0.0.1:"+str(PORT)+"/xc/player_api.php?discard=1#fragment',username="+repr(user)+",password="+repr(password)+",stale_stream_days=0,custom_properties={'hash_key':'url','auto_enable_new_groups_live':True});PeriodicTask.objects.all().update(enabled=False);PeriodicTasks.update_changed();r,_=CoreSettings.objects.get_or_create(key='backup_settings',defaults={'name':'Backup Settings','value':{}});CoreSettings.objects.filter(pk=r.pk).update(value={**(r.value or {}),'schedule_enabled':False});print(json.dumps({'account_id':a.pk,'secret_input_sha256':"+repr(hashlib.sha256((user+'\0'+password).encode()).hexdigest())+"}))"
    fixture._xc_live=fixture.sql(code,secret=True)

def run(fixture):
    if fixture.case!=CASE:raise ValueError('OwnedXCContextRequired')
    output=Path(fixture.output)/CASE;output.mkdir(exist_ok=False);private=output/'private';private.mkdir(mode=0o700);cells=[];ident=fixture._xc_live['account_id']
    # Use the existing monitor/Channels helper, calibrated against actual app
    # backend key construction. Metadata/results remain private where provider
    # exceptions or profile CP can include the real owned credentials.
    module=fixture.m6_module();fixture.role('xc-live-broker-observer',['/dispatcharrpy/bin/python','/normal-dvr-tools/broker_monitor.py'],extra={'NORMAL_DVR_LEDGER':'/shared/'+CASE+'/xc-live-publications.jsonl'})
    fixture.wait(lambda:(fixture.case_data/'xc-live-publications.ready').exists(),10,'actual-XC-producer-observer')
    subscription=module.subscribe(fixture)
    def snapshot():
        return fixture.sql("from apps.m3u.models import M3UAccount;from apps.channels.models import Stream,ChannelGroupM3UAccount;a=M3UAccount.objects.get(pk="+str(ident)+");print(json.dumps({'account':{'id':a.pk,'status':a.status,'last_message':a.last_message},'groups':list(ChannelGroupM3UAccount.objects.filter(m3u_account=a).values('channel_group__name','enabled','custom_properties','is_stale')),'streams':list(Stream.objects.filter(m3u_account=a).order_by('stream_id').values('id','name','url','stream_hash','logo_url','tvg_id','stream_id','stream_chno','custom_properties','is_adult','is_catchup','catchup_days','is_radio','is_stale'))},default=str))",secret=True)
    def terminal(task):
        key=fixture.sql("from celery import current_app;print(json.dumps(current_app.backend.get_key_for_task("+repr(task)+").decode()))",secret=True)
        raw=fixture.redis(2,"local key=ARGV[1];if redis.call('STRLEN',key)>262144 then return redis.error_reply('XC backend metadata capacity')end;return redis.call('GET',key)",key)
        return None if raw is None else json.loads(raw)
    seen=set()
    def invoke(label,expected_status):
        before=snapshot();save(private/(label+'-before.json'),before)
        response=fixture.api('POST','/api/m3u/refresh/'+str(ident)+'/',{})
        if response['status']!=202:raise AssertionError(('ActualXCRefreshAPI',response['status']))
        def published():
            path=fixture.case_data/'xc-live-publications.jsonl'
            if not path.exists():return False
            if path.stat().st_size>1048576:raise RuntimeError('ActualXCJournalBudget')
            for line in path.read_text().splitlines():
                try:value=json.loads(line)
                except json.JSONDecodeError:continue
                # Existing immutable observer preserves each exact LPUSH raw frame.
                frame=json.loads(value['raw_frame'])
                header=frame.get('headers',{})
                if header.get('task')=='apps.m3u.tasks.refresh_single_m3u_account'and header.get('id')not in seen:
                    return header['id']
            return False
        task=fixture.wait(published,15,'actual-XC-native-or-Source-API-publication');seen.add(task)
        backend=fixture.wait(lambda:terminal(task),90,'actual-XC-parent-backend');save(private/(label+'-backend.json'),backend)
        after=snapshot();save(private/(label+'-after.json'),after);trace=control(fixture,'/snapshot');save(output/(label+'-HTTP.json'),trace)
        if backend.get('status')!='SUCCESS'or after['account']['status']!=expected_status:raise AssertionError('ActualXCBackendOrStatusMismatch')
        for child in backend.get('children',[]):
            child_id=child[0][0];child_backend=fixture.wait(lambda:terminal(child_id),90,'actual-XC-profile-child-backend');save(private/(label+'-child-'+child_id+'.json'),child_backend)
            if child_backend.get('status')!='SUCCESS'or child_backend.get('parent_id')!=task:raise AssertionError('ActualXCChildResultOrLineage')
        save(private/(label+'-events.json'),module.events(fixture,subscription))
        return before,after,backend
    def cell(name,body):
        item={'name':name,'status':'running'};cells.append(item);save(output/'report.json',{'status':'running','cases':cells,'performance_verified':False})
        try:item['actual']=body();item['status']='passed'
        except BaseException as error:
            item.update(status='failed',error_type=type(error).__name__);(private/(name+'-failure.txt')).write_text(traceback.format_exc()[-8192:])
        save(output/'report.json',{'status':'running','cases':cells,'performance_verified':False})
    def initial():
        _,after,backend=invoke('initial','success');rows=after['streams'];assert len(rows)==1003
        first=next(row for row in rows if row['stream_id']==1);assert first['name']=='Owned normal XC - 1'and first['logo_url']is None and first['tvg_id']is None and first['stream_chno']==1.25
        assert len(backend.get('children',[]))==1
        assert next(row for row in rows if row['stream_id']==1002)['name']=='界'*512
        assert first['url'].startswith('http://127.0.0.1:'+str(PORT)+'/xc/live/')and first['url'].endswith('/1.ts')
        assert next(row for row in rows if row['stream_id']==2)['is_adult']
        assert next(row for row in rows if row['stream_id']==3)['catchup_days']==2
        assert next(row for row in rows if row['stream_id']==4)['is_radio']
        return {'stream_count':len(rows),'real_child_count':len(backend['children']),'nullable_fields_preserved':True,'Source_batch1000_crossed':True}
    def current_login_viewer():
        import urllib.request,time
        from pathlib import Path
        credentials=fixture.sql("from apps.m3u.models import M3UAccount;from apps.channels.models import Stream,ChannelStream;a=M3UAccount.objects.get(pk="+str(ident)+");s=Stream.objects.get(m3u_account=a,stream_id=1);ChannelStream.objects.filter(channel_id="+str(fixture.channel_id)+").delete();ChannelStream.objects.create(channel_id="+str(fixture.channel_id)+",stream=s,order=0);print(json.dumps({'username':a.username,'password':a.password,'stored_url':s.url,'stream_pk':s.pk}))",secret=True)
        new_user=uuid.uuid4().hex;new_password=uuid.uuid4().hex;before=control(fixture,'/snapshot');capture=output/'current-login-viewer.ts'
        try:
            control(fixture,'/control',{'rotate_login':[new_user,new_password]})
            fixture.sql("from apps.m3u.models import M3UAccount;M3UAccount.objects.filter(pk="+str(ident)+").update(username="+repr(new_user)+",password="+repr(new_password)+");print(json.dumps({'actual_account_login_rotated':True}))",secret=True)
            request=urllib.request.Request(fixture.base_url+'/proxy/ts/stream/'+fixture.channel_uuid,headers={'Authorization':'Bearer '+fixture.actual_admin_token(),'User-Agent':'owned-xc-current-login-viewer'})
            total=0;began=time.monotonic()
            with urllib.request.urlopen(request,timeout=5)as response,capture.open('wb')as stream:
                status=response.status
                while total<2000000:
                    fixture.remaining(1)
                    if time.monotonic()-began>30:raise TimeoutError('ActualXCViewerMediaBudget')
                    data=response.read1(65536)
                    if not data:break
                    stream.write(data);total+=len(data)
            trace=control(fixture,'/snapshot');live=[v for v in trace['trace'][len(before['trace']):]if v.get('path_kind')=='live']
            preserved=fixture.sql("from apps.channels.models import Stream;print(json.dumps(Stream.objects.get(pk="+str(credentials['stream_pk'])+").url))",secret=True)
            save(private/'current-login-Origin-observation.json',{'trace':trace,'stored_url_unchanged':preserved==credentials['stored_url']})
            assert status==200 and total>=2000000 and live and all(v['actual_auth_matches']for v in live)and preserved==credentials['stored_url']
            local='/evidence/'+str(capture.relative_to(fixture.output))
            probe=json.loads(fixture.command(fixture.compose+['exec','-T','baseline','ffprobe','-v','error','-show_entries','stream=codec_type','-of','json',local],30))
            progress=fixture.command(fixture.compose+['exec','-T','baseline','ffmpeg','-hide_banner','-loglevel','error','-xerror','-progress','pipe:1','-nostats','-i',local,'-t','3','-map','0:v:0','-map','0:a:0','-f','null','-'],30,allow=True)
            receipt=fixture.cli[-1];stderr=Path(receipt['stderr']).read_bytes();frames=max([int(line.split('=')[1])for line in progress.splitlines()if line.startswith('frame=')],default=0)
            actual={'actual_status':status,'bytes':total,'file_sha256':hashlib.sha256(capture.read_bytes()).hexdigest(),'Origin_current_login_requests':len(live),'stored_imported_url_unchanged':True,'actual_decode_exit':receipt['actual_exit'],'stderr_bytes':len(stderr),'video_frames':frames,'actual_AV':{'video','audio'}<={v['codec_type']for v in probe['streams']},'stdout_EOF':receipt['stdout_EOF'],'stderr_EOF':receipt['stderr_EOF'],'process_group_settled':receipt['process_group_settled']};save(output/'current-login-viewer.json',actual)
            assert receipt['actual_exit']==0 and not stderr and frames>=70 and actual['actual_AV']and all(actual[k]for k in ['stdout_EOF','stderr_EOF','process_group_settled'])
            return actual
        finally:
            control(fixture,'/control',{'rotate_login':[credentials['username'],credentials['password']]})
            fixture.sql("from apps.m3u.models import M3UAccount;M3UAccount.objects.filter(pk="+str(ident)+").update(username="+repr(credentials['username'])+",password="+repr(credentials['password'])+");print(json.dumps({'owned_login_restored':True}))",secret=True)
    def update():
        fixture.sql("from apps.channels.models import ChannelGroupM3UAccount;ChannelGroupM3UAccount.objects.filter(m3u_account_id="+str(ident)+",channel_group__name='XC disabled').update(enabled=False);print(json.dumps({'actual_boundary':'disable owned category input'}))")
        control(fixture,'/control',{'phase':'updated'});before,after,_=invoke('updated','success');assert len(after['streams'])==1001;first=next(row for row in after['streams']if row['stream_id']==1);assert first['name']=='Renamed first'and first['logo_url']=='https://owned.invalid/first.png'
        return {'before':len(before['streams']),'after':len(after['streams']),'stale_removed':True}
    def filters_rollup():
        setup=fixture.sql("from apps.m3u.models import M3UFilter;from apps.channels.models import Stream,Channel,ChannelStream;s=Stream.objects.get(m3u_account_id="+str(ident)+",stream_id=3);ChannelStream.objects.filter(channel_id="+str(fixture.channel_id)+").delete();ChannelStream.objects.create(channel_id="+str(fixture.channel_id)+",stream=s,order=0);Channel.objects.filter(pk="+str(fixture.channel_id)+").update(is_catchup=False,catchup_days=0);M3UFilter.objects.create(m3u_account_id="+str(ident)+",filter_type='name',regex_pattern='^xc STREAM 3$',exclude=False,order=1,custom_properties={'case_sensitive':False});M3UFilter.objects.create(m3u_account_id="+str(ident)+",filter_type='group',regex_pattern='XC enabled',exclude=True,order=2);print(json.dumps({'actual_mapped_stream_id':s.pk,'actual_owned_channel':"+str(fixture.channel_id)+"}))")
        before,after,_=invoke('ordered-filter-rollup','success');assert len(after['streams'])==1 and after['streams'][0]['stream_id']==3
        channel=fixture.sql("from apps.channels.models import Channel;print(json.dumps(Channel.objects.values('id','is_catchup','catchup_days').get(pk="+str(fixture.channel_id)+")))");save(private/'mapped-channel-after-filter.json',channel);assert channel['is_catchup']is True and channel['catchup_days']==2
        return {'actual_setup':setup,'ordered_first_include_overrides_later_group_exclude':True,'actual_streams_after':1,'actual_channel_rollup':channel}
    def self_heal():
        control(fixture,'/control',{'phase':'uncatchup'});_,after,_=invoke('mapped-self-heal','success');assert len(after['streams'])==1 and not after['streams'][0]['is_catchup']
        channel=fixture.sql("from apps.channels.models import Channel;print(json.dumps(Channel.objects.values('id','is_catchup','catchup_days').get(pk="+str(fixture.channel_id)+")))");save(private/'mapped-channel-after-self-heal.json',channel);assert channel['is_catchup']is False and channel['catchup_days']==0
        return {'actual_channel_rollup':channel,'existing_channel_identity_preserved':True}
    def empty():
        control(fixture,'/control',{'phase':'empty'});before,after,backend=invoke('empty','error');assert before['streams']==after['streams'];assert backend['result']=='Failed to update m3u account, no streams returned from provider';return {'no_destructive_cleanup':True,'normal_caught_result':backend['result']}
    def auth():
        control(fixture,'/control',{'phase':'auth-error'});before,after,backend=invoke('auth-error','error');assert before['streams']==after['streams'];assert backend['result']=='Failed to update m3u account - download failed or other error';return {'status':'error','no_destructive_cleanup':True,'credential_bearing_error_exported':False}
    for name,body in [('API-single-groups-provider-batch1000-nullability',initial),('current-rotated-login-stale-importedURL-live-AV',current_login_viewer),('actual-provider-update-enabled-filter-stale-cleanup',update),('ordered-account-filters-and-mapped-channel-rollup',filters_rollup),('mapped-channel-catchup-self-heal',self_heal),('empty-provider-caught-no-cleanup',empty),('auth-error-caught-queue-continuation',auth)]:cell(name,body)
    report={'status':'passed'if all(row['status']=='passed'for row in cells)else'failed','cases':cells,'new_Source_authority_required':True,'standalone_Group_native_API_producer_measured':False,'Source47_parity':False,'performance_verified':False};save(output/'report.json',report);return report
