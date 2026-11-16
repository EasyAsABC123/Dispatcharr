"""Natural aggregate POST/real Celery tasks in the existing owned cohort.
Original Dispatcharr/Django/Celery authors retain application/algorithm credit.
No Source task body/producer is replaced or invoked directly by this observer.
"""
import base64, json, traceback, uuid
from pathlib import Path
from workflow import save
import dvr_domain_cases as journal

CASE='m3u-refresh-all'
ROOT='apps.m3u.tasks.refresh_m3u_accounts'
CHILD='apps.m3u.tasks.refresh_single_m3u_account'

def prepare_context(f):
    if f.case!=CASE:raise ValueError('AggregateOwnedContextRequired')
    if f.backend=='Native':f._provider_environment['RUST_EXPERIMENTAL_SELECTED_BEAT']='0'
    f._aggregate_setup=f.sql("""
from apps.m3u.models import M3UAccount,M3UAccountProfile
from apps.channels.models import ChannelGroup,ChannelGroupM3UAccount
from core.models import CoreSettings
from django_celery_beat.models import PeriodicTask,PeriodicTasks
from apps.plugins.models import PluginConfig
PeriodicTask.objects.all().update(enabled=False);PeriodicTasks.update_changed()
assert not PluginConfig.objects.exists()
custom=M3UAccount.get_custom_account()
custom_before={'id':custom.pk,'name':custom.name,'is_active':custom.is_active,'locked':custom.locked,'file_path':custom.file_path,'server_url_present':bool(custom.server_url)}
M3UAccount.objects.all().update(is_active=False)
settings,_=CoreSettings.objects.get_or_create(key='stream_settings',defaults={'name':'Stream Settings','value':{}})
value=dict(settings.value or {});value['m3u_hash_key']='url';CoreSettings.objects.filter(pk=settings.pk).update(value=value)
rows=M3UAccount.objects.bulk_create([M3UAccount(name='Owned Aggregate '+letter,account_type='STD',file_path='/data/m3us/aggregate-'+letter+'.m3u',is_active=False,server_url=None,username=None,password=None,custom_properties={},stale_stream_days=0)for letter in ['A','B']])
for account,letter in zip(rows,['A','B']):
 M3UAccountProfile.objects.bulk_create([M3UAccountProfile(m3u_account=account,name='Default',is_default=True,is_active=True,search_pattern='',replace_pattern='')])
 group=ChannelGroup.objects.bulk_create([ChannelGroup(name='Owned Aggregate Group '+letter)])[0]
 ChannelGroupM3UAccount.objects.bulk_create([ChannelGroupM3UAccount(m3u_account=account,channel_group=group,enabled=True,auto_channel_sync=False)])
xc=M3UAccount.objects.bulk_create([M3UAccount(name='Owned Aggregate XC missing credentials',account_type='XC',is_active=False,server_url='http://127.0.0.1:1',username=None,password=None,custom_properties={})])[0]
print(json.dumps({'local_ids':[row.pk for row in rows],'xc_id':xc.pk,'custom':custom_before}))
""",secret=True)
    f.python("import pathlib,os\nfor letter in ['A','B']:\n p=pathlib.Path('/data/m3us')/('aggregate-'+letter+'.m3u');p.write_text('#EXTM3U\\n#EXTINF:-1 group-title=\"Owned Aggregate Group '+letter+'\",Owned Aggregate '+letter+'\\nhttp://owned.invalid/aggregate-'+letter+'\\n');os.chown(p,65532,65532);p.chmod(0o444)\n",secret=True)

def backend(f,ident):
    return f.sql("from celery import current_app;raw=current_app.backend.client.get(current_app.backend.get_key_for_task("+repr(ident)+"));print(raw.decode()if raw else 'null')",secret=True)

def broker(f):
    return f.sql("from celery import current_app;import redis;r=redis.Redis.from_url(current_app.conf.broker_url);print(json.dumps({'celery':r.llen('celery'),'dvr':r.llen('dvr'),'unacked':r.hlen('unacked'),'unacked_index':r.zcard('unacked_index')}));r.close()",secret=True)

def snapshot(f):
    return f.sql("from apps.m3u.models import M3UAccount;from apps.channels.models import Stream,ChannelGroupM3UAccount;from core.models import SystemEvent;assert SystemEvent.objects.count()<=256;print(json.dumps({'system_events':list(SystemEvent.objects.order_by('id').values()),'accounts':list(M3UAccount.objects.order_by('id').values('id','name','is_active','account_type','status','last_message','locked','file_path')),'streams':list(Stream.objects.filter(m3u_account_id__in="+repr(f._aggregate_setup['local_ids'])+").order_by('id').values('id','name','url','m3u_account_id','stream_hash')),'relationships':list(ChannelGroupM3UAccount.objects.filter(m3u_account_id__in="+repr(f._aggregate_setup['local_ids'])+").order_by('id').values('m3u_account_id','channel_group_id','enabled','auto_channel_sync'))},default=str))",secret=True)

CACHE_KEYS=[':1:coresettings:default_user_agent',':1:coresettings:default_user_agent:ver',':1:coresettings:group:system_settings',':1:coresettings:group:stream_settings',':1:coresettings:groupver:system_settings',':1:coresettings:groupver:stream_settings']
def cache_state(f):
    keys=json.dumps(CACHE_KEYS)
    return f.redis(0,"local keys=cjson.decode(ARGV[1]);local result={};for _,key in ipairs(keys)do local value=redis.call('DUMP',key);table.insert(result,{key,value and redis.sha1hex(value)or false});end;return result",keys)

def terminal(f,ident):
    value=backend(f,ident)
    return value if isinstance(value,dict)and value.get('status')in('SUCCESS','FAILURE','REVOKED')else False

def frames(observer):
    path=observer[1]/'snapshot.json'
    if not path.is_file():return []
    value=json.loads(path.read_text())
    if value.get('journal_error'):raise AssertionError(value)
    return [row['frame']for row in value['frames']]

def receiver_begin(f,label):
    name='specific.aggregate_'+uuid.uuid4().hex+'!receiver'
    actual=f.sql("import sys;sys.path.insert(0,'/normal-dvr-tools');import matching_guest;print(json.dumps(matching_guest.receiver_begin("+repr(name)+")))",secret=True)
    assert actual['actual_group_add_completed'] is True
    return name

def receiver_read(f,name):
    return f.sql("import redis,base64;from channels_redis.core import RedisChannelLayer;r=redis.Redis(host='127.0.0.1',port=6379,password='playback-fixture-only',db=0);layer=RedisChannelLayer(hosts=['redis://:playback-fixture-only@127.0.0.1:6379/0']);key=layer.prefix+"+repr(name.split('!')[0]+'!')+";raw=r.zrange(key,0,1024);assert len(raw)<=1024 and sum(map(len,raw))<=4194304;print(json.dumps({'actual_receiver':"+repr(name)+",'events':[layer.deserialize(v)for v in raw],'raw_events_base64':[base64.b64encode(v).decode()for v in raw]},default=str));r.close()",secret=True)

def _post(f,label,private,*,database=1):
    f._aggregate_receivers=getattr(f,'_aggregate_receivers',{});f._aggregate_receivers[label]=receiver_begin(f,label)
    observer=journal.observer_start(f,'aggregate-'+label,database=database)
    f._aggregate_open_observer=observer
    response=f.api('POST','/api/m3u/refresh/',{})
    save(private/(label+'-API.json'),response)
    assert response['status']==202 and response['body']=={'success':True,'message':'M3U refresh initiated.'}
    def root():
        observed=[row for row in frames(observer)if row['headers']['task']==ROOT]
        assert len(observed)<=1
        return observed[0]if observed else False
    frame=f.wait(root,15,'actual-aggregate-'+label+'-root')
    assert json.loads(base64.b64decode(frame['body']))==[[],{},{'callbacks':None,'errbacks':None,'chain':None,'chord':None}]
    assert frame['headers']['root_id']==frame['headers']['id'] and frame['headers']['parent_id']is None
    return observer,frame

def _settled(f,observer,frame,label,ids,private):
    ident=frame['headers']['id']
    parent=f.wait(lambda:terminal(f,ident),90,'actual-aggregate-'+label+'-parent-terminal')
    save(private/(label+'-parent-backend.json'),parent)
    assert parent['status']=='SUCCESS' and parent['result']==f'Queued M3U refresh for {len(ids)} active account(s).'
    def published():
        observed=[row for row in frames(observer)if row['headers']['task']==CHILD and row['headers']['parent_id']==ident]
        assert len(observed)<=len(ids)
        return observed if len(observed)==len(ids)else False
    children=f.wait(published,20,'actual-aggregate-'+label+'-children')if ids else []
    arguments=[json.loads(base64.b64decode(row['body']))[0]for row in children]
    assert sorted(arguments)==[[ident]for ident in sorted(ids)]
    actual_ids=[row['headers']['id']for row in children]
    assert len(set(actual_ids))==len(actual_ids)
    for row in children:
        assert row['headers']['root_id']==ident and row['headers']['parent_id']==ident
        assert row['headers']['timelimit']==[3600,3500]
    assert [row[0][0]for row in parent['children']]==actual_ids
    results={child:f.wait(lambda child=child:terminal(f,child),120,'actual-aggregate-'+label+'-child-terminal')for child in actual_ids}
    save(private/(label+'-children-backends.json'),results)
    assert all(value['status']=='SUCCESS'for value in results.values())
    f.wait(lambda:not any(broker(f).values()),20,'actual-aggregate-'+label+'-broker-drained')
    durable=journal.producer_observer_stop(f,observer);f._aggregate_open_observer=None
    save(private/(label+'-durable-journal.json'),durable)
    own=[row for row in durable['publications']if row['task']==CHILD and row['raw_frame']['headers']['parent_id']==ident]
    assert [row['id']for row in own]==actual_ids
    actual_WS=receiver_read(f,f._aggregate_receivers[label]);save(private/(label+'-actual-update-group.json'),actual_WS)
    return {'actual_update_group':actual_WS,'child_account_id_by_task':dict(zip(actual_ids,[a[0]for a in arguments])),'actual_root':ident,'actual_child_ids':actual_ids,'actual_account_ids':ids,'result':parent['result'],'child_backend_results':{child:value['result']for child,value in results.items()},'durable_journal_sha256':durable['raw_journal_sha256']}

def validate_custom_source55(f,custom,after,events,updates,result):
    if f.backend!='Native':raise AssertionError('NativeOnlyFollowupRequired')
    refs=f.binding['retained_source_aggregate55']
    import hashlib
    values={}
    for key,ref in refs.items():
        path=Path(ref['path']);assert hashlib.sha256(path.read_bytes()).hexdigest()==ref['sha256'],key
        values[key]=json.loads(path.read_text())
    source=values['custom_contract']
    def row_projection(row):
        return {key:row[key]for key in('name','is_active','locked','file_path','status','last_message')}
    assert row_projection(after)==row_projection(source['account_after'])
    assert result==source['actual_backend_result']
    def event_projection(rows):
        return [{key:row[key]for key in('event_type','channel_id','channel_name','details')}for row in rows]
    assert event_projection(events)==event_projection(source['actual_system_events'])
    def update_projection(rows,ident):
        return [{**row,'account':'<owned-custom-account>'}for row in rows if row['account']==ident]
    assert update_projection(updates,custom['id'])==update_projection(source['actual_update_group_payloads'],source['account_after']['id'])
    execution=values['execution'];assert execution['actual_exit']==0 and execution['stdout_EOF']and execution['stderr_EOF']and execution['process_group_settled']
    assert values['cleanup']['passed']and values['cleanup']['all_owned_resources_absent']
    assert values['aggregate']['exact_cleanup_passed']and values['aggregate']['combined_selected_workflow_passed']
    assert values['report']['status']=='passed'and len(values['report']['cases'])==4 and all(row['status']=='passed'for row in values['report']['cases'])

def run(f):
    assert f.backend=='Native','This revision is a Native-only Source55 followup'
    out=f.output/'m6-m3u-refresh-all';out.mkdir(exist_ok=False);private=out/'private';private.mkdir(mode=0o700)
    save(private/'setup.json',f._aggregate_setup)
    if f.backend=='Original':save(private/'actual-Beat-stop.json',f.role('beat',[],stop=True))
    if f.backend=='Native':
        status,raw=f.get(f.base_url+'/health/ready');health=json.loads(raw);save(private/'initial-Native-health.json',health)
        assert status==200 and health['selected_workers']['clocked_scheduler_enabled']is False
    for ref in f.binding['retained_source_aggregate55'].values():
        import hashlib
        assert hashlib.sha256(Path(ref['path']).read_bytes()).hexdigest()==ref['sha256']
    f.wait(lambda:not any(broker(f).values()),20,'actual-aggregate-initial-broker-drained')
    report={'status':'running','cases':[],'full_milestone_passed':False,'performance_verified':False,'general_custom_refresh_parity':False}
    def phase(name,body):
        row={'name':name,'status':'running'};report['cases'].append(row);save(out/'report.json',report)
        try:
            actual=body();save(private/(name+'-actual.json'),actual)
            row.update(status='passed',actual={key:value for key,value in actual.items()if key!='child_backend_results'})
        except BaseException as error:
            row.update(status='failed',failure_type=type(error).__name__);(private/(name+'-failure.txt')).write_text(traceback.format_exc()[-8192:])
            save(out/'report.json',report)
            observer=getattr(f,'_aggregate_open_observer',None)
            if observer is not None:
                try:save(private/(name+'-failure-observer-final.json'),journal.producer_observer_stop(f,observer));f._aggregate_open_observer=None
                except Exception as cleanup_error:row['observer_cleanup_error']=type(cleanup_error).__name__;save(out/'report.json',report)
            raise
        save(out/'report.json',report)
    phase('natural-POST-zero-active',lambda:_settled(f,*_post(f,'zero',private),'zero',[],private))
    def local():
        ids=f._aggregate_setup['local_ids'];f.sql('from apps.m3u.models import M3UAccount;M3UAccount.objects.filter(pk__in='+repr(ids)+').update(is_active=True);print(json.dumps(True))',secret=True)
        before=snapshot(f);save(private/'local-before.json',before)
        actual=_settled(f,*_post(f,'local',private),'local',ids,private)
        after=snapshot(f);save(private/'local-after.json',after)
        rows=[row for row in after['accounts']if row['id']in ids]
        assert len(rows)==2 and all(row['status']=='success'for row in rows)
        assert sorted(row['m3u_account_id']for row in after['streams'])==sorted(ids)
        assert all(value=='Dispatched jobs complete.'for value in actual['child_backend_results'].values())
        return {**actual,'actual_streams':2,'actual_account_successes':2}
    phase('natural-POST-local-STD-whole-children',local)
    def xc_missing_credentials():
        ident=f._aggregate_setup['xc_id']
        f.sql('from apps.m3u.models import M3UAccount;M3UAccount.objects.all().update(is_active=False);M3UAccount.objects.filter(pk='+str(ident)+').update(is_active=True);print(json.dumps(True))',secret=True)
        # Explicit owned cache-miss input; no production setting/TTL is changed.
        # The real child may fill caches AFTER the actual aggregate CAS.
        f.redis(0,"local keys=cjson.decode(ARGV[1]);for _,key in ipairs(keys)do redis.call('DEL',key);end;return #keys",json.dumps(CACHE_KEYS))
        save(private/'XC-cache-miss-input.json',cache_state(f))
        assert all(row[1]is None for row in cache_state(f))
        observed=_post(f,'XC',private,database=None)
        actual=_settled(f,*observed,'XC',[ident],private)
        save(private/'XC-after.json',snapshot(f))
        row=next(row for row in snapshot(f)['accounts']if row['id']==ident)
        assert row['status']=='error'
        assert list(actual['child_backend_results'].values())==['Failed to update m3u account - download failed or other error']
        if f.backend=='Native':
            durable=json.loads((private/'XC-durable-journal.json').read_text())
            rows=[json.loads(line)for line in Path(durable['raw_journal_path']).read_text().splitlines()]
            published=next(index for index,row in enumerate(rows)if row.get('db')==1 and row.get('command','').upper().startswith(('LPUSH CELERY ','RPUSH CELERY '))and ROOT in row['command'])
            claimed=next(index for index,row in enumerate(rows)if index>published and row.get('db')==1 and row.get('command','').upper()=='RPOP CELERY')
            writes=[]
            for row in rows[published:claimed]:
                parts=row.get('command','').split(' ',2)
                if row.get('db')==0 and len(parts)>1 and parts[1]in CACHE_KEYS and parts[0].upper()in('SET','SETEX','PSETEX','DEL','UNLINK','EXPIRE','PEXPIRE','EXPIREAT','PEXPIREAT','PERSIST'):writes.append(row)
            save(private/'XC-actual-preclaim-cache-window.json',{'actual_rows':rows[published:claimed+1],'forbidden_writes':writes})
            assert not writes
        updates=[row['data']for row in actual['actual_update_group']['events']if isinstance(row.get('data'),dict)and row['data'].get('type')=='m3u_refresh'and row['data'].get('account')==ident];assert updates and any(row.get('status')=='error'for row in updates)
        return {**actual,'actual_XC_error_is_caught_SUCCESS':True,'upstream_HTTP_required':False,'Native_cache_miss_preclaim_observed':f.backend=='Native'}
    phase('natural-POST-XC-caught-error-readonly-cache-miss',xc_missing_credentials)
    def mixed():
        ids=[f._aggregate_setup['custom']['id'],f._aggregate_setup['xc_id']]
        f.sql('from apps.m3u.models import M3UAccount;M3UAccount.objects.all().update(is_active=False);M3UAccount.objects.filter(pk__in='+repr(ids)+').update(is_active=True);print(json.dumps(True))',secret=True)
        before=snapshot(f);save(private/'mixed-before.json',before)
        cache_before=cache_state(f);save(private/'mixed-cache-before.json',cache_before)
        observer,frame=_post(f,'mixed',private,database=None if f.backend=='Native'else 1)
        actual=_settled(f,observer,frame,'mixed',ids,private);after=snapshot(f);save(private/'mixed-after.json',after)
        custom=f._aggregate_setup['custom'];custom_id=custom['id'];assert custom['is_active']is True and custom['locked']is True and not custom['file_path']and not custom['server_url_present']
        row=next(row for row in after['accounts']if row['id']==custom_id)
        assert row['is_active']is True and row['locked']==custom['locked']and row['file_path']==custom['file_path']
        assert row['status']=='error'and row['last_message']=='No M3U source available (missing URL and file)'
        task=next(task for task,account in actual['child_account_id_by_task'].items()if account==custom_id)
        assert actual['child_backend_results'][task]=='Failed to update m3u account - download failed or other error'
        old_event_ids={row['id']for row in before['system_events']};new_events=[row for row in after['system_events']if row['id']not in old_event_ids]
        custom_errors=[row for row in new_events if row['event_type']=='m3u_error'and row['details'].get('account_name')==custom['name']]
        assert len(custom_errors)==1;save(private/'mixed-custom-system-events.json',custom_errors)
        updates=[row['data']for row in actual['actual_update_group']['events']if isinstance(row.get('data'),dict)and row['data'].get('type')=='m3u_refresh'and row['data'].get('account')==custom_id]
        assert any(update.get('action')=='downloading'and update.get('status')=='error'and update.get('error')==row['last_message']and update.get('message')==row['last_message']for update in updates)
        active=[row['id']for row in before['accounts']if row['is_active']];assert sorted(active)==sorted(ids)
        inactive=[row['id']for row in before['accounts']if not row['is_active']];assert not set(inactive).intersection(actual['child_account_id_by_task'].values())
        save(private/'mixed-custom-exact-contract.json',{'account_before':next(row for row in before['accounts']if row['id']==custom_id),'account_after':row,'actual_child_task_id':task,'actual_backend_result':actual['child_backend_results'][task],'actual_system_events':custom_errors,'actual_update_group_payloads':updates,'actual_active_account_ids':active,'actual_inactive_account_ids':inactive})
        validate_custom_source55(f,custom,row,custom_errors,updates,actual['child_backend_results'][task])
        return {**actual,'actual_default_custom_caught_error_SUCCESS':True,'actual_default_custom_error_audits':1,'actual_Source_active_rows':active,'actual_inactive_rows_not_queued':inactive,'scope':'Actual default-custom and XC caught-error behavior; strict retained Source55 contract comparison.'}
    phase('natural-POST-mixed-defaultCustom-and-XC-caught-errors',mixed)
    report['status']='passed';save(out/'report.json',report);return report
