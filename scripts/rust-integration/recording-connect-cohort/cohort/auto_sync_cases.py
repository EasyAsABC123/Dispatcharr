"""One existing-fixture context, normal API -> remote ingest -> inline auto/EPG.
All original Dispatcharr/dependency authors retain application/algorithm credit.
No application/runtime starts on import. New Source characterization required.
"""
import hashlib,json,traceback
from pathlib import Path
CASE='auto-sync';PORT=18098;COUNT=6001

def save(path,value):
    Path(path).write_text(json.dumps(value,indent=2,allow_nan=False)+'\n')

def request(fixture,path,method='GET'):
    code="import json,urllib.request;req=urllib.request.Request("+repr('http://127.0.0.1:'+str(PORT)+path)+",method="+repr(method)+");r=urllib.request.build_opener(urllib.request.ProxyHandler({})).open(req,timeout=5);raw=r.read(1048577);assert len(raw)<=1048576;print(json.dumps(None)if not raw else raw.decode())"
    return json.loads(fixture.python(code,seconds=10,secret=True))

def prepare_context(fixture):
    if fixture.case!=CASE:return
    # A separate declared context, not a replay of qualified XC/provider cells.
    # Only data setup uses Source ORM; all task production below uses real API.
    fixture.role('auto-sync-origin',['/dispatcharrpy/bin/python','/normal-dvr-tools/auto_sync_origin.py'],extra={'AUTO_SYNC_DATA':'/shared/'+CASE})
    fixture.wait(lambda:(fixture.case_data/'auto-origin.ready').is_file(),15,'actual-auto-Origin-listener')
    setup=fixture.sql('''
from apps.m3u.models import M3UAccount,M3UAccountProfile
from apps.channels.models import ChannelGroup,ChannelGroupM3UAccount,ChannelProfile
from apps.epg.models import EPGSource,EPGData
from django_celery_beat.models import PeriodicTask,PeriodicTasks
from core.models import CoreSettings
# bulk_create intentionally avoids unrelated startup/post_save task dispatch in
# this DATA fixture. It is not evidence for account CRUD/signals parity.
a=M3UAccount.objects.bulk_create([M3UAccount(name='Owned Auto',account_type='STD',server_url='http://127.0.0.1:18098/owned.m3u',stale_stream_days=0,custom_properties={'hash_key':'url','auto_enable_new_groups_live':True,'orphan_channel_cleanup':'never'})])[0]
M3UAccountProfile.objects.bulk_create([M3UAccountProfile(m3u_account=a,name='Default',is_default=True,is_active=True)])
g=ChannelGroup.objects.create(name='Owned Auto')
p=ChannelProfile.objects.create(name='Owned Auto profile')
props={'channel_numbering_mode':'fixed','channel_sort_order':'name','name_match_regex':r'^Auto \\d+$','name_regex_pattern':r'^Auto (\\d+)$','name_replace_pattern':'Renamed $1','channel_profile_ids':[p.pk]}
r=ChannelGroupM3UAccount.objects.create(channel_group=g,m3u_account=a,enabled=True,auto_channel_sync=True,auto_sync_channel_start=20000,custom_properties=props)
e=EPGSource.objects.bulk_create([EPGSource(name='Owned Auto XML',source_type='xmltv',file_path='/data/epgs/auto.xml')])[0]
rows=EPGData.objects.bulk_create([EPGData(epg_source=e,tvg_id='auto-epg-'+str(i),name='Owned EPG '+str(i))for i in range(3)])
PeriodicTask.objects.all().update(enabled=False);PeriodicTasks.update_changed()
row,_=CoreSettings.objects.get_or_create(key='backup_settings',defaults={'name':'Backups','value':{}})
CoreSettings.objects.filter(pk=row.pk).update(value={**(row.value or {}),'schedule_enabled':False})
print(json.dumps({'account':a.pk,'group':g.pk,'relation':r.pk,'profile':p.pk,'epg':[x.pk for x in rows],'source':e.pk,'declared_streams':6001}))
''',secret=True)
    fixture._auto_sync=setup
    # Real complete XML input; both backends see the SAME /data file and UID.
    code="import pathlib,os,datetime;root=pathlib.Path("+repr('/shared/'+CASE+'/epgs/auto.xml')+");now=datetime.datetime.now(datetime.timezone.utc);start=(now-datetime.timedelta(minutes=5)).strftime('%Y%m%d%H%M%S +0000');end=(now+datetime.timedelta(hours=1)).strftime('%Y%m%d%H%M%S +0000');text='<?xml version=\"1.0\"?><tv>'+''.join('<channel id=\"auto-epg-%d\"><display-name>Owned %d</display-name></channel>'%(i,i)for i in range(3))+''.join('<programme channel=\"auto-epg-%d\" start=\"%s\" stop=\"%s\"><title>Owned Auto Programme %d</title></programme>'%(i,start,end,i)for i in range(3))+'</tv>';root.write_text(text);os.chown(root,65532,65532);root.chmod(0o444);print(__import__('json').dumps({'bytes':root.stat().st_size,'sha256':__import__('hashlib').sha256(root.read_bytes()).hexdigest()}))"
    setup['actual_XML']=json.loads(fixture.python(code,seconds=10,secret=True))
    save(Path(fixture.output)/'auto-inputs.json',setup)

def run(fixture):
    if fixture.case!=CASE:raise ValueError('OwnedAutoContextRequired')
    output=Path(fixture.output)/CASE;output.mkdir(exist_ok=False);private=output/'private';private.mkdir(mode=0o700)
    setup=fixture._auto_sync;account=setup['account'];cells=[];seen=set()
    # Dedicated observer budget is declared BEFORE either backend runs business.
    # The old recording observer/caps are unchanged. Source full Groups result
    # can appear in MONITOR even though only broker publications are retained.
    fixture.role('auto-sync-publications',['/dispatcharrpy/bin/python','/normal-dvr-tools/auto_sync_monitor.py'],extra={'NORMAL_DVR_LEDGER':'/shared/'+CASE+'/auto-publications.jsonl'})
    fixture.wait(lambda:(fixture.case_data/'auto-publications.ready').exists(),10,'actual-auto-publication-observer')
    def frames():
        path=fixture.case_data/'auto-publications.jsonl'
        if not path.exists():return []
        if path.stat().st_size>32*1024*1024:raise RuntimeError('AutoPublicationCaptureBudget')
        rows=[]
        for line in path.read_text().splitlines():
            try:rows.append(json.loads(line))
            except json.JSONDecodeError:continue
        return rows
    calibration='00000000-0000-0000-0000-000000000000'
    actual_key=fixture.sql("from celery import current_app;print(json.dumps(current_app.backend.get_key_for_task("+repr(calibration)+").decode()))",secret=True)
    assert actual_key.endswith(calibration);backend_prefix=actual_key[:-len(calibration)]
    fixture._auto_backend_prefix=backend_prefix
    def terminal(task):
        key=backend_prefix+task
        raw=fixture.redis(2,"local n=redis.call('STRLEN',ARGV[1]);if n>8388608 then return redis.error_reply('declared auto result capture limit') end;return redis.call('GET',ARGV[1])",key)
        return None if raw is None else json.loads(raw)
    def snapshot():
        return fixture.sql("from apps.channels.models import Channel,ChannelStream,ChannelProfileMembership;from apps.epg.models import ProgramData;from apps.m3u.models import M3UAccount;a=M3UAccount.objects.get(pk="+str(account)+");rows=list(Channel.objects.filter(channel_group_id="+str(setup['group'])+").order_by('channel_number','id').values('id','uuid','name','channel_number','epg_data_id','hidden_from_output'));norm=[{k:v for k,v in r.items()if k not in('id','uuid','epg_data_id')}for r in rows];print(json.dumps({'account_status':a.status,'last_message':a.last_message,'count':len(rows),'sample':rows[:8]+rows[-8:],'normalized_rows_sha256':__import__('hashlib').sha256(json.dumps(norm,sort_keys=True,default=str).encode()).hexdigest(),'stream_links':ChannelStream.objects.filter(channel_id__in=[r['id']for r in rows]).count(),'memberships':ChannelProfileMembership.objects.filter(channel_id__in=[r['id']for r in rows],channel_profile_id="+str(setup['profile'])+",enabled=True).count(),'programmes':list(ProgramData.objects.filter(epg_id__in="+repr(setup['epg'])+").values('epg_id','title'))},default=str))",secret=True)
    def invoke(label):
        response=fixture.api('POST','/api/m3u/refresh/'+str(account)+'/',{});save(private/(label+'-API.json'),response)
        if response['status']!=202:raise AssertionError('ActualAutoRefreshAPI')
        def parent():
            for row in frames():
                h=row.get('headers')or{}
                if h.get('task')=='apps.m3u.tasks.refresh_single_m3u_account'and h['id']not in seen:return h['id']
            return False
        task=fixture.wait(parent,20,'actual-auto-API-parent');seen.add(task)
        backend=fixture.wait(lambda:terminal(task),240,'actual-inline-auto-parent-terminal');save(private/(label+'-backend.json'),backend)
        after=snapshot();save(private/(label+'-SQL.json'),after)
        if backend.get('status')!='SUCCESS'or after['account_status']!='success':raise AssertionError('ActualAutoParentNotSuccess')
        child_ids=[]
        for child in backend.get('children',[]):
            ident=child[0][0];value=fixture.wait(lambda:terminal(ident),90,'actual-auto-descendant-terminal');save(private/(label+'-child-'+ident+'.json'),value)
            if value.get('status')!='SUCCESS'or value.get('parent_id')!=task:raise AssertionError('ActualAutoChildBackendLineage')
            child_ids.append(ident)
        save(private/(label+'-raw-publications.json'),frames())
        return after,task,child_ids
    def cell(name,body):
        row={'name':name,'status':'running'};cells.append(row);save(output/'report.json',{'status':'running','cases':cells})
        try:row.update(actual=body(),status='passed')
        except BaseException as error:
            row.update(status='failed',error_type=type(error).__name__);(private/(name+'-failure.txt')).write_text(traceback.format_exc()[-8192:])
        save(output/'report.json',{'status':'running','cases':cells})
    def initial():
        after,task,children=invoke('initial');fixture._auto_first=after
        assert after['count']==COUNT and after['stream_links']==COUNT and after['memberships']==COUNT
        assert all(r['name'].startswith('Renamed ')for r in after['sample'])and len(after['programmes'])==3
        assert len(children)==3
        return {'channels':after['count'],'Source_batch500_crossed':True,'previous4096_limit_crossed':True,'actual_EPG_children':len(children),'parent':task,'normalized_rows_sha256':after['normalized_rows_sha256']}
    def update_delete():
        before=snapshot();request(fixture,'/phase1','POST')
        fixture.sql("from apps.channels.models import ChannelGroupM3UAccount;r=ChannelGroupM3UAccount.objects.get(pk="+str(setup['relation'])+");p=dict(r.custom_properties);p['name_replace_pattern']='Edited $1';r.custom_properties=p;r.save(update_fields=['custom_properties']);print(json.dumps({'actual_configuration_changed':True}))",secret=True)
        after,task,children=invoke('changed');assert after['count']==COUNT and after['memberships']==COUNT and after['stream_links']==5001
        # Source updates linked rows; the never policy preserves disconnected
        # channels, including their preexisting names. Assert the full controlled
        # projection, rather than wrongly renaming preserved orphan samples.
        expected=[{'name':('Edited 'if i<=5001 else'Renamed ')+f'{i:05d}','channel_number':20000.0+i-1,'hidden_from_output':False}for i in range(1,COUNT+1)]
        expected_sha=hashlib.sha256(json.dumps(expected,sort_keys=True,default=str).encode()).hexdigest()
        assert after['normalized_rows_sha256']==expected_sha
        assert any(r['name'].startswith('Edited ')for r in after['sample'])and any(r['name'].startswith('Renamed ')for r in after['sample'])
        old={r['name'].split(' ',1)[1]:r['uuid']for r in before['sample']};new={r['name'].split(' ',1)[1]:r['uuid']for r in after['sample']}
        assert old.keys()&new.keys()and all(old[k]==new[k]for k in old.keys()&new.keys())
        assert len(children)==0
        return {'actual_deleted_channels':before['count']-after['count'],'actual_orphan_channels_preserved_by_never_policy':after['count']-after['stream_links'],'UUIDs_preserved_for_observed_survivors':True,'unchanged_EPG_no_children':True,'parent':task,'normalized_rows_sha256':after['normalized_rows_sha256']}
    def orphan_prune():
        # A NEW declared Source boundary: the preceding never policy preserves
        # disconnected channels after stream cleanup. Change the real policy,
        # then invoke the real API again to exercise Source Collector deletion.
        before=snapshot();assert before['count']==COUNT and before['stream_links']==5001
        fixture.sql("from apps.m3u.models import M3UAccount;a=M3UAccount.objects.get(pk="+str(account)+");p=dict(a.custom_properties);assert p['orphan_channel_cleanup']=='never';p['orphan_channel_cleanup']='always';a.custom_properties=p;a.save(update_fields=['custom_properties']);print(json.dumps({'orphan_channel_cleanup':'always'}))",secret=True)
        after,task,children=invoke('orphan-prune-policy-always')
        assert after['count']==5001 and after['memberships']==5001 and after['stream_links']==5001
        assert before['count']-after['count']==1000 and not children
        return {'new_Source_characterization_required':True,'actual_policy':'always','actual_Collector_deleted_channels':1000,'parent':task,'normalized_rows_sha256':after['normalized_rows_sha256']}
    def source_producer(script,secret=True):
        # Bind the SAME declared owned namespace before Django/Celery imports.
        # Fixture.sql configures SQL only; its default broker can be DB0, so it
        # cannot represent this Source-producer -> real DB1-consumer boundary.
        pre="import os,sys,json;from urllib.parse import quote;password=quote(os.environ['REDIS_PASSWORD'],safe='');os.environ['CELERY_BROKER_URL']='redis://:'+password+'@localhost:6379/1';os.environ['CELERY_RESULT_BACKEND']='redis://:'+password+'@localhost:6379/2';sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings');import django;django.setup();from celery import current_app;from urllib.parse import urlsplit;assert urlsplit(current_app.conf.broker_url).path=='/1';assert urlsplit(current_app.conf.result_backend).path=='/2';\n"
        return json.loads(fixture.python(pre+script,secret=True))
    def standalone():
        before=snapshot()
        produced=source_producer("from apps.m3u.tasks import sync_auto_channels;from apps.channels.models import Stream;from celery import current_app;from urllib.parse import urlsplit;import datetime;assert urlsplit(current_app.conf.broker_url).path=='/1';start=Stream.objects.filter(m3u_account_id="+str(account)+").order_by('last_seen').values_list('last_seen',flat=True).first()-datetime.timedelta(seconds=1);task=sync_auto_channels.apply_async(args=["+str(account)+",start.isoformat()]);print(json.dumps({'id':task.id,'scan_start':start.isoformat(),'producer':'actual pinned Source Celery/Kombu'}))",secret=True)
        task=produced['id'];save(private/'standalone-actual-publication.json',produced)
        backend=fixture.wait(lambda:terminal(task),240,'actual-Source-frame-auto-consumer');save(private/'standalone-backend.json',backend)
        observed=next((r for r in frames()if(r.get('headers')or{}).get('id')==task),None);save(private/'standalone-raw-frame.json',observed)
        after=snapshot();save(private/'standalone-SQL.json',after)
        assert observed and observed['headers']['task']=='apps.m3u.tasks.sync_auto_channels'
        assert backend.get('status')=='SUCCESS'and backend['result']['status']=='ok'
        assert backend['result']['channels_created']==0 and backend['result']['channels_deleted']==0
        assert after['count']==5001 and before['normalized_rows_sha256']==after['normalized_rows_sha256']
        return {'actual_producer':'pinned Source Celery/Kombu','actual_consumer':fixture.backend,'id':task,'no_reexec_or_restart_claim':True,'unchanged_SQL_digest':True}
    def large_metadata():
        # Real new domain: Source-less EPGData are legal persisted models. Both
        # actual TVG bodies return Source's "EPG not found", without fake plans
        # or5001 full XML re-parses. The auto task publishes all real children.
        configured=source_producer("from apps.channels.models import Stream;from apps.epg.models import EPGData;from apps.m3u.tasks import sync_auto_channels;from celery import current_app;from urllib.parse import urlsplit;import datetime;streams=list(Stream.objects.filter(m3u_account_id="+str(account)+").order_by('id'));assert len(streams)==5001;guides=EPGData.objects.bulk_create([EPGData(tvg_id='owned-large-'+str(v.pk),name='Owned Missing Source '+str(v.pk))for v in streams],batch_size=500);\nfor v,g in zip(streams,guides):v.tvg_id=g.tvg_id\nStream.objects.bulk_update(streams,['tvg_id'],batch_size=500);assert urlsplit(current_app.conf.broker_url).path=='/1';start=min(v.last_seen for v in streams)-datetime.timedelta(seconds=1);t=sync_auto_channels.apply_async(args=["+str(account)+",start.isoformat()]);print(json.dumps({'id':t.id,'epg_ids':[g.pk for g in guides],'declared_actual_children':len(guides)}))",secret=True)
        task=configured['id'];save(private/'large-configured-models-publication.json',configured)
        value=fixture.wait(lambda:terminal(task),360,'actual-large-auto-parent');save(private/'large-parent-complete.json',value)
        assert value['status']=='SUCCESS'and value['result']['status']=='ok'
        child_ids=[v[0][0]for v in value['children']];assert len(child_ids)==5001 and len(set(child_ids))==5001
        key=backend_prefix+task
        metrics=fixture.redis(2,"return{redis.call('STRLEN',ARGV[1]),redis.call('TTL',ARGV[1])}",key)
        assert metrics[0]>262144 and 0<metrics[1]<=3600
        # Calibrate prefix with the actual backend instead of assuming the
        # repeatedly troublesome celery-tasks: global keyprefix.
        first_key=backend_prefix+child_ids[0]
        assert first_key.endswith(child_ids[0]);prefix=first_key[:-len(child_ids[0])];collected=[]
        for offset in range(0,len(child_ids),100):
            ids=child_ids[offset:offset+100]
            def page():
                raw=fixture.redis(2,"local out={};for i=2,#ARGV do local k=ARGV[1]..ARGV[i];local n=redis.call('STRLEN',k);if n>65536 then return redis.error_reply('auto child body capture limit')end;local v=redis.call('GET',k);if not v then return false end;out[#out+1]=v end;return out",prefix,*ids)
                return None if raw is None else[json.loads(v)for v in raw]
            rows=fixture.wait(page,90,'actual-large-auto-child-page')
            for ident,row in zip(ids,rows):
                assert row['task_id']==ident and row['status']=='SUCCESS'and row['result']=='EPG not found'and row.get('parent_id')==task
            collected.extend(rows)
        save(private/'large-real-child-backends.json',collected)
        actual_frames=[r for r in frames()if(r.get('headers')or{}).get('parent_id')==task]
        assert len(actual_frames)==5001 and all(r['headers']['root_id']==task for r in actual_frames)
        assert {r['headers']['id']for r in actual_frames}==set(child_ids)
        save(private/'large-real-child-publications.json',actual_frames)
        return {'actual_children':len(child_ids),'actual_complete_metadata_bytes':metrics[0],'actual_metadata_TTL':metrics[1],'all_actual_child_results':'EPG not found','actual_parent_root_lineage':True,'read_scope':'actual Redis complete bytes; packaged Jobs full-reader API unmeasured','old_SUCCESS_preservation':'unmeasured','completion_PUBLISH_event':'unmeasured'}
    cell('actual-API-remoteSTD-large-inline-regex-profile-EPG-lineage',initial)
    cell('same-boot-provider-drop-rename-never-policy-preserve-orphans',update_delete)
    cell('same-boot-explicit-always-policy-Collector-delete1000',orphan_prune)
    cell('actual-Source-produced-standalone-auto-frame',standalone)
    cell('actual-large-parent-5001-descendants-full-metadata',large_metadata)
    report={'status':'passed'if all(c['status']=='passed'for c in cells)else'failed','cases':cells,'new_Source_characterization':True,'performance_verified':False,'Source47_parity':False,'unrun_required':['nonempty queue restart with surviving Source auto frame','held Collector physical uncertainty','mixed recording/live contention','regex timeout/invalid pattern branches','compact numbering','new SchedulesDirect guide']}
    save(output/'report.json',report);return report
