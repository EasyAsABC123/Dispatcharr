"""Real Core PATCH -> task -> effects extension for the shared milestone cohort.
Original Dispatcharr/dependency authors retain all application/protocol credit.
No applications start on import. Invoke only on a candidate containing this port.
"""
import datetime as dt
import hashlib
import importlib.util
import json
from pathlib import Path
import time
import uuid


def base(fixture):
    path=Path(fixture.binding['m6_adapter'])
    if hashlib.sha256(path.read_bytes()).hexdigest()!=fixture.binding['m6_adapter_sha256']:
        raise ValueError('ExactBoundM6AdapterRequired')
    spec=importlib.util.spec_from_file_location('m6_core_shared',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module


def frames(fixture):
    path=fixture.case_data/'m6-core-publications.jsonl'
    if not path.exists():return []
    if path.stat().st_size>fixture.binding['core_monitor_budget']['bytes']:raise ValueError('ActualCorePublicationBudget')
    return [json.loads(line)for line in path.read_text().splitlines()]


def core_patch(fixture,key,changes):
    row=fixture.sql("from core.models import CoreSettings;r=CoreSettings.objects.get(key="+repr(key)+");print(json.dumps({'id':r.pk,'value':r.value}))")
    value=dict(row['value']);value.update(changes)
    began=time.time();response=fixture.api('PATCH','/api/core/settings/'+str(row['id'])+'/',{'value':value})
    if response['status']!=200:raise AssertionError(('ActualCorePATCH',response))
    return began,response


def actual_task(fixture,module,task,began):
    def observed():
        rows=[row for row in frames(fixture)if row['headers']['task']==task and row['observed_epoch']>=began]
        return rows[0]if rows else None
    frame=fixture.wait(observed,30,'ActualCoreProducedTask:'+task)
    terminal=module.result(fixture,frame['headers']['id'],90)
    module.save(module.directory(fixture)/(task.rsplit('.',1)[-1]+'-actual-backend.json'),terminal)
    if terminal['metadata']['status']!='SUCCESS':raise AssertionError(('ActualCoreWorkerFailure',terminal))
    return {'scope':'actual Core HTTP producer -> packaged worker, no projected root frame','publication':frame,'backend':terminal}


def rehash(fixture,module):
    nonce=uuid.uuid4().hex[:10]
    seeded=fixture.sql("""
from apps.m3u.models import M3UAccount
from apps.channels.models import Stream,ChannelStream
from django.utils import timezone
import hashlib
custom=M3UAccount.get_custom_account()
props=dict(custom.custom_properties or {});props['hash_key']='name'
M3UAccount.objects.filter(pk=custom.pk).update(custom_properties=props)
account=M3UAccount.objects.filter(file_path__startswith='/data/m3us/').first()
if account is None:raise RuntimeError('MissingActualScannerAccount')
rows=[Stream(name='Core duplicate """+nonce+"""',url='http://owned.invalid/core-duplicate',tvg_id='core-'+str(i),m3u_account=account,stream_hash=hashlib.sha256(('old-core-"""+nonce+"""-'+str(i)).encode()).hexdigest())for i in range(2)]
Stream.objects.bulk_create(rows)
ChannelStream.objects.bulk_create([ChannelStream(channel_id="""+str(fixture.channel_id)+""",stream_id=rows[0].pk,order=10),ChannelStream(channel_id="""+str(fixture.channel_id)+""",stream_id=rows[1].pk,order=1)])
print(json.dumps({'ids':[r.pk for r in rows],'override':custom.pk,'before':list(Stream.objects.values('id','stream_hash','m3u_account_id'))}))
""")
    protected='task_lock_refresh_single_m3u_account_'+str(seeded['override']);token=uuid.uuid4().hex
    fixture.redis(0,"return redis.call('SET',ARGV[1],ARGV[2],'EX',600)",protected,token)
    try:
        subscription=module.subscribe(fixture)
        began,response=core_patch(fixture,'stream_settings',{'m3u_hash_key':'name,url'})
        observed=actual_task(fixture,module,'core.tasks.rehash_streams',began)
        after=fixture.sql("from apps.channels.models import Stream,ChannelStream;print(json.dumps({'streams':list(Stream.objects.values('id','stream_hash','m3u_account_id')),'relationships':list(ChannelStream.objects.values('channel_id','stream_id','order'))}))")
        ids={row['id']for row in after['streams']}
        removed,kept=seeded['ids']
        if removed in ids or kept not in ids:raise AssertionError(('ActualDuplicateWinner',after))
        if not any(row['stream_id']==kept and row['channel_id']==fixture.channel_id and row['order']==1 for row in after['relationships']):raise AssertionError('ActualDuplicateRelationshipOrder')
        previous={row['id']:row['stream_hash']for row in seeded['before']if row['m3u_account_id']==seeded['override']}
        if any(row['stream_hash']!=previous[row['id']]for row in after['streams']if row['id']in previous):raise AssertionError('OverrideStreamsWereRehashed')
        protected_after=fixture.redis(0,"return redis.call('GET',ARGV[1])or''",protected)
        if protected_after!=token:raise AssertionError('ForeignOverrideLockChanged')
        body=json.loads(__import__('base64').b64decode(json.loads(observed['publication']['raw_frame'])['body']))
        lock_ids=body[1]['preacquired_lock_ids']
        remaining=fixture.redis(0,"local out={};for i=1,#ARGV do out[i]=redis.call('GET',ARGV[i])or'' end;return out",*['task_lock_refresh_single_m3u_account_'+str(ident)for ident in lock_ids])
        if any(remaining):raise AssertionError(('OwnedRehashLocksNotReleased',remaining))
        emitted=module.events(fixture,subscription)
        if not any(row.get('data',{}).get('action')=='completed'and row['data'].get('duplicates_merged',0)>=1 for row in emitted):raise AssertionError('MissingActualRehashCompletionEvent')
        return {'api':response,'seed':seeded,'task':observed,'after':after,'events':emitted,'owned_locks_after':remaining,'foreign_override_lock_unchanged':True}
    finally:
        fixture.redis(0,"if redis.call('GET',ARGV[1])==ARGV[2]then return redis.call('DEL',ARGV[1])end return 0",protected,token)


def offsets(fixture,module):
    fixture.sql("from apps.channels.models import Channel;Channel.objects.filter(pk="+str(fixture.channel_id)+").update(logo_id=None,epg_data_id=None);print(json.dumps(True))")
    start=dt.datetime.now(dt.timezone.utc)+dt.timedelta(hours=2);end=start+dt.timedelta(minutes=10)
    programme={'title':fixture.channel_name,'start_time':start.isoformat(),'end_time':end.isoformat()}
    response=fixture.api('POST','/api/channels/recordings/',{'channel':fixture.channel_id,'start_time':start.isoformat(),'end_time':end.isoformat(),'custom_properties':{'program':programme}})
    if response['status']!=201:raise AssertionError(('ActualBookingHTTP',response))
    ident=response['body']['id']
    before=fixture.sql("from django_celery_beat.models import PeriodicTask;print(json.dumps({'recording':Recording.objects.values().get(pk="+str(ident)+"),'periodic':list(PeriodicTask.objects.filter(name='dvr-recording-"+str(ident)+"').values())},default=str))")
    subscription=module.subscribe(fixture)
    began,patch=core_patch(fixture,'dvr_settings',{'pre_offset_minutes':2,'post_offset_minutes':3})
    observed=actual_task(fixture,module,'apps.channels.tasks.reschedule_upcoming_recordings_for_offset_change',began)
    after=fixture.sql("from django_celery_beat.models import PeriodicTask,ClockedSchedule;row=Recording.objects.get(pk="+str(ident)+");p=PeriodicTask.objects.get(name='dvr-recording-"+str(ident)+"');print(json.dumps({'recording':{'start':row.start_time.isoformat(),'end':row.end_time.isoformat(),'task_id':row.task_id},'periodic':{'id':p.pk,'args':json.loads(p.args),'clocked':ClockedSchedule.objects.get(pk=p.clocked_id).clocked_time.isoformat()}},default=str))")
    expected_start=start-dt.timedelta(minutes=2);expected_end=end+dt.timedelta(minutes=3)
    if dt.datetime.fromisoformat(after['recording']['start'])!=expected_start or dt.datetime.fromisoformat(after['recording']['end'])!=expected_end:raise AssertionError(('ActualOffsetWindow',after))
    if after['periodic']['id']in [row['id']for row in before['periodic']]:raise AssertionError('OldPeriodicWasNotReplaced')
    if dt.datetime.fromisoformat(after['periodic']['args'][2])!=expected_start or dt.datetime.fromisoformat(after['periodic']['args'][3])!=expected_end:raise AssertionError('ActualRescheduledArgsWrong')
    result=observed['backend']['metadata']['result']
    if result['changed']<1 or result['pre']!=2 or result['post']!=3:raise AssertionError(('ActualOffsetReport',result))
    parent=observed['publication']['headers']['id']
    expected_ids={item[0][0]for item in observed['backend']['metadata']['children']}
    def all_publications():
        found=[row for row in frames(fixture)if row['headers'].get('parent_id')==parent]
        return found if {row['headers']['id']for row in found}==expected_ids else None
    children=fixture.wait(all_publications,10,'ActualCompleteCoreChildPublicationSet')
    if not any(row['headers']['task']=='apps.channels.tasks.prefetch_recording_artwork'and row['headers'].get('eta')for row in children):raise AssertionError('MissingRealParentedCountdownArtwork')
    outcomes=child_results(fixture,children,parent,expected_ids)
    return {'booking':response,'patch':patch,'before':before,'task':observed,'after':after,'children':outcomes,'events':module.events(fixture,subscription)}


def run(fixture):
    if fixture.case!='grouped-m6':raise ValueError('MustReuseGroupedM6ResetContext')
    module=base(fixture)
    ledger='/shared/'+fixture.case+'/m6-core-publications.jsonl'
    fixture.role('m6-core-publications',['/dispatcharrpy/bin/python','/normal-dvr-tools/broker_monitor.py'],extra={'NORMAL_DVR_LEDGER':ledger})
    cells=[]
    try:
        fixture.wait(lambda:(fixture.case_data/'m6-core-publications.ready').exists(),10,'ActualCorePublicationObserver')
        for name,callback in [('core-rehash',rehash),('core-offsets',offsets)]:
            cells.append(module.run_cell(fixture,name,lambda callback=callback:callback(fixture,module)))
    finally:fixture.role('m6-core-publications',[],stop=True)
    report={'status':'passed'if all(row['status']=='passed'for row in cells)else'failed','cells':cells,'scope':'real Core API producer chains; Source47/Beat/performance remain open'}
    module.save(module.directory(fixture)/'core-task-report.json',report)
    return report


def child_results(fixture,children,parent,expected_ids):
    ids=[row['headers']['id']for row in children]
    if len(ids)!=len(expected_ids)or set(ids)!=set(expected_ids):raise AssertionError('ActualCoreChildCoverageIncomplete')
    outputs=[]
    for index in range(0,len(children),128):
        batch=children[index:index+128]
        ids=[child['headers']['id']for child in batch]
        def settled():
            values=fixture.redis(2,"local r={};for i,id in ipairs(ARGV)do local a='celery-tasks:celery-task-meta-'..id;local b='celery-task-meta-'..id;r[i]={redis.call('GET',a)or'',redis.call('GET',b)or'',redis.call('TTL',a),redis.call('TTL',b)} end;return r",*ids)
            parsed=[]
            for first,second,first_ttl,second_ttl in values:
                if first and second:raise RuntimeError('ConflictingBackendNamespaces')
                raw=first or second
                parsed.append({'raw':raw,'metadata':json.loads(raw),'ttl':first_ttl if first else second_ttl}if raw else None)
            return parsed if all(value and value['metadata'].get('status')in('SUCCESS','FAILURE','REVOKED')for value in parsed)else None
        terminal=fixture.wait(settled,90,'ActualCoreChildBatchTerminal')
        for child,actual in zip(batch,terminal):
            metadata=actual['metadata']
            if metadata['status']!='SUCCESS'or metadata.get('parent_id')!=parent or metadata.get('result')!='ok':raise AssertionError(('ActualSignalChildFailureOrLineage',metadata))
            outputs.append({'publication':child,'backend':actual})
    return outputs
