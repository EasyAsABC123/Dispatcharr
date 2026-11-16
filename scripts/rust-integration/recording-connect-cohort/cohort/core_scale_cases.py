"""Scale context for the existing real Core PATCH shared-fixture callbacks.
Original Dispatcharr/dependency authors retain full application/protocol credit.
Import starts no applications. Root must declare matching pids1600 BEFORE boot.
"""
import hashlib
import importlib.util
import json
from pathlib import Path
import threading
import time
import uuid


def run(fixture):
    if fixture.case!='core-produced':raise ValueError('SharedGroupedM6ContextRequired')
    module_path=Path(fixture.binding['core_cases'])
    expected=fixture.binding['core_cases_sha256']
    if hashlib.sha256(module_path.read_bytes()).hexdigest()!=expected:raise ValueError('ExactCoreCallbackRequired')
    spec=importlib.util.spec_from_file_location('owned_core_scale_cases',module_path);core=importlib.util.module_from_spec(spec);spec.loader.exec_module(core)
    m6=core.base(fixture);out=m6.directory(fixture)/'scale';out.mkdir(parents=True,exist_ok=False)
    # Read only the declared cgroup limits, not complete inspect/env output.
    baseline=fixture.command(fixture.compose+['ps','-q','baseline']).strip()
    limits={'baseline':int(fixture.command([fixture.docker,'inspect','--format','{{.HostConfig.PidsLimit}}',baseline]))}
    if fixture.native:limits['Native']=int(fixture.command([fixture.docker,'inspect','--format','{{.HostConfig.PidsLimit}}',fixture.native]))
    if any(value!=1600 for value in limits.values()):raise ValueError(('PairedFunctionalPids1600RequiredBeforeBoot',limits))
    nonce=uuid.uuid4().hex[:10]
    seed_script=("""
from apps.m3u.models import M3UAccount
from apps.channels.models import Stream
from django.utils import timezone
from datetime import timedelta
import hashlib
account=M3UAccount.objects.filter(file_path__startswith='/data/m3us/').first()
if account is None:
 account=M3UAccount(name='Core scale input __NONCE__',account_type='STD',is_active=False,file_path='/data/m3us/core-scale.m3u',custom_properties={});M3UAccount.objects.bulk_create([account])
M3UAccount.objects.bulk_create([M3UAccount(name='Core scale __NONCE__-'+str(i),account_type='STD',is_active=False,custom_properties={})for i in range(1100)],batch_size=256)
Stream.objects.bulk_create([Stream(name='Scale stream __NONCE__-'+str(i),url=None if i%2 else 'http://owned.invalid/scale/'+str(i),tvg_id=None,m3u_account=account,stream_hash=hashlib.sha256(('scale-old-__NONCE__-'+str(i)).encode()).hexdigest())for i in range(5000)],batch_size=1000)
start=timezone.now()+timedelta(days=3)
Recording.objects.bulk_create([Recording(channel_id="""+str(fixture.channel_id)+""",start_time=start,end_time=start+timedelta(minutes=10),custom_properties={'core_scale':'__NONCE__','program':{'title':'__CHANNEL_NAME__','start_time':(start+timedelta(minutes=1)).isoformat(),'end_time':(start+timedelta(minutes=11)).isoformat()}})for _ in range(1025)],batch_size=256)
Recording.objects.bulk_create([Recording(channel_id="""+str(fixture.channel_id)+""",start_time=start,end_time=start+timedelta(minutes=10),custom_properties={'core_scale_manual':'__NONCE__'})for _ in range(17)],batch_size=256)
print(json.dumps({'accounts':M3UAccount.objects.count(),'streams':Stream.objects.count(),'future_recordings':Recording.objects.filter(start_time__gt=timezone.now()).count(),'scope':'bulk SQL context only; no booking/refresh producer claim for seeded rows'}))
""")
    seed=fixture.sql(seed_script.replace('__NONCE__',nonce).replace(repr('__CHANNEL_NAME__'),repr(fixture.channel_name)))
    m6.save(out/'declared-inputs.json',{'limits':limits,'seed':seed,'performance_claimed':False,'Source_thread_prerequisite':'Original starts one renewal OS thread per locked account; actual consumption is observed, not assumed'})
    ledger='/shared/'+fixture.case+'/core-scale-threads.jsonl'
    script="""import json,time,signal,os
from pathlib import Path
running=[True]
signal.signal(signal.SIGTERM,lambda *_:running.__setitem__(0,False))
path=Path("""+repr(ledger)+""")
deadline=time.monotonic()+180
with path.open('x')as output:
 while running[0]and time.monotonic()<deadline:
  rows=[]
  for p in Path('/proc').glob('[0-9]*/status'):
   try:
    fields=dict(line.split(':',1)for line in p.read_text().splitlines()if ':'in line)
    rows.append({'pid':int(p.parent.name),'name':fields.get('Name','').strip(),'threads':int(fields['Threads'])})
   except(FileNotFoundError,ProcessLookupError,PermissionError,KeyError):pass
  output.write(json.dumps({'epoch':time.time(),'processes':rows})+'\\n');output.flush()
  if output.tell()>4194304:raise RuntimeError('ActualThreadObserverBudget')
  time.sleep(.05)
"""
    if fixture.backend=='Original':raise ValueError('SourceCoreReplayNotAuthorizedInContinuation')
    stopping=threading.Event();native=[];thread=None
    if fixture.native:
        hostpid=int(fixture.command([fixture.docker,'inspect','--format','{{.State.Pid}}',fixture.native]))
        def sample():
            while not stopping.wait(.25):
                try:native.append({'epoch':time.time(),'host_pid':hostpid,'threads':sum(1 for _ in Path('/proc',str(hostpid),'task').iterdir())})
                except(FileNotFoundError,PermissionError):native.append({'epoch':time.time(),'unreadable_or_absent':True})
                if len(native)>7200:return
        thread=threading.Thread(target=sample,daemon=True);thread.start()
    try:
        ledger='/shared/'+fixture.case+'/m6-core-publications.jsonl'
        fixture.role('m6-core-publications',['/dispatcharrpy/bin/python','/normal-dvr-tools/core_broker_monitor.py'],extra={'NORMAL_DVR_LEDGER':ledger})
        cells=[]
        try:
            fixture.wait(lambda:(fixture.case_data/'m6-core-publications.ready').exists(),10,'ActualCoreScalePublicationObserver')
            for name,callback in [('core-rehash-scale',core.rehash),('core-offsets-scale',core.offsets)]:
                cell={'name':name,'status':'running','began_epoch':time.time()};m6.save(out/(name+'.json'),cell)
                try:
                    if fixture.backend=='Native':
                        route_path=Path(fixture.binding['core_route_regression'])
                        if hashlib.sha256(route_path.read_bytes()).hexdigest()!=fixture.binding['core_route_regression_sha256']:raise ValueError('ExactCoreRouteRegressionRequired')
                        route_spec=importlib.util.spec_from_file_location('owned_core_route_regression',route_path);route=importlib.util.module_from_spec(route_spec);route_spec.loader.exec_module(route)
                        observation=route.run(fixture,core,m6,callback)
                    else:observation=callback(fixture,m6)
                    cell.update(observation=observation,status='passed')
                except Exception as error:cell.update(status='failed',exception=type(error).__name__,message=str(error))
                finally:
                    # A count/minimal-field observation avoids the old observer's
                    # full-model JSON exceeding its one-MiB command pipe budget.
                    try:cell['after_counts']=fixture.sql("from apps.m3u.models import M3UAccount;from apps.channels.models import Stream;from django.utils import timezone;print(json.dumps({'accounts':M3UAccount.objects.count(),'streams':Stream.objects.count(),'future':Recording.objects.filter(start_time__gt=timezone.now()).count()}))")
                    except Exception as error:cell.update(status='failed',observation_error=repr(error))
                    cell['ended_epoch']=time.time();m6.save(out/(name+'.json'),cell)
                cells.append(cell)
            shifted_path=Path(fixture.binding['core_shifted_past_case'])
            if hashlib.sha256(shifted_path.read_bytes()).hexdigest()!=fixture.binding['core_shifted_past_case_sha256']:raise ValueError('ExactShiftedPastCallbackRequired')
            shifted_spec=importlib.util.spec_from_file_location('owned_core_shifted_past',shifted_path);shifted=importlib.util.module_from_spec(shifted_spec);shifted_spec.loader.exec_module(shifted)
            cell={'name':'core-offset-shifted-past','began_epoch':time.time()}
            try:cell.update(observation=shifted.run(fixture,core,m6),status='passed')
            except Exception as error:cell.update(status='failed',exception=type(error).__name__,message=str(error))
            cell['ended_epoch']=time.time();cells.append(cell);m6.save(out/'core-offset-shifted-past.json',cell)
        finally:fixture.role('m6-core-publications',[],stop=True)
        offset_cells=[c for c in cells if c['name']=='core-offsets-scale'and c['status']=='passed']
        if offset_cells:
            observed=offset_cells[0]['observation']['task']['backend']['metadata']['result']
            if observed['scanned']<=1024 or observed['changed']<1026:raise AssertionError(('ActualSourceOrderCursorDidNotCrossCutoffs',observed))
        checked=fixture.sql("from apps.channels.models import Stream;import hashlib;count=0;bad=[];\nfor row in Stream.objects.filter(name__startswith="+repr('Scale stream '+nonce+'-')+").values('name','url','stream_hash').iterator(chunk_size=256):\n expected=hashlib.sha256(json.dumps({'name':row['name'],'url':row['url']},sort_keys=True).encode()).hexdigest();count+=1\n if row['stream_hash']!=expected and len(bad)<8:bad.append({'name':row['name'],'actual':row['stream_hash'],'expected':expected})\nprint(json.dumps({'checked':count,'mismatches':bad}))")
        schedules=fixture.sql("from django_celery_beat.models import PeriodicTask;from django.utils import timezone;rows=Recording.objects.filter(custom_properties__core_scale="+repr(nonce)+");print(json.dumps({'epg_rows':rows.count(),'scheduled_rows':rows.exclude(task_id__isnull=True).exclude(task_id='').count(),'periodics':PeriodicTask.objects.filter(name__in=['dvr-recording-'+str(pk)for pk in rows.values_list('id',flat=True)]).count(),'manual_unchanged':Recording.objects.filter(custom_properties__core_scale_manual="+repr(nonce)+",task_id__isnull=True).count()}))")
        journal=fixture.redis(0,"local out={};local c='0';repeat local r=redis.call('SCAN',c,'MATCH','rust:rehash:owned:*','COUNT',256);c=r[1];for _,key in ipairs(r[2])do out[#out+1]=key end until c=='0';return out")
        readiness=fixture.api('GET','/health/ready',token=None)
        m6.save(out/'actual-selected-readiness-after-Core.json',readiness)
        report={'status':'passed'if all(c['status']=='passed'for c in cells)and checked['checked']==5000 and not checked['mismatches']and not journal and readiness['status']==200 and schedules=={'epg_rows':1025,'scheduled_rows':1025,'periodics':1025,'manual_unchanged':17}else'failed','cells':cells,'exact_null_and_string_hash_checks':checked,'internal_journal_after_terminal':journal,'actual_scaled_row_schedules':schedules,'selected_readiness':readiness}
    finally:
        stopping.set()
        if thread is not None:
            thread.join(5)
            if thread.is_alive():raise RuntimeError('ActualNativeThreadObserverUnsettled')
        m6.save(out/'actual-Native-threads.json',native)
    rows=[] # Source thread evidence is retained; no redundant baseline observer.
    peaks=max((row['threads']for sample in rows for row in sample['processes']),default=0)
    original_required=fixture.backend=='Original'
    thread_observed=not original_required or peaks>1024
    result={'status':'passed'if report['status']=='passed'and thread_observed else'failed','core':report,'actual_max_single_process_threads_in_baseline':peaks,'Source1025_plus_threads_observed':thread_observed if original_required else None,'scope':'paired functional scale only, not equivalent-budget performance measurement','performance_verified':False}
    m6.save(out/'report.json',result)
    return result
