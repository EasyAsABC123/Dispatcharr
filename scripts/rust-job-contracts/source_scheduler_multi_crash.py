"""Actual Original multi-entry graceful/hardloss restart order, no worker.
Original Dispatcharr/Celery/django-celery-beat authors retain credit/licenses.
Timing barriers wrap real first _do_sync exactly as the prior single gate.
"""
import json,os,signal,subprocess,time
from pathlib import Path
import source_scheduler_multi as reference
assert reference.report['passed'];f=reference.f
report={'passed':False,'checks':list(reference.report['checks']),'cases':list(reference.report['cases']),'scope':'Actual Original multi-known-interval heap/elapsed Beat/sentinel/ACL plus multi-entry graceful and SIGKILL SQLsync windows; no worker/native/general calendar authority'}
process=None;sources=[]
def check(name,value):
 report['checks'].append({'name':name,'passed':bool(value)})
 if not value:raise AssertionError(name)
def state(rows):
 result=[]
 for row in rows:
  row.refresh_from_db();result.append({'name':row.name,'enabled':row.enabled,'count':row.total_run_count,'last_run_at':row.last_run_at.isoformat()if row.last_run_at else None})
 return result
def prepare(label):
 f.broker.flushdb();f.backend.flushdb();scheduler=f.new_scheduler();interval=f.IntervalSchedule.objects.create(every=3600,period=f.IntervalSchedule.SECONDS);rows=[]
 for suffix,task,args in [('z-created-first-refresh',f.refresh_epg_data,[source.pk]),('a-created-second-series',f.evaluate_series_rules,[])]:rows.append(f.create('rust-owned-multi-'+label+'-'+suffix,task.name,args,{},interval))
 scheduler.close();return rows

def start(mode,gate):
 env=dict(os.environ,SCHEDULER_CRASH_MODE=mode,SCHEDULER_CRASH_GATE=str(gate))
 with Path('/evidence/source-multi-crash-private.log').open('ab')as log:return subprocess.Popen(['/dispatcharrpy/bin/python','/job-tools/source_scheduler_crash.py','--scheduler-child'],env=env,stdout=subprocess.PIPE,stderr=log,start_new_session=True)
def restart(gate):
 global process
 process=start('restart',gate);raw,_=process.communicate(timeout=20);check('multi-restarted-Original-exit0',process.returncode==0);process=None;marker=b'ORIGINAL_SCHEDULER_CRASH_JSON=';return json.loads(next(v[len(marker):]for v in raw.splitlines()if v.startswith(marker)))
try:
 source=f.EPGSource(name='rust-owned-multi-crash-inactive',source_type='xmltv',is_active=False,custom_properties={});f.EPGSource.objects.bulk_create([source]);sources.append(source.pk)
 for mode in ['before-sync','after-sync']:
  owned=prepare('hardloss-'+mode);before=state(owned);gate=Path('/evidence/source-multi-'+mode+'.gate');assert not gate.exists();process=start(mode,gate);deadline=time.monotonic()+20
  while not gate.exists():
   if process.poll()is not None:raise RuntimeError('Multi Original exited before barrier')
   if time.monotonic()>deadline:raise TimeoutError('Multi Original sync barrier')
   time.sleep(.02)
  at_loss=state(owned);check(mode+'-first-entry-real-publication',reference.delivery()==[owned[0].name]);check(mode+'-exact-first-sync-counts',[v['count']for v in at_loss]==([0,0]if mode=='before-sync'else[1,0]));os.killpg(process.pid,signal.SIGKILL);process.wait(timeout=5);process=None;check(mode+'-hardkill-did-not-sync',state(owned)==at_loss)
  first=restart(gate);first_state=state(owned);first_names=reference.delivery();second=restart(gate);after=state(owned);names=reference.delivery()
  expected=[owned[0].name,owned[0].name,owned[1].name]if mode=='before-sync'else[v.name for v in owned]
  check(mode+'-exact-restarted-publication-order',names==expected);check(mode+'-exact-restarted-counts',[v['count']for v in after]==[1,1]);check(mode+'-exact-restarted-delays',[first['delay'],second['delay']]==([0,0]if mode=='before-sync'else[0,5]));identifiers=[v['headers']['id']for v in f.queue()];check(mode+'-distinct-publication-IDs',len(set(identifiers))==len(identifiers));report['cases'].append({'name':'multi-hardloss-'+mode,'before':before,'at_hardloss':at_loss,'first_restart':{'delay':first['delay'],'rows':first_state,'delivery_names':first_names},'after_second_restart':{'delay':second['delay'],'rows':after,'delivery_names':names},'envelopes':f.queue(),'fixture_control':'Original _do_sync barrier before/after actual call, real process group SIGKILL, fresh original scheduler processes; no handler or queue replacement.'})
 owned=prepare('graceful');scheduler=f.DatabaseScheduler(app=f.app,lazy=False,max_interval=5);first=reference.tick(scheduler);scheduler.close();first_state=state(owned);check('multi-graceful-persists-first-only',[v['count']for v in first_state]==[1,0]);gate=Path('/evidence/source-multi-graceful-unused.gate');restarted=restart(gate);check('multi-graceful-restart-picks-other-due',reference.delivery()==[v.name for v in owned]and[v['count']for v in state(owned)]==[1,1]and restarted['delay']==0);report['cases'].append({'name':'multi-graceful','first':first,'after_close':first_state,'after_restart':state(owned),'envelopes':f.queue()});check('multi-no-worker-or-reservation',f.backend.dbsize()==0 and f.broker.hlen('unacked')==0);report['passed']=True
finally:
 if process is not None and process.poll()is None:os.killpg(process.pid,signal.SIGKILL);process.wait(timeout=5)
 f.EPGSource.objects.filter(pk__in=sources).delete();f.broker.flushdb();f.backend.flushdb();report['queue_and_results_after_cleanup']=[f.broker.dbsize(),f.backend.dbsize()];Path('/evidence/source-scheduler-multi-crash-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
