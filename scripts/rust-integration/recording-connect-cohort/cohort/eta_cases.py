"""Candidate15 ETA cells inside the existing grouped fixture and absolute lease.
Only actual natural startup publications are scheduled; no fabricated task wire.
Original Dispatcharr/Celery/Kombu authors retain all algorithm/application credit.
"""
import datetime,json,time
from startup_hooks import PLUGIN,snapshot
from workflow import save

def _raw(f,db,key):
 value=f.redis(db,"return redis.call('GET',ARGV[1])",key)
 return json.loads(value)if value else None

def _plugin_frame(f,exclude=()):
 def observed():
  snap=snapshot(f)
  if not snap:return False
  values=[r for r in snap['frames']if r['frame']['headers']['task']==PLUGIN and r['frame']['headers']['id']not in exclude]
  return values[0]if values else False
 return f.wait(observed,5,'actual-natural-plugin-publication')
def _reserved(f,row):
 frame=row['frame'];tag=frame['properties']['delivery_tag'];ident=frame['headers']['id']
 def state():
  result=f.redis(1,"return {redis.call('HGET','unacked',ARGV[1]),redis.call('ZSCORE','unacked_index',ARGV[1]),redis.call('LLEN','celery')}",tag)
  if not result[0]:return False
  value=json.loads(result[0]);backend=_raw(f,2,'celery-tasks:celery-task-meta-'+ident)
  if value[0]['headers']['id']!=ident or value[1:]!=['','celery']:raise AssertionError('UnownedPluginReservation')
  if backend is not None:raise AssertionError(('PluginBackendBeforeDue',backend))
  return {'task_id':ident,'tag':tag,'stored_tuple':result[0],'score':float(result[1]),'queue_length':result[2],'backend_absent':True,'eta':frame['headers']['eta'],'actual_observed_wall':time.time()}
 return f.wait(state,5,'actual-plugin-unacked-before-due')
def _terminal(f,ident):
 def success():
  value=_raw(f,2,'celery-tasks:celery-task-meta-'+ident)
  if not value:return False
  if value.get('status')!='SUCCESS'or value.get('task_id')!=ident or value.get('result')is not None:raise AssertionError(('ActualPluginTerminal',value))
  return value
 return f.wait(success,30,'actual-plugin-terminal-'+ident)
def _repo(f):
 return f.sql("from apps.plugins.models import PluginRepo;print(json.dumps(list(PluginRepo.objects.filter(enabled=True).values('id','name','url','cached_manifest','signature_verified','last_fetch_status'))))")
def _stop_observer(f):
 stopped=f.role('plugin-startup-observer',[],stop=True);receipt=json.loads((f._plugin_startup_root/'final.json').read_text())
 if not(receipt['terminal_barrier_observed']and receipt['thread_settled']and receipt['writer_closed']and not receipt['terminal_reader_error']and receipt['observer_error']is None):raise AssertionError(('ActualETAObserverStop',receipt))
 return {'role':stopped,'journal':receipt}
def _stop_app(f,crash=False):
 if f.backend=='Native':
  if crash:f.command([f.docker,'kill','--signal','KILL',f.native],10)
  return f.stop_native(expected=137 if crash else 0)
 if crash:
  f.role('beat',[],stop=True)
  record=f.roles.pop('celery');value=json.loads(f.python("import json,sys;sys.path.insert(0,'/normal-dvr-tools');from role_owner import crash;print(json.dumps(crash("+repr(record)+")))"))
  if value['actual_exit']!=-9 or not value['all_original_process_generations_absent']:raise AssertionError(('ActualCeleryCrash',value))
  f.collect_role_logs('celery',value);return value
 receipts=[]
 for role in ['beat','celery','dvr','web']:
  value=f.role(role,[],stop=True)
  if value is not None:receipts.append(value)
 return {'actual_role_stops':receipts,'scope':'Source web/default/DVR/Beat are separately supervised, unlike combined Native Main'}
def _restart(f,crash=False):
 if f.backend=='Native':f.start_native();return
 if not crash:
  f.start_original(consumer=False)
  f.role('celery',['/dispatcharrpy/bin/celery','-A','dispatcharr','worker','--pool=prefork','--concurrency=1','-Q','celery','--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING','--hostname=normal-default@owned']);return
 f.role('celery',['/dispatcharrpy/bin/celery','-A','dispatcharr','worker','--pool=prefork','--concurrency=1','-Q','celery','--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING','--hostname=normal-default@owned'])

def run(f):
 result={'passed':False,'mode':f.case,'Source_API_replays':0,'synthetic_task_frames':0,'physical_main_global6_unmeasured':True,'scope':'Real natural startup ETA/owned unsigned manifest; other plugin transport domains and full QoS remain open','business_network_fence':f._business_network_fence}
 path=f.output/(f.case+'-observations.json');save(path,result)
 try:
  if f.backend=='Original'and'celery'not in f.roles:
   f.role('celery',['/dispatcharrpy/bin/celery','-A','dispatcharr','worker','--pool=prefork','--concurrency=1','-Q','celery','--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING','--hostname=normal-default@owned'])
  row=_plugin_frame(f);receipt=_reserved(f,row);result.update(initial_publication=row,initial_reservation=receipt)
  if datetime.datetime.fromisoformat(receipt['eta']).timestamp()<=receipt['actual_observed_wall']:raise AssertionError('NoActualFutureETAWindow')
  schedule=f.sql("from django_celery_beat.models import PeriodicTask;print(json.dumps(list(PeriodicTask.objects.filter(name='plugin-repo-refresh-task').values('name','task','enabled','interval__every','interval__period','kwargs'))))")
  expected=f._plugin_periodic_expected
  if expected is None:
   if schedule:raise AssertionError(('ActualPluginZeroIntervalNotDeleted',schedule))
  elif len(schedule)!=1 or schedule[0]['task']!=PLUGIN or schedule[0]['interval__every']!=expected['every']or schedule[0]['interval__period']!='hours'or schedule[0]['enabled']!=expected['enabled']:raise AssertionError(('ActualPluginPeriodicSetup',schedule,expected))
  before=json.loads(f.get(f.origin+'/stats')[1]).get('plugin_manifest_requests',0)
  if before!=f._plugin_origin_before:raise AssertionError(('ManifestFetchBeforeETA',before,f._plugin_origin_before))
  result['actual_origin_before_eta']=before
  result['actual_periodic_setup']=schedule;result['Native_Beat_disabled_for_independence']=f.backend=='Native'
  if f.case=='plugin-eta-fifo':
   # Actual existing HTTP producer; Source98 already qualifies this route shape.
   seed=f.sql("from apps.channels.models import Channel;c=Channel.objects.first();c=c or Channel.objects.create(name='Owned ETA later',channel_number=101);print(json.dumps({'channel':c.pk}))")
   from channel_tools.absolute_http import Observation
   observation=Observation(f.base_url,seconds=min(5,f.remaining(5)))
   primary=None
   try:
    response=observation.request('/api/channels/channels/set-names-from-epg/',{'Content-Type':'application/json','Authorization':'Bearer '+f.accounts['admin']['access']},method='POST',body=json.dumps({'channel_ids':[seed['channel']]}).encode())
    body=response.raw.read(4097);result['later_HTTP']={'status':response.status_code,'body':body.decode('utf8'),'absolute_completion':observation.complete()};save(path,result)
   except BaseException as error:primary=error;raise
   finally:
    try:result['later_HTTP_physical_close']=observation.cancel();save(path,result)
    except BaseException as error:
     result['later_HTTP_close_error']={'type':type(error).__name__,'primary_preserved':primary is not None};save(path,result)
     if primary is None:raise
   if result['later_HTTP']['status']!=200:raise AssertionError(('ActualLaterHTTP',result['later_HTTP']))
   later=json.loads(body)['task_id']
   def later_done():
    value=_raw(f,2,'celery-tasks:celery-task-meta-'+later)
    if not value:return False
    if value.get('status')!='SUCCESS':raise AssertionError(('LaterTaskFailure',value))
    return value
   result['later_actual_terminal']=f.wait(later_done,5,'later-real-HTTP-task-not-blocked-by-future-plugin')
   result['later_completed_before_eta']=time.time()<datetime.datetime.fromisoformat(receipt['eta']).timestamp()
   if not result['later_completed_before_eta']:raise AssertionError('LaterTaskDidNotCompleteBeforeETA')
  else:
   crash=f.case=='plugin-eta-crash';result['actual_stop']=_stop_app(f,crash=crash)
   state=f.redis(1,"return {redis.call('HGET','unacked',ARGV[1]),redis.call('ZSCORE','unacked_index',ARGV[1]),redis.call('LRANGE','celery',0,31)}",receipt['tag']);result['after_stop']=state;save(path,result)
   if crash:
    if state[0]!=receipt['stored_tuple']or state[1]is None:raise AssertionError('CrashLostDurableReservation')
    aged=time.time()-3601
    updated=f.redis(1,"if redis.call('HGET','unacked',ARGV[1])~=ARGV[2]then return 0 end;redis.call('ZADD','unacked_index',ARGV[3],ARGV[1]);return 1",receipt['tag'],receipt['stored_tuple'],aged)
    if updated!=1:raise AssertionError('ActualOwnedVisibilityScoreNotUpdated')
    result['controlled_input_age']={'original':receipt['score'],'new':aged,'actual_wall':time.time(),'scope':'Explicit disposable Redis input aging to test3600s predicate; does not claim one hour elapsed'}
   else:
    restored=[json.loads(raw)for raw in state[2]if json.loads(raw)['headers']['id']==receipt['task_id']]
    if state[0]is not None or state[1]is not None or len(restored)!=1:raise AssertionError('WarmRestoreNotActual')
    if restored[0]['headers'].get('redelivered')is not True or restored[0]['properties']['delivery_info'].get('redelivered')is not True:raise AssertionError('WarmRestoreFlags')
    result['actual_redelivered_frame']=restored[0]
   _restart(f,crash=crash)
  result['actual_old_plugin_terminal']=_terminal(f,receipt['task_id'])
  # Native restarts naturally publish another root. Source consumer-only crash
  # does not restart its serving web process; do not invent an equivalent root.
  captured=snapshot(f);plugin_ids={r['frame']['headers']['id']for r in captured['frames']if r['frame']['headers']['task']==PLUGIN}
  result['actual_all_plugin_terminals']={ident:_terminal(f,ident)for ident in plugin_ids}
  rows=_repo(f);result['actual_repository_effects']=rows
  if len(rows)!=1 or rows[0]['last_fetch_status']!='200'or rows[0]['name']!='Owned ETA repository'or rows[0]['cached_manifest']!={'manifest':{'registry_name':'Owned ETA repository','plugins':[]}}:raise AssertionError(('OwnedManifestEffects',rows))
  after=json.loads(f.get(f.origin+'/stats')[1]).get('plugin_manifest_requests',0)
  result['actual_origin_after']=after;result['actual_origin_request_delta']=after-f._plugin_origin_before
  if result['actual_origin_request_delta']!=len(plugin_ids):raise AssertionError(('ActualManifestRequestCountVsNaturalRoots',result['actual_origin_request_delta'],len(plugin_ids)))
  result['observer_stop']=_stop_observer(f);result['passed']=True;save(path,result);return result
 except BaseException as error:
  result['primary_error']={'type':type(error).__name__,'message':str(error)[:2048]};save(path,result);raise
