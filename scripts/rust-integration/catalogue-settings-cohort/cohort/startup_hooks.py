"""Candidate15 additive hooks for the existing grouped fixture; no apps on import.
Source tasks and production imports are untouched. Original authors retain credit.
"""
import datetime,json,time
from pathlib import Path
PLUGIN='apps.plugins.tasks.refresh_plugin_repos'
STARTUPS={PLUGIN,'apps.channels.tasks.recover_recordings_on_startup','core.tasks.check_for_version_update'}
class StartupInfrastructureFailure(Exception):
 batch_infrastructure_failure=True
def owned_origin(f):return 'http://11.254.'+('252'if f.backend=='Native'else'251')+'.3:8080/owned-plugin-empty.json'
def prepare_context(fixture):
 # Called after disposable clone/reset, BEFORE either actual app starts. Fence
 # every default repository, then enable only an attested owned Origin in the
 # dedicated ETA contexts. No public repository can be contacted.
 dedicated=fixture.case.startswith('plugin-eta-');origin=owned_origin(fixture)
 fixture._plugin_environment={**getattr(fixture,'_plugin_environment',{}),**({'RUST_EXPERIMENTAL_SELECTED_BEAT':'0'}if dedicated else{})}
 # No direct external business HTTPS: Source/Native version paths catch a real
 # refused scoped proxy connection. Owned local HTTP and the provider's owned
 # CONNECT proxy remain usable; no production SSRF validator is bypassed.
 closed=fixture.python("import socket,json;s=socket.socket();s.settimeout(.2);value=s.connect_ex(('127.0.0.1',9));s.close();print(json.dumps({'loopback_proxy_port':9,'actual_connect_ex':value}))")
 closed=json.loads(closed)
 if closed['actual_connect_ex']!=111:raise RuntimeError('ExpectedClosedOwnedLoopbackProxyPort')
 environment=fixture._provider_environment
 for name in ['HTTP_PROXY','HTTPS_PROXY','ALL_PROXY','http_proxy','https_proxy','all_proxy']:environment.setdefault(name,'http://127.0.0.1:9')
 octet='252'if fixture.backend=='Native'else'251'
 owned=['localhost','127.0.0.1','::1','baseline','origin','11.254.'+octet+'.2','11.254.'+octet+'.3']
 for name in ['NO_PROXY','no_proxy']:
  existing=[value for value in environment.get(name,'').split(',')if value]
  environment[name]=','.join(dict.fromkeys(existing+owned))
 fixture._business_network_fence={'actual_closed_proxy':closed,'owned_NO_PROXY':owned,'provider_owned_proxy_preserved':fixture.case in('recording-provider-artwork','vod-ingestion'),'scope':'Actual application env; no HTTP-client/function replacement or Source SSRF bypass'}
 fixture._plugin_environment.update({key:value for key,value in environment.items()if key in ['HTTP_PROXY','HTTPS_PROXY','ALL_PROXY','http_proxy','https_proxy','all_proxy','NO_PROXY','no_proxy']})

 code="from apps.plugins.models import PluginRepo;from core.models import CoreSettings;PluginRepo.objects.all().update(enabled=False);"
 if dedicated:code+="PluginRepo.objects.update_or_create(url="+repr(origin)+",defaults={'name':'Owned ETA repository','is_official':False,'enabled':True,'public_key':''});"
 # Existing business contexts require quiet Beat; plugin startup must still run.
 # Zero disables ONLY plugin periodic production, preserving natural startup.
 interval={'plugin-eta-fifo':None,'plugin-eta-warm':0,'plugin-eta-crash':-8}.get(fixture.case,0)
 if interval is None:code+="CoreSettings.objects.filter(key='plugin_repo_settings').delete();"
 else:code+="s,_=CoreSettings.objects.get_or_create(key='plugin_repo_settings',defaults={'name':'Plugin Repository Settings','value':{}});s.value={'refresh_interval_hours':"+str(interval)+"};s.save();"
 code+="print(json.dumps({'enabled_repositories':list(PluginRepo.objects.filter(enabled=True).values('id','url','is_official')),'refresh_interval_input':"+repr('missing'if interval is None else interval)+"}))"
 fixture._plugin_periodic_expected=None if interval==0 else{'every':6 if interval is None else 1,'enabled':interval is None or interval>0}
 fixture._plugin_origin_before=json.loads(fixture.get(fixture.origin+'/stats')[1]).get('plugin_manifest_requests',0)
 proof=fixture.sql(code)
 if any(row['url']!=origin for row in proof['enabled_repositories']):raise ValueError('UnfencedRepository')
 if dedicated:
  inspections=json.loads(fixture.command([fixture.docker,'inspect',fixture.project+'-origin-1']))
  networks=inspections[0]['NetworkSettings']['Networks']
  if not any(row.get('IPAddress')==('11.254.'+('252'if fixture.backend=='Native'else'251')+'.3')for row in networks.values()):raise ValueError('OwnedPublicShapeOriginNotAttested')
 fixture._plugin_fence=proof
 # Observer output is bounded private fixture data, not a Source/evidence bind.
 root=fixture.case_data/'plugin-startup-observer'
 fixture.python("import os,pathlib;p=pathlib.Path("+repr('/shared/'+fixture.case+'/plugin-startup-observer')+");p.mkdir(mode=0o755);p.chmod(0o755);os.chown(p,65532,65532)")
 fixture._plugin_startup_root=root
 fixture.role('plugin-startup-observer',['/dispatcharrpy/bin/python','/normal-dvr-tools/startup_observer.py'],extra={'CHANNEL_OBSERVER_ROOT':'/shared/'+fixture.case+'/plugin-startup-observer','CHANNEL_FUNCTIONAL_DEADLINE':str(fixture.context['deadline_monotonic'])})
 def ready():
  record=fixture.roles['plugin-startup-observer']
  script="import json,sys;sys.path.insert(0,'/normal-dvr-tools');from role_owner import rpc;value=rpc("+repr(record)+",'status');print(json.dumps(value))"
  actual=json.loads(fixture.python(script))
  if not actual['running']:
   proof={'scope':'actual observer died before application startup','actual_exit':actual['actual_exit'],'actual_pid':actual['pid'],'actual_generation':actual['starttime']}
   from workflow import save
   save(fixture.output/(fixture.case+'-startup-observer-infrastructure-failure.json'),proof)
   raise StartupInfrastructureFailure(('ActualStartupObserverExitedBeforeReady',proof))
  path=root/'ready.json'
  if not path.is_file():return False
  observed=json.loads(path.read_text())
  if observed.get('actual_euid')!=65532 or observed.get('actual_egid')!=65532 or observed['pid']!=record['pid']or observed['generation']!=record['starttime']or observed['actual_MONITOR_subscribed']is not True:raise StartupInfrastructureFailure('ActualStartupObserverIdentityRequired')
  return observed
 try:fixture._plugin_observer_ready=fixture.wait(ready,10,'actual-startup-MONITOR-ready')
 except TimeoutError as error:raise StartupInfrastructureFailure('ActualStartupObserverReadyDeadline')from error
def snapshot(fixture):
 path=fixture._plugin_startup_root/'snapshot.json'
 if not path.is_file():return None
 value=json.loads(path.read_text())
 if value['journal_error']:raise RuntimeError('StartupObserverFailed:'+value['journal_error'])
 return value

def collect(fixture,*,stop=True):
 event_native=fixture.backend=='Native'and fixture.case in('recording-plugin-characterization','recording-connect-boundaries')
 limited_native=fixture.backend=='Native'and fixture.case in('m8-actions','recording-plugin-characterization','recording-connect-boundaries')
 expected_tasks=STARTUPS-{'apps.channels.tasks.recover_recordings_on_startup'}if limited_native else STARTUPS
 def capability():
  if not limited_native:return None
  status,raw=fixture.get(fixture.base_url+'/health/ready');assert len(raw)<=65536
  value=json.loads(raw);selected=value.get('selected_workers',{})
  assert status==200 and selected.get('ready')is True
  assert selected.get('dvr_plugin_domain')==('empty_directory_rechecked_before_recording_effects'if event_native else'unsupported_configured_plugin_hooks')
  assert selected.get('startup_recovery_deferred_unsupported_domain')is True
  assert fixture.redis(0,"return redis.call('GET','dvr:recover_dispatch_lock')")is None
  return {'actual_health_status':status,'selected_workers':selected,'expected_actual_startup_roots':2,'Source_three_root_parity':False,'DVR_plugin_domain_supported':event_native,'recovery_root_published':False,'scope':'Empty plugin recording Connect context; recovery explicitly deferred without root/result/lock.'if event_native else'Idle startup capability only; unsupported recording work must be refused separately.'}
 domain_capability=capability()
 def terminal():
  captured=snapshot(fixture)
  if not captured:return False
  frames=captured['frames'];roots={row['frame']['headers']['id']:row for row in frames}
  tasks=[row['frame']['headers']['task']for row in roots.values()]
  if len(frames)!=len(expected_tasks) or len(roots)!=len(expected_tasks) or set(tasks)!=expected_tasks:return False
  backend={}
  for ident in roots:
   raw=fixture.redis(2,"return redis.call('GET',ARGV[1])",'celery-tasks:celery-task-meta-'+ident)
   if not raw:return False
   value=json.loads(raw)
   if value.get('status')!='SUCCESS'or value.get('task_id')!=ident:return False
   backend[ident]={'metadata':value,'actual_key':'celery-tasks:celery-task-meta-'+ident}
  queues=fixture.redis(1,"return {redis.call('LLEN','celery'),redis.call('LLEN','dvr'),redis.call('HLEN','unacked'),redis.call('ZCARD','unacked_index')}")
  if queues!=[0,0,0,0]:return False
  row=next(row for row in roots.values()if row['frame']['headers']['task']==PLUGIN)
  frame=row['frame'];eta=datetime.datetime.fromisoformat(frame['headers']['eta']).timestamp();delay=eta-float(row['actual_redis_time'])
  if not 0<delay<=10.1:raise AssertionError(('ActualStartupCountdownVsPublication',delay))
  if backend[frame['headers']['id']]['metadata']['result']is not None:raise AssertionError('PluginTaskReturnNotNull')
  return {'passed':True,'root_ids':sorted(roots),'backend':backend,'queues':queues[:2],'unacked_counts':queues[2:],'actual_startup_frames':list(roots.values()),'plugin_countdown_at_LPUSH_seconds':delay,'repository_fence':fixture._plugin_fence,'business_network_fence':fixture._business_network_fence,'observer_actual_UID_proof':fixture._plugin_observer_ready,'scope':'Actual natural startup publications and terminal results; empty-directory Native Connect mode explicitly defers unbound recovery and has no Source three-root parity.'if event_native else'Actual natural startup publications and terminal results; limited Native populated-plugin mode explicitly defers recovery and has no Source three-root parity.'if limited_native else'Three actual natural startup publications and terminal results; task-specific broader effect authority remains separate.','actual_startup_root_count':len(roots),'DVR_domain_capability':domain_capability}
 result=fixture.wait(terminal,60,'actual-declared-startup-roots-terminal-before-business')
 if stop:
  result['observer_role_stop']=fixture.role('plugin-startup-observer',[],stop=True)
  final=json.loads((fixture._plugin_startup_root/'final.json').read_text())
  if not(final['terminal_barrier_observed']and final['thread_settled']and final['writer_closed']and not final['terminal_reader_error']and final['observer_error']is None):raise AssertionError(('StartupObserverNotSettled',final))
  result['observer_final']=final
 return result
