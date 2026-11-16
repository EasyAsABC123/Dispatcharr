"""Actual Main channel API/worker acceptance callback; no dispatch on import.
Original Dispatcharr and dependency authors retain all credit and licenses.
"""
import base64,datetime,hashlib,importlib.util,json,re,time,uuid
from pathlib import Path
HERE=Path(__file__).resolve().parent
TASKS={'normal_names':'set_channels_names_from_epg','normal_tvg':'set_channels_tvg_ids_from_epg','normal_logos':'set_channels_logos_from_epg','normal_bulk':'bulk_create_channels_from_streams'}
ROUTES={'normal_names':'set-names-from-epg/','normal_tvg':'set-tvg-ids-from-epg/','normal_logos':'set-logos-from-epg/','normal_bulk':'from-stream/bulk/'}
TABLES=['dispatcharr_channels_channel','dispatcharr_channels_logo','dispatcharr_channels_channelstream','dispatcharr_channels_channelprofilemembership']
def module(name):
 spec=importlib.util.spec_from_file_location('dispatcharr_channel_acceptance_'+name,HERE/(name+'.py'));value=importlib.util.module_from_spec(spec);spec.loader.exec_module(value);return value
def save(p,v):
 t=p.with_suffix(p.suffix+'.pending');t.write_text(json.dumps(v,indent=2,default=str)+'\n');t.replace(p)
def utc():return datetime.datetime.now(datetime.timezone.utc).isoformat()
def normalize_events(decoded,ident):
 out=[]
 for item in decoded:
  data=item.get('data',{});kind=data.get('type','')
  if data.get('task_id')==ident or kind=='channels_created':
   item=json.loads(json.dumps(item));item.pop('__asgi_channel__',None)
   if 'task_id'in item.get('data',{}):item['data']['task_id']='ACTUAL_BOUND_TASK_ID'
   out.append(item)
 return out

def actual_three_startup_roots(receipts):
 """Read retained actual publication identities/results; no new application call."""
 tasks={'apps.channels.tasks.recover_recordings_on_startup','core.tasks.check_for_version_update','apps.plugins.tasks.refresh_plugin_repos'}
 if receipts.get('passed')is not True or receipts.get('queues')!=[0,0]or receipts.get('unacked_counts')!=[0,0]:return False
 roots=receipts.get('root_ids',[]);backend=receipts.get('backend',{});frames=receipts.get('actual_startup_frames',[])
 if len(roots)!=3 or len(set(roots))!=3 or set(roots)!=set(backend)or len(frames)!=3:return False
 seen=set();kinds=set()
 for row in frames:
  frame=row.get('frame',{});headers=frame.get('headers',{});properties=frame.get('properties',{});ident=headers.get('id');task=headers.get('task')
  if ident not in backend or ident in seen or task not in tasks or task in kinds:return False
  if headers.get('root_id')!=ident or headers.get('parent_id')is not None or properties.get('correlation_id')!=ident:return False
  result=backend[ident];metadata=result.get('metadata',{})
  if result.get('actual_key')!='celery-tasks:celery-task-meta-'+ident or metadata.get('task_id')!=ident or metadata.get('status')!='SUCCESS':return False
  if task=='apps.plugins.tasks.refresh_plugin_repos'and(metadata.get('result')is not None or not isinstance(headers.get('eta'),str)):return False
  seen.add(ident);kinds.add(task)
 return seen==set(roots)and kinds==tasks

def run(fixture,startup_receipts,authority,http_authority,terminate=False):
 """Call only inside a DEDICATED fresh Fixture.independent_context for Native.
 Caller owns the single dispatcher slot/deadline/candidate image and final exact
 fixture/default cleanup. No Source HTTP or task function is invoked here.
 """
 if fixture.backend!='Native'or fixture.native is None:raise ValueError('ActualNativeMainRequired')
 if not actual_three_startup_roots(startup_receipts):raise ValueError('ActualThreeNaturalStartupRootsTerminalBeforeBusinessRequired')
 source=json.loads(Path(authority).read_bytes());references={r['name']:r for r in source['cases']};assert source['case_count']==9
 result={'passed':False,'runtime':'packaged actual Main','candidate_image':fixture.binding['candidate_image_id'],'startup_root_ids':startup_receipts['root_ids'],'source_authority_sha256':hashlib.sha256(Path(authority).read_bytes()).hexdigest(),'Source_API_calls':0,'Source_task_calls':0,'Source_replays':0,'Original_Django_setup_processes_before_HTTP_phase':1,'Original_Django_setup_count_scope':'One Source9 fixture seed; HTTP phase records separate seed/network-control management processes. These use ORM setup/signals, never Source API/task bodies.','cases':[],'required_unmeasured_Source_HTTP':[],'Source_HTTP_authority_sha256':hashlib.sha256(Path(http_authority).read_bytes()).hexdigest(),'traceback_byte_parity':False,'full_channel_maintenance':False,'full_M6':False,'performance_claims':False}
 out=fixture.output/('channel-acceptance-'+uuid.uuid4().hex[:8]);out.mkdir();guest='/tmp/owned-channel-acceptance-'+uuid.uuid4().hex[:8];case_path='/shared/'+fixture.case+'/'+out.name
 # These tiny observer files live in the Original observer's /tmp only; Native
 # keeps its inspected EXACT single /data bind, never Source/helper ancestors.
 fixture.python('import os;from pathlib import Path;p=Path('+repr(guest)+');p.mkdir(mode=0o755);os.chmod(p,0o755)')
 for name in ['guestops.py','observer.py','journal.py','observer_control.py']:
  data=(HERE/name).read_bytes()
  if len(data)>65536:raise ValueError('BoundedObserverHelperBytes')
  encoded=base64.b64encode(data).decode()
  fixture.python('import os,base64;from pathlib import Path;p=Path('+repr(guest+'/'+name)+');p.write_bytes(base64.b64decode('+repr(encoded)+'));os.chmod(p,0o644)')
 native_inspection=json.loads(fixture.command([fixture.docker,'inspect',fixture.native]))[0]
 if native_inspection['Image']!=fixture.binding['candidate_image_id']or not native_inspection['State']['Running']:raise ValueError('ActualCandidateGenerationRequired')
 result['actual_Main_inspection']=native_inspection
 def op(action,*args):
  script='import sys,json;sys.path.insert(0,'+repr(guest)+');import guestops;print(json.dumps(guestops.'+action+'('+','.join(repr(x)for x in args)+'),default=str))'
  return json.loads(fixture.python(script))
 def persist():save(out/'report.json',result)
 seed=op('seed');result['seed']=seed;receiver='specific.channel_'+uuid.uuid4().hex+'!receiver';result['receiver']=receiver
 fixture.python('import os,pathlib;p=pathlib.Path('+repr(case_path)+');p.mkdir(mode=0o755,exist_ok=True);os.chown(p,65532,65532)')
 # Journal's deadline is a delegation of the outer ABSOLUTE budget, not a reset.
 duration=fixture.remaining(600);deadline=time.monotonic()+duration
 role='channel-observer-'+uuid.uuid4().hex[:8]
 fixture.role(role,['/dispatcharrpy/bin/python',guest+'/observer.py'],extra={'PYTHONPATH':guest,'CHANNEL_OBSERVER_ROOT':case_path,'CHANNEL_FUNCTIONAL_DEADLINE':str(deadline),'CHANNEL_RECEIVER':receiver})
 observer_started=True;trigger=None
 def read(name):return json.loads(fixture.python('import json;print(open('+repr(case_path+'/'+name)+').read())'))
 def wait_file(name,seconds=10):return fixture.wait(lambda:read(name),seconds,'ActualObserverFile:'+name)
 controls=module('observer_control')
 def control(action,**extra):
  name='control-'+uuid.uuid4().hex;body={'action':action,**extra};seconds=min(10,fixture.remaining(10));record=fixture.roles[role]
  script='import json,sys;sys.path.insert(0,'+repr(guest)+');from observer_control import request;print(json.dumps(request('+','.join(repr(v)for v in [case_path,name,body,record['pid'],record['starttime'],time.monotonic()+seconds])+')))'
  try:answer=json.loads(fixture.python(script,seconds=seconds+2))
  except Exception as error:raise controls.ObserverUnavailable('Actual observer control transport failed:'+type(error).__name__)from error
  if 'observer_failure'in answer:
   result['observer_infrastructure_failure']=answer['observer_failure'];persist()
   raise controls.ObserverUnavailable(json.dumps(answer['observer_failure'],sort_keys=True))
  return answer

 identity_bindings={'epgs':{references['normal_names']['SQL_changes'][TABLES[0]]['changed'][0]['before']['epg_data_id']:seed['epg_id']},'seed_channel_pairs':[(references['normal_names']['args'][0][0],seed['channel_ids'][0]),(references['names_P0001']['args'][0][0],seed['channel_ids'][1]),(references['tvg_22001']['args'][0][0],seed['channel_ids'][2]),(references['logos_23505']['kwargs']['channel_ids'][0],seed['channel_ids'][3])]}
 def compare(row,reference):
  meta=row['backend']['metadata'];expected=reference['metadata'];checks={}
  checks['status']=meta['status']==expected['status'];checks['result']=meta['result']==expected['result'];checks['identity']=meta['task_id']==row['task_id'];checks['children']=meta['children']==expected['children']==[]
  checks['ttl']=0<row['backend']['ttl']<=3600;checks['progress']=normalize_events(row['after']['Channels_decoded'],row['task_id'])==normalize_events(reference['Channels']['decoded'],reference['actual_task_id'])
  if expected['status']=='SUCCESS':checks['success_traceback_null']=meta['traceback']is None
  else:checks['honest_Native_diagnostic']=isinstance(meta['traceback'],str)and 'Native Dispatcharr channel task diagnostic'in meta['traceback']and '/app/apps/channels/tasks.py'not in meta['traceback'];checks['failed_channel_rollback']=row['before']['tables'][TABLES[0]]==row['after']['tables'][TABLES[0]]
  # Row-by-row effects are checked independently below, not only table sets or counts.
  before={r['id']:r for r in row['before']['tables'][TABLES[0]]};after={r['id']:r for r in row['after']['tables'][TABLES[0]]}
  if row['name']=='normal_names':
   changed=[(before[k],v)for k,v in after.items()if k in before and before[k]!=v];checks['exact_name_only']=len(changed)==1 and changed[0][1]['name']=='Owned EPG name'and {k:v for k,v in changed[0][0].items()if k!='name'}=={k:v for k,v in changed[0][1].items()if k!='name'}
  if row['name']=='normal_tvg':
   changed=[(before[k],v)for k,v in after.items()if k in before and before[k]!=v];checks['exact_tvg_only']=len(changed)==1 and changed[0][1]['tvg_id']=='owned.epg.id'and {k:v for k,v in changed[0][0].items()if k!='tvg_id'}=={k:v for k,v in changed[0][1].items()if k!='tvg_id'}
  if row['name']=='normal_logos':
   logos={r['id']:r for r in row['after']['tables'][TABLES[1]]};changed=[(before[k],v)for k,v in after.items()if k in before and before[k]!=v];checks['exact_logo_only']=len(changed)==1 and logos[changed[0][1]['logo_id']]['url']=='http://owned.invalid/logo.png'and {k:v for k,v in changed[0][0].items()if k!='logo_id'}=={k:v for k,v in changed[0][1].items()if k!='logo_id'}
  if row['name']=='normal_bulk':
   inserted=[r for k,r in after.items()if k not in before];checks['one_insert']=len(inserted)==1
   if inserted:
    new=inserted[0];expected_new=reference['SQL_changes'][TABLES[0]]['inserted'][0];foreign={'epg_data_id':seed['epg_id']};volatile={'id','uuid','created_at','updated_at','epg_data_id','logo_id'};checks['all_nongenerative_fields']={k:v for k,v in new.items()if k not in volatile}=={k:v for k,v in expected_new.items()if k not in volatile};checks['actual_epg_binding']=new['epg_data_id']==foreign['epg_data_id'];checks['real_uuid4']=uuid.UUID(new['uuid']).version==4 and new['uuid']not in [r['uuid']for r in before.values()];checks['actual_time_bounds']=datetime.datetime.fromisoformat(row['before_utc'])<=datetime.datetime.fromisoformat(new['created_at'])<=datetime.datetime.fromisoformat(row['after_utc'])and datetime.datetime.fromisoformat(new['created_at'])<=datetime.datetime.fromisoformat(new['updated_at'])<=datetime.datetime.fromisoformat(row['after_utc']);logos={r['id']:r for r in row['after']['tables'][TABLES[1]]};checks['logo_binding']=logos[new['logo_id']]['url']=='http://owned.invalid/stream.png';links=[v for v in row['after']['tables'][TABLES[2]]if v['channel_id']==new['id']];checks['exact_stream_link']=len(links)==1 and links[0]['stream_id']==seed['stream_id']and links[0]['order']==0;checks['no_profile_membership']=not any(v['channel_id']==new['id']for v in row['after']['tables'][TABLES[3]])
  compare_effects=module('effects').compare
  row['effect_review']=compare_effects(row,reference,seed,identity_bindings);checks['all_SQL_changed_rows_and_fields']=row['effect_review']['passed']
  row['checks']=checks;row['passed']=all(checks.values());return row['passed']
 def one(name,route_key,body,reference=None,error=None,projection=False):
  nonlocal trigger
  row={'name':name,'entrypoint':'actual Main API producer'if not projection else'projected external noarg task frame','before_utc':utc(),'before':op('snapshot',seed,receiver)};result['cases'].append(row);persist();op('clear',receiver)
  if error:op('trigger',*error);trigger=error[0]
  try:
   row['redis_start']=op('snapshot',seed,receiver)['redis_time']
   if projection:
    template=json.loads((HERE/'source-common-template.json').read_text());projected=op('project_missing',template);row['projection']=projected;ident=projected['id']
   else:
    response=fixture.api('POST','/api/channels/channels/'+ROUTES[route_key],body);row['HTTP']=response;row['actual_input_args']=([body['stream_ids'],body.get('channel_profile_ids'),body.get('starting_channel_number')]if route_key=='normal_bulk'else([]if route_key=='normal_logos'else[body['channel_ids']]));row['actual_input_kwargs']=({'channel_ids':body['channel_ids']}if route_key=='normal_logos'else{});persist()
    if response['status']!=(202 if route_key=='normal_bulk'else 200):row.update(passed=False,primary_error='ActualHTTPstatusBeforeTaskParse');return row
    ident=response['body']['task_id'];uuid.UUID(ident)
   row['task_id']=ident;row['backend']=fixture.wait(lambda:(lambda value:value if value['metadata']and value['metadata']['status']in ['SUCCESS','FAILURE','REVOKED']else None)(op('backend',ident)),30,'ActualChannelTerminal:'+name);row['after']=op('snapshot',seed,receiver);row['after_utc']=utc();row['redis_end']=row['after']['redis_time'];compare(row,references[reference or name]);persist();return row
  except Exception as error:
   row['primary_error']={'type':type(error).__name__,'message':str(error)};row['passed']=False;persist();return row
  finally:
   if trigger:op('trigger',trigger,None,None,None,True);trigger=None
 try:
  ready=wait_file('observer-ready.json');result['observer_ready']=ready
  if not ready['actual_MONITOR_subscribed']or not ready['actual_group_add_completed']:raise RuntimeError('ActualObserverAdmissionRequired')
  result['Native_role_checks']=[]
  one('normal_names','normal_names',{'channel_ids':[seed['channel_ids'][0]]})
  one('normal_tvg','normal_tvg',{'channel_ids':[seed['channel_ids'][0]]})
  one('normal_logos','normal_logos',{'channel_ids':[seed['channel_ids'][0]]})
  one('normal_bulk','normal_bulk',{'stream_ids':[seed['stream_id']],'channel_profile_ids':[],'starting_channel_number':0})
  one('names_P0001','normal_names',{'channel_ids':[seed['channel_ids'][1]]},error=('dispatcharr_channels_channel','UPDATE','P0001','owned channel name update'))
  one('tvg_22001','normal_tvg',{'channel_ids':[seed['channel_ids'][2]]},error=('dispatcharr_channels_channel','UPDATE','22001','owned channel tvg update'))
  op('change_icon',seed)
  one('logos_23505','normal_logos',{'channel_ids':[seed['channel_ids'][3]]},error=('dispatcharr_channels_logo','INSERT','23505','owned channel logo insert'))
  one('bulk_23514','normal_bulk',{'stream_ids':[seed['stream_id']],'channel_profile_ids':[],'starting_channel_number':0},error=('dispatcharr_channels_channel','INSERT','23514','owned bulk channel insert'))
  one('logos_missing_arguments','normal_logos',{},projection=True)
  result['ordinary_cases_complete']=len(result['cases'])==9 and all(c.get('passed')for c in result['cases'])
  result['HTTP98']=module('http_cases').run(fixture,op,control,http_authority,out,native_inspection,module,save,receiver)
  # Keep the observer/gate owner alive through the destructive final phase.
  if terminate:
   physical=module('physical').run
   result['physical']=physical(fixture,op,seed,receiver,case_path,read,out)
  else:result['required_unmeasured_Physical']=['actual advisory-held SQL/shared-budget/TERM drain; enable terminate only as the final context callback']
 except Exception as error:result['primary_error']={'type':type(error).__name__,'message':str(error)}
 finally:
  if fixture.functional_budget_expired():fixture.begin_cleanup()
  if trigger:
   try:op('trigger',trigger,None,None,None,True)
   except Exception as error:result['trigger_cleanup_error']=str(error)
  if observer_started:
   try:result['observer_role_stop']=fixture.role(role,[],stop=True);result['observer_final']=read('observer-final.json')
   except Exception as error:result['observer_cleanup_error']=str(error)
  persist()
 # Parse the actually durable journal only after role normal0/terminal ECHO.
 journal_path=fixture.case_data/out.name/'redis-monitor-private.jsonl'
 if journal_path.exists():
  qualify=module('wire').qualify
  result['wire']=qualify(journal_path,result['cases'],fixture.binding['candidate_image_id'],native_inspection,fixture.binding)
 else:result['wire']={'passed':False,'primary_error':'Missing actual durable journal'}
 result['passed']=result.get('ordinary_cases_complete')is True and result.get('wire',{}).get('passed')is True and result.get('HTTP98',{}).get('passed')is True and all(c['expected_denial']for c in result.get('Native_role_checks',[]))and result.get('observer_final',{}).get('terminal_barrier_observed')is True and result.get('observer_final',{}).get('terminal_reader_error')is None and result.get('observer_final',{}).get('thread_settled')is True and 'observer_cleanup_error'not in result and (not terminate or result.get('physical',{}).get('passed')is True)
 result['outer_exact_fixture_cleanup_required']=True;result['outer_cleanup_qualified']=False;persist();return result
