"""Existing Native M8 fixture boundary controls, no runtime starts on import.
Original Dispatcharr/Celery/Django authors retain application/algorithm credit.
"""
import datetime,hashlib,json,traceback,uuid
from pathlib import Path

def save(root,name,value):
 (root/name).write_text(json.dumps(value,indent=2,sort_keys=True,default=str)+'\n')

def sql_state(f):
 # Actual bounded owned DB relations; content hashes avoid copying user payloads.
 return f.sql("""import hashlib
from apps.channels.models import Recording
from core.models import CoreSettings,SystemEvent
from django_celery_beat.models import PeriodicTask,PeriodicTasks
from apps.connect.models import DeliveryLog
result={}
for model in(Recording,CoreSettings,PeriodicTask,PeriodicTasks,SystemEvent,DeliveryLog):
 rows=list(model.objects.order_by('pk').values());assert len(rows)<=8192
 raw=json.dumps(rows,sort_keys=True,default=str).encode();assert len(raw)<=8388608
 result[model._meta.db_table]={'rows':len(rows),'sha256':hashlib.sha256(raw).hexdigest()}
 if model in(PeriodicTask,PeriodicTasks):
  assert len(rows)<=128 and len(raw)<=262144
  result[model._meta.db_table]['logical_rows']=rows
print(json.dumps(result,default=str))""",secret=True)

def cache_state(f):
 # Only actual DVR-group cache namespace, not unrelated auth/session caches.
 return f.redis(0,"local k=redis.call('KEYS','*coresettings:*dvr_settings*');if #k>32 then return redis.error_reply('DVR cache32')end;table.sort(k);local r={};for _,n in ipairs(k)do table.insert(r,{n,redis.sha1hex(redis.call('DUMP',n))})end;return r")

def broker_state(f):
 return f.redis(1,"return {redis.call('LRANGE','celery',0,64),redis.call('LRANGE','dvr',0,64),redis.call('HLEN','unacked'),redis.call('ZCARD','unacked_index')}")

def media_state(f):
 root=f.case_data/'recordings';rows=[];used=0
 for p in sorted(root.rglob('*')):
  if p.is_file():
   assert not p.is_symlink();used+=p.stat().st_size;assert used<=8388608 and len(rows)<4096
   rows.append([str(p.relative_to(root)),p.stat().st_size,hashlib.sha256(p.read_bytes()).hexdigest()])
 return rows

def observer_start(f,label,*,database=None):
 name='dvr-domain-observer-'+uuid.uuid4().hex[:8];host=f.case_data/name;guest='/shared/'+f.case+'/'+name
 f.python("import pathlib,os;p=pathlib.Path("+repr(guest)+");p.mkdir(mode=0o755);os.chown(p,65532,65532)",secret=True)
 if database is not None and(type(database)is not int or database!=1):raise ValueError('OnlyExplicitBrokerDatabase1Supported')
 extra={'CHANNEL_OBSERVER_ROOT':guest,'CHANNEL_FUNCTIONAL_DEADLINE':str(f.context['deadline_monotonic'])}
 if database is not None:extra['CHANNEL_OBSERVER_DATABASE']=str(database)
 f.role(name,['/dispatcharrpy/bin/python','/normal-dvr-tools/startup_observer.py'],extra=extra)
 actual=f.wait(lambda:json.loads((host/'ready.json').read_text())if(host/'ready.json').is_file()else False,10,'actual-domain-MONITOR-ready')
 assert actual['actual_euid']==65532 and actual['actual_MONITOR_subscribed']is True
 assert actual['database_filter']==database and actual['all_databases_recorded']==(database is None)
 return name,host

def observer_stop(f,observer,queue):
 name,host=observer;stop=f.role(name,[],stop=True);final=json.loads((host/'final.json').read_text())
 assert final['terminal_barrier_observed']and final['terminal_barrier_fsynced']and final['thread_settled']and final['writer_closed']and not final['terminal_reader_error']and final['observer_error']is None
 path=host/'private-startup-monitor.jsonl';assert path.stat().st_size<=16777216
 rows=[json.loads(line)for line in path.read_text().splitlines()];forbidden=[]
 for row in rows:
  if row.get('db')!=1:continue
  command=row.get('command','').upper();parts=command.split()
  if len(parts)>1 and((parts[0]in('RPOP','LPOP','BRPOP','BLPOP','RPOPLPUSH','BRPOPLPUSH')and parts[1]==queue.upper())or(parts[0]in('HDEL','ZREM')and parts[1]in('UNACKED','UNACKED_INDEX'))):forbidden.append(command)
 assert not forbidden
 return {'actual_redis_command_rows':len(rows),'claim_or_ACK_commands':0,'terminal':final,'observer_stop':stop,'private_journal_sha256':hashlib.sha256(path.read_bytes()).hexdigest()}

def domain_blocked(status,workers,lane):
 return status==503 and workers.get('ready')is False and workers.get(lane)=='Blocked(UnsupportedDomain)'

def beat_row_delta(before,after):
 # Diagnostic classification only. Every changed field still fails strict
 # equality; potential scheduler metadata requires actual journal correlation.
 result={}
 for table in('django_celery_beat_periodictask','django_celery_beat_periodictasks'):
  left={r['id']if'id'in r else r['ident']:r for r in before[table]['logical_rows']}
  right={r['id']if'id'in r else r['ident']:r for r in after[table]['logical_rows']}
  changes=[]
  for ident in sorted(set(left)|set(right)):
   a=left.get(ident);b=right.get(ident)
   fields=sorted(set(a or{})|set(b or{}))if a is None or b is None else sorted(k for k in set(a)|set(b)if a.get(k)!=b.get(k))
   if fields:changes.append({'id':ident,'before':a,'after':b,'changed_fields':fields,'classification':'potential_scheduler_metadata_requires_journal_correlation'if a is not None and b is not None and set(fields)<= {'last_run_at','total_run_count'}else'configuration_creation_deletion_or_other'})
  result[table]=changes
 return result

def producer_observer_stop(f,observer):
 name,host=observer;stop=f.role(name,[],stop=True);terminal=json.loads((host/'final.json').read_text())
 assert terminal['terminal_barrier_observed']and terminal['terminal_barrier_fsynced']and terminal['thread_settled']and terminal['writer_closed']and not terminal['terminal_reader_error']and terminal['observer_error']is None
 path=host/'private-startup-monitor.jsonl';assert path.stat().st_size<=16777216
 rows=[json.loads(line)for line in path.read_text().splitlines()];publications=[];queue_operations=[]
 for row in rows:
  if row.get('db')!=1:continue
  command=row.get('command','');parts=command.split(' ',2)
  if len(parts)>=2 and parts[0].upper()in('RPOP','LPOP','BRPOP','BLPOP','RPOPLPUSH','BRPOPLPUSH','HDEL','ZREM'):queue_operations.append(row)
  if len(parts)!=3 or parts[0].upper()not in('LPUSH','RPUSH')or parts[1].lower()not in('celery','dvr'):continue
  frame=json.loads(parts[2]);frame=json.loads(frame)if isinstance(frame,str)else frame
  publications.append({'actual_redis_time':row['time'],'queue':parts[1],'id':frame['headers']['id'],'task':frame['headers']['task'],'raw_frame':frame})
 forbidden=[r for r in publications if r['task']in('apps.channels.tasks.run_recording','apps.channels.tasks.recover_recordings_on_startup','apps.channels.tasks.reschedule_upcoming_recordings_for_offset_change')]
 claim_ACK=[]
 for row in queue_operations:
  parts=row['command'].upper().split()
  if len(parts)>1 and((parts[0]in('RPOP','LPOP','BRPOP','BLPOP','RPOPLPUSH','BRPOPLPUSH')and parts[1]=='DVR')or(parts[0]in('HDEL','ZREM')and parts[1]in('UNACKED','UNACKED_INDEX'))):claim_ACK.append(row)
 return{'actual_journal_rows':len(rows),'DVR_claim_or_ACK_commands':claim_ACK,'publications':publications,'recording_recovery_offset_publications':forbidden,'queue_operations':queue_operations,'raw_journal_path':str(path),'raw_journal_sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'terminal':terminal,'observer_stop':stop,'strict_Beat_equality_relaxed':False}

def health(f):
 status,raw=f.get(f.base_url+'/health/ready');assert len(raw)<=65536
 return status,json.loads(raw)

def restart_idle(f,private,label):
 # Initial disposable M8 startup must prove exactly two roots in startup_hooks.
 # Later same-DB restarts honor actual dispatch locks: capture 0..2 publications,
 # never fabricate two roots or reset production lock keys to force them.
 observer=observer_start(f,label+'-startup');name,host=observer
 f.start_native();generation=dict(f.containers[-1])
 f.wait(lambda:broker_state(f)==[[],[],0,0],30,'actual-boundary-restart-startup-settled')
 status,value=health(f);workers=value['selected_workers'];assert status==200 and workers['ready']is True and workers['dvr_plugin_domain']=='unsupported_configured_plugin_hooks'and workers['startup_recovery_deferred_unsupported_domain']is True and workers['clocked_scheduler_enabled']is False
 def roots_settled():
  path=host/'snapshot.json'
  if not path.is_file():return False
  observed=json.loads(path.read_text());assert not observed['journal_error'];frames=observed['frames'];assert len(frames)<=2
  roots={row['frame']['headers']['id']:row for row in frames};assert len(roots)==len(frames)
  assert all(row['frame']['headers']['task']in('apps.plugins.tasks.refresh_plugin_repos','core.tasks.check_for_version_update')for row in frames)
  backend={}
  for ident in roots:
   raw=f.redis(2,"return redis.call('GET',ARGV[1])",'celery-tasks:celery-task-meta-'+ident)
   if not raw:return False
   meta=json.loads(raw);assert meta['status']=='SUCCESS'and meta['task_id']==ident;backend[ident]=meta
  return {'actual_frames':frames,'actual_backend':backend,'actual_root_count':len(frames)}
 observed=f.wait(roots_settled,30,'actual-boundary-restart-startup-root-results')
 stop=f.role(name,[],stop=True);terminal=json.loads((host/'final.json').read_text());assert terminal['terminal_barrier_observed']and terminal['terminal_barrier_fsynced']and terminal['thread_settled']and terminal['writer_closed']and not terminal['terminal_reader_error']and terminal['observer_error']is None
 # The terminal fsynced journal, not an earlier potentially stale snapshot,
 # determines the complete actual startup frame set for this generation.
 journal=host/'private-startup-monitor.jsonl';assert journal.stat().st_size<=16777216
 frames=[]
 for row in map(json.loads,journal.read_text().splitlines()):
  parts=row.get('command','').split(' ',2)
  if row.get('db')!=1 or len(parts)!=3 or parts[0].upper()not in('LPUSH','RPUSH')or parts[1].lower()not in('celery','dvr'):continue
  frame=json.loads(parts[2]);frame=json.loads(frame)if isinstance(frame,str)else frame
  frames.append({'frame':frame,'actual_redis_time':row['time'],'queue':parts[1]})
 roots={row['frame']['headers']['id']:row for row in frames};assert len(roots)==len(frames)and len(roots)<=2
 assert all(row['frame']['headers']['task']in('apps.plugins.tasks.refresh_plugin_repos','core.tasks.check_for_version_update')for row in frames)
 def durable_roots_settled():
  backend={}
  for ident in roots:
   raw=f.redis(2,"return redis.call('GET',ARGV[1])",'celery-tasks:celery-task-meta-'+ident)
   if not raw:return False
   meta=json.loads(raw);assert meta['status']=='SUCCESS'and meta['task_id']==ident;backend[ident]=meta
  return {'actual_frames':frames,'actual_backend':backend,'actual_root_count':len(roots),'durable_journal_sha256':hashlib.sha256(journal.read_bytes()).hexdigest()}
 observed=f.wait(durable_roots_settled,30,'actual-durable-restart-root-results')
 assert workers['startup']=='Started'
 assert f.redis(0,"return redis.call('GET','dvr:recover_dispatch_lock')")is None
 save(private,label+'-actual-restart-startup.json',{'actual_Main_generation':generation,'selected_workers':workers,'observed_roots':observed,'observer_stop':stop,'terminal':terminal,'recovery_root_published':False,'same_DB_dispatch_locks_preserved':True,'Source_three_root_parity':False})

def run(f):
 if f.backend!='Native'or f.case!='m8-actions':raise ValueError('NativeOwnedM8Only')
 out=Path(f.output)/'dvr-domain-boundaries';out.mkdir();private=out/'private';private.mkdir(mode=0o700)
 report={'status':'running','Source_DVR_plugin_parity':False,'full_milestone_passed':False,'performance_verified':False,'cases':[],'scope':'Negative controls on fresh Main with autonomous Beat0; default Beat1 M8/restore acceptance not inferred'};save(out,'report.json',report)
 original_environment=dict(getattr(f,'_plugin_environment',{}))
 try:
  assert f.native is None, 'Negative Beat0 isolation requires physically reset Main'
  f._plugin_environment={**original_environment,'RUST_EXPERIMENTAL_SELECTED_BEAT':'0'}
  save(private,'negative-control-environment.json',{'RUST_EXPERIMENTAL_SELECTED_BEAT':'0','original_Beat_override':original_environment.get('RUST_EXPERIMENTAL_SELECTED_BEAT'),'SQL_hash_fields_waived':False})
  # Existing M8 workflow already proved normal Main stop and SQL generation retirement.
  # Boundary checks therefore own a fresh idle Main on the SAME fixture/data.
  if f.native is None:
   restart_idle(f,private,'initial-after-M8-stop')
  status,ready=health(f);selected=ready['selected_workers'];assert status==200 and selected['ready']and selected['dvr_plugin_domain']=='unsupported_configured_plugin_hooks'and selected['startup_recovery_deferred_unsupported_domain']is True and selected['clocked_scheduler_enabled']is False
  save(private,'idle-capability.json',{'status':status,'selected_workers':selected})
  # Bypass save SIGNALS only for inert fixture setup; production HTTP/tasks below
  # remain actual producers. Dates are future and every row belongs to this DB.
  seeded=f.sql("from django.utils import timezone;from datetime import timedelta;from core.models import CoreSettings;now=timezone.now();rows=Recording.objects.bulk_create([Recording(channel_id="+str(f.channel_id)+",start_time=now+timedelta(hours=1),end_time=now+timedelta(hours=2),custom_properties={})for _ in range(3)]);r=CoreSettings.objects.get(key='dvr_settings');CoreSettings.get_dvr_settings();print(json.dumps({'recordings':[r.pk for r in rows],'setting':{'id':r.pk,'value':r.value},'start':(now+timedelta(hours=1)).isoformat(),'end':(now+timedelta(hours=2)).isoformat()}))",secret=True)
  body={'channel':f.channel_id,'start_time':seeded['start'],'end_time':seeded['end'],'custom_properties':{}}
  settings=dict(seeded['setting']['value']);settings['pre_offset_minutes']=int(settings.get('pre_offset_minutes',0))+1
  requests=[('POST','/api/channels/recordings/',body),('PUT','/api/channels/recordings/'+str(seeded['recordings'][0])+'/',body),('PATCH','/api/channels/recordings/'+str(seeded['recordings'][0])+'/',{'end_time':seeded['end']}),('PATCH','/api/core/settings/'+str(seeded['setting']['id'])+'/',{'value':settings})]
  producer_fixture_valid=True
  for index,(method,path,payload)in enumerate(requests):
   observer=observer_start(f,'producer-'+str(index));before=response=after=None
   try:
    before={'SQL':sql_state(f),'DVR_cache':cache_state(f),'broker':broker_state(f),'media':media_state(f)}
    import time
    request_started_epoch=time.time();response=f.api(method,path,payload);request_finished_epoch=time.time()
    after={'SQL':sql_state(f),'DVR_cache':cache_state(f),'broker':broker_state(f),'media':media_state(f)}
   finally:journal=producer_observer_stop(f,observer)
   strict=response['status']==501 and before==after and not journal['recording_recovery_offset_publications']and not journal['DVR_claim_or_ACK_commands']
   diagnostic={'before':before,'response':response,'after':after,'request_started_epoch':request_started_epoch,'request_finished_epoch':request_finished_epoch,'Beat_logical_delta':beat_row_delta(before['SQL'],after['SQL']),'producer_phase_journal':journal,'strict_before_after_equal':before==after,'strict_case_passed':strict}
   save(private,'producer-'+str(index)+'.json',diagnostic)
   report['cases'].append({'name':method+' '+path,'status':'passed'if strict else'failed','SQL_Beat_DVR_cache_broker_unchanged':before==after,'actual_recording_recovery_offset_publications':len(journal['recording_recovery_offset_publications']),'actual_DVR_claim_ACK_commands':len(journal['DVR_claim_or_ACK_commands']),'strict_Beat_equality_relaxed':False})
   # A failed global Beat comparison does not erase independent queue controls.
   # Run those only if the owned non-Beat data/domain/broker remain intact.
   nonbeat=lambda value:{k:v for k,v in value.items()if k not in('django_celery_beat_periodictask','django_celery_beat_periodictasks')}
   producer_fixture_valid=producer_fixture_valid and response['status']==501 and nonbeat(before['SQL'])==nonbeat(after['SQL'])and before['DVR_cache']==after['DVR_cache']and before['broker']==after['broker']and before['media']==after['media']and not journal['recording_recovery_offset_publications']
   save(out,'report.json',report)
  if not producer_fixture_valid:
   report['cases'].append({'name':'independent-queued-heads','status':'blocked','reason':'Actual API producer altered non-Beat state or published work; no reset/rollback fabricated'})
   report['status']='failed';return report
  # Cleanup stays available under unsupported scheduling capability.
  stopped=f.api('POST','/api/channels/recordings/'+str(seeded['recordings'][1])+'/stop/',{});assert stopped['status']==200
  deleted=f.api('DELETE','/api/channels/recordings/'+str(seeded['recordings'][2])+'/');assert deleted['status']==204
  cleanup=f.sql('print(json.dumps({"stop_status":Recording.objects.get(pk='+str(seeded['recordings'][1])+').custom_properties.get("status"),"deleted_absent":not Recording.objects.filter(pk='+str(seeded['recordings'][2])+').exists()}))',secret=True);assert cleanup=={'stop_status':'stopped','deleted_absent':True};save(private,'cleanup-availability.json',cleanup);report['cases'].append({'name':'DELETE/stop available','status':'passed'})
  code,value=health(f)
  if code!=200 or value.get('selected_workers',{}).get('ready')is not True or broker_state(f)!=[[],[],0,0]:
   save(private,'producer-phase-before-independent-reset.json',{'health_status':code,'health':value,'broker':broker_state(f)})
   if f.native is not None:save(private,'producer-phase-Main-stop.json',f.stop_native(expected=None))
   restart_idle(f,private,'producer-phase-independent-reset')
  for label in('normal-recording','revoked-recording','missing-row-recording','recovery','offsets'):
   ident=str(uuid.uuid4());queue='dvr'if'recording'in label else'celery'
   if label=='revoked-recording':
    assert f.redis(2,"return redis.call('GET',ARGV[1])",'celery-tasks:celery-task-meta-'+ident)is None
    f.sql("from celery import current_app;current_app.control.revoke("+repr(ident)+",terminate=False,reply=False);print(json.dumps({'actual_pidbox_revoke_published':True}))",secret=True)
    def marked():
     raw=f.redis(2,"return redis.call('GET',ARGV[1])",'celery-tasks:celery-task-meta-'+ident)
     return raw if raw and json.loads(raw).get('status')=='REVOKED'else False
    revoked=f.wait(marked,10,'actual-control-REVOKED-before-owned-head');save(private,label+'-prior-backend.json',json.loads(revoked))
   # One real Source apply_async frame. Never handcraft Native task JSON.
   if'recording'in label:
    setup="from django.utils import timezone;from datetime import timedelta;now=timezone.now();r=Recording.objects.bulk_create([Recording(channel_id="+str(f.channel_id)+",start_time=now-timedelta(seconds=5),end_time=now+timedelta(minutes=10),task_id="+repr(ident)+",custom_properties={})])[0];rid=r.pk;"
    if label=='missing-row-recording':setup+="Recording.objects.filter(pk=rid)._raw_delete(Recording.objects.db);"
    setup+="print(json.dumps({'recording':rid,'start':(now-timedelta(seconds=5)).isoformat(),'end':(now+timedelta(minutes=10)).isoformat()}))"
    owned=f.sql(setup,secret=True)
    script="from apps.channels.tasks import run_recording;t=run_recording.apply_async(args=["+str(owned['recording'])+","+str(f.channel_id)+","+repr(owned['start'])+","+repr(owned['end'])+"],task_id="+repr(ident)+",queue='dvr');print(json.dumps({'id':t.id}))"
   else:
    name='recover_recordings_on_startup'if label=='recovery'else'reschedule_upcoming_recordings_for_offset_change'
    script='from apps.channels.tasks import '+name+';t='+name+'.apply_async(task_id='+repr(ident)+",queue='celery');print(json.dumps({'id':t.id}))"
   observer=observer_start(f,label)
   # Snapshot AFTER owned inert row setup, BEFORE actual task publication; this
   # catches effects performed before observing the parked head/fault.
   prepublication={'SQL':sql_state(f),'DVR_cache':cache_state(f),'broker':broker_state(f),'backend':f.redis(2,"return redis.call('GET',ARGV[1])",'celery-tasks:celery-task-meta-'+ident),'media':media_state(f)}
   assert prepublication['broker']==[[],[],0,0]
   made=f.sql(script,secret=True);assert made['id']==ident
   def observe():
    code,body=health(f);workers=body.get('selected_workers',{})
    return {'status':code,'workers':workers,'expected_lane':queue}if domain_blocked(code,workers,queue)else False
   blocked=f.wait(observe,20,'actual-unsupported-'+label+'-beforeACK')
   raw=f.redis(1,"return redis.call('LRANGE',ARGV[1],0,64)",queue);assert len(raw)==1
   frame=json.loads(raw[0]);assert frame['headers']['id']==ident
   before={'SQL':sql_state(f),'DVR_cache':cache_state(f),'broker':broker_state(f),'backend':f.redis(2,"return redis.call('GET',ARGV[1])",'celery-tasks:celery-task-meta-'+ident),'media':media_state(f)}
   assert before['SQL']==prepublication['SQL'] and before['DVR_cache']==prepublication['DVR_cache'] and before['backend']==prepublication['backend'] and before['media']==prepublication['media']
   # Three actual reads, with one health cadence second between each, demonstrate
   # stable parked head; a race consuming before the first read fails above.
   import time
   for _ in range(3):
    time.sleep(min(1,f.remaining(1)));assert broker_state(f)==before['broker'];assert sql_state(f)==before['SQL'];assert cache_state(f)==before['DVR_cache'];assert media_state(f)==before['media'];assert f.redis(2,"return redis.call('GET',ARGV[1])",'celery-tasks:celery-task-meta-'+ident)==before['backend']
   assert before['broker'][2:]==[0,0]
   if label!='revoked-recording':assert before['backend']is None
   save(private,label+'.json',{'producer':made,'typed_domain_refusal':blocked,'raw_frame':raw[0],'raw_frame_sha256':hashlib.sha256(raw[0].encode()).hexdigest(),'before_actual_publication':prepublication,'stable_observations':before,'no_ACK':True})
   stop=f.stop_native(expected=None);save(private,label+'-stop.json',stop)
   assert sql_state(f)==before['SQL'] and cache_state(f)==before['DVR_cache'] and media_state(f)==before['media'] and broker_state(f)==before['broker']
   assert f.redis(2,"return redis.call('GET',ARGV[1])",'celery-tasks:celery-task-meta-'+ident)==before['backend']
   observed=observer_stop(f,observer,queue);save(private,label+'-noACK-MONITOR.json',observed)
   # Exact owned frame removed only AFTER consumer physically stops; do not ACK,
   # requeue or erase unrelated work. Any unexpected queue row is a test failure.
   removed=f.redis(1,"if redis.call('LLEN',ARGV[1])~=1 or redis.call('LINDEX',ARGV[1],0)~=ARGV[2]then return redis.error_reply('Owned head identity changed')end;return redis.call('LREM',ARGV[1],1,ARGV[2])",queue,raw[0]);assert removed==1
   report['cases'].append({'name':label,'status':'passed','actual_Source_producer':True,'raw_head_unchanged':True,'no_ACK':True,'actual_MONITOR_claim_ACK_commands':observed['claim_or_ACK_commands'],'backend_preserved':True,'raw_frame_removed_only_after_stop':True,'actual_stop':stop})
   restart_idle(f,private,label+'-next')
  report['status']='passed'if all(c['status']=='passed'for c in report['cases'])else'failed'
 except BaseException as error:
  report.update(status='failed',error_type=type(error).__name__);(private/'failure.txt').write_text(traceback.format_exc()[-8192:]);raise
 finally:
  f._plugin_environment=original_environment
  save(out,'report.json',report)
 return report
