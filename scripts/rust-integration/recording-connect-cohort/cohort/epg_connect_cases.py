"""Real EPG Connect webhook/template effects in the existing SD context.
No app on import. Original Dispatcharr/Django/Requests authors retain credit.
"""
import base64,json,traceback,urllib.parse
from pathlib import Path
from backend_terminal import require_backend_dependency,wait_backend_terminal,BlockedBackendDependency
CASE='schedules-direct';ORIGIN='http://127.0.0.1:18097'
def save(path,value):path.write_text(json.dumps(value,indent=2,sort_keys=True)+'\n')
def prepare_context(fixture):
 if fixture.case!=CASE:return
 # Disabled subscriptions during ordinary SD/XML cases: enable only this new
 # affected phase. Actual handlers/renderer/model task bodies are never patched.
 setup=fixture.sql("from apps.connect.models import Integration,EventSubscription;rows=[];specs=[('plain','webhook','/connect/plain',None,True),('json','webhook','/connect/json','{% firstof source_name \"fallback\" as chosen %}{\"source\":\"{{ chosen|escapejs }}\"}',True),('rejected','webhook','/connect/rejected',None,True),('invalid','webhook','/connect/fallback','{% owned_invalid_tag %}',True),('unknown','api','/connect/should-not-run',None,True),('disabled','script','/connect/should-not-run','{% owned_invalid_tag %}',False),('nan','webhook','/connect/nan','NaN',True)];\nfor event in ['epg_refresh','epg_error']:\n for label,kind,path,template,enabled in specs:\n  i=Integration.objects.create(name='Owned '+event+' '+label,type=kind,config={'url':'"+ORIGIN+"'+path,'headers':{'X-Owned-Connect':'proof'}},enabled=enabled);s=EventSubscription.objects.create(event=event,integration=i,enabled=False,payload_template=template);rows.append({'id':s.pk,'event':event,'label':label,'integration':i.pk})\nprint(json.dumps({'subscriptions':rows}))",secret=True)
 fixture._epg_connect=setup
 save(Path(fixture.output)/'declared-EPG-Connect-inputs.json',{'context':CASE,'events':['epg_refresh','epg_error'],'subscriptions_per_event':7,'HTTP_requests_per_event':5,'expected_delivery_logs_per_event':6,'templates_mutate_shared_payload':True,'script_integration_disabled_only':True,'new_Source_and_Native_observations_required':True,'external_notifications':False})
def run(fixture):
 if fixture.case!=CASE:raise ValueError('OwnedSDContextRequired')
 out=Path(fixture.output)/'epg-connect';out.mkdir();private=out/'private';private.mkdir(mode=0o700);cells=[];setup=fixture._epg_connect
 def control(path,payload=None):
  script="import urllib.request,json;r=urllib.request.build_opener(urllib.request.ProxyHandler({})).open(urllib.request.Request("+repr(ORIGIN+path)+",data="+repr(None if payload is None else json.dumps(payload).encode())+",headers={'Content-Type':'application/json'}),timeout=5);raw=r.read(1048577);assert len(raw)<=1048576;print(raw.decode())"
  return json.loads(fixture.python(script,secret=True))
 def guest(script):
  prefix="import os,sys,json;from urllib.parse import quote;p=quote(os.environ['REDIS_PASSWORD'],safe='');os.environ['CELERY_BROKER_URL']='redis://:'+p+'@localhost:6379/1';os.environ['CELERY_RESULT_BACKEND']='redis://:'+p+'@localhost:6379/2';sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings');import django;django.setup();from celery import current_app;\n"
  return json.loads(fixture.python(prefix+script,secret=True))
 zero='00000000-0000-0000-0000-000000000000';key=guest('print(json.dumps(current_app.backend.get_key_for_task('+repr(zero)+').decode()))');assert key.endswith(zero);prefix=key[:-len(zero)]
 def terminal(ident):
  value=fixture.redis(2,"if redis.call('STRLEN',ARGV[1])>1048576 then return redis.error_reply('Connect backend budget')end;return redis.call('GET',ARGV[1])",prefix+ident)
  return None if value is None else json.loads(value)
 def subscription_state(event):
  ids=[r['id']for r in setup['subscriptions']if r['event']==event]
  return fixture.sql("from apps.connect.models import EventSubscription,DeliveryLog;from core.models import SystemEvent;print(json.dumps({'selected':list(EventSubscription.objects.filter(pk__in="+repr(ids)+",enabled=True).select_related('integration').values('id','integration__enabled','integration__type','payload_template')),'deliveries':list(DeliveryLog.objects.filter(subscription_id__in="+repr(ids)+").order_by('id').values('id','subscription_id','status','request_payload','response_payload','error_message','created_at')),'audit':list(SystemEvent.objects.filter(event_type="+repr(event)+",details__source_name='Owned SD guide').order_by('-id').values('id','details','timestamp')[:2])},default=str))",secret=True)
 def event_case(event):
  ids=[r['id']for r in setup['subscriptions']if r['event']==event];fixture.sql("from apps.connect.models import EventSubscription;EventSubscription.objects.filter(pk__in="+repr(ids)+").update(enabled=True);print(json.dumps({'declared_subscription_phase_enabled':True}))",secret=True)
  position=len(control('/snapshot')['trace']);before=subscription_state(event);save(private/(event+'-before.json'),before)
  source=fixture._sd_guide['source'];credentials=fixture._sd_credentials
  if event=='epg_error':
   # Same grounded SD auth-error branch as the retained guide cohort; only its
   # newly enabled Connect effects are new. No real provider/notification runs.
   fixture.sql("from apps.epg.models import EPGSource;EPGSource.objects.filter(pk="+str(source)+").update(password='owned-Connect-error-input');print(json.dumps({'owned_error_credentials_selected':True}))",secret=True)
  control('/control',{'case':'connect'if event=='epg_refresh'else'base'})
  made=guest('from apps.epg.sd_tasks import fetch_sd_mapped_guide_batch;t=fetch_sd_mapped_guide_batch.apply_async(args=['+str(source)+'],kwargs={"force":True});print(json.dumps({"id":t.id}))');save(private/(event+'-producer.json'),made)
  try:
   backend=wait_backend_terminal(fixture,terminal,made['id'],120,'actual-EPG-Connect-'+event+'-terminal',private);save(private/(event+'-backend.json'),backend);assert backend['status']=='SUCCESS'
   state=subscription_state(event);save(private/(event+'-after.json'),state);requests=[r for r in control('/snapshot')['trace'][position:]if r.get('kind')=='owned_connect_request'];save(private/(event+'-Origin.json'),requests)
   labels={r['id']:r['label']for r in setup['subscriptions']if r['event']==event};logs={labels[r['subscription_id']]:r for r in state['deliveries']};assert set(logs)=={'plain','json','rejected','invalid','unknown','nan'}
   assert len(requests)==5 and [r['path']for r in requests]==['/connect/plain','/connect/json','/connect/rejected','/connect/fallback','/connect/nan']
   assert all(logs[label]['status']=='success'for label in ['plain','json','invalid','nan']);assert logs['rejected']['status']=='failed'and logs['rejected']['response_payload']['status_code']==503
   assert logs['unknown']['status']=='failed'and logs['unknown']['error_message']=="No handler for integration type 'api'"and logs['unknown']['response_payload']=={}
   assert isinstance(logs['plain']['request_payload'],dict)and'chosen'not in logs['plain']['request_payload']
   assert all(logs[label]['request_payload']['chosen']=='Owned SD guide'for label in ['rejected','invalid','unknown'])
   assert json.loads(logs['json']['request_payload'])=={'source':'Owned SD guide'}and logs['nan']['request_payload']=='NaN'
   body=[base64.b64decode(r['body_base64'],validate=True)for r in requests]
   assert requests[1]['content_type']=='application/json'and json.loads(body[1])=={'source':'Owned SD guide'}
   assert requests[-1]['content_type']=='application/json'and body[-1]==b'NaN'
   plain=urllib.parse.parse_qs(body[0].decode(),keep_blank_values=True);fallback=urllib.parse.parse_qs(body[3].decode(),keep_blank_values=True);assert'chosen'not in plain and fallback['chosen']==['Owned SD guide']
   assert state['audit']and'chosen'not in state['audit'][0]['details']
   return {'task_id':made['id'],'delivery_count':6,'actual_HTTP_requests':5,'template_mutation_preserved_across_subscriptions':True,'prior_audit_not_retroactively_mutated':True,'Source_exception_message_or_clock_byte_parity_claimed':False}
  finally:
   fixture.sql("from apps.connect.models import EventSubscription;EventSubscription.objects.filter(pk__in="+repr(ids)+").update(enabled=False);print(json.dumps({'declared_subscription_phase_disabled':True}))",secret=True)
   if event=='epg_error':fixture.sql("from apps.epg.models import EPGSource;EPGSource.objects.filter(pk="+str(source)+").update(password="+repr(' '+credentials['password']+'\x1f')+");print(json.dumps({'owned_credentials_restored':True}))",secret=True)
 for event in ['epg_refresh','epg_error']:
  row={'name':'actual-'+event+'-Connect-form-template-mutation-status-catches','status':'running'};cells.append(row)
  try:
   require_backend_dependency(fixture,private,row['name'])
   row.update(status='passed',actual=event_case(event))
  except BlockedBackendDependency as error:row.update(status='blocked',reason='ActualNativeCommonCeleryFault',task_produced=False)
  except Exception as error:
   row.update(status='failed',error_type=type(error).__name__);p=private/(event+'-failure.txt');p.write_text(traceback.format_exc()[-8192:]);p.chmod(0o600)
  save(out/'report.json',{'status':'running','cases':cells})
 try:fixture.sql("from apps.connect.models import EventSubscription;EventSubscription.objects.filter(pk__in="+repr([r['id']for r in setup['subscriptions']])+").update(enabled=False);print(json.dumps({'all_owned_Connect_subscriptions_disabled':True}))",secret=True)
 except Exception as error:cells.append({'name':'actual-owned-subscription-phase-cleanup','status':'failed','error_type':type(error).__name__})
 report={'status':'passed'if all(c['status']=='passed'for c in cells)else'failed','cases':cells,'unqualified':['enabled scripts and physical script retirement','network exception text and post-send cancellation','non-scalar form payload iteration','apparent/other response charset','HTTP header-null/framing overrides','SQL refusal/unknown transport acceptance','XML refresh/download hook caller composition','large hook trail beyond legacy262KiB metadata cap'],'full_EPG_hook_or_M7_parity':False,'performance_verified':False};save(out/'report.json',report);return report
