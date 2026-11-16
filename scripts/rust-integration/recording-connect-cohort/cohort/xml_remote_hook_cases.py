"""One additional remote XML failure hook phase in the EXISTING schedules-direct context.
Actual pinned Source producer -> real backend consumer; no Native API parity claim.
Original Dispatcharr/Django/Celery/Requests authors retain algorithm credit.
"""
import base64,json,traceback,urllib.parse
from pathlib import Path
from backend_terminal import require_backend_dependency,wait_backend_terminal,BlockedBackendDependency
CASE='schedules-direct';ORIGIN='http://127.0.0.1:18097';NAME='Owned HTTP XMLTV main'
def save(path,value):path.write_text(json.dumps(value,indent=2,sort_keys=True)+'\n')
def prepare_context(fixture):
 if fixture.case!=CASE:return
 fixture._remote_xml_hooks={'source':fixture._xmltv_download['main']}
 save(Path(fixture.output)/'declared-remote-XML-hook-inputs.json',{'source':fixture._remote_xml_hooks,'required_prior':'existing xmltv_download prepare and successful install phase','HTTP_notification_targets':'same owned SD Origin only','new_paired_Source_Native_required':True,'pending_rejection_not_legacy_closed_proof':True})
def run(fixture):
 if fixture.case!=CASE:raise ValueError('OwnedSDContextRequired')
 out=Path(fixture.output)/'xml-remote-hooks';out.mkdir();private=out/'private';private.mkdir(mode=0o700)
 ids=[r['id']for r in fixture._epg_connect['subscriptions']if r['event']=='epg_error'];labels={r['id']:r['label']for r in fixture._epg_connect['subscriptions']if r['event']=='epg_error'}
 def guest(script):
  prefix="import os,sys,json;from urllib.parse import quote;p=quote(os.environ['REDIS_PASSWORD'],safe='');os.environ['CELERY_BROKER_URL']='redis://:'+p+'@localhost:6379/1';os.environ['CELERY_RESULT_BACKEND']='redis://:'+p+'@localhost:6379/2';sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings');import django;django.setup();from celery import current_app;\n"
  return json.loads(fixture.python(prefix+script,secret=True))
 def trace():return json.loads(fixture.python("import urllib.request,json;r=urllib.request.build_opener(urllib.request.ProxyHandler({})).open('"+ORIGIN+"/snapshot',timeout=5);raw=r.read(1048577);assert len(raw)<=1048576;print(raw.decode())",secret=True))['trace']
 def sqlstate():return fixture.sql("from apps.connect.models import DeliveryLog;from core.models import SystemEvent;from apps.epg.models import EPGSource,ProgramData;s=EPGSource.objects.get(pk="+str(fixture._remote_xml_hooks['source'])+");print(json.dumps({'source':{'id':s.pk,'status':s.status},'programmes':list(ProgramData.objects.filter(epg__epg_source=s).values('id','title')),'audit':list(SystemEvent.objects.filter(event_type='epg_error',details__source_name="+repr(NAME)+").values('id','details')),'deliveries':list(DeliveryLog.objects.filter(subscription_id__in="+repr(ids)+").values('id','subscription_id','status','request_payload','response_payload','error_message'))},default=str))",secret=True)
 row={'name':'actual-remote-XML-rejection-Connect-hooks-retired-before-SUCCESS','status':'running'}
 try:
  require_backend_dependency(fixture,private,row['name'])
  zero='00000000-0000-0000-0000-000000000000';prefix=guest('print(json.dumps(current_app.backend.get_key_for_task('+repr(zero)+').decode()))');assert prefix.endswith(zero);prefix=prefix[:-len(zero)]
  before=sqlstate();save(private/'before.json',before);prior={r['id']for r in before['deliveries']};position=len(trace())
  fixture.sql('from apps.connect.models import EventSubscription;EventSubscription.objects.filter(pk__in='+repr(ids)+').update(enabled=True);print(json.dumps({"owned_refresh_subscriptions_enabled":True}))',secret=True)
  fixture.sql('from apps.epg.models import EPGSource;EPGSource.objects.filter(pk='+str(fixture._remote_xml_hooks['source'])+').update(url='+repr(ORIGIN+'/xmltv/missing.xml')+');print(json.dumps({"owned_missing_XMLTV_URL_selected":True}))',secret=True)
  made=guest('from apps.epg.tasks import refresh_epg_data;t=refresh_epg_data.apply_async(args=['+str(fixture._remote_xml_hooks['source'])+'],kwargs={"force":True});print(json.dumps({"id":t.id}))');save(private/'producer.json',made)
  def terminal(ident):
   raw=fixture.redis(2,"if redis.call('STRLEN',ARGV[1])>1048576 then return redis.error_reply('Local hook metadata bound')end;return redis.call('GET',ARGV[1])",prefix+ident)
   return None if raw is None else json.loads(raw)
  backend=wait_backend_terminal(fixture,terminal,made['id'],150,'actual-remote-XML-hook-parent-terminal',private);save(private/'backend.json',backend);assert backend['status']=='SUCCESS'and backend['result']is None
  children=backend.get('children')or[];child_results=[]
  for child in children:
   ident=child[0][0];meta=wait_backend_terminal(fixture,terminal,ident,90,'actual-remote-XML-hook-descendant',private);child_results.append(meta);assert meta['status']=='SUCCESS'and meta.get('parent_id')==made['id']
  save(private/'children.json',child_results);assert not children
  after=sqlstate();save(private/'after.json',after);requests=[r for r in trace()[position:]if r.get('kind')=='owned_connect_request'];save(private/'Origin.json',requests)
  logs={labels[r['subscription_id']]:r for r in after['deliveries']if r['id']not in prior};assert set(logs)=={'plain','json','rejected','invalid','unknown','nan'}
  assert after['source']['status']=='error'and before['programmes']and after['programmes']==before['programmes']
  assert len(requests)==5 and [r['path']for r in requests]==['/connect/plain','/connect/json','/connect/rejected','/connect/fallback','/connect/nan']
  assert all(logs[x]['status']=='success'for x in ['plain','json','invalid','nan']);assert logs['rejected']['status']=='failed'and logs['rejected']['response_payload']['status_code']==503
  assert logs['unknown']['status']=='failed'and logs['unknown']['error_message']=="No handler for integration type 'api'"
  assert'chosen'not in logs['plain']['request_payload']and all(logs[x]['request_payload']['chosen']==NAME for x in ['rejected','invalid','unknown'])
  assert json.loads(logs['json']['request_payload'])=={'source':NAME}and logs['nan']['request_payload']=='NaN'
  assert len(after['audit'])==len(before['audit'])+1 and all('chosen'not in r['details']for r in after['audit'])
  assert base64.b64decode(requests[-1]['body_base64'],validate=True)==b'NaN'
  row.update(status='passed',actual={'parent':made['id'],'settled_native_children':0,'deliveries':6,'owned_HTTP_requests':5,'shared_template_mutation':True,'actual_backend_SUCCESS':True,'Native_API_producer_parity_claimed':False})
 except BlockedBackendDependency as error:row.update(status='blocked',reason='ActualNativeCommonCeleryFault',task_produced=False)
 except Exception as error:
  row.update(status='failed',error_type=type(error).__name__);p=private/'failure.txt';p.write_text(traceback.format_exc()[-8192:]);p.chmod(0o600)
 finally:
  try:fixture.sql('from apps.connect.models import EventSubscription;EventSubscription.objects.filter(pk__in='+repr(ids)+').update(enabled=False);print(json.dumps({"owned_refresh_subscriptions_disabled":True}))',secret=True)
  except Exception as error:row.update(status='failed',cleanup_error_type=type(error).__name__)
 report={'status':row['status'],'cases':[row],'unqualified':['remote enabled-hook physical cancellation/quarantine','local failure caught-task backend classification','script hooks and physical retirement','hook children beyond262KiB backend wire','full M6/M7/Source47 parity','performance']};save(out/'report.json',report);return report
