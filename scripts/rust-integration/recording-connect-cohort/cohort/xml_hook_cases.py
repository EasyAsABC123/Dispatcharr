"""One additional local XML hook phase in the EXISTING schedules-direct context.
Actual pinned Source producer -> real backend consumer; no Native API parity claim.
Original Dispatcharr/Django/Celery/Requests authors retain algorithm credit.
"""
import base64,json,traceback,urllib.parse
from pathlib import Path
from backend_terminal import require_backend_dependency,wait_backend_terminal,BlockedBackendDependency
CASE='schedules-direct';ORIGIN='http://127.0.0.1:18097';NAME='Owned local XML scoped hooks'
def save(path,value):path.write_text(json.dumps(value,indent=2,sort_keys=True)+'\n')
def require_local_audit_delta(before,after):
 # Earlier real scanner tasks may legitimately populate the same source.
 # Preserve every observed prior row; qualify only this completed task window.
 prior={row['id']:row for row in before['audit']}
 current={row['id']:row for row in after['audit']}
 assert len(prior)==len(before['audit'])and len(current)==len(after['audit'])
 assert all(ident in current and current[ident]==row for ident,row in prior.items())
 added=[row for row in after['audit']if row['id']not in prior]
 assert len(added)==1 and all('chosen'not in row['details']for row in after['audit'])
 return added[0]
def prepare_context(fixture):
 if fixture.case!=CASE:return
 root=fixture.case_data/'epgs';root.mkdir(exist_ok=True)
 file=root/'local-hooks.xml';file.write_text('<?xml version="1.0"?><tv><channel id="owned-local-hooks"><display-name>Owned local hooks channel</display-name></channel><programme start="20600101000000 +0000" stop="20600101010000 +0000" channel="owned-local-hooks"><title>Owned local hook programme</title></programme></tv>');file.chmod(0o444)
 fixture._xml_hooks=fixture.sql("from apps.epg.models import EPGSource,EPGData;from apps.channels.models import Channel;s=EPGSource.objects.bulk_create([EPGSource(name="+repr(NAME)+",source_type='xmltv',url='',file_path='/data/epgs/local-hooks.xml',custom_properties={'auto_apply_epg_logos':False})])[0];e=EPGData.objects.bulk_create([EPGData(epg_source=s,tvg_id='owned-local-hooks',name='Owned local hooks channel')])[0];c=Channel.objects.bulk_create([Channel(name='Owned local hooks channel',channel_number=64800,epg_data=e)])[0];print(json.dumps({'source':s.pk,'epg':e.pk,'channel':c.pk}))",secret=True)
 save(Path(fixture.output)/'declared-local-XML-hook-inputs.json',{'source':fixture._xml_hooks,'file':'/data/epgs/local-hooks.xml','source_name':NAME,'producer':'actual pinned Source refresh_epg_data.apply_async','new_paired_observations_required':True,'requires_existing_epg_connect_prepare':True,'HTTP_notification_targets':'same owned SD Origin only','no_new_fixture_or_application':True,'remote_download_hooks':False})
def run(fixture):
 if fixture.case!=CASE:raise ValueError('OwnedSDContextRequired')
 out=Path(fixture.output)/'xml-local-hooks';out.mkdir();private=out/'private';private.mkdir(mode=0o700)
 ids=[r['id']for r in fixture._epg_connect['subscriptions']if r['event']=='epg_refresh'];labels={r['id']:r['label']for r in fixture._epg_connect['subscriptions']if r['event']=='epg_refresh'}
 def guest(script):
  prefix="import os,sys,json;from urllib.parse import quote;p=quote(os.environ['REDIS_PASSWORD'],safe='');os.environ['CELERY_BROKER_URL']='redis://:'+p+'@localhost:6379/1';os.environ['CELERY_RESULT_BACKEND']='redis://:'+p+'@localhost:6379/2';sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings');import django;django.setup();from celery import current_app;\n"
  return json.loads(fixture.python(prefix+script,secret=True))
 def trace():return json.loads(fixture.python("import urllib.request,json;r=urllib.request.build_opener(urllib.request.ProxyHandler({})).open('"+ORIGIN+"/snapshot',timeout=5);raw=r.read(1048577);assert len(raw)<=1048576;print(raw.decode())",secret=True))['trace']
 def sqlstate():return fixture.sql("from apps.connect.models import DeliveryLog;from core.models import SystemEvent;from apps.epg.models import EPGSource,ProgramData;s=EPGSource.objects.get(pk="+str(fixture._xml_hooks['source'])+");print(json.dumps({'source':{'id':s.pk,'status':s.status},'programmes':list(ProgramData.objects.filter(epg__epg_source=s).values('id','title')),'audit':list(SystemEvent.objects.filter(event_type='epg_refresh',details__source_name="+repr(NAME)+").values('id','details')),'deliveries':list(DeliveryLog.objects.filter(subscription_id__in="+repr(ids)+").values('id','subscription_id','status','request_payload','response_payload','error_message'))},default=str))",secret=True)
 row={'name':'actual-local-XML-Connect-template-caught-continuation-and-terminal-trail','status':'running'}
 try:
  require_backend_dependency(fixture,private,row['name'])
  zero='00000000-0000-0000-0000-000000000000';prefix=guest('print(json.dumps(current_app.backend.get_key_for_task('+repr(zero)+').decode()))');assert prefix.endswith(zero);prefix=prefix[:-len(zero)]
  before=sqlstate();save(private/'before.json',before);prior={r['id']for r in before['deliveries']};position=len(trace())
  fixture.sql('from apps.connect.models import EventSubscription;EventSubscription.objects.filter(pk__in='+repr(ids)+').update(enabled=True);print(json.dumps({"owned_refresh_subscriptions_enabled":True}))',secret=True)
  made=guest('from apps.epg.tasks import refresh_epg_data;t=refresh_epg_data.apply_async(args=['+str(fixture._xml_hooks['source'])+'],kwargs={"force":True});print(json.dumps({"id":t.id}))');save(private/'producer.json',made)
  def terminal(ident):
   raw=fixture.redis(2,"if redis.call('STRLEN',ARGV[1])>1048576 then return redis.error_reply('Local hook metadata bound')end;return redis.call('GET',ARGV[1])",prefix+ident)
   return None if raw is None else json.loads(raw)
  backend=wait_backend_terminal(fixture,terminal,made['id'],150,'actual-local-XML-hook-parent-terminal',private);save(private/'backend.json',backend);assert backend['status']=='SUCCESS'and backend['result']is None
  children=backend.get('children')or[];child_results=[]
  for child in children:
   ident=child[0][0];meta=wait_backend_terminal(fixture,terminal,ident,90,'actual-local-XML-hook-descendant',private);child_results.append(meta);assert meta['status']=='SUCCESS'and meta.get('parent_id')==made['id']
  save(private/'children.json',child_results);assert len(children)==2
  after=sqlstate();save(private/'after.json',after);requests=[r for r in trace()[position:]if r.get('kind')=='owned_connect_request'];save(private/'Origin.json',requests)
  logs={labels[r['subscription_id']]:r for r in after['deliveries']if r['id']not in prior};assert set(logs)=={'plain','json','rejected','invalid','unknown','nan'}
  assert after['source']['status']=='success'and after['programmes']==[{'id':after['programmes'][0]['id'],'title':'Owned local hook programme'}]
  assert len(requests)==5 and [r['path']for r in requests]==['/connect/plain','/connect/json','/connect/rejected','/connect/fallback','/connect/nan']
  assert all(logs[x]['status']=='success'for x in ['plain','json','invalid','nan']);assert logs['rejected']['status']=='failed'and logs['rejected']['response_payload']['status_code']==503
  assert logs['unknown']['status']=='failed'and logs['unknown']['error_message']=="No handler for integration type 'api'"
  assert'chosen'not in logs['plain']['request_payload']and all(logs[x]['request_payload']['chosen']==NAME for x in ['rejected','invalid','unknown'])
  assert json.loads(logs['json']['request_payload'])=={'source':NAME}and logs['nan']['request_payload']=='NaN'
  require_local_audit_delta(before,after)
  assert base64.b64decode(requests[-1]['body_base64'],validate=True)==b'NaN'
  row.update(status='passed',actual={'parent':made['id'],'settled_native_children':2,'deliveries':6,'owned_HTTP_requests':5,'shared_template_mutation':True,'actual_backend_SUCCESS':True,'Native_API_producer_parity_claimed':False})
 except BlockedBackendDependency as error:row.update(status='blocked',reason='ActualNativeCommonCeleryFault',task_produced=False)
 except Exception as error:
  row.update(status='failed',error_type=type(error).__name__);p=private/'failure.txt';p.write_text(traceback.format_exc()[-8192:]);p.chmod(0o600)
 finally:
  try:fixture.sql('from apps.connect.models import EventSubscription;EventSubscription.objects.filter(pk__in='+repr(ids)+').update(enabled=False);print(json.dumps({"owned_refresh_subscriptions_disabled":True}))',secret=True)
  except Exception as error:row.update(status='failed',cleanup_error_type=type(error).__name__)
 report={'status':row['status'],'cases':[row],'unqualified':['enabled remote download failure hooks remain guarded','local failure caught-task backend classification','script hooks and physical retirement','hook children beyond262KiB backend wire','full M6/M7/Source47 parity','performance']};save(out/'report.json',report);return report
