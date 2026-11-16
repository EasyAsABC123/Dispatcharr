"""Real API/consumer remote XMLTV effects in the existing SD context/Origin.
Original Dispatcharr/Requests authors retain all application/algorithm credit.
"""
import datetime,hashlib,json,time,traceback
from pathlib import Path
from backend_terminal import require_backend_dependency,wait_backend_terminal,BlockedBackendDependency
CASE='schedules-direct';ORIGIN='http://127.0.0.1:18097'
def save(path,value):path.write_text(json.dumps(value,indent=2,sort_keys=True)+'\n')
def prepare_context(fixture):
 if fixture.case!=CASE:return
 setup=fixture.sql("from apps.epg.models import EPGSource,EPGData;from apps.channels.models import Channel;from core.models import CoreSettings,UserAgent;a=UserAgent.objects.create(name='Owned XMLTV request agent1',user_agent='Owned-XMLTV/1.0');b=UserAgent.objects.create(name='Owned XMLTV request agent2',user_agent='Owned-XMLTV/2.0');settings,_=CoreSettings.objects.get_or_create(key='stream_settings',defaults={'name':'Stream Settings','value':{}});v=dict(settings.value or {});v['default_user_agent']=a.pk;settings.value=v;settings.save(update_fields=['value']);sources=EPGSource.objects.bulk_create([EPGSource(name='Owned HTTP XMLTV main',source_type='xmltv',url='"+ORIGIN+"/xmltv/guide.xml',custom_properties={'auto_apply_epg_logos':False}),EPGSource(name='Owned HTTP XMLTV TVG',source_type='xmltv',url='"+ORIGIN+"/xmltv/guide.xml.gz',file_path='/data/cached_epg/owned-tvg-missing.xml',custom_properties={'auto_apply_epg_logos':False})]);epgs=EPGData.objects.bulk_create([EPGData(epg_source=s,tvg_id='owned-http-guide',name='Owned HTTP guide')for s in sources]);Channel.objects.filter(pk="+str(fixture.channel_id)+").update(epg_data_id=epgs[0].pk);print(json.dumps({'main':sources[0].pk,'tvg_source':sources[1].pk,'tvg_epg':epgs[1].pk,'settings':settings.pk,'agents':[a.pk,b.pk]}))",secret=True)
 fixture._xmltv_download=setup
 save(Path(fixture.output)/'declared-XMLTV-download-inputs.json',{'setup':setup,'HTTP_origin':ORIGIN,'max_wire_bytes':512*1024*1024,'max_decoded_bytes':2*1024*1024*1024,'each_declared_guide_above_bytes':4*1024*1024,'padding_programmes':50000,'retained_target_programmes':1,'per_channel_selected_rows_bound':4096,'per_channel_selected_bytes_bound':4*1024*1024,'new_Source_characterization_required':True})
def run(fixture):
 if fixture.case!=CASE:raise ValueError('OwnedSDContextRequired')
 setup=fixture._xmltv_download;out=Path(fixture.output)/'xmltv-download';out.mkdir(exist_ok=False);private=out/'private';private.mkdir(mode=0o700);cells=[];seen=set()
 # Declared observer phase boundary: preserve the original64/1MiB SD monitor,
 # retire it physically, then start one same-budget monitor for new XML jobs.
 stopped=fixture.role('sd-guide-publications',[],stop=True);save(private/'prior-observer-stop.json',stopped)
 fixture.role('xml-download-publications',['/dispatcharrpy/bin/python','/normal-dvr-tools/broker_monitor.py'],extra={'NORMAL_DVR_LEDGER':'/shared/'+CASE+'/xml-download-publications.jsonl'})
 fixture.wait(lambda:(fixture.case_data/'xml-download-publications.ready').exists(),10,'actual-XML-download-publication-observer')
 def guest(script):
  prefix="import os,sys,json;from urllib.parse import quote;password=quote(os.environ['REDIS_PASSWORD'],safe='');os.environ['CELERY_BROKER_URL']='redis://:'+password+'@localhost:6379/1';os.environ['CELERY_RESULT_BACKEND']='redis://:'+password+'@localhost:6379/2';sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings');import django;django.setup();from celery import current_app;\n"
  return json.loads(fixture.python(prefix+script,secret=True))
 calibration='00000000-0000-0000-0000-000000000000';prefix=guest('print(json.dumps(current_app.backend.get_key_for_task('+repr(calibration)+').decode()))');assert prefix.endswith(calibration);prefix=prefix[:-len(calibration)]
 def terminal(ident):
  raw=fixture.redis(2,"if redis.call('STRLEN',ARGV[1])>1048576 then return redis.error_reply('declared XML result bound')end;return redis.call('GET',ARGV[1])",prefix+ident)
  return None if raw is None else json.loads(raw)
 def wait_terminal(ident,label):
  value=wait_backend_terminal(fixture,terminal,ident,150,'actual-XML-download-terminal-'+label,private);save(private/(label+'-backend.json'),value);assert value['status']=='SUCCESS';return value
 def frames():
  path=fixture.case_data/'xml-download-publications.jsonl'
  if path.stat().st_size>1048576:raise AssertionError('DeclaredXMLDownloadLedgerBudget')
  return [json.loads(line)for line in path.read_text().splitlines()if line.strip()]
 def snapshot(source,label):
  value=fixture.sql('from apps.epg.models import EPGSource,EPGData,ProgramData;from core.models import SystemEvent;s=EPGSource.objects.get(pk='+str(source)+');print(json.dumps({"source":{"id":s.pk,"status":s.status,"last_message":s.last_message,"file_path":s.file_path,"extracted_file_path":s.extracted_file_path},"epgs":list(EPGData.objects.filter(epg_source=s).values("id","tvg_id","name")),"programmes":list(ProgramData.objects.filter(epg__epg_source=s).order_by("start_time").values("id","title","start_time","end_time","tvg_id")),"events":list(SystemEvent.objects.filter(event_type="epg_error",details__source_name=s.name).order_by("id").values("details"))},default=str))',secret=True)
  # Observe the actual committed Source/Native SQL path inside the SAME
  # owned baseline namespace. Source default /app/media is not case_data.
  path=value['source']['file_path']
  script="""import os,stat,json,hashlib,pathlib
raw=PATH
if raw is None:
 print(json.dumps({'file':{'exists':False,'observed_path':None},'private_incoming_names':[]}))
else:
 p=pathlib.Path(raw)
 if not p.is_absolute() or p.parent not in (pathlib.Path('/app/media/cached_epg'),pathlib.Path('/data/cached_epg')):raise ValueError('ActualOwnedXMLCachePathRequired')
 # Native image /app/media is its supported /data symlink; the baseline
 # observer namespace has a different /app/media. Map only this bound alias.
 if OBSERVED_NATIVE and p.parent==pathlib.Path('/app/media/cached_epg'):p=pathlib.Path('/data/cached_epg')/p.name
 names=[v.name for v in p.parent.iterdir()] if p.parent.exists() else []
 if len(names)>4096:raise ValueError('OwnedCacheDirectoryObservationBound')
 observed={'exists':p.is_file(),'observed_path':raw,'physical_observed_path':str(p)}
 if p.is_file():
  resolved=p.resolve(strict=True)
  with resolved.open('rb')as f:
   before=os.fstat(f.fileno())
   if not stat.S_ISREG(before.st_mode)or before.st_size>8388608:raise ValueError('OwnedFixtureFileObservation8MiBBound')
   digest=hashlib.sha256();count=0
   while True:
    chunk=f.read(65536)
    if not chunk:break
    count+=len(chunk)
    if count>8388608:raise ValueError('OwnedFixtureFileObservation8MiBBound')
    digest.update(chunk)
   after=os.fstat(f.fileno())
   if count!=before.st_size or(before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns)!=(after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns):raise ValueError('ActualFileChangedDuringObservation')
  observed.update(bytes=count,sha256=digest.hexdigest(),realpath=str(resolved),path_is_symlink=p.is_symlink(),dev=before.st_dev,inode=before.st_ino,mtime_ns=before.st_mtime_ns)
 print(json.dumps({'file':observed,'private_incoming_names':[v for v in names if v.startswith('.native-xml-')]}))
""".replace('PATH',repr(path)).replace('OBSERVED_NATIVE',repr(fixture.backend=='Native'))
  actual=json.loads(fixture.python(script,secret=True));value['cache_file']=actual['file'];value['private_incoming_names']=actual['private_incoming_names']
  save(private/(label+'-SQL-file.json'),value);return value
 def trace():
  import urllib.request
  script="import urllib.request,json;r=urllib.request.build_opener(urllib.request.ProxyHandler({})).open('"+ORIGIN+"/snapshot',timeout=5);raw=r.read(1048577);assert len(raw)<=1048576;print(raw.decode())"
  return json.loads(fixture.python(script,secret=True))['trace']
 def point(url):fixture.sql('from apps.epg.models import EPGSource;EPGSource.objects.filter(pk='+str(setup['main'])+').update(url='+repr(ORIGIN+url)+');print(json.dumps({"declared_XMLTV_URL_input_changed":True}))',secret=True)
 def refresh(label,path):
  point(path);position=len(trace());reply=fixture.api('POST','/api/epg/import/',{'id':setup['main'],'force':True});save(private/(label+'-API.json'),reply);assert reply['status']==202
  def actual():
   for row in frames():
    head=row.get('headers')or{}
    if head.get('task')=='apps.epg.tasks.refresh_epg_data'and head['id']not in seen:return row
   return False
  row=fixture.wait(actual,20,'actual-XML-refresh-API-publication');ident=row['headers']['id'];seen.add(ident);save(private/(label+'-publication.json'),row)
  parent=wait_terminal(ident,label);assert parent['result']is None
  for n,child in enumerate(parent.get('children')or[]):wait_terminal(child[0][0],label+'-child-'+str(n))
  state=snapshot(setup['main'],label);requests=[r for r in trace()[position:]if r.get('kind')=='owned_XMLTV_request'];save(private/(label+'-Origin.json'),requests);return {'state':state,'requests':requests,'parent':parent}
 def cell(name,body):
  row={'name':name,'status':'running'};cells.append(row)
  try:
   require_backend_dependency(fixture,private,name)
   row.update(status='passed',actual=body())
  except BlockedBackendDependency as error:row.update(status='blocked',reason='ActualNativeCommonCeleryFault',task_produced=False)
  except Exception as error:
   row.update(status='failed',error_type=type(error).__name__);p=private/(name+'-failure.txt');p.write_text(traceback.format_exc()[-8192:]);p.chmod(0o600)
  save(out/'report.json',{'status':'running','cases':cells})
 def installed(value,agent):
  state=value['state'];assert state['cache_file']['exists'] and state['cache_file']['exists'] and state['cache_file']['bytes']>4*1024*1024 and not state['private_incoming_names'];assert len(state['programmes'])==1 and state['programmes'][0]['title']=='Owned HTTP programme';assert state['source']['extracted_file_path']is None;assert all(r['user_agent']==agent for r in value['requests']);return value
 def plain():return installed(refresh('plain','/xmltv/guide.xml'),'Owned-XMLTV/1.0')
 def gzip_archive():
  value=installed(refresh('archive','/xmltv/guide.xml.gz'),'Owned-XMLTV/1.0');assert value['requests'][-1]['compression']=='archive-gzip';return value
 def content_encoding():
  value=installed(refresh('encoded','/xmltv/encoded.xml'),'Owned-XMLTV/1.0');r=value['requests'][-1];assert r['compression']=='HTTP-gzip'and r['wire_bytes']<r['decoded_fixture_bytes'];return value
 def redirect_and_cached_agent():
  settings=fixture.sql('from core.models import CoreSettings;s=CoreSettings.objects.get(pk='+str(setup['settings'])+');print(json.dumps(s.value))',secret=True);settings['default_user_agent']=setup['agents'][1]
  response=fixture.api('PATCH','/api/core/settings/'+str(setup['settings'])+'/',{'value':settings});save(private/'cached-agent-PATCH.json',response);assert response['status']==200
  value=installed(refresh('redirect','/xmltv/redirect.xml'),'Owned-XMLTV/2.0');assert [r['status']for r in value['requests']]==[302,200];return value
 def rejection(label,path,status):
  before=snapshot(setup['main'],label+'-before');value=refresh(label,path);after=value['state'];assert {k:v for k,v in before['cache_file'].items()if k in ('exists','bytes','sha256')}=={k:v for k,v in after['cache_file'].items()if k in ('exists','bytes','sha256')}and before['programmes']==after['programmes'];assert after['source']['status']=='error'and after['events'];assert value['requests'][-1]['status']==status;assert not after['private_incoming_names'];return value
 def tvg_missing():
  made=guest('from apps.epg.tasks import parse_programs_for_tvg_id;t=parse_programs_for_tvg_id.apply_async(args=['+str(setup['tvg_epg'])+'],kwargs={"force":True});print(json.dumps({"id":t.id}))');save(private/'TVG-missing-producer.json',made);value=wait_terminal(made['id'],'TVG-missing');assert value['result']is None;state=snapshot(setup['tvg_source'],'TVG-missing');assert len(state['programmes'])==1 and state['cache_file']['exists'] and state['cache_file']['bytes']>4*1024*1024 and not state['private_incoming_names'];return {'actual_forced_TVG_download_then_streamed_parse':True,'state':state}
 cell('actual-API-plain-above4MiB-import-descendants',plain);cell('actual-API-gzip-archive-preserving-promotion',gzip_archive);cell('actual-API-HTTP-content-encoding-wire-decoded-sizes',content_encoding);cell('actual-API-redirect-cached-user-agent-invalidation',redirect_and_cached_agent);cell('actual-API-404-preserves-prior-cache-and-programmes',lambda:rejection('404','/xmltv/missing.xml',404));cell('actual-API-incomplete-preserves-prior-cache-and-programmes',lambda:rejection('incomplete','/xmltv/incomplete.xml',200));cell('actual-missing-TVG-owned-download-then-parse',tvg_missing)
 report={'status':'passed'if all(c['status']=='passed'for c in cells)else'failed','cases':cells,'full_remote_XMLTV_parity':False,'performance_verified':False,'unqualified':['HTTP timeout/connection interruption physical cells','511MiB/2GiB boundaries','zip/xz alternatives','enabled plugin/Connect hooks','selected channel programme4096/4MiB limit','read-only index acceleration and same-family Source6 concurrency']};save(out/'report.json',report);return report
