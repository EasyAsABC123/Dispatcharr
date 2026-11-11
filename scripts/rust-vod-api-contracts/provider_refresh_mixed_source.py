"""Actual Original/native HTTP mixed-progress experiment. No performance claim.
Hold two upstream refreshes, then ordinary+cached reads on the same VOD router.
Original Dispatcharr authors retain application/algorithm credit.
"""
import os,sys,runpy,json,time,datetime,threading,http.client,hashlib,subprocess
from pathlib import Path
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from urllib.parse import urlsplit,parse_qs
from concurrent.futures import ThreadPoolExecutor
os.environ['VOD_REFRESH_CASE_FILTER']='__mixed_bootstrap__'
context=runpy.run_path(str(Path(__file__).with_name('provider_refresh_source.py')))
User=context['User'];Movie=context['Movie'];M3UAccount=context['M3UAccount'];M3UMovieRelation=context['M3UMovieRelation'];timezone=context['timezone'];prefix=context['prefix'];restore=context['restore'];fingerprints=context['fingerprints'];Encoder=context['Encoder'];baseline=context['baseline'];sequences=context['sequences'];baseline_sequences=context['baseline_sequences'];state=context['state']
Series=context['Series'];M3USeriesRelation=context['M3USeriesRelation']
refresh_kind=os.environ.get('VOD_REFRESH_MIXED_KIND','movie');assert refresh_kind in ['movie','series']
from rest_framework_simplejwt.tokens import AccessToken
binary=os.environ.get('VOD_REFRESH_ACTOR_NATIVE');assert binary and Path(binary).is_file()
held=threading.Event();condition=threading.Condition();pending=0;requests=[];origin_failures=[]
class Origin(BaseHTTPRequestHandler):
 def do_GET(self):
  global pending
  query=parse_qs(urlsplit(self.path).query);action=query.get('action',['authenticate'])[0]
  if urlsplit(self.path).path!='/player_api.php'or action not in ['authenticate','get_vod_info','get_series_info']:origin_failures.append(self.path)
  requests.append({'method':self.command,'query':query})
  if action in ['get_vod_info','get_series_info']:
   with condition:pending+=1;condition.notify_all()
   if not held.wait(12):origin_failures.append('Owned hold safety deadline')
   data={'info':{'plot':'Mixed refreshed metadata'},**({'episodes':{}}if action=='get_series_info'else{})}
  else:data={'user_info':{'auth':1}}
  body=json.dumps(data,separators=(',',':')).encode();self.send_response(200);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
 def log_message(self,*args):pass
origin=ThreadingHTTPServer(('127.0.0.1',0),Origin);threading.Thread(target=origin.serve_forever,daemon=True).start()
movies=list(Movie.objects.filter(m3u_relations__m3u_account__is_active=True).distinct().order_by('id')[:3]);assert len(movies)==3
refresh_parents=movies[:2]if refresh_kind=='movie'else list(Series.objects.filter(pk__in=M3USeriesRelation.objects.filter(m3u_account__is_active=True).values('series_id')).order_by('id')[:2]);assert len(refresh_parents)==2
user=User.objects.get(username=prefix+'standard');token=str(AccessToken.for_user(user))
def request(port,path):
 client=http.client.HTTPConnection('127.0.0.1',port,timeout=15);start=time.monotonic()
 try:
  client.request('GET',path,headers={'Host':'localhost','Accept':'application/json','Authorization':'Bearer '+token});response=client.getresponse();body=response.read(4*1024*1024+1);assert len(body)<=4*1024*1024
  return{'path':path,'status':response.status,'body_sha256':hashlib.sha256(body).hexdigest(),'body_base64':__import__('base64').b64encode(body).decode(),'headers':{k:response.getheader(k)for k in ['Content-Type','Content-Length','Allow','Vary','WWW-Authenticate','X-Content-Type-Options','X-Frame-Options','Referrer-Policy','Cross-Origin-Opener-Policy']},'elapsed_seconds_diagnostic':time.monotonic()-start}
 finally:client.close()
def prepare():
 global pending
 restore();held.clear();pending=0;requests.clear();now=setup_now;M3UAccount.objects.filter(name__startswith=prefix).update(server_url='http://127.0.0.1:'+str(origin.server_port))
 for movie in movies[:2]:M3UMovieRelation.objects.filter(movie=movie).update(custom_properties={'detailed_fetched':True,'detailed_info':{'plot':'Old cached'}},last_advanced_refresh=now-datetime.timedelta(hours=25))
 if refresh_kind=='series':
  for parent in refresh_parents:M3USeriesRelation.objects.filter(series=parent).update(custom_properties={'episodes_fetched':True,'detailed_fetched':True},last_episode_refresh=now-datetime.timedelta(hours=25))
 M3UMovieRelation.objects.filter(movie=movies[2]).update(custom_properties={'detailed_fetched':True,'detailed_info':{'plot':'Fresh cached'}},last_advanced_refresh=now-datetime.timedelta(hours=1))
setup_now=timezone.now()
records=[];server=None;server_log=None
try:
 for target,port in [('Original',5656),('native',9193)]:
  prepare()
  if target=='native':
   ready=Path('/evidence/native-refresh-ready');stop=Path('/evidence/native-refresh-stop');ready.unlink(missing_ok=True);stop.unlink(missing_ok=True);server_log=open('/evidence/mixed-native-server-private.log','w');os.chmod('/evidence/mixed-native-server-private.log',0o600)
   server=subprocess.Popen([binary,'vod_api_draft::vod_api_provider_refresh_peer::native_refresh_http_server','--ignored','--nocapture','--test-threads=1'],env=dict(os.environ,VOD_API_NATIVE_FIXTURE='playback-fixture-only'),stdin=subprocess.DEVNULL,stdout=server_log,stderr=subprocess.STDOUT)
   deadline=time.monotonic()+10
   while not ready.exists():
    assert server.poll()is None,'Native peer exited before readiness'
    assert time.monotonic()<deadline,'Native readiness deadline';time.sleep(.02)
  phase_before=state();before_hash=fingerprints();phase_started=timezone.now()
  with ThreadPoolExecutor(max_workers=4)as executor:
   refreshes=[executor.submit(request,port,'/api/vod/'+('movies'if refresh_kind=='movie'else'series')+'/'+str(parent.pk)+'/provider-info/')for parent in refresh_parents]
   try:
    with condition:assert condition.wait_for(lambda:pending==2,timeout=5),'Both refreshes must actually reach held upstream'
    controls=[executor.submit(request,port,path)for path in ['/api/vod/movies/?ordering=name','/api/vod/movies/'+str(movies[2].pk)+'/provider-info/']]
    observations=[future.result(timeout=10)for future in controls]
    actual_pending=pending;assert not held.is_set()
   finally:held.set()
   refreshed=[future.result(timeout=15)for future in refreshes]
  phase_finished=timezone.now();phase_after=state();after_hash=fingerprints()
  records.append({'before':phase_before,'after':phase_after,'before_fingerprints':before_hash,'after_fingerprints':after_hash,'wall_started':phase_started,'wall_finished':phase_finished,'target':target,'two_upstreams_held_before_controls':actual_pending==2,'controls':observations,'refreshes':refreshed,'origin_requests':list(requests),'pending_origin_at_controls':actual_pending})
  if server:
   Path('/evidence/native-refresh-stop').write_text('stop');server.wait(timeout=15);assert server.returncode==0;server=None;server_log.close();server_log=None
 source,native=records
 compare=lambda row:{k:v for k,v in row.items()if k!='elapsed_seconds_diagnostic'}
 exact=all(compare(a)==compare(b)for a,b in zip(source['controls'],native['controls']))
 exact_refresh=all(compare(a)==compare(b)for a,b in zip(source['refreshes'],native['refreshes']))
 differences=[];clocks=[]
 if source['before_fingerprints']!=native['before_fingerprints']:differences.append({'kind':'setup_fingerprints'})
 for table,expected_rows in source['after'].items():
  actual_rows=native['after'][table];before={r['id']:r for r in source['before'][table]}
  if [r['id']for r in expected_rows]!=[r['id']for r in actual_rows]:differences.append({'table':table,'kind':'ordered_ID_set'});continue
  for expected,actual in zip(expected_rows,actual_rows):
   for key,value in expected.items():
    old=before.get(expected['id'],{}).get(key)
    changed_clock=key in ['created_at','updated_at','last_advanced_refresh','last_episode_refresh','last_seen']and value!=old
    if changed_clock:
     a=isinstance(value,datetime.datetime)and source['wall_started']<=value<=source['wall_finished'];b=isinstance(actual[key],datetime.datetime)and native['wall_started']<=actual[key]<=native['wall_finished']and actual[key]!=old
     clocks.append({'table':table,'id':expected['id'],'field':key,'Source':value,'native':actual[key],'Source_own_interval':a,'native_own_interval':b})
     if not(a and b):differences.append({'table':table,'id':expected['id'],'field':key,'kind':'causal_clock'})
    elif json.dumps(value,cls=Encoder,separators=(',',':'))!=json.dumps(actual[key],cls=Encoder,separators=(',',':')):differences.append({'table':table,'id':expected['id'],'field':key,'kind':'field_value'})
 changed_tables={table for table in source['before_fingerprints']if source['before_fingerprints'][table]!=source['after_fingerprints'][table]}
 for phase in records:
  unexpected=[table for table in phase['before_fingerprints']if table not in changed_tables and phase['before_fingerprints'][table]!=phase['after_fingerprints'][table]]
  if unexpected:differences.append({'target':phase['target'],'kind':'unexpected_model_mutations','tables':unexpected})
 passed=not origin_failures and exact and exact_refresh and not differences and all(row['status']==200 for phase in records for row in phase['controls']+phase['refreshes'])
 report={'passed':passed,'scope':__doc__,'records':records,'exact_ordinary_cached_control_wire':exact,'refresh_kind':refresh_kind,'exact_refreshed_wire':exact_refresh,'exact_refreshed_Movie_wire':exact_refresh if refresh_kind=='movie'else None,'SQL_differences':differences,'own_interval_SQL_clocks':clocks,'origin_failures':origin_failures,'native_API_acceptance':False}
finally:
 held.set()
 if server:
  Path('/evidence/native-refresh-stop').write_text('stop')
  try:server.wait(timeout=15)
  except subprocess.TimeoutExpired:server.terminate();server.wait(timeout=5)
 if server_log:server_log.close()
 origin.shutdown();origin.server_close();restore()
if 'report'not in globals():report={'passed':False,'scope':__doc__,'records':records}
report.update(restored18=fingerprints()==baseline,sequences_restored=sequences()==baseline_sequences);print(json.dumps(report,cls=Encoder))
