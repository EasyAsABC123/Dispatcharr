"""Unexecuted finite24 real Native router collector; literal bodies stay literal."""
import datetime,hashlib,json,os,re,time
from pathlib import Path
from absolute_http import Observation
from native_state import state,need
ROOT=Path('/native-root');OUT=Path('/evidence')
def utc():return datetime.datetime.now(datetime.timezone.utc).isoformat()
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def main():
 need(os.environ.get('PLUGIN_REPO_NATIVE_CAPTURE')=='owned-native-repository-only','OwnedNativeCollector');binding=json.loads((ROOT/'inputs/authority.json').read_bytes());source_path=ROOT/'inputs/Source24-private.json';need(sha(source_path)==binding['source_raw_sha256'],'ExactOriginalSource24NoFallback');source=json.loads(source_path.read_bytes());plan_path=ROOT/'inputs/plan.json';need(sha(plan_path)==binding['plan_sha256'],'ExactPlan');plan=json.loads(plan_path.read_bytes());schema=json.loads((ROOT/'inputs/schema62-authority.json').read_bytes());ready=json.loads((OUT/'peer-ready.json').read_bytes());database=ready['database'];need(ready['actual_adapter']=='PluginRepositories::router'and ready['Main']is False and ready['cfgtest_business_substitution']is False,'RealCompiledInertRouter');address=ready['address'];need(re.fullmatch(r'127\.0\.0\.1:[1-9][0-9]{0,4}',address)is not None,'ExactOwnedTCPPeer')
 users={u['id']:u for u in source['seed']['users']};roles={r['alias']:r for r in plan['roles']};rows=[];began=time.monotonic();target=OUT/'Native24-private.json';need(not target.exists(),'FreshNative24Capture');result={'capture_only':True,'passed':False,'capture_complete':False,'actual_request_count':0,'completed_capture_count':0,'Source_scope':'retained APIClient/APIkey only; no new Source run','Native_transport':'actual loopback HTTP to compiled inert PluginRepositories router','raw_body_byte_parity_claimed':False,'final_Source_framework_vs_Native500_gap':True,'Main_or_fullM8':False,'seed_import':json.loads((OUT/'import-existing-state.json').read_bytes()),'peer_ready':ready,'before_requests':state(database,schema),'requests':rows,'phase_began_utc':utc()}
 def save():
  raw=json.dumps(result,indent=2,allow_nan=False).encode();need(len(raw)<=134217728,'NativeCapture128MiB');target.write_bytes(raw)
 save()
 try:
  for actual,spec in zip(source['requests'],plan['cases']):
   need(time.monotonic()-began<120,'FiniteNativePhase120');need(actual['index']==spec['index']and actual['name']==spec['name']and actual['method']==spec['method']and actual['body_hex']==spec['body_hex'],'ExactMeasured24InputOrder')
   path=actual['path'];need(re.fullmatch(r'/api/plugins/repos/(?:[0-9]{1,12}/)?',path)is not None,'ExactResolvedRouteGrammar');body=bytes.fromhex(spec['body_hex']);need(hashlib.sha256(body).hexdigest()==spec['body_sha256']and len(body)==spec['body_bytes'],'LiteralInputBody');headers={'Content-Type':spec['content_type']}
   if actual['actor']!='anonymous':headers['X-Api-Key']=users[roles[actual['actor']]['id']]['api_key']
   row={'index':actual['index'],'name':actual['name'],'actor':actual['actor'],'method':actual['method'],'path':path,'body_hex':body.hex(),'body_sha256':hashlib.sha256(body).hexdigest(),'body_bytes':len(body),'raw_headers_private':headers,'before':state(database,schema),'began_utc':utc(),'began_monotonic':time.monotonic(),'actual_request_invoked':False,'capture_complete':False};rows.append(row);save();observation=Observation('http://'+address,seconds=8);response=None;result['actual_request_count']+=1;row['actual_request_invoked']=True
   try:
    response=observation.request(path,headers,method=actual['method'],body=body);data=response.raw.read(1048577);need(len(data)<=1048576,'BoundedActualResponseBody');row.update(status=response.status_code,headers=list(response.raw.getheaders()),response_body_hex=data.hex(),response_body_bytes=len(data),response_body_sha256=hashlib.sha256(data).hexdigest(),json=json.loads(data),absolute_observation=observation.complete())
   finally:row['physical_close']=observation.cancel()
   row.update(completed_utc=utc(),completed_monotonic=time.monotonic(),after=state(database,schema),capture_complete=True);result['completed_capture_count']+=1;save()
  need(len(rows)==result['actual_request_count']==result['completed_capture_count']==24,'Exactly24NoExtraHTTPReadiness');result['capture_complete']=True
 finally:result['phase_completed_utc']=utc();result['phase_elapsed_seconds']=time.monotonic()-began;save()
if __name__=='__main__':main()
