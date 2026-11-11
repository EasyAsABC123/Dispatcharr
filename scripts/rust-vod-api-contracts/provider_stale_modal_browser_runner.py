"""Owned unchanged-React stale modal controller with exact per-backend SQL reset.
Original Dispatcharr authors retain application/algorithm credit and licensing.
No standalone peer, callable/task replacement, resource sampler or eager mode.
"""
import datetime,hashlib,json,secrets,subprocess,uuid
from pathlib import Path

CLOCK_FIELDS={'created_at','updated_at','last_seen','last_advanced_refresh','last_episode_refresh','last_login','timestamp'}
def stamp(value):
 return datetime.datetime.fromisoformat(value.replace('Z','+00:00')).timestamp()
def digest(value):return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()
def strict_equal(a,b):
 if type(a)is not type(b):return False
 if isinstance(a,dict):return set(a)==set(b)and all(strict_equal(a[k],b[k])for k in a)
 if isinstance(a,list):return len(a)==len(b)and all(strict_equal(x,y)for x,y in zip(a,b))
 return a==b
def compare_rows(a,b,intervals):
 differences=[];clocks=[];entropy=[]
 assert strict_equal(a['prepared_rows'],b['prepared_rows']),'Prepared18rows must be exactly equal before SQL comparison'
 if set(a['rows'])!=set(b['rows']):differences.append('table-set')
 for table,source in a['rows'].items():
  native=b['rows'].get(table,[]);before={r['id']:r for r in a['prepared_rows'][table]}
  if len(source)!=len(native):differences.append(table+'/row-count');continue
  for x,y in zip(source,native):
   if x['id']!=y['id']:differences.append(table+'/ordered-id');continue
   if set(x)!=set(y):differences.append(table+'/'+str(x['id'])+'/keys');continue
   for field,value in x.items():
    other=y.get(field);path=table+'/'+str(x['id'])+'/'+field
    if strict_equal(value,other):continue
    if field in CLOCK_FIELDS and value and other and value!=before.get(x['id'],{}).get(field)and other!=before.get(y['id'],{}).get(field):
     good=all(lo<=stamp(v)<=hi for v,(lo,hi)in zip([value,other],intervals))
     if good:clocks.append(path);continue
    if table=='vod_episode'and field=='uuid'and x['id']not in before:
     values=[uuid.UUID(v)for v in [value,other]]
     if all(v.version==4 for v in values):entropy.append(path);continue
    differences.append(path)
 return {'passed':not differences,'differences':differences,'own_interval_clock_paths':clocks,'new_Episode_UUIDv4_paths':entropy,'scope':'All18 tracked rows, predeclared PK order. Only changed SQL datetime fields within own browser interval and newEpisode UUIDv4 differ; every other raw field/type remains strict.'}

def probe(command,compose,environment,evidence,tools,source_base,candidate_base,origin_ip):
 password=secrets.token_urlsafe(32);evidence=Path(evidence);evidence.mkdir(parents=True,exist_ok=True)
 helper=compose+['exec','-T','-e','VOD_API_DISPOSABLE_FIXTURE=playback-fixture-only','baseline','/dispatcharrpy/bin/python','/vod-api-probe/provider_stale_modal_fixture.py']
 report={'passed':False,'scope':'Unchanged React missing/stale modal; exact per-backend18model/sequence reseed, actual provider wire and SQL effects. No task replacement/resource/M6/fullWebSocket acceptance.','no_Accept_override':True,'normal_login_count':2 if candidate_base else 1,'observations':{},'phases':[],'boundaries':[]};reference=None
 try:
  for target,base in [('Original',source_base)]+([('native',candidate_base)]if candidate_base else[]):
   attempted=False;phase={'target':target,'passed':False};report['phases'].append(phase)
   try:
    attempted=True;prepared=command(helper,payload=json.dumps({'op':'prepare','password':password,'origin_ip':origin_ip,'reseed_template':target!='Original'}),timeout=30);fixture=json.loads(prepared.stdout.splitlines()[-1]);phase['fixture']=fixture
    if reference is None:reference=fixture
    assert fixture['prepared_model_fingerprints']==reference['prepared_model_fingerprints'];assert fixture['prepared_sequences']==reference['prepared_sequences']
    phase['exact_source_prepared_rows_and_sequences']=True;phase['wall_started']=datetime.datetime.now(datetime.timezone.utc).timestamp()
    process=subprocess.run(['node',str(Path(tools)/'scripts/rust-vod-api-contracts/provider_stale_modal_browser.js')],input=json.dumps({**fixture,'password':password,'url':base})+'\n',cwd=tools,env=environment,text=True,capture_output=True,timeout=180)
    phase['wall_finished']=datetime.datetime.now(datetime.timezone.utc).timestamp();values=[json.loads(line)for line in process.stdout.splitlines()if line.startswith('{')];observation=values[-1]if values else{'passed':False,'error':'Missing browser output'};report['observations'][target]=observation
    (evidence/('provider-stale-modal-'+target+'.json')).write_text(json.dumps({'capability':values[0]if values else None,'result':observation,'exit_code':process.returncode},indent=2)+'\n')
    assert process.returncode==0 and observation.get('passed'),target+' actual browser failed'
    phase['passed']=True
   finally:
    if attempted:
     result=command(helper,payload=json.dumps({'op':'restore','label':target}),timeout=30);restored=json.loads(result.stdout.splitlines()[-1]);phase['restoration']=restored
     assert restored['restored']and restored['tracked18_models_exact']and restored['sequences_exact']and restored['owned_origin_only']
     assert len(restored['refreshed_relations'])==4 and all(r['fresh']and r['flags'].get('detailed_fetched')and(r['kind']=='movie'or r['flags'].get('episodes_fetched'))for r in restored['refreshed_relations'])
     assert restored['episode_replacement']=={'new_relations':4,'old_relations':0}
     assert all(phase['wall_started']<=stamp(r['last_refresh'])<=phase['wall_finished']for r in restored['refreshed_relations'])
     phase['all4_SQL_refresh_clocks_in_own_interval']=True
  if not candidate_base:
   # No second browser/native execution: qualify the exact reset/reseed seam itself.
   prepared=command(helper,payload=json.dumps({'op':'prepare','password':password,'origin_ip':origin_ip,'reseed_template':True}),timeout=30);fixture=json.loads(prepared.stdout.splitlines()[-1])
   try:
    assert fixture['boundary_reseed_exact'];assert fixture['prepared_model_fingerprints']==reference['prepared_model_fingerprints'];assert fixture['prepared_sequences']==reference['prepared_sequences']
    report['boundaries'].append({'passed':True,'kind':'Source-only exact reseed controller test; no Native execution','prepared_fingerprints_equal':True,'prepared_sequences_equal':True,'missing_stale_inputs_equal':fixture['initial_relations']==reference['initial_relations']});assert report['boundaries'][-1]['missing_stale_inputs_equal']
   finally:
    result=command(helper,payload=json.dumps({'op':'restore','label':'boundary'}),timeout=30);report['boundary_restoration']=json.loads(result.stdout.splitlines()[-1]);assert report['boundary_restoration']['restored']and report['boundary_restoration']['tracked18_models_exact']and report['boundary_restoration']['sequences_exact']
  else:
   report['exact_ordered_modal_observations']=report['observations']['Original']['results']==report['observations']['native']['results'];assert report['exact_ordered_modal_observations']
   wire=lambda p:[{k:r[k]for k in ['method','raw_target','headers']}for r in p['restoration']['origin_requests']if r['path']=='/player_api.php']
   source_wire,native_wire=map(wire,report['phases']);report['exact_provider_wire']={'passed':source_wire==native_wire,'Source_sha256':digest(source_wire),'native_sha256':digest(native_wire),'scope':'All ordered provider GET method/rawURI/header-name-insensitive values; artwork transport not folded into XC wire'};assert source_wire==native_wire
   sql=[json.loads((evidence/p['restoration']['effect_rows_file']).read_text())for p in report['phases']];intervals=[(p['wall_started'],p['wall_finished'])for p in report['phases']];report['SQL_comparison']=compare_rows(*sql,intervals);assert report['SQL_comparison']['passed']
  report['passed']=True
 finally:
  result=command(helper,payload=json.dumps({'op':'drop-template'}),timeout=30);report['template_cleanup']=json.loads(result.stdout.splitlines()[-1]);assert report['template_cleanup']['template_removed']and report['template_cleanup']['tracked_baseline_exact']
  (evidence/'provider-stale-modal-browser.json').write_text(json.dumps(report,indent=2)+'\n')
 return report
