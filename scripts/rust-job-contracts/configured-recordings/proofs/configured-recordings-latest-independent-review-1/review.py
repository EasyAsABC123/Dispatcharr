import json,os,sys,hashlib,datetime
from pathlib import Path
ROOT=Path(__file__).resolve().parents[3]; OUT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT/'artifacts/rust/configured-recordings-physical-prep-3/tools'))
from bounded_process import Collected,CollectionFailure
from run_physical import exact_absent_cid
reports=[]
for name,expected in [('configured-recordings-native-known-insert-1',0),('configured-recordings-native-physical-artwork-1',1)]:
 run=ROOT/'artifacts/rust'/name;r=json.loads((run/'integration.json').read_text());raw=json.loads((run/'source-configured-recordings-private.json').read_text())
 assert r['actual_host_exit']==r['actual_controller_exit']==expected and r['actual_controller_settled'] is True
 before=(run/'default-before-private.json').read_bytes();after=(run/'default-after-private.json').read_bytes();assert before==after
 manifest=json.loads(before)['manifest']; queries=[];docker=str(ROOT/'artifacts/rust/tooling/docker/docker')
 commands=[(kind,[docker,kind,'ls']+(['--all','--no-trunc']if kind=='container'else[])+['--filter','label=com.docker.compose.project='+r['project'],'--format','{{.Name}}'if kind=='volume'else'{{.ID}}'])for kind in ('container','network','volume')]
 commands += [('CID:'+cid,[docker,'inspect',cid])for cid in r['owned_containers']]
 for kind,argv in commands:
  job=Collected(argv,{k:v for k,v in os.environ.items()if not k.startswith('GIT_')},65536,lambda:None)
  try:job.wait(20)
  except CollectionFailure:
   if not(job.physically_settled and job.invalid is None):raise
  stdout,stderr=bytes(job.output['stdout']),bytes(job.output['stderr']); absent=(job.poll()==0 and not stdout.strip())if not kind.startswith('CID:')else exact_absent_cid(job.poll(),stdout,stderr,kind.split(':',1)[1])
  row={'kind':kind,'argv':argv,'exit':job.poll(),'stdout_hex':stdout.hex(),'stderr_hex':stderr.hex(),'actual_wait_completed':job.physically_settled,'pipes_settled':not any(t.is_alive()for t in job.threads),'absent':absent};queries.append(row)
  (OUT/(name+'-queries.json')).write_text(json.dumps(queries,indent=2)+'\n'); assert absent and row['pipes_settled']
 report={'run':name,'original_host_exit':expected,'original_guest_exit':expected,'original_attempt_passed':r['passed'],'cases':len(raw['cases']),'checks':len(raw['checks']),'all_check_values':all(c['ok'] for c in raw['checks']) if raw['checks'] and 'ok' in raw['checks'][0] else None,'manifest_bytes_equal':True,'manifest_sha256':hashlib.sha256(before).hexdigest(),'manifest_structure':{k:(len(v)if isinstance(v,(list,dict))else v)for k,v in manifest.items()},'queries':queries,'raw_sha256':hashlib.sha256((run/'source-configured-recordings-private.json').read_bytes()).hexdigest()};reports.append(report)
 (OUT/'review.json').write_text(json.dumps({'scope':'Fresh exact project/CID absence and retained actual manifest equality; original outcomes unchanged','reviewed_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'runs':reports},indent=2)+'\n')
print(json.dumps({'fresh_queries':sum(len(r['queries'])for r in reports),'runs':[{'run':r['run'],'original_host_exit':r['original_host_exit'],'manifest_structure':r['manifest_structure']}for r in reports]}))
