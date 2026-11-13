"""Exact former-project/CID read-only settlement; preserves failed physical1."""
import json,os,sys,hashlib,datetime
from pathlib import Path
ROOT=Path(__file__).resolve().parents[3];OUT=Path(__file__).resolve().parent;RUN=ROOT/'artifacts/rust/configured-recordings-native-physical-1';PREP=ROOT/'artifacts/rust/configured-recordings-physical-prep-2'
sys.path.insert(0,str(PREP/'tools'))
from bounded_process import Collected,CollectionFailure
from run_physical import exact_absent_cid
r=json.loads((RUN/'integration.json').read_text());raw=json.loads((RUN/'source-configured-recordings-private.json').read_text());docker=str(ROOT/'artifacts/rust/tooling/docker/docker');queries=[]
assert r['actual_host_exit']==r['actual_controller_exit']==1 and r['actual_controller_settled']is True
assert raw['cases'][1]['native_process']['exit']==-15 and raw['cases'][1]['native_process']['settled']is False
assert (RUN/'default-before-private.json').read_bytes()==(RUN/'default-after-private.json').read_bytes()
commands=[]
for kind in ('container','network','volume'):
 commands.append((kind,[docker,kind,'ls']+(['--all','--no-trunc']if kind=='container'else[])+['--filter','label=com.docker.compose.project='+r['project'],'--format','{{.Name}}'if kind=='volume'else'{{.ID}}']))
for cid in r['owned_containers']:commands.append(('CID:'+cid,[docker,'inspect',cid]))
for i,(kind,argv)in enumerate(commands):
 job=Collected(argv,{k:v for k,v in os.environ.items()if not k.startswith('GIT_')},65536,lambda:None)
 try:job.wait(20)
 except CollectionFailure:
  if not(job.physically_settled and job.invalid is None):raise
 stdout,stderr=bytes(job.output['stdout']),bytes(job.output['stderr']);absent=(job.poll()==0 and stdout.strip()==b'')if not kind.startswith('CID:')else exact_absent_cid(job.poll(),stdout,stderr,kind.split(':',1)[1])
 row={'kind':kind,'argv':argv,'exit':job.poll(),'stdout_hex':stdout.hex(),'stderr_hex':stderr.hex(),'actual_process_wait_completed':job.physically_settled,'pipes_settled':not any(t.is_alive()for t in job.threads),'absent':absent};queries.append(row);(OUT/'queries-partial.json').write_text(json.dumps(queries,indent=2)+'\n');assert absent and row['pipes_settled']
report={'passed':True,'scope':'Independent fresh exact project/CID absence only, plus retained default11 equality; original host/guest failure and inner invalid flag unchanged','actual_host_exit':1,'actual_guest_exit':1,'whole_attempt_passed':False,'first_phase_checks_passed':15,'failed_phase2_check':'actual-target-reply-held','remaining10_unexecuted':True,'inner_peer_exit':-15,'inner_collector_settled_flag':False,'inner_actual_raw_reader_writer_joined':raw['cases'][1]['native_process']['pipes_settled']and raw['cases'][1]['native_process']['writer_settled'],'finally_error_literal':raw['finally_errors'],'default11_retained_bytes_equal':True,'queries':queries,'reviewed_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'raw_sha256':hashlib.sha256((RUN/'source-configured-recordings-private.json').read_bytes()).hexdigest()}
(OUT/'review.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({'passed':True,'fresh_queries':len(queries),'original_attempt_still_failed':True}))
