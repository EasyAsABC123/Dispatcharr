"""Authorized finite exact-source no-run builds. No test/fixture execution."""
import hashlib,json,os,shutil,subprocess,time
from pathlib import Path
HERE=Path(__file__).resolve().parent
ROOT=HERE.parents[2]
FROZEN=ROOT/'artifacts/rust/m3u-failure-runtime-private-3'
SOURCE=FROZEN/'source'
TARGET=ROOT/'artifacts/rust/target-m3u-failure-qa-elf-1'
EXPECTED=json.loads((FROZEN/'source-inputs.json').read_text())
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def save(p,v):p.write_text(json.dumps(v,indent=2)+'\n')
def check_source():
 actual={str(p.relative_to(SOURCE)):sha(p)for p in sorted(SOURCE.rglob('*'))if p.is_file()}
 if actual!=EXPECTED:raise ValueError('Frozen798SourceChanged')
if TARGET.exists():raise ValueError('FreshTargetRequired')
check_source()
compiler=subprocess.run(['rustc','--version','--verbose'],capture_output=True,text=True,check=True)
save(HERE/'compiler.json',{'argv':['rustc','--version','--verbose'],'exit':compiler.returncode,'stdout':compiler.stdout,'stderr':compiler.stderr})
env=dict(os.environ,CARGO_TARGET_DIR=str(TARGET),CARGO_BUILD_JOBS='2',CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0',CARGO_PROFILE_DEV_INCREMENTAL='false',CARGO_PROFILE_TEST_INCREMENTAL='false')
save(HERE/'environment.json',{k:env.get(k)for k in ['CARGO_HOME','RUSTUP_HOME','RUSTUP_TOOLCHAIN','CARGO_TARGET_DIR','CARGO_BUILD_JOBS','CARGO_PROFILE_DEV_DEBUG','CARGO_PROFILE_TEST_DEBUG','CARGO_PROFILE_DEV_INCREMENTAL','CARGO_PROFILE_TEST_INCREMENTAL']})
commands=[('job-library',['cargo','test','--offline','--locked','--no-run','--message-format=json','-p','dispatcharr-job-transport','--lib']),('coordination-residual',['cargo','test','--offline','--locked','--no-run','--message-format=json','-p','dispatcharr-coordination','--test','task_lock_detailed'])]
results=[]
for name,argv in commands:
 check_source();row={'name':name,'argv':argv,'cwd':str(SOURCE/'rust'),'began_epoch':time.time(),'process_settled':False,'actual_exit':None,'source_map_sha256':sha(FROZEN/'source-inputs.json')}
 with (HERE/(name+'.messages.jsonl')).open('xb')as out,(HERE/(name+'.stderr.log')).open('xb')as err:
  proc=subprocess.Popen(argv,cwd=SOURCE/'rust',env=env,stdout=out,stderr=err);row['pid']=proc.pid;save(HERE/(name+'.started.json'),row);print('START '+name+' PID '+str(proc.pid),flush=True)
  row['actual_exit']=proc.wait();row['process_settled']=True
 row['finished_epoch']=time.time();row['stdout_sha256']=sha(HERE/(name+'.messages.jsonl'));row['stderr_sha256']=sha(HERE/(name+'.stderr.log'));check_source();row['source_unchanged_after_wait']=True
 save(HERE/(name+'.result.json'),row);results.append(row);print('DONE '+name+' '+str(row['actual_exit']),flush=True)
 if row['actual_exit']!=0:break
passed=len(results)==2 and all(v['actual_exit']==0 and v['process_settled']for v in results)
peers=[]
if passed:
 outside=HERE/'outside-peers';outside.mkdir()
 for row in results:
  candidates=[]
  for line in (HERE/(row['name']+'.messages.jsonl')).read_text().splitlines():
   value=json.loads(line)
   if value.get('reason')=='compiler-artifact'and value.get('executable')and value.get('profile',{}).get('test'):candidates.append(value)
  if len(candidates)!=1:raise ValueError('ExactOneActualHarness:'+row['name'])
  artifact=candidates[0];source=Path(artifact['executable']);destination=outside/row['name'];shutil.copy2(source,destination)
  if destination.read_bytes()[:4]!=b'\x7fELF'or sha(source)!=sha(destination):raise ValueError('ExactOutsideELF')
  peers.append({'name':row['name'],'compiled_path':str(source),'path':str(destination),'bytes':destination.stat().st_size,'sha256':sha(destination),'compiler_artifact':artifact,'copied_after_all_actual_waits0':True})
 check_source()
save(HERE/'build-record.json',{'passed':passed,'scope':'Two exact-source no-run test-harness builds and outside preservation, no tests/fixtures executed','source_root':str(SOURCE),'source_map_path':str(FROZEN/'source-inputs.json'),'source_map_sha256':sha(FROZEN/'source-inputs.json'),'source_inputs':len(EXPECTED),'compiler_record_sha256':sha(HERE/'compiler.json'),'environment_record_sha256':sha(HERE/'environment.json'),'phases':results,'outside_peers':peers,'all_source_unchanged':True,'no_test_or_fixture_execution':True})
raise SystemExit(0 if passed else 1)
