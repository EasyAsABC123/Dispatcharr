"""Finite exact28 single-node dispatcher; never runs without sealed Root clearance.
No automatic result acceptance: lanes remain owned until explicit semantic and
independent-cleanup review. Original Dispatcharr authors retain code credit.
"""
import argparse,datetime,fcntl,hashlib,json,os,subprocess,sys,time
from pathlib import Path
from contextlib import contextmanager
sys.dont_write_bytecode=True
from tooling_guard import preflight as tooling_preflight
LAST_TOOLING_PREFLIGHT=None
from policy import PLAN_SHA,HEAVY,GUIDE,SOURCE,NATIVE,Refusal,sha,validate_clearance,fresh_state,allowed,next_names
HERE=Path(__file__).resolve().parent;INPUT=HERE/'inputs';PLAN=json.loads((INPUT/'plan.json').read_text());NODES={n['name']:n for n in PLAN['groups']};MATRIX=Path(NODES[GUIDE]['argv'][NODES[GUIDE]['argv'].index('--output')+1]).parent;LOGS=MATRIX/'controller-logs';STATE=HERE/'state.json'
def atomic(path,value):
 path=Path(path);path.parent.mkdir(parents=True,exist_ok=True);tmp=path.with_name(path.name+'.tmp-'+str(os.getpid()));raw=(json.dumps(value,indent=2)+'\n').encode()
 with tmp.open('xb')as f:f.write(raw);f.flush();os.fsync(f.fileno())
 os.replace(tmp,path);fd=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY)
 try:os.fsync(fd)
 finally:os.close(fd)
@contextmanager
def lock():
 with(HERE/'state.lock').open('a')as f:
  fcntl.flock(f,fcntl.LOCK_EX)
  try:yield
  finally:fcntl.flock(f,fcntl.LOCK_UN)
def state():return json.loads(STATE.read_text())if STATE.exists()else fresh_state(PLAN)
def file_receipt(v):
 if not isinstance(v,dict)or not v.get('path')or sha(v['path'])!=v.get('sha256'):raise Refusal('Exact file receipt required')
 return Path(v['path'])
def seal_check():
 raw=(HERE/'execution-seal.json').read_bytes();seal=json.loads(raw)
 if sha(INPUT/'plan.json')!=PLAN_SHA:raise Refusal('Changed reviewed image54 plan')
 for name,digest in seal['files'].items():
  if sha(HERE/name)!=digest:raise Refusal('Changed scheduler/input file '+name)
 return hashlib.sha256(raw).hexdigest()
def input_check():
 global LAST_TOOLING_PREFLIGHT
 support=Path(PLAN['support_root']);meta=json.loads((INPUT/'support-inputs.json').read_text())
 if len(meta['files'])!=6148 or sha(INPUT/'support-inputs.json')!=PLAN['support_input_map_sha256']:raise Refusal('Exact6148 support manifest required')
 for name,digest in meta['files'].items():
  if sha(support/name)!=digest:raise Refusal('Changed frozen support '+name)
 env={k:v for k,v in os.environ.items()if not k.startswith('GIT_')};env.update(json.loads((INPUT/'environment.json').read_text()))
 if subprocess.check_output(['git','rev-parse','HEAD'],cwd=support,env=env,text=True).strip()!=PLAN['support_revision']:raise Refusal('Wrong private frozen HEAD')
 if subprocess.check_output(['git','status','--porcelain','--untracked-files=no'],cwd=support,env=env,text=True).strip():raise Refusal('Frozen support changed')
 candidate=json.loads(Path(PLAN['candidate']['manifest']).read_text())
 if sha(PLAN['candidate']['manifest'])!=PLAN['candidate']['manifest_sha256']or candidate['image_id']!=PLAN['candidate']['image_id']:raise Refusal('Exact54 candidate required')
 expected_raw=(INPUT/'runtime792.json').read_bytes();expected=json.loads(expected_raw)
 if hashlib.sha256(expected_raw).hexdigest()!='7acb3cf3c8777286cbfab1a8e338920ede7151d9ef173db5b259f77e3b6e802f' or len(expected)!=792:raise Refusal('Exact reviewed792 map required')
 runtime={k:v for k,v in candidate.get('source_sha256',{}).items()if k.startswith('rust/')}
 if runtime!=expected or candidate.get('git_revision')!='044f3ad0fb3ee68ffb17b23bbb796ccdae954298' or len(candidate.get('source_sha256',{}))!=1289:raise Refusal('Exact candidate792/app1289 source required')
 if candidate.get('source_tree_sha256')!='a8ab7ab0e400c1ebcdc1bc0e232434bb9408ba5165a048d1cbf2c0716f501256' or candidate.get('source_archive_sha256')!='86fb4e93032cc1565ada0d0935e21805dc6e0352897f5eeed06b7a0748b3c880':raise Refusal('Exact candidate54 source tree/archive required')
 if sha(Path(PLAN['candidate']['manifest']).parent/'source.tar')!=candidate['source_archive_sha256']:raise Refusal('Actual candidate54 archive required')
 if len(NODES)!=28 or any(n['cwd']!=str(support.resolve())for n in NODES.values()):raise Refusal('Exact28/cwd required')
 LAST_TOOLING_PREFLIGHT=tooling_preflight(env,json.loads((INPUT/'tooling-inputs.json').read_text()))
 return env
def heavy_idle(env):
 active=subprocess.check_output(['docker','ps','-q'],env=env,text=True).splitlines()
 if active:raise Refusal('All Docker must be idle at first/solo dispatch')
 builds=[]
 for p in Path('/proc').iterdir():
  if not p.name.isdigit():continue
  try:
   comm=(p/'comm').read_text().strip()
   if comm in ['cargo','rustc','rustdoc','ffmpeg','cc1','ld','ld.lld']:builds.append({'pid':p.name,'comm':comm})
  except OSError:pass
 if builds:raise Refusal('Other build/media work active')
 return {'all_Docker_empty':True,'visible_build_media_processes':builds,'checked_epoch':time.time()}
def load_clearance(path,digest,seal,first):
 if not path or not digest or sha(path)!=digest:raise Refusal('Explicit Root clearance file and digest required')
 c=json.loads(Path(path).read_text())
 if first:validate_clearance(c,seal,time.time())
 elif c.get('scheduler_seal_sha256')!=seal or c.get('authorized')is not True or c.get('plan_sha256')!=PLAN_SHA:raise Refusal('Same Root finite authorization required')
 return c
def execute(name,lane,clearance,clearance_sha):
 seal=seal_check();env=input_check()
 with lock():
  s=state();c=load_clearance(clearance,clearance_sha,seal,not s['started']);allowed(PLAN,s,name,lane,time.time())
  if s.get('clearance_sha256')and s['clearance_sha256']!=clearance_sha:raise Refusal('Finite cohort clearance changed')
  if not s['started']or name in HEAVY:idle=heavy_idle(env)
  else:idle=None
  node=NODES[name]
  if '--output'in node['argv']and Path(node['argv'][node['argv'].index('--output')+1]).exists():raise Refusal('Existing node output; no repeat/reuse')
  # Guide clock validation above precedes ANY matrix output/fixture creation.
  LOGS.mkdir(parents=True,exist_ok=True);receipt=LOGS/(name+'.result.json');started=LOGS/(name+'.started.json')
  if receipt.exists()or started.exists():raise Refusal('Prior attempted node is immutable')
  for suffix in ['stdout.log','stderr.log']:
   if(LOGS/(name+'.'+suffix)).exists():raise Refusal('Prior node logs already exist')
  s['started']=True;s['clearance_sha256']=clearance_sha;s['nodes'][name]={'status':'starting','lane':lane,'started_epoch':time.time()};s['lanes'][str(lane)]['node']=name
  result={'name':name,'argv':node['argv'],'cwd':node['cwd'],'plan_sha256':PLAN_SHA,'scheduler_seal_sha256':seal,'clearance_sha256':clearance_sha,'started_epoch':s['nodes'][name]['started_epoch'],'lane':lane,'returncode':None,'process_settled':False}
  atomic(started,{'name':name,'argv':node['argv'],'cwd':node['cwd'],'plan_sha256':PLAN_SHA,'scheduler_seal_sha256':seal,'clearance_sha256':clearance_sha,'required_environment':json.loads((INPUT/'environment.json').read_text()),'started_epoch':result['started_epoch'],'fresh_idle_observation':idle,'host_tooling_preflight':LAST_TOOLING_PREFLIGHT});atomic(STATE,s)
  stdout=(LOGS/(name+'.stdout.log')).open('x');stderr=(LOGS/(name+'.stderr.log')).open('x')
  try:
   # Spawn under the global claim lock: actual first process is the guide.
   process=subprocess.Popen(node['argv'],cwd=node['cwd'],env=env,stdout=stdout,stderr=stderr,close_fds=True);result['pid']=process.pid;s['nodes'][name].update(status='running',pid=process.pid);atomic(STATE,s)
  except BaseException as error:
   stdout.close();stderr.close();result.update(dispatch_error=type(error).__name__,completed_epoch=time.time());atomic(receipt,result);s['nodes'][name].update(status='awaiting-review',returncode=None,controller_result_sha256=sha(receipt));atomic(STATE,s);raise
 try:result.update(returncode=process.wait(),process_settled=True)
 except BaseException as error:
  result.update(dispatch_error=type(error).__name__,returncode=process.poll(),process_settled=process.poll()is not None);raise
 finally:
  stdout.close();stderr.close();result['completed_epoch']=time.time();atomic(receipt,result)
  with lock():
   s=state();s['nodes'][name].update(status='awaiting-review',returncode=result['returncode'],process_settled=result['process_settled'],completed_epoch=result['completed_epoch'],controller_result_sha256=sha(receipt));atomic(STATE,s)
 return result
def record_review(path,digest):
 if sha(path)!=digest:raise Refusal('Sealed actual review required')
 v=json.loads(Path(path).read_text());name=v.get('name')
 if name not in NODES:raise Refusal('Only exact28 reviews')
 with lock():
  s=state();item=s['nodes'][name]
  if item['status']!='awaiting-review':raise Refusal('Actual attempted node required')
  result=LOGS/(name+'.result.json')
  actual_sha=sha(result)
  if actual_sha!=item.get('controller_result_sha256'):
   s['halted']=True;item['status']='result-integrity-uncertain';atomic(STATE,s);raise Refusal('Actual controller result no longer matches its sealed state')
  if actual_sha!=v.get('controller_result_sha256')or v.get('actual_process_exit')!=item['returncode']:raise Refusal('Exact actual controller result required')
  actual_result=json.loads(result.read_text())
  if item.get('process_settled')is not True or actual_result.get('process_settled')is not True:
   s['halted']=True;item['status']='settlement-uncertain';atomic(STATE,s);raise Refusal('Actual sealed controller settlement is false/unknown; halt without automatic recovery')
  if v.get('passed')is True and item['returncode']!=0:raise Refusal('passed:true cannot hide nonzero exit')
  if v.get('actual_process_settled')is not True:raise Refusal('Interrupted/unknown live controller must settle before review')
  if type(v.get('passed'))is not bool or v.get('semantic_review_complete')is not True or not isinstance(v.get('observed_default_audits'),list):raise Refusal('Explicit semantic/audit scope review required')
  reports=v.get('actual_reports',[])
  if not reports:raise Refusal('Actual report/stdout bindings required')
  node=NODES[name];output=Path(node['argv'][node['argv'].index('--output')+1])if '--output'in node['argv']else LOGS
  for r in reports:
   p=file_receipt(r).resolve()
   if output.resolve()not in p.parents and p!=output.resolve():raise Refusal('Review cannot borrow another node output')
   if output==LOGS and p.name not in [name+'.stdout.log',name+'.stderr.log']:raise Refusal('Offline review needs its own exact stdout/stderr')
  cleanup=v.get('cleanup',{});kind=node['kind']
  if kind=='offline-typed-comparison':clean=cleanup.get('scope')=='offline-no-resources'
  else:
   clean=cleanup.get('scope')=='independent-owned-empty'and cleanup.get('verified')is True and cleanup.get('inventories')=={'containers':[],'networks':[],'volumes':[]}
   if clean:file_receipt(cleanup['query_receipt'])
  if not clean:
   s['halted']=True;item['status']='cleanup-uncertain';atomic(STATE,s);raise Refusal('Cleanup uncertainty halts all lanes')
  lane=item['lane'];item.update(status='passed-clean'if v['passed']else'failed-clean',review_sha256=digest,review_path=str(Path(path)),reviewed_epoch=time.time());s['lanes'][str(lane)]['node']=None
  if name==SOURCE and v['passed']:
   source=output/'source-refresh-workflow-private.json';raw=json.loads(source.read_text());clock=datetime.datetime.fromisoformat(raw['fixture_clock'].replace('Z','+00:00'))
   if clock.utcoffset()!=datetime.timedelta(0):raise Refusal('Actual Source UTC fixture clock required')
   item['reference_clock_epoch']=clock.timestamp();item['reference_clock_raw']=raw['fixture_clock'];item['reference_report_sha256']=sha(source);s['lanes'][str(lane)]['reserved']=NATIVE
  elif name==NATIVE:s['lanes'][str(lane)]['reserved']=None
  atomic(STATE,s)
 return {'name':name,'status':item['status']}
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('action',choices=['selfcheck','status','next','execute','review']);p.add_argument('--name',choices=list(NODES));p.add_argument('--lane',type=int,choices=[0,1,2],default=0);p.add_argument('--clearance',type=Path);p.add_argument('--clearance-sha256');p.add_argument('--review',type=Path);p.add_argument('--review-sha256');a=p.parse_args()
 if a.action=='selfcheck':print(json.dumps({'passed':True,'seal':seal_check(),'support_inputs_verified':len(json.loads((INPUT/'support-inputs.json').read_text())['files']) if input_check()else 0,'dispatch_executed':False,'host_tooling_preflight':LAST_TOOLING_PREFLIGHT}));return
 if a.action=='status':print(json.dumps(state(),indent=2));return
 if a.action=='next':print(json.dumps({'lane':a.lane,'eligible':next_names(PLAN,state(),a.lane,time.time()),'dispatch_authorized':False}));return
 if a.action=='execute':
  result=execute(a.name,a.lane,a.clearance,a.clearance_sha256);print(json.dumps(result));raise SystemExit(result['returncode']if type(result['returncode'])is int else 1)
 if not a.review or not a.review_sha256:raise Refusal('Review file+digest required')
 print(json.dumps(record_review(a.review,a.review_sha256)))
if __name__=='__main__':main()
