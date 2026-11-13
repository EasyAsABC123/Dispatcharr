"""Reviewed finite5 build runner, source-map-bound; no applications/Docker."""
from pathlib import Path
import datetime,hashlib,json,os,subprocess,time
H=Path(__file__).resolve().parent;Q=H.parent/'m8-backup-job-native-qa-4';S=Q/'source';plan=json.loads((Q/'compile-plan.json').read_text());expected=json.loads((Q/'full-source-map.json').read_text());target=Path(plan['fresh_target']);tmp=Path(plan['TMPDIR']);assert not target.exists()and not tmp.exists(),'FreshTargetOnly';target.mkdir();tmp.mkdir();env=os.environ.copy();env.update(plan['env']);env['CARGO_TARGET_DIR']=str(target);env['TMPDIR']=str(tmp)
def save(name,x):(H/name).write_text(json.dumps(x,indent=2)+'\n')
def now():return datetime.datetime.now(datetime.timezone.utc).isoformat()
def source_map():return {str(p.relative_to(S)):hashlib.sha256(p.read_bytes()).hexdigest()for p in S.rglob('*')if p.is_file()}
def start(pid):
 try:return Path('/proc',str(pid),'stat').read_text().rsplit(')',1)[1].split()[19]
 except FileNotFoundError:return None
pre=source_map();save('source-before.json',pre);assert pre==expected
receipt={'passed':False,'state':'RUNNING','pid':os.getpid(),'starttime':start(os.getpid()),'proc_PID_namespace':os.readlink('/proc/self/ns/pid'),'started_utc':now(),'source_root':str(S),'source_files':802,'source_map_sha256':hashlib.sha256((Q/'full-source-map.json').read_bytes()).hexdigest(),'reviewed_seal_sha256':hashlib.sha256((Q/'seal.json').read_bytes()).hexdigest(),'target':str(target),'TMPDIR':str(tmp),'environment_overrides':{k:env[k]for k in(*plan['env'],'CARGO_TARGET_DIR','TMPDIR')},'commands':[]};save('build.json',receipt)
for argv in(['rustc','-Vv'],['cargo','-V']):
 p=subprocess.run(argv,env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,check=True);save('toolchain-'+argv[0]+'.json',{'argv':argv,'exit':p.returncode,'stdout':p.stdout.decode(),'stderr':p.stderr.decode()})
try:
 for i,argv in enumerate(plan['commands'],1):
  record={'ordinal':i,'argv':argv,'started_utc':now(),'source_before_exact':source_map()==expected};assert record['source_before_exact'];receipt['commands'].append(record);save('build.json',receipt)
  with(H/f'command-{i}.stdout').open('xb')as stdout,(H/f'command-{i}.stderr').open('xb')as stderr:
   process=subprocess.Popen(argv,cwd=S/'rust',env=env,stdin=subprocess.DEVNULL,stdout=stdout,stderr=stderr);record.update(pid=process.pid,starttime=start(process.pid));save('build.json',receipt);code=process.wait();record.update(actual_wait=code,actual_poll=process.poll(),completed_utc=now(),original_PID_absent=start(process.pid)is None,source_after_exact=source_map()==expected)
  save('build.json',receipt)
  if code or not record['source_after_exact']:break
 receipt['passed']=len(receipt['commands'])==5 and all(r.get('actual_wait')==r.get('actual_poll')==0 and r.get('source_after_exact')for r in receipt['commands']);receipt['state']='SETTLED';receipt['completed_utc']=now();save('source-after.json',source_map());save('build.json',receipt)
except BaseException as error:
 receipt['state']='SETTLED_EXCEPTION';receipt['exception']=type(error).__name__+':'+str(error);receipt['completed_utc']=now();save('source-after.json',source_map());save('build.json',receipt);raise
print(json.dumps({'passed':receipt['passed'],'commands':len(receipt['commands']),'actual_waits':[r.get('actual_wait')for r in receipt['commands']],'target':str(target),'settled_utc':receipt['completed_utc']}),flush=True)
raise SystemExit(0 if receipt['passed']else 1)
