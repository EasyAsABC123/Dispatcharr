#!/usr/bin/env python3
"""Isolated actual Original/native cached Redis loss/restart differential.
Original Dispatcharr/Redis authors retain application/algorithm/server credit.
No production runtime reconnect behavior is modified by this probe.
"""
import argparse,hashlib,io,json,os,select,shutil,subprocess,tarfile,time,uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
ORIGINAL='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
class Peer:
 def __init__(self,args,env,log):
  self.log=log.open('wb');self.noise=0;self.process=subprocess.Popen(args,env=env,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=self.log,text=False,bufsize=0)
 def ask(self,action,**fields):
  self.process.stdin.write((json.dumps({'action':action,**fields})+'\n').encode());self.process.stdin.flush();deadline=time.monotonic()+40
  while time.monotonic()<deadline:
   if not select.select([self.process.stdout],[],[],max(0,deadline-time.monotonic()))[0]:break
   line=self.process.stdout.readline(32769)
   if not line:raise RuntimeError('Peer exited before bounded reply')
   if len(line)>32768:raise RuntimeError('Peer response limit')
   try:value=json.loads(line)
   except json.JSONDecodeError:value=None
   if isinstance(value,dict)and value.get('action')==action:return value
   self.noise+=1;self.log.write(b'UNEXPECTED_STDOUT: '+line);self.log.flush()
   if self.noise>16:raise RuntimeError('Bounded peer protocol noise limit')
  raise TimeoutError('Bounded peer command deadline')
 def close(self):
  if self.process.poll()is None:
   try:self.ask('shutdown');self.process.wait(timeout=3)
   except Exception:self.process.terminate()
  try:self.process.wait(timeout=3)
  except subprocess.TimeoutExpired:self.process.kill();self.process.wait(timeout=2)
  self.log.close()
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',type=Path,required=True);p.add_argument('--reuse-build-manifest',type=Path);a=p.parse_args();out=a.output.resolve();out.mkdir(parents=True,exist_ok=False)
 report={'passed':False,'execution_passed':False,'scope':'Isolated cached-client loss/restart component; no application or reconnect fix','original_revision':ORIGINAL,'image':IMAGE,'checks':[],'observations':[],'performance_verified':False};env=dict(os.environ);name='dispatcharr-redis-restart-'+uuid.uuid4().hex[:12];peers=[];status=1
 tools=out/'tool-source';toolmap={}
 for source in sorted((ROOT/'scripts/rust-redis-restart-contracts').glob('*')):
  if not source.is_file():continue
  data=source.read_bytes();target=tools/source.name;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(data);target.chmod(0o644);toolmap[str(source.relative_to(ROOT))]=hashlib.sha256(data).hexdigest()
 tools.chmod(0o755);report['tool_sha256']=toolmap
 if a.reuse_build_manifest:
  origin=json.loads(a.reuse_build_manifest.read_text());old=a.reuse_build_manifest.resolve().parent
  archive_bytes=(old/'rust-source.tar').read_bytes()
  if not origin.get('native_binary_sha256')or hashlib.sha256(archive_bytes).hexdigest()!=origin.get('rust_source_archive_sha256'):raise ValueError('Verified compiled source binding required')
  with tarfile.open(fileobj=io.BytesIO(archive_bytes))as archive:
   entries={member.name:hashlib.sha256(archive.extractfile(member).read()).hexdigest()for member in archive if member.isfile()}
  if entries!=origin.get('rust_source_sha256'):raise ValueError('Complete source byte manifest mismatch')
  binary_bytes=(old/'native-probe').read_bytes()
  if hashlib.sha256(binary_bytes).hexdigest()!=origin['native_binary_sha256']:raise ValueError('Compiled binary mismatch')
  (out/'rust-source.tar').write_bytes(archive_bytes);report['rust_source_sha256']=entries;report['rust_source_archive_sha256']=origin['rust_source_archive_sha256'];report['source_snapshot_origin']={'manifest':str(a.reuse_build_manifest),'sha256':hashlib.sha256(a.reuse_build_manifest.read_bytes()).hexdigest(),'previous_execution_passed':origin.get('execution_passed'),'scope':'Exact successfully compiled source/binary reused; prior fixture setup failure retained'}
 else:
  captured={}
  for directory,dirs,names in os.walk(ROOT/'rust'):
   dirs[:]=sorted(v for v in dirs if v not in{'target','.git','__pycache__'})
   for filename in sorted(names):
    source=Path(directory)/filename
    if source.is_symlink()or not source.is_file():raise ValueError('Regular Rust source required')
    captured[str(source.relative_to(ROOT))]=source.read_bytes()
  with tarfile.open(out/'rust-source.tar','w')as archive:
   for relative,data in sorted(captured.items()):
    item=tarfile.TarInfo(relative);item.mode=0o644;item.size=len(data);item.mtime=0;archive.addfile(item,io.BytesIO(data))
  report['rust_source_sha256']={path:hashlib.sha256(data).hexdigest()for path,data in captured.items()};report['rust_source_archive_sha256']=hashlib.sha256((out/'rust-source.tar').read_bytes()).hexdigest();(out/'frozen-source-ready.json').write_text(json.dumps({'closed':True,'sha256':report['rust_source_archive_sha256']}))
 (out/'frozen-source-ready.json').write_text(json.dumps({'closed':True,'sha256':report['rust_source_archive_sha256']}))
 frozen=out/'native-source';frozen.mkdir()
 with tarfile.open(out/'rust-source.tar')as archive:archive.extractall(frozen,filter='data')
 def command(args,timeout=30):return subprocess.check_output(args,env=env,text=True,stderr=subprocess.PIPE,timeout=timeout).strip()
 def check(label,passed):
  report['checks'].append({'name':label,'passed':bool(passed)})
  if not passed:raise AssertionError(label)
 try:
  if a.reuse_build_manifest:
   binary=out/'native-probe';binary.write_bytes(binary_bytes);binary.chmod(0o755);report['native_binary_sha256']=hashlib.sha256(binary_bytes).hexdigest()
  else:
   built=subprocess.run(['cargo','build','--manifest-path',str(frozen/'rust/Cargo.toml'),'-p','dispatcharr-ingest-epg','--example','redis_restart_probe','--locked','--offline','--message-format=json'],env=dict(env,CARGO_TARGET_DIR=str(out/'cargo-target')),capture_output=True,text=True,timeout=600);(out/'build.jsonl').write_text(built.stdout);(out/'build.log').write_text(built.stderr);built.check_returncode()
   executables=[json.loads(line)['executable']for line in built.stdout.splitlines()if line.startswith('{')and json.loads(line).get('reason')=='compiler-artifact'and json.loads(line).get('executable')]
   if len(executables)!=1:raise ValueError('One native frozen example required')
   binary=out/'native-probe';shutil.copyfile(executables[0],binary);binary.chmod(0o755);report['native_binary_sha256']=hashlib.sha256(binary.read_bytes()).hexdigest()
  source=out/'original-source';source.mkdir(mode=0o755);source.chmod(0o755);archive=subprocess.check_output(['git','archive',ORIGINAL],cwd=ROOT);(out/'original-source.tar').write_bytes(archive);report['original_archive_sha256']=hashlib.sha256(archive).hexdigest()
  subprocess.run(['tar','--same-permissions','--no-same-owner','-xf','-','-C',str(source)],input=archive,check=True)
  task='owned_redis_restart_'+uuid.uuid4().hex[:12];native_task=task+'_native'
  args=['docker','run','-d','--rm','--name',name,'--network','none','--read-only','--user','65532:65532','--cpus','1','--memory','512m','--tmpfs','/tmp:rw,size=128m,mode=1777','--tmpfs','/data:rw,size=16m,mode=1777','--mount',f'type=bind,src={source},dst=/app,readonly','--mount',f'type=bind,src={tools},dst=/restart-tools,readonly','--mount',f'type=bind,src={binary},dst=/native-probe,readonly','-e','REDIS_RESTART_DISPOSABLE_FIXTURE=playback-fixture-only','-e','REDIS_URL=redis://:playback-fixture-only@127.0.0.1:6379/0','-e','REDIS_TEST_URL=redis://:playback-fixture-only@127.0.0.1:6379/0','-e','SECRET_KEY=playback-fixture-only','-e','POSTGRES_DB=rust_probe_redis_restart_only','-e','DISPATCHARR_LOG_DIR=/tmp/restart-logs','-e','RESTART_PROBE_TASK='+task,'-e','RESTART_NATIVE_TASK='+native_task,'-e','PYTHONPATH=/app','--workdir','/app','--entrypoint','/dispatcharrpy/bin/python',IMAGE,'/restart-tools/supervisor.py']
  command(args);report['container']=name
  inspect=json.loads(command(['docker','inspect',name]))[0];report['actual_image_id']=inspect['Image'];check('network-none-readonly-uid65532',inspect['HostConfig']['NetworkMode']=='none'and inspect['HostConfig']['ReadonlyRootfs']and inspect['Config']['User']=='65532:65532')
  def control(action):return json.loads(command(['docker','exec',name,'/dispatcharrpy/bin/python','/restart-tools/control.py',action]))
  def ready():
   deadline=time.monotonic()+15
   while time.monotonic()<deadline:
    try:
     if command(['docker','exec','-e','REDISCLI_AUTH=playback-fixture-only',name,'redis-cli','--no-auth-warning','ping'])=='PONG':return
    except subprocess.SubprocessError:pass
    time.sleep(.1)
   raise TimeoutError('Owned Redis readiness')
  ready();server_info=command(['docker','exec','-e','REDISCLI_AUTH=playback-fixture-only',name,'redis-cli','--no-auth-warning','INFO','server']);before_runid=next(line.split(':',1)[1]for line in server_info.splitlines()if line.startswith('run_id:'));report['redis_run_ids']=[before_runid];initial=control('status');check('supervisor-running-generation1',initial=={'running':True,'generation':1})
  original=Peer(['docker','exec','-i',name,'/dispatcharrpy/bin/python','/restart-tools/source_client.py'],env,out/'source-peer-private.log');peers.append(original)
  native=Peer(['docker','exec','-i',name,'/native-probe'],env,out/'native-peer-private.log');peers.append(native)
  def observe(peer,label,action,**fields):
   result=peer.ask(action,**fields);report['observations'].append({'peer':label,'command':{'action':action,**fields},'result':result});return result
  src=observe(original,'original','init');check('original-cached-client-no-app-initialization',src.get('ready')and src['same_client']and not src['apps_ready']);check('actual-original-helper-source-bytes',all(hashlib.sha256((source/path.removeprefix('/app/')).read_bytes()).hexdigest()==digest for path,digest in src['source_sha256'].items()))
  init=observe(native,'native','init');check('native-cached-connections-ready',init.get('locks_ready')and init.get('transport_ready'))
  check('original-acquires-before-loss',observe(original,'original','acquire',id=1).get('result')is True);check('native-acquires-before-loss',observe(native,'native','acquire',id=1001).get('result')is True);check('native-transport-before-loss',observe(native,'native','ping').get('result')=='PONG');check('original-source-lock-before-loss',observe(original,'original','read',id=1).get('result')=='locked');check('native-fenced-lock-before-loss',str(observe(original,'original','read',id=1001,task=native_task).get('result')).startswith('rust:'))
  check('stop-redis-only',control('stop')=={'running':False,'generation':1})
  down=[observe(original,'original','release',id=1),observe(original,'original','acquire',id=2),observe(native,'native','release'),observe(native,'native','ping'),observe(native,'native','acquire',id=1002)];settled=observe(native,'native','settle');check('native-down-command-resources-settled',settled.get('settled')is True);check('all-down-operations-report-errors',all('error'in value for value in down))
  check('same-container-generation2',control('start')=={'running':True,'generation':2});ready();server_info=command(['docker','exec','-e','REDISCLI_AUTH=playback-fixture-only',name,'redis-cli','--no-auth-warning','INFO','server']);after_runid=next(line.split(':',1)[1]for line in server_info.splitlines()if line.startswith('run_id:'));report['redis_run_ids'].append(after_runid);check('actual-redis-server-restarted',before_runid!=after_runid)
  old=observe(original,'original','read',id=1);check('original-previous-lock-lost-on-no-persistence-restart','error'not in old and old.get('result')is None);old=observe(original,'original','read',id=1001,task=native_task);check('native-previous-lock-lost-on-no-persistence-restart','error'not in old and old.get('result')is None)
  recovered=observe(original,'original','acquire',id=3);check('original-same-cached-client-recovers',recovered.get('result')is True and recovered['same_client']);released=observe(original,'original','release',id=3);check('original-release-after-recovery','error'not in released and released.get('result')is None)
  native_attempts=[]
  for delay in [0,.1,1]:
   time.sleep(delay);native_attempts.append({'delay_before_seconds':delay,'ping':observe(native,'native','ping'),'acquire':observe(native,'native','acquire',id=1003)});settled=observe(native,'native','settle');check('native-post-restart-resources-settled-'+str(len(native_attempts)),settled.get('settled')is True)
   if native_attempts[-1]['acquire'].get('result')is True:observe(native,'native','release')
   key=observe(original,'original','read',id=1003,task=native_task);check('native-attempt-clean-or-absent-key-'+str(len(native_attempts)),'error'not in key and key.get('result')is None)
  rebuild=observe(native,'native','rebuild');check('new-native-connection-positive-control',rebuild.get('locks_ready')and rebuild.get('transport_ready'));check('rebuilt-native-transport-healthy',observe(native,'native','ping').get('result')=='PONG');check('rebuilt-native-locks-healthy',observe(native,'native','acquire',id=1004).get('result')is True);check('rebuilt-native-release-healthy',observe(native,'native','release').get('result')is True)
  check('source-and-native-persistent-processes',len({x['result']['pid']for x in report['observations']if x['peer']=='original'})==1 and len({x['result']['pid']for x in report['observations']if x['peer']=='native'})==1)
  report.update(execution_passed=True,source_recovery_verified=True,native_cached_transport_recovered=any(x['ping'].get('result')=='PONG'for x in native_attempts),native_cached_tasklocks_recovered=any(x['acquire'].get('result')is True for x in native_attempts),native_rebuild_positive_control=True,observed_retry_delays=[0,.1,1],original_apps_initialized=False,application_hook_verified=False,reconnect_runtime_modified=False)
  report['passed']=report['native_cached_transport_recovered']and report['native_cached_tasklocks_recovered'];report['migration_gate_passed']=report['passed'];report['known_mismatch_detected']=not report['passed'];status=0 if report['passed']else 1
 except Exception as error:report.update(error_type=type(error).__name__,error_message=str(error)[:1024])
 finally:
  for peer in reversed(peers):peer.close()
  try:(out/'container.log').write_text(command(['docker','logs',name]))
  except subprocess.SubprocessError:pass
  removed=subprocess.run(['docker','rm','-f',name],env=env,text=True,capture_output=True,timeout=20);report['cleanup']='owned container removed'if removed.returncode==0 else'cleanup failed';report['cleanup_passed']=removed.returncode==0
  if removed.returncode:report['passed']=False;status=1
  (out/'proof.json').write_text(json.dumps(report,indent=2));print(json.dumps({'passed':report['passed'],'execution_passed':report['execution_passed'],'evidence':str(out)}))
 return status
if __name__=='__main__':raise SystemExit(main())
