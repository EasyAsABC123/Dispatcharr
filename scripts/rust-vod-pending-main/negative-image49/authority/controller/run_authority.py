"""Independent fixed2.4s Original INCR/native Coordinator physical reply authority."""
import hashlib,importlib.util,json,os,shutil,subprocess,sys,time,uuid
from pathlib import Path
HERE=Path(__file__).resolve().parent;REPO=Path('/home/justin/github/Dispatcharr');ROOT=HERE.parent
BASE='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430';SUPPORT='c91ab5ece7926f1b6b04a8069c6ad38c5b5d8d55'
def save(p,v):Path(p).write_text(json.dumps(v,indent=2)+'\n')
def load(p,n):
 spec=importlib.util.spec_from_file_location(n,p);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m
def main():
 out=ROOT/'run-1';out.mkdir(exist_ok=False);controller=out/'controller';shutil.copytree(HERE,controller,ignore=shutil.ignore_patterns('__pycache__'));source=out/'original';source.mkdir();support=out/'support';support.mkdir()
 raw=subprocess.check_output(['git','-C',str(REPO),'archive',BASE]);assert hashlib.sha256(raw).hexdigest()=='f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff';subprocess.run(['tar','-x','-C',str(source)],input=raw,check=True);(source/'media').mkdir(exist_ok=True)
 raw=subprocess.check_output(['git','-C',str(REPO),'archive',SUPPORT,'scripts','docker']);subprocess.run(['tar','-x','-C',str(support)],input=raw,check=True);save(out/'support.json',{'commit':SUPPORT,'archive_sha256':hashlib.sha256(raw).hexdigest()})
 bounded=load(controller/'bounded_process.py','authority_bounded');ports=load(support/'scripts/rust-integration/fixture_ports.py','authority_ports')
 project='dispatcharr-pending-authority-'+uuid.uuid4().hex[:10];nonce=uuid.uuid4().hex;db='rust_pending_'+uuid.uuid4().hex[:10];report={'passed':False,'project':project,'scope':__doc__,'resource_sampling':False,'Main_replay':False};env=dict(os.environ,BASELINE_SOURCE_DIR=str(source),BASELINE_EVIDENCE_DIR=str(out));overlay=out/'fixture.json'
 save(overlay,{'services':{'baseline':{'volumes':[str(controller)+':/pending-tools:ro'],'environment':{'PENDING_MAIN_FIXTURE':'playback-fixture-only','PENDING_MAIN_NONCE':nonce}}}})
 portfile=out/'ports.yml';ports.write_port_overlay(portfile,{'baseline':[5656,6381]});compose=['docker','compose','-p',project,'-f',str(support/'docker/compose.rust-playback.yml'),'-f',str(support/'scripts/rust-vod-contracts/compose.yml'),'-f',str(overlay),'-f',str(portfile)];index=0;jobs={}
 def stop_owned():
  v=subprocess.run(compose+['kill'],env=env,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=15)
  if v.returncode:raise RuntimeError('AuthorityOwnedStopUnacknowledged')
 def command(argv,timeout=120,command_env=None):
  nonlocal index
  index+=1;return bounded.Collected(argv,env if command_env is None else command_env,8388608,stop_owned,stderr_path=out/('command-'+str(index)+'.stderr')).wait(timeout)
 def run(*args,timeout=120):return command(compose+list(args),timeout)
 def helper(database,action,*args):return json.loads(run('exec','-T','-e','POSTGRES_DB='+database,'baseline','/dispatcharrpy/bin/python','/pending-tools/db_fixture.py',action,*args).splitlines()[-1])
 def child(role,argv):jobs[role]=bounded.Collected(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/pending-tools/owned_process.py','start',role,*argv],env,8388608,stop_owned,stdout_path=out/(role+'.stdout'),stderr_path=out/(role+'.stderr'))
 def finish(role):
  run('exec','-T','baseline','/dispatcharrpy/bin/python','/pending-tools/owned_process.py','stop',role)
  try:jobs[role].wait(15)
  except bounded.CollectionFailure:
   if jobs[role].invalid is not None or jobs[role].process.returncode not in[0,143,-15]or any(t.is_alive()for t in jobs[role].threads):raise
 default_before=None
 try:
  run('up','-d','--no-build','baseline');end=time.monotonic()+180
  while True:
   try:run('exec','-T','baseline','/dispatcharrpy/bin/python','-c',"import urllib.request;assert urllib.request.urlopen('http://127.0.0.1:5656/api/core/version/',timeout=2).status==200",timeout=5);break
   except bounded.CollectionFailure:
    if time.monotonic()>end:raise TimeoutError('BaselineReady')
    time.sleep(.2)
  run('exec','-T','baseline','/dispatcharrpy/bin/python','/verification/supervisor.py','control','stop');default_before=helper('dispatcharr','default-audit')
  run('exec','-T','-e','PGPASSWORD=playback-fixture-only','baseline','psql','-h','127.0.0.1','-U','dispatch','-d','postgres','-v','ON_ERROR_STOP=1','-c','CREATE DATABASE '+db)
  helper(db,'migrate');helper(db,'seed');helper(db,'cap','1');before=helper(db,'fingerprint');seed=json.loads((out/'pending-main-private.json').read_text())
  child('authority-redis',['redis-server','--bind','0.0.0.0','--port','6391','--requirepass','playback-fixture-only','--save','','--appendonly','no','--dir','/tmp'])
  proof=load(controller/'candidate_inputs.py','authority_candidate').candidate(REPO/'artifacts/rust/candidate-build-cli-follower-image49-1/build.json','dispatcharr-rust-app:cli-follower-image49',controller/'expected-runtime.json',REPO/'artifacts/rust/candidate-build-cli-follower-image49-1/source.tar')
  child('authority-proxy',['/dispatcharrpy/bin/python','/pending-tools/physical_proxy.py','--sha',proof['reserve_script_sha1'],'--source-profile',str(seed['provider_profile_id'])]);end=time.monotonic()+20
  while True:
   try:assert run('exec','-T','baseline','redis-cli','-p','6381','-a','playback-fixture-only','PING').strip()==b'PONG';break
   except (bounded.CollectionFailure,AssertionError):
    if time.monotonic()>end:raise TimeoutError('AuthorityProxyReady')
    time.sleep(.1)
  run('exec','-T','-e','POSTGRES_DB='+db,'-e','REDIS_PORT=6381','baseline','/dispatcharrpy/bin/python','/pending-tools/source_probe.py',timeout=30);report['Original']=json.loads((out/'Original-physical-authority.json').read_text());assert report['Original']['passed']
  address=run('port','baseline','6381').decode().strip();native_env=dict(env,REDIS_TEST_URL='redis://:playback-fixture-only@'+address+'/0',PENDING_CAUSAL_FIXTURE='playback-fixture-only',PENDING_CAUSAL_OUTPUT=str(out/'native-Coordinator-causal.json'))
  command([str(ROOT/'native-causal-tests'),'actual_unchanged_two_second_observer_loses_success_before_physical_reply','--ignored','--nocapture','--test-threads=1'],30,native_env);report['native']=json.loads((out/'native-Coordinator-causal.json').read_text());assert report['native']['passed'];report['copied_rows_sequences_unchanged']=helper(db,'fingerprint')==before;assert report['copied_rows_sequences_unchanged'];report['passed']=True
 except Exception as error:report.update(error_type=type(error).__name__,error_message=str(error))
 finally:
  for role in jobs:
   try:finish(role)
   except Exception as error:report['passed']=False;report.setdefault('cleanup_errors',[]).append(role+type(error).__name__)
  if default_before:
   try:report['default_rows_sequences_unchanged']=helper('dispatcharr','default-audit')==default_before
   except Exception:report['default_rows_sequences_unchanged']=False
   if not report['default_rows_sequences_unchanged']:report['passed']=False
  p=out/'physical-proxy-private.json'
  if p.exists():
   proxy=json.loads(p.read_text());report['proxy']=proxy
   if proxy['invalid']or proxy['connections']!=0 or proxy['counts']!={'source':2,'native':1}:report['passed']=False
  else:report['passed']=False;report['proxy_missing']=True
  try:run('down','--volumes','--remove-orphans','--timeout','10',timeout=120)
  except Exception as error:report['passed']=False;report.setdefault('cleanup_errors',[]).append('down'+type(error).__name__)
  report['remaining']={}
  for kind,args in{'containers':['ps','-aq'],'networks':['network','ls','-q'],'volumes':['volume','ls','-q']}.items():
   try:report['remaining'][kind]=command(['docker',*args,'--filter','label=com.docker.compose.project='+project]).decode().strip()
   except Exception:report['remaining'][kind]='UNKNOWN'
  if any(report['remaining'].values()):report['passed']=False
  save(out/'report.json',report)
 print(json.dumps({'passed':report['passed'],'error_type':report.get('error_type'),'remaining':report['remaining']}));return 0 if report['passed']else 1
if __name__=='__main__':raise SystemExit(main())
