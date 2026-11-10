#!/usr/bin/env python3
"""Final frozen worker transport physical/lifecycle regression runner.
Original Dispatcharr authors retain full application credit and AGPL licensing.
"""
import argparse,hashlib,json,os,subprocess,sys,time,urllib.request,uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT/'scripts/rust-integration'))
from run import BASELINE,fixture_source
from fixture_ports import write_port_overlay
from tool_snapshot import snapshot_sources

def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',required=True,type=Path);p.add_argument('--frozen-rust',required=True,type=Path);a=p.parse_args();frozen_root=a.frozen_rust.resolve();assert frozen_root.is_relative_to(ROOT/'artifacts/rust') and (frozen_root/'rust/Cargo.lock').is_file(),'Retained fixture Rust root required';out=a.output.resolve();out.mkdir(parents=True,exist_ok=True)
 if any(out.iterdir()):p.error('Empty output required')
 tools=out/'tool-source';hashes=snapshot_sources(ROOT,tools)
 for path in(ROOT/'scripts/rust-job-contracts').glob('*'):
  if path.is_file():
   data=path.read_bytes();target=tools/path.relative_to(ROOT);target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(data);hashes[str(path.relative_to(ROOT))]=hashlib.sha256(data).hexdigest()
 nonce=uuid.uuid4().hex[:10];name='rust_probe_backup_jobs_'+nonce;project='dispatcharr-final-job-resources-'+nonce
 report={'passed':False,'scope':'Frozen complete worker transport physical/lifecycle and ETA limits regression; no algorithm/application claim','project':project,'copied_database':name,'original_revision':BASELINE,'tool_source_sha256':hashes,'native_jobs_verified':False,'performance_verified':False};status=1
 with fixture_source(report,False,out)as temp:
  source=Path(temp)/'source';source.mkdir();source.chmod(0o755);archive=subprocess.check_output(['git','archive',BASELINE],cwd=ROOT);subprocess.run(['tar','-x','--same-permissions','--no-same-owner','-C',str(source)],input=archive,check=True);report['original_archive_sha256']=hashlib.sha256(archive).hexdigest();(source/'media').mkdir(exist_ok=True)
  env=dict(os.environ,BASELINE_SOURCE_DIR=str(source),BASELINE_EVIDENCE_DIR=str(out),DISPATCHARR_ENV='aio')
  overlay=out/'job-fixture.yml';overlay.write_text('services:\n  baseline:\n    volumes:\n      - '+str(tools/'scripts/rust-job-contracts')+':/job-tools:ro\n      - '+str(tools/'scripts/rust-integration')+':/backup-tools:ro\n  origin:\n    entrypoint: [/dispatcharrpy/bin/python, /backup-tools/logo_origin.py]\n    volumes:\n      - '+str(tools/'scripts/rust-integration')+':/backup-tools:ro\n')
  with overlay.open('a')as file:file.write('networks:\n  default:\n    internal: true\n')
  ports=out/'fixture-ports.yml';report['ports']=write_port_overlay(ports,{'baseline':[5656],'origin':[8080]});compose=['docker','compose','-p',project,'-f',str(tools/'docker/compose.rust-playback.yml'),'-f',str(overlay),'-f',str(ports)]
  def command(args,body=None,timeout=180):return subprocess.check_output(args,env=env,input=body,text=True,stderr=subprocess.PIPE,timeout=timeout)
  def helper(database,action):
   raw=command(compose+['exec','-T','-e','POSTGRES_DB='+database,'-e','BACKUP_DISPOSABLE_FIXTURE=playback-fixture-only','baseline','/dispatcharrpy/bin/python','/backup-tools/backup_fixture.py'],json.dumps({'action':action}));return json.loads(raw.splitlines()[-1])
  # Freeze complete compiler inputs, then build from the retained copy.
  import tarfile,io,shutil
  frozen=out/'frozen'
  captured={}
  for file in(frozen_root/'rust').rglob('*'):
   if file.is_file()and not set(file.relative_to(frozen_root/'rust').parts)&{'target','.git','__pycache__'}:
    if file.is_symlink():raise RuntimeError('Regular source required')
    captured[str(file.relative_to(frozen_root))]=file.read_bytes()
  with tarfile.open(out/'rust-source.tar','w')as archive:
   for relative,data in sorted(captured.items()):
    member=tarfile.TarInfo(relative);member.size=len(data);member.mode=0o644;member.mtime=0;archive.addfile(member,io.BytesIO(data))
    target=frozen/relative;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(data)
  report['rust_source_sha256']={k:hashlib.sha256(v).hexdigest()for k,v in captured.items()}
  report['rust_source_archive_sha256']=hashlib.sha256((out/'rust-source.tar').read_bytes()).hexdigest()
  buildenv=dict(env,CARGO_TARGET_DIR=str(out/'target'))
  built=subprocess.run(['cargo','test','--manifest-path',str(frozen/'rust/Cargo.toml'),'-p','dispatcharr-job-transport','--test','resources','--no-run','--message-format=json','--locked','--offline'],env=buildenv,capture_output=True,text=True,timeout=600)
  (out/'build.log').write_text(built.stdout+built.stderr);built.check_returncode()
  products=[json.loads(line)for line in built.stdout.splitlines()if line.startswith('{')]
  executable=next(Path(v['executable'])for v in products if v.get('executable')and v.get('target',{}).get('name')=='resources')
  binary=out/'native-job-resources';shutil.copy2(executable,binary);binary.chmod(0o755);report['binary_sha256']=hashlib.sha256(binary.read_bytes()).hexdigest()
  eta_build=subprocess.run(['cargo','test','--manifest-path',str(frozen/'rust/Cargo.toml'),'-p','dispatcharr-job-transport','--test','eta_limits','--no-run','--message-format=json','--locked','--offline'],env=buildenv,capture_output=True,text=True,timeout=600);(out/'eta-build.log').write_text(eta_build.stdout+eta_build.stderr);eta_build.check_returncode()
  eta_products=[json.loads(line)for line in eta_build.stdout.splitlines()if line.startswith('{')];eta_executable=next(Path(v['executable'])for v in eta_products if v.get('executable')and v.get('target',{}).get('name')=='eta_limits');eta_binary=out/'native-eta-limits';shutil.copy2(eta_executable,eta_binary);eta_binary.chmod(0o755);report['eta_binary_sha256']=hashlib.sha256(eta_binary.read_bytes()).hexdigest()
  before=None
  try:
   command(compose+['up','-d','baseline']);deadline=time.monotonic()+180
   while True:
    try:
     command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','-c',"import urllib.request; r=urllib.request.urlopen('http://127.0.0.1:5656/api/core/version/',timeout=2); assert r.status==200"],timeout=5)
     break
    except subprocess.SubprocessError:
     if time.monotonic()>deadline:raise RuntimeError('Internal original readiness failed')
     time.sleep(.2)
   before=helper('dispatcharr','baseline_manifest')
   command(compose+['exec','-T','-e','PGPASSWORD=playback-fixture-only','baseline','psql','-h','127.0.0.1','-U','dispatch','-d','postgres','-v','ON_ERROR_STOP=1','-c','CREATE DATABASE '+name])
   seed=helper(name,'seed');report['copied_nonempty_tables']=sum(v['count']>0 for v in seed['manifest']['tables'].values())
   args=compose+['exec','-T','-e','POSTGRES_DB='+name,'-e','JOB_DISPOSABLE_FIXTURE=playback-fixture-only','-e','JOB_REDIS_TEST_URL=redis://:playback-fixture-only@127.0.0.1:6379/0','baseline','/evidence/native-job-resources','--ignored','--nocapture','--test-threads=1']
   # These individual physical tests historically ran in independent fixtures.
   # Reset only this project's dedicated broker8/backend9 between test cases;
   # never touch Source default DB0 or the application PostgreSQL database.
   listing=command(args[:args.index('/evidence/native-job-resources')+1]+['--list','--ignored'])
   names=[line.split(': test')[0]for line in listing.splitlines()if line.endswith(': test')];assert len(names)==11
   logs=[];count=0
   for test_name in names:
    reset="import os,redis; assert os.environ['POSTGRES_DB'].startswith('rust_probe_backup_jobs_'); [(redis.Redis(host='127.0.0.1',port=6379,password='playback-fixture-only',db=n).flushdb()) for n in (8,9)]"
    command(compose+['exec','-T','-e','POSTGRES_DB='+name,'baseline','/dispatcharrpy/bin/python','-c',reset])
    one_args=args[:args.index('/evidence/native-job-resources')+1]+['--ignored','--exact',test_name,'--nocapture','--test-threads=1']
    one=subprocess.run(one_args,env=env,text=True,capture_output=True,timeout=90);logs.append('CASE '+test_name+'\n'+one.stdout+one.stderr);(out/'resource-tests.log').write_text('\n'.join(logs));one.check_returncode();count+=1
   report['physical_test_isolation']='Dedicated owned broker8/result9 reset between individual physical unit tests; no Source application/DB0 task consumption.'
   eta_args=args.copy();eta_args[eta_args.index('/evidence/native-job-resources')]='/evidence/native-eta-limits';eta_result=subprocess.run(eta_args,env=env,text=True,capture_output=True,timeout=90);(out/'eta-tests.log').write_text(eta_result.stdout+eta_result.stderr);eta_result.check_returncode()
   import re
   eta_count=int(re.findall(r'test result: ok. (\d+) passed',eta_result.stdout)[-1]);assert count==11 and eta_count==3
   after=helper('dispatcharr','baseline_manifest');assert before==after,'Default fixtureDB changed'
   report.update(passed=True,tests=count,eta_tests=eta_count,eta_tests_log_sha256=hashlib.sha256((out/'eta-tests.log').read_bytes()).hexdigest(),tests_log_sha256=hashlib.sha256((out/'resource-tests.log').read_bytes()).hexdigest(),default_database_unchanged=True,native_worker_application_verified=False,index_driver_lifecycle_component_verified=True);status=0
  except(OSError,ValueError,RuntimeError,AssertionError,subprocess.SubprocessError)as error:
   report['error_type']=type(error).__name__
   if isinstance(error,subprocess.CalledProcessError):(out/'failure-private.log').write_text(str(error.stderr or '')[:65536])
  finally:
   if before is not None:
    try:
     report['default_database_unchanged']=helper('dispatcharr','baseline_manifest')==before
     if not report['default_database_unchanged']:report['passed']=False;status=1
    except subprocess.SubprocessError:report['default_database_check_failed']=True;report['passed']=False;status=1
   try:command(compose+['down','--volumes','--remove-orphans']);report['cleanup']='project removed'
   except subprocess.SubprocessError:report.update(passed=False,cleanup_error=True);status=1
 print(json.dumps({'passed':report['passed'],'evidence':str(out)}));return status
if __name__=='__main__':raise SystemExit(main())
