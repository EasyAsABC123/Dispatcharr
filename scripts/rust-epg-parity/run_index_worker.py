#!/usr/bin/env python3
"""Original-only real programme-index worker contract.
Original Dispatcharr authors retain full application credit and AGPL licensing.
"""
import argparse,hashlib,io,json,os,shutil,subprocess,sys,tarfile,time,urllib.parse,urllib.request,uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT/'scripts/rust-integration'))
from run import BASELINE,fixture_source
from fixture_ports import write_port_overlay
from tool_snapshot import snapshot_sources

def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',required=True,type=Path);p.add_argument('--controller',choices=['source_index_worker.py','source_index_paths.py','source_index_eta.py','source_index_eta_order.py','source_current_dispatch_failure.py','source_index_path_task.py','source_current_http_driver.py','source_index_failures.py','source_index_initialization.py'],default='source_index_worker.py');p.add_argument('--native-failure-scan',action='store_true');p.add_argument('--native-http-driver',action='store_true');p.add_argument('--native-paths',action='store_true');p.add_argument('--native-dispatch-failure',action='store_true');p.add_argument('--native-eta-limits',action='store_true');a=p.parse_args();out=a.output.resolve();out.mkdir(parents=True,exist_ok=True)
 if any(out.iterdir()):p.error('Empty output required')
 tools=out/'tool-source';hashes=snapshot_sources(ROOT,tools)
 for path in(ROOT/'scripts/rust-epg-parity').glob('*'):
  if path.is_file():
   data=path.read_bytes();target=tools/path.relative_to(ROOT);target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(data);hashes[str(path.relative_to(ROOT))]=hashlib.sha256(data).hexdigest()
 nonce=uuid.uuid4().hex[:10];database_name='rust_probe_backup_jobs_'+nonce;project='dispatcharr-index-worker-'+nonce
 report={'passed':False,'scope':'Original-only real index producer/worker; no native worker','project':project,'copied_database':database_name,'original_revision':BASELINE,'tool_source_sha256':hashes,'native_jobs_verified':False,'performance_verified':False};status=1
 if a.native_paths or a.native_dispatch_failure or a.native_eta_limits or a.native_http_driver or a.native_failure_scan:
  if sum([a.native_paths,a.native_dispatch_failure,a.native_eta_limits,a.native_http_driver,a.native_failure_scan])>1:p.error('Choose one native component gate')
  if a.native_failure_scan and a.controller!='source_index_failures.py':p.error('Failure scan requires actualOriginal worker corpus')
  if a.native_http_driver and a.controller!='source_current_http_driver.py':p.error('HTTP driver requires its actual Original reference')
  if a.native_eta_limits and a.controller!='source_index_worker.py':p.error('ETA limit gate uses the owned original base fixture')
  if a.native_paths and a.controller!='source_index_paths.py':p.error('Native path gate requires owned path controller')
  if a.native_dispatch_failure and a.controller!='source_current_dispatch_failure.py':p.error('Dispatch failure gate requires its Original HTTP dependency observer')
  captured={}
  for base,directories,names in os.walk(ROOT/'rust'):
   directories[:]=sorted(name for name in directories if name not in {'target','.git','__pycache__'})
   for name in sorted(names):
    path=Path(base)/name
    if path.is_symlink()or not path.is_file():raise ValueError('Regular snapshot files required')
    captured[str(path.relative_to(ROOT))]=path.read_bytes()
  report['rust_source_sha256']={name:hashlib.sha256(data).hexdigest()for name,data in captured.items()}
  with tarfile.open(out/'rust-source.tar','w')as archive:
   for name,data in sorted(captured.items()):
    info=tarfile.TarInfo(name);info.size=len(data);info.mode=0o644;info.mtime=0;archive.addfile(info,io.BytesIO(data))
  report['rust_source_archive_sha256']=hashlib.sha256((out/'rust-source.tar').read_bytes()).hexdigest()
 with fixture_source(report,False,out)as temp:
  source=Path(temp)/'source';source.mkdir();source.chmod(0o755);archive=subprocess.check_output(['git','archive',BASELINE],cwd=ROOT);subprocess.run(['tar','-x','--same-permissions','--no-same-owner','-C',str(source)],input=archive,check=True);report['original_archive_sha256']=hashlib.sha256(archive).hexdigest();(source/'media').mkdir(exist_ok=True)
  env=dict(os.environ,BASELINE_SOURCE_DIR=str(source),BASELINE_EVIDENCE_DIR=str(out),DISPATCHARR_ENV='aio')
  overlay=out/'job-fixture.yml';overlay.write_text('services:\n  baseline:\n    volumes:\n      - '+str(tools/'scripts/rust-epg-parity')+':/index-tools:ro\n      - '+str(tools/'scripts/rust-integration')+':/backup-tools:ro\n  origin:\n    entrypoint: [/dispatcharrpy/bin/python, /backup-tools/logo_origin.py]\n    volumes:\n      - '+str(tools/'scripts/rust-integration')+':/backup-tools:ro\n')
  with overlay.open('a')as file:file.write('networks:\n  default:\n    internal: true\n')
  ports=out/'fixture-ports.yml';report['ports']=write_port_overlay(ports,{'baseline':[5656],'origin':[8080]});compose=['docker','compose','-p',project,'-f',str(tools/'docker/compose.rust-playback.yml'),'-f',str(overlay),'-f',str(ports)]
  def command(args,body=None,timeout=180):return subprocess.check_output(args,env=env,input=body,text=True,stderr=subprocess.PIPE,timeout=timeout)
  def helper(database,action):
   raw=command(compose+['exec','-T','-e','POSTGRES_DB='+database,'-e','BACKUP_DISPOSABLE_FIXTURE=playback-fixture-only','baseline','/dispatcharrpy/bin/python','/backup-tools/backup_fixture.py'],json.dumps({'action':action}));return json.loads(raw.splitlines()[-1])
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
   command(compose+['exec','-T','-e','PGPASSWORD=playback-fixture-only','baseline','psql','-h','127.0.0.1','-U','dispatch','-d','postgres','-v','ON_ERROR_STOP=1','-c','CREATE DATABASE '+database_name])
   seed=helper(database_name,'seed');report['copied_nonempty_tables']=sum(v['count']>0 for v in seed['manifest']['tables'].values())
   args=compose+['exec','-T','-e','POSTGRES_DB='+database_name,'-e','BACKUP_ROOT=/evidence/jobs-backups','-e','JOB_DISPOSABLE_FIXTURE=playback-fixture-only','-e','CELERY_BROKER_URL=redis://:playback-fixture-only@localhost:6379/8','-e','CELERY_RESULT_BACKEND=redis://:playback-fixture-only@localhost:6379/9','baseline','/dispatcharrpy/bin/python','/index-tools/'+a.controller]
   result=subprocess.run(args,env=env,text=True,capture_output=True,timeout=480);(out/'source-controller-private.log').write_text(result.stdout+result.stderr);result.check_returncode()
   actual=json.loads((out/'source-index-worker-private.json').read_text());after=helper('dispatcharr','baseline_manifest');assert before==after,'Default fixtureDB changed'
   if a.native_paths or a.native_dispatch_failure or a.native_eta_limits or a.native_http_driver or a.native_failure_scan:
    frozen=out/'native-source';frozen.mkdir()
    with tarfile.open(out/'rust-source.tar')as archive:archive.extractall(frozen,filter='data')
    native_env=dict(env,CARGO_TARGET_DIR=str(out/'cargo-target'))
    targets=['current_paths_sql','index_dispatcher_sql']if a.native_paths else['index_failure_scan_sql']if a.native_failure_scan else['current_http_driver']if a.native_http_driver else['eta_limits']if a.native_eta_limits else['current_dispatch_sql']
    build_command=['cargo','test','--manifest-path',str(frozen/'rust/Cargo.toml'),'-p',('dispatcharr-job-transport'if a.native_eta_limits or a.native_http_driver else'dispatcharr-ingest-epg'),'--no-run','--message-format=json','--locked','--offline']
    for target in targets:build_command+=['--test',target]
    built=subprocess.run(build_command,env=native_env,text=True,capture_output=True,timeout=600)
    (out/'native-path-build.jsonl').write_text(built.stdout);(out/'native-path-build.log').write_text(built.stderr);built.check_returncode()
    executables={json.loads(line)['target']['name']:json.loads(line)['executable']for line in built.stdout.splitlines()if line.startswith('{')and json.loads(line).get('reason')=='compiler-artifact'and json.loads(line).get('executable')}
    if set(executables)!=set(targets):raise ValueError('Expected exact frozen native test executables')
    report['native_test_binary_sha256']={}
    for target,filename in executables.items():
     executable=Path(filename).resolve()
     if not executable.is_relative_to(out/'cargo-target'):raise ValueError('Test executable outside unique build target')
     binary=out/('native-'+target);shutil.copyfile(executable,binary);binary.chmod(0o755);report['native_test_binary_sha256'][target]=hashlib.sha256(binary.read_bytes()).hexdigest()
     native=subprocess.run(compose+['exec','-T','-e','POSTGRES_DB='+database_name,'-e','JOB_DISPOSABLE_FIXTURE=playback-fixture-only','-e','JOB_REDIS_TEST_URL=redis://:playback-fixture-only@127.0.0.1:6379/0','-e','DATABASE_TEST_URL=postgresql://dispatch:playback-fixture-only@127.0.0.1:5432/'+database_name,'-e','REDIS_TEST_URL=redis://:playback-fixture-only@127.0.0.1:6379/0','-e','EPG_PATH_DISPOSABLE_FIXTURE=playback-fixture-only','-e','EPG_PATH_FIXTURE=/evidence/source-index-worker-private.json','baseline','/evidence/native-'+target,'--ignored','--nocapture','--test-threads=1'],env=env,text=True,capture_output=True,timeout=180)
     (out/(target+'-tests.log')).write_text(native.stdout+native.stderr);native.check_returncode()
    report['native_path_component_verified']=a.native_paths;report['native_dispatcher_override_verified']=a.native_paths;report['native_current_dispatch_failure_verified']=a.native_dispatch_failure;report['native_eta_limits_verified']=a.native_eta_limits;report['native_http_index_driver_verified']=a.native_http_driver;report['native_failure_scan_verified']=a.native_failure_scan
   report.update(passed=actual['passed'],checks=len(actual['checks']),default_database_unchanged=True,source_report_sha256=hashlib.sha256((out/'source-index-worker-private.json').read_bytes()).hexdigest(),captures=len(actual['captures']),worker_started=actual['worker_started']);status=0 if report['passed']else 1
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
