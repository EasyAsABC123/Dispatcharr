#!/usr/bin/env python3
"""Pinned Original real Beat periodic timer and worker roundtrip, Source only.
Original Dispatcharr authors retain full application credit and AGPL licensing.
"""
import argparse,hashlib,json,os,shutil,subprocess,sys,time,urllib.request,uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT/'scripts/rust-integration'))
from run import BASELINE,fixture_source
from fixture_ports import write_port_overlay
from tool_snapshot import snapshot_sources

def run(a):
 out=a.output.resolve();out.mkdir(parents=True,exist_ok=True)
 if any(out.iterdir()):raise ValueError('Empty output required')
 tools=out/'tool-source';hashes=snapshot_sources(ROOT,tools)
 for path in(ROOT/'scripts/rust-job-contracts').glob('*'):
  if path.is_file():
   data=path.read_bytes();target=tools/path.relative_to(ROOT);target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(data);hashes[str(path.relative_to(ROOT))]=hashlib.sha256(data).hexdigest()
 nonce=uuid.uuid4().hex[:10];name='rust_probe_backup_jobs_'+nonce;project='dispatcharr-scheduler-calendar-'+nonce
 report={'passed':False,'scope':'Pinned Original scheduler real real Beat timer/publication/workers/results/graceful SQLsync, no native or worker acceptance','project':project,'copied_database':name,'original_revision':BASELINE,'tool_source_sha256':hashes,'native_jobs_verified':False,'performance_verified':False};status=1
 with fixture_source(report,False,out)as temp:
  source=Path(temp)/'source';source.mkdir();source.chmod(0o755);archive=subprocess.check_output(['git','archive',BASELINE],cwd=ROOT);subprocess.run(['tar','-x','--same-permissions','--no-same-owner','-C',str(source)],input=archive,check=True);report['original_archive_sha256']=hashlib.sha256(archive).hexdigest();assert report['original_archive_sha256']=='f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff';(source/'media').mkdir(exist_ok=True)
  env=dict(os.environ,BASELINE_SOURCE_DIR=str(source),BASELINE_EVIDENCE_DIR=str(out),DISPATCHARR_ENV='aio')
  overlay=out/'job-fixture.yml';overlay.write_text('services:\n  baseline:\n    volumes:\n      - '+str(tools/'scripts/rust-job-contracts')+':/job-tools:ro\n      - '+str(tools/'scripts/rust-integration')+':/backup-tools:ro\n  origin:\n    entrypoint: [/dispatcharrpy/bin/python, /backup-tools/logo_origin.py]\n    volumes:\n      - '+str(tools/'scripts/rust-integration')+':/backup-tools:ro\n')
  with overlay.open('a')as file:file.write('networks:\n  default:\n    internal: true\n')
  ports=out/'fixture-ports.yml';report['ports']=write_port_overlay(ports,{'baseline':[5656],'origin':[8080]});docker=shutil.which('docker') if a.fall_fold_source_only else 'docker';compose=[docker,'compose','-p',project,'-f',str(tools/'docker/compose.rust-playback.yml'),'-f',str(overlay),'-f',str(ports)]
  def command(args,body=None,timeout=180):return subprocess.check_output(args,env=env,input=body,text=True,stderr=subprocess.PIPE,timeout=timeout)
  def helper(database,action):
   raw=command(compose+['exec','-T','-e','POSTGRES_DB='+database,'-e','BACKUP_DISPOSABLE_FIXTURE=playback-fixture-only','baseline','/dispatcharrpy/bin/python','/backup-tools/backup_fixture.py'],json.dumps({'action':action}));return json.loads(raw.splitlines()[-1])
  before=None;owned=None;marker=None
  def inventory():
   label='label=com.docker.compose.project='+project
   return {kind:command([docker,kind,'ls',*options,'--filter',label],timeout=20).splitlines()
           for kind,options in (('container',('-aq','--no-trunc')),('network',('-q',)),('volume',('-q',)))}
  def generation(pid):
   try:return Path(f'/proc/{pid}/stat').read_text().rsplit(')',1)[1].split()[19]
   except FileNotFoundError:return None
  try:
   if a.fall_fold_source_only:
    assert not any(inventory().values()),'fresh exact owned project required'
    claim_path=ROOT/'.dispatcharr-recording-cohort-cleanup-required.json'
    claim={'scope':'Original Calendar fall fold','output':str(out),'project':project}
    with claim_path.open('x')as stream:json.dump(claim,stream)
    marker=claim_path
    command(compose+['up','-d','--no-build','--pull','never','baseline'])
    cids=inventory()['container']
    assert len(set(cids))==2,'exact Calendar baseline and Origin CIDs required'
    inspections=json.loads(command([docker,'container','inspect',*cids]))
    assert {item['Id']for item in inspections}==set(cids)
    assert {item['Config']['Labels']['com.docker.compose.service']for item in inspections}=={'baseline','origin'}
    owned=[]
    for inspection in inspections:
     pid=inspection['State']['Pid'];birth=generation(pid)
     assert inspection['State']['Running']is True and birth
     assert inspection['Image']=='sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
     owned.append((inspection['Id'],pid,birth))
   else:command(compose+['up','-d','baseline'])
   deadline=time.monotonic()+180
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
   guest='source_calendar_fall_fold.py' if a.fall_fold_source_only else 'source_scheduler_calendar.py'
   evidence='source-calendar-fall-fold-private.json' if a.fall_fold_source_only else 'source-scheduler-calendar-private.json'
   args=compose+['exec' ,'-T','-e','POSTGRES_DB='+name,'-e','INDEX_PRODUCTION_CONNECTOR=1','-e','BACKUP_ROOT=/evidence/jobs-backups','-e','JOB_DISPOSABLE_FIXTURE=playback-fixture-only','-e','CELERY_BROKER_URL=redis://:playback-fixture-only@localhost:6379/8','-e','CELERY_RESULT_BACKEND=redis://:playback-fixture-only@localhost:6379/9','baseline','/dispatcharrpy/bin/python','/job-tools/'+guest]
   result=subprocess.run(args,env=env,text=True,capture_output=True,timeout=480);(out/'source-controller-private.log').write_text(result.stdout+result.stderr);report['guest_actual_exit']=result.returncode;result.check_returncode()
   actual=json.loads((out/evidence).read_text());after=helper('dispatcharr','baseline_manifest');assert before==after,'Default fixtureDB changed'
   report.update(passed=actual['passed'],checks=len(actual.get('checks',[])),default_database_unchanged=True,source_report_sha256=hashlib.sha256((out/evidence).read_bytes()).hexdigest(),native_scheduler_verified=False,scheduler_multi_heap_reference_verified=not a.fall_fold_source_only,task_execution_verified=False);status=0 if report['passed']else 1
   if a.fall_fold_source_only:
    from source_calendar_fall_fold import clock_log,qualify
    report['scope']='pinned Original real Beat fall fold with controlled clock dependency'
    report['controlled_fall_fold']=qualify(actual)
    report['clock_log_sha256']=clock_log(actual,out/'calendar-fall-fold-clock-private.jsonl')
  except(OSError,ValueError,RuntimeError,AssertionError,subprocess.SubprocessError)as error:
   report['error_type']=type(error).__name__;report['passed']=False;status=1
   if isinstance(error,subprocess.CalledProcessError):(out/'failure-private.log').write_text(str(error.stderr or '')[:65536])
  finally:
   if before is not None:
    try:
     report['default_database_unchanged']=helper('dispatcharr','baseline_manifest')==before
     if not report['default_database_unchanged']:report['passed']=False;status=1
    except subprocess.SubprocessError:report['default_database_check_failed']=True;report['passed']=False;status=1
   try:
    if not a.fall_fold_source_only or marker is not None:
     command(compose+['down','--volumes','--remove-orphans'])
     report['cleanup']='project removed'
    if a.fall_fold_source_only and marker is not None:
     assert owned is not None and len(owned)==2,'exact original owner generations unknown'
     retired=[]
     for cid,pid,birth in owned:
      missing=subprocess.run([docker,'container','inspect',cid],env=env,capture_output=True,timeout=20)
      assert missing.returncode==1 and generation(pid)!=birth,'exact Original fold container retirement unverified'
      retired.append({'container_inspect_exit':missing.returncode,'generation_absent':True})
     remaining=inventory()
     assert not any(remaining.values()),'exact Original fold project inventory not empty'
     assert json.loads(marker.read_text())==claim,'exact owned marker changed'
     report['exact_cleanup']={'owned_containers':retired,'inventories':remaining}
     marker.unlink()
   except (OSError,AssertionError,ValueError,subprocess.SubprocessError):report.update(passed=False,cleanup_error=True);status=1
 if a.fall_fold_source_only and (report.get('resources_retained') or not report['passed']):status=1
 print(json.dumps({'passed':report['passed'],'evidence':str(out)}));return status
def main():
 p=argparse.ArgumentParser(description=__doc__)
 p.add_argument('--output',required=True,type=Path)
 p.add_argument('--fall-fold-source-only',action='store_true')
 args=p.parse_args()
 if not args.fall_fold_source_only:return run(args)
 if not __debug__:raise RuntimeError('Original fall acquisition requires assertion-enabled Python')
 import importlib.util
 tooling=ROOT/'artifacts/rust/tooling'
 assert os.environ.get('DOCKER_HOST')=='unix:///tmp/dispatcharr-docker.sock' and not os.environ.get('DOCKER_CONTEXT')
 assert Path(os.environ.get('DOCKER_CONFIG','')).resolve()==tooling/'docker-config'
 assert Path(shutil.which('docker') or '').resolve()==tooling/'docker/docker'
 lock=ROOT/'scripts/rust-milestone-batches/.dispatcher.lock'
 markers=[folder/leaf for folder in (ROOT,lock.parent) for leaf in
          ('.dispatcharr-recording-cohort-cleanup-required.json','.cleanup-required.json')]
 spec=importlib.util.spec_from_file_location('calendar_source_functional_lease',lock.with_name('functional_lease.py'))
 lease=importlib.util.module_from_spec(spec);spec.loader.exec_module(lease)
 os.umask(0o077)
 with lease.fixture_lease(lock):
  assert not any(os.path.lexists(marker) for marker in markers)
  return run(args)

if __name__=='__main__':raise SystemExit(main())
