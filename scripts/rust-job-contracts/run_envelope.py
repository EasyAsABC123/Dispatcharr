#!/usr/bin/env python3
"""DRAFT UNEXECUTED: complete original protocol2 producer-envelope extension.
Original Dispatcharr authors retain full application credit and AGPL licensing.
"""
import argparse,hashlib,json,os,subprocess,sys,time,urllib.request,uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT/'scripts/rust-integration'))
from run import BASELINE,fixture_source
from fixture_ports import write_port_overlay
from tool_snapshot import snapshot_sources

def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',required=True,type=Path);a=p.parse_args();out=a.output.resolve();out.mkdir(parents=True,exist_ok=True)
 if any(out.iterdir()):p.error('Empty output required')
 tools=out/'tool-source';hashes=snapshot_sources(ROOT,tools)
 for path in(ROOT/'scripts/rust-job-contracts').glob('*'):
  if path.is_file():
   data=path.read_bytes();target=tools/path.relative_to(ROOT);target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(data);hashes[str(path.relative_to(ROOT))]=hashlib.sha256(data).hexdigest()
 nonce=uuid.uuid4().hex[:10];name='rust_probe_backup_jobs_'+nonce;project='dispatcharr-job-envelopes-'+nonce
 report={'passed':False,'scope':'Original-only full envelope producer capture; no worker/native transport','project':project,'copied_database':name,'original_revision':BASELINE,'tool_source_sha256':hashes,'native_jobs_verified':False,'performance_verified':False};status=1
 with fixture_source(report,False,out)as temp:
  source=Path(temp)/'source';source.mkdir();source.chmod(0o755);archive=subprocess.check_output(['git','archive',BASELINE],cwd=ROOT);subprocess.run(['tar','-x','--same-permissions','--no-same-owner','-C',str(source)],input=archive,check=True);report['original_archive_sha256']=hashlib.sha256(archive).hexdigest();(source/'media').mkdir(exist_ok=True)
  env=dict(os.environ,BASELINE_SOURCE_DIR=str(source),BASELINE_EVIDENCE_DIR=str(out),DISPATCHARR_ENV='aio')
  overlay=out/'job-fixture.yml';overlay.write_text('services:\n  baseline:\n    volumes:\n      - '+str(tools/'scripts/rust-job-contracts')+':/job-tools:ro\n      - '+str(tools/'scripts/rust-integration')+':/backup-tools:ro\n  origin:\n    entrypoint: [/dispatcharrpy/bin/python, /backup-tools/logo_origin.py]\n    volumes:\n      - '+str(tools/'scripts/rust-integration')+':/backup-tools:ro\n')
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
   command(compose+['exec','-T','-e','PGPASSWORD=playback-fixture-only','baseline','psql','-h','127.0.0.1','-U','dispatch','-d','postgres','-v','ON_ERROR_STOP=1','-c','CREATE DATABASE '+name])
   seed=helper(name,'seed');report['copied_nonempty_tables']=sum(v['count']>0 for v in seed['manifest']['tables'].values())
   args=compose+['exec','-T','-e','POSTGRES_DB='+name,'-e','BACKUP_ROOT=/evidence/jobs-backups','-e','JOB_DISPOSABLE_FIXTURE=playback-fixture-only','-e','CELERY_BROKER_URL=redis://:playback-fixture-only@localhost:6379/8','-e','CELERY_RESULT_BACKEND=redis://:playback-fixture-only@localhost:6379/9','baseline','/dispatcharrpy/bin/python','/job-tools/source_envelope.py']
   result=subprocess.run(args,env=env,text=True,capture_output=True,timeout=480);(out/'source-controller-private.log').write_text(result.stdout+result.stderr);result.check_returncode()
   actual=json.loads((out/'source-job-envelope-private.json').read_text());after=helper('dispatcharr','baseline_manifest');assert before==after,'Default fixtureDB changed'
   report.update(passed=actual['passed'],checks=len(actual['checks']),default_database_unchanged=True,source_report_sha256=hashlib.sha256((out/'source-job-envelope-private.json').read_bytes()).hexdigest(),captures=len(actual['captures']),worker_started=actual['worker_started']);status=0 if report['passed']else 1
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
