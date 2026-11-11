#!/usr/bin/env python3
"""Actual Original/native production core trace with real Redis; no benchmark.
Original Dispatcharr authors retain full application credit and AGPL licensing.
"""
import argparse,hashlib,json,os,subprocess,sys,time,urllib.request,uuid,shutil
from pathlib import Path
ROOT=Path.cwd().resolve();SUPPORT=ROOT/'scripts/rust-core-perf/support'
_expected=json.loads((SUPPORT.parent/'support-manifest.json').read_text())
_actual={str(p.relative_to(SUPPORT)):hashlib.sha256(p.read_bytes()).hexdigest()for p in SUPPORT.rglob('*')if p.is_file()and'__pycache__'not in p.parts}
if _actual!=_expected:raise ValueError('Committed complete support map mismatch')
sys.path.insert(0,str(SUPPORT/'scripts/rust-integration'))
from run import BASELINE,fixture_source
from fixture_ports import write_port_overlay
from tool_snapshot import snapshot_sources

def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',required=True,type=Path);p.add_argument('--binary',required=True,type=Path);p.add_argument('--build-record',type=Path,required=True);p.add_argument('--compile-root',type=Path,required=True);p.add_argument('--source-only',action='store_true');p.add_argument('--scenario',choices=['full','pending-marker'],default='full');p.add_argument('--extra-binary',type=Path,action='append',default=[]);p.add_argument('--source-script',type=Path,default=None);a=p.parse_args();out=a.output.resolve();out.mkdir(parents=True,exist_ok=True)
 if any(out.iterdir()):p.error('Empty output required')
 build=json.loads(a.build_record.read_text())
 for binary in [a.binary,*a.extra_binary]:
  if hashlib.sha256(binary.read_bytes()).hexdigest()!=build['binaries'][binary.name]:raise ValueError('Built peer hash mismatch')
 if a.source_script is None:a.source_script=Path(__file__).resolve().parent/('source_buffer_extended.py'if a.scenario=='full'else'source_buffer_boundary.py')
 tools=out/'tool-source';shutil.copytree(SUPPORT,tools,ignore=shutil.ignore_patterns('__pycache__'))
 target=tools/'scripts/rust-core-contracts/source_buffer.py';target.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(a.source_script.resolve(),target);shutil.copy2(Path(__file__).resolve(),out/'controller.py');shutil.copy2(Path(__file__).resolve().parent/'trace_contract.py',out/'trace_contract.py');shutil.copy2(a.build_record,out/'build-record.json')
 hashes={str(x.relative_to(tools)):hashlib.sha256(x.read_bytes()).hexdigest()for x in tools.rglob('*')if x.is_file() and '__pycache__'not in x.parts}
 shutil.copy2(a.binary.resolve(),out/'native-core-peer');(out/'native-core-peer').chmod(0o755)
 nonce=uuid.uuid4().hex[:10];name='rust_probe_backup_jobs_'+nonce;project='dispatcharr-core-buffer-'+nonce
 report={'passed':False,'scope':'Actual Original StreamBuffer versus native production assembly/writer/InputStore/Channel/History, physical ownership and legacy component regressions; no performance transfer','project':project,'copied_database':name,'original_revision':BASELINE,'tool_source_sha256':hashes,'native_jobs_verified':False,'performance_verified':False,'native_build_record_sha256':hashlib.sha256(a.build_record.read_bytes()).hexdigest(),'native_source_map_canonical_sha256':build['source_map_canonical_sha256'],'native_binary_sha256':build['binaries'][a.binary.name],'complete_support_map_sha256':hashlib.sha256(json.dumps(_expected,sort_keys=True,separators=(',',':')).encode()).hexdigest()};status=1
 with fixture_source(report,False,out)as temp:
  source=Path(temp)/'source';source.mkdir();source.chmod(0o755);archive=subprocess.check_output(['git','archive',BASELINE],cwd=ROOT);subprocess.run(['tar','-x','--same-permissions','--no-same-owner','-C',str(source)],input=archive,check=True);report['original_archive_sha256']=hashlib.sha256(archive).hexdigest();assert report['original_archive_sha256']=='f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff';(source/'media').mkdir(exist_ok=True);(source/'rust/crates/input-store').mkdir(parents=True,exist_ok=True)
  env=dict(os.environ,BASELINE_SOURCE_DIR=str(source),BASELINE_EVIDENCE_DIR=str(out),DISPATCHARR_ENV='aio')
  overlay=out/'job-fixture.yml';overlay.write_text('services:\n  baseline:\n    volumes:\n      - '+str(tools/'scripts/rust-core-contracts')+':/job-tools:ro\n      - '+str(tools/'scripts/rust-integration')+':/backup-tools:ro\n      - '+str(source)+':'+str(a.compile_root.resolve())+':ro\n  origin:\n    entrypoint: [/dispatcharrpy/bin/python, /backup-tools/logo_origin.py]\n    volumes:\n      - '+str(tools/'scripts/rust-integration')+':/backup-tools:ro\n')
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
   args=compose+['exec','-T','-e','POSTGRES_DB='+name,'-e','INDEX_PRODUCTION_CONNECTOR=1','-e','BACKUP_ROOT=/evidence/jobs-backups','-e','M1_CORE_DISPOSABLE_FIXTURE=playback-fixture-only','-e','CELERY_BROKER_URL=redis://:playback-fixture-only@localhost:6379/8','-e','CELERY_RESULT_BACKEND=redis://:playback-fixture-only@localhost:6379/9','baseline','/dispatcharrpy/bin/python','/job-tools/source_buffer.py']
   result=subprocess.run(args,env=env,text=True,capture_output=True,timeout=480);(out/'source-controller-private.log').write_text(result.stdout+result.stderr);result.check_returncode()
   actual=json.loads(result.stdout.splitlines()[-1]);(out/'source-core-buffer.json').write_text(json.dumps(actual,indent=2)+'\n')
   differences=[];observed={}
   if not a.source_only:
    native_args=compose+['exec','-T','-e','POSTGRES_DB='+name,'-e','M1_CORE_DISPOSABLE_FIXTURE=owned-core-only','-e','REDIS_URL=redis://:playback-fixture-only@127.0.0.1:6379/0','-e','M1_CORE_OUTPUT=/evidence/native-core-buffer.json','baseline','/evidence/native-core-peer',('engine::core_trace_peer::actual_production_source13_trace'if a.scenario=='full'else'engine::core_trace_peer::pending_marker_boundary::actual_pending_marker_at_payload_cap'),'--exact','--ignored','--nocapture']
    native=subprocess.run(native_args,env=env,text=True,capture_output=True,timeout=120);(out/'native-controller.log').write_text(native.stdout+native.stderr);native.check_returncode();observed=json.loads((out/'native-core-buffer.json').read_text())
    from trace_contract import compare,compare_pending
    differences=(compare if a.scenario=='full'else compare_pending)(actual,observed)
   for extra in a.extra_binary:
    target=out/extra.name;shutil.copy2(extra.resolve(),target);target.chmod(0o755)
    result=subprocess.run(compose+['exec','-T','-e','REDIS_TEST_URL=redis://:playback-fixture-only@127.0.0.1:6379/7','-e','M1_CORE_DISPOSABLE_FIXTURE=owned-core-only','baseline','/evidence/'+extra.name,'--ignored','--test-threads=1'],env=env,text=True,capture_output=True,timeout=240)
    (out/('extra-'+extra.name+'.log')).write_text(result.stdout+result.stderr);result.check_returncode()
   (out/'differences.json').write_text(json.dumps(differences,indent=2)+'\n');after=helper('dispatcharr','baseline_manifest');assert before==after,'Default fixtureDB changed'
   report.update(passed=not differences,checks=len(actual['records']),default_database_unchanged=True,differences=len(differences),native_core_verified=not differences and not a.source_only,performance_verified=False,producer_finish_retained_reads=observed.get('producer_finish_retains_actual_reads',False));status=0 if report['passed']else 1

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
