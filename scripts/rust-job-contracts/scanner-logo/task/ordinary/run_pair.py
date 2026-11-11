#!/usr/bin/env python3
"""Actual bounded native logo component against immutable Original reference, fixture only.
Original Dispatcharr authors retain application/algorithm credit and licensing.
Explicit immutable binary/support inputs; no mutable Rust capture or app hooks.
"""
import argparse,hashlib,importlib.util,json,os,shutil,subprocess,sys,time,uuid
from pathlib import Path
BASE='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
ARCHIVE='f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff'
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--repo',type=Path,required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--support-root',type=Path,required=True);p.add_argument('--resources',action='store_true');p.add_argument('--force-resource-exit',type=int,default=0);p.add_argument('--native-binary',type=Path,required=True);p.add_argument('--float-binary',type=Path,required=True);p.add_argument('--reference',type=Path,required=True);p.add_argument('--mtime-reference',type=Path,required=True);a=p.parse_args()
 if a.force_resource_exit and (not a.resources or not 1<=a.force_resource_exit<=125):p.error('Owned resources fault requires --resources and exit1..125')
 root=a.repo.resolve();out=a.output.resolve();out.mkdir(parents=True,exist_ok=False);support=out/'support';shutil.copytree(a.support_root.resolve(),support,ignore=shutil.ignore_patterns('__pycache__'))
 sys.path.insert(0,str(support/'scripts/rust-integration'))
 spec=importlib.util.spec_from_file_location('_default_pair_fixture_support',support/'scripts/rust-integration/run.py');helper=importlib.util.module_from_spec(spec);spec.loader.exec_module(helper)
 from fixture_ports import write_port_overlay
 nonce=uuid.uuid4().hex[:10];database='rust_probe_backup_jobs_'+nonce;project='dispatcharr-scanner-source-'+nonce
 tools=out/'pair-tools';tools.mkdir()
 for name in ['run_pair.py','source_pair.py','periodic_worker_observer.py']:shutil.copy2(Path(__file__).with_name(name),tools/name)
 shutil.copy2(a.native_binary.resolve(),out/'native-logo-peer');shutil.copy2(a.float_binary.resolve(),out/'native-float-peer');shutil.copy2(a.reference.resolve(),out/'paired-reference.json');shutil.copy2(a.mtime_reference.resolve(),out/'mtime-reference.json')
 report={'passed':False,'scope':'Native bounded logo-only component effects against frozen Original12 reference; real SQL/Redis/Channels and same-file stat differential; no task consumer or scanner terminal writer',
   'native_elf_sha256':hashlib.sha256((out/'native-logo-peer').read_bytes()).hexdigest(),'float_elf_sha256':hashlib.sha256((out/'native-float-peer').read_bytes()).hexdigest(),'reference_sha256':hashlib.sha256((out/'paired-reference.json').read_bytes()).hexdigest(),'mtime_reference_sha256':hashlib.sha256((out/'mtime-reference.json').read_bytes()).hexdigest(),'project':project,'copied_database':database,'original_revision':BASE,'native_verified':False,
   'tools_sha256':{x.name:hashlib.sha256(x.read_bytes()).hexdigest()for x in tools.iterdir()},'performance_verified':False,'default_registry_enabled':False}
 status=1
 with helper.fixture_source(report,False,out)as temporary:
  source=Path(temporary)/'source';source.mkdir(mode=0o755);raw=subprocess.check_output(['git','archive',BASE],cwd=root)
  assert hashlib.sha256(raw).hexdigest()==ARCHIVE;subprocess.run(['tar','-x','--same-permissions','--no-same-owner','-C',str(source)],input=raw,check=True);(source/'media').mkdir(exist_ok=True)
  environment=dict(os.environ,BASELINE_SOURCE_DIR=str(source),BASELINE_EVIDENCE_DIR=str(out),DISPATCHARR_ENV='aio')
  overlay=out/'pair.yml';overlay.write_text(json.dumps({'services':{'baseline':{'volumes':[str(tools)+':/pair-tools:ro',str(support/'scripts/rust-integration')+':/backup-tools:ro',str(out/'native-logo-peer')+':/native-logo-peer:ro',str(out/'native-float-peer')+':/native-float-peer:ro']},'origin':{'entrypoint':['/dispatcharrpy/bin/python','-c','import time; time.sleep(3600)']}},'networks':{'default':{'internal':True}}}))
  ports=out/'ports.yml';write_port_overlay(ports,{'baseline':[5656],'origin':[8080]})
  compose=['docker','compose','-p',project,'-f',str(support/'docker/compose.rust-playback.yml'),'-f',str(overlay),'-f',str(ports)]
  def command(argv,body=None,timeout=180):return subprocess.check_output(argv,env=environment,input=body,text=True,stderr=subprocess.PIPE,timeout=timeout)
  def manifest(name,action):
   raw=command(compose+['exec','-T','-e','POSTGRES_DB='+name,'-e','BACKUP_DISPOSABLE_FIXTURE=playback-fixture-only','baseline','/dispatcharrpy/bin/python','/backup-tools/backup_fixture.py'],json.dumps({'action':action}));return json.loads(raw.splitlines()[-1])
  before=None
  try:
   command(compose+['up','-d','baseline']);end=time.monotonic()+180
   while True:
    try:
     command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','-c',"import urllib.request; assert urllib.request.urlopen('http://127.0.0.1:5656/api/core/version/',timeout=2).status==200"],timeout=5);break
    except subprocess.SubprocessError:
     if time.monotonic()>end:raise RuntimeError('Original readiness failed')
     time.sleep(.2)
   before=manifest('dispatcharr','baseline_manifest')
   (out/'baseline-before-private.json').write_text(json.dumps(before))
   command(compose+['exec','-T','-e','PGPASSWORD=playback-fixture-only','baseline','psql','-h','127.0.0.1','-U','dispatch','-d','postgres','-v','ON_ERROR_STOP=1','-c','CREATE DATABASE '+database])
   seed=manifest(database,'seed');report['copied_nonempty_tables']=sum(x['count']>0 for x in seed['manifest']['tables'].values())
   argv=compose+['exec','-T','-e','POSTGRES_DB='+database,'-e','JOB_DISPOSABLE_FIXTURE=playback-fixture-only','-e','DATABASE_URL=postgres://dispatch:playback-fixture-only@localhost:5432/'+database,
     '-e','CELERY_BROKER_URL=redis://:playback-fixture-only@localhost:6379/8','-e','CELERY_RESULT_BACKEND=redis://:playback-fixture-only@localhost:6379/9','baseline','/dispatcharrpy/bin/python','/pair-tools/source_pair.py']
   value=subprocess.run(argv,env=environment,capture_output=True,text=True,timeout=420);(out/'source-controller-private.log').write_text(value.stdout+value.stderr);value.check_returncode()
   actual=json.loads((out/'source-native-default-pair-private.json').read_text());report.update(passed=actual['passed'],checks=len(actual['checks']),native_component_verified=actual['passed'],actual_original_worker_verified=False,native_verified=actual['passed'],physical_tests=actual.get('physical_tests'));status=0 if report['passed']else 1

  except (OSError,ValueError,AssertionError,RuntimeError,subprocess.SubprocessError)as error:
   report['passed']=False;status=1;report['error_type']=type(error).__name__
   if isinstance(error,subprocess.CalledProcessError):(out/'failure-private.log').write_text(str(error.stderr or '')[-65536:])
  finally:
   if before is not None:
    try:
     after=manifest('dispatcharr','baseline_manifest');report['default_database_unchanged']=before==after;(out/'baseline-after-private.json').write_text(json.dumps(after))
     if before!=after:report['passed']=False;status=1
    except Exception as error:report['audit_error_type']=type(error).__name__;report['passed']=False;status=1
   try:command(compose+['down','--volumes','--remove-orphans']);report['cleanup']='owned project removed'
   except subprocess.SubprocessError:report['cleanup_error']=True;report['passed']=False;status=1
   (out/'integration.json').write_text(json.dumps(report,indent=2)+'\n')
 print(json.dumps({'passed':report['passed'],'output':str(out)}));return status
if __name__=='__main__':raise SystemExit(main())
