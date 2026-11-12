#!/usr/bin/env python3
"""Owned actual Original/native two-default-family interop, fixture only.
Original Dispatcharr authors retain application/algorithm credit and licensing.
Explicit immutable binary/support inputs; no mutable Rust capture or app hooks.
"""
import argparse,hashlib,importlib.util,json,os,shutil,subprocess,sys,time,uuid
from pathlib import Path
BASE='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
ARCHIVE='f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff'
def accept_capture(returncode,actual):
 if type(returncode)is not int or returncode!=0 or actual.get('passed')is not True or actual.get('native_verified')is not True:raise ValueError('Actual successful Native capture/exit required')
 checks=actual.get('checks');cases=actual.get('cases')
 if not isinstance(checks,list)or len(checks)<176 or any(not isinstance(v,dict)or type(v.get('name'))is not str or v.get('passed')is not True for v in checks)or len({v['name']for v in checks})!=len(checks):raise ValueError('Actual complete checks required')
 if not isinstance(cases,list)or [v.get('name')for v in cases]!=['missing-group', 'null-key', 'empty-key', 'cached-empty-map-over-url-db', 'db-url', 'cached-url-over-empty-db', 'list-key', 'oversized-db-group', 'oversized-cache-group', 'account-url-override', 'message-null', 'message-8192', 'message-8193', 'message-262144']:raise ValueError('Exact declared cases required')
 for c in cases:
  outputs=c.get('native_outputs')
  if not isinstance(outputs,list)or len(outputs)!=(2 if c.get('accept')is True else 1):raise ValueError('Exact native operation count required')
  if c.get('accept')is True:
   if any(not v.get('run','').startswith('Ok(Completed(')for v in outputs):raise ValueError('Actual accepted task completion required')
  elif c.get('accept')is False:
   if outputs[0].get('run')!='Err(Domain(Unsupported))'or c.get('Source_fallback')is not True:raise ValueError('Retained refusal/fallback required')
  else:raise ValueError('Typed accepted/refused role required')
  rows=c.get('projected_rows')
  if c['name'].startswith('message-'):
   if not isinstance(rows,dict)or type(rows.get('max_data_row_bytes'))is not int or rows.get('rows',0)<=0:raise ValueError('Real SQL DataRow witness required')
   size=rows['max_data_row_bytes']
   if c['name']=='message-8192':
    if not 8192<=size<=10000:raise ValueError('Actual exact boundary transfer required')
   elif size>2048:raise ValueError('Oversized/null projected row bound required')
 return actual
def verify_reference(path):
 expected=json.loads(Path(__file__).with_name('reference-input.json').read_text())
 raw=Path(path).read_bytes()
 if len(raw)>2097152 or hashlib.sha256(raw).hexdigest()!=expected['sha256']:raise ValueError('Exact Source70 reference required')
 return True
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--repo',type=Path,required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--support-root',type=Path,required=True);p.add_argument('--native-binary',type=Path,required=True);p.add_argument('--reference',type=Path,required=True);p.add_argument('--resources',action='store_true');p.add_argument('--force-resource-exit',type=int,default=0);a=p.parse_args()
 if a.force_resource_exit and (not a.resources or not 1<=a.force_resource_exit<=125):p.error('Owned resources fault requires --resources and exit1..125')

 expected_support=json.loads(Path(__file__).with_name('support-inputs.json').read_text())
 for name,expected in expected_support.items():
  if hashlib.sha256((a.support_root.resolve()/name).read_bytes()).hexdigest()!=expected:raise ValueError('Changed public fixture support: '+name)
 verify_reference(a.reference)
 root=a.repo.resolve();out=a.output.resolve();out.mkdir(parents=True,exist_ok=False);support=out/'support';shutil.copytree(a.support_root.resolve(),support,ignore=shutil.ignore_patterns('__pycache__'))
 sys.path.insert(0,str(support/'scripts/rust-integration'))
 spec=importlib.util.spec_from_file_location('_default_pair_fixture_support',support/'scripts/rust-integration/run.py');helper=importlib.util.module_from_spec(spec);spec.loader.exec_module(helper)
 from fixture_ports import write_port_overlay
 nonce=uuid.uuid4().hex[:10];database='rust_probe_backup_jobs_'+nonce;project='dispatcharr-scanner-children-'+nonce
 tools=out/'pair-tools';tools.mkdir()
 for name in ['run_pair.py','source_pair.py','periodic_worker_observer.py','owned_pg_row_proxy.py']:shutil.copy2(Path(__file__).with_name(name),tools/name)
 shutil.copy2(a.reference,tools/'reference.json')
 report={'native_binary_sha256':hashlib.sha256(a.native_binary.read_bytes()).hexdigest(),'passed':False,'scope':'Actual Original producer; normal Native readonly preACK hash admission/refusal and bounded last_message projection14cases',
   'project':project,'copied_database':database,'original_revision':BASE,'native_verified':False,
   'tools_sha256':{x.name:hashlib.sha256(x.read_bytes()).hexdigest()for x in tools.iterdir()},'performance_verified':False,'default_registry_enabled':False}
 status=1
 with helper.fixture_source(report,False,out)as temporary:
  source=Path(temporary)/'source';source.mkdir(mode=0o755);raw=subprocess.check_output(['git','archive',BASE],cwd=root)
  assert hashlib.sha256(raw).hexdigest()==ARCHIVE;subprocess.run(['tar','-x','--same-permissions','--no-same-owner','-C',str(source)],input=raw,check=True);(source/'media').mkdir(exist_ok=True)
  environment=dict(os.environ,BASELINE_SOURCE_DIR=str(source),BASELINE_EVIDENCE_DIR=str(out),DISPATCHARR_ENV='aio')
  overlay=out/'pair.yml';overlay.write_text(json.dumps({'services':{'baseline':{'volumes':[str(tools)+':/pair-tools:ro',str(a.native_binary.resolve())+':/native-m3u-peer:ro',str(support/'scripts/rust-integration')+':/backup-tools:ro']},'origin':{'entrypoint':['/dispatcharrpy/bin/python','-c','import time; time.sleep(3600)']}},'networks':{'default':{'internal':True}}}))
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
   value=subprocess.run(argv,env=environment,capture_output=True,text=True,timeout=540);(out/'source-controller-private.log').write_text(value.stdout+value.stderr);value.check_returncode()
   actual=accept_capture(value.returncode,json.loads((out/'source-logo-scanner-private.json').read_text()));report.update(passed=actual['passed'],checks=len(actual['checks']),actual_original_worker_verified=actual['passed'],native_verified=actual['passed'],cases=len(actual['cases']));status=0 if report['passed']else 1

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
