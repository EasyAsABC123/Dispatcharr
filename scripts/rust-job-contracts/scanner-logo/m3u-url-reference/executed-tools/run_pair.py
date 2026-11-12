#!/usr/bin/env python3
"""Actual pinned Original URL-hash19-step worker corpus, fixture only.
Original Dispatcharr authors retain application/algorithm credit and licensing.
Explicit immutable binary/support inputs; no mutable Rust capture or app hooks.
"""
import argparse,ast,hashlib,importlib.util,json,os,shutil,subprocess,sys,time,uuid
from pathlib import Path
BASE='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
ARCHIVE='f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff'
def validate_plan(plan):
 if plan['Source_revision']!=BASE or plan['native_execution'] is not False or len(plan['groups'])!=14:raise ValueError('Pinned Source-only14-group plan required')
 steps=plan['actual_worker_steps'];names=[v['label']for v in steps];seen=set()
 if len(steps)!=19 or len(set(names))!=19 or plan['fixed_per_step_drain_seconds']!=65:raise ValueError('Exact19-step fixed-drain corpus required')
 previous=0
 for item in steps:
  if item['group']not in range(1,13) or item['scanner_now']<=previous or item['scanner_now']-item['mtime']!=7.125:raise ValueError('Owned increasing fixture times required')
  created=item['cohort']not in seen;seen.add(item['cohort'])
  expected=(['apps.m3u.tasks.refresh_m3u_groups']if created else[])+['apps.m3u.tasks.refresh_single_m3u_account']
  if item['account_created_by_this_parent']is not created or item['expected_direct_child_families']!=expected:raise ValueError('Created-signal versus existing scanner family mismatch')
  previous=item['scanner_now']
  if hashlib.sha256(item['playlist'].encode()).hexdigest()!=item['playlist_sha256']:raise ValueError('Changed payload')
  if any(not row['url'].startswith('http://127.0.0.1:9/owned-url-')for row in item['rows']):raise ValueError('Owned synthetic local identifiers only')
 if any(g['step_labels'] for g in plan['groups'][12:]):raise ValueError('Native controls must remain deferred')
 return steps
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--repo',type=Path,required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--support-root',type=Path,required=True);p.add_argument('--resources',action='store_true');p.add_argument('--force-resource-exit',type=int,default=0);a=p.parse_args()
 if a.force_resource_exit and (not a.resources or not 1<=a.force_resource_exit<=125):p.error('Owned resources fault requires --resources and exit1..125')
 plan=json.loads(Path(__file__).with_name('predeclared-plan.json').read_text());validate_plan(plan)
 root=a.repo.resolve();out=a.output.resolve();out.mkdir(parents=True,exist_ok=False);support=out/'support';shutil.copytree(a.support_root.resolve(),support,ignore=shutil.ignore_patterns('__pycache__'))
 sys.path.insert(0,str(support/'scripts/rust-integration'))
 spec=importlib.util.spec_from_file_location('_default_pair_fixture_support',support/'scripts/rust-integration/run.py');helper=importlib.util.module_from_spec(spec);spec.loader.exec_module(helper)
 from fixture_ports import write_port_overlay
 nonce=uuid.uuid4().hex[:10];database='rust_probe_backup_jobs_'+nonce;project='dispatcharr-scanner-children-'+nonce
 tools=out/'pair-tools';tools.mkdir()
 for name in ['run_pair.py','source_pair.py','periodic_worker_observer.py','ledger_io.py','predeclared-plan.json']:shutil.copy2(Path(__file__).with_name(name),tools/name)
 report={'passed':False,'scope':'Actual pinned Original scanner and real default-queue descendant consumption; no native execution',
   'project':project,'copied_database':database,'original_revision':BASE,'native_verified':False,
   'tools_sha256':{x.name:hashlib.sha256(x.read_bytes()).hexdigest()for x in tools.iterdir()},'performance_verified':False,'default_registry_enabled':False}
 status=1
 with helper.fixture_source(report,False,out)as temporary:
  source=Path(temporary)/'source';source.mkdir(mode=0o755);raw=subprocess.check_output(['git','archive',BASE],cwd=root)
  assert hashlib.sha256(raw).hexdigest()==ARCHIVE;subprocess.run(['tar','-x','--same-permissions','--no-same-owner','-C',str(source)],input=raw,check=True);(source/'media').mkdir(exist_ok=True)
  environment=dict(os.environ,BASELINE_SOURCE_DIR=str(source),BASELINE_EVIDENCE_DIR=str(out),DISPATCHARR_ENV='aio')
  overlay=out/'pair.yml';overlay.write_text(json.dumps({'services':{'baseline':{'volumes':[str(tools)+':/pair-tools:ro',str(support/'scripts/rust-integration')+':/backup-tools:ro']},'origin':{'entrypoint':['/dispatcharrpy/bin/python','-c','import time; time.sleep(3600)']}},'networks':{'default':{'internal':True}}}))
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
   value=subprocess.run(argv,env=environment,capture_output=True,text=True,timeout=plan['host_controller_timeout_seconds']);(out/'source-controller-private.log').write_text(value.stdout+value.stderr);value.check_returncode()
   actual=json.loads((out/'source-url-hash-private.json').read_text());assert [v['label']for v in actual['cases']]==[v['label']for v in plan['actual_worker_steps']];assert actual['native_verified']is False;report.update(passed=actual['passed'],checks=len(actual['checks']),actual_original_worker_verified=actual['passed'],cases=len(actual['cases']));status=0 if report['passed']else 1

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
