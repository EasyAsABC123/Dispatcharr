#!/usr/bin/env python3
"""Finite owned M3U failure paired/physical/lease qualification, fixture only.
Original Dispatcharr authors retain application/algorithm credit and licensing.
Explicit immutable binary/support inputs; no mutable Rust capture or app hooks.
"""
import argparse,hashlib,importlib.util,json,os,re,shutil,subprocess,sys,time,uuid
from pathlib import Path
BASE='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
ARCHIVE='f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff'
def exact_absent_cid(returncode,stdout,stderr,cid):
 # Docker's actual owned-daemon spelling is lowercase; match ONLY this exact
 # missing-object diagnostic for the actual canonical CID, never another error.
 if type(returncode)is not int or returncode!=1 or type(stdout)is not bytes or type(stderr)is not bytes or type(cid)is not str or not re.fullmatch('[0-9a-f]{64}',cid):return False
 try:message=stderr.decode('utf-8',errors='strict').strip()
 except UnicodeDecodeError:return False
 return stdout.strip()in(b'',b'[]')and message.casefold()==('error: no such object: '+cid).casefold()
def accept_capture(returncode,actual):
 expected=['m3u-inactive', 'm3u-empty-active', 'm3u-one-stream-active', 'm3u-missing-local-file', 'm3u-groups-lock-busy', 'm3u-single-lock-busy', 'm3u-group-SQL-P0001', 'm3u-stream-SQL-P0001']
 if type(returncode)is not int or returncode!=0 or actual.get('passed')is not True or actual.get('native_verified')is not True:raise ValueError('Actual native capture/exit required')
 checks=actual.get('checks');cases=actual.get('cases')
 if not isinstance(checks,list)or not checks or any(not isinstance(v,dict)or not isinstance(v.get('name'),str)or v.get('passed')is not True for v in checks)or len({v['name']for v in checks})!=len(checks):raise ValueError('Incomplete/corrupt checks')
 if not isinstance(cases,list)or [v.get('name')for v in cases]!=expected:raise ValueError('Exact cases required')
 names=json.loads(Path(__file__).with_name('qa-policy.json').read_text())['paired_expected_check_names']
 if set(v['name']for v in checks)!=set(names)or len(checks)!=len(names):raise ValueError('ExactPredeclaredChecksNoDeletion')
 for case in cases:
  children=1 if case['name']=='m3u-inactive'else 2
  if case.get('source_fallback')is not False or len(case.get('native_ids',[]))!=children or len(case.get('Source_comparison',[]))!=children:raise ValueError('BothActualNativeChildrenNoFallback')
  if set(case.get('command_windows',{}))!=set(case['native_ids']) or 'case_window'not in case:raise ValueError('ActualNativeUTCWindows')
  if case.get('source_command_intervals','missing')is not None:raise ValueError('DoNotInventSourceCommandIntervals')
  if not case.get('native_pg',{}).get('physical_close_verified')or case['native_pg']['errors']:raise ValueError('ActualPGRelaySettlement')
 return actual
def accept_physical(returncode,actual):
 expected=['parent','claim','residual-confirm','group-error-ready','events','parsed-cache-event','result','result-lost','result-denied','old-SUCCESS']
 if returncode!=0 or actual.get('passed')is not True or actual.get('native_verified')is not True:raise ValueError('ActualPhysicalExitAndChecks')
 if [v.get('name')for v in actual.get('cases',[])]!=expected:raise ValueError('ExactPhysicalOrder')
 checks=actual.get('checks',[])
 if not checks or any(v.get('passed')is not True for v in checks)or len({v['name']for v in checks})!=len(checks):raise ValueError('ExactUniquePhysicalChecks')
 names=json.loads(Path(__file__).with_name('qa-policy.json').read_text())['physical_expected_check_names']
 if set(v['name']for v in checks)!=set(names)or len(checks)!=len(names):raise ValueError('ExactPhysicalChecksNoDeletion')
 if actual.get('cleanup_errors')!=[]:raise ValueError('PhysicalCleanupUnknown')
 if len(actual.get('Source_parent_exit_receipts',[]))!=10:raise ValueError('EveryActualSourceParentWait')
 if any(v.get('process_settled')is not True or v.get('actual_exit')!=0 for v in actual.get('Source_parent_exit_receipts',[])):raise ValueError('ActualSourceParentSettlement')
 for case in actual['cases']:
  if case['peer_close']['process_settled']is not True or case['peer_close']['actual_exit']!=0:raise ValueError('ActualNativePeerSettlement')
  for kind in ['pg','redis']:
   if kind in case and(case[kind]['physical_close_verified']is not True or case[kind]['errors']):raise ValueError('ActualProxySettlement')
 return actual
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--mode',choices=['paired','physical','lease-controls'],default='paired');p.add_argument('--coord-binary',type=Path);p.add_argument('--repo',type=Path,required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--support-root',type=Path,required=True);p.add_argument('--native-binary',type=Path,required=True);p.add_argument('--reference',type=Path,required=True);p.add_argument('--execution-inputs',type=Path,required=True);p.add_argument('--resources',action='store_true');p.add_argument('--force-resource-exit',type=int,default=0);a=p.parse_args()
 if a.resources or a.force_resource_exit:p.error('No ResourceExecutionInThisFunctionalClosure')
 if a.force_resource_exit and (not a.resources or not 1<=a.force_resource_exit<=125):p.error('Owned resources fault requires --resources and exit1..125')

 # Fail host-tooling and exact ELF/source/QA binding before any output/fixture.
 execution=json.loads(a.execution_inputs.read_text())
 expected_tools={'run_pair.py','source_pair.py','periodic_worker_observer.py','support-inputs.json','acceptance.py','pg_preflight_gate.py','reference.json','qa-policy.json','ledger_io.py','owned_pg_gate_proxy.py','compare_full.py','owned_redis_reply_proxy.py','fixture_runtime.py','source_physical.py'}
 if set(execution.get('tools_sha256',{}))!=expected_tools:raise ValueError('ExactCompleteQAClosureRequired')
 policy_path=Path(__file__).with_name('qa-policy.json');policy=json.loads(policy_path.read_text())
 if execution.get('authorized')is not True or execution.get('source_map_sha256')!=policy['source_map_sha256']:raise ValueError('ReviewedExactFrozen3ExecutionReceipt')
 if not re.fullmatch('sha256:[0-9a-f]{64}',execution.get('Original_image_id','')):raise ValueError('ActualReviewedOriginalImageID')
 if not isinstance(execution.get('Docker_host'),str)or not execution['Docker_host'].startswith('unix:///'):raise ValueError('ExplicitOwnedDockerSocket')
 if a.native_binary.read_bytes()[:4]!=b'\x7fELF':raise ValueError('ActualTestELFRequired')

 if a.mode=='lease-controls'and(not a.coord_binary or execution.get('coord_binary_sha256')!=hashlib.sha256(a.coord_binary.read_bytes()).hexdigest()):raise ValueError('ExactOutsideCoordinationELF')
 if execution.get('native_binary_sha256')!=hashlib.sha256(a.native_binary.read_bytes()).hexdigest():raise ValueError('ExactOutsideELF')
 if execution.get('reference_sha256')!=policy['reference_sha256']or hashlib.sha256(a.reference.read_bytes()).hexdigest()!=policy['reference_sha256']:raise ValueError('ExactPublishedSource4')
 for name,digest in execution['tools_sha256'].items():
  if hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest()!=digest:raise ValueError('ChangedSealedQAInput:'+name)
 docker=shutil.which('docker')
 if not docker or os.environ.get('DOCKER_HOST')!=execution.get('Docker_host')or hashlib.sha256(Path(docker).read_bytes()).hexdigest()!=execution.get('docker_executable_sha256'):raise ValueError('ExplicitReviewedToolingEnvironment')
 expected_support=json.loads(Path(__file__).with_name('support-inputs.json').read_text())
 for name,expected in expected_support.items():
  if hashlib.sha256((a.support_root.resolve()/name).read_bytes()).hexdigest()!=expected:raise ValueError('Changed public fixture support: '+name)
 root=a.repo.resolve();out=a.output.resolve();out.mkdir(parents=True,exist_ok=False);support=out/'support';shutil.copytree(a.support_root.resolve(),support,ignore=shutil.ignore_patterns('__pycache__'))
 sys.path.insert(0,str(support/'scripts/rust-integration'))
 spec=importlib.util.spec_from_file_location('_default_pair_fixture_support',support/'scripts/rust-integration/run.py');helper=importlib.util.module_from_spec(spec);spec.loader.exec_module(helper)
 from fixture_ports import write_port_overlay
 nonce=uuid.uuid4().hex[:10];database='rust_probe_backup_jobs_'+nonce;project='dispatcharr-scanner-children-'+nonce
 tools=out/'pair-tools';tools.mkdir()
 for name in execution['tools_sha256']:shutil.copy2(Path(__file__).with_name(name),tools/name)
 shutil.copy2(a.execution_inputs,tools/'execution-inputs.json')
 shutil.copy2(a.reference,tools/'reference.json')
 report={'mode':a.mode,'native_binary_sha256':hashlib.sha256(a.native_binary.read_bytes()).hexdigest(),'passed':False,'scope':('Actual coordination residual lease/Drop controls only; no Original task parity'if a.mode=='lease-controls'else'Actual Original scanner producer/native worker/Original decoder; finite paired or physical mode; no Source fallback/Main/performance'),
   'project':project,'copied_database':database,'original_revision':BASE,'native_verified':False,'execution_inputs_sha256':hashlib.sha256(a.execution_inputs.read_bytes()).hexdigest(),'source_map_sha256':policy['source_map_sha256'],
   'tools_sha256':{x.name:hashlib.sha256(x.read_bytes()).hexdigest()for x in tools.iterdir()},'performance_verified':False,'default_registry_enabled':False}
 status=1
 with helper.fixture_source(report,False,out)as temporary:
  source=Path(temporary)/'source';source.mkdir(mode=0o755);raw=subprocess.check_output(['git','archive',BASE],cwd=root)
  assert hashlib.sha256(raw).hexdigest()==ARCHIVE;subprocess.run(['tar','-x','--same-permissions','--no-same-owner','-C',str(source)],input=raw,check=True);(source/'media').mkdir(exist_ok=True)
  environment=dict(os.environ,BASELINE_SOURCE_DIR=str(source),BASELINE_EVIDENCE_DIR=str(out),DISPATCHARR_ENV='aio')
  overlay=out/'pair.yml';overlay.write_text(json.dumps({'services':{'baseline':{'volumes':[str(tools)+':/pair-tools:ro',str(a.native_binary.resolve())+':/native-m3u-peer:ro',str(support/'scripts/rust-integration')+':/backup-tools:ro']},'origin':{'entrypoint':['/dispatcharrpy/bin/python','-c','import time; time.sleep(3600)']}},'networks':{'default':{'internal':True}}}))
  if a.mode=='lease-controls':
   data=json.loads(overlay.read_text());data['services']['baseline']['volumes'].append(str(a.coord_binary.resolve())+':/native-coordination-peer:ro');overlay.write_text(json.dumps(data))
  ports=out/'ports.yml';write_port_overlay(ports,{'baseline':[5656],'origin':[8080]})
  compose=['docker','compose','-p',project,'-f',str(support/'docker/compose.rust-playback.yml'),'-f',str(overlay),'-f',str(ports)]
  def command(argv,body=None,timeout=180):return subprocess.check_output(argv,env=environment,input=body,text=True,stderr=subprocess.PIPE,timeout=timeout)
  def manifest(name,action):
   raw=command(compose+['exec','-T','-e','POSTGRES_DB='+name,'-e','BACKUP_DISPOSABLE_FIXTURE=playback-fixture-only','baseline','/dispatcharrpy/bin/python','/backup-tools/backup_fixture.py'],json.dumps({'action':action}));return json.loads(raw.splitlines()[-1])
  before=None
  try:
   command(compose+['up','-d','baseline']);end=time.monotonic()+180
   cid=command(compose+['ps','-q','baseline']).strip()
   if len(cid)!=64:raise ValueError('ActualOwnedBaselineCID')
   inspected=json.loads(command(['docker','inspect',cid]))[0]
   if inspected['Image']!=execution['Original_image_id']or inspected['Config']['Image']!='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f':raise ValueError('ActualPinnedOriginalImage')
   report['Original_container']={'id':cid,'image_id':inspected['Image'],'image_reference':inspected['Config']['Image']}

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
     '-e','CELERY_BROKER_URL=redis://:playback-fixture-only@localhost:6379/8','-e','CELERY_RESULT_BACKEND=redis://:playback-fixture-only@localhost:6379/9','baseline','/dispatcharrpy/bin/python','/pair-tools/'+('source_physical.py'if a.mode=='physical'else'source_pair.py') ]
   if a.mode=='lease-controls':
    names=['failed_m3u_groups_retains_key_without_drop_delete_and_preserves_foreign_owner','cancelled_m3u_residual_confirmation_keeps_caller_and_lease_until_actual_reply','non_m3u_groups_cannot_gain_no_delete_disposition'];results=[]
    for name in names:
     command_argv=compose+['exec','-T','-e','JOB_DISPOSABLE_FIXTURE=playback-fixture-only','-e','REDIS_TEST_URL=redis://:playback-fixture-only@127.0.0.1:6379/0','baseline','/native-coordination-peer','--exact',name,'--ignored','--nocapture','--test-threads=1']
     began=time.time();value=subprocess.run(command_argv,env=environment,capture_output=True,text=True,timeout=60);(out/(name+'.log')).write_text(value.stdout+value.stderr)
     results.append({'name':name,'argv':command_argv,'actual_exit':value.returncode,'began_epoch':began,'finished_epoch':time.time(),'process_settled':True});report['lease_controls']=list(results);value.check_returncode()
    report.update(passed=True,checks=3,cases=3,lease_controls=results,native_verified=True,scope='Actual coordination residual lease/Drop/foreign/cancellation controls only; no Original handler or task metadata parity');status=0
   else:
    value=subprocess.run(argv,env=environment,capture_output=True,text=True,timeout=540);(out/'source-controller-private.log').write_text(value.stdout+value.stderr);report['actual_controller_exit']=value.returncode;value.check_returncode()
    raw=json.loads((out/'source-logo-scanner-private.json').read_text());actual=accept_physical(value.returncode,raw)if a.mode=='physical'else accept_capture(value.returncode,raw)
    report.update(passed=actual['passed'],checks=len(actual['checks']),actual_original_producer_and_decoder_verified=actual['passed'],native_verified=actual['passed'],cases=len(actual['cases']));status=0

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
   except (OSError,subprocess.SubprocessError):report['cleanup_error']=True;report['passed']=False;status=1
   queries={};inventories={}
   for kind,args in [('containers',['ps','-aq']),('networks',['network','ls','-q']),('volumes',['volume','ls','-q'])]:
    query=['docker',*args,'--filter','label=com.docker.compose.project='+project]
    try:
     began=time.time();answer=subprocess.run(query,env=environment,capture_output=True,timeout=30)
     queries[kind]={'argv':query,'exit':answer.returncode,'stdout_hex':answer.stdout.hex(),'stdout_sha256':hashlib.sha256(answer.stdout).hexdigest(),'stderr_hex':answer.stderr.hex(),'began_epoch':began,'finished_epoch':time.time()}
     if answer.returncode:raise RuntimeError('IndependentInventoryFailure')
     inventories[kind]=answer.stdout.decode('utf-8',errors='strict').splitlines()
    except (OSError,ValueError,RuntimeError,subprocess.SubprocessError)as error:
     queries.setdefault(kind,{'argv':query});queries[kind]['error_type']=type(error).__name__;inventories[kind]=None
   cid_receipt=None
   if 'Original_container'in report:
    query=['docker','inspect',report['Original_container']['id']]
    try:
     value=subprocess.run(query,env=environment,capture_output=True,timeout=30)
     cid_receipt={'argv':query,'exit':value.returncode,'stdout_hex':value.stdout.hex(),'stderr_hex':value.stderr.hex(),'absent':exact_absent_cid(value.returncode,value.stdout,value.stderr,report['Original_container']['id'])}
    except (OSError,subprocess.SubprocessError)as error:cid_receipt={'argv':query,'absent':False,'error_type':type(error).__name__}
   cleanup={'exact_CID':cid_receipt,'queries':queries,'inventories':inventories,'all_owned_empty':all(v==[]for v in inventories.values())}
   (out/'independent-cleanup.json').write_text(json.dumps(cleanup,indent=2)+'\n');report['independent_cleanup']=cleanup
   if cid_receipt is not None and not cid_receipt['absent']:cleanup['all_owned_empty']=False
   if not cleanup['all_owned_empty']:report['cleanup_error']=True;report['passed']=False;status=1

   (out/'integration.json').write_text(json.dumps(report,indent=2)+'\n')
 print(json.dumps({'passed':report['passed'],'output':str(out)}));return status
if __name__=='__main__':raise SystemExit(main())
