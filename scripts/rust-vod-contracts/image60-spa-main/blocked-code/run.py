#!/usr/bin/env python3
"""Frozen blocked MainSPA executor. Only Root-reviewed flag-copy and fresh cue dispatch."""
import argparse,datetime,hashlib,json,os,re,shutil,stat,threading,time,uuid
from pathlib import Path
from bounded_process import Collected,CollectionFailure
from bounded_stop import collect as bounded_stop
from candidate_guard import validate as candidate_validate
from once_guard import admit
from identity import kernel
from execution_validation import validate as qualify
from bootstrap_context import validate as builtin_validate
import tooling_guard as T
HARD_BLOCKED=True
HERE=Path(__file__).resolve().parent
ROOT=Path('/home/justin/github/Dispatcharr')
ORIGINAL='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
ARCHIVE=ROOT/'artifacts/rust/m5-core-settings-source-executor-2/inputs/original-source.tar'
ARCHIVE_SHA='f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff'
GUEST=['origin.py','native_capture.py','native_control.py','audit_only.py','absolute_http.py','reader.py']
JSON_INPUTS={'plan.json','candidate-binding.json','expected-runtime.json','schema62.json','source-reference.json','tooling-inputs.json','cue-template.json','bootstrap-authority.json'}
def need(v,label):
 if not v:raise ValueError(label)
def digest(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def save(p,v):Path(p).write_text(json.dumps(v,indent=2,allow_nan=False)+'\n')
def start(pid):
 try:return Path('/proc',str(pid),'stat').read_text().rsplit(')',1)[1].split()[19]
 except FileNotFoundError:return None
def inputs():return {p.name for p in HERE.iterdir()if p.is_file()and(p.suffix in ['.py','.sh']or p.name in JSON_INPUTS or p.name=='PLAN.md')}
def check_seal(path):
 v=json.loads(Path(path).read_text());need(v['state']=='BLOCKED_IMAGE60_MAIN_SPA_ROOT_REVIEW'and set(v['files'])==inputs(),'CompleteBlockedInputClosure')
 for name,want in v['files'].items():
  file=HERE/name;need(file.is_file()and not file.is_symlink(),'RegularInput:'+name);raw=file.read_bytes()
  if name=='run.py':
   need(sum(line in [b'HARD_BLOCKED=True',b'HARD_BLOCKED=False']for line in raw.splitlines())==1,'SingleExplicitLaunchFlag');raw=raw.replace(b'\nHARD_BLOCKED=False\n',b'\nHARD_BLOCKED=True\n')
  need(hashlib.sha256(raw).hexdigest()==want,'ExactInput:'+name)
 for name,want in v['external_inputs'].items():need(digest(ROOT/name)==want,'ExactExternalInput:'+name)
 return v
def settled_output(job,timeout,allowed,stdout_path):
 classified=None
 try:job.wait(timeout)
 except CollectionFailure as error:
  if job.process.poll()not in allowed or job.invalid is not None or not job.physically_settled or any(t.is_alive()for t in job.threads):raise
  classified=type(error).__name__+':'+str(error)
 need(job.process.returncode in allowed and job.physically_settled and not any(t.is_alive()for t in job.threads),'ExplicitAllowedSettledExitOnly')
 return Path(stdout_path).read_bytes(),classified

def controlled_environment(meta):
 prefixes=('COMPOSE_','POSTGRES_','REDIS_','DJANGO_','CELERY_','DATABASE_','BASELINE_','BENCHMARK_','FIXTURE_','ENABLE_VOD','SOURCE_CAPTURE_','SPA_','GIT_','DOCKER_','RUST_','DISPATCHARR_')
 env={k:v for k,v in os.environ.items()if not k.startswith(prefixes)};env.update(meta['selected_environment']);T.assert_inputs(env,meta);return env
def export_guest(out):
 guest=out/'guest-tools';guest.mkdir(mode=0o755);hashes={}
 for name in GUEST:
  raw=(HERE/name).read_bytes();dest=guest/name;dest.write_bytes(raw);dest.chmod(0o444);hashes[name]=hashlib.sha256(raw).hexdigest()
 save(guest/'guest-inputs.json',{'files':hashes});(guest/'guest-inputs.json').chmod(0o444);guest.chmod(0o555)
 need(stat.S_IMODE(guest.stat().st_mode)==0o555 and all(stat.S_IMODE(p.stat().st_mode)==0o444 for p in guest.iterdir()),'MinimalGuestReadableModes')
 save(out/'guest-export.json',{'directory_mode':'0555','file_mode':'0444','files':hashes,'minimal_transitive_Guest_files':GUEST,'private_controller_directory_mode_changed':False});return guest

def execute(out,cue,binding):
 began=time.monotonic();work_deadline=began+510;cleanup_deadline=None;meta=json.loads((HERE/'tooling-inputs.json').read_text());env=controlled_environment(meta);docker=meta['docker']['path'];nonce=cue['nonce'];project='dispatcharr-spa-main-'+nonce[:10];database='rust_spa_'+nonce[:16];need(re.fullmatch('rust_spa_[a-f0-9]{16}',database)is not None,'OwnedDB')
 source=out/'original-source';source.mkdir();guest=export_guest(out);origin_evidence=out/'origin-evidence';origin_evidence.mkdir(mode=0o777);origin_evidence.chmod(0o777)
 report={'passed':False,'Native_only':True,'Source_requests':0,'Source_replay':0,'resource_sampling':False,'full_M4_acceptance':False,'no_adaptive_retry':True,'candidate_image':binding['image_id'],'candidate_Git':binding['Git'],'candidate_tree':binding['tree'],'project':project,'owned_database':database,'stage':'preflight','actual_business_request_invocations':0,'commands':[]};jobs=[];serial=0;claimed=False;roles={};default_before=None;first=None;stopping=False;stopped=threading.Event();stop_receipt={}
 composefile=out/'compose.json';base=[docker,'compose','-p',project,'-f',str(composefile)]
 def remaining():return max(.01,(cleanup_deadline if cleanup_deadline is not None else work_deadline)-time.monotonic())
 def stop_owned():
  nonlocal stopping
  if not claimed:return
  if stopping:need(stopped.wait(35)and stop_receipt.get('acknowledged')is True,'SameOwnedStopAcknowledged');return
  stopping=True
  try:bounded_stop(base+['stop','--timeout','10'],env,out,stop_receipt,limit=1048576,timeout=min(15,remaining()),save=save)
  finally:stopped.set()
 def command(argv,seconds=30,allowed=(0,)):
  nonlocal serial
  need(remaining()>.01,'HostBudgetRemaining');serial+=1;tag='command-'+str(serial).zfill(3);t0=time.time();job=Collected(argv,env,1048576,stop_owned,stdout_path=out/(tag+'.stdout'),stderr_path=out/(tag+'.stderr'));jobs.append(job);identity=start(job.process.pid);need(identity is not None,'ActualCLIStartIdentity');failure=None
  try:
   stdout,failure=settled_output(job,min(seconds,remaining()),allowed,out/(tag+'.stdout'))
  finally:
   row={'argv':argv,'cwd':os.getcwd(),'PID':job.process.pid,'starttime':identity,'began_epoch':t0,'completed_epoch':time.time(),'actual_exit':job.process.poll(),'allowed_exit_codes':list(allowed),'classified_nonzero':failure,'collection_invalid':job.invalid,'physically_settled':job.physically_settled,'pipe_threads_settled':not any(t.is_alive()for t in job.threads),'stdout_sha256':digest(out/(tag+'.stdout')),'stderr_sha256':digest(out/(tag+'.stderr'))};save(out/(tag+'.json'),row);report['commands'].append(row);save(out/'execution-private.json',report)
  return stdout
 def run(*argv,seconds=30,allowed=(0,)):return command(base+list(argv),seconds,allowed)
 def inspect(role):
  ids=run('ps','--all','-q',role).decode().splitlines();need(len(ids)==1,'OneExactRole:'+role);cid=ids[0];need(re.fullmatch('[0-9a-f]{64}',cid)is not None,'FullCID:'+role);value=json.loads(command([docker,'inspect',cid]))[0];need(value['Config']['Labels']['com.docker.compose.project']==project and value['Config']['Labels']['com.docker.compose.service']==role,'OwnedRoleLabel:'+role);roles[role]={'CID':cid,'initial_inspect':value}
  if value['State']['Running']:
   pid=value['State']['Pid'];identity=start(pid);need(identity is not None,'ActualHostContainerPID:'+role);roles[role].update(host_PID=pid,host_starttime=identity)
  save(out/'owned-role-bindings-private.json',roles);return value
 def management(label,db):
  need(label in ['default-before','default-after','owned-before','owned-after'],'FixedAuditLabel');run('exec','-T','-e','SPA_FIXTURE_AUTHORITY=owned-native-spa-get-only','-e','POSTGRES_DB='+db,'-e','SPA_AUDIT_LABEL='+label,'baseline','/dispatcharrpy/bin/python','manage.py','shell','-c',"import runpy;runpy.run_path('/spa-tools/audit_only.py',run_name='__main__')",seconds=40);return json.loads((out/(label+'.json')).read_text())
 def inventories():
  data={}
  for kind,fmt in [('container','{{.ID}}'),('network','{{.ID}}'),('volume','{{.Name}}')]:
   argv=[docker,kind,'ls']+(['--all','--no-trunc']if kind=='container'else[])+['--filter','label=com.docker.compose.project='+project,'--format',fmt];data[kind]=command(argv).decode().splitlines()
  return data
 def cleanup(name,fn):
  try:value=fn();report.setdefault('cleanup_steps',[]).append({'step':name,'settled':True,'value':value})
  except BaseException as error:report.setdefault('cleanup_steps',[]).append({'step':name,'settled':False,'type':type(error).__name__,'message':str(error)[:2048]})
  save(out/'execution-private.json',report)
 try:
  def metadata_runner(argv,**kwargs):
   from types import SimpleNamespace
   raw=command(argv,seconds=5);return SimpleNamespace(returncode=0,stdout=raw.decode(),stderr=(out/('command-'+str(serial).zfill(3)+'.stderr')).read_text())
  report['tooling_preflight']=T.preflight(env,meta,metadata_runner);need(digest(ARCHIVE)==ARCHIVE_SHA,'ExactOriginalArchive');command(['tar','-xf',str(ARCHIVE),'--no-same-owner','-C',str(source)],seconds=30);(source/'media').mkdir(exist_ok=True)
  common={'DISPATCHARR_ENV':'aio','DJANGO_SECRET_KEY':'playback-fixture-only','POSTGRES_DB':'dispatcharr','POSTGRES_USER':'dispatch','POSTGRES_PASSWORD':'playback-fixture-only','POSTGRES_HOST':'/var/run/postgresql','POSTGRES_PORT':'5432','POSTGRES_DIR':'/data/db','REDIS_HOST':'127.0.0.1','REDIS_PORT':'6379','REDIS_PASSWORD':'playback-fixture-only','REDISCLI_AUTH':'playback-fixture-only','PYTHONDONTWRITEBYTECODE':'1','PYTHONPATH':'/spa-tools:/app','PUID':'1000','PGID':'1000','LANG':'C.UTF-8','LC_ALL':'C.UTF-8'}
  spec={'services':{'baseline':{'image':ORIGINAL,'pull_policy':'never','platform':'linux/amd64','working_dir':'/app','entrypoint':['bash','/spa-host/bootstrap.sh'],'environment':common,'volumes':[str(source)+':/app:ro',str(HERE)+':/spa-host:ro',str(HERE)+':/spa-inputs:ro',str(guest)+':/spa-tools:ro',str(out)+':/evidence'],'tmpfs':['/data:mode=1777,size=2147483648','/tmp:mode=1777,size=268435456'],'mem_limit':'1g','pids_limit':256},'origin':{'image':ORIGINAL,'pull_policy':'never','user':'65532:65532','working_dir':'/tmp','entrypoint':['/dispatcharrpy/bin/python','/spa-tools/origin.py'],'environment':{'CAPTURE_NONCE':nonce,'SPA_FIXTURE_AUTHORITY':'owned-native-spa-get-only','PYTHONDONTWRITEBYTECODE':'1','PYTHONPATH':'/spa-tools'},'read_only':True,'cap_drop':['ALL'],'security_opt':['no-new-privileges:true'],'volumes':[str(guest)+':/spa-tools:ro',str(origin_evidence)+':/origin-evidence'],'tmpfs':['/tmp:mode=1777,size=4194304'],'mem_limit':'64m','pids_limit':32},'data-init':{'image':ORIGINAL,'pull_policy':'never','user':'0:0','network_mode':'none','entrypoint':['sh','-c','mkdir -p /data/logos && chown 65532:65532 /data /data/logos'],'read_only':True,'cap_drop':['ALL'],'cap_add':['CHOWN'],'volumes':['native-data:/data']},'candidate':{'image':binding['image_id'],'pull_policy':'never','environment':{'DATABASE_URL':'postgresql://dispatch:playback-fixture-only@baseline:5432/'+database,'DJANGO_SECRET_KEY':'playback-fixture-only','REDIS_URL':'redis://:playback-fixture-only@baseline:6379/1','RUST_BIND':'0.0.0.0:9192','TMPDIR':'/data','RUST_DATABASE_POOL_SIZE':'8','TOKIO_WORKER_THREADS':'1','DISPATCHARR_TRUSTED_PROXIES':'none','RUST_VOD_REDIS_RECOVERY_ENABLED':'0'},'read_only':True,'cap_drop':['ALL'],'security_opt':['no-new-privileges:true'],'volumes':['native-data:/data'],'depends_on':{'baseline':{'condition':'service_started'},'data-init':{'condition':'service_completed_successfully'}},'mem_limit':'1g','pids_limit':256}},'networks':{'default':{'internal':True}},'volumes':{'native-data':{}}};save(composefile,spec)
  need(not any(inventories().values()),'FreshOwnedProjectAbsent');claimed=True;report['stage']='bootstrap';run('up','-d','--no-build','--pull','never','baseline','origin',seconds=30);inspect('baseline');origin_info=inspect('origin');need(origin_info['Config']['User']=='65532:65532'and origin_info['HostConfig']['ReadonlyRootfs']is True,'NormalOriginIsolation')
  end=time.monotonic()+180
  while not(out/'bootstrap-ready').exists():need(time.monotonic()<end and remaining()>.01,'Bootstrap180');time.sleep(.1)
  need((origin_evidence/'origin-ready.json').exists()and(origin_evidence/'origin-guest-readable.json').exists(),'ActualOriginReadyAndGuestReadability');ready=json.loads((origin_evidence/'origin-ready.json').read_text());guest_readable=json.loads((origin_evidence/'origin-guest-readable.json').read_text());need(guest_readable['UID']==65532 and guest_readable['all_exact_readable']is True and guest_readable['files']==GUEST,'ActualGuestUIDReadableClosure');report['origin_identity']=ready
  default_before=management('default-before','dispatcharr');report['default_before']=default_before;context=json.loads((out/'default-before-builtin-context.json').read_text());report['migration_builtin_context']=context;report['migration_builtin_qualification']=builtin_validate(context,default_before)
  absent=run('exec','-T','baseline','psql','-XAtq','-h','/var/run/postgresql','-U','dispatch','-d','postgres','-v','ON_ERROR_STOP=1','-c',"SELECT count(*) FROM pg_database WHERE datname='"+database+"'").decode().strip();need(absent=='0','OwnedDBInitiallyAbsent');run('exec','-T','baseline','psql','-h','/var/run/postgresql','-U','dispatch','-d','postgres','-v','ON_ERROR_STOP=1','-c','CREATE DATABASE '+database+' TEMPLATE dispatcharr');report['owned_clone_created_without_rows_seeded']=True
  run('up','-d','--no-build','--pull','never','candidate',seconds=30);candidate=inspect('candidate');inspect('data-init');need(candidate['Image']==binding['image_id']and candidate['Config']['User']=='65532:65532'and candidate['HostConfig']['ReadonlyRootfs']is True and 'ALL'in candidate['HostConfig']['CapDrop'],'NormalPackagedMainImageUIDReadOnly');need(candidate['Config']['Entrypoint']==['/usr/local/bin/dispatcharr-rs'],'NormalMainEntrypoint');need(candidate['Config']['Env'].count('RUST_FRONTEND_DIR=/opt/dispatcharr-frontend')==1 and not any(m['Destination'].startswith('/opt/dispatcharr-frontend')for m in candidate['Mounts']),'NormalInstalledFrontend');cid=roles['candidate']['CID'];chunks=[command([docker,'exec',cid,'cat',path])for path in ['/proc/sys/kernel/hostname','/proc/1/stat','/proc/1/status','/proc/1/cgroup']];w=kernel(*chunks,1,native=True);report['Main_identity']={'PID':w['pid'],'UID':w['uid'],'comm':w['comm'],'raw_witness':w['raw_witness'],'starttime':w['starttime']};index=command([docker,'exec',cid,'cat','/opt/dispatcharr-frontend/index.html']);ref=json.loads((HERE/'source-reference.json').read_text());need(index==bytes.fromhex(ref['case']['body_hex']),'ActualInstalledIndexEqualsSource858');report['installed_index_sha256']=hashlib.sha256(index).hexdigest()
  end=time.monotonic()+120;controls=[]
  while True:
   need(time.monotonic()<end and remaining()>.01,'MainReadiness120');raw=run('exec','-T','baseline','/dispatcharrpy/bin/python','/spa-tools/native_control.py','ready',seconds=5,allowed=(0,1));row=report['commands'][-1];controls.append({'command':serial,'actual_exit':row['actual_exit']})
   if row['actual_exit']==0:need(json.loads(raw)['mode']=='ready','ActualMainHealthControl');break
   time.sleep(.2)
  report['readiness_controls']=controls;phase_began=time.monotonic();report['stage']='single-native-business-request';report['owned_before']=management('owned-before',database)
  run('exec','-T','-e','SPA_FIXTURE_AUTHORITY=owned-native-spa-get-only','-e','CAPTURE_NONCE='+nonce,'baseline','/dispatcharrpy/bin/python','/spa-tools/native_capture.py',seconds=25)
  capture=json.loads((out/'native-capture-private.json').read_text());report.update(capture);report['owned_after']=management('owned-after',database);report['capture_phase_elapsed_seconds']=time.monotonic()-phase_began;need(report['capture_phase_elapsed_seconds']<=40,'CompleteCapturePhase40')
 except BaseException as error:first=error;report['first_failure']={'type':type(error).__name__,'message':str(error)[:4096]}
 finally:
  cleanup_began=time.monotonic();cleanup_deadline=min(began+600,cleanup_began+90);report['cleanup_started_epoch']=time.time();report['stage']='cleanup'
  if (out/'native-capture-private.json').exists():
   partial=json.loads((out/'native-capture-private.json').read_text());report.update(partial)
  def stop_role(role,timeout):
   if role not in roles:return {'performed':False,'role_identity_not_captured_or_observed_absent':True}
   run('stop','--timeout',str(timeout),role,seconds=timeout+5);v=json.loads(command([docker,'inspect',roles[role]['CID']]))[0];need(v['State']['Running']is False and v['State']['ExitCode']==0,'NormalExit0:'+role);roles[role]['terminal_state']=v['State'];save(out/'owned-role-bindings-private.json',roles);return {'performed':True,'actual_exit':v['State']['ExitCode']}
  def discover():
   if not claimed:return {'performed':False}
   absent=[]
   for role in ['baseline','origin','candidate','data-init']:
    if role in roles:continue
    ids=run('ps','--all','-q',role).decode().splitlines()
    if not ids:absent.append(role)
    else:inspect(role)
   return {'performed':True,'roles_observed_absent':absent}
  cleanup('discover_partial_role_ownership',discover)
  cleanup('stop_normal_Main',lambda:stop_role('candidate',20))
  def backend():
   if not report.get('owned_clone_created_without_rows_seeded'):return {'performed':False}
   raw=run('exec','-T','baseline','psql','-XAtq','-h','/var/run/postgresql','-U','dispatch','-d','postgres','-v','ON_ERROR_STOP=1','-c',"SELECT count(*) FROM pg_stat_activity WHERE datname='"+database+"'");need(raw.strip()==b'0','OwnedBackendAbsenceAfterMainStop');report['owned_backends_after_stop']=0;return {'performed':True,'owned_backend_count':0}
  cleanup('owned_backend_absence_Unix_observer',backend)
  def default_after():
   if default_before is None:return {'performed':False,'default_after':'unmeasured'}
   value=management('default-after','dispatcharr');report['default_after']=value;need(value==default_before,'FullDefault6259Unchanged');return {'performed':True,'equal':True}
  cleanup('default_full_after_audit',default_after);cleanup('stop_owned_origin',lambda:stop_role('origin',10))
  def terminal_origin():
   if 'origin'not in roles:return {'performed':False}
   value=json.loads((origin_evidence/'origin-terminal.json').read_text());report['origin_terminal']=value;need(value['shutdown_acknowledged']and value['socket_listener_closed']and value['handler_threads_joined'],'ActualOriginTerminal');return {'performed':True,'physical_terminal':True}
  cleanup('origin_listener_socket_handler_ack',terminal_origin)
  cleanup('stop_management_PG_Redis',lambda:stop_role('baseline',25))
  cleanup('compose_down_owned_volumes',lambda:run('down','--volumes','--remove-orphans','--timeout','10',seconds=30).decode())if claimed else cleanup('compose_down_owned_volumes',lambda:{'performed':False})
  def absence():
   if not claimed:return {'performed':False}
   data=inventories();need(not any(data.values()),'FreshProjectInventoriesEmpty');exact=[]
   for role,row in roles.items():
    raw=command([docker,'container','ls','--all','--no-trunc','--filter','id='+row['CID'],'--format','{{.ID}}']);need(not raw.strip(),'FreshExactCIDGone:'+role);exact.append({'role':role,'CID':row['CID'],'absent':True})
   report['fresh_project_cleanup']=data;report['fresh_exact_CID_cleanup']=exact;return {'performed':True,'all_project_and_exact_CIDs_absent':True}
  cleanup('fresh_project_and_exact_CID_absence',absence)
  def processes():
   rows=[]
   for cmd in report['commands']:
    current=start(cmd['PID']);need(cmd['physically_settled']is True and cmd['pipe_threads_settled']is True and(current is None or current!=cmd['starttime']),'EveryOriginalCLIIdentitySettled');rows.append({'PID':cmd['PID'],'starttime':cmd['starttime'],'current_starttime':current})
   for role,v in roles.items():
    if 'host_PID'in v:
     current=start(v['host_PID']);need(current is None or current!=v['host_starttime'],'OriginalContainerHostPIDGone:'+role);rows.append({'role':role,'PID':v['host_PID'],'starttime':v['host_starttime'],'current_starttime':current})
   need(all(job.process.poll()is not None and not any(t.is_alive()for t in job.threads)for job in jobs),'AllOwnedCLIWaitsPipesSettled');report['original_host_identity_absence']=rows;return {'identities':len(rows),'settled':True}
  cleanup('all_original_PID_start_wait_pipe_settlement',processes)
  report['cleanup_elapsed_seconds']=time.monotonic()-cleanup_began;report['host_elapsed_seconds']=time.monotonic()-began;report['cleanup_within90']=report['cleanup_elapsed_seconds']<=90;report['host_within600']=report['host_elapsed_seconds']<=600
  if first is None and all(row['settled']for row in report['cleanup_steps'])and report['cleanup_within90']and report['host_within600']:
   try:report['qualification']=qualify(report);report['passed']=True
   except BaseException as error:report['qualification_failure']={'type':type(error).__name__,'message':str(error)[:2048]}
  report['controller_exit']=0 if report['passed']else 1;report['stage']='physically_settled_or_literal_failure';save(out/'execution-private.json',report)
 return report['controller_exit']

def main():
 need(Path.cwd().resolve()==ROOT,'ExactRepositoryCwd');p=argparse.ArgumentParser(description=__doc__);p.add_argument('--seal',type=Path,default=HERE/'seal.json');p.add_argument('--execute-reviewed-seal');p.add_argument('--cue',type=Path);p.add_argument('--output',type=Path);a=p.parse_args();check_seal(a.seal);proof=candidate_validate()
 if a.execute_reviewed_seal is None:print(json.dumps({'seal_valid':True,'candidate':proof,'application_executed':False}));return 0
 need(not HARD_BLOCKED,'RootFlagOnlyActivationRequired');need(a.execute_reviewed_seal==digest(a.seal),'ExactReviewedSeal');binding=json.loads((HERE/'candidate-binding.json').read_text());meta=json.loads((HERE/'tooling-inputs.json').read_text());controlled_environment(meta);cue=admit(a.cue,a.seal,Path(__file__),a.output,binding);a.output.mkdir(mode=0o700,parents=False,exist_ok=False);shutil.copyfile(a.cue,a.output/'Root-cue-private.json');return execute(a.output,cue,binding)
if __name__=='__main__':raise SystemExit(main())
