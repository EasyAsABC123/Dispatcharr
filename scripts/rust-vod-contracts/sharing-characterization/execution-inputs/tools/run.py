"""BLOCKED full owned normal-max2 Source/Main pair; no resource execution."""
import argparse,hashlib,importlib.util,json,os,shutil,subprocess,time,uuid
from pathlib import Path
HERE=Path(__file__).resolve().parent
EXECUTION_BLOCKED=False
BASE='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
ARCHIVE='f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff'
SUPPORT='c91ab5ece7926f1b6b04a8069c6ad38c5b5d8d55'
def load(path,name):
 spec=importlib.util.spec_from_file_location(name,path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module
def save(path,value):Path(path).write_text(json.dumps(value,indent=2)+'\n')
def fixture_guard(path):
 record=json.loads(Path(path).read_text());proof=record.get('proof',{})
 if record.get('passed')is not True or record.get('owned_container_removed')is not True or record.get('Source_application_executed')is not False:raise ValueError('QualifiedControlOnlyFixtureRequired')
 if proof.get('changed_literal_origin_qualified')is not True:raise ValueError('ChangedOriginLiteralPreflightRequired')
 decoder=proof.get('decoder',{})
 if decoder.get('retained_bytes')!=1048576 or decoder.get('frames',0)<50 or decoder.get('passed')is not True or decoder.get('retained_sha256')!='b4a9992aa94b69eaaebf85025ec5b55f3ebf6360b143196b0d056fcb04c6b097':raise ValueError('ExactActualPrefixDecodeBeforeClaim')
 for name in['base_origin.py','range_http.py','ledger_origin.py','decoder.py','local_command.py','bounded_process.py','fixture_preflight.py']:
  if hashlib.sha256((HERE/name).read_bytes()).hexdigest()!=record['tools_sha256'].get(name):raise ValueError('FixturePreflightUsedToolModified '+name)
 return{'qualification_sha256':hashlib.sha256(Path(path).read_bytes()).hexdigest(),'decoder':decoder,'Source_application_executed':False,'used_tool_subset_exact':True}
def main():
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--repo',type=Path,required=True);parser.add_argument('--candidate-image',required=True);parser.add_argument('--build-manifest',type=Path,required=True);parser.add_argument('--source-archive',type=Path,required=True);parser.add_argument('--fixture-preflight',type=Path,required=True);parser.add_argument('--cue',type=Path,required=True);parser.add_argument('--output',type=Path,required=True);args=parser.parse_args()
 if EXECUTION_BLOCKED:raise RuntimeError('RootReviewFreshPairCueRequired')
 # Read-only input qualification BEFORE any once claim or application fixture.
 from normal_guard import validate,digest,process_start,write_once_claim
 qualified=fixture_guard(args.fixture_preflight);candidate=load(HERE/'candidate_inputs.py','normal_candidate').bound_candidate(args.build_manifest,args.candidate_image,HERE/'expected-runtime.json',args.source_archive);cue_path=args.cue.resolve();cue=json.loads(cue_path.read_text());validate(cue,HERE.parent)
 if cue.get('candidate_manifest_sha256')!=candidate['manifest_sha256']or cue.get('candidate_image_id')!=candidate['image_id']or cue.get('fixture_preflight_sha256')!=qualified['qualification_sha256']:raise ValueError('ExactReviewedInputs')
 repo=args.repo.resolve();out=args.output.resolve()
 if Path(cue['new_output']).is_absolute()or'..'in Path(cue['new_output']).parts or out!=(repo/cue['new_output']).resolve()or out.exists():raise ValueError('FreshExactNormalOutput')
 claim_path=cue_path.with_name(cue_path.stem+'.claimed.json');claim={'cue_sha256':digest(cue_path),'phase':'exclusive_claim_before_dispatch','pid':os.getpid(),'boot_id':cue['boot_id'],'selection_sha256':cue['selection_sha256'],'claimed_epoch':time.time(),'pid_starttime':process_start(os.getpid()),'output':str(out),'repo':str(repo)};validate(cue,HERE.parent)
 write_once_claim(claim_path,claim)
 args.claim=claim_path;return execute(args,repo,out,candidate,qualified)
def execute(args,repo,out,candidate,qualified):
 out.mkdir(parents=True,exist_ok=False);tools=out/'tools';shutil.copytree(HERE,tools,ignore=shutil.ignore_patterns('__pycache__'));save(out/'candidate.json',candidate);save(out/'fixture-preflight-binding.json',qualified);shutil.copyfile(args.cue,out/'normal-cue.json');shutil.copyfile(args.claim,out/'normal-claim.json')
 original=out/'original';original.mkdir();raw=subprocess.check_output(['git','-C',str(repo),'archive',BASE]);assert hashlib.sha256(raw).hexdigest()==ARCHIVE;subprocess.run(['tar','-x','--no-same-owner','-C',str(original)],input=raw,check=True);(original/'media').mkdir(exist_ok=True)
 support=out/'support';support.mkdir();raw=subprocess.check_output(['git','-C',str(repo),'archive',SUPPORT,'scripts','docker']);subprocess.run(['tar','-x','--no-same-owner','-C',str(support)],input=raw,check=True);save(out/'support.json',{'revision':SUPPORT,'archive_sha256':hashlib.sha256(raw).hexdigest()})
 bounded=load(tools/'bounded_process.py','normal_bounded');ports=load(support/'scripts/rust-integration/fixture_ports.py','normal_ports');project='dispatcharr-normal-admission-'+uuid.uuid4().hex[:10];nonce=uuid.uuid4().hex;claim=json.loads(args.claim.read_text());save(out/'normal-activation.json',{**claim,'nonce':nonce,'claim_sha256':hashlib.sha256(args.claim.read_bytes()).hexdigest()});overlay=out/'overlay.json';portfile=out/'ports.yml';ports.write_port_overlay(portfile,{'baseline':[5656],'origin':[8080],'candidate':[9192]});database='rust_pending_'+uuid.uuid4().hex[:10];template=database+'_template';source_db=database+'_source';native_db=database+'_native';env=dict(os.environ,BASELINE_SOURCE_DIR=str(original),BASELINE_EVIDENCE_DIR=str(out));jobs={};command_number=0;report={'project':project,'HTTP_pair_complete':False,'resource_sampling':False,'full_M4_acceptance':False,'backends':{},'cleanup_errors':[]}
 def write_overlay(mode):
  save(overlay,{'services':{'baseline':{'volumes':[str(tools)+':/normal-tools:ro',str(tools)+':/pending-tools:ro',str(HERE.parent)+':/normal-root:ro'],'environment':{'PENDING_MAIN_FIXTURE':'playback-fixture-only','PENDING_MAIN_NONCE':nonce,'NORMAL_FIXTURE_NONCE':nonce,'NORMAL_REVIEWED_EXECUTION':'one-normal-max2-pair'}},'origin':{'entrypoint':['/dispatcharrpy/bin/python','/normal-tools/launch_origin.py'],'volumes':[str(tools)+':/normal-tools:ro',str(out)+':/evidence'],'environment':{'PENDING_MAIN_FIXTURE':'playback-fixture-only','NORMAL_FIXTURE_NONCE':nonce,'NORMAL_BACKEND':mode}},'candidate':{'image':args.candidate_image,'environment':{'DATABASE_URL':'postgresql://dispatch:playback-fixture-only@baseline:5432/'+native_db,'REDIS_URL':'redis://:playback-fixture-only@baseline:6391/0','RUST_VOD_REDIS_RECOVERY_ENABLED':'1','RUST_MAX_VOD_SESSIONS':'4','RUST_MAX_VOD_REQUESTS':'4'}}}})
 write_overlay('source');compose=['docker','compose','-p',project,'-f',str(support/'docker/compose.rust-playback.yml'),'-f',str(support/'scripts/rust-vod-contracts/compose.yml'),'-f',str(support/'docker/compose.rust-candidate.yml'),'-f',str(overlay),'-f',str(portfile)]
 def stop_owned():subprocess.run(compose+['kill'],env=env,stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=15,check=True)
 def command(argv,timeout=180):
  nonlocal command_number
  command_number+=1;return bounded.Collected(argv,env,8388608,stop_owned,stderr_path=out/('command-'+str(command_number)+'.stderr')).wait(timeout)
 def run(*argv,timeout=180):return command(compose+list(argv),timeout)
 def pg(sql):return run('exec','-T','-e','PGPASSWORD=playback-fixture-only','baseline','psql','-h','127.0.0.1','-U','dispatch','-d','postgres','-v','ON_ERROR_STOP=1','-At','-c',sql)
 def helper(db,action):return json.loads(run('exec','-T','-e','POSTGRES_DB='+db,'baseline','/dispatcharrpy/bin/python','/normal-tools/db_fixture.py',action).splitlines()[-1])
 def ready(url):
  end=time.monotonic()+120
  while time.monotonic()<end:
   try:run('exec','-T','baseline','/dispatcharrpy/bin/python','-c',"import urllib.request;assert urllib.request.urlopen("+repr(url)+",timeout=2).status==200",timeout=5);return
   except bounded.CollectionFailure:time.sleep(.1)
  raise TimeoutError('OwnedHTTPReadiness')
 def child(role,argv,db='dispatcharr',port=6379):
  jobs[role]=bounded.Collected(compose+['exec','-T','-e','POSTGRES_DB='+db,'-e','REDIS_PORT='+str(port),'baseline','/dispatcharrpy/bin/python','/normal-tools/owned_process.py','start',role]+argv,env,8388608,stop_owned,stdout_path=out/(role+'.stdout'),stderr_path=out/(role+'.stderr'))
 def finish(role):
  run('exec','-T','baseline','/dispatcharrpy/bin/python','/normal-tools/owned_process.py','stop',role)
  try:jobs[role].wait(15)
  except bounded.CollectionFailure:
   if jobs[role].invalid or jobs[role].process.returncode not in[0,143,-15]or jobs[role].process.poll()is None or any(thread.is_alive()for thread in jobs[role].threads):raise
  jobs.pop(role)
 try:
  run('up','-d','--no-build','baseline');ready('http://127.0.0.1:5656/api/core/version/');run('exec','-T','baseline','/dispatcharrpy/bin/python','/verification/supervisor.py','control','stop');before=helper('dispatcharr','default-audit');report['default_before']=before
  pg('CREATE DATABASE '+template);helper(template,'migrate');helper(template,'seed');seeded=helper(template,'fingerprint')
  for db in[source_db,native_db]:pg('CREATE DATABASE '+db+' TEMPLATE '+template)
  for db in[source_db,native_db]:
   assert helper(db,'fingerprint')['dataset_sha256']==seeded['dataset_sha256'];assert helper(db,'caps')['account_max_streams']==2 and helper(db,'caps')['profile_max_streams']==2
  for role,port in[('source-redis',6390),('native-redis',6391)]:
   child(role,['redis-server','--bind','0.0.0.0','--port',str(port),'--requirepass','playback-fixture-only','--save','','--appendonly','no','--dir','/tmp']);end=time.monotonic()+10
   while True:
    try:assert run('exec','-T','baseline','redis-cli','-p',str(port),'-a','playback-fixture-only','PING').strip().endswith(b'PONG');break
    except bounded.CollectionFailure:
     if time.monotonic()>end:raise
     time.sleep(.1)
  for mode,db,base in[('source',source_db,'http://127.0.0.1:5657'),('native',native_db,'http://candidate:9192')]:
   write_overlay(mode);run('up','-d','--no-build','origin');ready('http://origin:8080/health');report[mode+'_before']=helper(db,'fingerprint');assert report[mode+'_before']['dataset_sha256']==seeded['dataset_sha256']
   if mode=='source':child('source-web',['/dispatcharrpy/bin/python','/normal-tools/source_wsgi.py'],source_db,6390)
   else:
    run('up','-d','--no-build','candidate');cid=run('ps','-q','candidate').decode().strip();actual=json.loads(command(['docker','inspect',cid]))[0]['Image'];assert actual==candidate['image_id'];report['actual_Main_image_id']=actual
   ready(base+'/api/core/version/')
   try:run('exec','-T','-e','POSTGRES_DB='+db,'-e','NORMAL_MODE='+mode,'-e','NORMAL_BASE='+base,'baseline','/dispatcharrpy/bin/python','/normal-tools/phase.py',timeout=120)
   except bounded.CollectionFailure as error:
    if not(out/('normal-'+mode+'-private.json')).is_file():raise
    report[mode+'_phase_exit_error']=str(error)
   report['backends'][mode]=json.loads((out/('normal-'+mode+'-private.json')).read_text());report[mode+'_caps_after']=helper(db,'caps');report[mode+'_after']=helper(db,'fingerprint')
   if mode=='source':
    finish('source-web');run('exec','-T','baseline','/dispatcharrpy/bin/python','-c',"import socket; sockets=[socket.socket()for _ in range(2)]; assert all(sock.connect_ex(('127.0.0.1',port))!=0 for sock,port in zip(sockets,[6385,5657]));[sock.close()for sock in sockets]");report['Source_status_and_main_listener_closed']=True
   else:run('stop','--timeout','15','candidate',timeout=30)
   run('stop','--timeout','15','origin',timeout=30);cid=run('ps','-aq','origin').decode().strip();assert json.loads(command(['docker','inspect',cid]))[0]['State']['Running']is False
   report[mode+'_origin_terminal']=json.loads((out/('origin-'+mode+'-terminal.json')).read_text());assert report[mode+'_origin_terminal']['actual_open_TCP']==0 and not report[mode+'_origin_terminal']['invalid']and report[mode+'_origin_terminal']['server_thread_settled']
  report['HTTP_pair_complete']=True;report['comparison_scope']='VOD same-SID continuous-body origin characterization only; no TS-sharing or fullM4 transfer'
 except Exception as error:report['error_type']=type(error).__name__;report['error_message']=str(error)[:256]
 finally:
  for service in['candidate','origin']:
   try:run('stop','--timeout','15',service,timeout=30)
   except Exception as error:report['cleanup_errors'].append(service+type(error).__name__)
  for role in list(jobs):
   try:finish(role)
   except Exception as error:report['cleanup_errors'].append(role+type(error).__name__)
  try:report['default_after']=helper('dispatcharr','default-audit');report['default_named_tables_sequences_unchanged']=report['default_after']==report['default_before']
  except Exception as error:report['cleanup_errors'].append('default-audit'+type(error).__name__)
  try:run('down','--volumes','--remove-orphans','--timeout','10',timeout=120)
  except Exception as error:report['cleanup_errors'].append('down'+type(error).__name__)
  report['remaining']={}
  for kind,args2 in{'containers':['ps','-aq'],'networks':['network','ls','-q'],'volumes':['volume','ls','-q']}.items():
   try:report['remaining'][kind]=command(['docker']+args2+['--filter','label=com.docker.compose.project='+project]).decode().strip()
   except Exception:report['remaining'][kind]='UNVERIFIED'
  report['owned_cleanup_valid']=not report['cleanup_errors']and not any(report['remaining'].values());report['observation_complete']=report['HTTP_pair_complete']and all(item.get('HTTP_capture_valid')is True for item in report['backends'].values())and report['owned_cleanup_valid']and report.get('default_named_tables_sequences_unchanged',False);from sharing_acceptance import pair
  report.update(pair(report));save(out/'summary.json',report)
 return 0 if report.get('sharing_characterization_qualified')else 1
if __name__=='__main__':raise SystemExit(main())
