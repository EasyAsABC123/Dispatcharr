"""BLOCKED new-image three-case4s Main observation/qualification; validity and parity are distinct; no resource mode."""
import argparse,hashlib,importlib.util,json,os,shutil,subprocess,sys,time,urllib.request,uuid
from pathlib import Path
HERE=Path(__file__).resolve().parent
BASE='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
ARCHIVE='f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff'
SUPPORT='c91ab5ece7926f1b6b04a8069c6ad38c5b5d8d55'
def load(path,name):
 spec=importlib.util.spec_from_file_location(name,path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module
def save(path,value):Path(path).write_text(json.dumps(value,indent=2)+'\n')
def main():
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--repo',type=Path,required=True);parser.add_argument('--candidate-image',required=True);parser.add_argument('--build-manifest',type=Path,required=True);parser.add_argument('--source-archive',type=Path,required=True);parser.add_argument('--output',type=Path,required=True);args=parser.parse_args();repo=args.repo.resolve();out=args.output.resolve()
 # image54 private preparation remains blocked; approved launcher only after Root fresh cue.
 raise RuntimeError('IMAGE54_MAIN4S_EXECUTION_BLOCKED: Root review and fresh quiet cue required')
def execute(args,repo,out):
 out.mkdir(parents=True,exist_ok=False);controller=out/'controller';shutil.copytree(HERE,controller,ignore=shutil.ignore_patterns('__pycache__'))
 proof=load(controller/'candidate_inputs.py','candidate_guard').bound_candidate(args.build_manifest,args.candidate_image,controller/'expected-runtime.json',args.source_archive);save(out/'candidate-inputs.json',proof)
 original=out/'original-source';original.mkdir();raw=subprocess.check_output(['git','-C',str(repo),'archive',BASE]);assert hashlib.sha256(raw).hexdigest()==ARCHIVE;subprocess.run(['tar','-x','--no-same-owner','-C',str(original)],input=raw,check=True);(original/'media').mkdir(exist_ok=True)
 support=out/'support';support.mkdir();raw=subprocess.check_output(['git','-C',str(repo),'archive',SUPPORT,'scripts','docker']);subprocess.run(['tar','-x','--no-same-owner','-C',str(support)],input=raw,check=True);save(out/'support.json',{'commit':SUPPORT,'archive_sha256':hashlib.sha256(raw).hexdigest()})
 guards=load(controller/'fixture_guards.py','pending_fixture_guards');observation=load(controller/'observation.py','four_second_comparison');signal_module=load(controller/'request_observer.py','Source_signal_validation')
 ports=load(support/'scripts/rust-integration/fixture_ports.py','pending_ports');bounded=load(controller/'bounded_process.py','pending_bounded');results=[]
 for case in['success','busy','clientloss']:
  caseout=out/case;caseout.mkdir();project='dispatcharr-pending-main-'+uuid.uuid4().hex[:10];report={'passed':False,'observation_valid':False,'parity_passed':False,'case':case,'project':project,'performance_verified':False,'scope':'separate fixed4s Main SQL/Bearer physical-reply observation; validity is not parity'};jobs={};index=0;nonce=uuid.uuid4().hex
  database='rust_pending_'+uuid.uuid4().hex[:10];template=database+'_template';source_db=database+'_source';native_db=database+'_native'
  env=dict(os.environ,BASELINE_SOURCE_DIR=str(original),BASELINE_EVIDENCE_DIR=str(caseout));overlay=caseout/'fixture.json'
  save(overlay,{'services':{'baseline':{'volumes':[str(controller)+':/pending-tools:ro'],'environment':{'PENDING_MAIN_FIXTURE':'playback-fixture-only','PENDING_MAIN_NONCE':nonce}},'origin':{'environment':{'VOD_FIXTURE_BYTES_PER_SECOND':'32768'}},'candidate':{'image':args.candidate_image,'environment':{'DATABASE_URL':'postgresql://dispatch:playback-fixture-only@baseline:5432/'+native_db,'REDIS_URL':'redis://:playback-fixture-only@baseline:6381/0','RUST_VOD_REDIS_RECOVERY_ENABLED':'1','RUST_MAX_VOD_SESSIONS':'4','RUST_MAX_VOD_REQUESTS':'4'}}}})
  portfile=caseout/'ports.yml';ports.write_port_overlay(portfile,{'baseline':[5656],'origin':[8080],'candidate':[9192]})
  compose=['docker','compose','-p',project,'-f',str(support/'docker/compose.rust-playback.yml'),'-f',str(support/'scripts/rust-vod-contracts/compose.yml'),'-f',str(support/'docker/compose.rust-candidate.yml'),'-f',str(overlay),'-f',str(portfile)]
  def stop_owned():
   result=subprocess.run(compose+['kill'],env=env,stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=15)
   if result.returncode:raise RuntimeError('OwnedStopUnacknowledged')
  def command(argv,body=None,timeout=180):
   nonlocal index
   index+=1;return bounded.Collected(argv,env,8388608,stop_owned,stderr_path=caseout/('command-'+str(index)+'.stderr'),body=body).wait(timeout)
  def run(*argv,body=None,timeout=180):return command(compose+list(argv),body,timeout)
  def pg(sql):return run('exec','-T','-e','PGPASSWORD=playback-fixture-only','baseline','psql','-h','127.0.0.1','-U','dispatch','-d','postgres','-v','ON_ERROR_STOP=1','-At','-c',sql)
  def internal_ready(url):
   end=time.monotonic()+120
   while time.monotonic()<end:
    try:
     run('exec','-T','baseline','/dispatcharrpy/bin/python','-c',"import urllib.request;assert urllib.request.urlopen("+repr(url)+",timeout=2).status==200",timeout=5);return
    except bounded.CollectionFailure:time.sleep(.1)
   raise TimeoutError('OwnedInternalHTTPReady')
  def helper(db,action,port=6379):return json.loads(run('exec','-T','-e','POSTGRES_DB='+db,'-e','REDIS_PORT='+str(port),'baseline','/dispatcharrpy/bin/python','/pending-tools/db_fixture.py',action).splitlines()[-1])
  def ready(url):
   end=time.monotonic()+180
   while time.monotonic()<end:
    try:
     with urllib.request.urlopen(url,timeout=2)as response:
      if response.status==200:return
    except Exception:time.sleep(.25)
   raise TimeoutError('OwnedHTTPReady')
  def child(role,argv,db=None,port=6379):
   commandline=compose+['exec','-T','-e','POSTGRES_DB='+(db or'dispatcharr'),'-e','REDIS_PORT='+str(port),'baseline','/dispatcharrpy/bin/python','/pending-tools/owned_process.py','start',role]+argv
   jobs[role]=bounded.Collected(commandline,env,8388608,stop_owned,stdout_path=caseout/(role+'.stdout'),stderr_path=caseout/(role+'.stderr'))
  def finish(role):
   run('exec','-T','baseline','/dispatcharrpy/bin/python','/pending-tools/owned_process.py','stop',role)
   try:jobs[role].wait(15)
   except bounded.CollectionFailure:
    # SIGTERM is expected only AFTER exact PID/starttime stop. No unbounded
    # docker-exec client drop is accepted as process settlement.
    if jobs[role].invalid is not None or jobs[role].process.returncode not in[0,143,-15]or jobs[role].process.poll()is None or any(t.is_alive()for t in jobs[role].threads):raise
  try:
   run('up','-d','--no-build','baseline');ready('http://'+run('port','baseline','5656').decode().strip()+'/api/core/version/')
   run('exec','-T','baseline','/dispatcharrpy/bin/python','/verification/supervisor.py','control','stop')
   run('up','-d','--no-build','origin');ready('http://'+run('port','origin','8080').decode().strip()+'/health')
   report['origin_preflight']=json.loads(run('exec','-T','baseline','/dispatcharrpy/bin/python','/pending-tools/origin_guard.py').splitlines()[-1]);assert report['origin_preflight']['passed']
   default_before=helper('dispatcharr','default-audit');report['default_database_before']=default_before
   pg('CREATE DATABASE '+template);helper(template,'migrate');helper(template,'seed');before=helper(template,'fingerprint');report['template_fingerprint']=before
   for db in[source_db,native_db]:pg('CREATE DATABASE '+db+' TEMPLATE '+template)
   source_before=helper(source_db,'fingerprint');native_before=helper(native_db,'fingerprint');report['DB_clone_binding']=guards.clones(before,source_before,native_before,[template,source_db,native_db]);report['independent_DB_clones_verified']=True
   for role,port in[('source-redis',6390),('native-redis',6391)]:child(role,['redis-server','--bind','0.0.0.0','--port',str(port),'--requirepass','playback-fixture-only','--save','','--appendonly','no','--dir','/tmp'])
   for port in[6390,6391]:
    end=time.monotonic()+10
    while time.monotonic()<end:
     try:
      assert run('exec','-T','baseline','redis-cli','-p',str(port),'-a','playback-fixture-only','PING').strip().endswith(b'PONG');break
     except bounded.CollectionFailure:time.sleep(.1)
    else:raise TimeoutError('OwnedDedicatedRedisReady')
   child('gate',['/dispatcharrpy/bin/python','/pending-tools/resp_gate.py','--script-sha1',proof['reserve_script_sha1'],'--backend-port','6391','--evidence','/evidence/proxy-private.json'])
   child('source-gate',['/dispatcharrpy/bin/python','/pending-tools/source_resp_gate.py','--evidence','/evidence/source-proxy-private.json'])
   for proxy_port in[6381,6383]:
    end=time.monotonic()+10
    while time.monotonic()<end:
     try:assert run('exec','-T','baseline','redis-cli','-p',str(proxy_port),'-a','playback-fixture-only','PING').strip().endswith(b'PONG');break
     except bounded.CollectionFailure:time.sleep(.1)
    else:raise TimeoutError('OwnedPhysicalReplyProxyReady')
   child('source-web',['/dispatcharrpy/bin/python','/pending-tools/source_wsgi.py'],source_db,6383)
   # Explicitly verify server/control readiness and all fingerprints immediately
   # before phases; no copied Source-mutated rows feed the native candidate.
   for mode,db,port,base in[('source',source_db,6390,'http://127.0.0.1:5657'),('native',native_db,6391,'http://candidate:9192')]:
    if mode=='native':
     run('up','-d','--no-build','candidate');cid=run('ps','-q','candidate').decode().strip();inspection=json.loads(command(['docker','inspect',cid]))[0];assert inspection['Image']==proof['image_id'];report['actual_Main_image_id']=inspection['Image']
    internal_ready(base+'/api/core/version/')
    if mode=='source':
     report['Source_actual_client_config']=json.loads((caseout/'Source-client-config.json').read_text());assert report['Source_actual_client_config']['proxy_port']==6383 and report['Source_actual_client_config']['socket_timeout']==60
    phase_fingerprint=helper(db,'fingerprint',port);assert phase_fingerprint['dataset_sha256']==before['dataset_sha256'];report[mode+'_before']=phase_fingerprint
    argv=['exec','-T','-e','POSTGRES_DB='+db,'-e','REDIS_PORT='+str(port),'-e','PENDING_MODE='+mode,'-e','PENDING_CASE='+case,'-e','PENDING_BASE='+base,'baseline','/dispatcharrpy/bin/python','/pending-tools/phase.py'];run(*argv,timeout=90)
    report[mode]=json.loads((caseout/('pending-main-'+mode+'-'+case+'-private.json')).read_text());assert report[mode]['observation_valid']
    if mode=='source':
     end=time.monotonic()+10
     while True:
      raw_status=run('exec','-T','baseline','/dispatcharrpy/bin/python','-c',"import urllib.request; r=urllib.request.urlopen('http://127.0.0.1:6385/state',timeout=3); b=r.read(4097); assert len(b)<=4096; print(b.decode())",timeout=5)
      observer_status=json.loads(raw_status);report['Source_signal_status']=observer_status
      if observer_status['invalid']or(caseout/'Source-request-signals-invalid.json').exists():raise ValueError('SourceSignalObserverInvalid')
      try:
       signal_rows=[json.loads(line)for line in(caseout/'Source-request-signals.jsonl').read_text().splitlines()];report['Source_signal_coverage']=observation.signal_evidence(observer_status,signal_rows,['initial','target','follower','other']if case=='busy'else['initial','target','follower'],signal_module.validate);break
      except (FileNotFoundError,ValueError)as error:
       if isinstance(error,ValueError)and str(error)not in['MissingSignalCompletion','SignalRoleCoverage','SignalStatusJournalMismatch']and not isinstance(error,json.JSONDecodeError):raise
       if time.monotonic()>end:raise TimeoutError('SourceSignalCompletionCoverage')
       time.sleep(.1)
   report['comparison']=observation.compare(report['source'],report['native']);report['observation_valid']=report['comparison']['observation_valid'];report['parity_passed']=report['comparison']['parity_passed'];report['Source_reference_expectation_matched']=report['source']['Source_expected_reference_outcome_matched']
   report['passed']=report['observation_valid']and report['parity_passed']and report['Source_reference_expectation_matched'];report['full_parity_accepted']=False
  except Exception as error:report['observation_valid']=False;report['error_type']=type(error).__name__;report['error_message']=str(error)
  finally:
   try:run('stop','--timeout','15','candidate',timeout=30)
   except Exception as error:report['passed']=False;report.setdefault('physical_cleanup_errors',[]).append('CandidateStop'+type(error).__name__)
   if 'source-web'in jobs:
    try:
     raw_status=run('exec','-T','baseline','/dispatcharrpy/bin/python','-c',"import urllib.request; r=urllib.request.urlopen('http://127.0.0.1:6385/state',timeout=3); b=r.read(4097); assert len(b)<=4096; print(b.decode())",timeout=5)
     final_status=json.loads(raw_status);final_rows=[json.loads(line)for line in(caseout/'Source-request-signals.jsonl').read_text().splitlines()]
     if (caseout/'Source-request-signals-invalid.json').exists():raise ValueError('TerminalSourceSignalMarker')
     report['Source_signal_terminal_coverage']=observation.signal_evidence(final_status,final_rows,['initial','target','follower','other']if case=='busy'else['initial','target','follower'],signal_module.validate);report['Source_signal_terminal_status']=final_status;report['Source_signal_terminal_valid']=True
    except Exception as error:report['Source_signal_terminal_valid']=False;report.setdefault('observation_finalization_errors',[]).append('SourceSignalTerminal'+type(error).__name__)
   for role in list(jobs):
    try:finish(role)
    except Exception as error:report['passed']=False;report.setdefault('physical_cleanup_errors',[]).append(type(error).__name__)
   try:
    default_after=helper('dispatcharr','default-audit');report['default_database_after']=default_after;report['default_named_tables_sequences_unchanged']=default_after==default_before
    if default_after!=default_before:report['passed']=False
   except Exception as error:report['passed']=False;report.setdefault('physical_cleanup_errors',[]).append('DefaultAudit'+type(error).__name__)
   source_proxy_file=caseout/'source-proxy-private.json'
   if source_proxy_file.exists():
    source_proxy=json.loads(source_proxy_file.read_text());report['Source_proxy_terminal']=source_proxy
    if source_proxy['invalid']or source_proxy['connections']!=0 or source_proxy['seen']!=1:report['passed']=False
   elif 'source-gate'in jobs:report['passed']=False;report['Source_proxy_terminal_missing']=True
   proxy_file=caseout/'proxy-private.json'
   if proxy_file.exists():
    proxy_result=json.loads(proxy_file.read_text());report['proxy_terminal']=proxy_result
    if proxy_result['invalid']or proxy_result['connections']!=0:report['passed']=False
   elif 'gate'in jobs:report['passed']=False;report['proxy_terminal_missing']=True
   try:run('down','--volumes','--remove-orphans','--timeout','10',timeout=120)
   except Exception as error:report['passed']=False;report.setdefault('physical_cleanup_errors',[]).append('ComposeDown'+type(error).__name__)
   report['remaining']={}
   for kind,argv in{'containers':['ps','-aq'],'networks':['network','ls','-q'],'volumes':['volume','ls','-q']}.items():
    try:report['remaining'][kind]=command(['docker']+argv+['--filter','label=com.docker.compose.project='+project]).decode().strip()
    except Exception as error:report['passed']=False;report.setdefault('physical_cleanup_errors',[]).append('Inventory'+kind+type(error).__name__);report['remaining'][kind]='UNVERIFIED'
   if any(report['remaining'].values()):report['passed']=False
   report.update(observation.finalize(report))
   save(caseout/'report.json',report);results.append(report);save(out/'summary.json',{'passed':all(r['passed']for r in results),'observation_valid':all(r['observation_valid']for r in results),'parity_passed':all(r['parity_passed']for r in results),'cases':results,'scope':__doc__,'performance_verified':False})
 return 0 if all(r['passed']for r in results)else 1
if __name__=='__main__':raise SystemExit(main())
