"""One grouped actual Main TLS13 counterpart, extended from Native12 attempt1.
Original Dispatcharr and dependency authors retain all application/algorithm credit.
Default is definition-only. Root owns image proof, the global slot and execution.
"""
import argparse,hashlib,importlib.util,json,os,re,shutil,sys,time,uuid
from pathlib import Path
ROOT=Path('/home/justin/github/Dispatcharr');HERE=Path(__file__).resolve().parent
PRIOR=ROOT/'artifacts/rust/m478-source-normal-wsgi-cache-1'
IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
SOURCE12=ROOT/'artifacts/rust/m478-pg-tls-authority-3/qualified-source-summary.json'
PLAIN=ROOT/'artifacts/rust/m478-pg-allow-plaintext-source-1/report.json'

def need(value,label):
 if not value:raise ValueError(label)
def read(path):return json.loads(Path(path).read_bytes())
def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def save(path,value):
 temporary=path.with_suffix(path.suffix+'.tmp');temporary.write_text(json.dumps(value,indent=2,allow_nan=False)+'\n');temporary.replace(path)
def load(path,name):
 spec=importlib.util.spec_from_file_location(name,path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module

def definition():
 source=read(SOURCE12);plain=read(PLAIN)
 need(source['passed']and source['all_exact_cleanup']and source['Source_API_calls']==0,'ExistingQualifiedSource12')
 need(plain['actual_controller_exit']==0 and plain['exact_cleanup']and plain['default_unchanged']and len(plain['cases'])==1,'ExistingQualifiedSourcePlain1')
 ref={row['name']:row for row in source['authority_cases']};ref.update({row['name']:row for row in plain['cases']})
 changes=[('valid_verify_full',{}),('wrong_ca',{'POSTGRES_SSL_CA_CERT':'/tls/wrong-ca.pem'}),('missing_ca_path',{'POSTGRES_SSL_CA_CERT':'/tls/missing.pem'}),('hostname_mismatch',{'POSTGRES_HOST':'localhost'}),('verify_ca_mismatch',{'POSTGRES_HOST':'localhost','POSTGRES_SSL_MODE':'verify-ca'}),('no_client_cert',{'POSTGRES_SSL_CERT':'','POSTGRES_SSL_KEY':''}),('require_wrong_ca',{'POSTGRES_SSL_MODE':'require','POSTGRES_SSL_CA_CERT':'/tls/wrong-ca.pem'}),('unix_socket_ssl_true',{'POSTGRES_HOST':'/var/run/postgresql'}),('ipv6_verify_ca',{'POSTGRES_HOST':'::1','POSTGRES_SSL_MODE':'verify-ca'}),('source_allow',{'POSTGRES_SSL_MODE':'allow'}),('source_prefer',{'POSTGRES_SSL_MODE':'prefer'}),('sqlite',{'DB_ENGINE':'sqlite'}),('source_allow_plaintext_preference',{'POSTGRES_SSL_MODE':'allow'})]
 rows=[]
 for name,env in changes:
  source_row=ref[name];a=source_row['actual'];sqlite=name=='sqlite';ready=False if sqlite else a['connected'];observation=a.get('observation')
  if name=='source_allow_plaintext_preference':need(a['connected']and observation[4]is False,'MeasuredPlaintextPreference')
  rows.append({'name':name,'family':'sqlite'if sqlite else'postgres','changes':env,'server_phase':'plaintext_allowed'if name=='source_allow_plaintext_preference'else'TLS_required','source_connected':a['connected'],'expected_native_ready':ready,'expected_native_ssl':observation[4]if ready else None,'expected_native_client_dn':observation[7]if ready else None,'expected_Main_exit':0 if ready else 1,'Source_reference':str(PLAIN if name=='source_allow_plaintext_preference'else SOURCE12),'Source_reference_sha256':sha(PLAIN if name=='source_allow_plaintext_preference'else SOURCE12),'Source_case_sha256':hashlib.sha256(json.dumps(source_row,sort_keys=True,allow_nan=False).encode()).hexdigest()})
 need(len(rows)==13 and sum(r['family']=='postgres'for r in rows)==12,'Explicit13Cells12PG1SQLite')
 return {'format':1,'status':'not_run','mode':'verify-ca-allow-v1','global_fixture_slots':1,'single_bootstrapped_PG17_fixture':True,'planned_Main_starts':13,'PostgreSQL_compatibility_cells':12,'SQLite_required_parity':False,'SQLite_expected_native_unsupported_guard':True,'cases':rows,'Source_API_calls':0,'Source_driver_calls':0,'Source_replays':0,'Cargo_or_image_builds':0,'performance_claims':False}

def execute(args,plan):
 for name in ['image','proof','git','build_manifest','source_map','output']:need(getattr(args,name)is not None,'RequiredExecutionInput:'+name)
 proof=read(args.proof);build=read(args.build_manifest)
 need(proof['passed']and proof['image_id']==args.image and proof['git_revision']==args.git,'ActualPhysicalImageProof')
 need(build['passed']and build['exit_code']==0 and build['image_id']==args.image and build['git_revision']==args.git,'ActualBuild0MatchesProof')
 mapping=read(args.source_map);need(type(mapping)is dict and mapping and all(type(k)is str and type(v)is str and re.fullmatch('[a-f0-9]{64}',v)for k,v in mapping.items()),'TypedSourceMap')
 need({k:v for k,v in mapping.items()if k.startswith('rust/')}=={k:v for k,v in build['source_sha256'].items()if k.startswith('rust/')},'ExactValidatedPackagedRustInputs')
 need(re.fullmatch('sha256:[a-f0-9]{64}',args.image)is not None and re.fullmatch('[a-f0-9]{40}',args.git)is not None,'ConcreteImageGit')
 out=args.output.absolute();need(out.is_relative_to(ROOT/'artifacts/rust')and not out.exists(),'FreshOwnedOutput');out.mkdir(mode=0o700);tools=out/'tools';tools.mkdir()
 for p in (HERE/'tools').iterdir():
  if p.is_file():shutil.copyfile(p,tools/p.name)
 for name in ['bootstrap_env.py','start.sh']:shutil.copyfile(PRIOR/'tools'/name,tools/name)
 shutil.copytree(PRIOR/'inputs/pg-init',out/'pg-init');shutil.copytree(PRIOR/'schema',out/'schema')
 envmod=load(tools/'bootstrap_env.py','owned_environment');environment=envmod.storage_environment();bounded=load(ROOT/'artifacts/rust/m4-m7-m8-wide-batch-1/prerequisites/assets/bounded_cli.py','owned_bounded_cli')
 project='dispatcharr-native-pgtls-'+uuid.uuid4().hex[:10];database='rust_probe_pluginrepo_'+uuid.uuid4().hex[:10];started=time.monotonic();serial=0;used_bytes=0;source_cid=None;cleaning=False;cleanup_deadline=None
 report={'project':project,'database':database,'source_storage_image':IMAGE,'candidate':{'image':args.image,'git':args.git,'proof_path':str(args.proof.absolute()),'proof_sha256':sha(args.proof),'build_manifest_path':str(args.build_manifest.absolute()),'build_manifest_sha256':sha(args.build_manifest),'source_map_path':str(args.source_map.absolute()),'source_map_sha256':sha(args.source_map)},'definition':plan,'commands':[],'cases':[],'native_cids':[],'passed':False,'Source_API_calls':0,'Source_driver_calls':0,'Source_application_imports':0,'Source_replays':0,'performance_claims':False,'private_keys':'Only inside owned Docker TLS volume; never copied to host'}
 save(out/'report.json',report);save(out/'case-definitions.json',plan);save(out/'input-sha256.json',{str(p):sha(p)for p in [HERE/'run.py',*list(tools.iterdir()),SOURCE12,PLAIN,args.proof,args.build_manifest,args.source_map]if p.is_file()})
 def cmd(argv,seconds=30,check=True):
  nonlocal serial,used_bytes
  need(serial<200,'FiniteCLI200');serial+=1;deadline=cleanup_deadline if cleaning else started+900
  need(used_bytes<33554432,'FiniteOutput32MiB')
  stdout,stderr,status=bounded.collect(['docker','--host','unix:///tmp/dispatcharr-docker.sock',*argv],min(seconds,max(.01,deadline-time.monotonic())),min(2097152,33554432-used_bytes));used_bytes+=len(stdout)+len(stderr)
  (out/f'command-{serial:03d}.stdout').write_bytes(stdout);(out/f'command-{serial:03d}.stderr').write_bytes(stderr);report['commands'].append(status);save(out/'report.json',report)
  need(status['capture_complete']and not status['forced_termination'],'ActualWaitEOF')
  need(not check or status['actual_wait_exit']==0,'ActualCLI0');return stdout,status
 compose=['compose','-p',project,'-f',str(out/'compose.json')]
 def dc(*argv,seconds=30,check=True):return cmd(compose+list(argv),seconds,check)
 def sql(text):return dc('exec','-T','source','psql','-XAtq','-h','/var/run/postgresql','-U','dispatch','-d','postgres','-v','ON_ERROR_STOP=1','-c',text)[0]
 def hba():return json.loads(sql("SELECT json_build_object('ssl',current_setting('ssl'),'rules',(SELECT json_agg(r) FROM(SELECT rule_number,type,address,auth_method,error FROM pg_hba_file_rules ORDER BY rule_number)r))"))
 service={'image':IMAGE,'pull_policy':'never','network_mode':'none','entrypoint':['bash','/native-tools/start.sh'],'environment':environment,'volumes':[str(tools)+':/native-tools:ro',str(out/'schema')+':/native-source:ro',str(out/'pg-init')+':/pg-bootstrap:ro',str(out)+':/evidence','tls:/tls','socket:/var/run/postgresql','native-data:/native-data'],'tmpfs':['/data:mode=1777,size=2147483648','/tmp:mode=1777,size=268435456'],'cpus':1,'mem_limit':'1536m','pids_limit':256}
 initial={'services':{'source':service},'volumes':{'tls':{},'socket':{},'native-data':{}}};save(out/'compose.json',initial)
 base={'POSTGRES_DB':database,'POSTGRES_HOST':'127.0.0.1','POSTGRES_USER':'dispatch','POSTGRES_PASSWORD':'playback-fixture-only','POSTGRES_SSL':'true','POSTGRES_SSL_MODE':'verify-full','POSTGRES_SSL_CA_CERT':'/tls/ca.pem','POSTGRES_SSL_CERT':'/tls/client.pem','POSTGRES_SSL_KEY':'/tls/client.key','DJANGO_SECRET_KEY':'owned-pg-tls-context-only','RUST_BIND':'127.0.0.1:9192','RUST_DATABASE_POOL_SIZE':'8','RUST_INITIALIZE_EMPTY_DATABASE':'0'}
 try:
  need(not cmd(['container','ls','--all','--filter','label=com.docker.compose.project='+project,'--format','{{.ID}}'])[0].strip(),'FreshProject')
  dc('up','-d','--no-build','--pull','never','source',seconds=120);deadline=time.monotonic()+150
  while not(out/'bootstrap-ready').exists():need(time.monotonic()<deadline,'Bootstrap150s');time.sleep(.1)
  source_cid=dc('ps','--all','-q','source')[0].decode().strip();report['storage_cid']=source_cid;save(out/'storage-inspect.json',json.loads(cmd(['inspect',source_cid])[0]))
  sql('CREATE DATABASE '+database+' TEMPLATE dispatcharr');before=json.loads(dc('exec','-T','source','/dispatcharrpy/bin/python','-B','/native-tools/state.py')[0]);save(out/'default-before.json',before)
  dc('exec','-T','source','bash','/native-tools/tls.sh',seconds=40);save(out/'TLS-required-hba.json',hba())
  dc('exec','-T','source','/dispatcharrpy/bin/python','-c',"import sqlite3,os;c=sqlite3.connect('/native-data/dispatcharr.db');c.execute('CREATE TABLE sentinel(value TEXT)');c.execute(\"INSERT INTO sentinel VALUES('unchanged')\");c.commit();c.close();os.chown('/native-data',65532,65532);os.chown('/native-data/dispatcharr.db',65532,65532)")
  sqlite_before=dc('exec','-T','source','/dispatcharrpy/bin/python','-c',"import hashlib;print(hashlib.sha256(open('/native-data/dispatcharr.db','rb').read()).hexdigest())")[0].decode().strip();fixture_valid=True
  for spec in plan['cases']:
   row={'name':spec['name'],'family':spec['family'],'expected_native_ready':spec['expected_native_ready'],'source_connected':spec['source_connected'],'expected_match':False};report['cases'].append(row);save(out/'report.json',report);cid=None
   if not fixture_valid:row.update(state='blocked',reason='Earlier actual Main could not settle; fixture not reused');continue
   try:
    if spec['server_phase']=='plaintext_allowed':
     dc('exec','-T','source','bash','/native-tools/allow_plain.sh');rules=hba();need(rules['ssl']=='on','TLSStillEnabledWhenAllowingPlaintext');save(out/'plaintext-allowed-hba.json',rules)
    config=json.loads(json.dumps(initial));config['services']['native']={'image':args.image,'pull_policy':'never','network_mode':'service:source','environment':base|spec['changes'],'volumes':['tls:/tls:ro','socket:/var/run/postgresql','native-data:/data'],'cpus':1,'mem_limit':'768m','pids_limit':128,'stop_grace_period':'10s'};save(out/'compose.json',config)
    dc('up','-d','--no-build','--pull','never','native',seconds=30);cid=dc('ps','--all','-q','native')[0].decode().strip();need(re.fullmatch('[a-f0-9]{64}',cid)is not None,'ActualMainCID');report['native_cids'].append(cid)
    inspected=json.loads(cmd(['inspect',cid])[0])[0];save(out/(spec['name']+'-Main-inspect.json'),inspected);need(inspected['Image']==args.image and inspected['Config']['User']=='65532:65532'and inspected['Config']['Entrypoint']==['/usr/local/bin/dispatcharr-rs'],'ActualProductionImageUidEntrypoint')
    observed=json.loads(dc('exec','-T','-e','NATIVE_READY_EXPECTED='+('1'if spec['expected_native_ready']else'0'),'-e','TARGET_DB='+database,'source','/dispatcharrpy/bin/python','-B','/native-tools/observe.py',seconds=12)[0]);row['actual']=observed;ready=any(c.get('status')==200 for c in observed['HTTP_observations']);row['http_ready']=ready;row['sqlite_unchanged']=observed['sqlite_sha256']==sqlite_before
    row['expected_match']=ready==spec['expected_native_ready']and row['sqlite_unchanged']
    if ready:
     sessions=observed['native_sessions'];row['physical_mode_match']=bool(sessions)and all(s['ssl']is spec['expected_native_ssl']and s['client_dn']==spec['expected_native_client_dn']for s in sessions)
     if spec['name']=='unix_socket_ssl_true':row['physical_mode_match'] &= all(s['client_addr']is None for s in sessions)
     row['expected_match'] &= row['physical_mode_match'];row['compatibility_match']=spec['family']=='postgres'and ready==spec['source_connected']and row['physical_mode_match']
     (out/(spec['name']+'-Main-process.txt')).write_bytes(cmd(['exec',cid,'/bin/sh','-c','cat /proc/1/status; printf "HOSTNAME="; cat /proc/sys/kernel/hostname'],10)[0])
    else:row['expected_match'] &= not observed['native_sessions'];row['compatibility_match']=spec['family']=='postgres'and ready==spec['source_connected']and not observed['native_sessions']
    row['state']='captured'
   except BaseException as error:row.update(state='failed',error={'type':type(error).__name__,'message':str(error)})
   finally:
    if cid:
     try:
      cmd(['stop','--time','10',cid],20);row['actual_Main_wait_exit']=int(cmd(['wait',cid],15)[0].decode().strip());row['expected_match'] &= row['actual_Main_wait_exit']==spec['expected_Main_exit']
      logs=cmd(['logs',cid])[0];(out/(spec['name']+'-Main.log')).write_bytes(logs)
      if spec['family']=='sqlite':row['explicit_unsupported_guard']='SQLite backend is not yet implemented' in logs.decode();row['expected_match'] &= row['explicit_unsupported_guard']
      cmd(['rm','--volumes',cid],15);row['actual_Main_removed']=True
     except BaseException as error:row.update(expected_match=False,shutdown_error={'type':type(error).__name__,'message':str(error)});fixture_valid=False
    row['compatibility_match']=row.get('compatibility_match',False)and row.get('actual_Main_wait_exit')==spec['expected_Main_exit'];save(out/'report.json',report)
  after=json.loads(dc('exec','-T','source','/dispatcharrpy/bin/python','-B','/native-tools/state.py')[0]);save(out/'default-after.json',after);report['default_unchanged']=before==after
  pg=[r for r in report['cases']if r['family']=='postgres'];sqlite=[r for r in report['cases']if r['family']=='sqlite'];report['counts']={'planned_total':13,'actual_Main_starts':len(report['native_cids']),'captured_cells':sum(r.get('state')=='captured'for r in report['cases']),'expected_native_behavior_checks_passed':sum(bool(r['expected_match'])for r in report['cases']),'expected_native_behavior_checks_total':13,'PostgreSQL_Source_compatibility_matched':sum(bool(r.get('compatibility_match'))for r in pg),'PostgreSQL_Source_compatibility_total':12,'SQLite_required_parity_matched':False,'SQLite_unsupported_guard_verified':len(sqlite)==1 and sqlite[0]['expected_match']}
  report['passed']=len(report['cases'])==13 and len(report['native_cids'])==13 and all(r['expected_match']for r in report['cases'])and report['default_unchanged']
 except BaseException as error:report['error']={'type':type(error).__name__,'message':str(error)}
 finally:
  cleaning=True;cleanup_deadline=time.monotonic()+90
  try:
   dc('down','--volumes','--remove-orphans','--timeout','10',seconds=35);inventories=[]
   for family,fmt in [('container','{{.ID}}'),('network','{{.ID}}'),('volume','{{.Name}}')]:
    raw,_=cmd([family,'ls',*(['--all']if family=='container'else[]),'--filter','label=com.docker.compose.project='+project,'--format',fmt]);inventories.append({'family':family,'empty':not raw.strip()})
   for exact in ([source_cid]if source_cid else[])+report['native_cids']:
    raw,_=cmd(['container','ls','--all','--no-trunc','--filter','id='+exact,'--format','{{.ID}}']);inventories.append({'exact_cid':exact,'empty':not raw.strip()})
   report['independent_absence']=inventories;report['exact_cleanup']=all(r['empty']for r in inventories)
  except BaseException as error:report['cleanup_error']={'type':type(error).__name__,'message':str(error)}
  gone=[]
  for c in report['commands']:
   try:current=Path('/proc',str(c['pid']),'stat').read_text().rsplit(')',1)[1].split()[19]
   except FileNotFoundError:current=None
   gone.append({'pid':c['pid'],'observed_starttime':c['observed_starttime'],'current_starttime':current,'original_absent':current!=c['observed_starttime']})
  report['host_generations']=gone;report['all_host_generations_absent']=all(r['original_absent']for r in gone);report['actual_controller_exit']=0 if report['passed']and report.get('exact_cleanup')and report['all_host_generations_absent']else 2;report['full_migration_accepted']=False;report['unmeasured_modes']=['Allow EOF/IO/multi-address branches','Prefer CA/fallback branches'];save(out/'report.json',report)
 return report['actual_controller_exit']

def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--execute-batch',action='store_true');p.add_argument('--image');p.add_argument('--proof',type=Path);p.add_argument('--git');p.add_argument('--build-manifest',type=Path);p.add_argument('--source-map',type=Path);p.add_argument('--output',type=Path);a=p.parse_args();plan=definition()
 if not a.execute_batch:print(json.dumps(plan,indent=2));return 0
 return execute(a,plan)
if __name__=='__main__':raise SystemExit(main())
