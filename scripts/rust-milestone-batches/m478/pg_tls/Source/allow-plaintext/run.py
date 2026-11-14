import importlib.util,json,hashlib,os,shutil,time,uuid,sys
from pathlib import Path
ROOT=Path('/home/justin/github/Dispatcharr');OUT=Path(__file__).resolve().parent;PRIOR=ROOT/'artifacts/rust/m478-source-normal-wsgi-cache-1';IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
spec=importlib.util.spec_from_file_location('bounded',ROOT/'artifacts/rust/m4-m7-m8-wide-batch-1/prerequisites/assets/bounded_cli.py');cli=importlib.util.module_from_spec(spec);spec.loader.exec_module(cli)
def save(p,v):p.write_text(json.dumps(v,indent=2,allow_nan=False)+'\n')
def need(v,label):
 if not v:raise ValueError(label)
project='dispatcharr-pgallow-'+uuid.uuid4().hex[:10];db='rust_probe_pluginrepo_'+uuid.uuid4().hex[:10]
report={'project':project,'database':db,'source_image':IMAGE,'commands':[],'cases':[],'Source_API_calls':0,'performance_claims':False,'private_keys':'ONLY owned Docker tls volume; never copied to host','passed':False};save(OUT/'report.json',report)
start=time.monotonic();serial=0;cleanup=False

def cmd(args,seconds=30,check=True):
 global serial
 need(serial<100,'CommandBound100');serial+=1
 raw,err,row=cli.collect(['docker','--host','unix:///tmp/dispatcharr-docker.sock',*args],min(seconds,max(.01,(start+900)-time.monotonic())),2097152)
 (OUT/f'command-{serial:02d}.stdout').write_bytes(raw);(OUT/f'command-{serial:02d}.stderr').write_bytes(err);report['commands'].append(row);save(OUT/'report.json',report)
 need(row['capture_complete'] and not row['forced_termination'],'ActualWaitAndPipes')
 need(not check or row['actual_wait_exit']==0,'ActualCommand0')
 return raw,row
compose=['compose','-p',project,'-f',str(OUT/'compose.json')]
def dc(*args,seconds=30,check=True):return cmd(compose+list(args),seconds,check)
for name in ['bootstrap_env.py','native_state.py','public_manifest.py','redis_channels.py','start.sh']:shutil.copyfile(PRIOR/'tools'/name,OUT/'tools'/name)
shutil.copytree(PRIOR/'inputs/pg-init',OUT/'pg-init');shutil.copytree(PRIOR/'schema',OUT/'schema')
spec=importlib.util.spec_from_file_location('environment',OUT/'tools/bootstrap_env.py');envmod=importlib.util.module_from_spec(spec);spec.loader.exec_module(envmod)
env=envmod.storage_environment();source=ROOT/'artifacts/rust/m4-m7-m8-wide-batch-run-2-source16/source'
identity={str(p.relative_to(source)):hashlib.sha256(p.read_bytes()).hexdigest() for p in [source/'dispatcharr/settings.py',source/'dispatcharr/db/process_label.py',source/'dispatcharr/db/backends/postgresql_psycopg3/base.py']};save(OUT/'source-inputs.json',identity)
service={'image':IMAGE,'pull_policy':'never','network_mode':'none','entrypoint':['bash','/native-tools/start.sh'],'environment':env,'volumes':[str(OUT/'tools')+':/native-tools:ro',str(OUT/'schema')+':/native-source:ro',str(OUT/'pg-init')+':/pg-bootstrap:ro',str(source)+':/app:ro',str(OUT)+':/evidence','tls:/tls','socket:/var/run/postgresql'],'tmpfs':['/data:mode=1777,size=2147483648','/tmp:mode=1777,size=268435456'],'cpus':1,'mem_limit':'1536m','pids_limit':256}
save(OUT/'compose.json',{'services':{'source':service},'volumes':{'tls':{},'socket':{}}})
base={'POSTGRES_DB':db,'POSTGRES_HOST':'127.0.0.1','POSTGRES_SSL':'true','POSTGRES_SSL_MODE':'verify-full','POSTGRES_SSL_CA_CERT':'/tls/ca.pem','POSTGRES_SSL_CERT':'/tls/client.pem','POSTGRES_SSL_KEY':'/tls/client-root.key','DJANGO_SETTINGS_MODULE':'dispatcharr.settings','DJANGO_SECRET_KEY':'owned-pg-tls-context-only','PYTHONPATH':'/app','PYTHONDONTWRITEBYTECODE':'1','PGCONNECT_TIMEOUT':'3','HOME':'/tmp'}
cases=[('valid_verify_full',{},True),('wrong_ca',{'POSTGRES_SSL_CA_CERT':'/tls/wrong-ca.pem'},False),('missing_ca_path',{'POSTGRES_SSL_CA_CERT':'/tls/missing.pem'},False),('hostname_mismatch',{'POSTGRES_HOST':'localhost'},False),('verify_ca_mismatch',{'POSTGRES_HOST':'localhost','POSTGRES_SSL_MODE':'verify-ca'},True),('no_client_cert',{'POSTGRES_SSL_CERT':'','POSTGRES_SSL_KEY':''},False),('require_wrong_ca',{'POSTGRES_SSL_MODE':'require','POSTGRES_SSL_CA_CERT':'/tls/wrong-ca.pem'},False),('unix_socket_ssl_true',{'POSTGRES_HOST':'/var/run/postgresql'},True),('ipv6_verify_ca',{'POSTGRES_HOST':'::1','POSTGRES_SSL_MODE':'verify-ca'},True),('source_allow',{'POSTGRES_SSL_MODE':'allow'},True),('source_prefer',{'POSTGRES_SSL_MODE':'prefer'},True),('sqlite',{'DB_ENGINE':'sqlite'},True)]
cases=[('source_allow_plaintext_preference',{'POSTGRES_SSL_MODE':'allow'},True)]
save(OUT/'case-definitions.json',[{'name':n,'changes':e,'expected_source_connection':ok}for n,e,ok in cases]);cid=None
try:
 need(not cmd(['container','ls','--all','--filter','label=com.docker.compose.project='+project,'--format','{{.ID}}'])[0].strip(),'FreshOwnedProject')
 dc('up','-d','--no-build','--pull','never',seconds=120)
 end=time.monotonic()+150
 while not (OUT/'bootstrap-ready').exists():need(time.monotonic()<end,'Bootstrap150s');time.sleep(.1)
 cid=dc('ps','--all','-q','source')[0].decode().strip();report['source_cid']=cid;save(OUT/'report.json',report)
 save(OUT/'actual-source-container.json',json.loads(cmd(['inspect',cid])[0]))
 dc('exec','-T','source','psql','-XAtq','-h','/var/run/postgresql','-U','dispatch','-d','postgres','-v','ON_ERROR_STOP=1','-c','CREATE DATABASE '+db+' TEMPLATE dispatcharr')
 before=json.loads(dc('exec','-T','source','/dispatcharrpy/bin/python','-B','/native-tools/state.py')[0]);save(OUT/'default-before.json',before)
 dc('exec','-T','source','bash','/native-tools/tls.sh',seconds=40)
 for name,changes,expected in cases:
  appenv=base|changes|{'TLS_CASE':name};args=['exec','-T']
  for k,v in appenv.items():args+=['-e',k+'='+v]
  raw,status=dc(*args,'source','/dispatcharrpy/bin/python','-B','/native-tools/probe.py',seconds=12,check=False)
  lines=[line.removeprefix('TLS_RESULT=')for line in raw.decode().splitlines()if line.startswith('TLS_RESULT=')]
  record={'name':name,'expected_source_connection':expected,'command_exit':status['actual_wait_exit'],'actual':json.loads(lines[0])if len(lines)==1 else None};record['expected_match']=record['actual']is not None and record['actual']['connected']==expected
  report['cases'].append(record);save(OUT/'report.json',report)
 after=json.loads(dc('exec','-T','source','/dispatcharrpy/bin/python','-B','/native-tools/state.py')[0]);save(OUT/'default-after.json',after);report['default_unchanged']=before==after
 report['passed']=all(x['expected_match']for x in report['cases'])and report['default_unchanged']
except BaseException as e:report['error']={'type':type(e).__name__,'message':str(e)}
finally:
 try:
  dc('down','--volumes','--remove-orphans','--timeout','10',seconds=35)
  inventories=[]
  for family,fmt in [('container','{{.ID}}'),('network','{{.ID}}'),('volume','{{.Name}}')]:
   raw,_=cmd([family,'ls',*(['--all']if family=='container'else[]),'--filter','label=com.docker.compose.project='+project,'--format',fmt]);inventories.append({'family':family,'empty':not raw.strip()})
  if cid:
   raw,_=cmd(['container','ls','--all','--no-trunc','--filter','id='+cid,'--format','{{.ID}}']);inventories.append({'exact_cid':cid,'empty':not raw.strip()})
  report['independent_absence']=inventories;report['exact_cleanup']=all(x['empty']for x in inventories)
 except BaseException as e:report['cleanup_error']={'type':type(e).__name__,'message':str(e)}
 gone=[]
 for row in report['commands']:
  try:current=Path('/proc',str(row['pid']),'stat').read_text().rsplit(')',1)[1].split()[19]
  except FileNotFoundError:current=None
  gone.append({'pid':row['pid'],'observed_starttime':row['observed_starttime'],'current_starttime':current,'original_absent':current!=row['observed_starttime']})
 report['host_generations']=gone;report['all_host_generations_absent']=all(x['original_absent']for x in gone);report['actual_controller_exit']=0 if report['passed']and report.get('exact_cleanup')and report['all_host_generations_absent']else 2;save(OUT/'report.json',report)
sys.exit(report['actual_controller_exit'])
