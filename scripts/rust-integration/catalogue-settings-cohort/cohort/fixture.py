"""Owned Compose adapter for the ONE normal DVR workflow. No app on import.
Original Dispatcharr/dependency authors retain all application/algorithm credit.
Existing playback Compose/bootstrap/schema/seed/origin are reused without builds.
"""
import contextlib,datetime,hashlib,json,os,re,shutil,signal,socket,subprocess,sys,time,uuid
from pathlib import Path
from workflow import Unsupported,save,utc
from budget import LeaseBudget,LeaseExpired,bounded_read
ROOT=Path(__file__).resolve().parents[4]
IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
ORIGINAL='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
class NativeStartupFailure(Exception):
 def __init__(self,code,common):
  self.common_candidate_startup_failure=common
  super().__init__('ActualNativeStartupExit:'+str(code)+('; commonregistry'if common else''))
class Fixture:
 def __init__(self,binding,backend,context):
  self.binding=binding;self.backend=backend;self.context=context;self.budget=LeaseBudget(context['deadline_monotonic']);self.budget.remaining(1)
  self.output=Path(binding['fixture_output'])/(backend+'-'+uuid.uuid4().hex[:8]);self.output.mkdir(parents=True,exist_ok=False)
  self.project='dispatcharr-normal-dvr-'+uuid.uuid4().hex[:10];self.serial=0;self.roles={};self.native=None;self.containers=[];self.cli=[];self.failure=None;self._cleanup=None;self.recordings={};self.max_segments=0;self.case=None;self.maximum_by_recording={};self._deleted=set();self._sampler_stop=None;self._sampler=None;self.viewer=None
  self.environment={k:v for k,v in os.environ.items()if not k.startswith(('GIT_','COMPOSE_','DATABASE_','POSTGRES_','REDIS_','DJANGO_','CELERY_','FIXTURE_','BENCHMARK_'))}
  self.data=self.output/'data';self.data.mkdir(mode=0o755);self.data.chmod(0o755);self.tools=Path(__file__).resolve().parent
  self.functional_pids=int(binding['functional_pids_limit'])
  if self.functional_pids!=1600:raise ValueError('DeclaredPairedCoreFunctionalPids1600Required')
  self.support=Path(binding['support_root']).resolve();self.docker=binding['docker'];self.compose=None;self.admin=None
 def remaining(self,seconds):return self.budget.remaining(seconds)
 def functional_budget_expired(self):return self.budget.expired()
 def begin_cleanup(self):self.budget.begin_cleanup()
 def sleep(self,seconds):
  delay=self.remaining(seconds);time.sleep(delay)
  if delay<seconds:self.remaining(1)
 def command(self,argv,seconds=60,body=None,secret=False,allow=False):
  seconds=self.remaining(seconds)
  if body is not None:raise ValueError('NoUnboundedInputPipe')
  # Reuse the existing global runner's bounded pipe/PG-settlement primitive;
  # this is a command helper, never a nested fixture scheduler/pool.
  import importlib.util
  spec=importlib.util.spec_from_file_location('normal_dvr_global_command',ROOT/'scripts/rust-milestone-batches/coordinator.py');module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
  self.serial+=1;label='command-'+str(self.serial).zfill(4)
  directory=self.output
  if secret:directory=self.output/'private';directory.mkdir(mode=0o700,exist_ok=True)
  bound={'cwd':str(self.support),'environment':self.environment,'strip_inherited_prefixes':['GIT_','COMPOSE_','DATABASE_','POSTGRES_','REDIS_','DJANGO_','CELERY_','FIXTURE_','BENCHMARK_'],'output_bytes':1048576}
  receipt=module.command(argv,bound,directory,label,seconds)
  receipt['argv_sha256']=hashlib.sha256(json.dumps(argv).encode()).hexdigest();receipt['secret_output_excluded']=secret
  out=Path(receipt['stdout']).read_bytes();err=Path(receipt['stderr']).read_bytes()
  receipt.update(stdout_sha256=hashlib.sha256(out).hexdigest(),stderr_sha256=hashlib.sha256(err).hexdigest())
  save(self.output/(label+'.json'),receipt);self.cli.append(receipt)
  if receipt['failure']or not receipt['process_group_settled']or(receipt['actual_exit']and not allow):raise RuntimeError(('ActualCommand',receipt['actual_exit'],receipt['failure'],label))
  return out.decode('utf-8')
 @staticmethod
 def pid_start(pid):
  try:return Path('/proc',str(pid),'stat').read_text().rsplit(')',1)[1].split()[19]
  except FileNotFoundError:return None
 def python(self,script,seconds=40,secret=False):
  args=self.compose+['exec','-T']
  if self.case:args+=['-e','POSTGRES_DB='+self.database]
  args+=['baseline','/dispatcharrpy/bin/python','-c',script]
  return self.command(args,seconds,secret=secret)
 def sql(self,script,secret=False):
  pre="import os,sys,json;from urllib.parse import quote;password=quote(os.environ['REDIS_PASSWORD'],safe='');os.environ['CELERY_BROKER_URL']='redis://:'+password+'@localhost:6379/1';os.environ['CELERY_RESULT_BACKEND']='redis://:'+password+'@localhost:6379/2';sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings');import django;django.setup();from django.db import connection;from apps.channels.models import Recording;\n"
  return json.loads(self.python(pre+script,secret=secret))
 def redis(self,db,script,*values):
  return json.loads(self.command(self.compose+['exec','-T','baseline','redis-cli','-n',str(db),'--json','EVAL',script,'0',*map(str,values)],20))
 def wait(self,fn,seconds,label):
  until=time.monotonic()+self.remaining(seconds);last=None;diagnosed=False
  while time.monotonic()<until:
   self.remaining(1)
   if self.case=='auto-sync'and seconds>=20 and not diagnosed and until-time.monotonic()<=18:
    diagnosed=True;self.auto_failure_snapshot(label+'-pretimeout')
   try:
    value=fn()
    if value:return value
   except (OSError,ValueError,RuntimeError)as error:last=type(error).__name__
   time.sleep(min(.5,max(0,until-time.monotonic())))
  self.auto_failure_snapshot(label+'-wait-deadline')
  raise TimeoutError(label+':'+str(last))
 def auto_failure_snapshot(self,label):
  if self.case!='auto-sync':return
  # Observations only: bounded commands and no query text, raw frame/token or
  # backend values. Never replace the primary application/assertion failure.
  result={'scope':'actual bounded pre-teardown auto state','label':label,'backend':self.backend}
  try:
   self.remaining(15)
   script="import psycopg,json; c=psycopg.connect(host='127.0.0.1',dbname="+repr(self.database)+",user='dispatch',password='playback-fixture-only',connect_timeout=2,options='-c statement_timeout=2000');cur=c.cursor();cur.execute(\"SELECT pid,state,wait_event_type,wait_event,md5(query) FROM pg_stat_activity WHERE datname=current_database() ORDER BY pid LIMIT 24\");print(json.dumps(cur.fetchall()));c.close()"
   result['actual_PG_activity']=json.loads(self.command(self.compose+['exec','-T','baseline','/dispatcharrpy/bin/python','-c',script],3,secret=True))
   result['actual_queue_lengths']=json.loads(self.command(self.compose+['exec','-T','baseline','redis-cli','-n','1','--json','EVAL',"return {redis.call('LLEN','celery'),redis.call('LLEN','dvr'),redis.call('HLEN','unacked'),redis.call('ZCARD','unacked_index')}",'0'],3))
   if self.backend=='Native'and self.native:
    logs=self.command([self.docker,'logs','--tail','96',self.native],3,secret=True,allow=True)
    receipt=self.cli[-1];raw=logs+'\n'+Path(receipt['stderr']).read_text(errors='replace')
    result['actual_stage_lines']=[line for line in raw.splitlines()if re.fullmatch(r'rust_auto_stage task=[0-9a-f-]{36} stage=[A-Za-z0-9_:-]{1,96}',line)][-32:]
    code,body=self.get(self.base_url+'/health/ready');result['actual_Main_readiness']={'status':code,'body_sha256':hashlib.sha256(body).hexdigest()}
  except Exception as error:result['diagnostic_failure']=type(error).__name__
  save(self.output/('auto-diagnostic-'+uuid.uuid4().hex[:8]+'.json'),result)
 def endpoint(self,service,port):return 'http://'+self.command(self.compose+['port',service,str(port)]).strip()
 def get(self,url):
  import urllib.request,urllib.error
  try:r=urllib.request.urlopen(url,timeout=self.remaining(5))
  except urllib.error.HTTPError as e:r=e
  with r:return r.status,bounded_read(r,1048576,self.remaining)
 def bootstrap(self):
  self.remaining(1)
  archive=Path(self.binding['Original_archive']);assert hashlib.sha256(archive.read_bytes()).hexdigest()=='f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff'
  existing=self.binding.get('Original_source_root');self.source=Path(existing).resolve()if existing else self.output/'Original'
  if not self.source.exists():
   self.source.parent.mkdir(parents=True,exist_ok=True);self.source.mkdir(mode=0o755);self.source.chmod(0o755);self.command(['tar','-xf',str(archive),'-C',str(self.source),'--no-same-owner'],30)
   for p in self.source.rglob('*'):
    if p.is_dir():p.chmod(0o755)
    elif p.is_file():p.chmod(0o444|(p.stat().st_mode&0o111))
   (self.source/'media').mkdir(exist_ok=True)
  for name,h in self.binding['Original_source_sha256'].items():
   self.remaining(1)
   if hashlib.sha256((self.source/name).read_bytes()).hexdigest()!=h:raise ValueError('ExactOriginalSource:'+name)
  # Preserve old private-mode support captures. Stage only the two tiny,
  # hash-bound guest tooling directories once, readable by actual UID65532.
  readable=self.source.parent/'readable-support'
  for sub in ['rust-playback-baseline','rust-baseline']:
   target=readable/sub
   if not target.exists():
    target.parent.mkdir(parents=True,exist_ok=True);shutil.copytree(self.support/'scripts'/sub,target)
    for entry in [target,*target.rglob('*')]:
     if entry.is_dir():entry.chmod(0o755)
     elif entry.is_file():entry.chmod(0o444|(entry.stat().st_mode&0o111))
   for original in(self.support/'scripts'/sub).rglob('*'):
    if original.is_file()and '__pycache__'not in original.parts:
     if hashlib.sha256(original.read_bytes()).digest()!=hashlib.sha256((target/original.relative_to(self.support/'scripts'/sub)).read_bytes()).digest():raise ValueError('ReadableSupportCopyMismatch')
  self.environment.update(BASELINE_SOURCE_DIR=str(self.source),BASELINE_EVIDENCE_DIR=str(self.output),FIXTURE_SECONDS='3600'if self.binding.get('positive_comskip_epochs')else'1800',FIXTURE_CODEC='h264')
  sys.path.insert(0,str(self.support/'scripts/rust-integration'));from fixture_ports import write_port_overlay
  ports=self.output/'ports.json';write_port_overlay(ports,{'baseline':[5656,9192],'origin':[8080]})
  overlay={'services':{'baseline':{'environment':{'PUID':'65532','PGID':'65532'},'volumes':[str(self.data)+':/shared',str(self.tools)+':/normal-dvr-tools:ro',str(readable/'rust-playback-baseline')+':/verification:ro',str(readable/'rust-baseline')+':/baseline-tools:ro'],'cpus':2,'mem_limit':'3g','pids_limit':self.functional_pids},'origin':{'volumes':[str(readable/'rust-playback-baseline')+':/verification:ro'],'cpus':1,'mem_limit':'512m','pids_limit':128}},'networks':{'default':{'internal':False}}};
  plugin_octet='252'if self.backend=='Native'else'251'
  overlay['networks']['default']['ipam']={'config':[{'subnet':'11.254.'+plugin_octet+'.0/29'}]}
  overlay['services']['origin']['networks']={'default':{'ipv4_address':'11.254.'+plugin_octet+'.3'}}
  overlay['services']['baseline']['networks']={'default':{'ipv4_address':'11.254.'+plugin_octet+'.2'}}
  overlay['services']['origin']['volumes'].append(str(self.tools/'origin.py')+':/verification/origin.py:ro')
  save(self.output/'compose.json',overlay)
  self.compose=[self.docker,'compose','-p',self.project,'-f',str(self.support/'docker/compose.rust-playback.yml'),'-f',str(ports),'-f',str(self.output/'compose.json')]
  self.command(self.compose+['up','-d','--no-build','baseline','origin'],240)
  self.baseline=self.endpoint('baseline',5656);self.origin=self.endpoint('origin',8080)
  self.wait(lambda:(self.output/'seed.json').is_file()and self.get(self.origin+'/health')[0]==200,240,'actual-bootstrap')
  self.seed=json.loads((self.output/'seed.json').read_text());row=self.seed['channels'][0];self.channel_id=row['channel_pk'];self.channel_uuid=row['channel_uuid']
  self.channel_name=self.sql("from apps.channels.models import Channel;print(json.dumps(Channel.objects.get(pk="+str(self.channel_id)+").name))")
  self.command(self.compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/verification/supervisor.py','control','stop'],30)
  self.capture_containers();self.command(self.compose+['exec','-T','baseline','redis-cli','FLUSHALL'],20)
  # Seed authenticated users ONCE using the existing actual seed. Credentials
  # remain only in memory; never serialized as public request/command evidence.
  self.accounts=json.loads(self.command(self.compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/integration/seed_accounts.py'],secret=True))if self.binding.get('integration_mounted')else None
  if self.accounts is None:
   script=(self.support/'scripts/rust-integration/seed_accounts.py').read_text();self.accounts=json.loads(self.python(script,secret=True))
  self.admin=self.accounts['admin']['access']
  # Actual default profile, one upstream/provider slot; no custom fake media.
  self.provider=self.sql("from apps.m3u.models import M3UAccount;from core.models import CoreSettings; a=M3UAccount.get_custom_account();p=a.profiles.get(is_default=True);p.max_streams=1;p.save(update_fields=['max_streams']);s,_=CoreSettings.objects.get_or_create(key='dvr_settings',defaults={'name':'DVR Settings','value':{}});s.value={'pre_offset_minutes':0,'post_offset_minutes':0,'comskip_enabled':False};s.save();print(json.dumps({'profile':p.pk}))")['profile']
  self.command(self.compose+['exec','-T','baseline','redis-cli','FLUSHALL'],20)
 def capture_containers(self):
  ids=self.command([self.docker,'container','ls','--all','--filter','label=com.docker.compose.project='+self.project,'--format','{{.ID}}']).split()
  for cid in ids:
   detail=json.loads(self.command([self.docker,'inspect',cid]))[0]
   if detail['Id']not in [x['Id']for x in self.containers]:self.containers.append({'Id':detail['Id'],'pid':detail['State']['Pid'],'starttime':self.pid_start(detail['State']['Pid']),'image':detail['Image']})
 def role(self,name,args,stop=False,extra=None):
  if stop:
   if name in self.roles:
    record=self.roles.pop(name);response=json.loads(self.python("import json,sys;sys.path.insert(0,'/normal-dvr-tools');from role_owner import stop;print(json.dumps(stop("+repr(record)+")))",seconds=70));save(self.output/('role-'+name+'-stop-'+uuid.uuid4().hex[:5]+'.json'),response)
    retired=getattr(self,'_retired_role_logs',[])
    if len(retired)>=32:raise RuntimeError('StoppedRoleLogRecordBudget')
    retired.append((name,response));self._retired_role_logs=retired
    self.collect_role_logs(name,response)
    if not(response['all_original_process_generations_absent']and response['actual_exit']==0 and response['stdout_EOF']and response['stderr_EOF']and not response['forced']and not response['failures']):raise RuntimeError('ActualRoleNotRetired:'+name)
    return response
   return None
  environment={'POSTGRES_DB':self.database,'CELERY_BROKER_URL':'redis://:playback-fixture-only@localhost:6379/1','CELERY_RESULT_BACKEND':'redis://:playback-fixture-only@localhost:6379/2','DISPATCHARR_INTERNAL_TS_BASE_URL':'http://127.0.0.1:5656','DISPATCHARR_PLUGINS_DIR':'/data/plugins','C_FORCE_ROOT':'true'}
  if name in('web','celery','dvr','beat'):environment.update(getattr(self,'_provider_environment',{}))
  if name in('web','celery','dvr','beat'):environment.update(getattr(self,'_plugin_environment',{}))
  environment.update(extra or {})
  script="import json,sys;sys.path.insert(0,'/normal-dvr-tools');from role_owner import start;print(json.dumps(start("+repr(name)+","+repr(args)+","+repr(environment)+")))"
  record=json.loads(self.python(script,secret=bool(extra)or bool(getattr(self,'_provider_environment',{}))));self.roles[name]=record;save(self.output/('role-'+name+'-start-'+uuid.uuid4().hex[:5]+'.json'),record)
  if name in('beat','dvr','celery'):
   def actual_term_handler():
    script="import json;from pathlib import Path;p=Path('/proc',"+repr(str(record['pid']))+");stat=(p/'stat').read_text().rsplit(')',1)[1].split();status=dict(v.split(':',1)for v in(p/'status').read_text().splitlines()if ':'in v);print(json.dumps({'same_generation':stat[19]=="+repr(record['starttime'])+",'TERM_handler_installed':bool(int(status['SigCgt'].strip(),16)&(1<<14))}))"
    observed=json.loads(self.python(script));return observed if observed['same_generation']and observed['TERM_handler_installed']else False
   proof=self.wait(actual_term_handler,30,'actual-Source-'+name+'-SIGTERM-handler-ready')
   if name in('celery','dvr'):
    hostname='normal-default@owned'if name=='celery'else'normal-dvr@owned'
    def actual_intake_ping():
     script="import os,json;os.environ['CELERY_BROKER_URL']='redis://:playback-fixture-only@localhost:6379/1';from dispatcharr.celery import app;r=app.control.inspect(destination=["+repr(hostname)+"],timeout=1).ping();print(json.dumps(r))"
     response=json.loads(self.python(script));return response if response and response.get(hostname)=={'ok':'pong'}else False
    proof['actual_intake_ping']=self.wait(actual_intake_ping,30,'actual-Source-'+name+'-control-ping-ready')
   else:
    def actual_beat_pidfile():
     return self.python("from pathlib import Path;p=Path('/tmp/normal-dvr-beat.pid');print(p.read_text().strip()if p.exists()else'')").strip()==str(record['pid'])
    proof['actual_beat_pidfile_matches']=self.wait(actual_beat_pidfile,30,'actual-Source-Beat-pidfile-ready')
   save(self.output/('role-'+name+'-signal-ready-'+uuid.uuid4().hex[:5]+'.json'),proof)
 def collect_role_logs(self,name,response):
  # Actual role stop flushed these owned tmpfs files; Docker cp cannot see them.
  # Failed media contexts need bounded PRIVATE diagnostic bytes before container
  # teardown. Public evidence remains hashes/counts/fixed signatures only.
  signatures=['Traceback','ModuleNotFoundError','Permission denied','PermissionError','OperationalError','ConnectionRefusedError','AuthenticationError','KeyError','ImportError','TimeoutError','Restoring']
  capture_media=name=='web'and self.case in('vlc-live','input-promotion','output-profile')
  for kind,path in response['log_paths'].items():
   capture_private=capture_media or(kind=='stderr'and getattr(self,'_context_failed',False))
   script="import pathlib,json,hashlib,base64;p=pathlib.Path("+repr(path)+");size=p.stat().st_size;\nif size>4194304:raise RuntimeError('ActualRoleLogBudget')\nb=p.read_bytes();text=b.decode('utf8',errors='replace');signatures="+repr(signatures)+";value={'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest(),'read_completed':len(b)==size,'raw_log_payload_copied':False,'known_signature_counts':{key:text.count(key)for key in signatures if key in text}};private="+repr(capture_private)+";\nif private:\n head=min(len(b),131072);tail=max(head,len(b)-131072);value['private_chunks']=[{'offset':0,'base64':base64.b64encode(b[:head]).decode()}]+([{'offset':tail,'base64':base64.b64encode(b[tail:]).decode()}]if tail<len(b)else[])\nprint(json.dumps(value))"
   try:
    observed=json.loads(self.python(script,secret=capture_private));label='role-'+self.case+'-'+name+'-'+kind+'-'+uuid.uuid4().hex[:8]
    if capture_private:
     import base64
     chunks=observed.pop('private_chunks');captured=b''.join(base64.b64decode(row['base64'],validate=True)for row in chunks)
     if len(captured)>262144:raise RuntimeError('ActualPrivateRoleCaptureBudget')
     private=self.output/'private'/(label+'.raw');private.write_bytes(captured);private.chmod(0o600)
     observed.update(raw_log_payload_copied=True,raw_log_scope='private first128KiB plus nonoverlapping last128KiB; exact offsets retained',private_capture_bytes=len(captured),private_capture_sha256=hashlib.sha256(captured).hexdigest(),private_capture_offsets=[{'offset':row['offset'],'bytes':len(base64.b64decode(row['base64'],validate=True))}for row in chunks],private_raw_path=str(private.relative_to(self.output)),complete_private_capture=len(captured)==observed['bytes'])
     if observed['complete_private_capture']and observed['private_capture_sha256']!=observed['sha256']:raise RuntimeError('ActualPrivateRoleHashMismatch')
    save(self.output/(label+'.json'),observed)
   except Exception as error:raise RuntimeError('ActualRoleLogTransportFailed:'+name+':'+kind)from error
 def start_original(self,consumer=True,dvr_queue='dvr'):
  self.role('web',['uwsgi','--ini','/verification/uwsgi.ini']);self.base_url=self.baseline
  self.wait(lambda:self.get(self.base_url+'/api/channels/recordings/')[0]in(401,403),60,'OriginalHTTP-serving')
  if consumer:self.start_original_workers(dvr_queue=dvr_queue)
 def start_original_workers(self,dvr_queue='dvr'):
  base=['/dispatcharrpy/bin/celery','-A','dispatcharr']
  default=base+['worker','--pool=prefork','--concurrency=1','-Q','celery','--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING','--hostname=normal-default@owned']
  if self.case=='m8-actions':default+=['--include','owned_m8_actions.plugin']
  self.role('celery',default,extra={'PYTHONPATH':'/app:/data/plugins'}if self.case=='m8-actions'else None)
  self.role('dvr',base+['worker','--pool=threads','--concurrency=20','-Q',dvr_queue,'--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING','--hostname=normal-dvr@owned'])
  self.role('beat',base+['beat','--scheduler','django_celery_beat.schedulers:DatabaseScheduler','--max-interval','1','--pidfile','/tmp/normal-dvr-beat.pid','--schedule','/tmp/normal-dvr-beat','--loglevel=WARNING'])
 def begin_original_web_stop(self):
  if self.backend!='Original'or'web'not in self.roles:raise ValueError('ActualOwnedOriginalWebRequired')
  record=self.roles['web'];value=json.loads(self.python("import json,sys;sys.path.insert(0,'/normal-dvr-tools');from role_owner import begin_stop;print(json.dumps(begin_stop("+repr(record)+")))"))
  save(self.output/'actual-web-begin-stop.json',value)
  if not value['shutdown_started']or not value['signal_settled']or not value['actual_master_SIGTERM_sent']:raise RuntimeError('ActualSourceMasterSIGTERMNotSent')
  return value
 def stop_roles(self):
  errors=[]
  for name in dict.fromkeys(['beat','dvr','celery','web',*self.roles]):
   try:self.role(name,[],stop=True)
   except Exception as error:errors.append({'role':name,'type':type(error).__name__,'message':str(error)[:512]})
  if errors:raise RuntimeError(('ActualRoleOrLogCaptureFailures',errors))
 def start_native(self):
  name=self.project+'-native-'+uuid.uuid4().hex[:5];self.native=name
  env={'DATABASE_URL':'postgresql://dispatch:playback-fixture-only@127.0.0.1:5432/'+self.database,'DJANGO_SECRET_KEY':'playback-fixture-only','REDIS_URL':'redis://:playback-fixture-only@127.0.0.1:6379/0','CELERY_BROKER_URL':'redis://:playback-fixture-only@127.0.0.1:6379/1','CELERY_RESULT_BACKEND':'redis://:playback-fixture-only@127.0.0.1:6379/2','RUST_BIND':'0.0.0.0:9192','RUST_EXPERIMENTAL_SELECTED_JOBS':'1','RUST_EXPERIMENTAL_SELECTED_BEAT':'1','RUST_DATABASE_POOL_SIZE':'8','DISPATCHARR_PLUGINS_DIR':'/data/plugins','RUST_SHUTDOWN_SECONDS':'45','TOKIO_WORKER_THREADS':'2','TMPDIR':'/data','DISPATCHARR_TRUSTED_PROXIES':'none','RUST_AUTO_STAGE_DIAGNOSTICS':'1'}
  env['RUST_INPUT_PROCESS_DIAGNOSTICS']='1'
  env.update(getattr(self,'_provider_environment',{}))
  env.update(getattr(self,'_plugin_environment',{}))
  argv=[self.docker,'run','-d','--name',name,'--network','container:'+self.project+'-baseline-1','--label','com.docker.compose.project='+self.project,'--read-only','--cap-drop','ALL','--security-opt','no-new-privileges:true','--cpus','2','--memory','2g','--pids-limit',str(self.functional_pids),'-v',str(self.case_data)+':/data']
  for k,v in env.items():argv+=['-e',k+'='+v]
  argv+=[self.binding['candidate_image_id']];cid=self.command(argv,secret=bool(getattr(self,'_provider_environment',{}))).strip();info=json.loads(self.command([self.docker,'inspect',cid],secret=bool(getattr(self,'_provider_environment',{}))))[0];assert info['Image']==self.binding['candidate_image_id'];mounts=info['Mounts'];assert len(mounts)==1 and mounts[0]['Destination']=='/data'and Path(mounts[0]['Source']).resolve()==self.case_data.resolve();public_info=json.loads(json.dumps(info));public_info['Config']['Env']=[item.split('=',1)[0]+'=<private fixture key>'if item.split('=',1)[0]in('TMDB_API_KEY','OMDB_API_KEY')else item for item in public_info['Config']['Env']];save(self.output/(name+'-inspection.json'),public_info);self.containers.append({'Id':info['Id'],'pid':info['State']['Pid'],'starttime':self.pid_start(info['State']['Pid']),'image':info['Image']})
  self.base_url=self.endpoint('baseline',9192);checked=[0.0]
  def actual_serving():
   now=time.monotonic()
   if now-checked[0]>=1:
    checked[0]=now;state=json.loads(self.command([self.docker,'inspect','--format','{{json .State}}',name]))
    if not state['Running']:
     self.command([self.docker,'logs',name],allow=True,secret=True);receipt=self.cli[-1];stderr=Path(receipt['stderr']).read_bytes();common=b'Selected complete registry initialization failed'in stderr
     save(self.output/(name+'-startup-failure.json'),{'State':state,'stderr_sha256':hashlib.sha256(stderr).hexdigest(),'common_registry_failure':common,'raw_private_receipt':receipt['stderr']})
     raise NativeStartupFailure(state['ExitCode'],common)
   return self.get(self.base_url+'/health/ready')[0]==200
  self.wait(actual_serving,90,'NativeHTTP-serving')
 def stop_native(self,expected=0,seconds=55):
  if not self.native:return None
  name=self.native;self.command([self.docker,'stop','--time',str(seconds),name],seconds+15,allow=True);info=json.loads(self.command([self.docker,'inspect',name]))[0];logs=self.command([self.docker,'logs',name],allow=True,secret=True);(self.output/'private'/(name+'.logs')).write_text(logs);result={'actual_exit':info['State']['ExitCode'],'oom_killed':info['State']['OOMKilled'],'expected_exit':expected};save(self.output/(name+'-exit.json'),result)
  self.command([self.docker,'rm','-f',name]);self.native=None
  if expected is not None and result['actual_exit']!=expected:raise AssertionError(('ActualNativeExit',result))
  return result
 @contextlib.contextmanager
 def independent_context(self,name,*,start_consumers=True):
  self.remaining(1)
  self._provider_environment={}
  self._plugin_environment={}
  self._context_failed=False;self._retired_role_logs=[]
  self.case=name;self._acl_probe={};self.recordings={};self._deleted=set();self.maximum_by_recording={};self.case_data=self.data/name;self.case_data.mkdir(mode=0o755)
  for key in ['recordings','logos','plugins','logs','m3us','epgs','cached_m3u','cached_epg','cached_programs']:(self.case_data/key).mkdir(mode=0o755)
  self.database='normal_dvr_'+uuid.uuid4().hex[:16]
  # Bootstrap schema once; actual independent database/Redis/media namespace per
  # context. No producer/model function is replaced to manufacture a frame.
  self.python("import psycopg,os; c=psycopg.connect(dbname='postgres',user=os.environ['POSTGRES_USER'],password=os.environ['POSTGRES_PASSWORD'],host='/var/run/postgresql');c.autocommit=True;x=c.cursor();x.execute('CREATE DATABASE "+self.database+" WITH TEMPLATE dispatcharr');c.close()")
  self.command(self.compose+['exec','-T','baseline','redis-cli','FLUSHALL'],20)
  target='/shared/'+name
  self.python("import os,pathlib;root=pathlib.Path("+repr(target)+");[(os.chown(p,65532,65532),p.chmod(0o755))for p in [root,*root.iterdir()]];[(root/key).chmod(0o777)for key in ['m3us','epgs']];\nfor key in ['recordings','logos','plugins','m3us','epgs','cached_m3u','cached_epg','cached_programs']:\n p=pathlib.Path('/data')/key\n if p.is_symlink():p.unlink()\n elif p.exists():\n  if any(p.iterdir()):raise RuntimeError('UnownedSourceMediaPrefix')\n  p.rmdir()\n p.symlink_to(root/key,target_is_directory=True)\n")
  owned_octet='252'if self.backend=='Native'else'251';owned_cidr='11.254.'+owned_octet+'.0/29'
  actual_networks=json.loads(self.command([self.docker,'inspect','--format','{{json .NetworkSettings.Networks}}',self.project+'-baseline-1']))
  if not any(row.get('IPAddress')=='11.254.'+owned_octet+'.2'and row.get('Gateway')=='11.254.'+owned_octet+'.1'for row in actual_networks.values()):raise AssertionError('ActualOwnedFixtureNetworkRequired')
  # Real Source network policy explicitly admits only the inspected fixture
  # subnet. Preserve all preexisting ranges and later actual deny cells.
  # Extend only the exact owned fixture CIDR in the policy groups actually
  # used by this context. Source /output/epg uses M3U_EPG, distinct from STREAMS.
  policy_groups=['STREAMS']+(['M3U_EPG']if name in('schedules-direct','custom-guide-held')else[])
  policy=self.sql("from core.models import CoreSettings,NETWORK_ACCESS_KEY;s=CoreSettings.objects.get(key=NETWORK_ACCESS_KEY);v=dict(s.value);owned="+repr(owned_cidr)+";groups="+repr(policy_groups)+";observed={}\nfor key in groups:\n old=v.get(key,'');assert isinstance(old,str);parts=[p.strip()for p in old.split(',')if p.strip()];parts+=[]if owned in parts else[owned];v[key]=','.join(parts);observed[key]={'before':old,'after':v[key]}\ns.value=v;s.save();print(json.dumps({'key':NETWORK_ACCESS_KEY,'groups':observed,'old_STREAMS':observed['STREAMS']['before'],'actual_STREAMS':s.value['STREAMS'],'owned_fixture_cidr':owned}))")
  save(self.output/(name+'-actual-owned-network-policy.json'),policy)
  primary=None
  self._sampler_stop=None;self._sampler=None
  try:
   module=self.m6_module()
   if module is not None and hasattr(module,'prepare_context'):module.prepare_context(self,self.output)
   if self.case=='channel-maintenance':
    self.sql("from django_celery_beat.models import PeriodicTask,PeriodicTasks;from core.models import CoreSettings;n=PeriodicTask.objects.update(enabled=False);PeriodicTasks.update_changed();s,_=CoreSettings.objects.get_or_create(key='backup_settings',defaults={'name':'Backup Settings','value':{}});s.value={**(s.value or {}),'schedule_enabled':False};s.save();print(json.dumps({'default_beat_rows_disabled':n,'scope':'Native channel-only isolation'}))")
   if self.case=='normal-backup':self.normal_backup_module().prepare_context(self)
   if self.case=='recording-provider-artwork':self.provider_artwork_module().prepare_context(self)
   if self.case=='xc-live':self.xc_live_module().prepare_context(self)
   if self.case=='comskip-manual':
    import comskip_cases
    comskip_cases.prepare_context(self)
   if self.case=='live-vod-group-settings':
    import live_vod_settings_cases
    live_vod_settings_cases.prepare_context(self)
   if self.case=='vod-category-language-lifecycle':
    import vod_category_language_cases
    vod_category_language_cases.prepare_context(self)
   if self.case=='vod-catalogue-scale':
    import vod_catalogue_scale_cases
    vod_catalogue_scale_cases.prepare_context(self)
   if self.case in('vod-international-catalogue','vod-ordered-title-identity'):
    import vod_grouped_catalogue_cases
    vod_grouped_catalogue_cases.prepare_context(self)
   if self.case=='vod-external-identity':
    import vod_external_identity_cases
    vod_external_identity_cases.prepare_context(self)
   if self.case=='vod-shared-metadata':
    import vod_shared_metadata_cases
    vod_shared_metadata_cases.prepare_context(self)
   if self.case=='vod-dense-list':
    import vod_dense_list_cases
    vod_dense_list_cases.prepare_context(self)
   if self.case=='vod-series-advanced':
    import vod_series_advanced_cases
    vod_series_advanced_cases.prepare_context(self)
   if self.case=='vod-series-info':
    import vod_series_info_cases
    vod_series_info_cases.prepare_context(self)
   if self.case=='vod-series':
    import vod_series_cases
    vod_series_cases.prepare_context(self)
   if self.case=='vod-repeat':
    import vod_repeat_cases
    vod_repeat_cases.prepare_context(self)
   if self.case=='m3u-refresh-all':
    import m3u_refresh_all_cases
    m3u_refresh_all_cases.prepare_context(self)
   if self.case=='idle-viewing':self.idle_viewing_module().prepare_context(self)
   if self.case=='auto-sync':
    import auto_sync_cases,compact_cases,regex_cases
    auto_sync_cases.prepare_context(self);regex_cases.prepare_context(self);compact_cases.prepare_context(self)
   if self.case=='stream-file-crud':
    import stream_file_cases
    stream_file_cases.prepare_context(self)
   if self.case=='process-transports':
    import process_transport_cases
    process_transport_cases.prepare_context(self)
   if self.case=='input-promotion':
    import input_promotion_cases
    input_promotion_cases.prepare_context(self)
   if self.case=='output-profile':
    import output_profile_cases
    output_profile_cases.prepare_context(self)
   if self.case=='schedules-direct':
    import sd_guide_cases,xml_tvg_cases
    sd_guide_cases.prepare_context(self);xml_tvg_cases.prepare_context(self)
    import xmltv_download_cases,xmltv_selected_cases,xmltv_properties_output_cases
    xmltv_download_cases.prepare_context(self);xmltv_selected_cases.prepare_context(self);xmltv_properties_output_cases.prepare_context(self)
    import epg_connect_cases
    epg_connect_cases.prepare_context(self)
    import xml_hook_cases,xml_remote_hook_cases,dummy_fallback_cases,stream_name_cases
    xml_hook_cases.prepare_context(self);xml_remote_hook_cases.prepare_context(self);dummy_fallback_cases.prepare_context(self);stream_name_cases.prepare_context(self)
    import custom_guide_cases
    custom_guide_cases.prepare_context(self)
   if self.case=='custom-guide-held':
    import custom_guide_cases
    custom_guide_cases.prepare_held_context(self)
   if self.case=='m8-actions':
    from m8_actions import cases as m8_actions_cases
    m8_actions_cases.prepare_context(self)
   if self.case=='recording-output-profile':
    import recording_profile_cases
    recording_profile_cases.prepare_context(self)
   if self.case=='redirect-common':
    import redirect_cases
    redirect_cases.prepare_context(self)
   if self.case=='vlc-live':
    import vlc_live_cases
    vlc_live_cases.prepare_context(self)
   if self.case in('backup-held-graceful','backup-held-deadline','backup-delete-refusal'):self.backup_physical_module().prepare_context(self)
   if self.case in self.binding.get('restore_callbacks',{}):self.restore_module().prepare_context(self)
   if self.case=='recording-plugin-characterization':
    import recording_connect_cases
    recording_connect_cases.prepare_context(self)
   if self.case=='recording-connect-boundaries':
    import recording_connect_boundaries
    recording_connect_boundaries.prepare_context(self)
   if self.case=='channel-matching':
    import matching_cases
    matching_cases.prepare_context(self)
   if self.case=='core-produced':self.core_module().prepare_context(self,self.output)
   import threading
   self._sampler_stop=threading.Event()
   def sample():
    while not self._sampler_stop.is_set():
     for p in(self.case_data/'recordings').rglob('.dvr_*_hls'):
      match=re.fullmatch(r'\.dvr_([0-9]+)_hls',p.name)
      if match:
       count=sum(1 for f in p.glob('seg_*.ts'));ident=int(match.group(1));self.maximum_by_recording[ident]=max(self.maximum_by_recording.get(ident,0),count)
     self._sampler_stop.wait(1)
   self._sampler=threading.Thread(target=sample,daemon=True);self._sampler.start()
   import startup_hooks;startup_hooks.prepare_context(self)
   if self.case=='channel-matching':self._plugin_environment={**self._plugin_environment,'DISABLE_ML_DOWNLOADS':'true'}
   if self.case=='m8-actions':self._plugin_environment={**self._plugin_environment,'RUST_PLUGIN_EXECUTION_AUDIT':'1'}
   if self.backend=='Original':self.start_original(consumer=start_consumers)
   else:self.start_native()
   if not self.case.startswith('plugin-eta-'):
    actual_startup=startup_hooks.collect(self);startup_label='-actual-limited-startup-domain.json'if self.backend=='Native'and self.case in('m8-actions','recording-plugin-characterization','recording-connect-boundaries')else'-actual-three-startup-roots.json';save(self.output/(self.case+startup_label),actual_startup)
    if self.backend=='Native'and self.case=='recording-plugin-characterization':self._recording_connect_startup_receipt=actual_startup
    if self.backend=='Native'and self.case=='recording-connect-boundaries':self._recording_connect_boundary_startup_receipt=actual_startup
   yield self
  except BaseException as error:
   self._context_failed=True
   primary=error;save(self.output/(name+'-primary-exception.json'),{'type':type(error).__name__,'message':str(error)[:2048]});raise
  finally:
   if self.functional_budget_expired():self.begin_cleanup()
   errors=[];failure_logs_recollected=False
   if self._context_failed:
    failure_logs_recollected=True
    for retired_name,retired_response in list(self._retired_role_logs):
     try:self.collect_role_logs(retired_name,retired_response)
     except BaseException as error:errors.append({'scope':'retired-role-log','type':type(error).__name__})
   for label,action in [('viewer',lambda:self.stop_ordinary_viewer()if self.viewer else None),('Native',lambda:self.stop_native(expected=0)if self.native else None),('roles',lambda:__import__('vod_series_info_cases').stop_roles_for_default_restore(self)if self.case=='vod-series-info'else self.stop_roles()),('provider-info-default-agent',lambda:__import__('vod_series_info_cases').restore_context(self)if self.case=='vod-series-info'else None),('snapshot',lambda:self.snapshot(name+'-after-backend-stop'))]:
    try:action()
    except BaseException as error:
     self._context_failed=True
     errors.append({'scope':label,'type':type(error).__name__,'message':str(error)[:1024]})
   if errors and not failure_logs_recollected:
    self._context_failed=True
    for retired_name,retired_response in list(self._retired_role_logs):
     try:self.collect_role_logs(retired_name,retired_response)
     except BaseException as error:errors.append({'scope':'retired-role-log','type':type(error).__name__})
   if self._sampler_stop is not None:self._sampler_stop.set()
   if self._sampler is not None:
    try:self._sampler.join(self.remaining(5))
    except BaseException as error:errors.append({'scope':'sampler','type':type(error).__name__})
    if self._sampler.is_alive():errors.append({'scope':'sampler','type':'ActualFilesystemObserverUnsettled'})
   if errors:
    save(self.output/(name+'-teardown-errors.json'),{'primary_preserved':primary is not None,'errors':errors})
    if primary is None:raise RuntimeError('ActualContextTeardownFailed:'+name)
 def actual_admin_token(self):return self.admin
 def read_rows(self):return self.sql("print(json.dumps(list(Recording.objects.values()),default=str))")
 def snapshot(self,label):
  rows=self.read_rows()
  for row in rows:self.recordings.setdefault(row['id'],row)
  redis=self.redis(0,"local p='live:channel:'..ARGV[1];return {redis.call('GET','profile_connections:'..ARGV[2])or'0',redis.call('SCARD',p..':clients'),redis.call('HGETALL',p..':metadata'),redis.call('GET',p..':owner')or''}",self.channel_uuid,self.provider)
  origin=json.loads(self.get(self.origin+'/stats')[1]);processes=json.loads(self.python("import json,sys;sys.path.insert(0,'/normal-dvr-tools');from role_owner import processes;print(json.dumps(processes()))"))
  # Candidate images have no shell/Python; process argv is read through docker
  # top, then exact host PID/start generation recorded by the adapter.
  if self.native:
   top=self.command([self.docker,'top',self.native,'-eo','pid,args']);processes+=[{'argv_text':line,'host_pid':int(line.split()[0]),'starttime':self.pid_start(int(line.split()[0]))}for line in top.splitlines()[1:]if line.strip()]
  recorders=[p for p in processes if 'ffmpeg'in p.get('argv_text','')and 'Dispatcharr-DVR/recording-'in p.get('argv_text','')]
  files=[]
  for p in(self.case_data/'recordings').rglob('*'):
   if p.is_file():files.append({'path':str(p.relative_to(self.case_data)),'bytes':p.stat().st_size})
  segments=sum(1 for p in files if Path(p['path']).name.startswith('seg_')and p['path'].endswith('.ts'));self.max_segments=max(self.max_segments,segments)
  events=self.sql("from core.models import SystemEvent;print(json.dumps(list(SystemEvent.objects.filter(event_type='recording_end').values()),default=str))")
  cancelled=[e for e in events if(e.get('details')or{}).get('outcome')=='cancelled'];late=[e for e in events if(e.get('details')or{}).get('recording_id')in self.deleted_ids()and(e.get('details')or{}).get('outcome')!='cancelled']
  live_ids={r['id']for r in rows if(r.get('custom_properties')or{}).get('status')=='recording'}
  observed_live={int(m.group(1))for p in recorders for m in [re.search(r'Dispatcharr-DVR/recording-([0-9]+)',p.get('argv_text',''))]if m}
  result={'actual_rows':rows,'raw_client_metadata':redis,'actual_processes':processes,'actual_origin':origin,'files':files,'actual_events':events,'actual_upstream_connections':origin.get('active'),'actual_provider_slots':int(redis[0]),'actual_recording_ffmpeg_count':len(recorders),'all_owned_recording_children_retired':len(recorders)==0,'owned_hls_directories_absent':not any((self.case_data/'recordings').rglob('.dvr_*_hls')),'maximum_actual_segment_count':max(self.maximum_by_recording.values(),default=0),'cancelled_recording_end_count':len(cancelled),'late_duplicate_cancelled_end_count':len(late),'other_viewers_preserved':live_ids.issubset(observed_live)and(self.viewer is None or(not self.viewer['closed']and self.viewer['bytes']>self._viewer_before)),'actual_control_viewer':None if self.viewer is None else{k:v for k,v in self.viewer.items()if k not in('thread','stop','response')},'actual_internal_TS_probe':getattr(self,'_acl_probe',None),'actual_internal_TS_HTTP_denied':getattr(self,'_acl_probe',{}).get('status')==403,'invented_internal_credentials':getattr(self,'_acl_probe',{}).get('credential_present'),'loopback_policy_bypass':getattr(self,'_acl_probe',{}).get('status')!=403 or origin.get('active')!=0,'authentication_scope':'actual no-credential loopback probe plus attested recorder argv/source; no private key is supplied by fixture'}
  save(self.output/(label+'.json'),result);return result
 def deleted_ids(self):return getattr(self,'_deleted',set())
 def wait_for_hls_segments(self,ids,count,seconds):
  def ready():
   rows=self.read_rows();paths={r['id']:(r.get('custom_properties')or{}).get('_hls_dir')for r in rows};counts=[]
   for ident in ids:
    path=paths.get(ident);local=None if not path else self.case_data/Path(path).relative_to('/data');counts.append(0 if local is None or not local.exists()else len(list(local.glob('seg_*.ts'))))
   self.max_segments=max(self.max_segments,max(counts,default=0));return all(c>=count for c in counts)
  return self.wait(ready,seconds,'actual-HLS-segments')
 def wait_for_recording_absence(self,ident,seconds):
  self._deleted=getattr(self,'_deleted',set())|{ident};row=self.recordings.get(ident)
  def gone():
   if any(r['id']==ident for r in self.read_rows()):return False
   if row:
    for key in ['file_path','_hls_dir']:
     path=(row.get('custom_properties')or{}).get(key)
     if path and(self.case_data/Path(path).relative_to('/data')).exists():return False
   return self.snapshot('delete-absence-'+str(ident))['actual_recording_ffmpeg_count']<=1
  return self.wait(gone,seconds,'actual-row-child-file-absence')
 def wait_past_recording_original_deadline(self,ident,seconds):
  row=self.recordings[ident];end=datetime.datetime.fromisoformat(str(row['end_time']));remaining=(end-utc()).total_seconds()+seconds
  if remaining>0:self.sleep(remaining)
 def decode_complete_recording(self,ident):
  row=next(r for r in self.read_rows()if r['id']==ident);path=(row.get('custom_properties')or{})['file_path'];host=self.case_data/Path(path).relative_to('/data');assert host.is_file()
  local='/shared/'+self.case+'/'+str(Path(path).relative_to('/data'))
  probe=json.loads(self.command(self.compose+['exec','-T','baseline','ffprobe','-v','error','-count_frames','-show_entries','stream=codec_type,codec_name,nb_read_frames','-of','json',local],90))
  result=self.command(self.compose+['exec','-T','baseline','ffmpeg','-hide_banner','-v','error','-xerror','-i',local,'-map','0:v:0','-map','0:a:0','-f','null','-'],180,allow=True)
  actual=self.cli[-1];stderr=Path(actual['stderr']).read_bytes()
  counts={s['codec_type']:int(s.get('nb_read_frames','0'))for s in probe['streams']if str(s.get('nb_read_frames','')).isdigit()}
  return {'actual_exit':actual['actual_exit'],'process_group_settled':actual['process_group_settled'],'stdout_EOF':actual['stdout_EOF'],'stderr_EOF':actual['stderr_EOF'],'video_frames':counts.get('video',0),'audio_frames':counts.get('audio',0),'actual_file_bytes':host.stat().st_size,'file_sha256':hashlib.sha256(host.read_bytes()).hexdigest(),'internal_damage_detected':bool(stderr),'decoder_stderr_sha256':hashlib.sha256(stderr).hexdigest(),'actual_ffprobe':probe,'decoder_stdout':result,'actual_command_receipts':self.serial}
 def actual_Main_pool_sockets(self):
  from m8_actions.observer import capture
  return capture(self)
 def actual_isolation_valid(self):
  try:
   stats=json.loads(self.get(self.origin+'/stats')[1]);return stats.get('active')==0 and self.native is None and not self.roles
  except Exception:return False
 def api(self,method,path,body=None,token='admin',headers=None):
  import urllib.request,urllib.error
  auth={}if token is None else {'Authorization':'Bearer '+(self.admin if token=='admin'else self.accounts[token]['access'])}
  if headers:auth.update(headers)
  data=None if body is None else json.dumps(body).encode()
  if data is not None:auth['Content-Type']='application/json'
  request=urllib.request.Request(self.base_url+path,data=data,headers=auth,method=method)
  try:r=urllib.request.urlopen(request,timeout=self.remaining(15))
  except urllib.error.HTTPError as e:r=e
  with r:
   raw=bounded_read(r,1048576,self.remaining)
   if len(raw)>1048576:raise ValueError('ActualAPIControlReplyBudget')
   result={'context':self.case,'method':method,'path':path,'credential_role':token,'status':r.status,'body':json.loads(raw)if raw else None}
  save(self.output/(self.case+'-api-'+uuid.uuid4().hex[:8]+'.json'),result);return result
 def authorization_cases(self):
  self.accounts['manager']=self.sql("from apps.accounts.models import User;from rest_framework_simplejwt.tokens import RefreshToken;u=User.objects.create(username='normal-dvr-manager',user_level=1,is_active=True,custom_properties={'dvr_access':'manage'});print(json.dumps({'id':u.pk,'access':str(RefreshToken.for_user(u).access_token)}))",secret=True)
  start=utc()+datetime.timedelta(seconds=180);body={'channel':self.channel_id,'start_time':start.isoformat(),'end_time':(start+datetime.timedelta(seconds=60)).isoformat(),'custom_properties':{}}
  rows=[]
  for role,method,expected in [(None,'GET',401),('standard','GET',200),('standard','POST',403),('viewer','GET',403),('inactive','GET',401),('manager','POST',201)]:
   result=self.api(method,'/api/channels/recordings/',body if method=='POST'else None,role);result['expected_status']=expected;rows.append(result)
  created=[r['body']['id']for r in rows if r['status']==201]
  hidden=self.sql("from apps.channels.models import Channel;c=Channel.objects.create(name='Normal private channel',channel_number=900000,user_level=10);print(json.dumps(c.pk))")
  hidden_body=dict(body,channel=hidden)
  rejected=self.api('POST','/api/channels/recordings/',hidden_body,'manager');rejected['expected_status']=400;rows.append(rejected)
  visible=self.api('POST','/api/channels/recordings/',hidden_body,'admin');visible['expected_status']=201;rows.append(visible)
  if visible['status']==201:
   ident=visible['body']['id'];created.append(ident);lookup=self.api('GET',f'/api/channels/recordings/{ident}/',token='manager');lookup['expected_status']=404;rows.append(lookup)
  for ident in created:self.api('DELETE',f'/api/channels/recordings/{ident}/')
  result={'actual':rows,'passed':all(r['status']==r['expected_status']for r in rows)};save(self.output/'authorization-cases.json',result);return result
 def start_ordinary_viewer(self,recording_prefix):
  import threading,urllib.request
  self.viewer={'bytes':0,'closed':False,'failure':None,'response':None,'stop':threading.Event(),'user_agent':'Dispatcharr-DVR/recording-'+str(recording_prefix)+'0','scope':'actual prefix-control viewer; no matching Recording row or task authority fabricated'}
  def read():
   try:
    request=urllib.request.Request(self.base_url+'/proxy/ts/stream/'+self.channel_uuid,headers={'User-Agent':self.viewer['user_agent']})
    with urllib.request.urlopen(request,timeout=self.remaining(60))as response:
     self.viewer['response']=response;self.viewer['status']=response.status
     while not self.viewer['stop'].is_set():
      data=response.read1(65536)
      if not data:break
      self.viewer['bytes']+=len(data)
   except Exception as error:
    if not self.viewer['stop'].is_set():self.viewer['failure']=type(error).__name__
   finally:self.viewer['closed']=True
  thread=threading.Thread(target=read);self.viewer['thread']=thread;thread.start();self.wait(lambda:self.viewer['bytes']>0 or self.viewer['failure'],30,'actual-prefix-control-viewer')
  if self.viewer['failure']:raise RuntimeError(self.viewer['failure'])
  self._viewer_before=self.viewer['bytes']
 def stop_ordinary_viewer(self):
  viewer=self.viewer
  if viewer is None:return
  viewer['stop'].set();response=viewer['response']
  if response is not None:
   try:response.fp.raw._sock.shutdown(socket.SHUT_RDWR)
   except(OSError,AttributeError):pass
  viewer['thread'].join(10)
  receipt={k:v for k,v in viewer.items()if k not in('thread','stop','response')};receipt['actual_thread_settled']=not viewer['thread'].is_alive();save(self.output/('ordinary-viewer-'+uuid.uuid4().hex[:5]+'.json'),receipt)
  if not receipt['actual_thread_settled']:raise RuntimeError('ActualViewerObserverUnsettled')
  self.viewer=None
 def retained_native_case(self,name):
  if self.backend!='Native':return None
  value=self.binding.get('retained_native_contexts',{}).get(name)
  if value is None:return None
  if value['candidate_image_id']!=self.binding['candidate_image_id']or value.get('satisfies_dependency')is not True:raise ValueError('NativeAuthorityDifferentImageOrUnqualified')
  for reference in [*value['observations'],value['cleanup']]:
   if hashlib.sha256(Path(reference['path']).read_bytes()).hexdigest()!=reference['sha256']:raise ValueError('RetainedNativeObservationChanged')
  cleanup=json.loads(Path(value['cleanup']['path']).read_text())
  if not all(cleanup.get(key)is True for key in ['exact_owned_absence','all_original_process_generations_absent','host_CLI_groups_settled'])or cleanup.get('errors'):raise ValueError('RetainedNativeCleanupUnqualified')
  report=json.loads(Path(value['observations'][0]['path']).read_text())
  if value.get('kind')!='same-image12-passed-leaf'or len([row for row in report['cases']if row['case']==name and row['status']=='passed'])!=1:raise ValueError('RetainedNativeNamedLeafUnqualified')
  return value
 def retained_source_case(self,name):
  if self.backend!='Original':return None
  proof=self.binding.get('retained_source_contexts',{}).get(name)
  if proof is None:return None
  for reference in [*proof['observations'],proof['cleanup']]:
   self.remaining(1)
   if hashlib.sha256(Path(reference['path']).read_bytes()).hexdigest()!=reference['sha256']:raise ValueError('RetainedSourceReferenceChanged')
  cleanup=json.loads(Path(proof['cleanup']['path']).read_text())
  if not all(cleanup.get(key)is True for key in ['exact_owned_absence','all_original_process_generations_absent','host_CLI_groups_settled']):raise ValueError('RetainedSourceCleanupUnqualified')
  observed=[json.loads(Path(reference['path']).read_text())for reference in proof['observations']]
  if proof['kind']=='Source-authorization9':
   if len(observed)!=1 or observed[0].get('passed')is not True or len(observed[0]['actual'])!=9 or not all(row['status']==row['expected_status']for row in observed[0]['actual']):raise ValueError('RetainedSourceAuth9Unqualified')
  elif proof['kind']=='Source-selected-five-families':
   if len(observed)!=5 or not all(row.get('status')=='passed'for row in observed):raise ValueError('RetainedSourceFamiliesUnqualified')
  elif proof['kind']=='Source-backup12':
   if len(observed)!=1 or observed[0].get('status')!='passed' or len(observed[0].get('cells',[]))!=12 or any(row['status']!='passed'for row in observed[0]['cells']):raise ValueError('RetainedSourceBackup12Unqualified')
  elif proof['kind']=='Source-current-restore5':
   required=['actual_current_schema_archive','actual_intervening_database_mutation','actual_REST202_restore_and_post_migrate','same_Main_restored_login_settings_channels_and_pool_retirement','caught_archive_failure_is_SUCCESS_failed_dict']
   if len(observed)!=1 or observed[0].get('status')!='passed' or [row['name']for row in observed[0].get('cells',[])]!=required or any(row['status']!='passed'for row in observed[0]['cells']):raise ValueError('RetainedSourceCurrentRestore5Unqualified')
  elif proof['kind']=='Source-slow-reader-two-phase':
   if len(observed)!=1 or observed[0].get('executed_slice_passed')is not True or observed[0].get('Source_held_TERM_characterized')is not True or observed[0].get('required_unmeasured'):raise ValueError('RetainedSourceSlowReaderUnqualified')
  elif proof['kind']=='Source-backup-physical':
   if len(observed)!=1 or observed[0].get('status')!='passed':raise ValueError('RetainedSourceBackupPhysicalUnqualified')
  elif proof['kind']=='Source-restart-task-state10-only':
   state,gate,backend_report,decoder=observed
   checks={key:value for key,value in state.items()if isinstance(value,bool)}
   if len(checks)!=10 or not all(checks.values())or gate.get('applied_before_dvr_intake')is not True or not gate.get('actual_raw_removed_and_REVOKED_after_add'):raise ValueError('RetainedSourceRestartStateUnqualified')
   if decoder.get('actual_exit')!=0 or decoder.get('internal_damage_detected')is not True or not any(row['case']=='nonempty-restart'and row['status']=='failed'for row in backend_report['cases']):raise ValueError('HistoricalSourceMediaFailureMustRemainLiteral')
  elif proof['kind']=='Source-plugin-eta-scoped':
   report,refs=observed
   if report.get('qualified_Source_warm_crash_subset_passed')is not True or report.get('historical_whole_passed')is not False or report.get('full_Source_ETA_passed')is not False or report.get('Source_apps_replayed')!=0:raise ValueError('ScopedSourceETAQualificationRequired')
   matching=[row for row in report['cases']if row['case']==name]
   if len(matching)!=1 or matching[0].get('qualified_subset_passed')is not True or not all(matching[0]['checks'].values()):raise ValueError('ActualSourceETACaseChecksRequired')
   for reference in refs['paths']:
    if hashlib.sha256((Path(__file__).resolve().parents[4]/reference['path']).read_bytes()).hexdigest()!=reference['sha256']:raise ValueError('ScopedSourceETARawChanged')
  elif proof['kind']=='Source-controls':
   final,first,second,viewer,*requests=observed
   if not(final['cancelled_recording_end_count']==1 and final['late_duplicate_cancelled_end_count']==0 and final['other_viewers_preserved']and final['all_owned_recording_children_retired']and final['actual_recording_ffmpeg_count']==0):raise ValueError('RetainedSourceControlFinalUnqualified')
   for decoder in(first,second):
    if not(decoder['actual_exit']==0 and decoder['stdout_EOF']and decoder['stderr_EOF']and decoder['process_group_settled']and decoder['video_frames']>0 and decoder['audio_frames']>0 and decoder['internal_damage_detected']is False):raise ValueError('RetainedSourceControlDecoderUnqualified')
   if not(viewer['status']==200 and viewer['closed']and viewer['actual_thread_settled']and viewer['bytes']>0 and viewer['user_agent']=='Dispatcharr-DVR/recording-10'):raise ValueError('RetainedSourceControlViewerUnqualified')
   if [item['status']for item in requests]!=[201,201,201,200,200,204]or any(item['failure']is not None for item in requests):raise ValueError('RetainedSourceControlHTTPUnqualified')
  else:raise ValueError('UnknownRetainedSourceContext')
  return {'status':'retained_source_authority','satisfies_dependency':True,'actual_reference':proof,'scope':'Only named approvedSource body; no newSource calls/no broader milestone pass'}
 def m6_module(self):
  path=self.binding.get('m6_adapter')
  if not path:return None
  import importlib.util
  if hashlib.sha256(Path(path).read_bytes()).hexdigest()!=self.binding['m6_adapter_sha256']:raise ValueError('BoundM6AdapterChanged')
  spec=importlib.util.spec_from_file_location('owned_m6_shared_cohort',path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module
 def active_viewer_cases(self,output):
  module=self.m6_module()
  if module is None:return {'status':'not_bound','scope':'optional M6 callback, not a passed cell'}
  return module.active_viewer_cases(self,output)
 def run_selected_families(self,output):
  module=self.m6_module()
  if module is None:raise Unsupported('actual M6 callback not bound')
  return module.run_selected_families(self,output)
 def artwork_cases(self):
  ledger='/shared/'+self.case+'/artwork-publications.jsonl'
  self.role('broker-monitor',['/dispatcharrpy/bin/python','/normal-dvr-tools/broker_monitor.py'],extra={'NORMAL_DVR_LEDGER':ledger})
  self.wait(lambda:(self.case_data/'artwork-publications.ready').exists(),10,'actual-broker-publication-observer')
  channel=self.channel_id;start=utc()+datetime.timedelta(seconds=200);end=start+datetime.timedelta(seconds=100)
  cases=[]
  def reset():self.sql("from apps.channels.models import Channel;Channel.objects.filter(pk="+str(channel)+").update(logo_id=None,epg_data_id=None);print(json.dumps(True))")
  def setup_epg(overlap,properties):
   value=self.sql("from apps.channels.models import Channel;from apps.epg.models import EPGData,ProgramData;from datetime import datetime,timedelta;e=EPGData.objects.create(tvg_id='normal-'+str(__import__('uuid').uuid4()),name="+repr(self.channel_name)+");Channel.objects.filter(pk="+str(channel)+").update(epg_data_id=e.pk);p=ProgramData.objects.create(epg=e,tvg_id=e.tvg_id,start_time=datetime.fromisoformat("+repr(start.isoformat())+"),end_time=datetime.fromisoformat("+repr(start.isoformat())+")+timedelta(seconds="+str(overlap)+"),title="+repr(self.channel_name)+",sub_title='actual subtitle',description='actual programme',custom_properties="+repr(properties)+");print(json.dumps({'epg':e.pk,'program':p.pk}))")
   return value['program']
  def one(label,properties,expected):
   body={'channel':channel,'start_time':start.isoformat(),'end_time':end.isoformat(),'custom_properties':properties}
   result=self.api('POST','/api/channels/recordings/',body);row={'label':label,'HTTP':result,'expected_properties':expected,'status':'failed'};cases.append(row)
   if result['status']!=201:return
   ident=result['body']['id']
   try:
    def frame():
     import base64
     path=self.case_data/'artwork-publications.jsonl'
     if not path.exists():return None
     for line in path.read_text().splitlines():
      item=json.loads(line);value=json.loads(item['raw_frame']);args=json.loads(base64.b64decode(value['body']))[0]
      if value['headers']['task']=='apps.channels.tasks.prefetch_recording_artwork'and args==[ident]:return value
    observed=self.wait(frame,15,'actual-root-artwork-frame-'+str(ident));key=observed['headers']['id'];row['actual_frame']=observed
    def completed():
     raw=self.redis(2,"return redis.call('GET','celery-tasks:celery-task-meta-'..ARGV[1])or false",key)
     value=None if not raw else json.loads(raw)
     return value if value and value.get('status')in ('SUCCESS','FAILURE','REVOKED')else None
    backend=self.wait(completed,30,'actual-artwork-result-'+key);row['actual_backend']=backend
    actual=self.api('GET',f'/api/channels/recordings/{ident}/');row['actual_recording']=actual
    cp=(actual.get('body')or{}).get('custom_properties')or{}
    row['status']='passed'if backend.get('status')=='SUCCESS'and backend.get('result')=='ok'and all(cp.get(k)==v for k,v in expected.items())and ('program'in cp)==('program'in properties)else'failed'
   except Exception as error:row['failure']=type(error).__name__+':'+str(error)[:512]
   finally:
    try:
     row['actual_cleanup_HTTP']=self.api('DELETE',f'/api/channels/recordings/{ident}/')
     if row['actual_cleanup_HTTP']['status']!=204:row['status']='failed'
    except Exception as error:row['cleanup_failure']=type(error).__name__;row['status']='failed'
    save(self.output/('artwork-cell-'+str(ident)+'.json'),row)
  try:
   reset();one('manual-no-program-no-image',{}, {})
   reset();logo=self.sql("from apps.channels.models import Channel,Logo;l=Logo.objects.create(name='Actual channel logo fallback',url='/data/logos/normal-fixture.svg');Channel.objects.filter(pk="+str(channel)+").update(logo=l);print(json.dumps(l.pk))")
   one('manual-channel-logo-fallback',{}, {'poster_logo_id':logo})
   one('channel-title-provider-guard',{'program':{'title':self.channel_name}}, {'poster_logo_id':logo})
   reset();setup_epg(100,{})
   one('manual-EPG-atleast80-noimage-program-not-saved',{}, {})
   reset();setup_epg(50,{})
   one('manual-EPG-below80-no-guessed-program',{}, {})
   reset();program=setup_epg(100,{'rating':'PG','rating_system':'TV','season':0,'episode':0})
   one('EPG-noimage-rating-zero-preserves-Python-equality',{'program':{'id':program,'title':self.channel_name},'season':False},{'rating':'PG','rating_system':'TV','season':False,'episode':0})
  finally:self.role('broker-monitor',[],stop=True)
  result={'cases':cases,'passed':len(cases)==6 and all(c['status']=='passed'for c in cases),'provider_images':'not exercised/explicit unqualified'};save(self.output/'artwork-cases.json',result);return result
 def idle_viewing_module(self):
  import idle_viewing,live_reader
  for module,key in [(idle_viewing,'idle_viewing_sha256'),(live_reader,'live_reader_sha256')]:
   if hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest()!=self.binding[key]:raise ValueError('BoundIdleViewingHelperChanged')
  if hashlib.sha256(Path(idle_viewing.__file__).with_name('idle_profile_prepare.py').read_bytes()).hexdigest()!=self.binding['idle_profile_prepare_sha256']:raise ValueError('BoundIdleProfileTemplateChanged')
  return idle_viewing
 def idle_viewing_cases(self):return self.idle_viewing_module().run(self)
 def xc_live_module(self):
  import xc_live_cases
  # The Origin is an executable guest entrypoint, not a host-importable module:
  # verify its bound bytes without executing its environment/server startup.
  for path,key in [(Path(xc_live_cases.__file__),'xc_live_cases_sha256'),(Path(xc_live_cases.__file__).with_name('xc_live_origin.py'),'xc_live_origin_sha256')]:
   if hashlib.sha256(path.read_bytes()).hexdigest()!=self.binding[key]:raise ValueError('BoundXCModuleChanged')
  return xc_live_cases
 def xc_live_cases(self):return self.xc_live_module().run(self)
 def provider_artwork_module(self):
  import provider_cases,provider_origin
  for module,name in [(provider_cases,'provider_cases'),(provider_origin,'provider_origin')]:
   if hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest()!=self.binding[name+'_sha256']:raise ValueError('ExactOwnedProviderClosureRequired:'+name)
  return provider_cases
 def provider_artwork_cases(self):
  from startup_receipts import collect
  roots=collect(self);save(self.output/'provider-actual-startup-roots.json',roots)
  return self.provider_artwork_module().run(self)
 def restore_module(self):
  import importlib.util
  row=self.binding['restore_callbacks'][self.case];path=Path(row['path'])
  if self.backend not in row['backends']:raise ValueError('RestoreCallbackBackendScope')
  if hashlib.sha256(path.read_bytes()).hexdigest()!=row['sha256']:raise ValueError('ExactRestoreCallbackRequired')
  spec=importlib.util.spec_from_file_location('owned_restore_'+self.case.replace('-','_'),path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module
 def restore_cases(self):
  # Existing context manager already prepared fixture inputs and awaited the
  # actual startup roots before this ordinary workflow dispatch.
  return self.restore_module().run(self)
 def normal_backup_module(self):
  import importlib.util
  path=Path(self.binding['normal_backup_case'])
  if hashlib.sha256(path.read_bytes()).hexdigest()!=self.binding['normal_backup_case_sha256']:raise ValueError('ExactNormalBackupCallbackRequired')
  spec=importlib.util.spec_from_file_location('owned_normal_backup_case',path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module
 def backup_physical_module(self):
  import backup_physical_case
  path=Path(backup_physical_case.__file__)
  if hashlib.sha256(path.read_bytes()).hexdigest()!=self.binding['backup_physical_case_sha256']:raise ValueError('ExactBackupPhysicalCallbackRequired')
  return backup_physical_case
 def backup_physical_cases(self):
  from startup_receipts import collect
  roots=collect(self);save(self.output/(self.case+'-actual-startup-roots.json'),roots)
  module=self.backup_physical_module()
  callback=module.run_delete_refusal if self.case=='backup-delete-refusal'else module.run_held
  return callback(self,self.m6_module(),self.normal_backup_module())
 def normal_backup_cases(self):
  from startup_receipts import collect
  roots=collect(self);save(self.output/'backup-actual-startup-roots.json',roots)
  return self.normal_backup_module().run(self,self.m6_module())
 def channel_maintenance_cases(self):
  import importlib.util
  from startup_receipts import collect
  path=Path(self.binding['channel_callback'])
  if hashlib.sha256(path.read_bytes()).hexdigest()!=self.binding['channel_callback_sha256']:raise ValueError('ExactNativeChannelCallbackRequired')
  spec=importlib.util.spec_from_file_location('owned_Native_channel_maintenance',path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
  roots=collect(self);save(self.output/'channel-actual-startup-roots.json',roots)
  return module.run(self,roots,self.binding['channel_source9_authority'],self.binding['channel_source98_authority'],terminate=True)
 def source_retained_slow_reader(self):
  import importlib.util
  adapter=Path(self.binding['source_read_fixture_adapter']);authority=Path(self.binding['source_read_authority'])
  if hashlib.sha256(adapter.read_bytes()).hexdigest()!=self.binding['source_read_fixture_sha256']or hashlib.sha256(authority.read_bytes()).hexdigest()!=self.binding['source_read_authority_sha256']:raise ValueError('BoundRetainedSourceReadFixtureChanged')
  spec=importlib.util.spec_from_file_location('owned_retained_Original_read_fixture',adapter);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
  return module.run(self,authority)
 def slow_reader_cases(self,ident):
  import importlib.util
  path=Path(self.binding['slow_reader_adapter'])
  if hashlib.sha256(path.read_bytes()).hexdigest()!=self.binding['slow_reader_sha256']:raise ValueError('BoundSlowReaderAdapterChanged')
  spec=importlib.util.spec_from_file_location('owned_recording_slow_reader',path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
  return module.run(self,ident,terminate=True)
 def apply_actual_streams_deny_policy_for_loopback(self):
  self._system_before=self.sql("from core.models import CoreSettings;s=CoreSettings.objects.filter(key='network_access').first();print(json.dumps(None if s is None else s.value))")
  self.sql("from core.models import CoreSettings;s,_=CoreSettings.objects.get_or_create(key='network_access',defaults={'name':'Network Access','value':{}});s.value={'UI':'0.0.0.0/0,::/0','STREAMS':'192.0.2.1/32'};s.save();print(json.dumps(True))")
  self._acl_probe=self.internal_status()
 def restore_actual_streams_policy(self):
  self.sql("from core.models import CoreSettings;s=CoreSettings.objects.get(key='network_access');s.value="+repr(self._system_before)+";s.save();print(json.dumps(True))")
 def internal_status(self):
  port=5656 if self.backend=='Original'else 9192
  script="import urllib.request,urllib.error,json;u='http://127.0.0.1:"+str(port)+"/proxy/ts/stream/"+self.channel_uuid+"';\ntry:r=urllib.request.urlopen(u,timeout=5)\nexcept urllib.error.HTTPError as e:r=e\nwith r:print(json.dumps({'status':r.status,'body':r.read(4096).decode(errors='replace'),'credential_present':False}))"
  # Both backends share only the owned storage network namespace. Native
  # retains its own filesystem/PID namespace and exactly one /data bind.
  return json.loads(self.python(script))
 def produce_original_bookings_with_dvr_stopped(self):
  # Preserve actual SQL/media from a real capture interrupted before end. Start
  # Source only when needed; use the same real producer API in both backends.
  if self.backend=='Original':
   # independent_context already started the actual ready Source worker graph.
   # Do not immediately tear it down and restart it before producing bookings.
   if self.native or set(self.roles)!={'web','celery','dvr','beat'}:raise RuntimeError('ActualReadyOriginalProducerGraphRequired')
  else:
   if self.native:self.stop_native(expected=0)
   self.stop_roles();self.start_original(consumer=True)
  def request(method,path,value=None):
   import urllib.request,urllib.error
   encoded=None if value is None else json.dumps(value).encode();headers={'Authorization':'Bearer '+self.admin,'Content-Type':'application/json'}
   try:r=urllib.request.urlopen(urllib.request.Request(self.baseline+path,data=encoded,headers=headers,method=method),timeout=self.remaining(15))
   except urllib.error.HTTPError as e:r=e
   with r:data=r.read(1048576);return r.status,json.loads(data)if data else None
  def book(start,end,label):
   code,row=request('POST','/api/channels/recordings/',{'channel':self.channel_id,'start_time':start.isoformat(),'end_time':end.isoformat(),'custom_properties':{'program':{'title':self.channel_name},'fixture_case_label':label}})
   self._restart_statuses.append(code)
   if code!=201:raise AssertionError(('ActualOriginalBooking',code,row))
   return row['id']
  self._restart_statuses=[];now=utc();start=now+datetime.timedelta(seconds=4)
  # One expires during actual downtime, one remains in-window for actual resume.
  expired=book(start,start+datetime.timedelta(seconds=24),'actual-expired-recovery')
  missing=book(start,start+datetime.timedelta(seconds=25),'actual-expired-missing-playlist')
  resume=book(start+datetime.timedelta(seconds=1),start+datetime.timedelta(seconds=95),'actual-in-window-resume')
  self.wait_for_hls_segments([expired,resume,missing],2,45)
  self.role('beat',[],stop=True)
  # A real KILL of the dedicated worker and its inherited child group leaves
  # the actual in-progress SQL status/media untouched, matching owner loss.
  record=self.roles.pop('dvr')if 'dvr'in self.roles else None
  if record is None:raise RuntimeError('ActualDvrOwnerMissingBeforeCrash')
  crash=json.loads(self.python("import json,sys;sys.path.insert(0,'/normal-dvr-tools');from role_owner import crash;print(json.dumps(crash("+repr(record)+")))"));save(self.output/'restart-actual-worker-crash.json',crash)
  self.collect_role_logs('dvr-crash',crash)
  if crash['actual_exit']!=-9 or not crash['all_original_process_generations_absent']:raise AssertionError(('ActualSourceDvrCrash',crash))
  self.role('celery',[],stop=True)
  before=self.read_rows();self._restart_rows={r['id']:r for r in before}
  if any((self._restart_rows[i].get('custom_properties')or{}).get('status')!='recording'for i in [expired,resume,missing]):raise AssertionError('ActualCrashRowsWereRewritten')
  missing_path=(self._restart_rows[missing]['custom_properties']['_hls_dir'])+'/index.m3u8'
  self.python("from pathlib import Path;p=Path("+repr(missing_path)+");assert p.is_file();p.unlink();print('actual declared missing-playlist injection')")
  # Natural due Beat publication produces the old UUID frame, then an actual
  # API booking edit changes channel/window without rewriting that queued frame.
  old=book(utc()+datetime.timedelta(seconds=2),utc()+datetime.timedelta(seconds=80),'actual-old-clocked-frame')
  self.role('beat',['/dispatcharrpy/bin/celery','-A','dispatcharr','beat','--scheduler','django_celery_beat.schedulers:DatabaseScheduler','--max-interval','1','--pidfile','/tmp/normal-dvr-beat.pid','--schedule','/tmp/normal-dvr-beat','--loglevel=WARNING'])
  def queue():return self.redis(1,"return redis.call('LRANGE','dvr',0,-1)")
  def frame_for(ident):
   import base64
   for raw in queue():
    frame=json.loads(raw);args=json.loads(base64.b64decode(frame['body']))[0]
    if args[0]==ident:return {'raw':raw,'frame':frame,'args':args}
  self.wait(lambda:frame_for(old),15,'actual-Beat-UUID-frame')
  self.role('beat',[],stop=True)
  old_before=frame_for(old)
  new_channel=self.sql("from apps.channels.models import Channel;old=Channel.objects.get(pk="+str(self.channel_id)+");c=Channel.objects.create(name=old.name,channel_number=900001,stream_profile=old.stream_profile);c.streams.set(old.streams.all());print(json.dumps({'id':c.pk,'uuid':str(c.uuid)}))")
  code,changed=request('POST',f'/api/channels/recordings/{old}/extend/',{'extra_minutes':1});assert code==200,(code,changed)
  # Explicit independent row-writer interference, preserving the real HTTP/
  # Beat-published raw frame. This is NOT claimed as API PATCH publication
  # parity (PATCH would naturally publish another task and race its owner).
  channel_change=self.sql("r=Recording.objects.get(pk="+str(old)+");before={'channel':r.channel_id,'start':str(r.start_time),'end':str(r.end_time)};Recording.objects.filter(pk=r.pk).update(channel_id="+str(new_channel['id'])+");print(json.dumps({'before':before,'after_channel':"+str(new_channel['id'])+"}))")
  if frame_for(old)['raw']!=old_before['raw']:raise AssertionError('OriginalQueuedFrameWasRewritten')
  immediate=book(utc()-datetime.timedelta(seconds=1),utc()+datetime.timedelta(seconds=85),'actual-direct-named-frame')
  deleted=book(utc()-datetime.timedelta(seconds=1),utc()+datetime.timedelta(seconds=85),'actual-deleted-old-frame')
  revoked=book(utc()-datetime.timedelta(seconds=1),utc()+datetime.timedelta(seconds=85),'actual-revoked-deleted-frame')
  deleted_frames={i:frame_for(i)for i in [deleted,revoked]}
  for ident in [deleted,revoked]:
   code,_=request('DELETE',f'/api/channels/recordings/{ident}/');assert code==204
  future=book(utc()+datetime.timedelta(seconds=180),utc()+datetime.timedelta(seconds=240),'actual-future-repair')
  repair_damage=self.sql("from django_celery_beat.models import PeriodicTask;p=PeriodicTask.objects.get(name='dvr-recording-"+str(future)+"');before={'id':p.pk,'clocked':p.clocked_id};p.delete();print(json.dumps({'actual_deleted_periodic':before}))")
  self.wait(lambda:self.redis(0,"return redis.call('EXISTS',unpack(ARGV))",*['dvr:active:'+str(i)for i in [expired,resume,missing]])==0,55,'actual-crashed-active-key-expiry')
  self.wait(lambda:json.loads(self.get(self.origin+'/stats')[1]).get('active')==0,20,'actual-Source-upstream-retired')
  checkpoint={'status':'quiescing_Source_producer','scope':'atomic complete cache0 loss; actual broker1/results2 before-after proof','actual_roles_before':sorted(self.roles)}
  checkpoint_path=self.output/'restart-declared-cache-loss.json';save(checkpoint_path,checkpoint)
  try:
   # All required Source HTTP mutations are complete. Retire the last actual
   # producer before observing storage loss; do not race a live background
   # callback then assert that a non-atomic DBSIZE must remain zero forever.
   checkpoint['actual_Source_web_stop']=self.role('web',[],stop=True)
   checkpoint['actual_roles_after']=sorted(self.roles)
   if self.roles:raise AssertionError('ActualSourceProducerGraphNotQuiescent')
   script="""import os,json,hashlib,redis
clients={db:redis.Redis(host='127.0.0.1',port=6379,db=db,password=os.environ['REDIS_PASSWORD'])for db in (0,1,2)}
def keys(client):
 values=[];size=0
 for key in client.scan_iter(count=128):
  size+=len(key)
  if len(values)>=1024 or size>131072:raise RuntimeError('OwnedCacheCheckpointKeyBudget')
  values.append(key)
 return sorted(values)
def queue():
 client=clients[1];count=client.llen('dvr')
 if count>32:raise RuntimeError('OwnedRestartFrameCountBudget')
 rows=client.lrange('dvr',0,-1)
 if any(len(row)>16384 for row in rows)or sum(map(len,rows))>524288:raise RuntimeError('OwnedRestartFrameBytesBudget')
 return [row.decode()for row in rows]
def fingerprint(client):
 digest=hashlib.sha256();total=0;selected=keys(client)
 for key in selected:
  raw=client.dump(key)
  if raw is None:raise RuntimeError('OwnedResultChangedDuringSnapshot')
  total+=len(raw)
  if len(raw)>262144 or total>524288:raise RuntimeError('OwnedRestartResultBytesBudget')
  digest.update(len(key).to_bytes(4,'big'));digest.update(key);digest.update(len(raw).to_bytes(8,'big'));digest.update(raw)
 return {'keys':len(selected),'RDB_bytes':total,'sha256':digest.hexdigest()}
before={'cache_keys':[key.decode()for key in keys(clients[0])],'broker_frames':queue(),'broker_fingerprint':fingerprint(clients[1]),'results':fingerprint(clients[2])}
with clients[0].pipeline(transaction=True)as pipe:
 pipe.flushdb();pipe.dbsize();atomic=pipe.execute()
after={'cache_size_atomic_after_flush':atomic[1],'cache_size_after_checkpoint':clients[0].dbsize(),'broker_frames':queue(),'broker_fingerprint':fingerprint(clients[1]),'results':fingerprint(clients[2])}
print(json.dumps({'atomic_MULTI_EXEC_results':atomic,'before':before,'after':after,'broker_preserved':before['broker_frames']==after['broker_frames']and before['broker_fingerprint']==after['broker_fingerprint'],'results_preserved':before['results']==after['results']}))
for client in clients.values():client.close()
"""
   observed=json.loads(self.python(script,seconds=20,secret=True));checkpoint.update(observed)
   checkpoint.update(cache_keys_before=observed['before']['cache_keys'],cache_size_after=observed['after']['cache_size_atomic_after_flush'],broker_frames_before=observed['before']['broker_frames'],broker_frames_after=observed['after']['broker_frames'])
   checkpoint['status']='captured';save(checkpoint_path,checkpoint)
  except Exception as error:
   checkpoint.update(status='capture_failed',error_type=type(error).__name__);save(checkpoint_path,checkpoint);raise
  self._restart_cache_loss=checkpoint
  # Each actual outcome is persisted BEFORE its distinct assertion. A later
  # cache refill is reported separately; it does not falsify atomic loss proof.
  if checkpoint['atomic_MULTI_EXEC_results']!=[True,0]:raise AssertionError('DeclaredAtomicCacheLossFailed')
  if not checkpoint['broker_preserved']:raise AssertionError('DeclaredCacheLossChangedBroker')
  if not checkpoint['results_preserved']:raise AssertionError('DeclaredCacheLossChangedResults')
  checkpoint['status']='passed';save(checkpoint_path,checkpoint)
  self._restart={'expired':expired,'resume':resume,'missing':missing,'old':old,'immediate':immediate,'deleted':deleted,'revoked':revoked,'future':future,'old_frame':old_before,'deleted_frames':deleted_frames,'actual_extension':changed,'actual_independent_channel_writer':channel_change,'repair_damage':repair_damage,'queue_before':queue()}
  self.establish_actual_expired_recovery_inputs()
  # Hold the real Recording relation before consumer startup. Constructors do
  # not need it; actual admission is blocked while actual pidbox can subscribe.
  self.start_sql_lock(None,'restart-table-lock')
  return {'actual_HTTP_statuses':self._restart_statuses,'actual_dvr_queue_length':len(queue()),'actual_source_frames_retained':bool(old_before and all(deleted_frames.values())),'no_frame_rewrite':frame_for(old)['raw']==old_before['raw'],'actual_rows_before_crash':before,'actual_hls_missing_playlist':missing_path,'source_worker_crash':crash,'raw_source_queue':queue(),'repair_damage':repair_damage}
 def establish_actual_expired_recovery_inputs(self):
  # Establish the declared expired branch using actual clocks, not startup
  # timing or active-key TTL assumptions. No SQL timestamp is rewritten.
  ids=[self._restart['expired'],self._restart['missing']]
  planned={str(i):str(self._restart_rows[i]['end_time'])for i in ids}
  proof={'scope':'actual expired input barrier before consumer startup','maximum_wait_seconds':30,'planned_end_times':planned,'no_DB_time_rewrite':True,'observations':[]}
  path=self.output/'restart-expired-input-barrier.json';save(path,proof)
  script="import datetime;source_now=datetime.datetime.now(datetime.timezone.utc);\nwith connection.cursor()as c:\n c.execute('SELECT clock_timestamp()');db_now=c.fetchone()[0];c.execute('SELECT id,end_time FROM dispatcharr_channels_recording WHERE id IN (%s,%s) ORDER BY id',"+repr(tuple(ids))+");rows=c.fetchall()\nprint(json.dumps({'Source_UTC_clock':source_now.isoformat(),'DB_clock':db_now.isoformat(),'actual_end_times':{str(row[0]):row[1].isoformat()for row in rows}}))"
  def expired():
   actual=self.sql(script);proof['observations'].append(actual);save(path,proof)
   if len(proof['observations'])>64:raise RuntimeError('ExpiredInputObservationBudget')
   observed=actual['actual_end_times']
   if set(observed)!=set(planned)or any(datetime.datetime.fromisoformat(observed[i])!=datetime.datetime.fromisoformat(planned[i])for i in planned):raise AssertionError('ActualRecoveryEndTimesChanged')
   latest=max(datetime.datetime.fromisoformat(value)for value in observed.values())
   return actual if min(datetime.datetime.fromisoformat(actual['Source_UTC_clock']),datetime.datetime.fromisoformat(actual['DB_clock']))>=latest else False
  proof['actual_expired_before_consumer_start']=self.wait(expired,30,'actual-both-expired-before-consumer-start')
  proof['status']='actual_expired_inputs_established';save(path,proof)
 def start_sql_lock(self,recording,role_name):
  ready='/shared/'+self.case+'/'+role_name+'.ready'
  sql='LOCK TABLE dispatcharr_channels_recording IN ACCESS EXCLUSIVE MODE'if recording is None else 'SELECT id FROM dispatcharr_channels_recording WHERE id='+str(int(recording))+' FOR UPDATE'
  script="import os,signal,time,pathlib,psycopg;running=[True];signal.signal(signal.SIGTERM,lambda *unused:running.__setitem__(0,False));c=psycopg.connect(dbname=os.environ['POSTGRES_DB'],user=os.environ['POSTGRES_USER'],password=os.environ['POSTGRES_PASSWORD'],host='/var/run/postgresql');x=c.cursor();x.execute("+repr(sql)+");pathlib.Path("+repr(ready)+").write_text(str(os.getpid()));\nwhile running[0]:time.sleep(.1)\nc.rollback();c.close()"
  self.role(role_name,['/dispatcharrpy/bin/python','-c',script]);self.wait(lambda:(self.case_data/(role_name+'.ready')).is_file(),15,'actual-PG-lock-ready')
 def start_backend_on_preserved_nonempty_database_and_queue(self):
  self.role('web',[],stop=True)
  control_queue='normal-control-only-'+uuid.uuid4().hex
  if self.backend=='Original':self.start_original(dvr_queue=control_queue)
  else:self.start_native()
  # Actual subscription is necessary, never sufficient applied-revoke proof.
  channel='/1.celery.pidbox'
  if self.backend=='Original':
   # Kombu uses PSUBSCRIBE, so direct-only NUMSUB is not its authority.
   # A targeted real control reply proves the actual fanout path instead.
   def actual_source_control():
    script="import os,json;os.environ['CELERY_BROKER_URL']='redis://:playback-fixture-only@localhost:6379/1';from dispatcharr.celery import app;r=app.control.inspect(destination=['normal-default@owned','normal-dvr@owned'],timeout=1).ping();print(json.dumps(r))"
    replies=json.loads(self.python(script));expected={'normal-default@owned','normal-dvr@owned'}
    return replies if replies and set(replies)==expected and all(v=={'ok':'pong'}for v in replies.values())else False
   proof={'kind':'actual-targeted-Source-pidbox-ping','replies':self.wait(actual_source_control,20,'actual-Source-pidbox-control-reply')}
  else:
   proof={'kind':'actual-Native-direct-subscription','observed':self.wait(lambda:self.redis(1,"return redis.call('PUBSUB','NUMSUB',ARGV[1])",channel)[1]>0,20,'actual-pidbox-subscriber')}
  save(self.output/'restart-actual-pidbox-readiness.json',proof)
  key=self._restart['deleted_frames'][self._restart['revoked']]['frame']['headers']['id']
  if self.backend=='Original':
   import source_preclaim_revocation
   primary=None
   try:
    self._restart_control=source_preclaim_revocation.apply(self,key,control_queue)
    save(self.output/'restart-actual-revoke-publication.json',self._restart_control)
   except BaseException as error:
    primary=error
    raise
   finally:
    # Release the fixture-owned blocker before warm-stopping a worker whose
    # startup recovery can be waiting on it, including failed control gates.
    try:self.role('restart-table-lock',[],stop=True)
    except Exception as error:
     save(self.output/'restart-control-lock-cleanup-error.json',{'type':type(error).__name__,'primary_preserved':primary is not None})
     if primary is None:raise
   return
  import revocation_observation
  expected_raw=self._restart['deleted_frames'][self._restart['revoked']]['raw']
  proof={'scope':'actual Native immediate revoke backend before claim, then consumer terminal after claim','task_key':key}
  path=self.output/'restart-Native-preclaim-control.json';primary=None
  try:
   proof['before_control']=revocation_observation.observe(self,key,expected_raw);save(path,proof)
   if not(proof['before_control']['exact_raw_queued']and proof['before_control']['backend_absent']):raise AssertionError('ActualNativePreclaimInputRequired')
   script="import json;from celery import Celery;a=Celery('dispatcharr',broker='redis://:playback-fixture-only@localhost:6379/1');r=a.control.revoke("+repr(key)+",terminate=False);print(json.dumps({'actual_control_invoked':True,'return':r,'task_key':"+repr(key)+"}))"
   self._restart_control=json.loads(self.python(script));save(self.output/'restart-actual-revoke-publication.json',self._restart_control)
   def applied():
    value=revocation_observation.observe(self,key,expected_raw);proof['after_control_before_SQL_release']=value;save(path,proof)
    return value if value['exact_raw_queued']and value['same_task_sixfield_REVOKED_before_claim']else False
   proof['actual_applied_before_claim']=self.wait(applied,20,'actual-Native-sixfield-REVOKED-before-claim');save(path,proof)
  except BaseException as error:
   primary=error
   raise
  finally:
   try:self.role('restart-table-lock',[],stop=True)
   except Exception as error:
    save(self.output/'restart-control-lock-cleanup-error.json',{'type':type(error).__name__,'primary_preserved':primary is not None})
    if primary is None:raise
 def observe_complete_restart_recovery(self):
  import base64
  plan=self._restart
  def actual():return {r['id']:r for r in self.read_rows()}
  def completed():
   rows=actual()
   if not all(i in rows for i in [plan['expired'],plan['resume'],plan['missing'],plan['old'],plan['immediate']]):return False
   return all((rows[i].get('custom_properties')or{}).get('status')in('completed','interrupted','stopped')and 'remux_success'in(rows[i].get('custom_properties')or{})for i in [plan['expired'],plan['resume'],plan['missing'],plan['old'],plan['immediate']])
  try:self.wait(completed,200,'actual-restart-recording-finalization')
  except BaseException:
   diagnostic={'scope':'actual restart partial state before teardown/reset','actual_rows':actual()}
   try:
    if self.backend=='Native':
     code,body=self.get(self.base_url+'/health/ready');diagnostic['actual_Main_readiness']={'status':code,'body':json.loads(body)}
    diagnostic['actual_dvr_queue']=self.redis(1,"return redis.call('LRANGE','dvr',0,-1)")
    diagnostic['actual_celery_queue']=self.redis(1,"return redis.call('LRANGE','celery',0,-1)")
    diagnostic['actual_backend']=self.redis(2,"local keys=redis.call('KEYS','celery-tasks:celery-task-meta-*');if #keys>64 then return redis.error_reply('restart metadata count budget')end;local out={};for _,k in ipairs(keys)do if redis.call('STRLEN',k)>262144 then return redis.error_reply('restart metadata size budget')end;out[#out+1]={k,redis.call('GET',k)}end;return out")
   except BaseException as error:diagnostic['diagnostic_failure']=type(error).__name__+':'+str(error)[:512]
   save(self.output/'restart-before-teardown-diagnostic.json',diagnostic)
   raise
  rows=actual();frames=[json.loads(raw)for raw in plan['queue_before']]
  results={}
  for frame in frames:
   key=frame['headers']['id'];raw=self.redis(2,"return redis.call('GET','celery-tasks:celery-task-meta-'..ARGV[1])or false",key);results[key]=None if not raw else json.loads(raw)
  old=plan['old_frame'];old_key=old['frame']['headers']['id'];deleted_key=plan['deleted_frames'][plan['deleted']]['frame']['headers']['id'];revoked_key=plan['deleted_frames'][plan['revoked']]['frame']['headers']['id']
  pending=self.redis(1,"return redis.call('LRANGE','dvr',0,-1)")
  pending_ids={json.loads(raw)['headers']['id']for raw in pending}
  future=self.sql("from django_celery_beat.models import PeriodicTask;print(json.dumps(list(PeriodicTask.objects.filter(name='dvr-recording-"+str(plan['future'])+"').values('id','clocked_id','args'))))")
  end_events=self.sql("from core.models import SystemEvent;print(json.dumps(list(SystemEvent.objects.filter(event_type='recording_end').values()),default=str))")
  original_window=all(any((e.get('details')or{}).get('recording_id')==i and datetime.datetime.fromisoformat((e['details']['start_time']).replace('Z','+00:00'))==datetime.datetime.fromisoformat(str(rows[i]['start_time'])) and datetime.datetime.fromisoformat((e['details']['end_time']).replace('Z','+00:00'))==datetime.datetime.fromisoformat(str(rows[i]['end_time']))for e in end_events)for i in [plan['expired'],plan['resume'],plan['missing'],plan['old'],plan['immediate']])
  snap=self.snapshot('restart-final')
  media_ids=[i for i in [plan['expired'],plan['resume'],plan['old'],plan['immediate']]if(rows[i].get('custom_properties')or{}).get('remux_success')is True]
  result={'actual_rows':rows,'actual_backend_results':results,'actual_raw_source_frames':plan['queue_before'],'actual_pending_frames':pending,'actual_future_tasks':future,'actual_end_events':end_events,'actual_revoke_publication':self._restart_control,
   'old_frame_actual_cas':old_key not in pending_ids and results.get(old_key) is not None,
   'named_and_UUID_keys_preserved':all(v is None or v.get('task_id')==key for key,v in results.items())and any(re.fullmatch('[0-9a-f-]{36}',k)for k in results)and any(k.startswith('dvr-now-')for k in results),
   'due_actual_media':all(i in media_ids for i in [plan['resume'],plan['old'],plan['immediate']]),
   'expired_actual_remux':plan['expired']in media_ids and(rows[plan['expired']].get('custom_properties')or{}).get('interrupted_reason')=='server_restarted_after_end',
   'expired_missing_playlist_interrupted':(rows[plan['missing']].get('custom_properties')or{}).get('remux_success')is False and(rows[plan['missing']].get('custom_properties')or{}).get('interrupted_reason')=='server_restarted_after_end',
   'future_periodic_repaired':len(future)==1,
   'queued_deleted_row_consumed':results.get(deleted_key,{}).get('status')=='SUCCESS'if results.get(deleted_key)is not None else False,
   'revoked_deleted_row_consumed':results.get(revoked_key,{}).get('status')=='REVOKED'and results.get(revoked_key,{}).get('children')==[]and revoked_key not in pending_ids if results.get(revoked_key)is not None else False,
   'original_recording_window_preserved':original_window,
   'all_owned_recording_children_retired':snap['all_owned_recording_children_retired'],'actual_media_recording_ids':media_ids}
  return result
 def native_held_drain(self):
  if self.backend!='Native':raise ValueError('NativeFailClosedDiagnosticOnly')
  import urllib.request,urllib.error,threading
  start=utc()+datetime.timedelta(seconds=180);payload={'channel':self.channel_id,'start_time':start.isoformat(),'end_time':(start+datetime.timedelta(seconds=60)).isoformat(),'custom_properties':{}}
  req=urllib.request.Request(self.base_url+'/api/channels/recordings/',data=json.dumps(payload).encode(),headers={'Authorization':'Bearer '+self.admin,'Content-Type':'application/json'},method='POST')
  with urllib.request.urlopen(req,timeout=self.remaining(15))as response:row=json.loads(bounded_read(response,65536,self.remaining));code=response.status
  if code!=201:raise AssertionError(('ActualHeldBooking',code))
  self.start_sql_lock(row['id'],'held-row-lock');request_result={}
  def stop_request():
   request=urllib.request.Request(self.base_url+f"/api/channels/recordings/{row['id']}/stop/",data=b'{}',headers={'Authorization':'Bearer '+self.admin,'Content-Type':'application/json'},method='POST')
   try:
    with urllib.request.urlopen(request,timeout=self.remaining(90))as response:request_result.update(status=response.status,bytes=len(bounded_read(response,65536,self.remaining)))
   except Exception as error:request_result.update(failure=type(error).__name__)
  thread=threading.Thread(target=stop_request);thread.start()
  blocked=self.wait(lambda:self.sql("with connection.cursor()as c:\n c.execute(\"SELECT pid,wait_event_type,query FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query ILIKE '%UPDATE dispatcharr_channels_recording%'\");print(json.dumps(c.fetchall()))"),15,'actual-HTTP-write-held')
  native=self.stop_native(expected=1,seconds=55)
  # Only AFTER real process exit release the external blocker; never replay the
  # unknown write or turn a disconnected request into a successful stop result.
  self.role('held-row-lock',[],stop=True);thread.join(self.remaining(10))
  result={'actual_blocked_SQL':blocked,'native_exit':native,'actual_HTTP_observer':request_result,'observer_thread_settled':not thread.is_alive(),'unknown_write_not_replayed':True,'actual_row_after_release':self.read_rows()};save(self.output/'held-drain.json',result)
  if native['actual_exit']!=1 or native['oom_killed']or thread.is_alive()or request_result.get('status')in(200,201,204):raise AssertionError(('ActualFailClosedDrain',result))
  return result
 def cleanup_proof(self):
  if self._cleanup is None:raise RuntimeError('ActualFixtureCleanupNotYetCollected')
  return self._cleanup
 def close(self):
  self.begin_cleanup()
  errors=[]
  if self._sampler_stop:self._sampler_stop.set()
  if self.compose:
   try:self.capture_containers()
   except Exception as error:errors.append('cleanup-identity:'+type(error).__name__)
  if self.viewer:
   try:self.stop_ordinary_viewer()
   except Exception as e:errors.append(type(e).__name__)
  if self.native:
   try:self.stop_native(expected=None)
   except Exception as e:errors.append(type(e).__name__)
  try:self.stop_roles()
  except Exception as e:errors.append(type(e).__name__)
  if self._sampler_stop:self._sampler_stop.set()
  if self._sampler:
   try:self._sampler.join(self.remaining(5))
   except Exception as error:errors.append('sampler:'+type(error).__name__)
   if self._sampler.is_alive():errors.append('ActualSamplerThreadUnsettled')
  if self.compose:
   try:self.command(self.compose+['down','--volumes','--remove-orphans','--timeout','10'],90,allow=True)
   except Exception as e:errors.append(type(e).__name__)
  # docker run utility/Main containers also have the exact project label. Only
  # the independently observed identities in this fixture may be removed.
  for row in self.containers:
   cid=row['Id']
   try:self.command([self.docker,'rm','-f',cid],20,allow=True)
   except Exception as e:errors.append(type(e).__name__)
  inventories={}
  for kind in ['container','network','volume']:
   args=[self.docker,kind,'ls']+(['--all']if kind=='container'else[])+['--filter','label=com.docker.compose.project='+self.project,'--format','{{.ID}}'if kind!='volume'else'{{.Name}}']
   try:inventories[kind]=self.command(args,20).splitlines()
   except Exception as e:inventories[kind]=['unknown'];errors.append(type(e).__name__)
  # Runtime task-exit notification can precede kernel generation retirement.
  # Wait boundedly for actual absence; preserve false if it does not settle.
  try:
   until=time.monotonic()+self.remaining(5)
   while any(row.get('pid')and self.pid_start(row['pid'])==row['starttime']for row in self.containers):
    if time.monotonic()>=until:break
    self.sleep(min(.05,until-time.monotonic()))
  except Exception as error:errors.append('generation-settlement:'+type(error).__name__)
  cid_absence=[]
  for row in self.containers:
   # Exact missing-object stderr is retained in command receipts; an arbitrary
   # nonzero exit is not used as missing-object authority.
   missing=False
   try:
    self.command([self.docker,'inspect',row['Id']],20,allow=True);receipt=self.cli[-1];stderr=Path(receipt['stderr']).read_text().strip();stdout=Path(receipt['stdout']).read_text().strip();missing=receipt['actual_exit']==1 and stdout in('','[]')and stderr.casefold()==('error: no such object: '+row['Id']).casefold()
   except Exception as error:errors.append('exact-CID:'+type(error).__name__)
   cid_absence.append({'Id':row['Id'],'actual_missing_object':missing,'original_host_generation_absent':None if not row.get('pid')else self.pid_start(row['pid'])!=row['starttime']})
  observed=[r for r in cid_absence if r['original_host_generation_absent']is not None]
  self._cleanup={'project':self.project,'captured_original_process_generations':[{key:row.get(key)for key in('Id','pid','starttime','image')}for row in self.containers],'inventories':inventories,'exact_CID_absence':cid_absence,'errors':errors,'exact_owned_absence':not errors and not any(inventories.values())and all(r['actual_missing_object']for r in cid_absence),'all_original_process_generations_absent':bool(observed)and all(r['original_host_generation_absent']for r in observed),'host_CLI_groups_settled':all(r['process_group_settled']for r in self.cli)}
  save(self.output/'independent-cleanup.json',self._cleanup)

 def core_module(self):
  import importlib.util
  path=Path(self.binding['core_context_adapter'])
  if hashlib.sha256(path.read_bytes()).hexdigest()!=self.binding['core_context_adapter_sha256']:raise ValueError('ExactCoreContextAdapterRequired')
  spec=importlib.util.spec_from_file_location('owned_bound_core_context',path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module
 def run_core_context(self,output):
  return self.core_module().run(self,output)

@contextlib.contextmanager
def open_fixture(binding,backend,context):
 fixture=Fixture(binding,backend,context)
 try:fixture.bootstrap();yield fixture
 except BaseException as e:fixture.failure=type(e).__name__;raise
 finally:fixture.close()
