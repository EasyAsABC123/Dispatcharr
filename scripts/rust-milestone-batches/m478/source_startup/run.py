"""One bounded new Source normal-startup context; explicit --execute required."""
import argparse,hashlib,importlib.util,json,os,re,shutil,sys,tarfile,time,uuid
from pathlib import Path
ROOT=Path('/home/justin/github/Dispatcharr');HERE=Path(__file__).resolve().parent
IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
PRIOR=ROOT/'artifacts/rust/milestone-batch2-plugin41-5';ORIGINAL=ROOT/'artifacts/rust/m4-m7-m8-wide-batch-run-2-source16/source'
def need(x,label):
 if not x:raise ValueError(label)
def save(path,v):path.write_text(json.dumps(v,indent=2,allow_nan=False)+'\n')
def module(path,name):
 s=importlib.util.spec_from_file_location(name,path);m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m

def run(out):
 need(out.is_relative_to(ROOT/'artifacts/rust')and not out.exists(),'FreshOwnedOutput');out.mkdir(mode=0o700);tools=out/'tools';tools.mkdir();inputs=out/'inputs';inputs.mkdir(mode=0o700)
 for n in ['normal_http.py','seed.py']:shutil.copyfile(HERE/n,tools/n)
 for n in ['start.sh','native_state.py','public_manifest.py','redis_channels.py','absolute_http.py','reader.py','bootstrap_env.py']:shutil.copyfile(PRIOR/'tools'/n,tools/n)
 for n in ['transport16-source.json','schema62-authority.json']:shutil.copyfile(PRIOR/'inputs'/n,inputs/n)
 shutil.copytree(PRIOR/'inputs/pg-init',inputs/'pg-init');shutil.copytree(PRIOR/'schema',out/'schema')
 # Verify Original Python inputs against the exact retained original archive.
 archive=ROOT/'artifacts/rust/m8-pluginrepo-manifest-fetch-source-executor-6/inputs/original-source.tar'
 need(hashlib.sha256(archive.read_bytes()).hexdigest()=='f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff','ExactOriginalArchive')
 with tarfile.open(archive) as source_tar:
  for entry in source_tar.getmembers():
   if entry.isfile()and entry.name.endswith('.py'):
    need((ORIGINAL/entry.name).read_bytes()==source_tar.extractfile(entry).read(),'ExactOriginalPython:'+entry.name)
 # Only wrapper/helper bytes are new; the Source mount remains unchanged.
 identity={str(p.relative_to(ORIGINAL)):hashlib.sha256(p.read_bytes()).hexdigest()for p in ORIGINAL.rglob('*.py')if '__pycache__'not in p.parts};save(out/'Original-python-inputs.json',identity)
 cli=module(ROOT/'artifacts/rust/m4-m7-m8-wide-batch-1/prerequisites/assets/bounded_cli.py','bounded');envmod=module(tools/'bootstrap_env.py','env')
 project='dispatcharr-source-wsgi-'+uuid.uuid4().hex[:10];db='rust_probe_pluginrepo_'+uuid.uuid4().hex[:10];report={'project':project,'database':db,'image':IMAGE,'commands':[],'passed':False,'Source41_replays':0,'performance_claims':False,'original_function_replacements':False};save(out/'report.json',report);began=time.monotonic();cleaning=False;cid=None;serial=used=0
 def cmd(args,seconds=30,check=True):
  nonlocal serial,used
  need(serial<64 and used<12582912,'BoundedCommandsAndOutput');serial+=1;raw,err,row=cli.collect(['docker','--host','unix:///tmp/dispatcharr-docker.sock',*args],min(seconds,max(.01,(cleanup_began+90 if cleaning else began+480)-time.monotonic())),min(2097152,16777216-used));used+=len(raw)+len(err);(out/f'command-{serial:02d}.stdout').write_bytes(raw);(out/f'command-{serial:02d}.stderr').write_bytes(err);report['commands'].append(row);save(out/'report.json',report);need(row['invalid']is None and row['both_actual_EOF_observed']and not row['forced_termination'],'ActualWaitAndEOF');need(not check or row['actual_wait_exit']==row['actual_poll_exit']==0,'ActualCommand0');return raw
 compose=['compose','-p',project,'-f',str(out/'compose.json')]
 def dc(*args,seconds=30):return cmd(compose+list(args),seconds)
 environment=envmod.storage_environment();service={'image':IMAGE,'pull_policy':'never','network_mode':'none','entrypoint':['bash','/native-tools/start.sh'],'environment':environment,'volumes':[str(tools)+':/native-tools:ro',str(inputs)+':/native-root/inputs:ro',str(out/'schema')+':/native-source:ro',str(inputs/'pg-init')+':/pg-bootstrap:ro',str(ORIGINAL)+':/app:ro',str(out)+':/evidence'],'tmpfs':['/data:mode=1777,size=2147483648','/tmp:mode=1777,size=268435456'],'cpus':1,'mem_limit':'1536m','pids_limit':256}
 save(out/'compose.json',{'services':{'source':service}})
 appenv={'POSTGRES_DB':db,'DJANGO_SETTINGS_MODULE':'dispatcharr.settings','DJANGO_SECRET_KEY':'owned-functional-context-only','PYTHONPATH':'/native-tools:/app','REDIS_HOST':'127.0.0.1','REDIS_DB':'1','REDIS_URL':'redis://:playback-fixture-only@127.0.0.1:6379/1','CELERY_BROKER_URL':'redis://:playback-fixture-only@127.0.0.1:6379/2','CELERY_RESULT_BACKEND':'redis://:playback-fixture-only@127.0.0.1:6379/3','DISPATCHARR_SKIP_PLUGIN_AUTODISCOVERY':'1','DISPATCHARR_ENABLE_IP_LOOKUP':'false','DISPATCHARR_LOG_DIR':'/data/logs','HTTP_PROXY':'','HTTPS_PROXY':'','ALL_PROXY':'','http_proxy':'','https_proxy':'','all_proxy':'','NO_PROXY':'*'}
 def guest(script,seconds=30):
  args=['exec','-T'];
  for k,v in appenv.items():args+=['-e',k+'='+v]
  return dc(*args,'source','/dispatcharrpy/bin/python','-B','/native-tools/'+script,seconds=seconds)
 try:
  need(not cmd(['container','ls','--all','--filter','label=com.docker.compose.project='+project,'--format','{{.ID}}']).strip(),'FreshProject');dc('up','-d','--no-build','--pull','never','source',seconds=120)
  end=time.monotonic()+150
  while not(out/'bootstrap-ready').exists():need(time.monotonic()<end,'Bootstrap150s');time.sleep(.05)
  cid=dc('ps','--all','-q','source').decode().strip();actual=json.loads(cmd(['inspect',cid]))[0];save(out/'actual-container.json',actual);need(actual['HostConfig']['NetworkMode']=='none'and not actual['NetworkSettings']['Ports'],'NoNetworkOrPublishedPorts')
  dc('exec','-T','source','psql','-XAtq','-h','/var/run/postgresql','-U','dispatch','-d','postgres','-v','ON_ERROR_STOP=1','-c','CREATE DATABASE '+db+' TEMPLATE dispatcharr')
  guest('seed.py');guest('normal_http.py',seconds=120)
  capture=json.loads((out/'normal-startup.json').read_bytes());need(capture['capture_complete']and capture['actual_HTTP_calls']==4 and capture['default_unchanged'],'CompleteNormalContext');report['passed']=True
 except BaseException as error:report['error']={'type':type(error).__name__,'message':str(error)}
 finally:
  cleaning=True;cleanup_began=time.monotonic()
  try:
   dc('down','--volumes','--remove-orphans','--timeout','10',seconds=35)
   queries=[['container','ls','--all','--filter','label=com.docker.compose.project='+project,'--format','{{.ID}}'],['network','ls','--filter','label=com.docker.compose.project='+project,'--format','{{.ID}}'],['volume','ls','--filter','label=com.docker.compose.project='+project,'--format','{{.Name}}']]+([['container','ls','--all','--filter','id='+cid,'--format','{{.ID}}']]if cid else[])
   report['absence_queries']=[]
   for q in queries:
    raw=cmd(q);report['absence_queries'].append({'argv':q,'stdout':raw.decode()});need(not raw.strip(),'ExactOwnedAbsence')
   report['exact_cleanup']=True
  except BaseException as e:report['cleanup_error']={'type':type(e).__name__,'message':str(e)}
  pids=[]
  for c in report['commands']:
   try:now=Path('/proc',str(c['pid']),'stat').read_text().rsplit(')',1)[1].split()[19]
   except FileNotFoundError:now=None
   pids.append({'pid':c['pid'],'observed_starttime':c['observed_starttime'],'current_starttime':now,'original_absent':now!=c['observed_starttime']and c['observed_starttime']is not None})
  report['host_PID_absence']=pids;report['all_host_generations_absent']=all(x['original_absent']for x in pids);report['cleanup_seconds']=time.monotonic()-cleanup_began;report['passed']=report['passed']and report.get('exact_cleanup')is True and report['all_host_generations_absent'];report['actual_controller_exit']=0 if report['passed']else 2;save(out/'report.json',report)
 return report['actual_controller_exit']
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--execute',action='store_true');p.add_argument('--output',type=Path);a=p.parse_args()
 if not a.execute:print(json.dumps({'status':'not_run','new_context_HTTP_calls':4,'Source41_replays':0,'global_fixture_slots':1}));sys.exit(0)
 need(a.output is not None,'OutputRequired');sys.exit(run(a.output.absolute()))
