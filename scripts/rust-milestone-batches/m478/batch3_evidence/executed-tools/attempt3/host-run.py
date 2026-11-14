"""One bounded Native production-Main normal-startup context; explicit --execute required."""
import argparse,hashlib,importlib.util,json,os,re,shutil,sys,tarfile,time,uuid
from pathlib import Path
ROOT=Path('/home/justin/github/Dispatcharr');HERE=Path(__file__).resolve().parent
IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
PRIOR=ROOT/'artifacts/rust/milestone-batch2-plugin41-5'
def need(x,label):
 if not x:raise ValueError(label)
def save(path,v):path.write_text(json.dumps(v,indent=2,allow_nan=False)+'\n')
def module(path,name):
 s=importlib.util.spec_from_file_location(name,path);m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m

def run(out,candidate_path,reuse_finite_assets,finite_only):
 candidate=json.loads(candidate_path.read_bytes());gate=module(ROOT/'scripts/rust-milestone-batches/m478/batch.py','candidate_gate');need(gate.candidate_ready(candidate)and candidate.get('plugin_numeric_mode')=='finite-v1','ActualFiniteCandidateSourceBuildImageBinding')
 need(out.is_relative_to(ROOT/'artifacts/rust')and not out.exists(),'FreshOwnedOutput');out.mkdir(mode=0o700);tools=out/'tools';tools.mkdir();inputs=out/'inputs';inputs.mkdir(mode=0o700)
 shutil.copyfile(HERE/'native_http.py',tools/'native_http.py');shutil.copyfile(ROOT/'artifacts/rust/m478-finite-gpg-prep-1/finite_assets.py',tools/'finite_assets.py');shutil.copyfile(ROOT/'artifacts/rust/m478-source-startup-cache-prep-1/seed.py',tools/'seed.py')
 for n in ['start.sh','native_state.py','public_manifest.py','redis_channels.py','absolute_http.py','reader.py','bootstrap_env.py']:shutil.copyfile(PRIOR/'tools'/n,tools/n)
 for n in ['transport16-source.json','schema62-authority.json']:shutil.copyfile(PRIOR/'inputs'/n,inputs/n)
 save(inputs/'owned-finite-context.json',{'owned_fixture':True,'Original_application_runs':0,'public_contacts':0});shutil.copytree(PRIOR/'inputs/pg-init',inputs/'pg-init');shutil.copytree(PRIOR/'schema',out/'schema')
 build=json.loads(Path(candidate['build_manifest_file']).read_bytes())
 for schema_file in (out/'schema/rust/crates/store/schema').glob('*.sql'):
  name=str(schema_file.relative_to(out/'schema'));need(hashlib.sha256(schema_file.read_bytes()).hexdigest()==build['source_sha256'][name],'FixtureSchemaMatchesCandidateBuild:'+name)
 cli=module(ROOT/'artifacts/rust/m4-m7-m8-wide-batch-1/prerequisites/assets/bounded_cli.py','bounded');envmod=module(tools/'bootstrap_env.py','env')
 project='dispatcharr-native-startup-'+uuid.uuid4().hex[:10];db='rust_probe_pluginrepo_'+uuid.uuid4().hex[:10];report={'project':project,'database':db,'image':IMAGE,'commands':[],'passed':False,'Source41_replays':0,'finite_only':finite_only,'performance_claims':False,'Original_application_runs':0,'candidate':candidate};save(out/'report.json',report);began=time.monotonic();cleaning=False;cid=None;serial=used=0
 def cmd(args,seconds=30,check=True,combine=False,docker=True):
  nonlocal serial,used
  need(serial<64 and used<12582912,'BoundedCommandsAndOutput');serial+=1;raw,err,row=cli.collect((['docker','--host','unix:///tmp/dispatcharr-docker.sock',*args]if docker else args),min(seconds,max(.01,(cleanup_began+90 if cleaning else began+480)-time.monotonic())),min(2097152,16777216-used));used+=len(raw)+len(err);(out/f'command-{serial:02d}.stdout').write_bytes(raw);(out/f'command-{serial:02d}.stderr').write_bytes(err);report['commands'].append(row);save(out/'report.json',report);need(row['invalid']is None and row['both_actual_EOF_observed']and not row['forced_termination'],'ActualWaitAndEOF');need(not check or row['actual_wait_exit']==row['actual_poll_exit']==0,'ActualCommand0');return raw+err if combine else raw
 compose=['compose','-p',project,'-f',str(out/'compose.json')]
 def dc(*args,seconds=30):return cmd(compose+list(args),seconds)
 environment=envmod.storage_environment();service={'image':IMAGE,'pull_policy':'never','networks':{'owned':{'ipv4_address':'11.253.250.3'}},'dns':['127.0.0.1'],'sysctls':{'net.ipv6.conf.all.disable_ipv6':'1','net.ipv6.conf.default.disable_ipv6':'1'},'entrypoint':['bash','/native-tools/start.sh'],'environment':environment,'volumes':[str(tools)+':/native-tools:ro',str(inputs)+':/native-root/inputs:ro',str(out/'schema')+':/native-source:ro',str(inputs/'pg-init')+':/pg-bootstrap:ro',str(inputs/'owned-finite-context.json')+':/owned-finite-context:ro',str(out)+':/evidence'],'tmpfs':['/data:mode=1777,size=2147483648','/tmp:mode=1777,size=268435456'],'cpus':1,'mem_limit':'1536m','pids_limit':256}
 native={'image':candidate['image_id'],'pull_policy':'never','read_only':True,'network_mode':'service:source','environment':{'RUST_BIND':'0.0.0.0:9194','DATABASE_URL':'postgres://dispatch:playback-fixture-only@127.0.0.1:5432/'+db,'DJANGO_SECRET_KEY':'owned-functional-context-only','REDIS_URL':'redis://:playback-fixture-only@127.0.0.1:6379/1','TZ':'UTC','RUST_PLUGIN_GPG_TRACE':'1'},'tmpfs':['/tmp:mode=1777,size=134217728','/data:mode=1777,size=134217728'],'cpus':1,'mem_limit':'512m','pids_limit':128,'cap_drop':['ALL'],'security_opt':['no-new-privileges:true']}
 save(out/'compose.json',{'services':{'source':service,'candidate':native},'networks':{'owned':{'internal':True,'enable_ipv6':False,'driver_opts':{'com.docker.network.bridge.enable_ip_masquerade':'false'},'ipam':{'config':[{'subnet':'11.253.250.0/29','gateway':'11.253.250.1'}]}}}})
 appenv={'POSTGRES_DB':db,'PYTHONPATH':'/native-tools'}
 def guest(script,*tail,seconds=30):
  args=['exec','-T'];
  for k,v in appenv.items():args+=['-e',k+'='+v]
  return dc(*args,'source','/dispatcharrpy/bin/python','-B','/native-tools/'+script,*tail,seconds=seconds)
 try:
  guard=module(ROOT/'artifacts/rust/m8-pluginrepo-unsigned-native-normal-prep-2/controller/network_guard.py','network_guard');routes=json.loads(cmd(['ip','-j','-4','route','show','table','all'],docker=False));ids=cmd(['network','ls','--format','{{.ID}}']).decode().splitlines();need(len(ids)<=256,'BoundedNetworkInventory');existing=json.loads(cmd(['network','inspect',*ids]))if ids else[];save(out/'host-network-nonoverlap.json',guard.host_nonoverlap(routes,existing))
  need(not cmd(['container','ls','--all','--filter','label=com.docker.compose.project='+project,'--format','{{.ID}}']).strip(),'FreshProject');dc('up','-d','--no-build','--pull','never','source',seconds=120)
  end=time.monotonic()+150
  while not(out/'bootstrap-ready').exists():need(time.monotonic()<end,'Bootstrap150s');time.sleep(.05)
  cid=dc('ps','--all','-q','source').decode().strip();actual=json.loads(cmd(['inspect',cid]))[0];save(out/'actual-container.json',actual);need(actual['HostConfig']['NetworkMode']==project+'_owned'and not actual['NetworkSettings']['Ports'],'OwnedFencedNetworkAndNoPublishedPorts')
  dc('exec','-T','source','psql','-XAtq','-h','/var/run/postgresql','-U','dispatch','-d','postgres','-v','ON_ERROR_STOP=1','-c','CREATE DATABASE '+db+' TEMPLATE dispatcharr')
  guest('seed.py')
  if reuse_finite_assets is None:guest('finite_assets.py','--execute-owned-container','--output','/evidence/finite-assets',seconds=120)
  else:
   need(reuse_finite_assets.is_relative_to(ROOT/'artifacts/rust')and not any(q.is_symlink()for q in [reuse_finite_assets,*reuse_finite_assets.parents]),'OwnedRetainedFiniteAssets');proof=json.loads((reuse_finite_assets/'asset-generation-report.json').read_bytes());need(proof['passed']is True and proof['owned_temporary_home_absent']is True,'RetainedActualToolCryptoAndCleanup')
   for name,row in proof['asset_files'].items():
    if name=='asset-generation-report.json':continue # generator final report is intentionally not self-hashed
    f=reuse_finite_assets/name;need(f.is_file()and not f.is_symlink()and f.stat().st_size==row['bytes']and hashlib.sha256(f.read_bytes()).hexdigest()==row['sha256'],'ExactRetainedPublicCryptoAsset')
   shutil.copytree(reuse_finite_assets,out/'finite-assets');report['retained_crypto_tool_authority']={'path':str(reuse_finite_assets),'report_sha256':hashlib.sha256((reuse_finite_assets/'asset-generation-report.json').read_bytes()).hexdigest(),'private_home_already_removed':True,'new_key_generations':0}
  guest('native_http.py','before')
  dc('up','-d','--no-build','--pull','never','candidate');main_cid=dc('ps','--all','-q','candidate').decode().strip();report['Main_CID']=main_cid
  main_inspect=json.loads(cmd(['inspect',main_cid]))[0];save(out/'actual-Main.json',main_inspect);need(main_inspect['Image']==candidate['image_id']and main_inspect['Config']['User']=='65532:65532'and main_inspect['Config']['Entrypoint']==['/usr/local/bin/dispatcharr-rs'],'ActualProductionMainImageUIDEntrypoint');need(main_inspect['HostConfig']['NetworkMode']=='container:'+cid,'ActualOwnedSharedNetNS')
  guest('native_http.py','collect-finite'if finite_only else'collect',seconds=120)
  logs=cmd(['logs',main_cid],combine=True);(out/'Main.logs').write_bytes(logs)
  trace_module=module(ROOT/'scripts/rust-milestone-batches/m478/plugin_functional/gpg_trace.py','trace')
  captured=json.loads((out/'native-startup.json').read_bytes());finite=[q for q in captured['requests']if q['label'].startswith('finite-signed-')]
  receipts=[json.loads(f.read_bytes())for f in sorted((out/'finite-assets/receipts').glob('command-*.json'))];verified=[row for row in receipts if '--verify'in row['argv']];need(len(verified)==2 and [q['wait']for q in verified]==[0,1],'ActualToolValidInvalidWaits')
  reference={'requests':[{'index':i}for i in range(2)],'GPG_observer_terminal':{'value':{'direct_GPG_processes':[]}}}
  for i,actual in enumerate(verified):
   reference['GPG_observer_terminal']['value']['direct_GPG_processes'] += [{'case':i,'argv':['gpg','--import'],'actual_wait_result':[None,0]},{'case':i,'argv':['gpg','--verify'],'actual_wait_result':[None,actual['wait']<<8]}]
  proof=trace_module.validate(reference,{'capture_complete':captured['capture_complete'],'actual_request_count':2,'completed_capture_count':2,'requests':finite},logs);proof['reference_scope']='Import success required; verify statuses from actual owned tool GPG over Python Original canonical bytes, not Source application/API replay';save(out/'finite-GPG-review.json',proof)
  pids=[str(c['pid'])for c in proof['children']];absence=cmd(['exec',main_cid,'/bin/sh','-c','for pid do test ! -e "/proc/$pid" || exit 1; done','owned-GPG-absence',*pids]);save(out/'finite-GPG-live-namespace-absence.json',{'passed':True,'actual_children':proof['children'],'stdout':absence.decode(),'inside_actual_Main_PID_namespace':True})
  dc('stop','--timeout','10','candidate',seconds=20);stopped=json.loads(cmd(['inspect',main_cid]))[0];save(out/'stopped-Main.json',stopped);need(stopped['State']['Status']=='exited'and stopped['State']['ExitCode']==0,'ActualNormalMainExit0')
  capture=json.loads((out/'native-startup.json').read_bytes());need(capture['capture_complete']and capture['actual_HTTP_calls']==(2 if finite_only else 6) and capture['default_unchanged'],'CompleteNormalContext');report['passed']=True
 except BaseException as error:report['error']={'type':type(error).__name__,'message':str(error)}
 finally:
  cleaning=True;cleanup_began=time.monotonic()
  try:
   dc('down','--volumes','--remove-orphans','--timeout','10',seconds=35)
   queries=([['container','ls','--all','--filter','id='+report['Main_CID'],'--format','{{.ID}}']]if 'Main_CID'in report else[])+[['container','ls','--all','--filter','label=com.docker.compose.project='+project,'--format','{{.ID}}'],['network','ls','--filter','label=com.docker.compose.project='+project,'--format','{{.ID}}'],['volume','ls','--filter','label=com.docker.compose.project='+project,'--format','{{.Name}}']]+([['container','ls','--all','--filter','id='+cid,'--format','{{.ID}}']]if cid else[])
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
 p=argparse.ArgumentParser();p.add_argument('--execute',action='store_true');p.add_argument('--output',type=Path);p.add_argument('--candidate',type=Path);p.add_argument('--reuse-finite-assets',type=Path);p.add_argument('--finite-only',action='store_true');a=p.parse_args()
 if not a.execute:print(json.dumps({'status':'not_run','new_context_HTTP_calls':6,'cache_calls':4,'finite_signature_calls':2,'Source41_replays':0,'global_fixture_slots':1}));sys.exit(0)
 need(a.output is not None and a.candidate is not None,'OutputAndCandidateRequired');sys.exit(run(a.output.absolute(),a.candidate.absolute(),a.reuse_finite_assets.absolute()if a.reuse_finite_assets else None,a.finite_only))
