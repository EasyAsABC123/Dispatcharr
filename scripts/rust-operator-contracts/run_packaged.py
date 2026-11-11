"""Actual shipped safe CLI on owned official PostgreSQL/Redis; observer only is Python.
Original Dispatcharr authors retain all application/algorithm credit.
"""
import argparse,hashlib,json,os,pathlib,select,shutil,subprocess,sys,tarfile,time,uuid
from datetime import datetime,timezone
R=pathlib.Path(__file__).resolve().parents[2];OWN=pathlib.Path(__file__).resolve().parent
p=argparse.ArgumentParser();p.add_argument('--output',type=pathlib.Path,required=True);p.add_argument('--build-manifest',type=pathlib.Path,required=True);p.add_argument('--source-cases',type=pathlib.Path,required=True);args=p.parse_args();out=args.output.resolve();out.mkdir()
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
build=json.loads(args.build_manifest.read_text());assert build['passed'];image=build['tag'];inspected=json.loads(subprocess.check_output(['docker','image','inspect',image]))[0];assert inspected['Id']==build['image_id'];assert inspected['Config']['Labels']['org.opencontainers.image.source-tree-sha256']==build['source_tree_sha256']
shutil.copyfile(args.source_cases,out/'source-cases.json');shutil.copyfile(args.build_manifest,out/'build-manifest.json');tools=out/'tool-source';tools.mkdir();shutil.copyfile(OWN/'package_observer.py',tools/'observer.py');shutil.copyfile(OWN/'run_packaged.py',tools/'run.py');shutil.copyfile(OWN/'operator_compare.py',tools/'operator_compare.py')
base=build.get('base_commit') or build['git_revision'];schema=subprocess.check_output(['git','show',base+':rust/crates/store/schema/original-1241b3-schema.sql'],cwd=R);(out/'schema.sql').write_bytes(schema)
archive=subprocess.check_output(['git','archive','1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'],cwd=R);(out/'original.tar').write_bytes(archive);original=out/'original';original.mkdir();subprocess.run(['tar','-xf',str(out/'original.tar'),'-C',str(original)],check=True)
nonce=uuid.uuid4().hex[:10];database='rust_probe_backup_operator_'+nonce;project='dispatcharr-operator-package-'+nonce
pg='postgres@sha256:665dc7b37ac52a17e298511414c21d23ffa4e6a6ef6225b3e5d59c7afb35e994';redis='redis@sha256:c5d34a98396a4775e223c74e929162c35c5a2182901ba26b50ade939941ce14b';baseline='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
compose={'services':{'database':{'image':pg,'environment':{'POSTGRES_DB':database,'POSTGRES_USER':'dispatch','POSTGRES_PASSWORD':'playback-fixture-only'},'tmpfs':['/var/lib/postgresql/data:mode=1777,size=1073741824']},'cache':{'image':redis,'command':['redis-server','--save','','--appendonly','no','--requirepass','playback-fixture-only'],'tmpfs':['/data:mode=1777,size=134217728']},'operator':{'image':image,'entrypoint':['/bin/sh','-c','exec sleep 600'],'read_only':True,'user':'65532:65532','cap_drop':['ALL'],'security_opt':['no-new-privileges:true'],'environment':{'DATABASE_URL':'postgresql://dispatch:playback-fixture-only@database:5432/'+database,'REDIS_URL':'redis://:playback-fixture-only@cache:6379/0'}},'observer':{'image':baseline,'entrypoint':['/bin/sh','-c','exec sleep 600'],'read_only':True,'tmpfs':['/tmp:mode=1777,size=134217728'],'cap_drop':['ALL'],'security_opt':['no-new-privileges:true'],'environment':{'POSTGRES_DB':database,'POSTGRES_USER':'dispatch','POSTGRES_PASSWORD':'playback-fixture-only','POSTGRES_HOST':'database','POSTGRES_PORT':'5432','REDIS_HOST':'cache','REDIS_PORT':'6379','REDIS_PASSWORD':'playback-fixture-only','OPERATOR_DISPOSABLE_FIXTURE':'playback-fixture-only'},'volumes':[str(original)+':/app:ro',str(tools)+':/operator-tools:ro',str(out)+':/evidence:ro']}},'networks':{'default':{'internal':True}}}
(out/'compose.yml').write_text(json.dumps(compose));cmd=['docker','compose','-p',project,'-f',str(out/'compose.yml')]
report={'passed':False,'scope':'Private packaged native safe CLI on official owned PostgreSQL/Redis; Original Python container is read/fixture/Channels observer only, no Original command/runtime service','image_id':build['image_id'],'source_tree_sha256':build['source_tree_sha256'],'build_manifest_sha256':sha(args.build_manifest),'source_cases_sha256':sha(args.source_cases),'tool_sha256':{p.name:sha(p) for p in tools.iterdir()},'schema_sha256':hashlib.sha256(schema).hexdigest(),'original_archive_sha256':hashlib.sha256(archive).hexdigest(),'project':project,'cases':[],'native_help_and_stderr_exact':False,'full_django_cli_verified':False};peer=None;err=None

def command(arguments,**kwargs):return subprocess.run(cmd+arguments,capture_output=True,timeout=90,**kwargs)
def read(seconds=20):
 deadline=time.monotonic()+seconds
 while time.monotonic()<deadline:
  if not select.select([peer.stdout],[],[],max(.01,deadline-time.monotonic()))[0]:break
  line=peer.stdout.readline()
  if not line:raise RuntimeError('Observer exited')
  try:return json.loads(line)
  except json.JSONDecodeError:continue
 raise TimeoutError('Bounded observer response')
def send(data):peer.stdin.write(json.dumps(data)+'\n');peer.stdin.flush();return read()
try:
 result=command(['up','-d']);assert result.returncode==0,result.stderr[-2000:]
 deadline=time.monotonic()+90
 while True:
  result=command(['exec','-T','database','pg_isready','-h','127.0.0.1','-U','dispatch','-d',database])
  cache=command(['exec','-T','cache','redis-cli','-a','playback-fixture-only','ping'])
  if result.returncode==cache.returncode==0:break
  if time.monotonic()>deadline:raise TimeoutError('Official database/cache ready')
  time.sleep(.2)
 result=command(['exec','-T','-e','PGPASSWORD=playback-fixture-only','database','psql','-h','127.0.0.1','-U','dispatch','-d',database,'-v','ON_ERROR_STOP=1'],input=schema);(out/'schema-apply.log').write_bytes(result.stdout+result.stderr);assert result.returncode==0
 err=(out/'observer-stderr.log').open('w');peer=subprocess.Popen(cmd+['exec','-T','observer','/dispatcharrpy/bin/python','/operator-tools/observer.py'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=err,text=True);ready=read();assert ready['ready']
 for index,case in enumerate(json.loads((out/'source-cases.json').read_text())['cases']):
  assert send({'op':'restore','index':index})['restored']
  start=datetime.now(timezone.utc);result=command(['exec','-T','operator','/usr/local/bin/dispatcharr-operator',*case['arguments']]);end=datetime.now(timezone.utc)
  observed=send({'op':'collect','index':index,'start':start.isoformat(),'end':end.isoformat()});checks=observed['checks'];checks['actual_packaged_exit']=result.returncode==case['exit_code']
  if not case['id'].endswith('-help'):checks['stdout_exact']=result.stdout.decode()==case['stdout']
  row={'id':case['id'],'arguments':case['arguments'],'execution_window':{'start':start.isoformat(),'end':end.isoformat()},'exit_code':result.returncode,'stdout':result.stdout.decode(),'stderr':result.stderr.decode(),'checks':checks,'passed':all(checks.values()),'after':observed['after'],'events':observed['events']};report['cases'].append(row);(out/'proof.json').write_text(json.dumps(report,indent=2)+'\n')
 report['passed']=all(row['passed'] for row in report['cases']) and len(report['cases'])==17
except BaseException as error:report['failure']=type(error).__name__+': '+str(error)[:2000]
finally:
 if peer:
  try:peer.stdin.write('{"op":"close"}\n');peer.stdin.flush();peer.wait(timeout=10)
  except Exception:peer.kill();peer.wait()
 if err:err.close()
 result=command(['down','--volumes','--remove-orphans']);report['cleanup_passed']=result.returncode==0;report['passed']=report['passed'] and report['cleanup_passed'];(out/'proof.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps({'passed':report['passed'],'cases':len(report['cases']),'failure':report.get('failure')}));raise SystemExit(0 if report['passed'] else 1)
