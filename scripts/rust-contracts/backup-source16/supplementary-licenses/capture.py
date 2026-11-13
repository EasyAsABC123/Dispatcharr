"""Owned never-started pinned image supplementary license bytes; no imports/start."""
from pathlib import Path
import hashlib,json,re,subprocess,uuid
ROOT=Path(__file__).resolve().parent;IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f';project='dispatcharr-backup-licenses-'+uuid.uuid4().hex[:10];cid=None;commands=[];files=[]
def run(args,allow=False):
 p=subprocess.run(['docker',*args],stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=30);assert len(p.stdout)+len(p.stderr)<=1048576;commands.append({'argv':['docker',*args],'exit':p.returncode,'stdout':p.stdout.decode(errors='replace'),'stderr':p.stderr.decode(errors='replace')});(ROOT/'commands.json').write_text(json.dumps(commands,indent=2)+'\n');assert allow or p.returncode==0;return p
try:
 cid=run(['create','--label','com.docker.compose.project='+project,'--name',project,'--network','none','--read-only','--entrypoint','/bin/true',IMAGE]).stdout.decode().strip();assert re.fullmatch('[a-f0-9]{64}',cid);before=json.loads(run(['inspect',cid]).stdout)[0];assert before['State']['Pid']==0 and before['State']['Status']=='created'and before['HostConfig']['ReadonlyRootfs']is True and before['HostConfig']['NetworkMode']=='none'
 def capture(relative):
  target=ROOT/'bytes'/relative;target.parent.mkdir(parents=True,exist_ok=True);run(['cp',cid+':/dispatcharrpy/lib/python3.13/site-packages/'+relative,str(target)]);data=target.read_bytes();assert len(data)<=262144 and sum(x['bytes']for x in files)+len(data)<=1048576;files.append({'path':str(target.relative_to(ROOT)),'runtime_path':'/dispatcharrpy/lib/python3.13/site-packages/'+relative,'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest()});return data
 for package in('celery','kombu'):
  data=capture(package+'/__init__.py');match=re.search(rb"__version__\s*=\s*['\"]([^'\"]+)['\"]",data);assert match;version=match[1].decode();assert re.fullmatch('[0-9.]+',version);capture(package+'-'+version+'.dist-info/licenses/LICENSE')
 after=json.loads(run(['inspect',cid]).stdout)[0];assert after['State']['Pid']==0 and after['State']['Status']=='created'
finally:
 if cid is not None:run(['rm',cid])
absent=run(['inspect',cid],allow=True);inventory=run(['container','ls','--all','--no-trunc','--filter','label=com.docker.compose.project='+project,'--format','{{.ID}}']);assert absent.returncode!=0 and json.loads(absent.stdout)==[]and not inventory.stdout
(ROOT/'proof.json').write_text(json.dumps({'passed':True,'supplementary_only_not_executed_Source108_input':True,'image':IMAGE,'project':project,'CID':cid,'before':before,'after':after,'files':files,'total_bytes':sum(x['bytes']for x in files),'container_started':False,'container_programs_or_app_imports':0,'PID0_before_after':True,'exact_CID_and_project_absent':True},indent=2)+'\n');print(json.dumps({'passed':True,'files':len(files),'total_bytes':sum(x['bytes']for x in files),'never_started':True}))
