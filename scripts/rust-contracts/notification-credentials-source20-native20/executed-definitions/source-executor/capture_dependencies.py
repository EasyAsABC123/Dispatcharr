#!/usr/bin/env python3
"""Data-only pinned-image bytes. Never starts container or executes its programs."""
import argparse,ast,datetime,hashlib,json,os,subprocess,uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parent

def main():
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--capture-owned-bytes',action='store_true');args=parser.parse_args();plan=json.loads((ROOT/'dependency-capture-plan.json').read_text())
 if not args.capture_owned_bytes:print(json.dumps(plan,indent=2));return
 out=ROOT/'dependency-capture';out.mkdir(exist_ok=False);dest=ROOT/'inputs/actual-image';dest.mkdir(exist_ok=False);project='dispatcharr-notifcred-deps-'+uuid.uuid4().hex[:10];cid=None;proof={'state':'CAPTURE_ATTEMPT','container_started':False,'app_imports':False,'container_program_executions':0,'project':project,'image':plan['image'],'files':[],'commands':[],'started_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()};serial=0
 def save():(out/'proof.json').write_text(json.dumps(proof,indent=2)+'\n')
 def cmd(argv,check=True):
  nonlocal serial
  serial+=1;r=subprocess.run(argv,stdin=subprocess.DEVNULL,capture_output=True,timeout=30);row={'argv':argv,'exit':r.returncode,'stdout_hex':r.stdout.hex(),'stderr_hex':r.stderr.hex(),'utc':datetime.datetime.now(datetime.timezone.utc).isoformat()};proof['commands'].append(row);(out/f'command-{serial:03d}.json').write_text(json.dumps(row,indent=2)+'\n');save()
  if check and r.returncode!=0:raise RuntimeError('OwnedByteCommandExit'+str(r.returncode))
  return r
 try:
  proof['image_inspect']=json.loads(cmd(['docker','image','inspect',plan['image']]).stdout)
  cid=cmd(['docker','create','--name',project,'--label','com.docker.compose.project='+project,'--network','none','--read-only','--cpus','0.25','--memory','64m','--pids-limit','16','--entrypoint','/bin/true',plan['image']]).stdout.decode().strip();assert len(cid)==64;proof['CID']=cid
  first=json.loads(cmd(['docker','inspect',cid]).stdout)[0];proof['created_inspect']=first;assert first['State']['Status']=='created'and first['State']['Pid']==0 and first['State']['Running']is False and first['HostConfig']['ReadonlyRootfs']is True and first['HostConfig']['NetworkMode']=='none';used=0
  for rel in plan['files']:
   relative=Path(rel);assert not relative.is_absolute()and'..'not in relative.parts;f=dest/relative;f.parent.mkdir(parents=True,exist_ok=True);runtime=plan['site_packages']+'/'+rel;cmd(['docker','cp',cid+':'+runtime,str(f)]);assert f.is_file()and not f.is_symlink();raw=f.read_bytes();used+=len(raw);assert len(raw)<=plan['max_single_bytes']and used<=plan['max_total_bytes'];proof['files'].append({'path':str(f.relative_to(ROOT)),'runtime_path':runtime,'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw)});save()
  last=json.loads(cmd(['docker','inspect',cid]).stdout)[0];proof['final_before_remove']=last;assert last['State']['Status']=='created'and last['State']['Pid']==0 and last['State']['Running']is False;assert len(proof['files'])==plan['max_files']==23;proof['total_bytes']=used;proof['copied_all23']=True;proof['literal_versions']={}
  for pkg in ('rest_framework_simplejwt','jwt'):
   tree=ast.parse((dest/pkg/'__init__.py').read_bytes())
   for n in tree.body:
    if isinstance(n,ast.Assign)and any(isinstance(t,ast.Name)and t.id=='__version__'for t in n.targets)and isinstance(n.value,ast.Constant):proof['literal_versions'][pkg]=n.value.value
  proof['state']='EXACT_NEVER_STARTED_IMAGE_BYTES_CAPTURED'
 finally:
  if cid is not None:
   cmd(['docker','rm','--volumes','--force',cid],check=False);inventory=cmd(['docker','container','ls','--all','--no-trunc','--filter','id='+cid,'--format','{{.ID}}']);proof['exact_CID_absent']=not inventory.stdout.strip();project_inventory=cmd(['docker','container','ls','--all','--no-trunc','--filter','label=com.docker.compose.project='+project,'--format','{{.ID}}']);proof['all_project_containers_absent']=not project_inventory.stdout.strip()
   proof['anonymous_volume_absence']=[]
   for mount in proof.get('created_inspect',{}).get('Mounts',[]):
    if mount.get('Type')=='volume':
     name=mount['Name'];r=cmd(['docker','volume','ls','--filter','name='+name,'--format','{{.Name}}']);proof['anonymous_volume_absence'].append({'name':name,'raw_hex':r.stdout.hex(),'absent':name not in r.stdout.decode().splitlines()})
  proof['completed_utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();proof['passed']=proof['state']=='EXACT_NEVER_STARTED_IMAGE_BYTES_CAPTURED'and proof.get('exact_CID_absent')is True and proof.get('all_project_containers_absent')is True and all(x['absent']for x in proof.get('anonymous_volume_absence',[]));save();(ROOT/'inputs/dependency-proof.json').write_bytes((out/'proof.json').read_bytes())
 print(json.dumps({k:proof[k]for k in ('state','passed','CID','total_bytes','literal_versions','exact_CID_absent','container_started','container_program_executions')},indent=2));assert proof['passed']
if __name__=='__main__':main()
