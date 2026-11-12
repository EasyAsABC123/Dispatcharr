"""Reconstruct immutable support and render commands; NEVER dispatch workloads.
Original Dispatcharr/dependency authors retain application/algorithm credit.
This portability utility is separate from the actually executed historical tools.
"""
import argparse,hashlib,io,json,os,subprocess,tarfile
from pathlib import Path
HERE=Path(__file__).resolve().parent
REV='f0bfc2f200dcfd1f64ae32a9855ea250731ce67f'
ORIGINAL='/home/justin/github/Dispatcharr'
def sha(b):return hashlib.sha256(b).hexdigest()
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--repository',required=True,type=Path);p.add_argument('--output',required=True,type=Path);a=p.parse_args();repo=a.repository.resolve();out=a.output.resolve()
 if out.exists():raise ValueError('Fresh destination required')
 closure=json.loads((HERE/'closure.json').read_text())
 for x in closure['files']:
  f=HERE/x['packet_path']
  if sha(f.read_bytes())!=x['sha256']:raise ValueError('Packet input changed: '+str(f))
 evidence=HERE/'evidence';mapfile=evidence/'artifacts/rust/image53-package-plan-2/support-inputs.json';inputmap=json.loads(mapfile.read_text())
 plan=json.loads((evidence/'artifacts/rust/image53-package-plan-2/plan.json').read_text())
 if inputmap['revision']!=REV or plan['support_revision']!=REV:raise ValueError('Support revision mismatch')
 if inputmap['file_count']!=len(inputmap['files'])or inputmap['file_count']!=6148:raise ValueError('Support count mismatch')
 if sha(mapfile.read_bytes())!=plan['support_input_map_sha256']:raise ValueError('Plan support-map binding differs')
 manifest='artifacts/rust/candidate-build-worker-url-notifications-image53-1/build.json';build=json.loads((evidence/manifest).read_text())
 if sha((evidence/manifest).read_bytes())!=plan['candidate']['manifest_sha256']or build['image_id']!=plan['candidate']['image_id']:raise ValueError('Candidate manifest binding differs')
 base_env={k:v for k,v in os.environ.items()if not k.startswith('GIT_')}
 archive=subprocess.check_output(['git','-C',str(repo),'archive','--format=tar',REV],env=base_env)
 if sha(archive)!=inputmap['support_archive_sha256']:raise ValueError('Immutable Git archive differs')
 out.mkdir(parents=True);support=out/'artifacts/rust/image53-package-plan-1/support';support.mkdir(parents=True)
 with tarfile.open(fileobj=io.BytesIO(archive))as t:t.extractall(support,filter='data')
 expected=inputmap['files']
 for name,digest in expected.items():
  f=support/name
  if not f.is_file()or sha(f.read_bytes())!=digest:raise ValueError('Support mismatch: '+name)
 actual={str(x.relative_to(support))for x in support.rglob('*')if x.is_file()}
 if actual!=set(expected):raise ValueError('Support file-set mismatch')
 gitdir=support/'.git';subprocess.run(['git','init','--bare','--quiet',str(gitdir)],env=base_env,check=True)
 objects=Path(subprocess.check_output(['git','-C',str(repo),'rev-parse','--git-path','objects'],env=base_env,text=True).strip());objects=objects if objects.is_absolute()else repo/objects
 (gitdir/'objects/info/alternates').write_text(str(objects.resolve())+'\n');(gitdir/'HEAD').write_text(REV+'\n')
 env=base_env|{'GIT_DIR':str(gitdir),'GIT_WORK_TREE':str(support),'GIT_INDEX_FILE':str(gitdir/'index'),'GIT_OPTIONAL_LOCKS':'0','DISPATCHARR_REPO':str(support),'PYTHONDONTWRITEBYTECODE':'1'}
 subprocess.run(['git','read-tree',REV],env=env,check=True)
 status=subprocess.check_output(['git','status','--porcelain','--untracked-files=no'],cwd=support,env=env,text=True)
 if status:raise ValueError('Reconstructed tracked support is dirty')
 def render(x):
  if isinstance(x,str):return x.replace(ORIGINAL,str(out))
  if isinstance(x,list):return[render(y)for y in x]
  if isinstance(x,dict):return{k:render(v)for k,v in x.items()}
  return x
 manifest='artifacts/rust/candidate-build-worker-url-notifications-image53-1/build.json';dest=out/manifest;dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes((evidence/manifest).read_bytes())
 rendered=render(plan)
 for group in rendered['groups']:group['dispatch_authorized']=False
 rendered['dispatch_authorized']=False
 (out/'rendered-plan.json').write_text(json.dumps(rendered,indent=2)+'\n')
 portable_env={k:env[k]for k in ['GIT_DIR','GIT_WORK_TREE','GIT_INDEX_FILE','GIT_OPTIONAL_LOCKS','DISPATCHARR_REPO','PYTHONDONTWRITEBYTECODE']};(out/'environment.json').write_text(json.dumps(portable_env,indent=2)+'\n')
 (out/'materialization.json').write_text(json.dumps({'scope':'Git-only support reconstruction and path-rendered command proposal; NO workload/cue/clearance replay','revision':REV,'support_archive_sha256':sha(archive),'verified_support_files':len(expected),'old_execution_plan_preserved_in_packet':True,'new_rendered_plan_is_unexecuted':True,'candidate_image_must_match_exact_recorded_ID':True},indent=2)+'\n')
 print(json.dumps({'materialized':True,'support_files':len(expected),'workloads_started':0,'output':str(out)}))
if __name__=='__main__':main()
