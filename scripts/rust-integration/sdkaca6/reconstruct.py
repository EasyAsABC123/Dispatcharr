#!/usr/bin/env python3
"""Reconstruct exact Gitaca6 source/private Git context locally; never runs SDK/apps."""
import argparse,io,json,os,subprocess,tarfile
from pathlib import Path,PurePosixPath
from validate import REF,archive_git,sha,require,validate
HERE=Path(__file__).resolve().parent

def reconstruct(repo,destination):
 validate(HERE);out=destination.resolve();require(not out.exists(),'FreshPrivateDestinationOnly');raw=archive_git(repo);expected=json.loads((HERE/'raw/all-source-inputs.json').read_text());plan=json.loads((HERE/'raw/plan.json').read_text());require(sha(raw)==plan['archive_sha256'],'ExactArchive');source=out/'source';source.mkdir(parents=True)
 with tarfile.open(fileobj=io.BytesIO(raw))as archive:
  for entry in archive.getmembers():
   path=PurePosixPath(entry.name);require(not path.is_absolute()and'..'not in path.parts and(entry.isfile()or entry.isdir()),'SafeRegularArchive')
  archive.extractall(source,filter='data')
 actual={str(p.relative_to(source)):sha(p.read_bytes())for p in source.rglob('*')if p.is_file()};require(actual==expected,'Exact8273ReconstructedFiles')
 env={k:v for k,v in os.environ.items()if not k.startswith('GIT_')};env.update(GIT_CONFIG_GLOBAL='/dev/null',GIT_CONFIG_NOSYSTEM='1');meta=out/'git-metadata';subprocess.run(['git','init','--bare','--template=',str(meta)],env=env,capture_output=True,check=True)
 common=subprocess.check_output(['git','-C',str(repo.resolve()),'rev-parse','--path-format=absolute','--git-common-dir'],env=env,text=True).strip();(meta/'HEAD').write_text(REF+'\n');alternate=meta/'objects/info/alternates';alternate.write_text(str(Path(common)/'objects')+'\n');alternate.chmod(0o444)
 for key,value in [('core.bare','false'),('gc.auto','0')]:subprocess.run(['git','config','--file',str(meta/'config'),key,value],env=env,check=True)
 private={'GIT_DIR':str(meta),'GIT_WORK_TREE':str(source),'GIT_INDEX_FILE':str(meta/'index'),'GIT_OBJECT_DIRECTORY':str(meta/'objects'),'GIT_OPTIONAL_LOCKS':'0'};env.update(private);subprocess.run(['git','read-tree',REF],cwd=source,env=env,check=True);head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=source,env=env,text=True).strip();status=subprocess.check_output(['git','status','--short'],cwd=source,env=env,text=True).strip();require(head==REF and not status,'PrivateDetachedHeadAndIndexExact')
 (out/'private-environment.json').write_text(json.dumps(private,indent=2)+'\n');return {'passed':True,'ref':REF,'source_files':len(actual),'archive_bytes':len(raw),'archive_sha256':sha(raw),'private_detached_HEAD':head,'private_git_status':status,'alternate_mode':oct(alternate.stat().st_mode&0o777),'SDK_Docker_Cargo_application_network_runs':0}
if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--repo',type=Path,required=True);p.add_argument('--destination',type=Path,required=True);a=p.parse_args();print(json.dumps(reconstruct(a.repo,a.destination),indent=2))
