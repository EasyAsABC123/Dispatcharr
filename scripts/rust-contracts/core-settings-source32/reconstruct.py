#!/usr/bin/env python3
"""Restore exact reviewed Source32 controller inputs from public payloads + immutable Original Git archive. No app dispatch."""
import argparse,hashlib,json,os,shutil,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT/'executed-executor/controller'))
from bounded_process import Collected
REVISION='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
SEAL='a56abb23005deed4791f08cc986bb175098ca40dc699f156abb93f4927f6c5d8'
ARCHIVE='f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def need(ok,label):
 if not ok:raise ValueError(label)
def run(repo,out):
 repo=Path(repo).resolve();out=Path(out);need(not out.exists()and not out.is_symlink(),'FreshArtifactOutput');seal=ROOT/'executed-executor/seal.json';need(sha(seal)==SEAL,'ExactExecutedSource32Seal');data=json.loads(seal.read_bytes());need(data['Original_revision']==REVISION,'ExactOriginalRevision');shutil.copytree(ROOT/'executed-executor',out,copy_function=shutil.copyfile)
 archive=out/'inputs/original-source.tar';need(not archive.exists(),'NoPrivateArchiveFallback');env={k:v for k,v in os.environ.items()if not k.startswith('GIT_')};env.update(GIT_CONFIG_GLOBAL='/dev/null',GIT_CONFIG_SYSTEM='/dev/null');stderr=out.parent/(out.name+'.archive.stderr')
 job=Collected(['git','--git-dir',str(repo/'.git'),'archive','--format=tar',REVISION],env,33554432,lambda:None,stdout_path=archive,stderr_path=stderr);job.wait(30)
 need(job.physically_settled is True and not any(t.is_alive()for t in job.threads)and all(f.closed for f in job.files.values())and job.invalid is None and sha(archive)==ARCHIVE,'ExactPhysicallySettledGitArchive')
 for row in data['files']:
  rel=Path(row['path']);need(not rel.is_absolute()and'..'not in rel.parts,'SafeSealedPath');p=out/rel;need(p.is_file()and not p.is_symlink()and p.stat().st_size==row['bytes']and sha(p)==row['sha256'],'ExactRestoredPayload:'+row['path'])
 actual={str(x.relative_to(out))for x in out.rglob('*')if x.is_file()and'__pycache__'not in x.parts and x!=out/'seal.json'};need(actual=={r['path']for r in data['files']},'Complete138Restoration');return {'passed':True,'restored_payloads':len(data['files']),'seal_sha256':SEAL,'Original_revision':REVISION,'archive_sha256':ARCHIVE,'application_dispatched':False,'Git_writes':False}
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--repo',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();print(json.dumps(run(a.repo,a.output),indent=2))
