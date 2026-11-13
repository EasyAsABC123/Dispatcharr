#!/usr/bin/env python3
"""Offline original108 executable input reconstruction; never starts apps."""
import argparse,hashlib,json,shutil,subprocess
from pathlib import Path
ROOT=Path(__file__).resolve().parent
REV='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def reconstruct(repo,out):
 out.mkdir(parents=True,exist_ok=False);shutil.copytree(ROOT/'executed-definition',out,dirs_exist_ok=True)
 target=out/'inputs/original-source.tar'
 with target.open('xb')as sink:result=subprocess.run(['git','-C',str(repo),'archive','--format=tar',REV],stdin=subprocess.DEVNULL,stdout=sink,stderr=subprocess.PIPE,timeout=120)
 if result.returncode or sha(target)!='f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff':raise ValueError('ExactPinnedOriginalArchive')
 if sha(out/'seal.json')!='1f7bace09a0c787d70a821fd48109e7ee6ad5b8a83fc54d3843c3fd5d78cd735':raise ValueError('ExactOriginalSeal')
 seal=json.loads((out/'seal.json').read_text())
 for row in seal['files']:
  p=out/row['path']
  if p.stat().st_size!=row['bytes']or sha(p)!=row['sha256']:raise ValueError('Exact108Input:'+row['path'])
 return {'passed':True,'original_inputs':len(seal['files']),'Original_Git':REV,'Source_or_Native_application_runs':0,'projected_reference_is_not_original_executed_bytes':True}
if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--repository',required=True,type=Path);p.add_argument('--output',required=True,type=Path);a=p.parse_args();print(json.dumps(reconstruct(a.repository,a.output),indent=2))
