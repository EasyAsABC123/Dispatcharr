#!/usr/bin/env python3
"""Local Git exact input reconstruction only; never starts Source/Docker/Native."""
import argparse,json,shutil,subprocess
from pathlib import Path
from validate import HERE,REV,closures,digest,require,validate_manifest

def materialize(repo,destination,version='3',root=HERE):
    validate_manifest(root);meta=closures(root);require(version in meta['executors'],'KnownExecutorVersion')
    out=destination.resolve();require(not out.exists(),'NewMaterializationOnly');row=meta['executors'][version];base=root/row['path'];seal=json.loads((base/'seal.json').read_text());out.mkdir(mode=0o700,parents=True)
    check=subprocess.run(['git','-C',str(repo.resolve()),'rev-parse',REV+'^{commit}'],capture_output=True,timeout=10);require(check.returncode==0 and check.stdout.decode().strip()==REV,'PinnedLocalGitObjectsRequired')
    for item in seal['files']:
        if item['path']=='inputs/original-source.tar':continue
        target=out/item['path'];target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(base/item['path'],target)
    archive=out/'inputs/original-source.tar'
    with archive.open('xb')as stream:
        result=subprocess.run(['git','-C',str(repo.resolve()),*meta['archive']['git_args']],stdout=stream,stderr=subprocess.PIPE,timeout=30)
    require(result.returncode==0 and len(result.stderr)<=65536,'LocalGitArchiveCompleted')
    require(archive.stat().st_size==meta['archive']['bytes']and digest(archive)==meta['archive']['sha256'],'ExactReproducedArchive')
    shutil.copyfile(base/'seal.json',out/'seal.json')
    for item in seal['files']:require((out/item['path']).stat().st_size==item['bytes']and digest(out/item['path'])==item['sha256'],'ExactReconstructedInput:'+item['path'])
    return {'passed':True,'executor':version,'exact_inputs':len(seal['files']),'seal_sha256':digest(out/'seal.json'),'Source_or_Docker_or_Native_executed':False}
if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--repo',type=Path);p.add_argument('--destination',type=Path);p.add_argument('--executor',choices=['1','2','3'],default='3');a=p.parse_args()
    if a.destination is None:validate_manifest();closures();print(json.dumps({'passed':True,'Source_executed':False,'all_three_historical_input_closures_checked':True}))
    else:require(a.repo is not None,'LocalRepositoryRequired');print(json.dumps(materialize(a.repo,a.destination,a.executor)))
