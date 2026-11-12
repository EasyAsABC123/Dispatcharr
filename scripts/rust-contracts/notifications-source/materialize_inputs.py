#!/usr/bin/env python3
"""Reconstruct exact already-executed Source inputs without executing Source.
Default invocation is read-only validation. --destination creates a NEW owned
input tree from public exact blobs and pinned local Git objects; no network,
Docker, Django, application, migration or request is invoked.
"""
import argparse,hashlib,json,shutil,subprocess
from pathlib import Path
HERE=Path(__file__).resolve().parent
REVISION='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
SEAL='2004c823a728d3ecdb7f7750867b0b0d3385fdac5779b4a074afb899b892ba59'
ARCHIVE='f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff'
def require(ok,label):
    if not ok:raise ValueError(label)
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def validate(root=HERE):
    seal_path=root/'raw/executed-seal.json';require(sha(seal_path)==SEAL,'OriginalExecutedSealBytes')
    value=json.loads(seal_path.read_text());require(value['Original_revision']==REVISION and len(value['files'])==29,'PinnedOriginal29Inputs')
    seen=set();present=[]
    for row in value['files']:
        relative=Path(row['path']);require(not relative.is_absolute()and'..'not in relative.parts and row['path']not in seen,'UniqueRelativeInput');seen.add(row['path'])
        if row['path']=='inputs/original-source.tar':require(row['sha256']==ARCHIVE and row['bytes']==16609280,'PinnedArchiveRecipe');continue
        path=root/'executed'/relative;require(path.is_file()and path.stat().st_size==row['bytes']and sha(path)==row['sha256'],'ExactPublicExecutedInput:'+row['path']);present.append(row['path'])
    require(len(present)==28,'ExactPublic28Inputs')
    metadata=json.loads((root/'executable-closure.json').read_text());require(metadata['executed_seal_sha256']==SEAL and metadata['Original_revision']==REVISION and metadata['public_exact_input_files']==28 and metadata['Source_controller_exit_preserved']==1 and metadata['Source_reruns']==metadata['operation_registrations']==0,'ClosureScope')
    return value

def materialize(repository,destination,root=HERE):
    value=validate(root);repo=repository.resolve();out=destination.resolve();require(not out.exists(),'NewOwnedMaterializationRequired');out.mkdir(parents=True,mode=0o700)
    # Local immutable Git object reads only; no fetch, worktree or index change.
    result=subprocess.run(['git','-C',str(repo),'rev-parse',REVISION+'^{commit}'],stdin=subprocess.DEVNULL,capture_output=True,timeout=10);require(result.returncode==0 and result.stdout.decode().strip()==REVISION,'PinnedLocalGitCommitRequired')
    for row in value['files']:
        target=out/row['path'];target.parent.mkdir(parents=True,exist_ok=True)
        if row['path']!='inputs/original-source.tar':shutil.copyfile(root/'executed'/row['path'],target)
    archive=out/'inputs/original-source.tar'
    with archive.open('xb')as stream:
        result=subprocess.run(['git','-C',str(repo),'-c','tar.umask=0002','-c','core.attributesFile=/dev/null','archive','--format=tar',REVISION],stdin=subprocess.DEVNULL,stdout=stream,stderr=subprocess.PIPE,timeout=30)
    require(result.returncode==0 and len(result.stderr)<=65536,'GitArchiveCompleted')
    require(archive.stat().st_size==16609280 and sha(archive)==ARCHIVE,'ReproducedExactGitArchive')
    shutil.copyfile(root/'raw/executed-seal.json',out/'seal.json')
    for row in value['files']:require((out/row['path']).stat().st_size==row['bytes']and sha(out/row['path'])==row['sha256'],'All29MaterializedInputsExact')
    return {'passed':True,'executed_seal_sha256':SEAL,'exact_materialized_inputs':29,'archive_sha256':sha(archive),'Source_application_or_request_executed':False,'original_Source_controller_exit':1,'output':str(out)}
if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--repo',type=Path);parser.add_argument('--destination',type=Path);args=parser.parse_args()
    if args.destination is None:validate();print(json.dumps({'passed':True,'public_exact_inputs':28,'reproducible_archive_input':1,'Source_executed':False,'operation_registrations':0}))
    else:
        require(args.repo is not None,'ExplicitLocalRepositoryRequired');print(json.dumps(materialize(args.repo,args.destination)))
