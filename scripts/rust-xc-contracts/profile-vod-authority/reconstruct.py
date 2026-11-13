"""Git-only historical prototype/Original/support reconstruction; never executes it."""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import tarfile
import bounded_process

HERE=Path(__file__).resolve().parent
INPUTS_SHA256='e916f73ea7273b44f2c684ec9e183e8dd49a82977ec397112980210ad6fa2d7e'

def need(value,label):
    if not value:raise ValueError(label)
def sha(raw):return hashlib.sha256(raw).hexdigest()
def git(repo,*args):
    env={k:v for k,v in os.environ.items() if not k.startswith('GIT_')}
    process=bounded_process.Collected(['git','-C',str(repo),*args],env,536870912,lambda:None)
    raw=process.wait(60)
    need(process.physically_settled and process.invalid is None,'ActualGitProcessSettlement')
    need(len(process.output['stderr'])==0,'CleanGitStderr')
    return raw

def contents(raw):
    result={}
    with tarfile.open(fileobj=io.BytesIO(raw)) as archive:
        for member in archive:
            path=Path(member.name)
            need(not path.is_absolute() and '..'not in path.parts,'SafeArchivePath')
            need(member.isdir() or member.isfile(),'RegularArchiveMembersOnly')
            if member.isfile():
                need(member.name not in result and member.size<=536870912,'UniqueBoundedArchiveMember')
                result[member.name]=archive.extractfile(member).read()
    return result

def write_tree(output,files):
    need(not output.exists() and not output.is_symlink(),'FreshOutput')
    output.mkdir(parents=True)
    for name,raw in files.items():
        relative=Path(name)
        need(not relative.is_absolute() and '..'not in relative.parts,'SafeReconstructionPath')
        target=output/relative;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(raw)

def reconstruct(repo,output,mode):
    raw=(HERE/'prototype-inputs.json').read_bytes();need(sha(raw)==INPUTS_SHA256,'ImmutablePrototypeManifest')
    spec=json.loads(raw)
    if mode=='prototype':
        rows=spec['all_result_inputs'];overlays=spec['overlays'];preimages=spec['base_preimages']
        need(len(rows)==1290 and sum(p.startswith('rust/') for p in rows)==793 and set(overlays)==set(preimages) and len(overlays)==6,'ExactPrototypeClosureCounts')
        names=[name for name in rows if name not in preimages or preimages[name] is not None]
        need(len(names)==1289,'ExactBaseApplicationCount')
        files=contents(git(repo,'archive',spec['base_revision'],*names))
        need(set(files)==set(names),'ExactBaseFileClosure')
        for name,data in files.items():
            need(sha(data)==preimages.get(name,rows[name]),'ExactImmutableBaseBlob:'+name)
        for name,digest in overlays.items():
            path=HERE/'prototype-overlay'/name;need(path.is_file() and not path.is_symlink(),'RegularOverlay')
            data=path.read_bytes();need(sha(data)==digest,'ExactSelectedOverlay:'+name);files[name]=data
        need({name:sha(data) for name,data in files.items()}==rows,'CompletePrototypeHashDictionary')
    elif mode=='original':
        archive=git(repo,'archive',spec['Original_revision']);need(sha(archive)==spec['Original_full_archive_sha256'],'ExactOriginalFullArchive')
        files=contents(archive)
    else:
        archive=git(repo,'archive',spec['support_revision'],*spec['support_archive_paths']);need(sha(archive)==spec['support_archive_sha256'],'ExactFrozenSupportArchive')
        files=contents(archive)
    write_tree(output,files)
    return {'mode':mode,'files':len(files),'all_file_hashes_verified':True,'Git_mutated':False,'build_or_fixture_execution':False}

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--mode',choices=['prototype','original','support'],default='prototype')
    args=parser.parse_args()
    print(json.dumps(reconstruct(args.repo.resolve(),args.output.resolve(),args.mode),indent=2))
if __name__=='__main__':main()
