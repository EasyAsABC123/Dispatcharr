"""Reconstruct historical source/input bytes from local Git; no compiler/app/network."""
import argparse
import hashlib
import io
import json
import shutil
import subprocess
from pathlib import Path

ROOT=Path(__file__).resolve().parent
BASE='e194c9e1d96db7e33857b93df85bfb9b0a97808f'

def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def require(ok,label):
    if not ok:raise ValueError(label)

def reconstruct(destination,git_repo,source12=None,elf=None):
    source12=source12 or ROOT.parent/'notification-forms-source12'
    require(not destination.exists(),'FreshDestinationRequired')
    mapping=json.loads((ROOT/'compiled813-map.json').read_text())
    require(len(mapping)==813 and sha(ROOT/'compiled813-map.json')=='e6c01223d13dab1a3b185ae38b58b9f3883a7b5f294de4e64abc7b3f7f430a91','ExactHistorical813Map')
    selection=json.loads((ROOT/'runtime-selected-files.json').read_text())['runtime_files']
    overlays={row['path']:row for row in selection}
    names=sorted(mapping)
    for name in names:
        path=Path(name);require(not path.is_absolute() and '..' not in path.parts,'SourceRelativePaths')
    command=['git','-C',str(git_repo),'cat-file','--batch']
    run=subprocess.run(command,input=''.join(BASE+':'+name+'\n' for name in names).encode(),stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    require(run.returncode==0,'LocalGitBatchExit')
    output=io.BytesIO(run.stdout);blobs={}
    for name in names:
        line=output.readline().decode().strip().split()
        if line[-1]=='missing':require(name in overlays and overlays[name]['base_preimage_sha256'] is None,'OnlyDeclaredNewPathAbsent');blobs[name]=None;continue
        require(len(line)==3 and line[1]=='blob','ExpectedGitBlob')
        data=output.read(int(line[2]));require(output.read(1)==b'\n','GitBatchBoundary');blobs[name]=data
    require(not output.read(),'CompleteGitBatchConsumed')
    destination.mkdir(parents=True,exist_ok=False)
    source=destination/'source'
    for name in names:
        data=blobs[name]
        if name in overlays:
            row=overlays[name]
            require((None if data is None else hashlib.sha256(data).hexdigest())==row['base_preimage_sha256'],'OriginalPreimageBeforeOverlay')
            candidate=ROOT/'runtime'/name;require(sha(candidate)==row['sha256'],'ExactSelectedOverlay');data=candidate.read_bytes()
        require(data is not None and hashlib.sha256(data).hexdigest()==mapping[name],'ExactReconstructedSourceBlob')
        target=source/name;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(data)
    closure=json.loads((ROOT/'input-closure.json').read_text());copied=0;missing=[]
    for name,row in closure['inputs'].items():
        path=Path(name);require(not path.is_absolute() and '..' not in path.parts,'InputRelativePaths')
        if row['kind']=='historical-ELF-not-shipped':
            if elf is None:missing.append(name);continue
            origin=elf;require(origin.stat().st_size==row['bytes'],'HistoricalELFSize')
        elif row['kind']=='existing-Source12-packet':origin=source12/row['path']
        else:require(row['kind']=='exact-embedded','DeclaredInputKind');origin=ROOT/row['path']
        require(origin.is_file() and sha(origin)==row['sha256'],'ExactOneInputBindingNoFallback:'+name)
        target=destination/'execution'/name;target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(origin,target);copied+=1
    result={'state':'SOURCE_AND_ALL267_LOCAL_INPUTS_RECONSTRUCTED' if not missing else 'SOURCE_AND266_LOCAL_INPUTS_RECONSTRUCTED_HISTORICAL_ELF_REQUIRED','source_inputs':len(mapping),'exact_local_inputs':copied,'missing_explicit_historical_ELF':missing,'base_Git':BASE,'Source_app_runs':0,'Native_app_runs':0,'new_reference_binding_or_binary_qualification_claimed':False}
    (destination/'reconstruction.json').write_text(json.dumps(result,indent=2)+'\n')
    return result

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--destination',type=Path,required=True);parser.add_argument('--git-repo',type=Path,required=True);parser.add_argument('--source12',type=Path);parser.add_argument('--elf',type=Path);args=parser.parse_args();print(json.dumps(reconstruct(args.destination,args.git_repo,args.source12,args.elf)))
