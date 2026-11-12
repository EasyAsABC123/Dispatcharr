"""Restore source792+five coupled inputs from pinned Git and exact three overlays only."""
import argparse,hashlib,json,subprocess
from pathlib import Path
HERE=Path(__file__).resolve().parent
MAP_SHA='46d649d3503e6ee202f1393c3981871fe74da75f4a554ae275247e84915090b9'
SELECTION_SHA='b527243fb8bf63a40db0c63eef29969e4ae745009caa938b65727e1b2b9d5a98'
BASE='f0bfc2f200dcfd1f64ae32a9855ea250731ce67f'
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def reconstruct(repo,output,root=HERE):
    root=Path(root);output=Path(output)
    if output.exists()or output.is_symlink()or any(p.is_symlink()for p in output.parents):raise ValueError('FreshUnaliasedOutput')
    if sha(root/'source/full797-map.json')!=MAP_SHA or sha(root/'source/selected-three.json')!=SELECTION_SHA:raise ValueError('ExactSourceAuthorities')
    expected=json.loads((root/'source/full797-map.json').read_text());selection=json.loads((root/'source/selected-three.json').read_text())
    if len(expected)!=797 or sum(k.startswith('rust/')for k in expected)!=792 or selection['base_revision']!=BASE or len(selection['files'])!=3:raise ValueError('Exact792Plus5AndThree')
    overlays={x['path']:x for x in selection['files']};data={}
    def blob(name,missing=False):
        result=subprocess.run(['git','-C',str(Path(repo).resolve()),'show',BASE+':'+name],capture_output=True,timeout=10)
        if result.returncode:
            if missing:return None
            raise ValueError('RequiredImmutableGitBlob:'+name)
        if len(result.stdout)>33554432:raise ValueError('BoundedGitBlob')
        return result.stdout
    for name,wanted in expected.items():
        rel=Path(name)
        if rel.is_absolute()or '..'in rel.parts:raise ValueError('SafeInputPath')
        if name in overlays:
            row=overlays[name];prior=blob(name,missing=row['before_sha256']is None)
            if (None if prior is None else hashlib.sha256(prior).hexdigest())!=row['before_sha256']:raise ValueError('ExactGitPreimage')
            p=root/'source/overlay'/name
            if p.is_symlink()or not p.is_file()or sha(p)!=row['sha256']:raise ValueError('ExactSelectedOverlay')
            raw=p.read_bytes()
        else:raw=blob(name)
        if hashlib.sha256(raw).hexdigest()!=wanted:raise ValueError('ExactSourceHash:'+name)
        data[name]=raw
    output.mkdir(parents=True,exist_ok=False)
    for name,raw in data.items():p=output/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(raw)
    return {'restored_Rust_inputs':792,'restored_coupled_inputs':5,'full797_map_sha256':MAP_SHA,'base_revision':BASE,'selected_overlays':3,'Cargo_or_app_or_Source_or_Docker_execution':False,'historical_ELF_hash_reproducibility_claimed':False,'execution_inputs21_fully_restored':False}
if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--repo',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();print(json.dumps(reconstruct(a.repo,a.output),indent=2))
