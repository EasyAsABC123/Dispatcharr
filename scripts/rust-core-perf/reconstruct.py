#!/usr/bin/env python3
"""Reconstruct the exact measured Rust inputs; never use mutable checkout files."""
import argparse,hashlib,io,json,subprocess,tarfile
from pathlib import Path
BASE='24d9e72b64df1afe871f935826824a4c96fd6ade'
CANONICAL='dd3e02227a38f595ace68cfa89f5414bb24efd55f87e6c3f072452280319fd25'
HERE=Path(__file__).resolve().parent

def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def reconstruct(repo,dest):
    expected=json.loads((HERE/'measured-source-map.json').read_text())
    if hashlib.sha256(json.dumps(expected,sort_keys=True,separators=(',',':')).encode()).hexdigest()!=CANONICAL:raise ValueError('Measured source map changed')
    dest=Path(dest);dest.mkdir(parents=True,exist_ok=True)
    if any(dest.iterdir()):raise ValueError('Fresh empty reconstruction required')
    raw=subprocess.check_output(['git','archive',BASE,'rust'],cwd=repo)
    with tarfile.open(fileobj=io.BytesIO(raw)) as archive:archive.extractall(dest,filter='data')
    for source in (HERE/'source-overlays').rglob('*'):
        if source.is_file():
            name=str(source.relative_to(HERE/'source-overlays'))
            if sha(source)!=expected[name]:raise ValueError('Measured overlay changed')
            target=dest/name;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(source.read_bytes())
    actual={str(p.relative_to(dest)):sha(p)for p in (dest/'rust').rglob('*')if p.is_file()}
    if actual!=expected:
        raise ValueError('Whole measured map mismatch: '+repr([k for k in expected.keys()|actual.keys()if expected.get(k)!=actual.get(k)]))
    return {'passed':True,'base':BASE,'source_map_sha256':CANONICAL,'files':len(actual)}
if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--repo',type=Path,default=Path.cwd());p.add_argument('--output',type=Path,required=True);a=p.parse_args();print(json.dumps(reconstruct(a.repo,a.output)))
