#!/usr/bin/env python3
"""Reconstruct immutable Core8 + exactly two benchmark overlays.
Never reads mutable Rust or changes historical dd3 reconstruction/publication.
"""
import argparse
import io
import json
from pathlib import Path,PurePosixPath
import subprocess
import tarfile
import importlib.util
_schema_path=Path(__file__).resolve().with_name('schema.py')
_schema_spec=importlib.util.spec_from_file_location('_dispatcharr_core8_schema',_schema_path)
_schema=importlib.util.module_from_spec(_schema_spec);_schema_spec.loader.exec_module(_schema)
BASE=_schema.BASE
PRODUCTION=_schema.PRODUCTION
BENCHMARK=_schema.BENCHMARK
OVERLAYS=_schema.OVERLAYS
digest=_schema.digest
require=_schema.require
sha=_schema.sha
HERE=Path(__file__).resolve().parent

def regular_map(root):
    entries={}
    for path in (root/'rust').rglob('*'):
        require(not path.is_symlink(),'Symlink in Rust source')
        if path.is_file():entries[str(path.relative_to(root))]=sha(path)
    return entries

def extract(raw,destination):
    with tarfile.open(fileobj=io.BytesIO(raw)) as archive:
        for item in archive.getmembers():
            name=PurePosixPath(item.name)
            require(bool(name.parts) and not name.is_absolute() and '..'not in name.parts and name.parts[0]=='rust','Unsafe archive member')
            require(item.isfile()or item.isdir(),'Only regular source files/directories accepted')
        archive.extractall(destination,filter='data')

def overlay_bytes(path):return path.read_bytes()

def reconstruct(repo,destination):
    ready=json.loads((HERE/'ready-manifest.json').read_text());expected=ready['source_map']
    require(digest(expected)==BENCHMARK,'Benchmark source seal changed')
    destination=Path(destination)
    if destination.exists():require(not destination.is_symlink()and not any(destination.iterdir()),'Fresh empty source required')
    destination.mkdir(parents=True,exist_ok=True)
    raw=subprocess.check_output(['git','archive',BASE,'rust'],cwd=repo)
    extract(raw,destination)
    require(digest(regular_map(destination))==PRODUCTION,'Immutable production f16a mismatch')
    overlays=HERE/'source-overlays'
    paths={str(p.relative_to(overlays)) for p in overlays.rglob('*')if p.is_file()}
    require(paths==OVERLAYS and not any(p.is_symlink() for p in overlays.rglob('*')),'Exactly two regular overlays required')
    for name in sorted(paths):
        data=overlay_bytes(overlays/name)
        import hashlib
        require(hashlib.sha256(data).hexdigest()==expected[name],'Benchmark overlay changed')
        (destination/name).write_bytes(data)
    actual=regular_map(destination)
    require(actual==expected and digest(actual)==BENCHMARK,'Whole benchmark map mismatch')
    return {'passed':True,'production_base':BASE,'production_map':PRODUCTION,'benchmark_map':BENCHMARK,'files':len(actual),'measured_ELF_reproduced':False}
if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--repo',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    print(json.dumps(reconstruct(a.repo,a.output)))
