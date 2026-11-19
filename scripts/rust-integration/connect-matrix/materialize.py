"""Reconstruct the exact Connect fixture from pinned Git and small overlays."""
import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
import sys
import tempfile

HERE=Path(__file__).resolve().parent
ROOT=HERE.parents[2]
INPUTS=HERE/'fixture-inputs'
RECIPE_SHA='411e7f6b97d1fe310a2601a4ab37c3626379d7e1a29eb84b5d3bf3dd44faf070'
FIXTURE_SHA='ba55511baad9857507254324e8784e64371dfb9b54871fbf513af4f08d29aeb6'
SHA=re.compile('[0-9a-f]{64}')
REVISION=re.compile('[0-9a-f]{40}')
SOURCES=('foundation_receipt.py','plan.py','run.py','test_matrix.py')


def body_sha(body):
    return hashlib.sha256(body).hexdigest()


def sha(path):
    return body_sha(path.read_bytes())


def load_recipe():
    recipe_path=HERE/'materialization.json'
    freeze_path=INPUTS/'freeze.json'
    patch=HERE/'fixture-overlays.patch'
    if sha(recipe_path)!=RECIPE_SHA or sha(freeze_path)!=FIXTURE_SHA:
        raise ValueError('Pinned fixture recipe or seal changed')
    recipe=json.loads(recipe_path.read_bytes())
    freeze=json.loads(freeze_path.read_bytes())
    names=recipe['copied_paths']
    if recipe['format_version']!=1 or sha(patch)!=recipe['copy_patch_sha256']:
        raise ValueError('Pinned Git overlay differs')
    if (recipe['fixture_seal_sha256']!=FIXTURE_SHA or recipe['original_revision']!=freeze['original_revision']
            or recipe['archive_sha256']!=freeze['copied_inputs']['original-source.tar']
            or set(names)!=set(freeze['copied_inputs'])-{'original-source.tar'}):
        raise ValueError('Fixture materialization dependency set changed')
    return recipe, freeze


def pinned_git(root,*args):
    response=subprocess.run(['git',*args],cwd=root,capture_output=True)
    if response.returncode:
        raise ValueError('A pinned Git object is missing. Use a complete fork history.')
    return response.stdout


def write_bytes(path,body):
    path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('xb') as stream:
        stream.write(body)


def safe_name(path):
    name=PurePosixPath(path)
    return not name.is_absolute() and '..' not in name.parts and str(name)==path


def populate(output,root,recipe,freeze):
    copied=output/'copied'
    for path,item in recipe['copied_paths'].items():
        if not safe_name(path):
            raise ValueError('Unexpected fixture path')
        if 'git_revision' in item:
            revision=item['git_revision']
            if not REVISION.fullmatch(revision) or not SHA.fullmatch(item['git_blob_sha256']):
                raise ValueError('Invalid pinned Git reference')
            contents=pinned_git(root,'show',f'{revision}:{path}')
            expected=item['git_blob_sha256']
        else:
            name=item['name']
            if '/' in name or not safe_name(name):
                raise ValueError('Invalid local fixture module')
            contents=(INPUTS/name).read_bytes()
            expected=item['content_sha256']
        if body_sha(contents)!=expected:
            raise ValueError(f'Exact Connect fixture preimage differs for {path}')
        write_bytes(copied/path,contents)
    subprocess.run(['git','apply','--check','--unidiff-zero','--whitespace=error',str(HERE/'fixture-overlays.patch')],
                   cwd=copied,check=True)
    subprocess.run(['git','apply','--unidiff-zero','--whitespace=error',str(HERE/'fixture-overlays.patch')],
                   cwd=copied,check=True)
    revision=recipe['original_revision']
    if not REVISION.fullmatch(revision):
        raise ValueError('Invalid pinned Original revision')
    source=pinned_git(root,'archive','--format=tar',revision)
    if body_sha(source)!=recipe['archive_sha256']:
        raise ValueError('Original Git archive identity changed')
    write_bytes(copied/'original-source.tar',source)
    for name in ('freeze.json',*SOURCES):
        write_bytes(output/name,(INPUTS/name).read_bytes())
    with (output/'matrix-plan.json').open('xb') as stream:
        subprocess.run([sys.executable,'-B','-c','''
from pathlib import Path
import json, sys
import plan
plan.REPO=Path(sys.argv[1])
print(json.dumps(plan.prepare(),indent=2,sort_keys=True))
''',str(root)],cwd=output,stdout=stream,check=True)
    verify(output,freeze)


def verify(output,freeze):
    if sha(output/'freeze.json')!=FIXTURE_SHA:
        raise ValueError('Materialized fixture seal changed')
    for folder,key in ((output,'driver_inputs'),(output/'copied','copied_inputs')):
        for name,expected in freeze[key].items():
            path=folder/name
            if path.is_symlink() or sha(path)!=expected:
                raise ValueError(f'Materialized fixture drifted: {name}')
    leaves={str(path.relative_to(output/'copied')) for path in (output/'copied').rglob('*') if path.is_file()}
    if leaves!=freeze['copied_inputs'].keys():
        raise ValueError('Unexpected materialized copied files')


def materialize(output,root,recipe,freeze):
    if output.parent!=root/'artifacts/rust':
        raise ValueError('Use a fresh direct child of this checkout’s artifacts/rust directory')
    if output.exists():
        raise ValueError('Materialization output already exists')
    with tempfile.TemporaryDirectory(prefix='dispatcharr-connect-fixture-') as scratch:
        stage=Path(scratch)/'artifacts/rust/fixture'
        populate(stage,root,recipe,freeze)
        output.parent.mkdir(parents=True,exist_ok=True)
        shutil.copytree(stage,output)
    verify(output,freeze)


def self_test(output,root):
    subprocess.run([sys.executable,'-B','-c','''
from pathlib import Path
import sys, unittest
import plan
plan.REPO=Path(sys.argv[1])
unittest.main(module="test_matrix",argv=["test_matrix","-v"])
''',str(root)],cwd=output,check=True)


def main():
    argument=argparse.ArgumentParser(description=__doc__)
    operation=argument.add_mutually_exclusive_group(required=True)
    operation.add_argument('--output',type=Path)
    operation.add_argument('--verify',type=Path)
    argument.add_argument('--self-test',action='store_true')
    options=argument.parse_args()
    recipe,freeze=load_recipe()
    output=(options.output or options.verify).resolve()
    if options.output:
        materialize(output,ROOT,recipe,freeze)
    else:
        verify(output,freeze)
    if options.self_test:
        self_test(output,ROOT)
    print(json.dumps({'fixture':str(output),'copied_inputs':len(freeze['copied_inputs']),
                      'fixture_seal_sha256':FIXTURE_SHA,'runtime_executed':False},sort_keys=True))
    return 0


if __name__=='__main__':
    raise SystemExit(main())
