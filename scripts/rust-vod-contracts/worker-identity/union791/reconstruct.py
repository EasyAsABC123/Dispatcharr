"""Exact committed union791 + five coupled inputs. No mutable/private fallback."""
import argparse
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import subprocess
import tarfile

HERE = Path(__file__).resolve().parent
REVISION = 'f0bfc2f200dcfd1f64ae32a9855ea250731ce67f'
COUPLED_SHA = 'c8188d7fe02c17d66ab73b0af1ff70335d9a25fadc590a90ae61f30c0fbb5b27'
INPUTS_SHA = 'a729127b8f57f261433152d958b60695fb9e25ce824b65678b614378ac6bc8c5'


def sha(raw): return hashlib.sha256(raw).hexdigest()


def expected():
    raw = (HERE / 'source-inputs.json').read_bytes()
    if sha(raw) != INPUTS_SHA: raise ValueError('Qualified791MapChanged')
    value = json.loads(raw)
    if len(value) != 791: raise ValueError('Exact791Inputs')
    coupled_raw = (HERE / 'coupled-inputs.json').read_bytes()
    if sha(coupled_raw) != COUPLED_SHA: raise ValueError('FiveCoupledMapChanged')
    coupled = json.loads(coupled_raw)
    if len(coupled) != 5 or set(coupled) & set(value): raise ValueError('ExactFiveCoupledInputs')
    return {**value, **coupled}


def verify(root):
    for name, digest in expected().items():
        path = Path(root) / name
        if path.is_symlink() or not path.is_file() or sha(path.read_bytes()) != digest: raise ValueError('ReconstructedInputChanged:' + name)
    return {'runtime_inputs': 791, 'coupled_inputs': 5, 'revision': REVISION}


def reconstruct(repo, output):
    output = Path(output)
    if output.exists() or output.is_symlink(): raise ValueError('FreshSourceOutputRequired')
    inputs = expected()
    actual = subprocess.check_output(['git', '-C', str(Path(repo).resolve()), 'rev-parse', '--verify', REVISION + '^{commit}'], text=True).strip()
    if actual != REVISION: raise ValueError('ExactImmutableRevisionRequired')
    raw = subprocess.check_output(['git', '-C', str(Path(repo).resolve()), 'archive', REVISION, '--', 'rust', *[name for name in inputs if not name.startswith('rust/')]])
    if len(raw) > 134217728: raise ValueError('ArchiveByteCapacity')
    files = {}
    with tarfile.open(fileobj=io.BytesIO(raw)) as archive:
        for member in archive:
            if member.isdir(): continue
            path = PurePosixPath(member.name)
            if not member.isfile() or path.is_absolute() or '..' in path.parts or member.name not in inputs or member.name in files or member.size > 33554432: raise ValueError('ExactRegularArchive')
            data = archive.extractfile(member).read()
            if sha(data) != inputs[member.name]: raise ValueError('GitBlobMismatch')
            files[member.name] = data
    if set(files) != set(inputs): raise ValueError('Complete796InputClosure')
    output.mkdir(parents=True, exist_ok=False)
    for name, data in files.items():
        path = output / name; path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('xb') as stream: stream.write(data)
    return verify(output)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(reconstruct(args.repo, args.output), indent=2))
