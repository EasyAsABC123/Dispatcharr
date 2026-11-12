"""Reconstruct exact qualified789 inputs from immutable Git plus five blobs.
No mutable source/private fallback, application execution, or repository writes.
"""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import subprocess
import tarfile

HERE = Path(__file__).resolve().parent
BASE = '98e447186af099dd17496539aec46312f6cf4f62'
SELECTION_SHA = 'dfda9d60f812785e0397be8a0f58be0723cb5504ff7a975fa951fba9091f13eb'
BASE_MAP_SHA = '650eed662dacc2676793f9ae6404160ef3ba334e3f440ca02212a34e78038fbb'
FINAL_MAP_SHA = '12b68a9662c29f85b07c4afc5290fd8a68aada7ec916cef9e9ad3783a06937e0'
ARCHIVE_BINDING_SHA = 'c21c366bbeb5248a422ab1dff6184591118f4e9aa43f583a4f72006d56631f2b'

def digest(data):
    return hashlib.sha256(data).hexdigest()

def relative(name):
    path = PurePosixPath(name)
    if not isinstance(name, str) or path.is_absolute() or '..' in path.parts or str(path) != name or not name.startswith('rust/'):
        raise ValueError('ScopedRelativeRustPath')
    return path

def payload(root, record):
    name = relative(record['path'])
    path = root / 'selected' / name
    if any(p.is_symlink() for p in [path, *path.parents] if p != root.parent):
        raise ValueError('NoSymlinkPayload')
    if not path.is_file() or not path.resolve().is_relative_to(root.resolve()):
        raise ValueError('OwnedRegularPayload')
    data = path.read_bytes()
    if len(data) != record['bytes'] or digest(data) != record['sha256']:
        raise ValueError('ExactSelectedBlob')
    return data

def verified_json(root, name, expected):
    path = root / name
    if path.is_symlink() or digest(path.read_bytes()) != expected:
        raise ValueError('ExactPublicManifest:' + name)
    return json.loads(path.read_bytes())

def prepare(root):
    root = Path(root)
    selected = verified_json(root, 'runtime-selection.json', SELECTION_SHA)
    base = verified_json(root, 'base-inputs.json', BASE_MAP_SHA)['source_sha256']
    final = verified_json(root, 'inputs.json', FINAL_MAP_SHA)['source_sha256']
    archive = verified_json(root, 'archive-binding.json', ARCHIVE_BINDING_SHA)
    if selected['base_revision'] != BASE or archive['base_revision'] != BASE or len(selected['files']) != 5 or len(base) != 788 or len(final) != 789:
        raise ValueError('ExactBaseAndFinalCounts')
    patches = {}
    for row in selected['files']:
        name = str(relative(row['path']))
        if name in patches or row['base_sha256'] != base.get(name) or row['sha256'] != final.get(name):
            raise ValueError('SelectedBaseOrFinalIdentity')
        patches[name] = payload(root, row)
    return base, final, archive, patches

def map_rust(output):
    return {str(p.relative_to(output)): digest(p.read_bytes()) for p in (output / 'rust').rglob('*') if p.is_file()}

def reconstruct(repo, output, root=HERE):
    base, final, binding, patches = prepare(root)
    output = Path(output).absolute()
    if output.exists() or output.is_symlink():
        raise ValueError('FreshOutputRequired')
    env = {k: v for k, v in os.environ.items() if not k.startswith('GIT_')}
    repo = subprocess.check_output(['git', '-C', str(repo), 'rev-parse', '--show-toplevel'], env=env, text=True).strip()
    raw = subprocess.check_output(['git', '-C', repo, 'archive', '--format=tar', BASE, 'rust', 'scripts'], env=env)
    if len(raw) != binding['archive_bytes'] or digest(raw) != binding['archive_sha256']:
        raise ValueError('ExactGitRootArchive')
    with tarfile.open(fileobj=io.BytesIO(raw), mode='r:') as archive:
        members = archive.getmembers()
        if len(members) > 10000:
            raise ValueError('ArchiveMemberBound')
        seen = set()
        for member in members:
            name = PurePosixPath(member.name)
            if name.is_absolute() or '..' in name.parts or not name.parts or name.parts[0] not in ('rust', 'scripts') or member.name in seen or not (member.isdir() or member.isfile()):
                raise ValueError('RegularScopedArchive')
            seen.add(member.name)
        output.mkdir(parents=True, mode=0o755)
        archive.extractall(output, filter='data')
    if map_rust(output) != base:
        raise ValueError('All788BaseHashes')
    for name, data in patches.items():
        path = output / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
    if map_rust(output) != final:
        raise ValueError('All789QualifiedHashes')
    record = {'passed': True, 'base_revision': BASE, 'base_Rust_inputs': 788, 'qualified_Rust_inputs': 789, 'source_sha256': final, 'selected_manifest_sha256': SELECTION_SHA, 'no_mutable_or_private_fallback': True, 'application_executed': False}
    (output / 'reconstruction.json').write_text(json.dumps(record, indent=2) + '\n')
    return record

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = reconstruct(args.repo, args.output)
    print(json.dumps({k: v for k, v in result.items() if k != 'source_sha256'}))
