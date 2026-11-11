"""Copy pinned DRF static files without a Python runtime dependency.
Original Dispatcharr, DRF and bundled dependency authors retain all credit.
Pinned-image export is validated against the committed Source manifest.
"""
import hashlib
import json
from pathlib import Path
import shutil
import stat


def collect(source, destination, *, maximum_files=4096, maximum_bytes=32 << 20):
    source = Path(source).resolve(strict=True)
    destination = Path(destination)
    if destination.exists() or destination.is_symlink():
        raise ValueError('Fresh owned destination required')
    records = []
    total = 0
    for path in sorted(source.rglob('*')):
        info = path.lstat()
        if stat.S_ISLNK(info.st_mode):
            raise ValueError('Static asset symlink needs explicit provenance')
        if stat.S_ISDIR(info.st_mode):
            continue
        if not stat.S_ISREG(info.st_mode):
            raise ValueError('Static assets must be regular files')
        total += info.st_size
        if len(records) >= maximum_files or total > maximum_bytes:
            raise ValueError('Pinned static export exceeds declared bounds')
        records.append((path, path.relative_to(source), info.st_size))
    if not records:
        raise ValueError('Expected pinned DRF static distribution')
    destination.mkdir(parents=True, exist_ok=False)
    manifest = []
    for original, relative, size in records:
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(original, target)
        original_sha = hashlib.sha256(original.read_bytes()).hexdigest()
        if hashlib.sha256(target.read_bytes()).hexdigest() != original_sha:
            raise ValueError('Static asset copy changed bytes')
        manifest.append({'path': str(relative), 'bytes': size,
                         'sha256': original_sha, 'mtime_ns': original.stat().st_mtime_ns})
    return {'format_version': 1, 'files': manifest, 'total_bytes': total,
            'credit': 'Original Dispatcharr, Encode/DRF and bundled upstream authors retain credit and their respective licenses.'}


def main():
    import importlib.metadata
    import importlib.util
    expected = json.loads(Path(__file__).with_name('drf-static-source-1.json').read_text())
    spec = importlib.util.find_spec('rest_framework')
    package = Path(spec.origin).parent
    version = importlib.metadata.version('djangorestframework')
    assert version == expected['DRF_version']
    output = Path('/media-runtime/opt/dispatcharr-frontend/static/rest_framework')
    actual = collect(package / 'static/rest_framework', output)
    fields = ['path', 'bytes', 'sha256', 'mtime_ns']
    assert [{k: f[k] for k in fields} for f in actual['files']] == [{k: f[k] for k in fields} for f in expected['files']]
    docs = Path('/media-runtime/usr/share/doc/dispatcharr')
    docs.mkdir(parents=True, exist_ok=True)
    distribution = importlib.metadata.distribution('djangorestframework')
    for name in expected['licenses']:
        source = Path(distribution.locate_file(name))
        assert source.is_file() and not source.is_symlink() and source.stat().st_size <= 131072
        shutil.copy2(source, docs / ('DRF-' + source.name))
    shutil.copy2(Path(__file__).with_name('drf-static-source-1.json'), docs / 'drf-static-source-1.json')
    (docs / 'drf-static-export.json').write_text(json.dumps(actual, indent=2) + '\n')


if __name__ == '__main__':
    main()
