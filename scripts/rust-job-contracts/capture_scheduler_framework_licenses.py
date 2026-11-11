"""Retain exact installed pinned framework notices with scheduler evidence.
Original framework authors retain all copyright/license credit.
"""
import hashlib
import importlib.metadata
from pathlib import Path


def capture():
    root = Path('/evidence/framework-licenses')
    result = {}
    for name in ['celery', 'django-celery-beat', 'kombu']:
        distribution = importlib.metadata.distribution(name)
        notices = []
        for item in distribution.files or []:
            filename = Path(str(item)).name
            if filename.lower() not in {'license', 'license.txt', 'license.md', 'copying', 'authors', 'authors.txt'}:
                continue
            if '.dist-info/' not in str(item):
                continue
            source = Path(distribution.locate_file(item))
            data = source.read_bytes()
            if len(data) > 1024 * 1024 or len(notices) >= 16:
                raise RuntimeError('Pinned framework notice bound')
            target = root / name / filename
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
            notices.append({'distribution_file': str(item), 'evidence_file': str(target),
                            'sha256': hashlib.sha256(data).hexdigest(), 'bytes': len(data)})
        if not notices:
            raise RuntimeError('Installed framework notices missing: ' + name)
        result[name] = {'version': distribution.version, 'notices': notices}
    return result
