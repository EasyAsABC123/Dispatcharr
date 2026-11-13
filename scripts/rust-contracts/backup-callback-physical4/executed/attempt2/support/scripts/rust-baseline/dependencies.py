"""Report exact installed dependencies and fail on drift from pinned source."""
import importlib.metadata
import json
import platform
import tomllib
from pathlib import Path

from packaging.requirements import Requirement

project = tomllib.loads(Path('/app/pyproject.toml').read_text())['project']
requirements = []
compatible = True
for text in project['dependencies']:
    requirement = Requirement(text)
    try:
        installed = importlib.metadata.version(requirement.name)
        matches = not requirement.specifier or installed in requirement.specifier
    except importlib.metadata.PackageNotFoundError:
        installed, matches = None, False
    if requirement.marker and not requirement.marker.evaluate():
        matches = True
    compatible &= matches
    requirements.append({'requirement': text, 'installed': installed, 'matches': matches})
print(json.dumps({'python': platform.python_version(), 'compatible': compatible,
                  'requirements': requirements}, indent=2))
raise SystemExit(0 if compatible else 1)
