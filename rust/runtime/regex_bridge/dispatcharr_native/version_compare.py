"""Same pinned Source packaging parser; no app, SQL, network or subprocess port.
Original Dispatcharr/packaging/CPython authors retain all behavior credit.
"""
from packaging import version as pkg_version

def compare(current, latest):
    if (not isinstance(current, str) or not isinstance(latest, str)
        or not current or not latest
        or len(current.encode('utf8')) > 128 or len(latest.encode('utf8')) > 247):
        raise ValueError('Version comparison capacity or schema')
    try:
        current = pkg_version.parse(current)
        latest = pkg_version.parse(latest)
    except pkg_version.InvalidVersion:
        return {'kind': 'invalid', 'exception': 'InvalidVersion'}
    ordering = int(latest > current) - int(latest < current)
    return {'kind': 'parsed', 'newer': ordering > 0, 'ordering': ordering}
