"""Regex dependency adapter for Original Dispatcharr's channel rename.

The original Dispatcharr and Python regex authors retain full algorithm credit.
"""
from time import monotonic_ns

import regex

from dispatcharr_native.wire import CompatibilityUnavailable


def evaluate(names, config):
    if (not isinstance(names, list) or len(names) > 256
            or any(not isinstance(name, str) or len(name) > 512 for name in names)
            or not isinstance(config, dict)):
        raise CompatibilityUnavailable('Channel rename names')
    pattern, replacement = config.get('find'), config.get('replace')
    flags, deadline = config.get('ignore_case'), config.get('deadline_ns')
    if (not isinstance(pattern, str) or not isinstance(replacement, str)
            or max(len(pattern.encode()), len(replacement.encode())) > 65536
            or type(flags) is not bool or type(deadline) is not int
            or not monotonic_ns() < deadline <= monotonic_ns() + 5_000_000_000):
        raise CompatibilityUnavailable('Channel rename options')
    try:
        pattern = regex.sub(r'\(\?<([^>]+)>', r'(?P<\1>', pattern)
    except Exception as error:
        return {'error': f'Failed to normalize pattern: {error}', 'overflow': False, 'names': []}
    try:
        replacement = replacement.replace('$$', '$').replace('$&', r'\g<0>')
        replacement = regex.sub(r'\$<([A-Za-z_][A-Za-z0-9_]*)>', r'\\g<\1>', replacement)
        replacement = regex.sub(r'\$(\d+)', r'\\g<\1>', replacement)
        compiled = regex.compile(pattern, flags=regex.IGNORECASE if flags else 0)
    except Exception as error:
        return {'error': f'Invalid regex pattern: {error}', 'overflow': False, 'names': []}
    changed = []
    for name in names:
        remaining = (deadline - monotonic_ns()) / 1_000_000_000
        if remaining <= 0:
            raise CompatibilityUnavailable('Channel rename deadline')
        try:
            result = compiled.sub(replacement, name, timeout=min(.1, remaining))
        except TimeoutError as error:
            raise CompatibilityUnavailable('Channel rename match deadline') from error
        except (regex.error, IndexError, ValueError):
            changed.append(None)
            continue
        if result == name or not result.strip():
            changed.append(None)
            continue
        if len(result) > 512 or '\0' in result:
            # Original's bulk SQL write rejects the entire set on this constraint.
            return {'error': None, 'overflow': True, 'names': []}
        changed.append(result)
    return {'error': None, 'overflow': False, 'names': changed}
