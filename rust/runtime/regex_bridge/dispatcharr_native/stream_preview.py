"""Bounded calls to the pinned regex dependency for Original Dispatcharr's preview."""
from time import monotonic_ns

import regex

from dispatcharr_native.wire import CompatibilityUnavailable


def evaluate(names, config):
    if not isinstance(names, list) or len(names) > 2048 or any(
        not isinstance(name, str) or len(name) > 512 for name in names
    ):
        raise CompatibilityUnavailable("Stream preview name batch capacity")
    if not isinstance(config, dict):
        raise CompatibilityUnavailable("Stream preview configuration")
    keys = ("find", "match", "exclude")
    patterns = [config.get(key) for key in keys]
    replacement = config.get("replace")
    limit = config.get("limit")
    deadline_ns = config.get("deadline_ns")
    if (
        any(pattern is not None and (not isinstance(pattern, str) or len(pattern) > 512)
            for pattern in patterns)
        or not isinstance(replacement, str)
        or len(replacement) > 512
        or type(limit) is not int
        or not 1 <= limit <= 50
        or type(deadline_ns) is not int
        or deadline_ns > monotonic_ns() + 5_000_000_000
    ):
        raise CompatibilityUnavailable("Stream preview configuration capacity")

    def check_deadline():
        if monotonic_ns() >= deadline_ns:
            raise CompatibilityUnavailable("Stream preview request deadline")

    results = [{"samples": [], "count": 0, "error": None} for _ in keys]
    compiled = [None] * len(keys)
    for index, pattern in enumerate(patterns):
        if pattern:
            check_deadline()
            try:
                compiled[index] = regex.compile(pattern)
            except regex.error as error:
                results[index]["error"] = str(error)
    replacement = regex.sub(r"\$(\d+)", r"\\\1", replacement)

    for name in names:
        for index, pattern in enumerate(compiled):
            if pattern is None:
                continue
            check_deadline()
            try:
                if index == 0:
                    renamed = pattern.sub(replacement, name, timeout=0.1, concurrent=False)[:512]
                    sample = {"before": name, "after": renamed} if renamed != name else None
                else:
                    matched = pattern.search(name, timeout=0.1, concurrent=False)
                    sample = {"name": name, "matches": True} if matched else None
            except (TimeoutError, regex.error) as error:
                results[index]["error"] = f"Pattern timed out: {error}"
                compiled[index] = None
                # Source continues to the next stream, skipping subsequent
                # patterns on this one row after a runtime pattern failure.
                break
            if sample is not None:
                result = results[index]
                result["count"] += 1
                if len(result["samples"]) < limit:
                    result["samples"].append(sample)
    check_deadline()
    return dict(zip(("find", "include", "exclude"), results))
