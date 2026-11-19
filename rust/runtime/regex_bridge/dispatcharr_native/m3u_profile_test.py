"""Pinned Python JSON/regex primitives for the Original profile preview.
Original Dispatcharr authors retain all application credit.
"""
import json

import regex

from dispatcharr_native.profile_urls import _replacement
from dispatcharr_native.wire import CompatibilityUnavailable


URL_LIMIT = 8192
PATTERN_LIMIT = 255
TIMEOUT = 0.1
# Leave room for IPC escaping of the JSON scalar and the marked preview in
# the existing eight MiB actor frame. Count encoded fragments before retaining
# an unbounded substitution result, including reverse and zero-width matches.
RESULT_LIMIT = 2 * 1024 * 1024


def _substitute(url, search, replacement):
    if not search or not replacement:
        return url

    try:
        safe = _replacement(replacement)
        size = encoded_size(url)

        def expand(match):
            nonlocal size
            value = match.expand(safe)
            if len(value) > RESULT_LIMIT:
                raise CompatibilityUnavailable("Profile preview result capacity")
            size += encoded_size(value) - encoded_size(match.group(0))
            if size > RESULT_LIMIT:
                raise CompatibilityUnavailable("Profile preview result capacity")
            return value

        transformed, count = regex.subn(search, expand, url, timeout=TIMEOUT)
        return transformed if count else None
    except CompatibilityUnavailable:
        raise
    except Exception:
        return None


def encoded_size(value):
    return len(json.dumps(value)) - 2


def evaluate(request):
    if not isinstance(request, str):
        raise CompatibilityUnavailable("Profile preview request shape")

    fields = json.loads(request)
    url = fields.get("url") or ""
    search = fields.get("search") or ""
    replacement = fields.get("replace") or ""
    if any(
        not isinstance(value, str) or len(value) > limit
        for value, limit in (
            (url, URL_LIMIT),
            (search, PATTERN_LIMIT),
            (replacement, PATTERN_LIMIT),
        )
    ):
        preview = result = url[:URL_LIMIT] if isinstance(url, str) else ""
    else:
        try:
            preview = regex.sub(
                search,
                lambda match: f"<mark>{match.group(0)}</mark>",
                url,
                timeout=TIMEOUT,
            )
        except Exception:
            preview = url
        result = _substitute(url, search, replacement)

    # ASCII JSON escapes preserve surrogate code points through the UTF-8 IPC.
    return {"search_preview": json.dumps(preview), "result": json.dumps(result)}
