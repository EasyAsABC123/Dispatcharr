"""Supplement the pinned Original route roster with framework declarations."""
import argparse
from collections import Counter
import hashlib
import inspect
import json
import os
from pathlib import Path
import sys

HTTP_METHODS = ("get", "post", "put", "patch", "delete", "head", "options", "trace")


def class_methods(view):
    declared = getattr(view, "http_method_names", ())
    methods = {}
    for method in HTTP_METHODS:
        if method not in declared:
            continue
        if callable(getattr(view, method, None)):
            methods[method.upper()] = method
        elif method == "head" and callable(getattr(view, "get", None)):
            # Django View.setup aliases HEAD to GET when no head handler exists.
            methods["HEAD"] = "get"
    return methods


def decorated_methods(callback, require_http_methods):
    # Match the actual wrapper code created by the admitted installed Django
    # decorator. Arbitrary wrapped functions and guessed code shapes stay open.
    async def async_request(request):
        return request

    wrap = require_http_methods([])
    wrapper_codes = {wrap(lambda request: request).__code__, wrap(async_request).__code__}
    seen = set()
    while callback is not None and id(callback) not in seen:
        seen.add(id(callback))
        if getattr(callback, "__code__", None) in wrapper_codes:
            variables = inspect.getclosurevars(callback).nonlocals
            methods = variables.get("request_method_list")
            if type(methods) in (list, tuple) and all(type(method) is str for method in methods):
                return {method: "wrapped_function" for method in sorted(set(methods))}
        callback = getattr(callback, "__wrapped__", None)
    return None


def declarations(callback, require_http_methods):
    actions = getattr(callback, "actions", None)
    if isinstance(actions, dict) and actions:
        return "DRF_router_actions", {key.upper(): value for key, value in actions.items()}
    view = getattr(callback, "cls", getattr(callback, "view_class", None))
    if view is not None:
        return "Django_class_handlers", class_methods(view)
    methods = decorated_methods(callback, require_http_methods)
    if methods is not None:
        return "Django_explicit_method_decorator", methods
    return "dynamic_or_undeclared", {}


def route_rows(patterns, require_http_methods, url_pattern, url_resolver, prefix="", namespaces=()):
    for pattern in patterns:
        path = prefix + str(pattern.pattern)
        if isinstance(pattern, url_resolver):
            nested = namespaces + ((pattern.namespace,) if pattern.namespace else ())
            yield from route_rows(pattern.url_patterns, require_http_methods, url_pattern, url_resolver,
                                  path, nested)
        elif isinstance(pattern, url_pattern):
            callback = pattern.callback
            view = getattr(callback, "cls", getattr(callback, "view_class", callback))
            kind, observed = declarations(callback, require_http_methods)
            yield {
                "pattern": path,
                "name": ":".join((*namespaces, pattern.name or "")),
                "view": f"{view.__module__}.{view.__qualname__}",
                "methods": getattr(callback, "actions", None),
                "dispatch_declaration": {"kind": kind, "methods": observed},
            }


def reconcile(retained, rows):
    def identity(row):
        return tuple(row[name] for name in ("pattern", "name", "view"))

    if [identity(row) for row in retained] != [identity(row) for row in rows]:
        raise ValueError("Actual Original URL resolver roster differs from retained ownership")
    report = []
    for ordinal, (prior, row) in enumerate(zip(retained, rows, strict=True)):
        if prior["methods"] != row["methods"]:
            # The baseline generates its OpenAPI schema before capturing the
            # roster, and router action maps may be modified during discovery.
            raise ValueError(f"Original router action map differs at route {ordinal}")
        report.append({"resolver_ordinal": ordinal, **row})
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--retained", type=Path, required=True)
    parser.add_argument("--retained-sha256", required=True)
    parser.add_argument("--app", default="/app")
    args = parser.parse_args()
    raw = args.retained.read_bytes()
    if hashlib.sha256(raw).hexdigest() != args.retained_sha256:
        raise ValueError("Retained Original resolver digest differs")
    retained = json.loads(raw)["routes"]
    sys.path.insert(0, args.app)
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "dispatcharr.settings")
    from contextlib import redirect_stdout
    with redirect_stdout(sys.stderr):
        import django
        django.setup()
        from django.urls import URLPattern, URLResolver, get_resolver
        from django.views.decorators.http import require_http_methods
        from drf_spectacular.generators import SchemaGenerator
        SchemaGenerator().get_schema(request=None, public=True)
        rows = list(route_rows(get_resolver().url_patterns, require_http_methods,
                               URLPattern, URLResolver))
    resolved = reconcile(retained, rows)
    print(json.dumps({
        "scope": "framework method declarations for the admitted Original resolver",
        "retained_inventory_sha256": args.retained_sha256,
        "kinds": Counter(row["dispatch_declaration"]["kind"] for row in resolved),
        "routes": resolved,
        "HTTP_requests_performed": 0,
    }, indent=2))


if __name__ == "__main__":
    main()
