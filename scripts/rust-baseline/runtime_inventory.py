"""Resolve Django routes and serializer schema from the actual baseline runtime."""
import json
import os
import sys
from contextlib import redirect_stdout

sys.path.insert(0, '/app')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dispatcharr.settings')
import django
with redirect_stdout(sys.stderr):
    django.setup()
from django.urls import URLPattern, URLResolver, get_resolver
from drf_spectacular.generators import SchemaGenerator


def walk(patterns, prefix='', namespaces=()):
    for pattern in patterns:
        path = prefix + str(pattern.pattern)
        if isinstance(pattern, URLResolver):
            yield from walk(pattern.url_patterns, path,
                            namespaces + ((pattern.namespace,) if pattern.namespace else ()))
        elif isinstance(pattern, URLPattern):
            callback = pattern.callback
            view = getattr(callback, 'cls', getattr(callback, 'view_class', callback))
            yield {'pattern': path, 'name': ':'.join((*namespaces, pattern.name or '')),
                   'view': f'{view.__module__}.{view.__qualname__}',
                   'methods': getattr(callback, 'actions', None)}


with redirect_stdout(sys.stderr):
    schema = SchemaGenerator().get_schema(request=None, public=True)
print(json.dumps({'routes': list(walk(get_resolver().url_patterns)),
                  'openapi': schema}, indent=2, default=str))
