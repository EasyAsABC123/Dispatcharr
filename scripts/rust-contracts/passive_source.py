"""Pinned Original passive factory binding; NOT HTTP acceptance.
Original Dispatcharr/Django/DRF authors retain application/framework credit.
This tool interrupts actual framework as_view factories BEFORE dispatch and
never calls authentication, permission, serializer, queryset or view handlers.
"""
import contextlib
import hashlib
import inspect
import json
import os
from pathlib import Path
import socket
import sys
import types
from unittest.mock import patch

if os.environ.get('CONTRACT_PASSIVE_DISPOSABLE_FIXTURE') != 'playback-fixture-only':
    raise RuntimeError('Explicit disposable fixture required')
expected=json.loads(Path('/passive-tools/expected-register.json').read_text())
# The expected file is copied, never regenerated from an inferred route matcher.
expected_rows=expected['runtime_routes']
sys.path.insert(0,'/app')
sys.argv=['passive-contract-probe','shell']
os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
os.environ['PYTHONDONTWRITEBYTECODE']='1'

class Blocked(Exception):pass
class Captured(Exception):pass
current={'stage':'bootstrap','ordinal':None}
violations=[]
def violation(kind):
    # Never record SQL text/params, endpoint URLs, credentials or arbitrary errors.
    violations.append({'kind':kind,'stage':current['stage'],'resolver_ordinal':current['ordinal']})
    raise Blocked(kind)
def no_connect(*args,**kwargs):return violation('network_connect_attempt')
def no_sql(execute,sql,params,many,context):return violation('SQL_attempt')
def no_database_connect(*args,**kwargs):return violation('database_connection_attempt')
def symbol(value):
    return str(getattr(value,'__module__',''))+'.'+str(getattr(value,'__qualname__',type(value).__qualname__))
def static(value,name,default=None):
    try:return inspect.getattr_static(value,name)
    except AttributeError:return default

report={'passed':False,'scope':'Passive actual pinned framework factory/allowed-method metadata only; no HTTP/status/roles or method-declaration acceptance',
        'routes':[],'violations':violations,'handler_invocations':0,'dispatch_executions':0,
        'middleware_auth_permission_queryset_serializer_execution':False,
        'credit':'Original Dispatcharr/Django/DRF authors retain application/framework credit and licensing.'}

def factory_codes(method):
    method=getattr(method,'__func__',method)
    return {item for item in method.__code__.co_consts if isinstance(item,types.CodeType) and item.co_name=='view'}

def leaves(patterns,prefix='',namespaces=()):
    from django.urls import URLPattern,URLResolver
    for pattern in patterns:
        path=prefix+str(pattern.pattern)
        if isinstance(pattern,URLResolver):
            yield from leaves(pattern.url_patterns,path,namespaces+((pattern.namespace,) if pattern.namespace else ()))
        elif isinstance(pattern,URLPattern):
            callback=pattern.callback
            view=getattr(callback,'cls',getattr(callback,'view_class',callback))
            yield {'pattern':path,'name':':'.join((*namespaces,pattern.name or '')),'view':symbol(view)},callback,view

def validate_inventory(observed, rows):
    if len(observed)!=592 or len(rows)!=592:raise Blocked('route_count_mismatch')
    if len({r['route_id'] for r in rows})!=592:raise Blocked('duplicate_route_id')
    if sum(not r['declared_methods_resolved'] for r in rows)!=322:raise Blocked('unresolved_count_mismatch')
    for ordinal,((identity,callback,view),route) in enumerate(zip(observed,rows)):
        if route['resolver_ordinal']!=ordinal:raise Blocked('resolver_ordinal_mismatch')
        if (identity['pattern'],identity['name'],identity['view'])!=(route['pattern_exact'],route['name'],route['source_handler']):
            raise Blocked('route_identity_mismatch')

def underlying_factory(callback,codes):
    wrappers=[];seen=set()
    for _ in range(12):
        if id(callback) in seen:return None,wrappers
        seen.add(id(callback))
        if isinstance(callback,types.FunctionType) and callback.__code__ in codes:return callback,wrappers
        wrappers.append(symbol(callback))
        callback=static(callback,'__wrapped__')
        if callback is None:break
    return None,wrappers

def bind_one(route,callback,view,View,allowed_codes):
    result={'contract_ids':[op['contract_id'] for op in route['operations']],
            'method_declarations_unchanged':[op['method'] for op in route['operations']],
            'state':'unresolved','binding':None,'exception_type':None}
    if not inspect.isclass(view) or not issubclass(view,View):
        result['reason']='not_supported_View_class';return result
    # Unknown application constructors/setup hooks can contain arbitrary handler
    # calls or effects. Refuse them; do not bypass and call it actual setup.
    if static(view,'__init__') is not View.__init__:
        result['reason']='custom_constructor_not_invoked';return result
    if static(view,'setup') is not View.setup:
        result['reason']='custom_setup_not_invoked';return result
    if (static(view,'__getattribute__') is not object.__getattribute__ or static(view,'__getattr__') is not None
        or static(view,'__setattr__') is not object.__setattr__ or static(view,'__new__') is not object.__new__
        or static(type(view),'__call__') is not type.__call__):
        result['reason']='custom_attribute_lookup_not_invoked';return result
    if static(view,'_allowed_methods') is not View._allowed_methods:
        result['reason']='custom_allowed_methods_not_invoked';return result
    names=static(view,'http_method_names')
    if not isinstance(names,(list,tuple)) or len(names)>16 or not all(type(x)is str and x in ['get','post','put','patch','delete','head','options','trace','connect'] for x in names):
        result['reason']='unsupported_http_method_names';return result
    actions=getattr(callback,'actions',None)
    if actions is not None and (type(actions)is not dict or len(actions)>16 or not all(type(k)is str and type(v)is str for k,v in actions.items())):
        result['reason']='unsupported_action_map';return result
    # as_view binding may getattr an action, setup may getattr get/head, and
    # _allowed_methods uses hasattr. Refuse dynamic/described application fields.
    attr_names=set(names)|set((actions or {}).values())
    origins={}
    for name in attr_names:
        value=static(view,name)
        if value is None:continue
        if not isinstance(value,types.FunctionType):
            result['reason']='unsupported_method_descriptor';return result
        origins[name]=symbol(value)
    factory,wrappers=underlying_factory(callback,allowed_codes)
    result['skipped_wrappers']=wrappers
    if factory is None:
        result['reason']='not_exact_framework_factory_closure';return result
    closure=dict(zip(factory.__code__.co_freevars,factory.__closure__ or ()))
    kwargs=closure.get('initkwargs')
    if kwargs is not None:
        values=kwargs.cell_contents
        if type(values)is not dict or len(values)>32 or any(inspect.isdatadescriptor(static(view,name)) for name in values):
            result['reason']='unsafe_initkwargs_descriptor';return result
    # DRF's actual ViewSet factory can insert HEAD into its shared actions dict.
    # Copy its closure cell rather than mutate callback.actions across aliases.
    cells=factory.__closure__ or ()
    def cell(value):return (lambda:value).__closure__[0]
    if 'actions' in factory.__code__.co_freevars:
        replaced=[]
        for name,item in zip(factory.__code__.co_freevars,cells):
            replaced.append(cell(dict(item.cell_contents)) if name=='actions' else item)
        factory=types.FunctionType(factory.__code__,factory.__globals__,factory.__name__,factory.__defaults__,tuple(replaced))
    observed={}
    def before_dispatch(instance,request,*args,**kwargs):
        current['stage']='allowed_methods_metadata'
        # Call only the pinned framework implementation, NOT a custom property,
        # custom dispatch, OPTIONS handler or View.initial/get_permissions path.
        allowed=View._allowed_methods(instance)
        if not isinstance(allowed,list) or not all(type(x)is str for x in allowed):raise Blocked('unexpected_metadata_shape')
        observed.update(allowed_methods=allowed,action_map=dict(getattr(instance,'action_map',{})),
                        bound_handlers={name:symbol(getattr(instance,name)) for name in names if hasattr(instance,name)},
                        original_handler_symbols=origins)
        raise Captured()
    try:
        current['stage']='framework_factory_constructor_setup'
        with patch.object(view,'dispatch',before_dispatch):
            factory(None)
    except Captured:
        result.update(state='metadata_observed',binding=observed,reason=None,
                      factory_binding_without_dispatch=True,
                      implicit_HEAD_separate='HEAD' in observed['allowed_methods'] and 'head' not in origins and 'get' in origins)
    except Exception as error:
        result.update(state='blocked_or_unresolved',exception_type=type(error).__name__,reason='factory_or_metadata_exception')
    else:
        result.update(state='blocked_or_unresolved',reason='factory_returned_without_interception')
    return result

try:
    with contextlib.ExitStack() as guards:
        guards.enter_context(patch.object(socket.socket,'connect',no_connect))
        guards.enter_context(patch.object(socket.socket,'connect_ex',no_connect))
        guards.enter_context(patch.object(socket,'create_connection',no_connect))
        guards.enter_context(patch.object(socket,'getaddrinfo',no_connect))
        guards.enter_context(patch.object(socket,'gethostbyname',no_connect))
        guards.enter_context(patch.object(socket.socket,'sendto',no_connect))
        # Install SQL and connection guards BEFORE application setup/URL imports.
        # If initialization needs database I/O, fail honestly; do not relax guard.
        import django
        from django.db import connections
        from django.db.backends.base.base import BaseDatabaseWrapper
        guards.enter_context(patch.object(BaseDatabaseWrapper,'ensure_connection',no_database_connect))
        for alias in connections:
            guards.enter_context(connections[alias].execute_wrapper(no_sql))
        from contextlib import redirect_stdout
        with redirect_stdout(sys.stderr):django.setup()
        from django.urls import get_resolver
        from django.views.generic.base import View
        from rest_framework.viewsets import ViewSetMixin
        current['stage']='resolver_import'
        observed=list(leaves(get_resolver().url_patterns))
        validate_inventory(observed,expected_rows)
        allowed_codes=factory_codes(View.as_view)|factory_codes(ViewSetMixin.as_view)
        report['framework_code_sha256']={symbol(fn):hashlib.sha256(inspect.getsource(fn).encode()).hexdigest() for fn in [View.as_view,ViewSetMixin.as_view,View.__init__,View.setup,View._allowed_methods]}
        report['metadata_layer']='Actual View._allowed_methods after trusted framework as_view constructor/setup/action binding, intercepted before dispatch; unknown overrides refused'
        for ordinal,((identity,callback,view),route) in enumerate(zip(observed,expected_rows)):
            if (identity['pattern'],identity['name'],identity['view'])!=(route['pattern_exact'],route['name'],route['source_handler']):
                raise Blocked('route_identity_mismatch')
            current['ordinal']=ordinal
            base={'route_id':route['route_id'],'resolver_ordinal':ordinal,'identity':identity,
                  'method_declarations_unchanged':[op['method'] for op in route['operations']]}
            if route['declared_methods_resolved']:
                base.update(state='already_declared_not_reprobed')
            else:base.update(bind_one(route,callback,view,View,allowed_codes))
            report['routes'].append(base)
        report.update(passed=not violations,versions={'Django':django.get_version()},
                      actual_HTTP_or_role_acceptance=False,unresolved_rows=sum(not r['declared_methods_resolved'] for r in expected_rows),
                      metadata_observed=sum(r['state']=='metadata_observed' for r in report['routes']))
except Exception as error:
    report.update(error_type=type(error).__name__,failure_stage=dict(current))
print(json.dumps(report,separators=(',',':')))
raise SystemExit(0 if report['passed'] else 1)
