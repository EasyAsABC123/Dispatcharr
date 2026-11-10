"""Read-only dispatch-candidate discovery; never executes a view or proves roles.
Original Dispatcharr authors retain application credit; Django/DRF authors retain
framework credit. The supported original image and owned fixture are required.
"""
import hashlib,inspect,json,os,sys
if os.environ.get('CONTRACT_METHOD_DISPOSABLE_FIXTURE')!='playback-fixture-only':raise RuntimeError('Explicit owned fixture required')
sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from django.db import connection
from django.urls import URLPattern,URLResolver,get_resolver

def static(value,name,default=None):
 try:return inspect.getattr_static(value,name)
 except AttributeError:return default
def symbol(value):return str(getattr(value,'__module__',''))+'.'+str(getattr(value,'__qualname__',type(value).__qualname__))
def walk(patterns,prefix='',namespaces=()):
 for pattern in patterns:
  path=prefix+str(pattern.pattern)
  if isinstance(pattern,URLResolver):yield from walk(pattern.url_patterns,path,namespaces+((pattern.namespace,)if pattern.namespace else()))
  elif isinstance(pattern,URLPattern):
   callback=pattern.callback;view=getattr(callback,'cls',getattr(callback,'view_class',callback));actions=getattr(callback,'actions',None)
   record={'pattern':path,'name':':'.join((*namespaces,pattern.name or'')),'view':symbol(view),'viewset_actions':actions,'handler_method_candidates':None,'implicit_HEAD_candidate':False,'default_permission_candidates':None,'dynamic_permission_method':None,'operational_method_acceptance':False,'role_acceptance':False}
   if inspect.isclass(view):
    declared=static(view,'http_method_names')
    if isinstance(declared,(list,tuple))and all(isinstance(v,str)for v in declared):
     present=[name.upper()for name in declared if static(view,name)is not None];setup=static(view,'setup')
     if 'GET'in present and 'HEAD'not in present and 'head'in declared and symbol(setup)=='django.views.generic.base.View.setup':present.append('HEAD');record['implicit_HEAD_candidate']=True
     record['handler_method_candidates']=sorted(present)
    permissions=static(view,'permission_classes')
    if isinstance(permissions,(tuple,list)):record['default_permission_candidates']=[symbol(v)for v in permissions]
    method=static(view,'get_permissions')
    if method is not None:record['dynamic_permission_method']=symbol(method)
   yield record
queries=[]
def no_query(execute,sql,params,many,context):
 queries.append(str(sql)[:200]);raise RuntimeError('Method inspection must not execute SQL')
with connection.execute_wrapper(no_query):records=list(walk(get_resolver().url_patterns))
print(json.dumps({'scope':'Read-only handler/action dispatch candidates; not executed HTTP or role authorization','versions':{'Django':django.get_version()},'routes':records,'view_invocations':0,'inspection_SQL_statements':len(queries),'actual_method_or_permission_acceptance':False,'credit':'Original Dispatcharr application authors and Django/DRF framework authors retain their credit and licensing.'},separators=(',',':')))
