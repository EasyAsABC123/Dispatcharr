"""Exact original preview/transform bodies, isolated from ORM/auth dependencies.

The original authors retain application credit. Auth behavior is separately
wire-proven; this fixture supplies only the captured admin-level constant.
"""
import ast
import asyncio
import hashlib
import json
from pathlib import Path
import sys
import time
import types
import typing
import regex

source=Path('/source/consumers.py').read_text();urls=Path('/source/url_utils.py').read_text()
consumer_tree=ast.parse(source);url_tree=ast.parse(urls)
receive=next(node for node in ast.walk(consumer_tree) if isinstance(node,ast.AsyncFunctionDef) and node.name=='receive')
admin=next(node for node in consumer_tree.body if isinstance(node,ast.FunctionDef) and node.name=='user_is_admin')
transform=next(node for node in url_tree.body if isinstance(node,ast.FunctionDef) and node.name=='transform_url')
constants=[node for node in consumer_tree.body if isinstance(node,ast.Assign) and any(isinstance(target,ast.Name) and target.id.startswith('_M3U_PROFILE_TEST_') for target in node.targets)]
constants += [node for node in url_tree.body if isinstance(node,ast.Assign) and any(isinstance(target,ast.Name) and target.id=='URL_TRANSFORM_REGEX_TIMEOUT' for target in node.targets)]
namespace={'regex':regex,'json':json,'Optional':typing.Optional,'logger':types.SimpleNamespace(debug=lambda *a,**kw:None,info=lambda *a,**kw:None,warning=lambda *a,**kw:None,error=lambda *a,**kw:None)}
exec(compile(ast.Module(body=[*constants,admin,transform,receive],type_ignores=[]),'/source/exact-methods.py','exec'),namespace)
models=types.ModuleType('apps.accounts.models');models.User=types.SimpleNamespace(UserLevel=types.SimpleNamespace(ADMIN=10));sys.modules[models.__name__]=models
utils=types.ModuleType('apps.proxy.live_proxy.url_utils');utils.transform_url=namespace['transform_url'];sys.modules[utils.__name__]=utils
class Consumer:
 def __init__(self,role):self.scope={'user':types.SimpleNamespace(is_authenticated=True,user_level=role)};self.sent=[]
 async def send(self,text_data):self.sent.append(json.loads(text_data))
async def main():
 request=json.load(sys.stdin);results=[]
 for row in request['cases']:
  consumer=Consumer(row.get('role',10));start=time.monotonic();error=None
  try:await namespace['receive'](consumer,json.dumps(row['input']))
  except Exception as exception:error=type(exception).__name__
  results.append({'case':row['case'],'input':row['input'],'role':row.get('role',10),'sent':consumer.sent,'exception':error,'elapsed_seconds':time.monotonic()-start})
 native=Path(regex._regex.__file__)
 print(json.dumps({'scope':'unchanged original consumer.receive and transform_url; auth/ORM dependency shims only',
  'python':sys.version.split()[0],'regex_version':regex.__version__,'regex_default_version':str(regex.DEFAULT_VERSION),
  'regex_native_sha256':hashlib.sha256(native.read_bytes()).hexdigest(),
  'source_sha256':{'dispatcharr/consumers.py':hashlib.sha256(source.encode()).hexdigest(),'apps/proxy/live_proxy/url_utils.py':hashlib.sha256(urls.encode()).hexdigest()},
  'method_sha256':{'receive':hashlib.sha256(ast.get_source_segment(source,receive).encode()).hexdigest(),'user_is_admin':hashlib.sha256(ast.get_source_segment(source,admin).encode()).hexdigest(),'transform_url':hashlib.sha256(ast.get_source_segment(urls,transform).encode()).hexdigest()},
  'cases':results,'case_count':len(results)}))
asyncio.run(main())
