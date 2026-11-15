"""Pinned regex dependency only. Original Dispatcharr authors retain credit.
No Source task/body, Django, application, SQL, Redis, HTTP or plugin import.
The Rust owner retains all ordering/effects and process/admission accounting.
"""
import json,os,sys
import regex
from dispatcharr_native.wire import CompatibilityUnavailable,operation

def caught(error):
 return {'actual_class':type(error).__name__,'message':str(error)[:4096]}

def evaluate(value):
 if not isinstance(value,dict):raise CompatibilityUnavailable('Regex request shape')
 names=value.get('names');config=value.get('config')
 if not isinstance(names,list)or len(names)>100 or any(not isinstance(s,str)or len(s)>512 for s in names):raise CompatibilityUnavailable('Regex name batch capacity')
 if not isinstance(config,dict)or len(json.dumps(config,ensure_ascii=False).encode('utf8'))>65536:raise CompatibilityUnavailable('Regex configuration capacity')
 # Inclusion/exclusion are separate calls so Rust preserves the original
 # filter -> sorting -> per-stream rename order and QuerySet/list distinction.
 kind=value.get('kind')
 if kind=='translate':
  translated=regex.sub(r'\$(\d+)',r'\\\1',config.get('replacement'))
  if len(translated.encode('utf8'))>131072:raise CompatibilityUnavailable('Regex translated replacement capacity')
  return {'replacement':translated}
 if kind=='filter':
  pattern=config.get('pattern')
  if not pattern:return {'active':False,'matches':None,'caught':None}
  try:
   compiled=regex.compile(pattern,regex.IGNORECASE)
   matches=[bool(compiled.search(name))for name in names]
  except regex.error as error:return {'active':False,'matches':None,'caught':caught(error)}
  # TypeError and other exceptions propagate to the actual operation failure.
  # Search has no Source timeout. Lifecycle termination is unknown, not false.
  return {'active':True,'matches':matches,'caught':None}
 if kind=='rename':
  pattern=config.get('pattern');replacement=config.get('replacement')
  if replacement is None:replacement=''
  result=[]
  for name in names:
   try:
    renamed=name if pattern is None else regex.sub(pattern,regex.sub(r'\$(\d+)',r'\\\1',replacement),name,timeout=.1)
   except (regex.error,TimeoutError)as error:
    result.append({'name':name[:512],'caught':caught(error),'other_failure':None})
   except Exception as error:
    result.append({'name':None,'caught':None,'other_failure':caught(error)})
   else:
    # Clamp this actual result before retaining/serializing the next item.
    result.append({'name':renamed[:512],'caught':None,'other_failure':None})
  return {'items':result}
 raise CompatibilityUnavailable('Unknown regex operation')

def serve(channel):
 channel.send({'kind':'ready','actual_pid':os.getpid(),'version':sys.version,'regex_version':regex.__version__,'application_server_started':False,'dependency_only':True})
 try:
  while True:
   request=channel.calls.get()
   if request is None:break
   ident=request['id'];token=operation.set(ident);finish=False
   try:
    if request['method']=='shutdown':result=None;finish=True
    elif request['method']=='regex':result=evaluate(request['args'])
    else:raise CompatibilityUnavailable('Unknown regex-only method')
    channel.send({'kind':'result','id':ident,'result':result,'error':None})
   except Exception as error:channel.send({'kind':'result','id':ident,'result':None,'error':caught(error)})
   finally:operation.reset(token)
   if finish:break
 finally:
  channel.close_writer();channel.thread.join(30)
  if channel.thread.is_alive():raise CompatibilityUnavailable('Regex RPC reader has not observed EOF')
