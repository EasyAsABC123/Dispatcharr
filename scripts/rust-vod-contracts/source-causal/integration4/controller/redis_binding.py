"""Read-only redis-py code binding; no client/method replacement or traffic."""
import hashlib,types
from pathlib import Path

def describe():
 import redis
 from redis.client import Redis,Pipeline
 selected=[]
 for cls,name in[(Redis,'_send_command_parse_response'),(Pipeline,'_execute_transaction'),(Pipeline,'_execute_pipeline')]:
  function=getattr(cls,name,None);code=getattr(function,'__code__',None)
  if not isinstance(code,types.CodeType):raise ValueError('PinnedRedisWireCodeMissing '+name)
  selected.append(code)
 paths={code.co_filename for code in selected}
 return{'version':redis.__version__,'files':{name:hashlib.sha256(Path(name).read_bytes()).hexdigest()for name in paths},'codes':[{'qualname':c.co_qualname,'firstline':c.co_firstlineno,'code_sha256':hashlib.sha256(c.co_code).hexdigest()}for c in selected]},selected

def bind(probe,expected_files):
 metadata,codes=describe()
 if metadata['files']!=expected_files:raise ValueError('ExactRedisLibraryFileBindingRequired')
 for code in codes:probe.add(code,'wire_call')
 return metadata
