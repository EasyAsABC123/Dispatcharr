"""Existing restricted primitive Django cache decoder; no Python globals/imports.
Original Dispatcharr authors retain application credit.
"""
import base64,io,math,pickle,pickletools
from row_compare import require
class Restricted(pickle.Unpickler):
 def find_class(self,*unused):raise ValueError('NoPickleGlobals')
 def persistent_load(self,*unused):raise ValueError('NoPersistentPickle')
def cache_value(raw):
 if raw is None:return None
 data=base64.b64decode(raw,validate=True);require(len(data)<=1048576,'CacheByteCap')
 forbidden={'GLOBAL','STACK_GLOBAL','REDUCE','BUILD','NEWOBJ','NEWOBJ_EX','OBJ','INST','EXT1','EXT2','EXT4','PERSID','BINPERSID'}
 require(all(op.name not in forbidden for op,_,_ in pickletools.genops(data)),'DataOnlyPickleOpcodes')
 value=Restricted(io.BytesIO(data)).load();nodes=0
 def check(v,depth):
  nonlocal nodes
  nodes+=1;require(nodes<=16384 and depth<=32,'CacheLogicalCap')
  require(type(v)in(dict,list,str,int,float,bool,type(None)),'CachePrimitiveOnly')
  if type(v)is float:require(math.isfinite(v),'FiniteCacheFloat')
  if type(v)is dict:
   require(all(type(k)is str for k in v),'CacheStringKeys')
   for item in v.values():check(item,depth+1)
  elif type(v)is list:
   for item in v:check(item,depth+1)
 check(value,0);return value
