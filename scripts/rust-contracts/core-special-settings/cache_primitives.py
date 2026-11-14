"""Restricted retained-fixture cache decoder, NEVER pickle.loads/Unpickler.
Only actual Source51 UA string/owned boolean sentinel primitive opcodes.
No globals, reducers, classes, memo reads, recursion or execution.
"""
import pickletools

def decode(raw):
 if type(raw)is not bytes or len(raw)>65536:raise ValueError('CacheByteCapacity')
 if raw and raw.lstrip(b'-').isdigit():return int(raw)
 stack=[];count=0;stopped=False
 for opcode,arg,pos in pickletools.genops(raw):
  count+=1
  if count>2048:raise ValueError('CacheOpcodeCapacity')
  name=opcode.name
  if name=='PROTO':
   if arg not in(4,5):raise ValueError('PrimitiveProtocol')
  elif name=='FRAME':
   if type(arg)is not int or arg<0 or arg>65536:raise ValueError('PrimitiveFrameCapacity')
  elif name in('SHORT_BINUNICODE','BINUNICODE'):
   if type(arg)is not str or len(arg.encode())>65536:raise ValueError('PrimitiveStringCapacity')
   stack.append(arg)
  elif name=='EMPTY_DICT':stack.append({})
  elif name in('NEWTRUE','NEWFALSE','NONE'):stack.append(True if name=='NEWTRUE'else False if name=='NEWFALSE'else None)
  elif name=='MEMOIZE':pass
  elif name=='SETITEM':
   if len(stack)<3:raise ValueError('PrimitiveStack')
   value,key=stack.pop(),stack.pop()
   if type(key)is not str or type(stack[-1])is not dict:raise ValueError('PrimitiveDict')
   stack[-1][key]=value
  elif name=='STOP':
   if len(stack)!=1 or pos+1!=len(raw):raise ValueError('PrimitiveTerminal')
   stopped=True
  else:raise ValueError('UnsupportedPrimitiveOpcode:'+name)
  if len(stack)>32:raise ValueError('PrimitiveStackCapacity')
 if not stopped:raise ValueError('PrimitiveSTOPRequired')
 return stack[0]
