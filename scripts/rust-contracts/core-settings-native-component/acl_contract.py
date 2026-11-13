"""Bounded fixture ACL reply/readback contract; no Redis imports or dispatch."""
def need(ok,label):
 if not ok:raise ValueError(label)
def typed(value):
 budget={'nodes':0,'bytes':0}
 def walk(v,depth=0):
  budget['nodes']+=1;need(depth<=8 and budget['nodes']<=512,'ACLReplyStructureBound')
  if type(v)is bytes:
   budget['bytes']+=len(v);need(budget['bytes']<=65536,'ACLReplyByteBound');return {'type':'bytes','hex':v.hex()}
  if type(v)is str:
   budget['bytes']+=len(v.encode());need(budget['bytes']<=65536,'ACLReplyByteBound');return {'type':'str','value':v}
  if type(v)is bool:return {'type':'bool','value':v}
  if type(v)is int:
   need(-(2**63)<=v<=2**63-1,'ACLIntegerBound');return {'type':'int','value':v}
  if v is None:return {'type':'NoneType'}
  if type(v)is list:return {'type':'list','items':[walk(x,depth+1)for x in v]}
  need(False,'UnexpectedACLReplyType')
 return walk(value)
def untyped(v,depth=0):
 need(type(v)is dict and depth<=8,'TypedACLShape')
 kind=v.get('type')
 if kind=='bytes':need(set(v)=={'type','hex'}and type(v['hex'])is str and len(v['hex'])<=131072,'TypedACLBytes');out=bytes.fromhex(v['hex'])
 elif kind=='str':need(set(v)=={'type','value'}and type(v['value'])is str,'TypedACLString');out=v['value']
 elif kind=='bool':need(set(v)=={'type','value'}and type(v['value'])is bool,'TypedACLBool');out=v['value']
 elif kind=='int':need(set(v)=={'type','value'}and type(v['value'])is int,'TypedACLInt');out=v['value']
 elif kind=='NoneType':need(set(v)=={'type'},'TypedACLNone');out=None
 elif kind=='list':need(set(v)=={'type','items'}and type(v['items'])is list and len(v['items'])<=512,'TypedACLList');out=[untyped(x,depth+1)for x in v['items']]
 else:raise ValueError('UnknownTypedACLKind')
 if depth==0:typed(out)
 return out
def ok_reply(v):
 typed(v)
 return v is True or type(v)is bytes and v==b'OK' or type(v)is str and v=='OK'
def text(v):
 need(type(v)in(bytes,str),'ACLTextType');s=v.decode('ascii')if type(v)is bytes else v;need(s.isascii()and len(s)<=8192,'ACLAsciiBound');return s
def snapshot(value):
 typed(value);need(type(value)is list and len(value)%2==0 and 8<=len(value)<=16,'ACLGetUserRESP2')
 out={}
 for i in range(0,len(value),2):
  key=text(value[i]);need(key not in out and key in{'flags','passwords','commands','keys','channels','selectors'},'ACLUniqueKnownFields');out[key]=value[i+1]
 need({'flags','passwords','commands','keys'}<=set(out),'MandatoryACLFields');need(type(out['flags'])is list and 'on'in[text(x)for x in out['flags']],'ACLDefaultUserOn');need(type(out['passwords'])is list,'ACLPasswordArray');text(out['commands']);return out
def tokens(s):
 parts=text(s).split();need(parts and len(parts)==len(set(parts))and all(x.startswith(('+','-'))for x in parts),'ACLCommandTokens');return set(parts)
def disabled_DEL(before,after):
 need(set(before)==set(after),'SameACLFields');a=tokens(before['commands']);b=tokens(after['commands']);need('+@all'in a and '-del'not in a,'InitialDELAllowedByFixtureAllCommands');need(b==(a-{'+del'})|{'-del'},'OnlyDELRemoved')
 for k in before:
  if k=='commands':continue
  if k=='flags':need({text(x)for x in before[k]}-{'allcommands'}=={text(x)for x in after[k]}-{'allcommands'},'OnlyDerivedAllCommandsFlagMayChange')
  else:need(before[k]==after[k],'OtherACLStateUnchanged')
 return True
def restored(before,after):need(before==after,'ExactACLRestoration');return True

# Proposed syntax remains blocked until the pinned-image primitive qualifies it.
def restore_rule_tokens(before):
 rules=text(before['commands']).split();tokens(before['commands'])
 return ['nocommands',*rules]
