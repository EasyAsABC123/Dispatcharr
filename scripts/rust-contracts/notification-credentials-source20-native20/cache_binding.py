"""Read-only binding of the finite <=2-entry auth history to actual Redis DUMP.
Only the retained short Redis string/plain-or-LZF encoding and harmless pickle list/float
opcodes are accepted. This is not a general RDB or untrusted pickle loader.
"""
import math,pickle,pickletools
KEY=b':1:throttle_api_key_auth_127.0.0.1'.hex()
def require(ok,label):
 if not ok:raise ValueError(label)
def payload(dump):
 require(len(dump)>=12 and dump[0]==0,'RetainedRedisStringOnly')
 if dump[1]<64:
  size=dump[1];require(len(dump)==2+size+10,'ExactPlainDumpLength');return dump[2:2+size]
 require(dump[1]==0xc3 and dump[2]<64 and dump[3]<64,'RetainedSmallLZFOnly');compressed_size,size=dump[2:4];require(size<=128 and len(dump)==4+compressed_size+10,'ExactLZFDumpLength');data=dump[4:4+compressed_size];out=bytearray();i=0
 while i<len(data):
  control=data[i];i+=1
  if control<32:
   count=control+1;require(i+count<=len(data),'LZFLiteralBounds');out.extend(data[i:i+count]);i+=count
  else:
   count=control>>5;offset=(control&31)<<8
   if count==7:require(i<len(data),'LZFExtendedLength');count+=data[i];i+=1
   require(i<len(data),'LZFReferenceOffset');offset+=data[i];i+=1;position=len(out)-offset-1;require(position>=0,'LZFReferenceBounds')
   for _ in range(count+2):require(position<len(out),'LZFReferenceRead');out.append(out[position]);position+=1;require(len(out)<=128,'LZFDecodedCapacity')
  require(len(out)<=size,'LZFDeclaredCapacity')
 require(len(out)==size,'ExactLZFDecodedLength');return bytes(out)
def validate(cache,*,native):
 history=cache['api_key_failed_auth_history'];rows=[r for r in cache['rows']if r['key_hex']==KEY]
 if history is None:
  require(rows==[],'MissingHistoryMeansNoActualRedisKey')
  if native:require(cache['api_key_failed_auth_raw_hex']is None,'MissingHistoryMeansNoRawGET')
  return
 require(type(history)is list and len(history)<=2 and all(type(x)in(int,float)and math.isfinite(x)for x in history),'FiniteLiteralHistory');require(len(rows)==1,'ExactlyOneActualHistoryRedisKey');raw=payload(bytes.fromhex(rows[0]['dump_hex']))
 operations=list(pickletools.genops(raw));allowed={'PROTO','FRAME','EMPTY_LIST','MARK','BINFLOAT','APPEND','APPENDS','MEMOIZE','BINPUT','LONG_BINPUT','STOP'};require(operations and operations[-1][0].name=='STOP'and operations[-1][2]==len(raw)-1 and all(op.name in allowed for op,arg,pos in operations),'HarmlessExactPickleOpcodesOnly');decoded=pickle.loads(raw);require(type(decoded)is list and all(type(x)is float and math.isfinite(x)for x in decoded)and decoded==history,'DumpPickleMatchesDecodedHistory')
 if native:require(bytes.fromhex(cache['api_key_failed_auth_raw_hex'])==raw,'NativeGETBytesMatchActualDump')
