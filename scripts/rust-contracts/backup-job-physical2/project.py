"""Exact scoped owned API-key projection, no app/crypto-signing on import."""
import copy,hashlib,json
def evidence(raw):return {"sha256":hashlib.sha256(raw).hexdigest(),"bytes":len(raw)}
def project(original):
 out=copy.deepcopy(original);changes=[]
 def walk(value,path="$" ):
  if isinstance(value,list):
   for i,item in enumerate(value):walk(item,path+"["+str(i)+"]")
  elif isinstance(value,dict):
   for key,item in list(value.items()):
    here=path+"."+key
    if key=="api_key"and type(item)is str:
     value[key]=evidence(item.encode());changes.append({"path":here,"original_byte_sha256":value[key]["sha256"],"original_bytes":value[key]["bytes"],"rule":"owned persisted APIkey string -> exactbyte SHA/length"})
    else:walk(item,here)
 walk(out);return out,changes
