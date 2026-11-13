"""Exact field projections; no application or secret/token output on import."""
import copy,hashlib,json
RULES_VERSION=1
def evidence(raw):return {'redacted':True,'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}
def canonical(v):return json.dumps(v,sort_keys=True,separators=(',',':')).encode()
def project(original):
 out=copy.deepcopy(original);changes=[]
 def replace(parent,key,new,path,kind):
  old=parent[key];parent[key]=new;changes.append({'path':path,'kind':kind,'original_JSON_sha256':hashlib.sha256(canonical(old)).hexdigest(),'projected_JSON_sha256':hashlib.sha256(canonical(new)).hexdigest()})
 def walk(v,path='$'):
  if isinstance(v,dict):
   for key,x in list(v.items()):
    here=path+'.'+key
    if key in ('api_key','password','task_token','created_task_token','unknown_task_token')and isinstance(x,str):replace(v,key,evidence(x.encode()),here,'credential/task-token scalar -> byte hash/length')
    elif key=='raw_headers_private':
     for h,hexed in list(x.items()):replace(x,h,evidence(bytes.fromhex(hexed)),here+'.'+h,'raw authentication header hex -> byte hash/length')
    elif key=='path'and isinstance(x,str)and'?token='in x:replace(v,key,{'redacted':True,'template':x.split('?token=',1)[0]+'?token=<redacted>','original_bytes':len(x.encode()),'original_sha256':hashlib.sha256(x.encode()).hexdigest()},here,'task-token query URL -> route template/hash/length')
    elif key=='body_hex'and isinstance(x,str)and(v.get('ordinal')in(4,16)):
     replace(v,key,evidence(bytes.fromhex(x)),here,'case4 task-token JSON body / case16 credential-bearing PG archive -> exact byte hash/length')
    else:walk(x,here)
  elif isinstance(v,list):
   for i,x in enumerate(v):walk(x,path+'['+str(i)+']')
 walk(out);return out,changes
