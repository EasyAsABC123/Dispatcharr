"""Hash-bound credential projections. No application, crypto signing or I/O on import."""
import base64,copy,hashlib,json
RULES_VERSION='notification-forms-native19-credential-projection-v1'
def evidence(raw):return {'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw)}
def project(original):
 out=copy.deepcopy(original);changes=[]
 def replace(parent,key,value,path,kind):
  previous=parent[key];parent[key]=value;changes.append({'path':path,'kind':kind,'original_JSON_sha256':hashlib.sha256(json.dumps(previous,sort_keys=True,separators=(',',':')).encode()).hexdigest(),'projected_JSON_sha256':hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()})
 def walk(value,path='$'):
  if isinstance(value,dict):
   for k,item in list(value.items()):
    here=path+'.'+k
    if k=='tokens_private':raise ValueError('NoNativeTokensDeclared')
    elif k=='raw_headers_private':
     for name,raw in list(item.items()):replace(item,name,evidence(bytes.fromhex(raw)),here+'.'+name,'raw authentication header hex replaced by exact byte hash/length')
    elif k=='api_key'and isinstance(item,str):replace(value,k,evidence(item.encode()),here,'raw API-key value replaced by exact byte hash/length')
    else:walk(item,here)
  elif isinstance(value,list):
   for i,item in enumerate(value):walk(item,path+'['+str(i)+']')
 walk(out);return out,changes
