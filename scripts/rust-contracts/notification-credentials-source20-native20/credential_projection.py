"""Hash-bound credential projections. No application, crypto signing or I/O on import."""
import base64,copy,hashlib,json
RULES_VERSION=1
def evidence(raw):return {'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw)}
def project(original):
 out=copy.deepcopy(original);changes=[]
 def replace(parent,key,value,path,kind):
  previous=parent[key];parent[key]=value;changes.append({'path':path,'kind':kind,'original_JSON_sha256':hashlib.sha256(json.dumps(previous,sort_keys=True,separators=(',',':')).encode()).hexdigest(),'projected_JSON_sha256':hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()})
 def walk(value,path='$'):
  if isinstance(value,dict):
   for k,item in list(value.items()):
    here=path+'.'+k
    if k=='tokens_private':
     for alias,token in item.items():
      raw=token['raw'];claims=json.loads(base64.urlsafe_b64decode(raw.split('.')[1]+'='*((-len(raw.split('.')[1]))%4)));public={'raw_evidence':evidence(raw.encode()),'sha256':token['sha256'],'authorization_header_evidence':evidence(('Bearer '+raw).encode()),'claim_summary':{'user_id':claims['user_id'],'token_type':claims['token_type'],'lifetime_seconds':claims['exp']-claims['iat']}}
      replace(item,alias,public,here+'.'+alias,'JWT raw/iat/exp/jti/signature omitted; hash+length+restricted claim summary')
    elif k=='raw_headers_private':
     for name,raw in list(item.items()):replace(item,name,evidence(bytes.fromhex(raw)),here+'.'+name,'raw authentication header hex replaced by exact byte hash/length')
    elif k=='api_key'and isinstance(item,str):replace(value,k,evidence(item.encode()),here,'raw API-key value replaced by exact byte hash/length')
    else:walk(item,here)
  elif isinstance(value,list):
   for i,item in enumerate(value):walk(item,path+'['+str(i)+']')
 walk(out);return out,changes
