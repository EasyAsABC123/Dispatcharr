"""Explicit Source12 credential projection; no app, signing or network activity."""
import copy,hashlib,json
RULES_VERSION='notification-forms-source12-credential-projection-v1'
def encode(value):return json.dumps(value,sort_keys=True,separators=(',',':')).encode()
def evidence(raw):return {'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw)}
def project(original):
 output=copy.deepcopy(original);changes=[]
 def replace(parent,key,value,path,kind,fingerprint=False):
  old=parent[key];parent[key]=value;entry={'path':path,'kind':kind,'original_JSON_sha256':hashlib.sha256(encode(old)).hexdigest(),'projected_JSON_sha256':hashlib.sha256(encode(value)).hexdigest()}
  if fingerprint:entry.update(original_fingerprint=old,projected_fingerprint=value)
  changes.append(entry)
 def walk(value,path='$'):
  if isinstance(value,list):
   for index,item in enumerate(value):walk(item,path+'['+str(index)+']')
  elif isinstance(value,dict):
   for key,item in list(value.items()):
    if key=='api_key'and type(item)is str:replace(value,key,evidence(item.encode()),path+'.'+key,'persisted API-key bytes -> hash/length')
    elif key=='raw_headers_private':
     for name,raw in list(item.items()):replace(item,name,evidence(bytes.fromhex(raw)),path+'.'+key+'.'+name,'authentication header hex -> exact byte hash/length')
    elif key=='raw_json_text'and isinstance(item,list):
     for index,text in enumerate(item):
      row=json.loads(text);private=row.get('api_key')
      if type(private)is str:
       literal=json.dumps(private,ensure_ascii=False);assert text.count(literal)==1
       replacement=json.dumps(evidence(private.encode()),sort_keys=True,ensure_ascii=False)
       replace(item,index,text.replace(literal,replacement,1),path+'.'+key+'['+str(index)+']','SQL raw text API-key literal -> evidence object; NEW projected text, not original SQL bytes')
    else:walk(item,path+'.'+key)
   if {'database','tables','sequences','dataset_sha256'}<=set(value):
    for table,row in value['tables'].items():
     raw=b'['+b','.join(t.encode()for t in row['raw_json_text'])+b']';new=hashlib.sha256(raw).hexdigest()
     if new!=row['sha256']:replace(row,'sha256',new,path+'.tables.'+table+'.sha256','dependent SHA recomputed ONLY over newly projected SQL text',True)
    new=hashlib.sha256(json.dumps({'tables':value['tables'],'sequences':value['sequences']},sort_keys=True).encode()).hexdigest()
    if new!=value['dataset_sha256']:replace(value,'dataset_sha256',new,path+'.dataset_sha256','dependent dataset SHA recomputed ONLY over projected table records',True)
   if 'audit'in value and 'public_manifest'in value:
    for table,row in value['audit']['tables'].items():
     new=hashlib.md5(('['+', '.join(sorted(row['raw_json_text']))+']').encode()).hexdigest();target=value['public_manifest']['tables'][table]
     if target['sha_like_md5']!=new:replace(target,'sha_like_md5',new,path+'.public_manifest.tables.'+table+'.sha_like_md5','dependent legacy MD5 recomputed ONLY over new projected SQL text; actual original MD5 retained as opaque evidence',True)
 walk(output);return output,changes
