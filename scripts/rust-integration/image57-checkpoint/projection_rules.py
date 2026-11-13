import copy,hashlib,json,re
canonical=lambda x:hashlib.sha256(json.dumps(x,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()
secret={'password','api_key','access_token','refresh_token','authorization','proxy-authorization','cookie','set-cookie','secret','boot_id','nonce','x-api-key','http_x_api_key','http_authorization'}
jwt=re.compile(r'eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}')
redactions=[]
def concealed(x,path,why):
 h=canonical(x);redactions.append({'field_path':path,'private_value_sha256':h,'reason':why});return {'private_value_sha256':h,'excluded':why}
def hassecret(x):
 if isinstance(x,dict):return any(k.lower()in secret or hassecret(v)for k,v in x.items())
 if isinstance(x,list):return any(hassecret(v)for v in x)
 return False
def clean(x,path=''):
 if isinstance(x,dict):return{k:concealed(v,path+'/'+k,'private credential or once-only identity')if k.lower()in secret else clean(v,path+'/'+k)for k,v in x.items()}
 if isinstance(x,list):
  if len(x)==2 and isinstance(x[0],str)and x[0].lower()in secret:return[x[0],concealed(x[1],path+'/1','private response/request header')]
  return[clean(v,path+'/'+str(i))for i,v in enumerate(x)]
 if isinstance(x,str):
  if jwt.search(x):return concealed(x,path,'JWT-containing opaque field')
  # Whole opaque auth/user DTO bodies are deliberately not decoded or rebuilt as observed bytes.
  candidates=[x]
  if len(x)%2==0 and re.fullmatch('[0-9a-fA-F]{2,}',x):
   try:candidates.append(bytes.fromhex(x).decode())
   except (ValueError,UnicodeDecodeError):pass
  for s in candidates:
   if jwt.search(s):return concealed(x,path,'JWT-containing literal or hex opaque field')
   try:v=json.loads(s)
   except (ValueError,TypeError):continue
   if hassecret(v):return concealed(x,path,'opaque serialized DTO containing private fields')
  return x
 return x

secret.update({'request_query_private','access','refresh','xc_password'})
