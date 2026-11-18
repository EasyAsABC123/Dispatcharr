"""Pinned regex/urllib dependency operations only; Rust owns SQL and selection.
Source URL semantics from Dispatcharr; Original authors retain all credit.
No application, network, DB/cache, task, plugin or subprocess import.
"""
import json
from urllib.parse import urlparse,urlunparse
import regex
MAX_PACKET=1024*1024
MAX_VALUE=65536
class Capacity(Exception):pass

def _normalize(value):
 if not value:return value
 parsed=urlparse(value.strip());path=parsed.path.rstrip('/');last=path.rsplit('/',1)[-1]
 if last.endswith('.php'):path=path[:-(len(last)+1)]if'/'in path else''
 return urlunparse((parsed.scheme,parsed.netloc,path,'','',''))
def _replacement(value):
 return regex.sub(r'\$(\d+)',r'\\\1',regex.sub(r'\$<([^>]+)>',r'\\g<\1>',value))
def _substitute(value,search,replacement):
 if not search or not replacement:return value,None
 # Keep actual pinned replacement expansion. A callable prevents the engine
 # accumulating a result beyond the admitted wire/URL budget. Source field
 # codepoint bounds also cap any one match.expand temporary before this check.
 last=0;used=0
 try:
  safe=_replacement(replacement)
  def append(match):
   nonlocal last,used
   piece=match.expand(safe)
   used+=len(value[last:match.start()].encode('utf8'))+len(piece.encode('utf8'))
   if used>MAX_VALUE:raise Capacity('Profile URL replacement capacity')
   last=match.end();return piece
  result,count=regex.subn(search,append,value,timeout=.1)
  if count and used+len(value[last:].encode('utf8'))>MAX_VALUE:raise Capacity('Profile URL tail capacity')
 except Capacity:raise
 except Exception as error:return None,{'actual_class':type(error).__name__}
 return(result if count else None),None
def _credentials(row):
 base=_normalize(row['server_url']);user=row['username'];password=row['password']
 if not(base and user and password):return None,None
 if not(row['search']and row['replacement']):return(base,user,password),None
 transformed,caught=_substitute(f"{base.rstrip('/')}/live/{user}/{password}/1234.ts",row['search'],row['replacement'])
 if transformed is None:return None,caught
 parsed=urlparse(transformed);parts=[p for p in parsed.path.split('/')if p]
 if len(parts)<4 or parts[-1]!='1234.ts':return None,caught
 prefix=('/'+'/'.join(parts[:-4]))if parts[:-4]else''
 return(f'{parsed.scheme}://{parsed.netloc}{prefix}',parts[-3],parts[-2]),caught
def _build(server,user,password,stream):
 # Literal Source interpolation: no percent decoding, quoting or Url canonicalization.
 return f"{server.rstrip('/')}/live/{user}/{password}/{stream}.ts"
def resolve(row):
 if row['account_type']=='XC'and row['stream_id']:
  credentials,caught=_credentials(row)
  if credentials is None:return None,caught
  return _build(*credentials,str(row['stream_id'])),caught
 return _substitute(row['stored_url']or'',row['search'],row['replacement'])
def _evaluate(rows):
 if not isinstance(rows,list)or not 1<=len(rows)<=100:raise Capacity('Profile URL batch capacity')
 if len(json.dumps(rows,ensure_ascii=False).encode('utf8'))>MAX_PACKET:raise Capacity('Profile URL input capacity')
 expected={'account_type','stored_url','server_url','username','password','stream_id','search','replacement'}
 for row in rows:
  if not isinstance(row,dict)or set(row)!=expected:raise ValueError('Profile URL input schema')
  limits={'account_type':3,'stored_url':4096,'server_url':1000,'username':255,'password':255,'search':255,'replacement':255,'stream_id':4096}
  for key,value in row.items():
   if key=='stream_id':
    if value is not None and(type(value)is not int and not isinstance(value,str)):raise ValueError('Profile stream identity')
   elif value is not None and not isinstance(value,str):raise ValueError('Profile URL field type')
   if isinstance(value,str)and(len(value)>limits[key]or len(value.encode('utf8'))>MAX_VALUE):raise Capacity('Profile URL Source field capacity')
 results=[];total=0
 for row in rows:
  try:url,caught=resolve(row)
  except Capacity:raise
  except Exception as error:
   # Source's uncaught urllib/base-data error is distinct from caught regex/no-match.
   results.append({'url':None,'source_exception':{'actual_class':type(error).__name__},'caught':None});continue
  if url is not None:
   size=len(url.encode('utf8'));total+=size
   if size>MAX_VALUE or total>MAX_PACKET:raise Capacity('Profile URL result capacity')
  results.append({'url':url,'source_exception':None,'caught':caught})
 return {'capacity':False,'rows':results}
def evaluate(rows):
 try:return _evaluate(rows)
 except Capacity:return {'capacity':True,'rows':None}
