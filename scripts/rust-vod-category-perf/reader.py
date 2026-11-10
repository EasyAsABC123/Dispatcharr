"""Bounded exact category JSON reader; no workload starts on import.
Original Dispatcharr authors retain full application credit.
"""
import hashlib,http.client,json,time
from urllib.parse import urlsplit
from protocol import PATHS,SPEC as POLICY,canonical

def validate(data,expected=None):
 if len(data)>POLICY['body_limit_bytes']:raise ValueError('Category body byte limit')
 value=json.loads(data)
 if type(value)is not list or len(value)!=242:raise ValueError('Category fixture count')
 ids=set();tuples=[];links=0;disabled=0
 for row in value:
  if set(row)!={'id','name','category_type','category_type_display','m3u_accounts'}or type(row['id'])is not int or row['id']in ids or type(row['name'])is not str:raise ValueError('Category identity/shape')
  ids.add(row['id']);kind=row['category_type']
  if kind not in ['movie','series']or row['category_type_display']!={'movie':'Movie','series':'Series'}[kind]:raise ValueError('Category type/display')
  tuples.append((kind,row['name']))
  if type(row['m3u_accounts'])is not list:raise ValueError('Nested relation list type')
  for relation in row['m3u_accounts']:
   if set(relation)!={'category','m3u_account','enabled','custom_properties'}or type(relation['category'])is not int or relation['category']!=row['id']or type(relation['m3u_account'])is not int or type(relation['enabled'])is not bool or type(relation['custom_properties'])is not dict:raise ValueError('Nested relation shape/types')
   if len(json.dumps(relation['custom_properties'],ensure_ascii=False,separators=(',',':'),allow_nan=False).encode())>256:raise ValueError('Declared property bound')
   links+=1;disabled+=not relation['enabled']
 if sum(kind=='movie'for kind,_ in tuples)!=121 or sum(name=='Uncategorized'for _,name in tuples)!=2:raise ValueError('Category type/global counts')
 if len(set(tuples))!=242 or tuples!=sorted(tuples)or links!=501 or disabled!=177:raise ValueError('Declared ordering/complete relation metadata')
 result={'kind':'json','value':value,'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest()}
 if expected is not None and canonical(result)!=canonical(expected):raise ValueError('Exact original category bytes/types mismatch')
 return result

def request(base,path,token,timeout=10,expected=None):
 target=urlsplit(base)
 if target.scheme!='http'or target.hostname not in ['127.0.0.1','localhost','::1']or target.username or target.password or path not in PATHS or not token or not 0<timeout<=10:raise ValueError('Owned bounded VOD request required')
 began=time.monotonic();deadline=began+timeout;connection=http.client.HTTPConnection(target.hostname,target.port,timeout=timeout)
 try:
  connection.connect();transport=connection.sock
  def budget():
   remaining=deadline-time.monotonic()
   if remaining<=0:raise TimeoutError('VOD deadline')
   transport.settimeout(remaining)
  budget();connection.request('GET',path,headers={'Host':POLICY['host_header'],'Authorization':'Bearer '+token,'Accept':'application/json','Accept-Encoding':'identity','User-Agent':'Dispatcharr-VOD-Perf/1'})
  budget();response=connection.getresponse()
  if response.status!=200 or response.getheader('Content-Type')!='application/json'or response.getheader('Content-Encoding','identity')!='identity':raise ValueError('VOD status/MIME/encoding mismatch')
  budget();first=response.read(1);first_ms=(time.monotonic()-began)*1000
  if not first:raise ValueError('Empty body')
  data=bytearray(first)
  while not response.isclosed():
   budget();chunk=response.read(65536)
   if not chunk:break
   data.extend(chunk)
   if len(data)>POLICY['body_limit_bytes']:raise ValueError('VOD body exceeds4MiB')
  result=validate(bytes(data),expected)
  return result,first_ms,(time.monotonic()-began)*1000
 finally:connection.close()
