"""Bounded exact ordinary JSON reader; no workload starts on import.
Original Dispatcharr authors retain full application credit.
"""
import hashlib,http.client,json,time
from urllib.parse import urlsplit
from protocol import PATHS,POLICY,canonical

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
  value=json.loads(data)
  if type(value)is not dict or set(value)!= {'count','next','previous','results'}or value['count']!=4500 or len(value['results'])!=24:raise ValueError('Declared fixture/page mismatch')
  result={'kind':'json','value':value,'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest()}
  if expected is not None and canonical(result)!=canonical(expected):raise ValueError('Exact original VOD body mismatch')
  return result,first_ms,(time.monotonic()-began)*1000
 finally:connection.close()
