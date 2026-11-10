"""Bounded POST/streamed-XML request adapter; no trial execution or body/token logs."""
import hashlib,http.client,json,time,math
from xml.etree.ElementTree import XMLPullParser
from urllib.parse import urlsplit
UNSET=object()
class ResponseFailure(ValueError):
 def __init__(self,status,mime=None,body_class=None,body_keys=None):
  super().__init__('Guide status/encoding mismatch');self.status=status;self.mime=mime;self.body_class=body_class;self.body_keys=body_keys
def canonical(value):return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
def request(base,spec,token=None,timeout=10,expected=UNSET):
 target=urlsplit(base)
 if target.scheme!='http'or target.hostname not in ['127.0.0.1','localhost','::1']or target.username or target.password or type(timeout)not in [int,float]or not math.isfinite(timeout)or not 0<timeout<=10:raise ValueError('Owned loopback HTTP fixture required')
 if spec['kind']not in ['json','xml']or not spec['path'].startswith('/')or spec['method']not in ['GET','POST']:raise ValueError('Invalid bounded guide request')
 data=canonical(spec['body'])if spec['method']=='POST'else None
 headers={'Host':'guide-perf.invalid:8088','Accept-Encoding':'identity','Accept':'application/json'if spec['kind']=='json'else'application/xml','User-Agent':'Dispatcharr-Guide-Perf/1'}
 if data is not None:headers['Content-Type']='application/json'
 if spec['kind']=='json':
  if not token:raise ValueError('Current programme admin token required')
  headers['Authorization']='Bearer '+token
 parser=XMLPullParser(events=['end'])if spec.get('validate_xml')else None;counts={'channels':0,'programmes':0}
 def parse(chunk):
  if parser is not None:
   parser.feed(chunk)
   for _,element in parser.read_events():
    if element.tag=='channel':counts['channels']+=1
    elif element.tag=='programme':counts['programmes']+=1
    element.clear()
 began=time.monotonic();deadline=began+timeout;connection=http.client.HTTPConnection(target.hostname,target.port,timeout=timeout)
 try:
  connection.connect();transport=connection.sock
  def budget():
   remaining=deadline-time.monotonic()
   if remaining<=0:raise TimeoutError('Guide request deadline')
   transport.settimeout(remaining)
  budget();connection.request(spec['method'],spec['path'],body=data,headers=headers);budget();response=connection.getresponse()
  if response.status!=200 or response.getheader('Content-Encoding','identity')!='identity':
   budget();error_data=response.read(4097);body_class='over4096'if len(error_data)>4096 else'bytes';body_keys=None
   if len(error_data)<=4096:
    try:
     error_value=json.loads(error_data);body_class=type(error_value).__name__
     if type(error_value)is dict and len(error_value)<=32:body_keys=sorted(str(key)[:64]for key in error_value)
    except(ValueError,UnicodeError):pass
   raise ResponseFailure(response.status,response.getheader('Content-Type','').split(';')[0],body_class,body_keys)
  mime=response.getheader('Content-Type','').split(';')[0]
  if mime!=('application/json'if spec['kind']=='json'else'application/xml'):raise ValueError('Guide MIME mismatch')
  budget();first=response.read(1);first_ms=(time.monotonic()-began)*1000
  if not first:raise ValueError('Empty guide response')
  parse(first)
  limit=4<<20 if spec['kind']=='json'else 32<<20;size=1;digest=hashlib.sha256(first);payload=bytearray(first)if spec['kind']=='json'else None
  while not response.isclosed():
   budget();chunk=response.read(65536)
   if not chunk:break
   size+=len(chunk)
   if size>limit:raise ValueError('Guide response exceeds declared byte ceiling')
   digest.update(chunk);parse(chunk)
   if payload is not None:payload.extend(chunk)
  result={'kind':'json','value':json.loads(payload)}if payload is not None else{'kind':'xml','bytes':size,'sha256':digest.hexdigest()}
  if parser is not None:parser.close();result['xml_counts']=counts
  if expected is not UNSET and canonical(result)!=canonical(expected):raise ValueError('Guide body differs from original fixture')
  return result,first_ms,(time.monotonic()-began)*1000
 finally:connection.close()
