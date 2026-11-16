"""Owned HTTPS CONNECT provider Origin inside the existing fixture namespace.
Original Dispatcharr/Requests authors retain application/protocol credit.
Private CA/leaf keys remain ONLY in this Source-side process's private tmpdir.
"""
import zlib,base64,datetime,hashlib,http.server,json,os,re,signal,socket,socketserver,ssl,subprocess,sys,tempfile,threading,time,urllib.parse
from pathlib import Path

HOSTS=('json.schedulesdirect.org',)
PORT=18097
LIMIT=1048576
class State:
 def __init__(self,config):
  self.config=config;self.lock=threading.Lock();self.case='base';self.trace=[];self.active=0;self.stopping=False;self.token_serial=0;self.expired=False;self.dates=[]
 def record(self,value):
  with self.lock:
   if len(self.trace)>=512:raise RuntimeError('OwnedProviderTraceCount')
   if len(json.dumps(self.trace))+len(json.dumps(value))>LIMIT:raise RuntimeError('OwnedProviderTraceBytes')
   self.trace.append(value)
 def snapshot(self):
  with self.lock:return {'case':self.case,'trace':list(self.trace),'active':self.active,'private_keys_exported':False}

def certificate(root,ca_target):
 root.chmod(0o700)
 config=root/'leaf.cnf';config.write_text('[req]\nprompt=no\ndistinguished_name=dn\n[dn]\nCN=Dispatcharr owned provider\n[ext]\nsubjectAltName='+','.join('DNS:'+host for host in HOSTS)+'\nextendedKeyUsage=serverAuth\nkeyUsage=critical,digitalSignature,keyEncipherment\nbasicConstraints=critical,CA:FALSE\nsubjectKeyIdentifier=hash\nauthorityKeyIdentifier=keyid:always,issuer\n')
 commands=[['openssl','req','-x509','-newkey','rsa:2048','-nodes','-days','2','-subj','/CN=Dispatcharr owned provider CA','-addext','basicConstraints=critical,CA:TRUE','-addext','keyUsage=critical,keyCertSign,cRLSign','-addext','subjectKeyIdentifier=hash','-addext','authorityKeyIdentifier=keyid:always','-keyout',str(root/'ca.key'),'-out',str(root/'ca.pem')],['openssl','req','-new','-newkey','rsa:2048','-nodes','-config',str(config),'-keyout',str(root/'leaf.key'),'-out',str(root/'leaf.csr')],['openssl','x509','-req','-in',str(root/'leaf.csr'),'-CA',str(root/'ca.pem'),'-CAkey',str(root/'ca.key'),'-CAcreateserial','-days','2','-extfile',str(config),'-extensions','ext','-out',str(root/'leaf.pem')]]
 for command in commands:
  result=subprocess.run(command,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=10)
  if result.returncode or len(result.stdout)+len(result.stderr)>65536:raise RuntimeError('OwnedProviderCertificateSetup')
 for key in ('ca.key','leaf.key'):(root/key).chmod(0o600)
 ca_target.write_bytes((root/'ca.pem').read_bytes());ca_target.chmod(0o444)
 context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);context.minimum_version=ssl.TLSVersion.TLSv1_2;context.load_cert_chain(root/'leaf.pem',root/'leaf.key')
 return context

def digest(value):return base64.b64encode(hashlib.md5(value.encode()).digest()).decode().rstrip('=')
def response(state,host,path,method,headers,payload):
 parsed=urllib.parse.urlsplit(path);case=state.case
 if host!='json.schedulesdirect.org':return 502,b'{}',{}
 if parsed.path=='/__owned_tls_probe'and method=='GET':return 200,b'owned-source-TLS-trust-proof',{'Content-Type':'text/plain'}
 if method=='POST'and parsed.path=='/20141201/token':
  if not isinstance(payload,dict)or list(payload)!=['username','password']:return 400,b'{"code":4003}',{}
  username=payload.get('username');password=payload.get('password')
  if not isinstance(username,str)or not isinstance(password,str)or not re.fullmatch('[0-9a-f]{40}',password):return 400,b'{"code":4002}',{}
  if case=='debug2055'and headers.get('RouteTo')=='debug':return 400,b'{"code":2055}',{}
  if case=='busy':return 200,b'{"code":3001,"message":"owned busy"}',{}
  if hashlib.sha256(username.encode()).hexdigest()!=state.config['username_sha256']or hashlib.sha256(password.encode()).hexdigest()!=state.config['password_sha1_sha256']:
   return 200,b'{"code":4003,"message":"owned incorrect credentials"}',{}
  with state.lock:state.token_serial+=1;serial=state.token_serial
  return 200,json.dumps({'code':0,'token':'owned-sd-token-'+str(serial),'tokenExpires':time.time()+3600}).encode(),{}
 if not headers.get('token','').startswith('owned-sd-token-'):return 403,b'{"code":4006}',{}
 if case=='expired'and not state.expired:
  with state.lock:state.expired=True
  return 403,b'{"code":4006}',{}
 if parsed.path=='/20141201/status'and method=='GET':value={'systemStatus':[{'status':'Online'}]}
 elif parsed.path=='/20141201/lineups'and method=='GET':value={'lineups':[{'lineupID':'GBR-Owned-001'}]}
 elif parsed.path=='/20141201/schedules/md5'and method=='POST':
  if not isinstance(payload,list)or not payload:return 400,b'{}',{}
  value={}
  for row in payload:
   if set(row)!={'stationID','date'}or len(row['date'])!=20 or len(set(row['date']))!=20:return 400,b'{}',{}
   days=[datetime.date.fromisoformat(d)for d in row['date']]
   if any((b-a).days!=1 for a,b in zip(days,days[1:])):return 400,b'{}',{}
   with state.lock:
    if state.dates and state.dates!=row['date']:raise RuntimeError('ActualSDDateWindowChanged')
    state.dates=row['date']
   sid=row['stationID'];epoch=case if case in('changed','metadata-error','connect')else'base';value[sid]={row['date'][0]:{'md5':digest(epoch+sid),'lastModified':row['date'][0]+'T01:00:00Z','code':0}}
 elif parsed.path=='/20141201/schedules'and method=='POST':
  if not isinstance(payload,list):return 400,b'{}',{}
  value=[]
  for row in payload:
   sid=row['stationID'];day=row['date'][0];pid='EP12345678'+sid[-4:];epoch=case if case in('changed','metadata-error','connect')else'base'
   value.append({'stationID':sid,'programs':[{'programID':pid,'airDateTime':day+'T12:00:00Z','duration':3600,'md5':digest(('metadata-error'if case=='metadata-error'else'programme')+pid),'new':case=='changed'}],'metadata':{'startDate':day,'md5':digest(epoch+sid),'modified':day+'T01:00:00Z'}})
 elif parsed.path=='/20141201/programs'and method=='POST':
  if case=='metadata-error':return 503,b'{"code":3001,"message":"owned retry"}',{}
  if not isinstance(payload,list):return 400,b'{}',{}
  value=[{'programID':pid,'titles':[{'title120':'Owned SD '+pid}],'episodeTitle150':'Owned episode','descriptions':{'description1000':[{'description':'Owned full description'}]},'metadata':[{'Other':{}},{'Gracenote':{'season':2,'episode':3}}],'contentRating':[{'country':'USA','code':'TV-PG','body':'US'},{'country':'GBR','code':'12','body':'BBFC'}],'cast':[{'name':'Owned guest','role':'Guest Star','characterName':'Owned character','billingOrder':'1'}],'originalAirDate':'2020-01-02','duration':1800,'genres':['Owned'],'entityType':'Episode'}for pid in payload]
 elif parsed.path=='/20141201/metadata/programs/'and method=='POST':
  if not isinstance(payload,list):return 400,b'{}',{}
  value=[{'programID':pid,'data':[{'uri':'owned-portrait','category':'Iconic','aspect':'2x3','width':'240','primary':'yes'},{'uri':'owned-landscape','category':'Banner','aspect':'16x9','width':400,'primary':False}]}for pid in payload]
 else:return 404,b'{}',{}
 return 200,json.dumps(value,separators=(',',':')).encode(),{}

class Inner(http.server.BaseHTTPRequestHandler):
 protocol_version='HTTP/1.1'
 def log_message(self,*unused):pass
 def do_HEAD(self):self.respond()
 def do_GET(self):self.respond()
 def do_POST(self):self.respond()
 def respond(self):
  if len(self.path)>65536:raise RuntimeError('OwnedProviderPathBudget')
  headers_bytes=sum(len(k)+len(v)for k,v in self.headers.items())
  if headers_bytes>65536:raise RuntimeError('OwnedProviderHeaderBudget')
  state=self.server.state;host=self.server.target;parsed=urllib.parse.urlsplit(self.path);query=urllib.parse.parse_qs(parsed.query,keep_blank_values=True)
  size=int(self.headers.get('Content-Length','0'))
  if size<0 or size>8*1024*1024:raise RuntimeError('OwnedSDRequestBodyBudget')
  raw=self.rfile.read(size)
  if len(raw)!=size:raise RuntimeError('OwnedSDIncompleteBody')
  payload=json.loads(raw)if raw else None
  status,body,extra=response(state,host,self.path,self.command,self.headers,payload)
  # Never retain literal API keys/query values or arbitrary request headers.
  state.record({'epoch':time.time(),'case':state.case,'host':host,'method':self.command,'path':parsed.path,'query_sha256':{k:[hashlib.sha256(v.encode()).hexdigest()for v in values]for k,values in query.items()},'range':self.headers.get('Range'),'status':status,'response_bytes':len(body),'response_sha256':hashlib.sha256(body).hexdigest(),'actual_TLS':True,'request_body_sha256':hashlib.sha256(raw).hexdigest(),'request_json_shape':list(payload)if isinstance(payload,dict)else 'list'if isinstance(payload,list)else None,'actual_token_sha256':hashlib.sha256(self.headers.get('token','').encode()).hexdigest(),'debug_header':self.headers.get('RouteTo')=='debug','user_agent':self.headers.get('User-Agent')})
  self.send_response(status);self.send_header('Content-Length',str(len(body)));self.send_header('Content-Type',extra.pop('Content-Type','application/json'))
  for k,v in extra.items():self.send_header(k,v)
  self.send_header('Connection','close');self.end_headers()
  if self.command!='HEAD':self.wfile.write(body)
  self.wfile.flush();self.close_connection=True

class Proxy(http.server.BaseHTTPRequestHandler):
 protocol_version='HTTP/1.1'
 def log_message(self,*unused):pass
 def do_CONNECT(self):
  target=urllib.parse.urlsplit('//'+self.path)
  if target.hostname not in HOSTS or target.port!=443 or target.username or target.password: self.send_error(502);return
  self.send_response(200,'Connection Established');self.end_headers();self.wfile.flush()
  self.connection.settimeout(8)
  context=self.server.context
  with context.wrap_socket(self.connection,server_side=True)as encrypted:
   # The true Source/Native client authenticates the requested hostname against
   # the leaf SAN/owned CA. No upstream sockets or DNS are opened by this proxy.
   endpoint=type('BoundOrigin',(),{'state':self.server.state,'target':target.hostname})()
   Inner(encrypted,self.client_address,endpoint)
  self.close_connection=True
 def xmltv(self):
  parsed=urllib.parse.urlsplit(self.path);leaf=parsed.path.rsplit('/',1)[-1]
  if leaf=='selected.xml':
   base=datetime.datetime.now(datetime.timezone.utc).replace(hour=0,minute=0,second=0,microsecond=0)+datetime.timedelta(days=1)
   prefix=b'<?xml version="1.0"?><tv><channel id="owned-http-selected"><display-name>Owned selected guide</display-name></channel>\n';suffix=b'</tv>'
   def row(index):
    start=(base+datetime.timedelta(minutes=index)).strftime('%Y%m%d%H%M%S +0000');end=(base+datetime.timedelta(minutes=index+1)).strftime('%Y%m%d%H%M%S +0000')
    return ('<programme channel="owned-http-selected" start="'+start+'" stop="'+end+'"><title>Selected '+str(index).zfill(5)+'</title><desc>'+('d'*1024)+'</desc></programme>\n').encode()
   invalid=b'<programme channel="owned-http-selected" start="20300101100000 +0000"><title>Skipped invalid sibling</title></programme>\n'
   decoded=len(prefix)+len(suffix)+len(invalid)+sum(len(row(i))for i in range(6001));self.send_response(200);self.send_header('Content-Type','text/xml');self.send_header('Content-Length',str(decoded));self.send_header('Connection','close');self.end_headers();sent=0;sha=hashlib.sha256();disconnected=False
   def write(raw):
    nonlocal sent
    self.wfile.write(raw);sent+=len(raw);sha.update(raw)
   try:
    write(prefix)
    for i in range(6001):write(row(i))
    write(invalid);write(suffix);self.wfile.flush()
   except(BrokenPipeError,ConnectionResetError):disconnected=True
   self.close_connection=True;self.server.state.record({'kind':'owned_XMLTV_selected_request','path':parsed.path,'status':200,'selected_valid_programmes':6001,'invalid_sibling_programmes':1,'wire_bytes':sent,'decoded_fixture_bytes':decoded,'wire_sha256':sha.hexdigest(),'consumer_disconnected':disconnected});return
  if leaf=='redirect.xml':
   self.send_response(302);self.send_header('Location','/xmltv/guide.xml');self.send_header('Content-Length','0');self.send_header('Connection','close');self.end_headers();self.close_connection=True;self.server.state.record({'kind':'owned_XMLTV_request','path':parsed.path,'status':302,'user_agent':self.headers.get('User-Agent'),'wire_bytes':0});return
  if leaf=='missing.xml':
   body=b'owned XMLTV not found';self.send_response(404);self.send_header('Content-Length',str(len(body)));self.send_header('Connection','close');self.end_headers();self.wfile.write(body);self.close_connection=True;self.server.state.record({'kind':'owned_XMLTV_request','path':parsed.path,'status':404,'user_agent':self.headers.get('User-Agent'),'wire_bytes':len(body)});return
  if leaf not in('guide.xml','guide.xml.gz','encoded.xml','incomplete.xml'):
   self.send_error(404);return
  day=self.server.state.config.get('xml_day')or(datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(days=1)).strftime('%Y%m%d')
  prefix=b'<?xml version="1.0"?><tv>\n<channel id="owned-http-guide"><display-name>Owned HTTP guide</display-name></channel>\n'
  padding=b'<programme channel="unmapped-padding" start="20300101100000 +0000" stop="20300101103000 +0000"><title>Not selected</title></programme>\n'
  target=('<programme channel="owned-http-guide" start="'+day+'100000 +0000" stop="'+day+'103000 +0000"><title>Owned HTTP programme</title></programme>\n').encode()
  suffix=b''if leaf=='incomplete.xml'else b'</tv>'
  count=50000;decoded=len(prefix)+len(padding)*count+len(target)+len(suffix);compressed=leaf in('guide.xml.gz','encoded.xml')
  self.send_response(200);self.send_header('Content-Type','application/gzip'if leaf=='guide.xml.gz'else'text/xml')
  if leaf=='encoded.xml':self.send_header('Content-Encoding','gzip')
  if compressed:self.send_header('Transfer-Encoding','chunked')
  else:self.send_header('Content-Length',str(decoded))
  self.send_header('Connection','close');self.end_headers();sent=0;hasher=hashlib.sha256();encoder=zlib.compressobj(wbits=31)if compressed else None;disconnected=False
  def write(raw):
   nonlocal sent
   if not raw:return
   if compressed:self.wfile.write(('%x\r\n'%len(raw)).encode()+raw+b'\r\n')
   else:self.wfile.write(raw)
   sent+=len(raw);hasher.update(raw)
  try:
   for raw in [prefix]:write(encoder.compress(raw)if encoder else raw)
   for _ in range(count):write(encoder.compress(padding)if encoder else padding)
   for raw in [target,suffix]:write(encoder.compress(raw)if encoder else raw)
   if encoder:write(encoder.flush());self.wfile.write(b'0\r\n\r\n')
   self.wfile.flush()
  except(BrokenPipeError,ConnectionResetError):disconnected=True
  self.close_connection=True;self.server.state.record({'kind':'owned_XMLTV_request','path':parsed.path,'status':200,'user_agent':self.headers.get('User-Agent'),'wire_bytes':sent,'decoded_fixture_bytes':decoded,'wire_sha256':hasher.hexdigest(),'consumer_disconnected':disconnected,'compression':'archive-gzip'if leaf=='guide.xml.gz'else'HTTP-gzip'if leaf=='encoded.xml'else'none'})
 def do_GET(self):
  if self.path.startswith('/xmltv/'):
   self.xmltv();return
  if self.path=='/owned.m3u':
   body=('#EXTM3U\n'+''.join('#EXTINF:-1 tvg-id="'+str(10000+i)+'" group-title="Owned SD",Owned SD '+str(i)+'\nhttp://unrequested-sd.invalid/'+str(i)+'.ts\n'for i in range(1,4))).encode()
   self.send_response(200);self.send_header('Content-Length',str(len(body)));self.send_header('Connection','close');self.end_headers();self.wfile.write(body);self.close_connection=True;return
  if self.path!='/snapshot':self.send_error(404);return
  self.output(self.server.state.snapshot())
 def do_POST(self):
  if self.path.startswith('/connect/'):
   size=int(self.headers.get('Content-Length','0'))
   if size>262144:self.send_error(413);return
   body=self.rfile.read(size)
   if len(body)!=size:self.send_error(400);return
   status=503 if self.path=='/connect/rejected'else 200
   self.server.state.record({'kind':'owned_connect_request','method':'POST','path':self.path,'status':status,'content_type':self.headers.get('Content-Type'),'user_agent':self.headers.get('User-Agent'),'body_base64':base64.b64encode(body).decode(),'body_bytes':len(body),'body_sha256':hashlib.sha256(body).hexdigest(),'headers_names':list(self.headers.keys()),'credential_values_captured':False})
   reply=b'owned rejection'if status==503 else b'owned delivery'
   self.send_response(status);self.send_header('Content-Type','text/plain; charset=utf-8');self.send_header('Content-Length',str(len(reply)));self.send_header('Connection','close');self.end_headers();self.wfile.write(reply);self.close_connection=True;return
  if self.path!='/control':self.send_error(404);return
  size=int(self.headers.get('Content-Length','0'))
  if size>4096:self.send_error(413);return
  value=json.loads(self.rfile.read(size));case=value.get('case')
  if case not in ('base','changed','metadata-error','debug2055','expired','busy','connect'):self.send_error(400);return
  with self.server.state.lock:self.server.state.case=case;self.server.state.expired=False
  self.output({'actual_case':case})
 def output(self,value):
  body=json.dumps(value,separators=(',',':')).encode()
  if len(body)>LIMIT:raise RuntimeError('OwnedProviderControlBudget')
  self.send_response(200);self.send_header('Content-Length',str(len(body)));self.send_header('Connection','close');self.end_headers();self.wfile.write(body);self.close_connection=True

class Server(socketserver.ThreadingMixIn,http.server.HTTPServer):
 daemon_threads=False;block_on_close=True
 def __init__(self,address,state,context):
  self.slots=threading.BoundedSemaphore(4);self.state=state;self.context=context;super().__init__(address,Proxy)
 def process_request(self,request,address):
  self.slots.acquire()
  with self.state.lock:self.state.active+=1
  try:super().process_request(request,address)
  except BaseException:
   with self.state.lock:self.state.active-=1
   self.slots.release();raise
 def process_request_thread(self,request,address):
  try:super().process_request_thread(request,address)
  finally:
   with self.state.lock:self.state.active-=1
   self.slots.release()
 def handle_error(self,request,address):
  # Protocol failures still produce a retained, fixed diagnostic marker without
  # printing request URLs/API keys/certificate material in Source role logs.
  error=sys.exc_info()[1];value={'epoch':time.time(),'case':self.state.case,'kind':'actual_transport_error','exception_type':type(error).__name__}
  if isinstance(error,ssl.SSLError):
   reason=getattr(error,'reason',None);code=getattr(error,'verify_code',None)
   if isinstance(reason,str)and re.fullmatch('[A-Z0-9_]{1,96}',reason):value['actual_SSL_reason']=reason
   if isinstance(code,int):value['actual_verify_code']=code
  self.state.record(value)

def main():
 config=json.loads(Path(os.environ['OWNED_SD_CONFIG']).read_text());state=State(config)
 private=Path(tempfile.mkdtemp(prefix='dispatcharr-sd-keys-'));context=certificate(private,Path(config['ca_path']))
 context.set_servername_callback(lambda _socket,name,_context: state.record({'epoch':time.time(),'kind':'actual_TLS_SNI','host':name}) if name in HOSTS else (_ for _ in ()).throw(ssl.SSLError('unowned SNI')))
 server=Server(('127.0.0.1',PORT),state,context);server.timeout=.2;running=[True]
 signal.signal(signal.SIGTERM,lambda *_:running.__setitem__(0,False))
 Path(config['ready']).write_text(json.dumps({'actual_listener':server.server_address,'actual_CA_sha256':hashlib.sha256(Path(config['ca_path']).read_bytes()).hexdigest(),'actual_hosts':HOSTS,'private_keys_exported':False}));Path(config['ready']).chmod(0o444)
 try:
  while running[0]:server.handle_request()
 finally:
  server.server_close();Path(config['final']).write_text(json.dumps(state.snapshot()));Path(config['final']).chmod(0o444)
  for path in private.iterdir():path.unlink()
  private.rmdir()
if __name__=='__main__':main()
