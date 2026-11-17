"""Owned HTTPS CONNECT provider Origin inside the existing fixture namespace.
Original Dispatcharr/Requests authors retain application/protocol credit.
Private CA/leaf keys remain ONLY in this Source-side process's private tmpdir.
"""
import hashlib,http.server,json,os,re,signal,socket,socketserver,ssl,subprocess,sys,tempfile,threading,time,urllib.parse
from pathlib import Path

HOSTS=('api.themoviedb.org','www.omdbapi.com','api.tvmaze.com','itunes.apple.com','image.tmdb.org','artwork-owned.invalid','xc-vod-owned.invalid')
PORT=18095
LIMIT=1048576
class State:
 def __init__(self,config):
  self.config=config;self.lock=threading.Lock();self.case='empty';self.trace=[];self.active=0;self.stopping=False
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

def response(state,host,path,method,headers):
 parsed=urllib.parse.urlsplit(path);query=urllib.parse.parse_qs(parsed.query,keep_blank_values=True)
 case=state.case;status=200;value={};body=None;extra={}
 if host in('artwork-owned.invalid','xc-vod-owned.invalid')and parsed.path=='/__owned_tls_probe'and not query:return 200,b'owned-source-TLS-trust-proof',{'Content-Type':'text/plain'}
 if host=='xc-vod-owned.invalid':
  if parsed.path!='/xc/player_api.php' or method!='GET':return 404,b'{}',{}
  if any(query.get(name)!=[state.config['vod_login'][name]]for name in('username','password')):return 401,b'{"error":"owned credential mismatch"}',{}
  action=query.get('action',[None])[0]
  responses={None:{'user_info':{'auth':1,'status':'Active'},'server_info':{}},'get_vod_categories':[{'category_id':'4','category_name':'Owned Films'}],'get_series_categories':[],'get_vod_streams':[{'num':1,'stream_id':9101,'name':'Owned Movie (2020)','category_id':'4','container_extension':'mp4','is_adult':'0'},{'num':2,'stream_id':9102,'name':'Owned Second Movie (2021)','category_id':'4','container_extension':'mkv','is_adult':'0'}],'get_series':[]}
  if case in ('vod-repeat-update','vod-repeat-prune','vod-repeat-empty-movies','vod-repeat-empty-categories'):
   responses['get_vod_categories']=[{'category_id':'8','category_name':'Updated Films'}]
   responses['get_vod_streams']=[{'num':1,'stream_id':9101,'name':'Owned Movie (2020)','category_id':'8','container_extension':'mkv'}]
   if case=='vod-repeat-update':responses['get_vod_streams'] += [{'num':2,'stream_id':9102,'name':'Owned Replacement (2022)','category_id':'8','container_extension':'mp4','is_adult':'0'},{'num':3,'stream_id':9103,'name':'Owned Movie (2020)','category_id':'8','container_extension':'mp4','is_adult':'1'}]
   if case=='vod-repeat-empty-movies':responses['get_vod_streams']=[]
   if case=='vod-repeat-empty-categories':responses['get_vod_categories']=[]
  if case in ('vod-series-initial','vod-series-update','vod-series-empty'):
   group='15'if case!='vod-series-initial'else'11';label='Updated Series'if case!='vod-series-initial'else'Owned Series'
   responses['get_vod_categories']=[];responses['get_vod_streams']=[]
   responses['get_series_categories']=[{'category_id':group,'category_name':label}]
   responses['get_series']=[{'num':1,'series_id':9201,'name':'Owned Series (2020)','category_id':group},{'num':2,'series_id':9202,'name':'Owned Second Series','category_id':group}]
   if case=='vod-series-update':responses['get_series']=[responses['get_series'][0],{'num':2,'series_id':9202,'name':'Owned Replacement Series','category_id':group},{'num':3,'series_id':9203,'name':'Owned Series (2020)','category_id':group}]
   if case=='vod-series-empty':responses['get_series']=[]
  if case in state.config.get('vod_series_info_cases',{}):
   responses={None:{'user_info':{'auth':1,'status':'Active'}},'get_series_info':state.config['vod_series_info_cases'][case]}
   if action=='get_series_info'and query.get('series_id')!=['3000']:return 400,b'{}',{}
  if case in state.config.get('vod_series_advanced_cases',{}):
   responses['get_vod_categories']=[];responses['get_vod_streams']=[];responses['get_series_categories']=[{'category_id':'11','category_name':'rust-probe-vod-rest-Drama'}];responses['get_series']=state.config['vod_series_advanced_cases'][case]
  if case in state.config.get('vod_dense_list_cases',{}):
   responses.update(state.config['vod_dense_list_cases'][case])
  if action not in responses:return 400,b'{"error":"unmeasured VOD action"}',{}
  return 200,json.dumps(responses[action],separators=(',',':')).encode(),{}
 if host in ('api.themoviedb.org','www.omdbapi.com'):
  name='api_key'if host=='api.themoviedb.org'else'apikey';actual=(query.get(name)or[''])[0]
  if hashlib.sha256(actual.encode()).hexdigest()!=state.config['key_sha256']:
   return 401,b'{"error":"owned credential mismatch"}',{}
 if host=='api.themoviedb.org':
  if parsed.path.startswith('/3/find/'):
   value={'movie_results':[{'poster_path':'/owned-find-first.jpg'}],'tv_results':[{'poster_path':'/owned-find-later.jpg'}]}if case=='tmdb-find'else{'movie_results':[],'tv_results':[],'tv_episode_results':[],'tv_season_results':[]}
  elif parsed.path=='/3/search/multi':
   value={'results':[{'popularity':9,'poster_path':'/owned-search-first.jpg'},{'popularity':9,'poster_path':'/owned-search-tie.jpg'},{'popularity':1,'poster_path':'/owned-search-low.jpg'}]}if case=='tmdb-search'else{'results':[]}
   if case=='caught-errors':status=503
  else:status=404
 elif host=='www.omdbapi.com':
  value={'Poster':'https://artwork-owned.invalid/omdb.jpg'}if case=='omdb'else{'Poster':'N/A'}
  if case=='caught-errors':body=b'{actual malformed owned provider JSON'
 elif host=='api.tvmaze.com':
  value={'image':{'original':'https://artwork-owned.invalid/tvmaze-original.jpg','medium':'https://artwork-owned.invalid/tvmaze-medium.jpg'}}if case in ('tvmaze','logo-reuse')else{}
  if case=='logo-reuse':value={'image':{'original':'https://artwork-owned.invalid/reused.jpg'}}
  if case=='caught-errors':status=500
 elif host=='itunes.apple.com':
  value={'results':[{'artworkUrl100':'https://artwork-owned.invalid/100x100bb.jpg'}]}if case=='itunes'and(query.get('media')or[''])[0]=='tvShow'else{'results':[]}
 elif host in ('image.tmdb.org','artwork-owned.invalid'):
  if parsed.path=='/range-only.jpg'and method=='HEAD':status=405;body=b''
  else:
   # URL validation uses HTTP status, not an invented image-decoder result.
   body=b'owned poster payload';extra={'Content-Type':'image/jpeg'}
   if parsed.path=='/range-only.jpg':
    if headers.get('Range')!='bytes=0-0':return 416,b'',{}
    body=b'x';status=206;extra['Content-Range']='bytes 0-0/20'
 else:status=502
 if body is None:body=json.dumps(value,separators=(',',':')).encode()
 return status,body,extra

class Inner(http.server.BaseHTTPRequestHandler):
 protocol_version='HTTP/1.1'
 def log_message(self,*unused):pass
 def do_HEAD(self):self.respond()
 def do_GET(self):self.respond()
 def respond(self):
  if len(self.path)>65536:raise RuntimeError('OwnedProviderPathBudget')
  headers_bytes=sum(len(k)+len(v)for k,v in self.headers.items())
  if headers_bytes>65536:raise RuntimeError('OwnedProviderHeaderBudget')
  state=self.server.state;host=self.server.target;parsed=urllib.parse.urlsplit(self.path);query=urllib.parse.parse_qs(parsed.query,keep_blank_values=True)
  status,body,extra=response(state,host,self.path,self.command,self.headers)
  # Never retain literal API keys/query values or arbitrary request headers.
  state.record({'epoch':time.time(),'case':state.case,'host':host,'method':self.command,'path':parsed.path,'query_sha256':{k:[hashlib.sha256(v.encode()).hexdigest()for v in values]for k,values in query.items()},'range':self.headers.get('Range'),'actual_user_agent':self.headers.get('User-Agent'),'actual_request_headers':{key:self.headers.get(key)for key in ['User-Agent','Accept','Accept-Encoding','Connection']},'status':status,'response_bytes':len(body),'response_sha256':hashlib.sha256(body).hexdigest(),'actual_TLS':True})
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
 def do_GET(self):
  if self.path!='/snapshot':self.send_error(404);return
  self.output(self.server.state.snapshot())
 def do_POST(self):
  if self.path!='/control':self.send_error(404);return
  size=int(self.headers.get('Content-Length','0'))
  if size>4096:self.send_error(413);return
  value=json.loads(self.rfile.read(size));case=value.get('case')
  if case not in ('empty','epg','vod','tmdb-find','tmdb-search','omdb','tvmaze','itunes','direct','existing-logo','channel-logo','caught-errors','logo-reuse','vod-repeat-update','vod-repeat-prune','vod-repeat-empty-movies','vod-repeat-empty-categories','vod-series-initial','vod-series-update','vod-series-empty',*self.server.state.config.get('vod_series_info_cases',{}),*self.server.state.config.get('vod_series_advanced_cases',{}),*self.server.state.config.get('vod_dense_list_cases',{})):self.send_error(400);return
  with self.server.state.lock:self.server.state.case=case
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
 config=json.loads(Path(os.environ['OWNED_PROVIDER_CONFIG']).read_text());state=State(config)
 private=Path(tempfile.mkdtemp(prefix='dispatcharr-provider-keys-'));context=certificate(private,Path(config['ca_path']))
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
