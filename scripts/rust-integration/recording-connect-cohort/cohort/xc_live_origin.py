"""Owned actual XC provider Origin for the existing milestone Compose namespace.
No upstream sockets or DNS. Original Dispatcharr authors retain application credit.
"""
import hashlib,http.server,json,os,pathlib,signal,threading,time,uuid,urllib.parse
ROOT=pathlib.Path(os.environ['XC_LIVE_DATA']);USER=os.environ['XC_LIVE_USERNAME'];PASSWORD=os.environ['XC_LIVE_PASSWORD'];PORT=18096
LOCK=threading.Lock();TRACE=[];PHASE='initial'
def rows():
    data=[{'name':'XC stream '+str(i),'stream_id':i,'category_id':'7','num':i+.25,'stream_icon':None if i==1 else '', 'epg_channel_id':None if i==1 else 'xc-'+str(i),'is_adult':1 if i==2 else 0,'tv_archive':1 if i==3 else 0,'tv_archive_duration':2 if i==3 else 0,'stream_type':'radio_streams'if i==4 else 'live'}for i in range(1,1003)]
    data[0]['name']=''
    data[1001]['name']='界'*513
    data.append({'name':'Disabled group stream','stream_id':3000,'category_id':'8'})
    if PHASE=='uncatchup':
        for row in data:row['tv_archive']=0;row['tv_archive_duration']=0
    if PHASE=='updated':data=[r for r in data if r['stream_id']not in(2,3000)];data[0]['name']='Renamed first';data[0]['stream_icon']='https://owned.invalid/first.png'
    return data
class Server(http.server.ThreadingHTTPServer):
    daemon_threads=False
    def __init__(self,*a,**k):super().__init__(*a,**k);self.slots=threading.BoundedSemaphore(4)
    def process_request(self,r,address):
        self.slots.acquire()
        try:super().process_request(r,address)
        except BaseException:self.slots.release();raise
    def process_request_thread(self,*a):
        try:super().process_request_thread(*a)
        finally:self.slots.release()
class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version='HTTP/1.1'
    def setup(self):
        super().setup();self.connection.settimeout(5)
    def log_message(self,*_):pass
    def reply(self,status,data,cookie=None):
        raw=json.dumps(data,allow_nan=False).encode();self.send_response(status);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(raw)))
        if cookie:self.send_header('Set-Cookie','xc_owned='+cookie+'; Path=/')
        self.end_headers();self.wfile.write(raw)
    def do_POST(self):
        global PHASE,USER,PASSWORD
        if self.path!='/control':return self.reply(404,{})
        length=int(self.headers.get('Content-Length','0'))
        if length>4096:return self.reply(413,{})
        value=json.loads(self.rfile.read(length))
        if 'rotate_login' in value:
            login=value['rotate_login']
            if not isinstance(login,list)or len(login)!=2 or not all(isinstance(v,str)and 0<len(v)<=128 for v in login):return self.reply(400,{})
            with LOCK:USER,PASSWORD=login
            return self.reply(200,{'actual_login_rotated':True})
        phase=value.get('phase')
        if phase not in('initial','updated','uncatchup','empty','auth-error'):return self.reply(400,{})
        with LOCK:PHASE=phase
        self.reply(200,{'phase':phase})
    def do_GET(self):
        split=urllib.parse.urlsplit(self.path)
        if split.path=='/snapshot':
            with LOCK:value={'phase':PHASE,'trace':list(TRACE)}
            return self.reply(200,value)
        if split.path.startswith('/xc/live/'):
            parts=split.path.split('/')
            # /xc/live/user/password/1.ts has six split components including leading empty.
            valid=len(parts)==6 and parts[3]==USER and parts[4]==PASSWORD and parts[5]=='1.ts'
            with LOCK:TRACE.append({'method':'GET','path_kind':'live','stream_id':'1','actual_auth_matches':valid,'actual_query_keys':sorted(urllib.parse.parse_qs(split.query)),'time_ns':time.time_ns()})
            if not valid:return self.reply(401,{'error':'Owned live authentication rejected'})
            target=os.environ['XC_LIVE_MEDIA_TARGET']
            self.send_response(302);self.send_header('Location',target);self.send_header('Content-Length','0');self.end_headers();return
        if split.path!='/xc/player_api.php':return self.reply(404,{})
        query=urllib.parse.parse_qs(split.query,keep_blank_values=True);action=query.get('action',[None])[0];valid=query.get('username')==[USER]and query.get('password')==[PASSWORD]
        with LOCK:TRACE.append({'method':'GET','path':split.path,'action':action,'actual_query_keys':sorted(query),'actual_auth_matches':valid,'actual_user_agent':self.headers.get('User-Agent'),'session_cookie':self.headers.get('Cookie'),'phase':PHASE,'time_ns':time.time_ns()})
        if not valid or PHASE=='auth-error':return self.reply(401,{'error':'Owned fixture authentication rejected'})
        if action is None:return self.reply(200,{'user_info':{'username':USER,'password':PASSWORD,'auth':1,'status':'Active','exp_date':str(int(time.time())+86400*90),'max_connections':'4','allowed_output_formats':['ts']},'server_info':{'url':'127.0.0.1','port':str(PORT),'server_protocol':'http'}},uuid.uuid4().hex)
        if action=='get_live_categories':return self.reply(200,[{'category_id':'7','category_name':'XC enabled'},{'category_id':'8','category_name':'XC disabled'}])
        if action=='get_live_streams':return self.reply(200,[]if PHASE=='empty'else rows())
        return self.reply(400,{'error':'Unknown actual XC action'})
# Owned fixture Origin has an explicit graceful process contract. Main/Celery
# roles stop first; bounded idle keepalive handlers retire before server_close.
stop=threading.Event()
def request_stop(*_):stop.set()
signal.signal(signal.SIGTERM,request_stop);signal.signal(signal.SIGINT,request_stop)
with Server(('127.0.0.1',PORT),Handler)as server:
    server.timeout=.2
    (ROOT/'xc-live-origin.ready').write_text(json.dumps({'port':PORT,'outbound_requests':False,'credentials_exported':False,'TERM_handler_installed':True}))
    while not stop.is_set():server.handle_request()
