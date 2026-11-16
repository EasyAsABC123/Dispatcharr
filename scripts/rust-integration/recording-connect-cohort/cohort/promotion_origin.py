"""One owned HLS infrastructure role or finite nonloop UDP role; no Django/app/DB imports."""
import hashlib,json,os,signal,subprocess,threading,time
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from pathlib import Path
MODE=os.environ.get('PROMOTION_TRANSPORT','hls');UDP_PORT=int(os.environ.get('PROMOTION_UDP_PORT','19979'));
if MODE not in('hls','udp')or not 1024<=UDP_PORT<=65535:raise ValueError('OwnedTransportConfiguration')
ROOT=Path(os.environ['PROMOTION_ROOT']);ROOT.mkdir(exist_ok=True);STOP=threading.Event();LOCK=threading.Lock();STATE={'requests':[],'active':0,'failed_primary':False,'error':None};process=None
class Handler(BaseHTTPRequestHandler):
 def log_message(self,*args):pass
 def do_GET(self):
  path=self.path.split('?',1)[0]
  with LOCK:
   if len(STATE['requests'])>=4096:self.send_error(503);return
   STATE['requests'].append({'path':path,'user_agent':self.headers.get('User-Agent',''),'monotonic':time.monotonic()})
  if path=='/health':
   self.reply(200 if ((ROOT/'index.m3u8').exists()if MODE=='hls'else(ROOT/'fixture.ts').exists())and process.poll()is None else 503,b'ok');return
  if path=='/stats':
   with LOCK:data=json.dumps(STATE).encode()
   self.reply(200,data,'application/json');return
  if path=='/control/fail-primary':
   with LOCK:STATE['failed_primary']=True
   self.reply(200,b'ok');return
  if not path.startswith('/primary/'):self.send_error(404);return
  if STATE['failed_primary']:self.send_error(503);return
  filename=path.rsplit('/',1)[1]
  if not(filename=='index.m3u8'or(filename.startswith('segment-')and filename.endswith('.ts')and filename[8:-3].isdigit())):self.send_error(404);return
  file=ROOT/filename
  try:data=file.read_bytes()
  except FileNotFoundError:self.send_error(404);return
  if len(data)>4*1024*1024:self.send_error(503);return
  with LOCK:STATE['active']+=1
  try:self.reply(200,data,'application/vnd.apple.mpegurl'if filename.endswith('m3u8')else'video/mp2t')
  finally:
   with LOCK:STATE['active']-=1
 def reply(self,status,data,content='text/plain'):
  self.send_response(status);self.send_header('Content-Type',content);self.send_header('Content-Length',str(len(data)));self.send_header('Connection','close');self.end_headers();self.wfile.write(data)
def generation(pid):
 try:return Path('/proc',str(pid),'stat').read_text().rsplit(')',1)[1].split()[19]
 except FileNotFoundError:return None
def main():
 global process
 if os.geteuid()!=65532:raise RuntimeError('OwnedUID65532Required')
 fixture=ROOT/'fixture.ts'
 create=subprocess.run(['ffmpeg','-hide_banner','-v','error','-filter_threads','1','-filter_complex_threads','1','-f','lavfi','-i','testsrc2=size=160x90:rate=25','-f','lavfi','-i','sine=frequency=440:sample_rate=48000','-t',('40'if MODE=='udp'else'6'),'-c:v','libx264','-threads:v','1','-preset','ultrafast','-pix_fmt','yuv420p','-g','25','-c:a','aac','-threads:a','1','-b:a','64k','-f','mpegts','-y',str(fixture)],capture_output=True,timeout=20)
 (ROOT/'create.stderr').write_bytes(create.stderr)
 if create.returncode or not fixture.exists():raise RuntimeError('ActualOwnedHLSFixtureCreateFailed')
 stderr=(ROOT/'producer.stderr').open('wb')
 command=['ffmpeg','-hide_banner','-v','error']
 if MODE=='hls':command+=['-stream_loop','-1']
 command+=['-re','-threads','1','-i',str(fixture),'-map','0','-c','copy']
 if MODE=='hls':command+=['-f','hls','-hls_time','1','-hls_list_size','6','-hls_delete_threshold','1','-hls_flags','delete_segments+append_list','-hls_segment_filename',str(ROOT/'segment-%06d.ts'),str(ROOT/'index.m3u8')]
 else:command+=['-f','mpegts','udp://127.0.0.1:'+str(UDP_PORT)+'?pkt_size=1316']
 process=subprocess.Popen(command,stdin=subprocess.PIPE,stdout=subprocess.DEVNULL,stderr=stderr,start_new_session=True)
 pid=process.pid;start=generation(pid)
 server=ThreadingHTTPServer(('127.0.0.1',18098),Handler);server.daemon_threads=False;server.timeout=.2
 for sig in(signal.SIGINT,signal.SIGTERM):signal.signal(sig,lambda *_:STOP.set())
 failed=False
 try:
  while not STOP.is_set():
   status=process.poll()
   if status is not None and not(MODE=='udp'and status==0):raise RuntimeError('ActualMediaProducerExited')
   # A finite clean UDP EOF is observed, never restarted or looped. The
   # owner still collects this exact generation on requested role shutdown.
   total=0
   for item in ROOT.iterdir():
    try:
     if item.is_file():total+=item.stat().st_size
    except FileNotFoundError:pass
   if total>32*1024*1024:raise RuntimeError('HLSFiles32MiBBudget')
   server.handle_request()
 except BaseException as error:
  STATE['error']=type(error).__name__;failed=True
 finally:
  server.server_close();forced=False
  try:
   if process.poll()is None:process.stdin.write(b'q\n');process.stdin.flush()
   process.stdin.close();status=process.wait(timeout=10)
  except BaseException:
   forced=True;os.killpg(pid,signal.SIGKILL);status=process.wait(timeout=5)
  stderr.close();receipt={'transport':MODE,'target_port':UDP_PORT if MODE=='udp'else None,'actual_exit':status,'forced':forced,'wait_completed':True,'producer_generation_absent':generation(pid)!=start,'fixture_sha256':hashlib.sha256(fixture.read_bytes()).hexdigest(),'fixture_bytes':fixture.stat().st_size,'fixture_seconds':40 if MODE=='udp'else 6,'looped':MODE=='hls','stats':STATE}
  (ROOT/'final.json').write_text(json.dumps(receipt,indent=2)+'\n')
  print(json.dumps(receipt))
 return 1 if failed or status!=0 or forced or generation(pid)==start else 0
if __name__=='__main__':raise SystemExit(main())
