"""Owned same-namespace Redis process restart; no Django/application startup.
Original Dispatcharr/Redis authors retain application and server credit.
"""
import json,os,signal,socket,subprocess
if os.environ.get('REDIS_RESTART_DISPOSABLE_FIXTURE')!='playback-fixture-only':raise SystemExit(2)
path='/tmp/redis-restart-control.sock';server=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM);server.bind(path);server.listen(2);server.settimeout(.2);process=None;generation=0;closing=False

def stop():
 global process
 if process is not None and process.poll()is None:
  process.terminate()
  try:process.wait(timeout=3)
  except subprocess.TimeoutExpired:process.kill();process.wait(timeout=2)
 process=None

def start():
 global process,generation
 if process is None or process.poll()is not None:
  process=subprocess.Popen(['redis-server','--port','6379','--bind','127.0.0.1','--protected-mode','yes','--requirepass','playback-fixture-only','--save','','--appendonly','no','--dir','/tmp','--loglevel','warning'])
  generation+=1

def terminate(*unused):
 global closing
 closing=True
signal.signal(signal.SIGTERM,terminate);signal.signal(signal.SIGINT,terminate);start()
try:
 while not closing:
  try:client,_=server.accept()
  except socket.timeout:continue
  with client:
   client.settimeout(2);raw=bytearray()
   while b'\n'not in raw and len(raw)<1024:
    part=client.recv(1024-len(raw))
    if not part:break
    raw.extend(part)
   request=json.loads(raw)
   if request.get('action')=='stop':stop()
   elif request.get('action')=='start':start()
   elif request.get('action')!='status':raise ValueError('Owned control action only')
   client.sendall((json.dumps({'running':process is not None and process.poll()is None,'generation':generation})+'\n').encode())
finally:stop();server.close()
