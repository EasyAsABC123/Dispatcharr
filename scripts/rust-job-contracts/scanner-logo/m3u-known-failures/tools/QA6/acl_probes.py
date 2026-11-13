"""Owned Redis ACL wire diagnostics. No import-time network/command execution.
DRYRUN never executes SETEX/PUBLISH. AUTH request values are never recorded.
"""
import base64,hashlib,re,socket,time
from owned_redis_reply_proxy import frame

def encode(values):
 data=[v if type(v)is bytes else str(v).encode()for v in values]
 return b'*'+str(len(data)).encode()+b'\r\n'+b''.join(b'$'+str(len(v)).encode()+b'\r\n'+v+b'\r\n'for v in data)
def projection(value):
 if type(value)is bytes:return {'raw_base64':base64.b64encode(value).decode()}
 if type(value)is list:return[projection(v)for v in value]
 return value
def profile_exact(value):
 try:
  if type(value)is not list or len(value)%2:return False
  pairs=dict(zip(value[::2],value[1::2]))
  if len(pairs)*2!=len(value):return False
  return b'on'in pairs[b'flags']and pairs[b'commands'].split()==[b'+@all',b'-setex']and pairs[b'keys']==b'~*'and pairs[b'channels']==b'&*'and pairs.get(b'selectors')==[]
 except (KeyError,TypeError,AttributeError):return False
def setex_denied(raw,value,username):
 if type(username)is not str or not re.fullmatch('owned_m3u_failure_[1-9][0-9]{0,9}',username):return False
 expected=b"User "+username.encode()+b" has no permissions to run the 'setex' command"
 # Redis ACL DRYRUN reports a denied operation as a BULK value, not an error.
 # This proves a nonexecuted permission decision, not an actual SETEX error.
 return type(value)is bytes and value==expected and raw==b'$'+str(len(expected)).encode()+b'\r\n'+expected+b'\r\n'
def permission_profile(username,key):
 report={'owned_profile_exact':False,'setex_denied':False,'publish_allowed':False,'nonmutating':False,'wire':[]}
 sock=None;stream=None
 try:
  if not re.fullmatch('owned_m3u_failure_[1-9][0-9]{0,9}',username)or type(key)is not bytes or not key.startswith(b'celery-tasks:celery-task-meta-'):raise ValueError('OwnedACLProbeScope')
  sock=socket.create_connection(('127.0.0.1',6379),timeout=5);stream=sock.makefile('rb')
  def command(label,args):
   began=time.time();sock.sendall(encode(args));raw,value=frame(stream);report['wire'].append({'label':label,'actual_raw_reply_base64':base64.b64encode(raw).decode(),'reply_sha256':hashlib.sha256(raw).hexdigest(),'parsed':projection(value),'began_epoch':began,'finished_epoch':time.time()});return raw,value
  raw,value=command('AUTH-response-only',['AUTH','playback-fixture-only'])
  if raw!=b'+OK\r\n':raise ValueError('OwnedAdminAuth')
  raw,value=command('SELECT9',['SELECT',9])
  if raw!=b'+OK\r\n':raise ValueError('OwnedBackend9')
  raw,before=command('GET-before',['GET',key]);before_none=before is None and raw==b'$-1\r\n'
  raw,profile=command('ACL-GETUSER',['ACL','GETUSER',username]);report['owned_profile_exact']=profile_exact(profile)
  raw,value=command('ACL-DRYRUN-SETEX',['ACL','DRYRUN',username,'SETEX',key,3600,'owned-probe-not-executed']);report['setex_denied']=setex_denied(raw,value,username)
  raw,value=command('ACL-DRYRUN-PUBLISH',['ACL','DRYRUN',username,'PUBLISH',key,'owned-probe-not-published']);report['publish_allowed']=raw==b'+OK\r\n'and value==b'OK'
  raw,after=command('GET-after',['GET',key]);report['nonmutating']=before_none and after is None and raw==b'$-1\r\n'
 except BaseException as error:report.update(error_type=type(error).__name__,error_text=str(error)[:1024])
 finally:
  if stream:stream.close()
  if sock:sock.close()
 return report

def denied_write_observation(receipt,script_sha):
 try:
  rows=[v for v in receipt['receipts']if v.get('mode')=='result-denied'and not v.get('script_cache_miss')]
  if len(rows)!=1 or receipt['target_replies']!=1:return False
  row=rows[0];raw=base64.b64decode(row['raw_base64'],validate=True)
  return row['actual_server_reply_observed']is True and row['script_sha1']==script_sha and raw.startswith(b'-')and(b"can't run this command"in raw or b'NOPERM'in raw)and hashlib.sha256(raw).hexdigest()==row['reply_sha256']and len(raw)==row['reply_bytes']
 except (KeyError,ValueError,TypeError):return False
