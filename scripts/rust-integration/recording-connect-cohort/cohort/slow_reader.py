"""Actual completed-media backpressure/FD-cancellation and Native TERM observer.
No apps on import. Invoke only inside Root's granted shared Recording cohort.
Original Dispatcharr authors retain all application/algorithm credit.
"""
import array,fcntl,hashlib,json,os,socket,time,urllib.error,urllib.parse,urllib.request,uuid
from pathlib import Path
CURRENT_FIXTURE=None
def budget(seconds):
 return seconds if CURRENT_FIXTURE is None else CURRENT_FIXTURE.remaining(seconds)

IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'

def save(path,value):
 temporary=path.with_suffix(path.suffix+'.pending');temporary.write_text(json.dumps(value,indent=2)+'\n');os.replace(temporary,path)
def samples(directory):
 try:data=(directory/'samples.jsonl').read_bytes()
 except FileNotFoundError:return[]
 lines=data.split(b'\n');return[json.loads(line)for line in lines[:-1]if line]
def queued(connection):
 value=array.array('i',[0]);fcntl.ioctl(connection.fileno(),0x541B,value,True);return value[0]
def digest(path):
 h=hashlib.sha256()
 with path.open('rb')as f:
  for block in iter(lambda:f.read(65536),b''):h.update(block)
 return h.hexdigest()
def deadline_wait(predicate,seconds,label):
 deadline=time.monotonic()+budget(seconds)
 while time.monotonic()<deadline:
  budget(max(.001,deadline-time.monotonic()))
  value=predicate();budget(.001)
  if value:return value
  time.sleep(budget(.05))
 raise TimeoutError(label)

def connect_reader(fixture,ident):
 endpoint=urllib.parse.urlsplit(fixture.base_url)
 if endpoint.scheme!='http' or endpoint.hostname not in('127.0.0.1','::1','localhost'):raise ValueError('OwnedLoopbackHTTPOnly')
 addresses=socket.getaddrinfo(endpoint.hostname,endpoint.port or 80,type=socket.SOCK_STREAM);family,kind,protocol,_,address=addresses[0]
 connection=socket.socket(family,kind,protocol);connection.settimeout(budget(5));connection.setsockopt(socket.SOL_SOCKET,socket.SO_RCVBUF,1024)
 clamp=getattr(socket,'TCP_WINDOW_CLAMP',None);options={'requested_SO_RCVBUF':1024,'actual_SO_RCVBUF':connection.getsockopt(socket.SOL_SOCKET,socket.SO_RCVBUF),'actual_TCP_WINDOW_CLAMP':None}
 if clamp is not None:
  connection.setsockopt(socket.IPPROTO_TCP,clamp,1024);options['actual_TCP_WINDOW_CLAMP']=connection.getsockopt(socket.IPPROTO_TCP,clamp)
 try:
  connection.settimeout(budget(5));connection.connect(address);options['actual_connected_SO_RCVBUF']=connection.getsockopt(socket.SOL_SOCKET,socket.SO_RCVBUF)
  if clamp is not None:options['actual_connected_TCP_WINDOW_CLAMP']=connection.getsockopt(socket.IPPROTO_TCP,clamp)
  path=f'/api/channels/recordings/{int(ident)}/file/'
  # Credential remains only in memory; never stored in request/command evidence.
  request=('GET '+path+' HTTP/1.1\r\nHost: '+endpoint.netloc+'\r\nAuthorization: Bearer '+fixture.admin+'\r\nConnection: close\r\n\r\n').encode('ascii');connection.sendall(request)
  raw=b''
  while b'\r\n\r\n'not in raw:
   if len(raw)>65536:raise ValueError('ActualHTTPHeader64KiB')
   connection.settimeout(budget(5));part=connection.recv(4096)
   if not part:raise EOFError('ActualEOFBeforeResponseHeaders')
   raw+=part
  head,prefix=raw.split(b'\r\n\r\n',1);lines=head.split(b'\r\n');status=int(lines[0].split()[1]);headers={}
  for line in lines[1:]:
   key,value=line.split(b':',1);headers[key.decode('latin1').lower()]=value.decode('latin1').strip()
  if status!=200:raise ValueError('ActualFileStatus:'+str(status))
  connection.settimeout(budget(.2))
  return connection,{'actual_status':status,'headers':{k:v for k,v in headers.items()if k not in('set-cookie','location')},'body_prefix_consumed_bytes':len(prefix),'socket_options':options,'actual_local_address':connection.getsockname(),'actual_peer_address':connection.getpeername()}
 except BaseException:connection.close();raise

def held(directory,connection,file_size,after,seconds=8):
 def inspect():
  rows=[row for row in samples(directory)if row['epoch']>=after]
  for latest in reversed(rows):
   for fd in latest['fds']:
    if fd['position']>=file_size:continue
    signature=(fd['pid'],fd['generation'],fd['fd'],fd['inode'],fd['position'])
    matching=[]
    for row in reversed(rows):
     if any((v['pid'],v['generation'],v['fd'],v['inode'],v['position'])==signature for v in row['fds']):matching.append(row)
     else:break
    unread=queued(connection)
    if len(matching)>=6 and latest['epoch']-matching[-1]['epoch']>=.25 and unread>0:
     return {'actual_FD':fd,'stable_observations':len(matching),'stable_seconds':latest['epoch']-matching[-1]['epoch'],'actual_socket_unread_bytes':unread,'file_not_fully_read':True,'first_epoch':matching[-1]['epoch'],'last_epoch':latest['epoch']}
  return None
 return deadline_wait(inspect,seconds,'ActualPreEOFFileFDAndUnreadSocketBackpressureNotProven')

def close_reader(connection):
 if connection is None:return
 try:connection.shutdown(socket.SHUT_RDWR)
 except OSError:pass
 connection.close()
def intake(fixture):
 deadline=time.monotonic()+budget(3);observations=[]
 while time.monotonic()<deadline:
  budget(max(.001,deadline-time.monotonic()))
  try:
   request=urllib.request.Request(fixture.base_url+'/health/ready')
   try:response=urllib.request.urlopen(request,timeout=budget(.5))
   except urllib.error.HTTPError as error:response=error
   with response:observations.append({'epoch':time.time(),'transport':'HTTP','status':response.status,'body_consumed':False})
   if observations[-1]['status']==503:break
  except OSError as error:
   observations.append({'epoch':time.time(),'transport':'refused_or_closed','error_type':type(error).__name__,'errno':getattr(error,'errno',None),'reason_type':type(getattr(error,'reason',None)).__name__});break
  time.sleep(budget(.05))
 return observations

def run(fixture,ident,terminate=True):
 global CURRENT_FIXTURE
 CURRENT_FIXTURE=fixture;budget(1)
 row=next(v for v in fixture.read_rows()if v['id']==ident);path=Path((row.get('custom_properties')or{})['file_path']);host=fixture.case_data/path.relative_to('/data')
 metadata=host.stat();directory=fixture.output/('slow-reader-'+str(ident)+'-'+uuid.uuid4().hex[:6]);directory.mkdir(mode=0o777);directory.chmod(0o777)
 report={'status':'failed','backend':fixture.backend,'recording_id':ident,'actual_file_bytes':metadata.st_size,'actual_file_sha256':digest(host),'Source_API_or_task_replay':False,'media_synthesized':False,'performance_claims':False,'phases':[],'observer':{},'required_scope':'Actual FD/backpressure/cancellation; Native additionally held-reader TERM/drain.','Source_held_TERM_characterized':False};reader=None;observer=None;observer_info=None;native_term=False;source_term=False;observer_stopped=False
 try:
  if fixture.backend=='Native':
   target=json.loads(fixture.command([fixture.docker,'inspect',fixture.native]))[0];config={'kind':'Native','target_pid':1,'target_generation':fixture.pid_start(target['State']['Pid'])}
  else:
   target=json.loads(fixture.command([fixture.docker,'inspect',fixture.project+'-baseline-1']))[0];config={'kind':'Original','target_pgid':fixture.roles['web']['actual_pgid'],'target_pid':fixture.roles['web']['pid'],'target_generation':fixture.roles['web']['starttime']}
  config.update(file_inode=metadata.st_ino,file_bytes=metadata.st_size);save(directory/'config.json',config);(directory/'config.json').chmod(0o644)
  helper=Path(__file__).with_name('observer.py');(directory/'observer.py').write_bytes(helper.read_bytes());(directory/'observer.py').chmod(0o444)
  name=fixture.project+'-fd-'+uuid.uuid4().hex[:5];argv=[fixture.docker,'run','-d','--name',name,'--label','com.docker.compose.project='+fixture.project,'--pid','container:'+target['Id'],'--network','none','--workdir','/observer','--user','65532:65532','--read-only','--tmpfs','/app:ro,mode=000,size=65536','--cap-drop','ALL','--security-opt','no-new-privileges:true','--cpus','.5','--memory','96m','--pids-limit','16','-e','PYTHONDONTWRITEBYTECODE=1','-v',str(directory)+':/observer','--entrypoint','/dispatcharrpy/bin/python',IMAGE,'-B','/observer/observer.py']
  observer=fixture.command(argv).strip();observer_info=json.loads(fixture.command([fixture.docker,'inspect',observer]))[0];assert observer_info['HostConfig']['PidMode']=='container:'+target['Id'] and observer_info['Config']['User']=='65532:65532';assert 'ro' in observer_info['HostConfig']['Tmpfs']['/app'] and 'mode=000'in observer_info['HostConfig']['Tmpfs']['/app'];binds=[m for m in observer_info['Mounts']if m['Type']=='bind'];assert len(binds)==1 and binds[0]['Destination']=='/observer' and Path(binds[0]['Source']).resolve()==directory.resolve();fixture.containers.append({'Id':observer_info['Id'],'pid':observer_info['State']['Pid'],'starttime':fixture.pid_start(observer_info['State']['Pid']),'image':observer_info['Image']})
  deadline_wait(lambda:(directory/'ready.json').exists(),8,'ActualObserverReady');ready=json.loads((directory/'ready.json').read_bytes());assert ready['actual_UID']==65532 and ready['observer_pid_namespace']==ready['target_pid_namespace'] and ready['masked_app_mode']=='0o0'and ready['masked_app_read_refused'];report['observer'].update(actual_CID=observer,actual_image=observer_info['Image'],actual_same_PID_namespace=True,actual_UID=65532,target_CID=target['Id'])
  began=time.time();reader,http=connect_reader(fixture,ident);phase={'name':'live-reader-cancellation','HTTP':http,'held':held(directory,reader,metadata.st_size,began)};report['phases'].append(phase);cancel=time.time();close_reader(reader);reader=None;phase['reader_cancelled_epoch']=cancel
  empty=deadline_wait(lambda:next((v for v in reversed(samples(directory))if v['epoch']>=cancel and v['target_pids'] and not v['fds'] and not v['errors']),None),5,'ActualFDNotRetiredAfterReaderCancellation');phase['actual_post_cancel_FD_absence']=empty;phase['passed']=True
  if fixture.backend=='Native' and terminate:
   began=time.time();reader,http=connect_reader(fixture,ident);phase={'name':'held-reader-Native-SIGTERM','HTTP':http,'held':held(directory,reader,metadata.st_size,began)};report['phases'].append(phase)
   fixture.command([fixture.docker,'kill','--signal','TERM',fixture.native],10);native_term=True;phase['actual_SIGTERM_epoch']=time.time();phase['new_intake_actual_transport']=intake(fixture);phase['actual_new_intake_closed']=any(v['transport']=='refused_or_closed' or v.get('status')==503 for v in phase['new_intake_actual_transport']);phase['external503_required']=False
   assert phase['actual_new_intake_closed'],'ActualNewIntakeClosureNotObserved'
   phase['held_after_SIGTERM']=held(directory,reader,metadata.st_size,phase['actual_SIGTERM_epoch'],5)
   # PID namespace init exit kills sibling observers. Settle this observer0 while
   # the actual retained file/socket still holds Main, before cancelling reader.
   (directory/'stop').write_text('actual observer stop before reader cancellation\n');deadline_wait(lambda:(directory/'receipt.json').exists(),5,'ActualObserverReceipt');code=int(fixture.command([fixture.docker,'wait',observer],10).strip());report['observer']['actual_wait_exit']=code;observer_stopped=True
   receipt=json.loads((directory/'receipt.json').read_bytes());report['observer']['receipt']=receipt;assert code==0 and receipt['passed'] and receipt['writer_closed'] and receipt['stop_marker_observed']
   phase['reader_cancelled_epoch']=time.time();close_reader(reader);reader=None
   code=int(fixture.command([fixture.docker,'wait',fixture.native],20).strip());state=json.loads(fixture.command([fixture.docker,'inspect',fixture.native]))[0]['State'];phase['actual_Main_wait_exit']=code;phase['actual_Main_state']=state;phase['original_Main_host_generation_absent']=fixture.pid_start(target['State']['Pid'])!=config['target_generation'];phase['FD_retired_by_actual_process_exit']=phase['original_Main_host_generation_absent'];assert code==0 and not state['Running'] and not state['OOMKilled'] and phase['FD_retired_by_actual_process_exit'];phase['normal_stop_receipt']=fixture.stop_native(expected=0);phase['passed']=True
  elif fixture.backend=='Original' and terminate:
   began=time.time();reader,http=connect_reader(fixture,ident);phase={'name':'held-reader-Original-SIGTERM-characterization','HTTP':http,'held':held(directory,reader,metadata.st_size,began)};report['phases'].append(phase)
   source_term=True;phase['actual_SIGTERM_request_epoch']=time.time();phase['actual_begin_stop_receipt']=fixture.begin_original_web_stop();signal=phase['actual_begin_stop_receipt'];assert signal['shutdown_started'] and signal['signal_settled'] and signal['actual_master_SIGTERM_sent'];phase['actual_SIGTERM_settled_epoch']=time.time()
   phase['new_intake_actual_transport']=intake(fixture);phase['external503_required']=False
   post=deadline_wait(lambda:next((v for v in reversed(samples(directory))if v['epoch']>=phase['actual_SIGTERM_settled_epoch']),None),5,'ActualSourcePostSignalFDSample');phase['actual_post_SIGTERM_FDs']=post;phase['Source_retired_FD_before_reader_cancel']=not post['fds'];phase['Source_retained_FD_before_reader_cancel']=bool(post['fds']);phase['identical_to_Native_hold_required']=False
   phase['reader_cancelled_epoch']=time.time();close_reader(reader);reader=None
   phase['actual_after_cancel_FD_absence']=deadline_wait(lambda:next((v for v in reversed(samples(directory))if v['epoch']>=phase['reader_cancelled_epoch'] and not v['fds'] and not v['errors']),None),5,'ActualSourceFDNotRetiredAfterCancellation')
   stop=fixture.role('web',[],stop=True);phase['actual_normal_role_stop_receipt']=stop;assert stop['actual_exit']==0 and stop['stdout_EOF'] and stop['stderr_EOF'] and not stop['forced'] and not stop['failures'] and stop['all_original_process_generations_absent'];phase['passed']=True
   report['Source_shutdown_scope']='Actual second stalled reader plus supervised master TERM, literal early-retirement/continued-hold observation, cancellation and normal role wait/EOF/generation receipt; no identical Native timing requirement.'
  else:
   report['Source_shutdown_scope']='Cancellation only; held-TERM explicitly not requested.'
  report['status']='passed'
 except BaseException as error:report['failure']={'type':type(error).__name__,'message':str(error)}
 finally:
  close_reader(reader)
  if fixture.functional_budget_expired():fixture.begin_cleanup()
  if observer is not None:
   if not observer_stopped:
    try:
     logs=fixture.command([fixture.docker,'logs','--tail','40',observer],10,secret=True,allow=True)
     report['observer']['private_startup_log_sha256']=hashlib.sha256(logs.encode()).hexdigest();report['observer']['private_startup_log_bytes']=len(logs.encode())
    except BaseException as error:report['observer']['startup_log_failure']=type(error).__name__
    try:
     (directory/'stop').write_text('actual finally stop\n');deadline_wait(lambda:(directory/'receipt.json').exists(),5,'ActualObserverFinallyReceipt')
    except BaseException as error:report['status']='failed';report['observer']['marker_failure']={'type':type(error).__name__,'message':str(error)}
    try:report['observer']['actual_wait_exit']=int(fixture.command([fixture.docker,'wait',observer],10).strip())
    except BaseException as error:
     report['status']='failed';report['observer']['wait_failure']={'type':type(error).__name__,'message':str(error)}
     try:fixture.command([fixture.docker,'stop','--time','2',observer],8,allow=True);report['observer']['forced_cleanup_requested']=True
     except BaseException as error:report['observer']['stop_failure']={'type':type(error).__name__,'message':str(error)}
    if(directory/'receipt.json').exists():report['observer']['receipt']=json.loads((directory/'receipt.json').read_bytes())
   try:fixture.command([fixture.docker,'rm',observer],10);report['observer']['removed']=True
   except BaseException as error:report['status']='failed';report['observer']['remove_failure']={'type':type(error).__name__,'message':str(error)}
   if report['observer'].get('actual_wait_exit')!=0 or not report['observer'].get('receipt',{}).get('passed'):report['status']='failed'
  if native_term and fixture.native is not None:
   try:report['cleanup_Main_stop']=fixture.stop_native(expected=0)
   except BaseException as error:report['status']='failed';report['cleanup_Main_failure']={'type':type(error).__name__,'message':str(error)}
  if source_term and 'web' in fixture.roles:
   try:report['cleanup_Source_web_stop']=fixture.role('web',[],stop=True)
   except BaseException as error:report['status']='failed';report['cleanup_Source_failure']={'type':type(error).__name__,'message':str(error)}
  report['required_unmeasured']=(['held-reader TERM was explicitly not requested']if not terminate else [])
  report['actual_file_unchanged']=host.exists() and host.stat().st_size==metadata.st_size and digest(host)==report['actual_file_sha256'];report['status']='passed'if report['status']=='passed'and report['actual_file_unchanged']else'failed';report['executed_slice_passed']=report['status']=='passed';report['Source_held_TERM_characterized']=fixture.backend=='Original' and terminate and len(report['phases'])==2 and all(v.get('passed')for v in report['phases']) and report['status']=='passed';report['physical_reader_shutdown_qualified']=fixture.backend=='Native' and terminate and len(report['phases'])==2 and all(v.get('passed')for v in report['phases']) and report['status']=='passed';report['output']=str(directory);save(directory/'report.json',report)
 CURRENT_FIXTURE=None
 return report
