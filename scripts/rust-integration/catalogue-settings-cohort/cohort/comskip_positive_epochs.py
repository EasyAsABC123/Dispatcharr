"""Actual Native positive epochs after unchanged old manual3.
Original Dispatcharr, Comskip, FFmpeg and dependency authors retain all credit.
No application/media starts on import; run requires the bound public Source6.
"""
import ast,base64,datetime,hashlib,importlib.util,json,math,os,shutil,threading,time,traceback,uuid
from pathlib import Path
from workflow import save,utc
import dvr_domain_cases as journal
import m3u_refresh_all_cases as aggregate
TASK='apps.channels.tasks.comskip_process_recording'
RECORD='apps.channels.tasks.run_recording'
ARTWORK='apps.channels.tasks.prefetch_recording_artwork'
def guest_media(file,decode=False):
 """Read-only actual file/probe/decode observation, separate from business tools."""
 import subprocess
 path=Path(file);result={'file_path':file,'exists':path.is_file(),'observer_only':True}
 if not result['exists']:return result
 def sha(p):
  h=hashlib.sha256()
  with p.open('rb')as stream:
   for chunk in iter(lambda:stream.read(65536),b''):h.update(chunk)
  return h.hexdigest()
 result.update(bytes=path.stat().st_size,sha256=sha(path));e=path.with_suffix('.edl')
 result['EDL']={'exists':e.is_file()}
 if e.is_file():
  assert e.stat().st_size<=1024*1024;text=e.read_text();result['EDL'].update(text=text,sha256=hashlib.sha256(text.encode()).hexdigest())
 args=['ffprobe','-v','error','-show_entries','format=duration:stream=codec_type,codec_name','-of','json',file]
 p= subprocess.run(args,capture_output=True,text=True,timeout=20);result['probe']={'argv':args,'actual_exit':p.returncode,'stdout':p.stdout,'stderr':p.stderr}
 if p.returncode==0:result['probe']['actual']=json.loads(p.stdout)
 sidecars=[]
 for item in sorted(path.parent.glob(path.stem+'*')):
  if item==path or not item.is_file():continue
  row={'name':item.name,'bytes':item.stat().st_size,'sha256':sha(item)}
  if item.suffix in('.log','.txt','.edl'):
   with item.open('rb')as stream:prefix=stream.read(65536)
   row.update(prefix=prefix.decode('utf-8','replace'),prefix_truncated=item.stat().st_size>len(prefix))
  sidecars.append(row)
 result['actual_sidecars']=sidecars
 result['fixed_cut_paths_remaining']=[str(item)for item in sorted(path.parent.glob('segment_*.mkv'))]+[str(path.parent/'concat_list.txt')]if(path.parent/'concat_list.txt').exists()else[str(item)for item in sorted(path.parent.glob('segment_*.mkv'))]
 if decode:
  args=['ffmpeg','-v','error','-threads','1','-err_detect','explode','-i',file,'-map','0:v:0','-map','0:a:0','-f','null','-']
  p=subprocess.run(args,capture_output=True,text=True,timeout=90);result['full_actual_A_V_decode']={'argv':args,'actual_exit':p.returncode,'stderr':p.stderr[-65536:]}
 return result

def _row(f,ident):return f.sql("print(json.dumps(Recording.objects.values().get(pk="+str(ident)+"),default=str))",secret=True)

def _snapshot(f):return f.sql("from core.models import CoreSettings,SystemEvent;from django_celery_beat.models import PeriodicTask,ClockedSchedule;print(json.dumps({'recordings':list(Recording.objects.values()),'DVR':CoreSettings.objects.get(key='dvr_settings').value,'dvr_clocked':list(PeriodicTask.objects.filter(task='apps.channels.tasks.run_recording').values()),'clocked':list(ClockedSchedule.objects.values()),'events':list(SystemEvent.objects.order_by('id').values())},default=str))",secret=True)

def _body(frame):return json.loads(base64.b64decode(frame['body']))

def _statuses(events,ident):return [e['data']for e in events['events']if isinstance(e.get('data'),dict)and e['data'].get('type')=='comskip_status'and e['data'].get('recording_id')==ident]

class EDLWatch:
 """Retain actual generated EDL before successful cut removes it; never writes it."""
 def __init__(self,root,excluded=(),capture_directory=None):
  self.root=Path(root);self.excluded=set(excluded);self.stop=threading.Event();self.observed={};self.errors=[];self.inputs={};self.input_identities={};self.capture_directory=capture_directory
  if capture_directory is not None:
   Path(capture_directory).mkdir(parents=True,mode=0o755,exist_ok=False);Path(capture_directory).chmod(0o755)
  self.thread=threading.Thread(target=self.read,daemon=True)
 def read(self):
  while not self.stop.is_set():
   try:
    for movie in self.root.rglob('*.mkv'):
     info=movie.stat();self.input_identities.setdefault(str(movie),(info.st_dev,info.st_ino))
    for p in self.root.rglob('*.edl'):
     if str(p)in self.excluded or not p.is_file():continue
     raw=p.read_bytes();assert len(raw)<=1024*1024
     if raw:
      self.observed[str(p)]={'sha256':hashlib.sha256(raw).hexdigest(),'text':raw.decode(),'observed_epoch':time.time()}
      if self.capture_directory is not None and str(p)not in self.inputs:
       original=p.with_suffix('.mkv');fd=os.open(original,os.O_RDONLY|os.O_NOFOLLOW)
       with os.fdopen(fd,'rb')as source:
        before=os.fstat(source.fileno());assert(before.st_dev,before.st_ino)==self.input_identities[str(original)],'Original input was already replaced before observation'
        assert 0<before.st_size<=64*1024*1024
        target=Path(self.capture_directory)/(str(len(self.inputs))+'.mkv');h=hashlib.sha256();copied=0
        with target.open('xb')as destination:
         for block in iter(lambda:source.read(65536),b''):h.update(block);destination.write(block);copied+=len(block)
        target.chmod(0o644)
        after=os.fstat(source.fileno());assert copied==before.st_size==after.st_size and before.st_mtime_ns==after.st_mtime_ns,'Completed original input changed during retained observation'
       self.inputs[str(p)]={'path':str(target),'bytes':copied,'sha256':h.hexdigest(),'observed_original_inode':list(self.input_identities[str(original)]),'actual_input_retained_before_replacement':True}
   except (OSError,UnicodeError,AssertionError,KeyError)as e:self.errors.append(type(e).__name__);self.errors=self.errors[-8:]
   self.stop.wait(.025)
 def __enter__(self):self.thread.start();return self
 def __exit__(self,*_):self.stop.set();self.thread.join(2);assert not self.thread.is_alive(),'EDLreadwatchthreadsettlement'

def _intervals(text,duration):
 ranges=[]
 for line in text.splitlines():
  parts=line.split()
  if len(parts)>=2:
   try:s,e=map(float,parts[:2])
   except ValueError:continue
   assert math.isfinite(s)and math.isfinite(e);ranges.append((max(0,s),min(duration,e)))
 ranges.sort();return ranges

RECEIVER_SOURCE='"""Continuous version of the existing channel-layer fixture receiver.\nLong recording/detector phases outlive Channels\' ordinary60s message expiry.\nNo task/model invocation, frontend mock or websocket publication is performed.\nOriginal Dispatcharr/Channels/Redis authors retain protocol/application credit.\n"""\nimport base64,hashlib,json,os,signal,threading,time\nfrom pathlib import Path\nfrom urllib.parse import quote\nimport redis\nfrom channels_redis.core import RedisChannelLayer\nfrom asgiref.sync import async_to_sync\nroot=Path(os.environ[\'POSITIVE_WS_ROOT\']);name=os.environ[\'POSITIVE_WS_CHANNEL\'];root.mkdir(mode=0o755,exist_ok=False)\npassword=os.environ[\'REDIS_PASSWORD\'];layer=RedisChannelLayer(hosts=[\'redis://:\'+quote(password,safe=\'\')+\'@127.0.0.1:6379/0\']);client=redis.Redis(host=\'127.0.0.1\',port=6379,password=password,db=0,socket_connect_timeout=3,socket_timeout=3)\nstop=threading.Event();signal.signal(signal.SIGTERM,lambda *_:stop.set());signal.signal(signal.SIGINT,lambda *_:stop.set());seen=set();messages=[];failure=None\nkey=layer.prefix+name.split(\'!\')[0]+\'!\'\ndef save(path,value):\n raw=json.dumps(value,allow_nan=False).encode();assert len(raw)<=8*1024*1024\n temporary=path.with_suffix(path.suffix+\'.pending\');temporary.write_bytes(raw);os.replace(temporary,path)\ndef observe():\n raw=client.zrange(key,0,1024);assert len(raw)<=1024 and sum(map(len,raw))<=4*1024*1024\n for value in raw:\n  fingerprint=hashlib.sha256(value).hexdigest()\n  if fingerprint in seen:continue\n  assert len(messages)<2048\n  seen.add(fingerprint);messages.append({\'sha256\':fingerprint,\'raw_base64\':base64.b64encode(value).decode(),\'actual\':layer.deserialize(value),\'observed_epoch\':time.time()})\n save(root/\'snapshot.json\',{\'actual_receiver\':name,\'events\':[r[\'actual\']for r in messages],\'raw_events_base64\':[r[\'raw_base64\']for r in messages],\'actual_records\':messages,\'continuous_retention\':True})\nasync_to_sync(layer.group_add)(\'updates\',name);save(root/\'ready.json\',{\'actual_group_add_completed\':True,\'receiver\':name,\'actual_euid\':os.geteuid(),\'continuous_retention\':True})\ntry:\n while not stop.is_set():observe();stop.wait(.25)\n observe()\nexcept BaseException as error:failure=type(error).__name__\nfinally:\n try:async_to_sync(layer.group_discard)(\'updates\',name);discarded=True\n except BaseException as error:discarded=False;failure=failure or type(error).__name__\n client.close();save(root/\'final.json\',{\'actual_receiver\':name,\'records\':len(messages),\'reader_closed\':True,\'group_discard_completed\':discarded,\'failure_type\':failure,\'pass\':failure is None and discarded})\n'
ORIGIN_SOURCE='"""Owned real packet-aligned feed for natural DVR roots; no application mocks.\nOriginal Dispatcharr/FFmpeg contributors retain application/algorithm credit.\n"""\nimport argparse,hashlib,json,signal,threading,time\nfrom pathlib import Path\nfrom http.server import BaseHTTPRequestHandler,ThreadingHTTPServer\nCHUNK=188*7;RATE=250000\nif __name__==\'__main__\':\n p=argparse.ArgumentParser();p.add_argument(\'--clip\',type=Path,required=True);p.add_argument(\'--ready\',type=Path,required=True);p.add_argument(\'--final\',type=Path,required=True);p.add_argument(\'--port\',type=int,default=18096);a=p.parse_args()\n assert a.clip.is_file()and 0<a.clip.stat().st_size<=512*1024*1024 and a.clip.stat().st_size%188==0\n h=hashlib.sha256()\n with a.clip.open(\'rb\')as f:\n  for block in iter(lambda:f.read(1024*1024),b\'\'):h.update(block)\n lock=threading.Lock();stop=threading.Event();stats={\'active\':0,\'maximum_active\':0,\'connections\':0,\'bytes\':0,\'closed\':0,\'EOF\':0,\'clip_sha256\':h.hexdigest(),\'clip_bytes\':a.clip.stat().st_size,\'bytes_per_second\':RATE}\n class Handler(BaseHTTPRequestHandler):\n  protocol_version=\'HTTP/1.1\'\n  def log_message(self,*args):pass\n  def do_GET(self):\n   if self.path in(\'/health\',\'/snapshot\'):\n    with lock:data=json.dumps(stats).encode()\n    self.send_response(200);self.send_header(\'Content-Type\',\'application/json\');self.send_header(\'Content-Length\',str(len(data)));self.end_headers();self.wfile.write(data);return\n   if self.path!=\'/live.ts\':self.send_error(404);return\n   self.send_response(200);self.send_header(\'Content-Type\',\'video/mp2t\');self.send_header(\'Content-Length\',str(a.clip.stat().st_size));self.end_headers();self.close_connection=True\n   with lock:stats[\'active\']+=1;stats[\'connections\']+=1;stats[\'maximum_active\']=max(stats[\'maximum_active\'],stats[\'active\'])\n   began=time.monotonic();sent=0\n   try:\n    with a.clip.open(\'rb\')as stream:\n     while not stop.is_set():\n      block=stream.read(CHUNK)\n      if not block:\n       with lock:stats[\'EOF\']+=1\n       break\n      self.wfile.write(block);self.wfile.flush();sent+=len(block)\n      with lock:stats[\'bytes\']+=len(block)\n      stop.wait(max(0,began+sent/RATE-time.monotonic()))\n   except (BrokenPipeError,ConnectionResetError,OSError):pass\n   finally:\n    with lock:stats[\'active\']-=1;stats[\'closed\']+=1\n server=ThreadingHTTPServer((\'0.0.0.0\',a.port),Handler);server.daemon_threads=True\n def finish(*_):stop.set();threading.Thread(target=server.shutdown,daemon=True).start()\n signal.signal(signal.SIGTERM,finish);signal.signal(signal.SIGINT,finish)\n a.ready.write_text(json.dumps({\'port\':a.port,**stats})+\'\\n\')\n try:server.serve_forever(poll_interval=.1)\n finally:\n  stop.set();server.server_close();a.final.write_text(json.dumps(stats)+\'\\n\')\n'

def _load(ref,name):
 path=Path(ref['path']);assert hashlib.sha256(path.read_bytes()).hexdigest()==ref['sha256'],name
 spec=importlib.util.spec_from_file_location(name,path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module

def _drain(f,label):f.wait(lambda:not any(aggregate.broker(f).values()),30,label+'-broker-settled')

def _health(f,beat,out):
 status,raw=f.get(f.base_url+'/health/ready');value=json.loads(raw);save(out,value)
 assert status==200 and value['selected_workers']['ready']is True and value['selected_workers']['clocked_scheduler_enabled']is beat
 return value

def _setting(f,mode,automatic,ini):
 value=f.sql("from core.models import CoreSettings;s=CoreSettings.objects.get(key='dvr_settings');before=s.value;s.value={**(s.value or {}),'comskip_mode':"+repr(mode)+",'comskip_enabled':"+repr(automatic)+",'comskip_hw_accel':'none','comskip_custom_path':"+repr(ini)+"};s.save();print(json.dumps({'before':before,'after':s.value}))",secret=True)
 assert value['after']['comskip_mode']==mode and value['after']['comskip_enabled']is automatic
 return value

def _guest(f,file):
 assert type(file)is str and file.startswith('/data/')
 parts=file.split('/');assert parts[:2]==['','data']and len(parts)>2 and all(part not in('','.','..')for part in parts[2:]),'Exact owned path components required'
 root=f.case_data.resolve();data=f.data.resolve();host=root.joinpath(*parts[2:]);resolved=host.resolve()
 assert resolved.is_relative_to(root)and resolved.is_relative_to(data),'Observer path escapes owned fixture'
 assert not host.is_symlink(),'Regular owned recording path required'
 return '/shared/'+str(resolved.relative_to(data))

def _media(f,file,decode=False):
 actual=_guest(f,file)
 code="import json,sys;sys.path.insert(0,'/normal-dvr-tools');from comskip_positive_epochs import guest_media;v=guest_media("+repr(actual)+","+repr(decode)+");v['observer_input_path']=v['file_path'];v['file_path']="+repr(file)+";print(json.dumps(v))"
 return json.loads(f.python(code,seconds=120 if decode else 25,secret=True))

def _receiver_begin(f,label):
 name='positive-ws-'+uuid.uuid4().hex[:8];host=f.case_data/name;guest='/shared/'+f.case+'/'+name;channel='specific.positive_'+uuid.uuid4().hex+'!receiver'
 f.role(name,['/dispatcharrpy/bin/python','-c',RECEIVER_SOURCE],extra={'POSITIVE_WS_ROOT':guest,'POSITIVE_WS_CHANNEL':channel})
 ready=f.wait(lambda:json.loads((host/'ready.json').read_text())if(host/'ready.json').is_file()else False,15,label+'-continuous-receiver')
 assert ready['actual_group_add_completed']and ready['continuous_retention'];return name,host,ready

def _receiver_end(f,receiver,p):
 name,host,ready=receiver;stop=f.role(name,[],stop=True);final=json.loads((host/'final.json').read_text());value=json.loads((host/'snapshot.json').read_text())
 save(p/'receiver-ready.json',ready);save(p/'receiver-final.json',final);save(p/'receiver-stop.json',stop)
 assert final['pass']and final['reader_closed']and final['group_discard_completed'];return value

def _reset(f,allowed,rows,out):
 # Only the already-settled fixture-owned Recording rows and its own sequence.
 # Raw SQL delete + bulk_create bypass Source post_save/task producers entirely.
 code="""from apps.channels.models import Recording
from django.db import connection,transaction
from django.core.serializers.json import DjangoJSONEncoder
allowed=ALLOWED;rows=ROWS
with transaction.atomic():
 before=list(Recording.objects.order_by('id').values());assert set(r['id']for r in before)==set(allowed)
 table=Recording._meta.db_table
 with connection.cursor()as cursor:
  cursor.execute('SELECT pg_get_serial_sequence(%s,%s)',[table,'id']);seq=cursor.fetchone()[0]
  cursor.execute('SELECT last_value,is_called FROM '+connection.ops.quote_name(seq.split('.')[-1]));sequence_before=cursor.fetchone()
  cursor.execute('DELETE FROM '+connection.ops.quote_name(table))
  cursor.execute('SELECT setval(%s,1,false)',[seq])
 Recording.objects.bulk_create([Recording(**row)for row in rows])
 with connection.cursor()as cursor:
  if rows:cursor.execute('SELECT setval(%s,%s,true)',[seq,max(r['id']for r in rows)])
  cursor.execute('SELECT last_value,is_called FROM '+connection.ops.quote_name(seq.split('.')[-1]));sequence_after=cursor.fetchone()
 print(json.dumps({'before':before,'after':list(Recording.objects.order_by('id').values()),'Recording_sequence_before':sequence_before,'Recording_sequence_after':sequence_after,'only_owned_Recording_and_sequence_reset':True,'Source_business_functions_called':False},cls=DjangoJSONEncoder))
""".replace('ALLOWED',repr(allowed)).replace('ROWS',repr(rows))
 value=f.sql(code,secret=True);save(out,value);return value

def _manual_inputs(f,root):
 rows=[]
 for mode,label in [('mark','positive-mark'),('cut','positive-cut')]:
  row=json.loads((root/'cells'/label/'SQL-before.json').read_text());assert row['id']==(1 if mode=='mark'else 2)and row['channel_id']==f.channel_id
  path='/data/positive-manual/'+mode+'/recording.mkv';row['custom_properties']={**row['custom_properties'],'file_path':path};rows.append(row)
  guest=_guest(f,path)
  f.python("import os,shutil;from pathlib import Path;p=Path("+repr(guest)+");p.parent.mkdir(parents=True,exist_ok=False);p.parent.chmod(0o755);os.chown(p.parent,65532,65532);shutil.copyfile('/shared/comskip-positive-media/master.mkv',p);p.chmod(0o644);os.chown(p,65532,65532)",secret=True)
 return rows

def _manual_cell(f,label,ident,file,mode,repeat,out,root,business,content,assets):
 p=out/'private'/label;p.mkdir(exist_ok=True);before=_row(f,ident);before_media=_media(f,file);receiver=_receiver_begin(f,label);observer=journal.observer_start(f,label,database=1)
 frame=result=response=None;watch=EDLWatch(f.case_data/Path(file).relative_to('/data').parent)
 try:
  with watch:
   response=f.api('POST',f'/api/channels/recordings/{ident}/comskip/',{});save(p/'HTTP.json',response)
   def seen():
    frames=[x for x in aggregate.frames(observer)if x['headers']['task']==TASK];assert len(frames)<=1;return frames[0]if frames else False
   frame=f.wait(seen,25,label+'-natural-root');result=f.wait(lambda:aggregate.terminal(f,frame['headers']['id']),660,label+'-terminal');_drain(f,label)
 except BaseException as error:
  save(p/'primary-failure.json',{'type':type(error).__name__,'message':str(error)[:2048],'traceback':traceback.format_exc()[-16384:]});raise
 finally:
  durable=journal.producer_observer_stop(f,observer);after=_row(f,ident);media=_media(f,file,not repeat);events=_receiver_end(f,receiver,p)
  for name,value in {'SQL-before':before,'SQL-after':after,'media-before':before_media,'media-after':media,'HTTP':response,'frame':frame,'backend':result,'journal':durable,'WS':events,'actual-generated-EDL':watch.observed,'EDL-watch-errors':watch.errors}.items():save(p/(name+'.json'),value)
 assert response['status']==200 and response['body']=={'success':True,'queued':True}
 assert frame['headers']['parent_id']is None and frame['headers']['root_id']==frame['headers']['id']
 assert [(v['task'],v['id'])for v in durable['publications']]==[(TASK,frame['headers']['id'])]
 assert result['status']=='SUCCESS'and result['result']==('already_processed'if repeat else'ok')and result['children']==[]
 if repeat:assert before==after and before_media['sha256']==media['sha256']and before_media['EDL']==media['EDL']and not _statuses(events,ident)
 else:
  assert len(watch.observed)==1 and not watch.errors
  if mode=='mark':assert media['sha256']==before_media['sha256']and media['EDL']['exists']
  else:
   assert media['full_actual_A_V_decode']['actual_exit']==0 and not media['EDL']['exists']and not media['fixed_cut_paths_remaining']
   authority=json.loads((root/'manual-cut-content.json').read_text());argv=list(authority['decoder_argv']);guest=_guest(f,file);argv[argv.index('-i')+1]=guest
   transcript=_manual_content_decode(f,p,argv)
   proof=content.compare_manual_cut(transcript,decoder_image=assets['actual_tool_image'],ffmpeg_sha256=assets['actual_tool']['sha256'],decoder_argv=argv,input_path=guest);save(p/'manual-content-proof.json',proof)
 ini='/data/comskip-positive-media/positive.ini'if mode=='mark'else'/app/docker/comskip.ini'
 proof=business.compare_business_cell(root,label,p,recording_file=file,effective_ini=ini,ini_role='custom-positive-ini'if mode=='mark'else'default-packaged-ini',recording_id_map={ident:ident});save(p/'Source-comparison.json',proof)
 return {'name':label,'status':'passed','raw_directory':str(p),'actual_task':frame['headers']['id'],'Source_comparison':proof}

def _disable_non_dvr(f):
 return f.sql("from django_celery_beat.models import PeriodicTask,PeriodicTasks;PeriodicTask.objects.exclude(task='apps.channels.tasks.run_recording').update(enabled=False);PeriodicTasks.update_changed();print(json.dumps(list(PeriodicTask.objects.values('id','name','task','enabled','date_changed')),default=str))",secret=True)

def _auto_start(f,out):
 _drain(f,'positive-manual-final');save(out/'manual-Main-settlement.json',f.stop_native())
 _reset(f,[1,2],[],out/'declared-manual-to-automatic-reset.json')
 save(out/'automatic-schedules-before-start.json',_disable_non_dvr(f))
 f._provider_environment={**getattr(f,'_provider_environment',{}),'RUST_EXPERIMENTAL_SELECTED_BEAT':'1'};f.start_native();_health(f,True,out/'automatic-Main-health.json');identity=f.native
 observer=journal.observer_start(f,'positive-auto-startup',database=1)
 try:
  save(out/'automatic-schedules-after-start.json',_disable_non_dvr(f));_drain(f,'automatic-startup')
  # Preserve the entire startup journal separately; the business monitor starts
  # only after this explicit quiet interval, without task-kind filtering.
  observed=len(aggregate.frames(observer));deadline=time.monotonic()+25
  while time.monotonic()<deadline:
   f.remaining(1);assert f.native==identity;assert len(aggregate.frames(observer))==observed,'Unsettled startup producer during automatic quiet interval';time.sleep(.25)
 finally:save(out/'complete-automatic-startup-journal.json',journal.producer_observer_stop(f,observer))
 save(out/'automatic-schedules-after-quiet.json',_disable_non_dvr(f));return identity

def _origin(f,out):
 ready='/shared/'+f.case+'/positive-origin.ready';final='/shared/'+f.case+'/positive-origin-final.json'
 f.role('positive-media-origin',['/dispatcharrpy/bin/python','-c',ORIGIN_SOURCE,'--clip','/shared/comskip-positive-media/feed.ts','--ready',ready,'--final',final,'--port','18096'])
 receipt=f.wait(lambda:json.loads((f.case_data/'positive-origin.ready').read_text())if(f.case_data/'positive-origin.ready').exists()else False,20,'positive-feed-role-ready')
 assert receipt['clip_sha256']=='133e0c68dcaae0c7d6de306983eccc2e621cda665f711915197e5fcbf334f804'and receipt['clip_bytes']==135999764
 declared=next(row for row in f.seed['channels']if row['channel_pk']==f.channel_id)
 links=f.sql("from apps.channels.models import ChannelStream;print(json.dumps(list(ChannelStream.objects.filter(channel_id="+str(f.channel_id)+").order_by('order','id').values('id','stream_id','order','stream__url','stream__m3u_account__is_active'))))")
 save(out/'actual-stream-links-before-selection.json',{'seed':declared,'links':links});assert links[0]['stream_id']==declared['stream_pk']and links[0]['order']==0 and links[0]['stream__m3u_account__is_active']is True
 value=f.sql("from apps.channels.models import Stream;s=Stream.objects.get(pk="+str(declared['stream_pk'])+");before=s.url;s.url='http://11.254.252.2:18096/live.ts';s.save(update_fields=['url']);print(json.dumps({'stream':s.pk,'before':before,'after':s.url}))")
 save(out/'declared-primary-URL-delta.json',value);save(out/'actual-pinned-feed-ready.json',receipt)
 return value

def _auto_cell(f,mode,out,root,business):
 label='natural-automatic-'+mode;p=out/'private'/label;p.mkdir();save(p/'settings-delta.json',_setting(f,mode,True,'/data/comskip-positive-media/positive.ini'));_drain(f,label+'-before')
 before=_snapshot(f);receiver=_receiver_begin(f,label);observer=journal.observer_start(f,label,database=1)
 captures=f.data/'comskip-positive-native-input-observations';captures.mkdir(mode=0o755,exist_ok=True);captures.chmod(0o755)
 existing_edls=[str(path)for path in(f.case_data/'recordings').rglob('*.edl')];prior_edls=_small_edl_proofs(existing_edls);save(p/'prior-owned-EDL-exclusion.json',{'existing_paths':existing_edls,'proofs':prior_edls,'scope':'only prior completed owned epoch outputs; verify bytes unchanged after this cell'})
 watch=EDLWatch(f.case_data/'recordings',existing_edls,captures/label);response=parent=child=parent_result=child_result=None;ident=None
 try:
  with watch:
   begin=utc()+datetime.timedelta(seconds=6);end=begin+datetime.timedelta(seconds=520)
   response=f.api('POST','/api/channels/recordings/',{'channel':f.channel_id,'start_time':begin.isoformat(),'end_time':end.isoformat(),'custom_properties':{'program':{'title':f.channel_name},'fixture_case_label':label}});save(p/'HTTP.json',response);assert response['status']==201;ident=response['body']['id']
   def frame(kind):
    found=[v for v in aggregate.frames(observer)if v['headers']['task']==kind and _body(v)[0][0]==ident];assert len(found)<=1;return found[0]if found else False
   parent=f.wait(lambda:frame(RECORD),40,label+'-natural-recording-root');child=f.wait(lambda:frame(TASK),650,label+'-natural-child');capture_native_argv(f,p,ident)
   parent_result=f.wait(lambda:aggregate.terminal(f,parent['headers']['id']),40,label+'-parent-terminal');child_result=f.wait(lambda:aggregate.terminal(f,child['headers']['id']),660,label+'-child-terminal');_drain(f,label+'-final')
 except BaseException as error:
  save(p/'primary-failure.json',{'type':type(error).__name__,'message':str(error)[:2048],'traceback':traceback.format_exc()[-16384:]});raise
 finally:
  durable=journal.producer_observer_stop(f,observer);after=_snapshot(f);events=_receiver_end(f,receiver,p);row=None if ident is None else next((v for v in after['recordings']if v['id']==ident),None)
  media=None if row is None or not row['custom_properties'].get('file_path')else _media(f,row['custom_properties']['file_path'],True)
  artwork_rows=[v for v in durable['publications']if v['task']==ARTWORK];artwork=artwork_rows[0]['raw_frame']if len(artwork_rows)==1 else None
  artwork_result=None if artwork is None else aggregate.backend(f,artwork['headers']['id'])
  input_media=None
  if len(watch.inputs)==1:
   captured=Path(next(iter(watch.inputs.values()))['path']);guest='/shared/'+str(captured.relative_to(f.data));code="import sys,json;sys.path.insert(0,'/normal-dvr-tools');from comskip_positive_epochs import guest_media;print(json.dumps(guest_media("+repr(guest)+",True)))";input_media=json.loads(f.python(code,seconds=120,secret=True))
  values={'SQL-before':before,'SQL-after':after,'HTTP':response,'recording-frame':parent,'comskip-frame':child,'recording-backend':parent_result,'comskip-backend':child_result,'artwork-frame':artwork,'artwork-backend':artwork_result,'journal':durable,'WS':events,'media-after':media,'actual-generated-EDL':watch.observed,'EDL-watch-errors':watch.errors,'actual-detector-inputs':watch.inputs,'actual-detector-input-media':input_media}
  for name,value in values.items():save(p/(name+'.json'),value)
 prior_after=_small_edl_proofs(existing_edls);save(p/'prior-owned-EDL-after.json',prior_after);assert prior_after==prior_edls,'Prior owned MARK EDL changed during CUT'
 assert response['status']==201 and row is not None and artwork is not None
 assert [(v['task'],v['id'])for v in durable['publications']]==[(ARTWORK,artwork['headers']['id']),(RECORD,parent['headers']['id']),(TASK,child['headers']['id'])]
 assert parent_result['status']=='SUCCESS'and parent_result['result']is None and parent_result['children']==[[[child['headers']['id'],None],None]]
 assert child_result['status']=='SUCCESS'and child_result['result']=='ok'and child_result['parent_id']==parent['headers']['id']and child_result['children']==[]
 assert artwork_result['status']=='SUCCESS'and artwork_result['result']=='ok'and artwork_result['children']==[]
 assert len(watch.inputs)==len(watch.observed)==1 and not watch.errors and input_media is not None
 assert input_media['sha256']==next(iter(watch.inputs.values()))['sha256']and input_media['full_actual_A_V_decode']['actual_exit']==0 and media['full_actual_A_V_decode']['actual_exit']==0
 duration=float(input_media['probe']['actual']['format']['duration']);ranges=_intervals(next(iter(watch.observed.values()))['text'],duration)
 # Fixed original feed:1s GOP +1024/48000 AAC block +1316/250000
 # packet pace +1s scheduler/stop polling. Keep all actual timing values.
 bound=1+1024/48000+1316/250000+1
 assert abs(duration-520)<=bound,'Natural fixed520s feed/capture endpoint exceeded explicit two-boundary budget'
 assert ranges and sum(e-s for s,e in ranges)>.5
 cp=row['custom_properties']['comskip'];assert cp['commercials']==len(ranges)and cp['ini_path']=='/data/comskip-positive-media/positive.ini'
 if mode=='mark':assert media['sha256']==input_media['sha256']and media['EDL']['exists']and cp['mode']=='mark'
 else:assert media['sha256']!=input_media['sha256']and not media['EDL']['exists']and not media['fixed_cut_paths_remaining']and cp['segments_kept']>0
 proof=business.compare_business_cell(root,label,p,recording_file=row['custom_properties']['file_path'],effective_ini='/data/comskip-positive-media/positive.ini',ini_role='custom-positive-ini',recording_id_map={1:ident});save(p/'Source-comparison.json',proof)
 save(p/'natural-media-protocol-proof.json',{'passed':True,'planned_capture_seconds':520,'actual_original_input_seconds':duration,'explicit_endpoint_bound_seconds':bound,'bound_terms':{'one_GOP':1,'one_AAC_block':1024/48000,'one_origin_packet':1316/250000,'scheduler_stop_poll':1},'input_retained_before_replace':True,'input_full_AV_decode':True,'output_full_AV_decode':True,'actual_EDL_ranges':ranges,'Source_vs_Native_whole_natural_content_equality_claimed':False,'exact_feed_SHA':'133e0c68dcaae0c7d6de306983eccc2e621cda665f711915197e5fcbf334f804'})
 return {'name':label,'status':'passed','raw_directory':str(p),'recording_id':ident,'actual_parent':parent['headers']['id'],'actual_child':child['headers']['id'],'Source_comparison':proof}

def run(f):
 assert f.backend=='Native'and f.case=='comskip-manual'
 refs=f.binding['retained_source_positive_comskip'];root=Path(refs['qualification']['path']).parent
 for name,ref in refs.items():
  if isinstance(ref,dict)and 'path'in ref:assert hashlib.sha256(Path(ref['path']).read_bytes()).hexdigest()==ref['sha256'],name
 q=json.loads((root/'qualification.json').read_text());assert q['actual_Source_business_leaves']==6 and q['manual_mark_repeat_cut_repeat_qualified']and q['natural_CUT_actual_controller_exit']==0 and not q['whole_six_cell_controller_pass_exists']
 business=_load(refs['business'],'positive_Source_business');content=_load(refs['content'],'positive_Source_content');asset_module=_load(refs['assets'],'positive_exact_assets')
 out=f.output/'comskip-positive-epochs';out.mkdir();(out/'private').mkdir(mode=0o700);report={'status':'running','cases':[],'Source_business_replayed':False,'old_manual3_preserved':True,'full_milestone_passed':False,'performance_verified':False};save(out/'report.json',report)
 try:
  _drain(f,'old-manual3');save(out/'old-manual3-Main-settlement.json',f.stop_native())
  assets=asset_module.prepare_in_existing_role(lambda argv,timeout:f.command(argv,seconds=timeout,secret=True),f.compose,f.data);save(out/'actual-assets.json',assets)
  # Main receives its own tiny INI copy; master/feed remain immutable originals.
  f.python("import os,shutil;from pathlib import Path;p=Path('/shared/"+f.case+"/comskip-positive-media');p.mkdir();p.chmod(0o755);os.chown(p,65532,65532);q=p/'positive.ini';shutil.copyfile('/shared/comskip-positive-media/positive.ini',q);q.chmod(0o644);os.chown(q,65532,65532)")
  rows=_manual_inputs(f,root);owned=[f._comskip_rows['real'],f._comskip_rows['no_file']];_reset(f,owned,rows,out/'declared-old3-to-positive-manual-reset.json')
  save(out/'manual-settings-initial.json',_setting(f,'mark',False,'/data/comskip-positive-media/positive.ini'));save(out/'manual-disabled-schedules.json',_disable_non_dvr(f));f._provider_environment={**getattr(f,'_provider_environment',{}),'RUST_EXPERIMENTAL_SELECTED_BEAT':'0'};f.start_native();_health(f,False,out/'positive-manual-Main-health.json')
  for label,ident,mode,repeat in [('positive-mark',1,'mark',False),('mark-repeat',1,'mark',True),('positive-cut',2,'cut',False),('cut-repeat',2,'cut',True)]:
   p=out/'private'/label;p.mkdir(exist_ok=True);save(p/'settings-delta.json',_setting(f,mode,False,'/data/comskip-positive-media/positive.ini'if mode=='mark'else''))
   report['cases'].append(_manual_cell(f,label,ident,'/data/positive-manual/'+mode+'/recording.mkv',mode,repeat,out,root,business,content,assets));save(out/'report.json',report)
  identity=_auto_start(f,out);_origin(f,out)
  for mode in ['mark','cut']:
   assert f.native==identity;report['cases'].append(_auto_cell(f,mode,out,root,business));save(out/'report.json',report)
  save(out/'positive-origin-stop.json',f.role('positive-media-origin',[],stop=True));save(out/'positive-origin-final.json',json.loads((f.case_data/'positive-origin-final.json').read_text()));report['status']='passed'
 except BaseException as error:
  save(out/'private/failure.json',{'type':type(error).__name__,'message':str(error)[:2048],'traceback':traceback.format_exc()[-16384:]});report.update(status='failed',failure_type=type(error).__name__)
 finally:
  report['epoch_proofs']=['old-manual3-Main-settlement.json','declared-old3-to-positive-manual-reset.json','positive-manual-Main-health.json','manual-Main-settlement.json','declared-manual-to-automatic-reset.json','automatic-Main-health.json','complete-automatic-startup-journal.json'];save(out/'report.json',report)
 return report

def capture_native_argv(f,p,ident):
 """One bounded actual Native PID-namespace read, no long-lived guest owner."""
 script=r'''import os,time,json,hashlib
from pathlib import Path
until=time.monotonic()+3;observed={}
while time.monotonic()<until:
 rows=list(Path('/proc').glob('[0-9]*/stat'));assert len(rows)<=256
 for stat in rows:
  try:
   before=stat.read_text().rsplit(')',1)[1].split();pid=int(stat.parent.name)
   raw=(stat.parent/'cmdline').read_bytes();assert len(raw)<=16384
   argv=[x.decode('utf8')for x in raw.split(b'\0')if x]
   candidates=[i for i,arg in enumerate(argv)if arg in('/opt/dispatcharr-comskip/bin/comskip','/usr/local/bin/comskip')]
   if len(candidates)!=1:continue
   logical=argv[candidates[0]:];uids=next(line for line in(stat.parent/'status').read_text().splitlines()if line.startswith('Uid:')).split()[1:]
   after=stat.read_text().rsplit(')',1)[1].split()
   if before[19]!=after[19]:continue
   binary=Path(logical[0]);digest=hashlib.sha256(binary.read_bytes()).hexdigest()
   observed[str(pid)+':'+before[19]]={'PID':pid,'starttime':before[19],'actual_full_argv':argv,'argv':logical,'UIDs':uids,'stable_generation':True,'Comskip_ELF_sha256':digest,'observed_epoch':time.time()}
  except(FileNotFoundError,ProcessLookupError,PermissionError):continue
 time.sleep(.02)
print(json.dumps({'actual_UID':os.getuid(),'observed':observed,'bounded_read_seconds':3,'no_child_spawned':True}))
'''
 value=json.loads(f.command([f.docker,'exec',f.native,'/usr/local/bin/dispatcharr-python','-I','-B','-c',script],seconds=12,secret=True));save(p/'actual-Comskip-child-argv.json',{'actual_observation':value,'actual_native_container':f.native,'physical_exec_receipt':f.cli[-1]})
 file=_row(f,ident)['custom_properties']['file_path']
 assert value['actual_UID']==65532 and len(value['observed'])==1,'Actual Native Comskip argv missed or foreign process observed'
 actual=next(iter(value['observed'].values()));assert actual['UIDs']==['65532']*4 and actual['Comskip_ELF_sha256']=='1452696c945a64f5351977143ecad3d384e7036479ba5d85ed9c928d6e20d460'
 assert actual['argv'][-1]==file and [v for v in actual['argv']if v.startswith('--ini=')]==['--ini=/data/comskip-positive-media/positive.ini']

def _manual_content_decode(f,p,argv):
 """Keep full framehash out of Fixture.command's unchanged1MiB pipe budget."""
 base=f.data/'positive-decoder-proof';base.mkdir(mode=0o700,exist_ok=True)
 owned=base/uuid.uuid4().hex;owned.mkdir(mode=0o700)
 host=owned/'complete.framehash';error=owned/'complete.stderr'
 guest='/shared/'+str(host.relative_to(f.data));guest_error='/shared/'+str(error.relative_to(f.data))
 code="import subprocess,resource,os,signal,json;from pathlib import Path;argv="+repr(argv)+";out=Path("+repr(guest)+");err=Path("+repr(guest_error)+");\nwith out.open('xb')as stdout,err.open('xb')as stderr:\n process=subprocess.Popen(argv,stdin=subprocess.DEVNULL,stdout=stdout,stderr=stderr,start_new_session=True,preexec_fn=lambda:resource.setrlimit(resource.RLIMIT_FSIZE,(8388608,8388608)))\n try:status=process.wait(timeout=180)\n except subprocess.TimeoutExpired:os.killpg(process.pid,signal.SIGKILL);process.wait();raise\nreceipt={'actual_exit':status,'actual_wait_completed':True,'stdout_EOF':stdout.closed,'stderr_EOF':stderr.closed,'stdout_bytes':out.stat().st_size,'stderr_bytes':err.stat().st_size,'actual_argv':argv,'positive_PID':process.pid,'RLIMIT_FSIZE_bytes':8388608};print(json.dumps(receipt));assert status==0 and err.stat().st_size==0 and out.stat().st_size<=8388608"
 receipt=json.loads(f.python(code,seconds=200,secret=True));save(p/'manual-content-decoder-receipt.json',receipt)
 assert host.stat().st_size==receipt['stdout_bytes']<=8388608
 target=p/'full-decoded-all-streams.framehash';shutil.copyfile(host,target)
 return target


def _small_edl_proofs(paths):
 result={}
 for name in paths:
  path=Path(name)
  with path.open('rb')as source:raw=source.read(1048577)
  assert len(raw)<=1048576
  result[name]={'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}
 return result
