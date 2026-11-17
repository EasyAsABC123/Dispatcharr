"""Bounded tool-only Source CUT reference and full decoded A/V comparison.
No Django, application jobs, booking, Comskip detector or network imports.
"""
import argparse,hashlib,json,math,os,resource,selectors,shutil,signal,subprocess,time
from pathlib import Path
EXPECTED_FFMPEG='5831afcbcb9dc46107b633dc09f276794dc094b70aacfda6096eeceb83f013f0'
MAX_TRANSCRIPT=8*1024*1024;MAX_PIPE=256*1024;MAX_INPUT=64*1024*1024

def digest(path):
 h=hashlib.sha256();count=0
 with Path(path).open('rb')as source:
  for part in iter(lambda:source.read(65536),b''):h.update(part);count+=len(part)
 return {'sha256':h.hexdigest(),'bytes':count}

def keep_intervals(text,duration):
 assert type(duration)is float and math.isfinite(duration)and 0<duration<=600
 assert len(text.encode())<=1024*1024
 commercials=[]
 for line in text.splitlines():
  parts=line.strip().split()
  if len(parts)>=2:
   try:s=float(parts[0]);e=float(parts[1])
   except ValueError:continue
   # Refuse non-finite/wider fixtures; do not change Source's admitted arithmetic.
   assert math.isfinite(s)and math.isfinite(e)
   commercials.append((max(0.0,s),min(duration,e)))
 commercials.sort();keep=[];cur=0.0
 for s,e in commercials:
  if s>cur:keep.append((cur,max(cur,s)))
  cur=max(cur,e)
 if cur<duration:keep.append((cur,duration))
 assert commercials and sum(e-s for s,e in commercials)>.5 and 1<=len(keep)<=16
 return commercials,[(s,e)for s,e in keep if max(0.0,e-s)>.01]

def generation(pid):
 try:return Path('/proc',str(pid),'stat').read_text().rsplit(')',1)[1].split()[19]
 except FileNotFoundError:return None

def group_absent(pgid):
 try:os.killpg(pgid,0)
 except ProcessLookupError:return True
 return False

class OwnedCommands:
 def __init__(self,evidence,seconds):self.evidence=Path(evidence);self.deadline=time.monotonic()+seconds;self.rows=[];self.active=None;self.cancelled=False
 def cancel(self,*_):self.cancelled=True
 def settle(self,p):
  if p.poll()is None:
   try:os.killpg(p.pid,signal.SIGTERM)
   except ProcessLookupError:pass
   try:p.wait(timeout=2)
   except subprocess.TimeoutExpired:
    try:os.killpg(p.pid,signal.SIGKILL)
    except ProcessLookupError:pass
    p.wait(timeout=2)
 def run(self,argv,label,seconds=90,transcript=None):
  assert self.active is None and not self.cancelled
  remaining=self.deadline-time.monotonic();assert remaining>0
  output=self.evidence/(label+'.stdout');error=self.evidence/(label+'.stderr');limit=MAX_TRANSCRIPT if transcript else 128*1024*1024
  def bounds():
   resource.setrlimit(resource.RLIMIT_FSIZE,(limit,limit))
   pid=os.getpid();(self.evidence/(label+'.child-start.json')).write_text(json.dumps({'PID':pid,'starttime':generation(pid),'process_group':os.getpgrp(),'PID_namespace':os.readlink('/proc/self/ns/pid'),'captured_before_exec':True})+'\n')
  file=None;p=None;sel=selectors.DefaultSelector();buffers={'stdout':bytearray(),'stderr':bytearray()};eofs=set();failure=None;started=time.monotonic();row={'argv':argv,'label':label,'seconds_bound':min(seconds,remaining),'stdout_transcript':str(transcript)if transcript else None}
  try:
   if transcript:file=Path(transcript).open('xb')
   p=subprocess.Popen(argv,stdin=subprocess.DEVNULL,stdout=file if file else subprocess.PIPE,stderr=subprocess.PIPE,start_new_session=True,preexec_fn=bounds);self.active=p;child=json.loads((self.evidence/(label+'.child-start.json')).read_text());assert child['PID']==p.pid==child['process_group']and child['starttime'];row.update(child)
   if p.stdout:sel.register(p.stdout,selectors.EVENT_READ,'stdout')
   sel.register(p.stderr,selectors.EVENT_READ,'stderr');until=started+min(seconds,remaining)
   while sel.get_map():
    if self.cancelled or time.monotonic()>=until:failure='Cancelled'if self.cancelled else'Timeout';break
    for key,_ in sel.select(timeout=min(.1,max(0,until-time.monotonic()))):
     data=os.read(key.fileobj.fileno(),16384)
     if not data:sel.unregister(key.fileobj);eofs.add(key.data);continue
     buffer=buffers[key.data]
     if len(buffer)+len(data)>MAX_PIPE:failure='BoundedPipeOverflow';break
     buffer.extend(data)
    if failure:break
   if failure:self.settle(p)
   else:
    try:p.wait(timeout=max(.001,until-time.monotonic()))
    except subprocess.TimeoutExpired:failure='WaitTimeout';self.settle(p)
  except BaseException as exc:failure=type(exc).__name__;raise
  finally:
   if p is not None:
    self.settle(p)
    # Drain the remaining bounded owned pipe bytes after the child is reaped.
    end=time.monotonic()+2
    while sel.get_map()and time.monotonic()<end:
     for key,_ in sel.select(timeout=.05):
      data=os.read(key.fileobj.fileno(),16384)
      if not data:sel.unregister(key.fileobj);eofs.add(key.data)
      elif len(buffers[key.data])+len(data)<=MAX_PIPE:buffers[key.data].extend(data)
      else:failure=failure or'BoundedPipeOverflow'
    row.update(actual_exit=p.returncode,wait_completed=p.poll()is not None,original_generation_absent=generation(p.pid)!=row['starttime'],process_group_settled=group_absent(p.pid),stdout_EOF=None if file else'stdout'in eofs,stderr_EOF='stderr'in eofs)
   if file:file.close();row['stdout_direct_file_closed']=file.closed;row['stdout_sink']='bounded_direct_file'
   for stream in(p.stdout,p.stderr)if p else[]:
    if stream:stream.close()
   sel.close();output.write_bytes(buffers['stdout']);error.write_bytes(buffers['stderr']);row.update(failure=failure,elapsed_seconds=time.monotonic()-started,stdout=digest(output),stderr=digest(error));self.rows.append(row);self.active=None;(self.evidence/'commands.json').write_text(json.dumps(self.rows,indent=2)+'\n')
  assert failure is None and row['actual_exit']==0 and(row.get('stdout_direct_file_closed')is True or row['stdout_EOF']is True)and row['stderr_EOF']and row['process_group_settled']and row['original_generation_absent'],row
  if transcript:assert Path(transcript).stat().st_size<=MAX_TRANSCRIPT
  return bytes(buffers['stdout'])

def decode_argv(input_path):
 # Reuse the exact already-qualified recipe, including copyts, passthrough and
 # explicit encoder time bases. Only its one input pathname may differ.
 authority=json.loads(Path('/tool/decoder-authority.json').read_text())
 assert authority['ffmpeg_sha256']==EXPECTED_FFMPEG
 argv=list(authority['decoder_argv']);old=argv[argv.index('-i')+1]
 assert argv.count(old)==1 and argv.count('pcm_f32le')==1 and '-copyts'in argv and argv[argv.index('-fps_mode')+1]=='passthrough'and argv[argv.index('-avoid_negative_ts')+1]=='disabled'
 argv[argv.index('-i')+1]=str(input_path)
 return argv

def framehash(path):
 header=[];counts={0:0,1:0}
 with Path(path).open('r',encoding='ascii')as source:
  for line in source:
   if line.startswith('#'):header.append(line.rstrip('\n'));continue
   if not line.strip():continue
   fields=[v.strip()for v in line.split(',')];assert len(fields)==6 and fields[0]in('0','1')and all(v.lstrip('-').isdigit()for v in fields[1:5])and len(fields[5])==64 and all(c in'0123456789abcdef'for c in fields[5]);counts[int(fields[0])]+=1
 assert counts[0]>0 and counts[1]>0 and '#codec_id 0: rawvideo'in header and '#codec_id 1: pcm_f32le'in header
 return {'file':digest(path),'headers':header,'record_counts':counts}

def main():
 p=argparse.ArgumentParser();p.add_argument('--seconds',type=int,default=300);a=p.parse_args();assert 1<=a.seconds<=300
 evidence=Path('/evidence');work=Path('/work');commands=OwnedCommands(evidence,a.seconds)
 for sig in(signal.SIGTERM,signal.SIGINT):signal.signal(sig,commands.cancel)
 result={'passed':False,'PID_namespace':os.readlink('/proc/self/ns/pid'),'Source_apps_jobs_bookings_or_Comskip_detector_started':False,'performance_claim':False,'container_byte_equivalence_claimed':False};failure=None
 try:
  tool=Path(shutil.which('ffmpeg')).resolve();assert digest(tool)['sha256']==EXPECTED_FFMPEG
  result['tool']={'ffmpeg_path':str(tool),**digest(tool),'ffprobe_path':shutil.which('ffprobe'),'version':commands.run(['ffmpeg','-version'],'tool-version',15).decode().splitlines()[0]}
  before={str(path):{'stat':list((lambda s:(s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns,s.st_ctime_ns))(path.stat())),**digest(path)}for path in[Path('/inputs/original.mkv'),Path('/inputs/actual.mkv'),Path('/inputs/generated.edl')]}
  assert 0<before['/inputs/original.mkv']['bytes']<=MAX_INPUT and 0<before['/inputs/actual.mkv']['bytes']<=MAX_INPUT and before['/inputs/generated.edl']['bytes']<=1024*1024
  probe=json.loads(commands.run(['ffprobe','-v','error','-show_entries','format=duration:stream=index,codec_type,codec_name,width,height,sample_rate,channels','-of','json','/inputs/original.mkv'],'input-probe',20));assert [(s['codec_type'],s['codec_name'])for s in probe['streams']]==[('video','h264'),('audio','aac')]
  result['input_probe']=probe
  assert probe['streams'][1]['sample_rate']=='48000'
  duration=float(probe['format']['duration']);commercials,keep=keep_intervals(Path('/inputs/generated.edl').read_text(),duration);parts=[];intervals=[]
  for i,(s,e)in enumerate(keep):
   path=work/f'segment_{i:03d}.mkv';argv=['ffmpeg','-y','-ss',f'{s:.3f}','-i','/inputs/original.mkv','-t',f'{max(0,e-s):.3f}','-c','copy','-avoid_negative_ts','1',str(path)];commands.run(argv,f'segment-{i:03d}',90);assert path.stat().st_size<=128*1024*1024;parts.append(path);intervals.append({'start':s,'end':e,'argv_start':f'{s:.3f}','argv_duration':f'{max(0,e-s):.3f}'})
  listing=work/'concat_list.txt';listing.write_text(''.join("file '"+str(path)+"'\n"for path in parts));reference=work/'reference.cut.mkv';commands.run(['ffmpeg','-y','-f','concat','-safe','0','-i',str(listing),'-c','copy',str(reference)],'concat',90);assert reference.stat().st_size<=128*1024*1024
  reference_probe=json.loads(commands.run(['ffprobe','-v','error','-show_entries','format=duration:stream=index,codec_type,codec_name,width,height,sample_rate,channels','-of','json',str(reference)],'reference-probe',20));actual_probe=json.loads(commands.run(['ffprobe','-v','error','-show_entries','format=duration:stream=index,codec_type,codec_name,width,height,sample_rate,channels','-of','json','/inputs/actual.mkv'],'actual-probe',20));result.update(reference_probe=reference_probe,actual_probe=actual_probe,reference_raw=digest(reference),actual_raw=digest(Path('/inputs/actual.mkv')),keep_intervals=intervals,commercials=commercials,input_custody=before);assert actual_probe['streams']==reference_probe['streams']
  for label,path in[('reference',reference),('actual',Path('/inputs/actual.mkv'))]:commands.run(decode_argv(path),label+'-decode',180,evidence/(label+'.framehash'))
  reference_hash=framehash(evidence/'reference.framehash');actual_hash=framehash(evidence/'actual.framehash');result.update(reference_framehash=reference_hash,actual_framehash=actual_hash)
  if reference_hash!=actual_hash:
   import itertools
   with (evidence/'reference.framehash').open('r')as left,(evidence/'actual.framehash').open('r')as right:
    for number,(a,b)in enumerate(itertools.zip_longest(left,right),1):
     if a!=b:result['first_framehash_difference']={'line':number,'reference':None if a is None else a[:512],'actual':None if b is None else b[:512]};break
   raise AssertionError('FullDecodedAVContentOrTimeMismatch')
  for name,value in before.items():
   path=Path(name);after={'stat':list((lambda s:(s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns,s.st_ctime_ns))(path.stat())),**digest(path)};assert after==value,'ReadOnlyInputChanged:'+name
  result.update(passed=True,input_probe=probe,reference_probe=reference_probe,actual_probe=actual_probe,commercials=commercials,keep_intervals=intervals,input_custody=before,reference_raw=digest(reference),actual_raw=digest(Path('/inputs/actual.mkv')),reference_framehash=reference_hash,actual_framehash=actual_hash,all_streams_and_full_timing_exact=True,all_source_files_unchanged=True,scratch_only_transform=True)
 except BaseException as exc:failure={'type':type(exc).__name__,'message':str(exc)[:2000]}
 finally:
  if commands.active:commands.settle(commands.active)
  result.update(failure=failure,commands=len(commands.rows),scratch_files=[p.name for p in work.iterdir()]);(evidence/'result.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({'passed':result['passed'],'failure':failure,'commands':len(commands.rows)}))
 return 0 if result['passed']else 1
if __name__=='__main__':raise SystemExit(main())
