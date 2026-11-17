"""Existing-role adapter over byte-exact Source6-calibrated primitives.
No Compose/coordinator/flock/daemon or application call exists in this entrypoint.
"""
import argparse,fcntl,importlib.util,json,os,signal,types
from pathlib import Path
HERE=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('calibrated_cut_reference',HERE/'guest.py');ref=importlib.util.module_from_spec(spec);spec.loader.exec_module(ref)

def decoder(input_path,authority):
 assert authority['ffmpeg_sha256']==ref.EXPECTED_FFMPEG
 argv=list(authority['decoder_argv']);old=argv[argv.index('-i')+1];assert argv.count(old)==1 and '-copyts'in argv and argv[argv.index('-c:a')+1]=='pcm_f32le'and argv[argv.index('-fps_mode')+1]=='passthrough'
 argv[argv.index('-i')+1]=str(input_path);return argv

def evaluate(original,actual,edl,work,evidence,authority,seconds):
 import shutil,itertools
 original=Path(original).resolve(strict=True);actual=Path(actual).resolve(strict=True);edl=Path(edl).resolve(strict=True);work=Path(work);evidence=Path(evidence)
 assert original!=actual and not work.exists()and not evidence.exists();work.mkdir(mode=0o700);evidence.mkdir(mode=0o755);evidence.chmod(0o755)
 assert all(not path.is_relative_to(work.resolve())and not path.is_relative_to(evidence.resolve())for path in(original,actual,edl))
 # Evidence contains no credentials or media; host custody requires readability
 # even when the guest image starts with a restrictive inherited umask.
 previous_umask=os.umask(0o022)
 result={'passed':False,'existing_baseline_role_only':True,'new_containers_or_coordinators_or_locks':0,'production_or_Source_application_tasks_called':0,'container_byte_equivalence_claimed':False,'readonly_mount_claimed':False,'per_file_scratch_bound_bytes':128*1024*1024,'scratch_post_command_stop_threshold_bytes':256*1024*1024,'scratch_worst_case_bound_bytes':384*1024*1024};commands=ref.OwnedCommands(evidence,seconds);fds=[];failure=None;before={};stdlib=ref.subprocess
 for sig in(signal.SIGTERM,signal.SIGINT):signal.signal(sig,commands.cancel)
 try:
  # The existing /shared mount is RW, so no false RO-mount claim: input handles
  # are actually O_RDONLY/NOFOLLOW, only these FDs are inherited by tools, and
  # all source bytes/stat identities must remain exact before/after.
  mapping={}
  for name,path in [('original',original),('actual',actual)]:
   fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW);fds.append(fd);assert fcntl.fcntl(fd,fcntl.F_GETFL)&os.O_ACCMODE==os.O_RDONLY
   s=os.fstat(fd);assert 0<s.st_size<=ref.MAX_INPUT;before[name]={'path':str(path),'readonly_fd':fd,'stat':[s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns,s.st_ctime_ns],**ref.digest(path)};mapping[str(path)]='/proc/self/fd/'+str(fd)
  assert edl.stat().st_size<=1024*1024;before['edl']={'path':str(edl),**ref.digest(edl)}
  def popen(argv,**kwargs):
   assert 'pass_fds'not in kwargs
   for fd in fds:os.lseek(fd,0,os.SEEK_SET)
   argv[:]=[mapping.get(str(arg),arg)for arg in argv]
   return stdlib.Popen(argv,pass_fds=tuple(fds),**kwargs)
  # Explicit FD/path transport adapter, scoped to this isolated module/process;
  # the calibrated command owner, deadlines, pipes and wait logic stay exact.
  ref.subprocess=types.SimpleNamespace(Popen=popen,PIPE=stdlib.PIPE,DEVNULL=stdlib.DEVNULL,TimeoutExpired=stdlib.TimeoutExpired)
  tool=Path(shutil.which('ffmpeg')).resolve();assert ref.digest(tool)['sha256']==ref.EXPECTED_FFMPEG
  probe_args=['ffprobe','-v','error','-show_entries','format=duration:stream=index,codec_type,codec_name,width,height,sample_rate,channels','-of','json']
  probe=json.loads(commands.run([*probe_args,str(original)],'input-probe',20));assert [(s['codec_type'],s['codec_name'])for s in probe['streams']]==[('video','h264'),('audio','aac')]and probe['streams'][1]['sample_rate']=='48000'
  commercials,keep=ref.keep_intervals(edl.read_text(),float(probe['format']['duration']));parts=[]
  for i,(s,e)in enumerate(keep):
   path=work/f'segment_{i:03d}.mkv';commands.run(['ffmpeg','-y','-ss',f'{s:.3f}','-i',str(original),'-t',f'{max(0,e-s):.3f}','-c','copy','-avoid_negative_ts','1',str(path)],f'segment-{i:03d}',90);assert sum(x.stat().st_size for x in work.iterdir())<=256*1024*1024;parts.append(path)
  listing=work/'concat_list.txt';listing.write_text(''.join("file '"+str(path)+"'\n"for path in parts));reference=work/'reference.cut.mkv';commands.run(['ffmpeg','-y','-f','concat','-safe','0','-i',str(listing),'-c','copy',str(reference)],'concat',90);assert sum(x.stat().st_size for x in work.iterdir())<=256*1024*1024
  probes={label:json.loads(commands.run([*probe_args,str(path)],label+'-probe',20))for label,path in [('reference',reference),('actual',actual)]};result.update(input_probe=probe,output_probes=probes,input_custody=before,keep_intervals=keep,commercials=commercials,reference_raw=ref.digest(reference),actual_raw=ref.digest(actual))
  assert probes['reference']['streams']==probes['actual']['streams']
  authority=json.loads(Path(authority).read_text())
  for label,path in [('reference',reference),('actual',actual)]:commands.run(decoder(path,authority),label+'-decode',180,evidence/(label+'.framehash'))
  framehashes={label:ref.framehash(evidence/(label+'.framehash'))for label in('reference','actual')};result['framehashes']=framehashes
  if framehashes['reference']!=framehashes['actual']:
   with (evidence/'reference.framehash').open()as left,(evidence/'actual.framehash').open()as right:
    for line,(a,b)in enumerate(itertools.zip_longest(left,right),1):
     if a!=b:result['first_difference']={'line':line,'reference':a,'actual':b};break
   raise AssertionError('FullDecodedAVContentOrTimeMismatch')
  for name,path in [('original',original),('actual',actual)]:
   s=path.stat();after={'path':str(path),'readonly_fd':before[name]['readonly_fd'],'stat':[s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns,s.st_ctime_ns],**ref.digest(path)};assert after==before[name]
  assert ref.digest(edl)=={k:before['edl'][k]for k in('sha256','bytes')}
  result.update(passed=True,all_decoded_AV_content_and_time_exact=True,source_files_unchanged=True,input_file_handles_O_RDONLY=True,complete_transcript_bound_bytes=ref.MAX_TRANSCRIPT)
 except BaseException as exc:failure={'type':type(exc).__name__,'message':str(exc)[:2000]}
 finally:
  if commands.active:commands.settle(commands.active)
  for fd in fds:os.close(fd)
  ref.subprocess=stdlib
  result.update(failure=failure,all_input_handles_closed=True,command_count=len(commands.rows));(evidence/'result.json').write_text(json.dumps(result,indent=2)+'\n')
  for path in evidence.iterdir():
   assert path.is_file()and not path.is_symlink();path.chmod(0o644)
  os.umask(previous_umask)
 return result

def main():
 p=argparse.ArgumentParser();p.add_argument('--original',type=Path,required=True);p.add_argument('--actual',type=Path,required=True);p.add_argument('--edl',type=Path,required=True);p.add_argument('--work',type=Path,required=True);p.add_argument('--evidence',type=Path,required=True);p.add_argument('--authority',type=Path,required=True);p.add_argument('--seconds',type=int,default=300);a=p.parse_args();assert 1<=a.seconds<=300
 result=evaluate(a.original,a.actual,a.edl,a.work,a.evidence,a.authority,a.seconds);print(json.dumps({'passed':result['passed'],'failure':result['failure'],'result_sha256':ref.digest(a.evidence/'result.json')['sha256']}));return 0 if result['passed']else 1
if __name__=='__main__':raise SystemExit(main())
