#!/usr/bin/env python3
"""Root-authorized retained-data codec review; never runs app/task/recording."""
import hashlib,json,os,re,stat,sys,time,uuid
from pathlib import Path
ROOT=Path('/home/justin/github/Dispatcharr');OUT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT/'artifacts/rust/m4-vod-spa-native-main-prep-3'))
import run as ENV
from bounded_process import Collected
from bounded_stop import collect as stop_collect
IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
SOURCE=ROOT/'artifacts/rust/clocked-recordings-native-physical-4-1/retained-physical-recordings/TV_Shows/Owned Parented Artwork/S01E01.mkv'
EXPECTED='a65b9199bbc047f4009a9ddbd3cac6b44e45ffbd705f53a491f12bb7b68928ef'
def save(name,v):(OUT/name).write_text(json.dumps(v,indent=2,allow_nan=False)+'\n')
def need(v,k):
 if not v:raise ValueError(k)
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def actual_start(pid):return ENV.start(pid)
def main():
 need(not(OUT/'execution.json').exists(),'FreshOwnedOutput');began=time.monotonic();work_deadline=began+45;hard_deadline=began+60;project='dispatcharr-dvr4-codec-'+uuid.uuid4().hex[:10];meta=json.loads((ENV.HERE/'tooling-inputs.json').read_text());env=ENV.controlled_environment(meta);docker=meta['docker']['path'];commands=[];roles={};current=None;serial=0;first=None;cleanup=[];jobs=[];in_cleanup=False
 result={'passed':False,'application_task_recording_replays':0,'resource_or_performance_measurement':False,'Source_duration_equality_or_causal_gain_claim':False,'pinned_image':IMAGE,'source':str(SOURCE.relative_to(ROOT)),'expected_input_sha256':EXPECTED,'commands':commands,'project':project,'budgets':{'overall_seconds':60,'work_seconds':45,'cleanup_reserved_seconds':15,'per_stream_inflight_bytes':1048576,'memory_bytes':134217728,'pids':64},'sidecars':roles}
 def remaining():return max(.01,(hard_deadline if in_cleanup else work_deadline)-time.monotonic())
 def stop_owned():
  if current:
   p=OUT/('owned-stop-'+str(serial));p.mkdir(exist_ok=True);stop_collect([docker,'stop','--time','2',current],env,p,{},limit=1048576,timeout=min(5,remaining()))
 def command(argv,seconds=10):
  nonlocal serial
  need(remaining()>.01,'OverallWorkOrCleanupBudget');serial+=1;tag='command-'+str(serial).zfill(3);stdout=OUT/(tag+'.stdout');stderr=OUT/(tag+'.stderr');t=time.time();job=Collected(argv,env,1048576,stop_owned,stdout_path=stdout,stderr_path=stderr);identity=actual_start(job.process.pid);need(identity is not None,'ActualCLIStartIdentity');jobs.append(job)
  try:raw,_=ENV.settled_output(job,min(seconds,remaining()),(0,),stdout);return raw
  finally:
   row={'argv':argv,'cwd':str(ROOT),'PID':job.process.pid,'starttime':identity,'began_epoch':t,'completed_epoch':time.time(),'actual_exit':job.process.poll(),'collection_invalid':job.invalid,'physically_settled':job.physically_settled,'pipe_threads_settled':not any(t.is_alive()for t in job.threads),'stdout_sha256':sha(stdout),'stderr_sha256':sha(stderr)};commands.append(row);save(tag+'.json',row);save('execution.json',result)
 def inspect(cid):return json.loads(command([docker,'inspect',cid]))[0]
 try:
  fd=os.open(SOURCE,os.O_RDONLY|os.O_NOFOLLOW)
  try:
   s=os.fstat(fd);need(stat.S_ISREG(s.st_mode)and s.st_size==980283,'ExpectedRegularRetainedMKV');raw=b''
   while len(raw)<=980283:
    b=os.read(fd,min(65536,980284-len(raw)))
    if not b:break
    raw+=b
   need(len(raw)==980283 and hashlib.sha256(raw).hexdigest()==EXPECTED,'ActualRetainedHashBeforeCodec')
  finally:os.close(fd)
  copy=OUT/'owned-input.mkv';fd=os.open(copy,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o444)
  try:
   view=memoryview(raw)
   while view:
    n=os.write(fd,view);need(n>0,'CopyWriteProgress');view=view[n:]
   os.fsync(fd)
  finally:os.close(fd)
  copy.chmod(0o444);need(sha(copy)==EXPECTED,'ByteIdentical0444Copy');result['input']={'bytes':len(raw),'sha256':EXPECTED,'copy_mode':'0444','original_device':s.st_dev,'original_inode':s.st_ino,'original_mode':oct(s.st_mode&0o777),'single_bind_copy':True}
  info=json.loads(command([docker,'image','inspect',IMAGE]))[0];need(IMAGE in info['RepoDigests'],'ExactPinnedLocalRepoDigest');result['actual_image_id']=info['Id'];save('image-inspect-private.json',info)
  need(not command([docker,'container','ls','--all','--no-trunc','--filter','label=com.docker.compose.project='+project,'--format','{{.ID}}']).strip(),'FreshExactProjectAbsent')
  for role,args in [('ffprobe',['-v','error','-show_format','-show_streams','-of','json','/tmp/owned-recording.mkv']),('ffmpeg',['-threads','1','-filter_threads','1','-v','error','-i','/tmp/owned-recording.mkv','-map','0:v','-f','null','-'])]:
   name=project+'-'+role;argv=[docker,'create','--pull','never','--name',name,'--label','com.docker.compose.project='+project,'--label','com.docker.compose.service='+role,'--network','none','--read-only','--user','65532:65532','--cap-drop','ALL','--security-opt','no-new-privileges','--memory','128m','--pids-limit','64','--mount','type=bind,src='+str(copy)+',dst=/tmp/owned-recording.mkv,readonly','--entrypoint',role,IMAGE]+args
   cid=command(argv).decode().strip();need(re.fullmatch('[a-f0-9]{64}',cid)is not None,'ActualFullCID');current=cid;roles[role]={'CID':cid,'name':name,'args':args};v=inspect(cid);save(role+'-created-inspect-private.json',v)
   need(v['Config']['Image']==IMAGE and v['Image']==info['Id']and v['Config']['User']=='65532:65532'and v['Config']['Labels']['com.docker.compose.project']==project,'ExactOwnedImageUIDLabels')
   hc=v['HostConfig'];need(hc['NetworkMode']=='none'and hc['ReadonlyRootfs']is True and'ALL'in hc['CapDrop']and'no-new-privileges'in hc['SecurityOpt']and hc['Memory']==134217728 and hc['PidsLimit']==64,'RequestedCodecIsolation')
   mounts=v['Mounts'];need(len(mounts)==1 and mounts[0]['Type']=='bind'and mounts[0]['Source']==str(copy)and mounts[0]['Destination']=='/tmp/owned-recording.mkv'and mounts[0]['RW']is False,'SingleReadonlyOwnedCopyMountOnly')
   t=time.monotonic();data=command([docker,'start','--attach',cid],seconds=60);elapsed=time.monotonic()-t;roles[role]['attached_command']=serial;roles[role]['elapsed_seconds']=elapsed
   # Attached CLI waits for actual codec PID1 exit. Post-exit inspect and Docker wait bind that outcome.
   terminal=inspect(cid);need(terminal['State']['Running']is False and terminal['State']['ExitCode']==0 and terminal['State']['OOMKilled']is False,'ActualCodecEOFExit0NoOOM');roles[role]['terminal_state']=terminal['State'];save(role+'-terminal-inspect-private.json',terminal);need(command([docker,'wait',cid]).strip()==b'0','ActualContainerWait0')
   if role=='ffprobe':
    probe=json.loads(data);save('ffprobe.json',probe);video=[x for x in probe['streams']if x['codec_type']=='video'];need(len(video)>=1,'AtLeastOneActualVideoStream');result['media']={'format_duration_seconds':probe['format'].get('duration'),'format_name':probe['format'].get('format_name'),'video_streams':[{k:x.get(k)for k in ['index','codec_name','codec_long_name','profile','width','height','pix_fmt','r_frame_rate','avg_frame_rate','duration','nb_frames']}for x in video]}
   else:need(data==b''and(OUT/('command-'+str(roles[role]['attached_command']).zfill(3)+'.stderr')).read_bytes()==b'','FullVideoDecodeNoErrorOutput');result['full_video_decode']={'actual_exit':0,'maps_all_video_streams':True,'seek_or_duration_limit_or_frame_limit':False,'stderr_bytes':0,'full_to_EOF':True,'audio_decode_claim':False}
   command([docker,'rm',cid]);current=None;need(not command([docker,'container','ls','--all','--no-trunc','--filter','id='+cid,'--format','{{.ID}}']).strip(),'SequentialSidecarRemovedBeforeNext')
 except BaseException as e:first=e;result['first_failure']={'type':type(e).__name__,'message':str(e)[:2048]}
 finally:
  in_cleanup=True
  # Discover a partial successful create even if its CLI reply/transport failed.
  try:
   ids=command([docker,'container','ls','--all','--no-trunc','--filter','label=com.docker.compose.project='+project,'--format','{{.ID}}']).decode().splitlines()
   for cid in ids:
    need(re.fullmatch('[a-f0-9]{64}',cid)is not None,'DiscoveredFullOwnedCID');v=inspect(cid);role=v['Config']['Labels'].get('com.docker.compose.service');need(role in ['ffprobe','ffmpeg']and v['Name']=='/'+project+'-'+role and v['Config']['Labels']['com.docker.compose.project']==project and v['Config']['Image']==IMAGE,'DiscoveredExactNameLabelImage')
    if role not in roles:roles[role]={'CID':cid,'name':project+'-'+role,'partial_create_discovered':True}
    else:need(roles[role]['CID']==cid,'SameActualRoleCID')
  except BaseException as e:result['cleanup_discovery_failure']={'type':type(e).__name__,'message':str(e)[:1024]}
  for role,row in list(roles.items()):
   try:
    current=row['CID'];ids=command([docker,'container','ls','--all','--no-trunc','--filter','id='+current,'--format','{{.ID}}']).decode().splitlines()
    if ids:
     need(ids==[current],'ExactCIDOnly');v=inspect(current);need(v['Config']['Labels']['com.docker.compose.project']==project and v['Name']=='/'+row['name'],'ExactCIDNameLabelBeforeRemoval');command([docker,'rm','--force',current])
    current=None
    for query in [['--filter','id='+row['CID']],['--filter','name=^/'+row['name']+'$']]:need(not command([docker,'container','ls','--all','--no-trunc']+query+['--format','{{.ID}}']).strip(),'FreshExactCIDAndNameAbsent')
    cleanup.append({'role':role,'CID':row['CID'],'name':row['name'],'absent':True})
   except BaseException as e:cleanup.append({'role':role,'passed':False,'type':type(e).__name__,'message':str(e)[:1024]})
  try:
   need(not command([docker,'container','ls','--all','--no-trunc','--filter','label=com.docker.compose.project='+project,'--format','{{.ID}}']).strip(),'FreshOwnedProjectEmpty');result['project_containers_empty']=True
   absence=[]
   for c in commands:need(c['physically_settled']and c['pipe_threads_settled']and actual_start(c['PID'])!=c['starttime'],'ActualCLIPhysicalIdentityAbsent');absence.append({'PID':c['PID'],'starttime':c['starttime'],'current_starttime':actual_start(c['PID'])})
   need(all(j.process.poll()is not None and all(not t.is_alive()for t in j.threads)for j in jobs),'EveryCLIWaitAndPipeSettled');result['original_CLI_identity_absence']=absence;need(sha(SOURCE)==EXPECTED and sha(OUT/'owned-input.mkv')==EXPECTED,'SourceAndCopyHashUnchangedAfterCodec');result['input_hashes_unchanged']=True
  except BaseException as e:result['cleanup_first_failure']={'type':type(e).__name__,'message':str(e)[:2048]}
  result['cleanup']=cleanup;result['elapsed_seconds']=time.monotonic()-began;result['passed']=first is None and'cleanup_first_failure'not in result and'cleanup_discovery_failure'not in result and len(roles)==2 and len(cleanup)==2 and all(x.get('absent')is True for x in cleanup)and result['elapsed_seconds']<=60;result['controller_exit']=0 if result['passed']else 1;save('execution.json',result)
 return result['controller_exit']
if __name__=='__main__':raise SystemExit(main())
