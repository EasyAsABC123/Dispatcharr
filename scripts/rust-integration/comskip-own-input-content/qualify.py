"""Existing coordinator + one bounded network-none codec role; Root grant required.
qualify-source6 first; compare accepts a future Native CUT's own three files.
"""
import argparse,fcntl,hashlib,importlib.util,json,os,signal,stat,time,uuid
from pathlib import Path
R=Path(__file__).resolve().parents[3];H=Path(__file__).resolve().parent
IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
SOURCE_IMAGE='sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
MAX_INPUT=64*1024*1024

def load(p):return json.loads(Path(p).read_text())
def save(p,v):Path(p).write_text(json.dumps(v,indent=2)+'\n')
def digest(path):
 h=hashlib.sha256();count=0
 with Path(path).open('rb')as source:
  for data in iter(lambda:source.read(65536),b''):h.update(data);count+=len(data)
 return {'sha256':h.hexdigest(),'bytes':count}

def generation(pid):
 try:return Path('/proc',str(pid),'stat').read_text().rsplit(')',1)[1].split()[19]
 except FileNotFoundError:return None

def validate_file(path,limit):
 path=Path(path);assert not path.is_symlink();resolved=path.resolve(strict=True);s=resolved.stat();assert stat.S_ISREG(s.st_mode)and 0<s.st_size<=limit and ','not in str(resolved)and ':'not in str(resolved)and '\n'not in str(resolved)
 proof={'stat':[s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns,s.st_ctime_ns],**digest(resolved)};after=resolved.stat();assert proof['stat']==[after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns,after.st_ctime_ns];return resolved,proof

def main():
 p=argparse.ArgumentParser();p.add_argument('mode',choices=['qualify-source6','compare']);p.add_argument('--docker',type=Path,required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--input',type=Path);p.add_argument('--actual-cut',type=Path);p.add_argument('--edl',type=Path);p.add_argument('--source-qualification',type=Path);a=p.parse_args();assert a.docker.is_absolute()
 O=a.output.resolve();O.mkdir(parents=True,mode=0o700,exist_ok=False);(O/'codec-evidence').mkdir(mode=0o700);start=time.monotonic();deadline=start+300;identity={'project':'dispatcharr-cut-content-'+uuid.uuid4().hex[:12]};commands=[];failure=None;result={};runtime_started=False
 spec=importlib.util.spec_from_file_location('existing_owned_coordinator',R/'scripts/rust-milestone-batches/coordinator.py');coordinator=importlib.util.module_from_spec(spec);spec.loader.exec_module(coordinator)
 env=dict(os.environ,DOCKER_HOST='unix:///tmp/dispatcharr-docker.sock',DOCKER_CONFIG=str(R/'artifacts/rust/tooling/docker-config'));docker=str(a.docker)
 def command(argv,seconds=20,allow=False,cleanup=False):
  remaining=(cleanup_deadline if cleanup else deadline)-time.monotonic();assert remaining>0,'OwnedWholeBudgetExpired'
  label=f'command-{len(commands)+1:04d}';row=coordinator.command(argv,{'cwd':str(R),'environment':env,'output_bytes':1024*1024},O,label,min(seconds,remaining));row['argv']=argv;commands.append(row);save(O/'commands.json',commands)
  assert coordinator.settled(row)and(row['actual_exit']==0 or allow),row
  return Path(row['stdout']).read_text()
 def stop(*_):raise KeyboardInterrupt('OwnedToolCancelled')
 old_handlers={s:signal.signal(s,stop)for s in(signal.SIGTERM,signal.SIGINT)}
 lock=None
 try:
  # Reuse the actual local app/codec lease, never declare false overlap.
  lock=(R/'scripts/rust-milestone-batches/.dispatcher.lock').open('a+');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
  if a.mode=='qualify-source6':
   assert a.input is None and a.actual_cut is None and a.edl is None and a.source_qualification is None
   public=R/'scripts/rust-integration/comskip-positive-lifecycle-source1';cell=public/'cells/natural-automatic-cut';copies=load(cell/'actual-detector-inputs.json');assert len(copies)==1;original=Path(next(iter(copies.values()))['path']);media=load(cell/'media-after.json');generated=load(cell/'actual-generated-EDL.json');assert len(generated)==1
   original_edl=Path(next(iter(generated)));actual=original_edl.with_suffix('.mkv');assert actual.is_relative_to(R/'artifacts/rust/normal-graph-comskip-positive-source-6')
   text=next(iter(generated.values()))['text'];edl=O/'Source6-captured.edl';edl.write_bytes(text.encode());edl.chmod(0o444);assert digest(edl)['sha256']==next(iter(generated.values()))['sha256']
   expected_source={'original':{k:next(iter(copies.values()))[k]for k in('bytes','sha256')},'actual':{k:media[k]for k in('bytes','sha256')}}
   result['Source6_metadata_inputs']={str(path):digest(path)for path in[cell/'actual-detector-inputs.json',cell/'media-after.json',cell/'actual-generated-EDL.json',public/'qualification.json']}
  else:
   assert all(x is not None for x in[a.input,a.actual_cut,a.edl,a.source_qualification])
   source=load(a.source_qualification);assert source['tool_inputs']['guest']==digest(H/'guest.py')and source['tool_inputs']['host']==digest(Path(__file__))and source['tool_inputs']['decoder_authority']==digest(R/'scripts/rust-integration/comskip-positive-lifecycle-source1/manual-cut-content.json');assert source['passed']and source['mode']=='qualify-source6'and source['codec_result']['all_streams_and_full_timing_exact']and source['owned_cleanup']['passed']
   original,actual,edl=a.input,a.actual_cut,a.edl;result['Source_tool_qualification']={'path':str(a.source_qualification.resolve()),**digest(a.source_qualification)}
  selected={}
  for name,path,limit in[('original',original,MAX_INPUT),('actual',actual,MAX_INPUT),('edl',edl,1024*1024)]:selected[name]=validate_file(path,limit)
  if a.mode=='qualify-source6':
   for name,expected in expected_source.items():assert {k:selected[name][1][k]for k in('bytes','sha256')}==expected,'ExactRetainedSource6:'+name
  save(O/'input-custody-before.json',{name:{'path':str(path),**proof}for name,(path,proof)in selected.items()})
  project=identity['project'];config=O/'codec-compose.json';guest=H/'guest.py';decoder_authority=R/'scripts/rust-integration/comskip-positive-lifecycle-source1/manual-cut-content.json';save(O/'tool-inputs.json',{'guest':digest(guest),'host':digest(Path(__file__)),'decoder_authority':digest(decoder_authority),'coordinator':digest(R/'scripts/rust-milestone-batches/coordinator.py'),'image':IMAGE});result['tool_inputs']=load(O/'tool-inputs.json')
  owner="import signal,sys;signal.signal(signal.SIGTERM,lambda *_:sys.exit(0));signal.alarm(330);print('owned-codec-ready',flush=True);signal.pause()"
  service={'image':IMAGE,'platform':'linux/amd64','pull_policy':'never','user':'0:0','network_mode':'none','read_only':True,'cap_drop':['ALL'],'security_opt':['no-new-privileges:true'],'cpus':2,'mem_limit':'512m','pids_limit':64,'tmpfs':['/tmp:rw,nosuid,nodev,size=32m','/work:rw,nosuid,nodev,size=256m'],'volumes':[str(guest)+':/tool/guest.py:ro',str(decoder_authority)+':/tool/decoder-authority.json:ro',str(O/'codec-evidence')+':/evidence',*[str(path)+':/inputs/'+target+':ro'for path,target in[(selected['original'][0],'original.mkv'),(selected['actual'][0],'actual.mkv'),(selected['edl'][0],'generated.edl')]]],'entrypoint':['/dispatcharrpy/bin/python','-I','-B','-u','-c',owner],'stop_grace_period':'5s'}
  save(config,{'services':{'baseline':service}});compose=[docker,'compose','-p',project,'-f',str(config)];runtime_started=True;command(compose+['up','-d','--no-build','baseline'],40);cid=command(compose+['ps','-q','baseline'],10).strip();assert len(cid)==64
  info=json.loads(command([docker,'inspect',cid],10))[0];pid=info['State']['Pid'];started=generation(pid);assert info['Image']==SOURCE_IMAGE and info['State']['Running']and pid>0 and started and info['HostConfig']['NetworkMode']=='none'and info['HostConfig']['ReadonlyRootfs']
  identity.update(CID=cid,hostPID=pid,starttime=started,image=info['Image'],PID_namespace=os.readlink('/proc/'+str(pid)+'/ns/pid'));save(O/'identity.json',identity)
  remaining=deadline-time.monotonic();assert remaining>20;seconds=min(300,int(remaining)-10)
  command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','-I','-B','/tool/guest.py','--seconds',str(seconds)],remaining-1,allow=True)
  codec=load(O/'codec-evidence/result.json');result['codec_result']=codec;assert codec['PID_namespace']==identity['PID_namespace'];guest_commands=load(O/'codec-evidence/commands.json');assert all(row['actual_exit']==0 and row['wait_completed']and row['original_generation_absent']and row['process_group_settled']and row['stderr_EOF']and(row.get('stdout_direct_file_closed')is True or row['stdout_EOF']is True)and row['PID_namespace']==identity['PID_namespace']for row in guest_commands);assert codec['passed']and codec['all_streams_and_full_timing_exact']and codec['all_source_files_unchanged']
  for name,(path,proof)in selected.items():assert validate_file(path,MAX_INPUT if name!='edl' else 1024*1024)[1]==proof,'HostReadOnlyInputChanged:'+name
 except BaseException as exc:failure={'type':type(exc).__name__,'message':str(exc)[:2000]}
 finally:
  for sig in old_handlers:signal.signal(sig,signal.SIG_IGN)
  cleanup_deadline=time.monotonic()+90;cleanup_errors=[]
  if runtime_started:
   try:command(compose+['down','--timeout','5','--remove-orphans'],20,cleanup=True)
   except BaseException as exc:cleanup_errors.append(type(exc).__name__)
   if identity.get('CID'):
    try:
     absent=not command([docker,'ps','-aq','--no-trunc','--filter','id='+identity['CID']],10,cleanup=True).strip();identity['CID_absent']=absent
     if not absent:command([docker,'rm','-f',identity['CID']],10,cleanup=True);identity['CID_absent']=not command([docker,'ps','-aq','--no-trunc','--filter','id='+identity['CID']],10,cleanup=True).strip()
     identity['original_host_generation_absent']=generation(identity['hostPID'])!=identity['starttime']
    except BaseException as exc:cleanup_errors.append(type(exc).__name__)
   try:
    inventories={'containers':not command([docker,'ps','-aq','--filter','label=com.docker.compose.project='+identity['project']],10,cleanup=True).strip(),'networks':not command([docker,'network','ls','-q','--filter','label=com.docker.compose.project='+identity['project']],10,cleanup=True).strip(),'volumes':not command([docker,'volume','ls','-q','--filter','label=com.docker.compose.project='+identity['project']],10,cleanup=True).strip()};identity['project_inventories_empty']=inventories;identity['project_absent']=all(inventories.values())
   except BaseException as exc:cleanup_errors.append(type(exc).__name__)
  cleanup={'passed':not cleanup_errors and bool(identity.get('CID_absent'))and bool(identity.get('original_host_generation_absent'))and bool(identity.get('project_absent')),'identity':identity,'errors':cleanup_errors};save(O/'owned-cleanup.json',cleanup)
  passed=failure is None and cleanup['passed'];result.update(passed=passed,mode=a.mode,failure=failure,seconds=time.monotonic()-start,owned_cleanup=cleanup,application_or_Comskip_detector_or_booking_calls=0,source_application_business_replay=False,performance_claim=False,full_milestone_claim=False);save(O/'result.json',result)
  if lock:lock.close()
  for sig,handler in old_handlers.items():signal.signal(sig,handler)
  print(json.dumps({'passed':passed,'failure':failure,'result':str(O/'result.json')}))
 return 0 if passed else 1
if __name__=='__main__':raise SystemExit(main())
