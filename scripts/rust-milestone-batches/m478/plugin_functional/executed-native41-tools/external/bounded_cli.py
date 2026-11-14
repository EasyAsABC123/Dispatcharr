"""Default read-only; exact9-file TLS/GPG metadata NEVER-STARTED pinned-image capture."""
import argparse,datetime,hashlib,io,json,os,re,selectors,subprocess,tarfile,time,uuid
from pathlib import Path,PurePosixPath
ROOT=Path(__file__).resolve().parent
IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
def need(ok,label):
 if not ok:raise ValueError(label)
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def save(p,v):p.write_text(json.dumps(v,indent=2,allow_nan=False)+'\n')
RUNTIME_PATHS=(
 '/usr/lib/python3.13/ssl.py',
 '/dispatcharrpy/lib/python3.13/site-packages/certifi/__init__.py',
 '/dispatcharrpy/lib/python3.13/site-packages/certifi/core.py',
 '/var/lib/dpkg/info/gpg.list',
 '/var/lib/dpkg/info/gpg.md5sums',
 '/var/lib/dpkg/info/gpgv.list',
 '/var/lib/dpkg/info/gpgv.md5sums',
 '/var/lib/dpkg/info/openssl.list',
 '/var/lib/dpkg/info/openssl.md5sums',
)
def relative(runtime):
 need(type(runtime)is str and runtime in RUNTIME_PATHS,'ExactDeclaredRuntimePath')
 if runtime.startswith('/usr/lib/python3.13/'):return 'stdlib/'+runtime.removeprefix('/usr/lib/python3.13/')
 if runtime.startswith('/dispatcharrpy/lib/python3.13/site-packages/'):return 'site-packages/'+runtime.removeprefix('/dispatcharrpy/lib/python3.13/site-packages/')
 return 'dpkg/'+runtime.removeprefix('/var/lib/dpkg/info/')
def check():
 seal=json.loads((ROOT/'seal.json').read_text());need(seal['state']=='NEVER_STARTED_TLS_GPG_METADATA9_ONLY','DataOnlyIdentity');need(set(seal['files'])=={'capture.py','plan.json','test_controls.py','definition-seal.json','README.md'},'CompleteToolInputClosure')
 for name,h in seal['files'].items():need(sha(ROOT/name)==h,'ExactCaptureInput:'+name)
 plan=json.loads((ROOT/'plan.json').read_text());need(plan['image']==IMAGE and tuple(plan['runtime_paths'])==RUNTIME_PATHS and plan['file_count']==9,'ExactPinned9Paths');need(all(relative(p)for p in plan['runtime_paths']),'ExactRuntimePaths');need(plan['never_started']is True and plan['no_volumes']is True and sha(ROOT/'definition-seal.json')==plan['definition_seal_sha256'],'BoundReviewedDefinitionAndNoExecution');return plan

def collect(argv,timeout,limit):
 p=subprocess.Popen(argv,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE);identity=Path('/proc',str(p.pid),'stat');starttime=identity.read_text().rsplit(')',1)[1].split()[19]if identity.exists()else None;s=selectors.DefaultSelector();s.register(p.stdout,selectors.EVENT_READ,0);s.register(p.stderr,selectors.EVENT_READ,1);buffers=[bytearray(),bytearray()];start=time.monotonic();invalid=None;eof=set();forced=False
 try:
  need(type(starttime)is str and starttime.isdigit()and int(starttime)>0,'ActualHostIdentityRequired')
  while s.get_map():
   if time.monotonic()-start>timeout:raise ValueError('Command20s')
   for key,_ in s.select(.05):
    raw=os.read(key.fileobj.fileno(),65536)
    if not raw:eof.add(key.data);s.unregister(key.fileobj);key.fileobj.close();continue
    capacity=limit if key.data==0 else 65536;keep=min(len(raw),max(0,capacity-len(buffers[key.data])));buffers[key.data].extend(raw[:keep])
    if keep!=len(raw):raise ValueError('BoundedCommandBytes; retained prefix only, incomplete')
  p.wait(timeout=max(.01,timeout-(time.monotonic()-start)))
 except BaseException as error:invalid=type(error).__name__+':'+str(error)
 finally:
  if p.poll()is None:
   forced=True;p.terminate()
   try:p.wait(timeout=2)
   except subprocess.TimeoutExpired:p.kill();p.wait(timeout=2)
  for pipe in(p.stdout,p.stderr):
   if not pipe.closed:pipe.close()
  s.close()
 return bytes(buffers[0]),bytes(buffers[1]),{'argv':argv,'pid':p.pid,'observed_starttime':starttime,'actual_wait_exit':p.wait(),'actual_poll_exit':p.poll(),'physical_pipes_closed':True,'both_actual_EOF_observed':eof=={0,1},'forced_termination':forced,'elapsed_seconds':time.monotonic()-start,'invalid':invalid,'PID_namespace':os.readlink('/proc/self/ns/pid'),'capture_complete':invalid is None and eof=={0,1}}

def only_regular_file(raw,name,maximum):
 with tarfile.open(fileobj=io.BytesIO(raw))as tar:
  rows=tar.getmembers();need(len(rows)==1 and rows[0].type in(tarfile.REGTYPE,tarfile.AREGTYPE)and rows[0].isfile()and not rows[0].issym()and rows[0].name==name and rows[0].size<=maximum,'OneBoundedExactRegularMember');data=tar.extractfile(rows[0]).read(maximum+1);need(len(data)==rows[0].size<=maximum,'ExactCopiedMemberBytes');return data
def inspect_created(value,cid):
 need(type(value)is list and len(value)==1 and value[0]['Id']==cid,'ExactOwnedCIDInspect');v=value[0];need(v['Config']['Image']==IMAGE and v['HostConfig']['NetworkMode']=='none'and v['HostConfig']['ReadonlyRootfs']is True and type(v['State']['Pid'])is int and v['State']['Pid']==0 and v['State']['Running']is False and v['State']['Status']=='created','NeverStartedPID0ReadonlyNone');need(not v.get('Mounts')and not v['HostConfig'].get('Binds')and not v['Config'].get('Volumes'),'NoVolumeOrBindMounts');return v
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--execute-reviewed-seal');p.add_argument('--output',type=Path);args=p.parse_args();plan=check()
 if args.execute_reviewed_seal is None:print(json.dumps({'passed':True,'read_only':True,'Docker_commands':0,'Source_programs':0,'explicit_files':9}));return 0
 need(args.execute_reviewed_seal==sha(ROOT/'seal.json'),'ExactReviewedSeal');out=ROOT.parent/plan['output'];need(args.output is not None and args.output.absolute()==out and not out.exists()and not out.is_symlink(),'FixedFreshOutput');out.mkdir(mode=0o700);project='dispatcharr-tls-gpg-metadata-'+uuid.uuid4().hex[:10];name=project+'-never-started';cid=None;claimed=False;serial=used=0;cleanup_began=None;began=time.monotonic();report={'passed':False,'state':'PARTIAL_NEVER_STARTED_DATA_ONLY','image':IMAGE,'project':project,'container_started':False,'program_executions':0,'rows':[],'commands':[],'volumes_created':None}
 def cmd(argv,limit=262144):
  nonlocal serial,used
  cleaning=cleanup_began is not None;cap=(5242880 if cleaning else 4194304)-used;need(serial<(36 if cleaning else 24)and cap>65536,'FixedCommandAndCleanupOutputReserve');serial+=1;remaining=(cleanup_began+90 if cleaning else began+180)-time.monotonic();need(remaining>0,'BoundedAttemptOrCleanupDeadline');stdout,stderr,row=collect(['docker']+argv,min(20,remaining),min(limit,cap-65536));used+=len(stdout)+len(stderr);(out/f'command-{serial:02d}.stdout').write_bytes(stdout);(out/f'command-{serial:02d}.stderr').write_bytes(stderr);report['commands'].append(row);save(out/'capture-report.json',report);need(row['actual_wait_exit']==row['actual_poll_exit']==0 and row['invalid']is None and row['both_actual_EOF_observed']is True and row['forced_termination']is False,'ActualDataCLIWaitPipes0');need(used<=5242880,'ActualCollectedTotal5MiB');return stdout
 def exact_absence():
  for tag,argv in [('project',['container','ls','--all','--no-trunc','--filter','label=com.docker.compose.project='+project,'--format','{{.ID}}']),('name',['container','ls','--all','--no-trunc','--filter','name=^/'+name+'$','--format','{{.ID}}'])]+([('CID',['container','ls','--all','--no-trunc','--filter','id='+cid,'--format','{{.ID}}'])]if cid else[]):
   raw=cmd(argv);save(out/(tag+'-absence.json'),{'argv':argv,'stdout':raw.decode(),'absent':not raw.strip()});need(not raw.strip(),'ExactOwned'+tag+'Absent')
 try:
  initial_project=cmd(['container','ls','--all','--no-trunc','--filter','label=com.docker.compose.project='+project,'--format','{{.ID}}']);initial_name=cmd(['container','ls','--all','--no-trunc','--filter','name=^/'+name+'$','--format','{{.ID}}']);save(out/'initial-owned-namespace.json',{'project':initial_project.decode(),'name':initial_name.decode()});need(not initial_project.strip()and not initial_name.strip(),'FreshOwnedNamespaceBeforeClaim');claimed=True
  created_cid=cmd(['create','--pull=never','--name',name,'--label','com.docker.compose.project='+project,'--network','none','--read-only','--pids-limit','32','--memory','64m','--entrypoint','/bin/true',IMAGE]).decode().strip();need(re.fullmatch('[a-f0-9]{64}',created_cid)is not None,'ActualCID');cid=created_cid;report['CID']=cid;created=inspect_created(json.loads(cmd(['inspect',cid])),cid);need(created['Config']['Labels'].get('com.docker.compose.project')==project,'ExactCreatedLabel');save(out/'created-inspect.json',created);report['volumes_created']=[];copied=0
  for runtime in plan['runtime_paths']:
   raw=cmd(['cp',cid+':'+runtime,'-'],524288+32768);data=only_regular_file(raw,Path(runtime).name,524288);copied+=len(data);need(copied<=2097152,'TotalCopied2MiB');dest=out/'actual-image'/relative(runtime);dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes(data);report['rows'].append({'path':str(dest.relative_to(out)),'runtime_path':runtime,'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest()});save(out/'capture-report.json',report)
  final=inspect_created(json.loads(cmd(['inspect',cid])),cid);need(final['Config']['Labels'].get('com.docker.compose.project')==project,'FinalExactLabel');save(out/'final-before-remove.json',final);report.update(passed=True,state='EXACT9_NEVER_STARTED_TLS_GPG_METADATA_CAPTURED',copied_total_bytes=copied)
 except BaseException as error:report['error']={'type':type(error).__name__,'message':str(error),'full_bytes':len(str(error).encode()),'full_sha256':hashlib.sha256(str(error).encode()).hexdigest()}
 finally:
  cleanup_began=time.monotonic();report['cleanup_started_utc']=datetime.datetime.now(datetime.timezone.utc).isoformat()
  try:
   if cid:cmd(['rm','--volumes',cid])
   elif claimed:
    raw=cmd(['container','ls','--all','--no-trunc','--filter','name=^/'+name+'$','--filter','label=com.docker.compose.project='+project,'--format','{{.ID}}']).decode().splitlines();need(len(raw)<=1,'AtMostOneExactPartialCreate')
    if raw:cmd(['rm','--volumes',raw[0]])
   if claimed:exact_absence();report['exact_owned_cleanup']=True
   else:report['cleanup_not_claimed_namespace_left_untouched']=True
  except BaseException as error:report.update(passed=False,cleanup_error={'type':type(error).__name__,'message':str(error)})
  gone=[]
  for row in report['commands']:
   pid=row['pid'];old=row['observed_starttime']
   try:current=Path('/proc',str(pid),'stat').read_text().rsplit(')',1)[1].split()[19]
   except FileNotFoundError:current=None
   gone.append({'pid':pid,'observed_starttime':old,'current_starttime':current,'original_absent':type(old)is str and old.isdecimal()and int(old)>0 and current!=old})
  save(out/'host-command-PID-absence.json',gone);report['all_known_host_PIDs_absent']=all(r['original_absent']for r in gone);report['cleanup_elapsed_seconds']=time.monotonic()-cleanup_began;report['actual_command_count']=serial;report['actual_collected_output_bytes']=used;report['data_capture_complete']=report['passed'];report['passed']=report['passed']and report.get('exact_owned_cleanup')is True and report['all_known_host_PIDs_absent']and report['cleanup_elapsed_seconds']<=90;report['actual_controller_exit']=0 if report['passed']else 1;report['settled_utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();save(out/'capture-report.json',report)
 return report['actual_controller_exit']
if __name__=='__main__':raise SystemExit(main())
