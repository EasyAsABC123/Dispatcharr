"""Exact retained test assets; Original Dispatcharr/FFmpeg authors retain credit.
No application or detector starts. A Root resource grant must precede actual CLI.
"""
import argparse,hashlib,json,os,signal,subprocess,time,uuid
from pathlib import Path
HERE=Path(__file__).resolve().parent

def digest(path):
 h=hashlib.sha256();count=0
 with Path(path).open('rb')as stream:
  for block in iter(lambda:stream.read(65536),b''):h.update(block);count+=len(block)
 return {'sha256':h.hexdigest(),'bytes':count}
def generation(pid):
 try:return Path('/proc',str(pid),'stat').read_text().rsplit(')',1)[1].split()[19]
 except FileNotFoundError:return None

def main():
 p=argparse.ArgumentParser();p.add_argument('--docker',required=True);p.add_argument('--master',type=Path,default=HERE/'fixtures/master.mkv');p.add_argument('--output',type=Path,required=True);p.add_argument('--try-master-once',action='store_true');args=p.parse_args()
 output=args.output.resolve();output.mkdir(parents=True,exist_ok=False);data=output/'data';data.mkdir();assets=data/'comskip-positive-media';assets.mkdir()
 recipe=json.loads((HERE/'recipe.json').read_text());expected=recipe['expected_files'];master=args.master.resolve();before=master.stat();assert digest(master)=={k:expected['master'][k]for k in ['sha256','bytes']},'Exact retained Source input required'
 project='dispatcharr-comskip-assets-'+uuid.uuid4().hex[:12];config=output/'codec-compose.json'
 config.write_text(json.dumps({'services':{'baseline':{'image':recipe['Source_image'],'network_mode':'none','read_only':True,'cap_drop':['ALL'],'cpus':2,'mem_limit':'2g','pids_limit':512,'tmpfs':['/tmp:rw,nosuid,nodev,size=64m'],'volumes':[str(data)+':/shared'],'entrypoint':['/dispatcharrpy/bin/python','-u','-c',"import signal,sys;signal.signal(signal.SIGTERM,lambda *_:sys.exit(0));print('owned-codec-ready',flush=True);signal.pause()"]}}}))
 compose=[args.docker,'compose','-p',project,'-f',str(config)];commands=[];identity={'project':project};started=time.monotonic();failure=None;result={}
 def save(name,value):(output/name).write_text(json.dumps(value,indent=2)+'\n')
 def command(argv,timeout=120):
  prefix=output/f'command-{len(commands)+1:04d}';t=time.monotonic();proc=subprocess.Popen(argv,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,start_new_session=True)
  try:stdout,stderr=proc.communicate(timeout=timeout);fault=None
  except subprocess.TimeoutExpired:
   os.killpg(proc.pid,signal.SIGKILL);stdout,stderr=proc.communicate();fault='TimeoutExpired'
  prefix.with_suffix('.stdout').write_text(stdout);prefix.with_suffix('.stderr').write_text(stderr)
  row={'argv':argv,'actual_exit':proc.returncode,'failure':fault,'seconds':time.monotonic()-t,'stdout':str(prefix.with_suffix('.stdout')),'stderr':str(prefix.with_suffix('.stderr')),'stdout_EOF':True,'stderr_EOF':True,'wait_completed':True,'process_group':proc.pid};commands.append(row);save('commands.json',commands)
  if fault or proc.returncode:raise RuntimeError('OwnedCodecCommandFailed:'+str(row))
  return stdout
 try:
  command(compose+['up','-d','--no-build','baseline'],60);cid=command(compose+['ps','-q','baseline'],15).strip();assert len(cid)==64
  state=json.loads(command([args.docker,'inspect','--format','{{json .State}}',cid],15));pid=state['Pid'];start=generation(pid);assert pid>0 and start
  image=json.loads(command([args.docker,'inspect','--format','{{json .Image}}',cid],15));assert image==recipe['Source_image'];identity.update(CID=cid,hostPID=pid,starttime=start,image=image);save('identity.json',identity)
  tool=json.loads(command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','-c',"import hashlib,json,shutil;from pathlib import Path;p=Path(shutil.which('ffmpeg')).resolve();h=hashlib.sha256();f=p.open('rb');[h.update(x)for x in iter(lambda:f.read(65536),b'')];print(json.dumps({'path':str(p),'sha256':h.hexdigest()}))"],20));assert tool['sha256']=='5831afcbcb9dc46107b633dc09f276794dc094b70aacfda6096eeceb83f013f0'
  version=command(compose+['exec','-T','baseline','ffmpeg','-version'],15).splitlines()[0];assert version==recipe['ffmpeg_version'];result['actual_tool']={'ELF':tool,'version':version}
  destination=assets/'master.mkv'
  if args.try_master_once:
   command(compose+['exec','-T','baseline',*recipe['commands'][0]],660)
   observed=digest(destination);match=observed=={k:expected['master'][k]for k in ['sha256','bytes']};result['one_master_regeneration']={'actual':observed,'exact_Source_SHA_match':match,'argv':recipe['commands'][0],'attempts':1,'tuning_performed':False}
   if not match:destination.rename(assets/'master-trial-mismatch.mkv')
  if not destination.exists():
   with master.open('rb')as source,destination.open('xb')as target:
    for block in iter(lambda:source.read(65536),b''):target.write(block)
   assert digest(destination)=={k:expected['master'][k]for k in ['sha256','bytes']};destination.chmod(0o444);result['master_custody']='exact retained Source master copied; no input normalization'
  else:result['master_custody']='one actual regeneration matched exact retained input bytes'
  command(compose+['exec','-T','baseline',*recipe['commands'][1]],120)
  feed=digest(assets/'feed.ts');assert feed=={k:expected['feed'][k]for k in ['sha256','bytes']},'Exact recorded feed recipe did not match; no tuning permitted'
  from assets import copy_exact
  ini=HERE/'fixtures/positive.ini';result['INI']=copy_exact(ini,assets/'positive.ini',{'sha256':recipe['INI']['selected_sha256'],'bytes':ini.stat().st_size})
  result['feed']=feed;after=master.stat();assert(before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns,before.st_ctime_ns)==(after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns,after.st_ctime_ns)and digest(master)=={k:expected['master'][k]for k in ['sha256','bytes']};result['retained_Source_input_unchanged']=True
 except BaseException as error:failure={'type':type(error).__name__,'message':str(error)[:4000]}
 finally:
  try:command(compose+['down','--timeout','10','--remove-orphans'],30)
  except BaseException as error:identity['cleanup_error']=type(error).__name__
  try:identity['project_absent']=not command([args.docker,'ps','-aq','--filter','label=com.docker.compose.project='+project],15).strip()
  except BaseException:identity['project_absent']=False
  if identity.get('CID'):
   identity['original_generation_absent']=generation(identity['hostPID'])!=identity['starttime'];identity['CID_absent']=not command([args.docker,'ps','-aq','--filter','id='+identity['CID']],15).strip()
  passed=failure is None and all(identity.get(k)for k in ['project_absent','CID_absent','original_generation_absent'])
  save('identity.json',identity);result.update(passed=passed,failure=failure,seconds=time.monotonic()-started,owned_cleanup=identity,new_Source_business_tasks=0,new_applications=0,performance_claim=False);save('result.json',result);print(json.dumps(result))
 return 0 if passed else 1
if __name__=='__main__':raise SystemExit(main())
