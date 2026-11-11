#!/usr/bin/env python3
"""Functional authenticated live playback fixture; no resource measurements.
Original Dispatcharr authors retain application/algorithm credit and licensing.
"""
import argparse,hashlib,importlib.util,json,os,shutil,subprocess,sys,tempfile,time,uuid
from pathlib import Path
from workload import run as load
REV='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
def module(path,name):
 s=importlib.util.spec_from_file_location(name,path);m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m

def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--repo',type=Path,required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--build-manifest',type=Path,required=True);p.add_argument('--viewers',type=int,nargs='+',default=[1,10,100]);a=p.parse_args()
 root=a.repo.resolve();out=a.output.resolve();out.mkdir(parents=True,exist_ok=False);own=Path(__file__).resolve().parent
 manifest=json.loads(a.build_manifest.read_bytes());assert manifest['passed'];tools=out/'tools';tools.mkdir()
 archive=subprocess.check_output(['git','-C',str(root),'archive','HEAD','docker','scripts','docs'])
 subprocess.run(['tar','-x','--same-permissions','--no-same-owner','-C',str(tools)],input=archive,check=True)
 live=tools/'scripts/rust-live-perf';shutil.copytree(own,live,ignore=shutil.ignore_patterns('__pycache__'))
 sys.path.insert(0,str(tools/'scripts/rust-integration'));integration=module(tools/'scripts/rust-integration/run.py','_m2_controlled_integration');ports=module(tools/'scripts/rust-integration/fixture_ports.py','_m2_controlled_ports')
 plan=json.loads((live/'protocol.json').read_text());project='dispatcharr-m2-functional-'+uuid.uuid4().hex[:10]
 report={'passed':False,'project':project,'scope':'Actual authenticated live TS continuous delivery and bounded prefix decodes only; concurrent development/no resource sampling','performance_verified':False,'source_revision':REV,'candidate_image':manifest['image_id'],'phases':[],'protocol_sha256':hashlib.sha256((live/'protocol.json').read_bytes()).hexdigest(),'tool_base_commit':subprocess.check_output(['git','-C',str(root),'rev-parse','HEAD'],text=True).strip(),'tools_sha256':{str(x.relative_to(tools)):hashlib.sha256(x.read_bytes()).hexdigest()for x in tools.rglob('*')if x.is_file()and '__pycache__'not in x.parts}}
 status=1
 with integration.fixture_source(report,False,out)as tmp:
  source=Path(tmp)/'source';source.mkdir(mode=0o755);raw=subprocess.check_output(['git','-C',str(root),'archive',REV]);assert hashlib.sha256(raw).hexdigest()=='f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff';subprocess.run(['tar','-x','--same-permissions','--no-same-owner','-C',str(source)],input=raw,check=True);(source/'media').mkdir(exist_ok=True)
  env=dict(os.environ,BASELINE_SOURCE_DIR=str(source),BASELINE_EVIDENCE_DIR=str(out),FIXTURE_SECONDS='300',FIXTURE_CODEC='h264',BENCHMARK_SHUTDOWN_DELAY='5',BENCHMARK_RETENTION_SECONDS='60',BENCHMARK_STARTUP_BEHIND='5')
  candidate=integration.candidate_overlay(tools/'docker/compose.rust-candidate.yml',out/'candidate.yml',manifest['image_id'])
  config={'services':{'baseline':{'cpus':1,'mem_limit':'1g'},'origin':{'cpus':2,'mem_limit':'512m'},'candidate':{'cpus':2,'mem_limit':'512m'},'source':{'image':IMAGE,'working_dir':'/app','entrypoint':['/dispatcharrpy/bin/python','/m4-perf/source_web.py'],'cpus':2,'mem_limit':'512m','environment':{'DJANGO_SECRET_KEY':'playback-fixture-only','POSTGRES_DB':'dispatcharr','POSTGRES_USER':'dispatch','POSTGRES_PASSWORD':'playback-fixture-only','POSTGRES_HOST':'baseline','POSTGRES_PORT':'5432','REDIS_HOST':'baseline','REDIS_PORT':'6379','REDIS_PASSWORD':'playback-fixture-only','DJANGO_SETTINGS_MODULE':'dispatcharr.settings','DISPATCHARR_ENV':'aio','DISPATCHARR_LOG_LEVEL':'WARNING','PYTHONDONTWRITEBYTECODE':'1','DISPATCHARR_TRUSTED_PROXIES':'none','LANG':'C.UTF-8','LC_ALL':'C.UTF-8'},'volumes':[str(source)+':/app:ro',str(tools/'scripts/rust-playback-baseline')+':/verification:ro',str(tools/'scripts/rust-m4-perf')+':/m4-perf:ro'],'tmpfs':['/data:mode=1777,size=67108864','/app/media:mode=1777,size=67108864','/tmp:mode=1777,size=134217728'],'ports':['127.0.0.1::5656','127.0.0.1::9191'],'stop_grace_period':'15s'}},'networks':{'default':{'internal':False}}}
  overlay=out/'live.yml';overlay.write_text(json.dumps(config));portfile=out/'ports.yml';ports.write_port_overlay(portfile,{'baseline':[5656],'origin':[8080],'source':[5656,9191],'candidate':[9192]})
  compose=['docker','compose','-p',project,'-f',str(tools/'docker/compose.rust-playback.yml'),'-f',str(candidate),'-f',str(overlay),'-f',str(portfile)]
  def command(argv,body=None,timeout=180):return subprocess.check_output(argv,input=body,env=env,stderr=subprocess.PIPE,timeout=timeout)
  def execute(*args,body=None,timeout=180):return command(compose+list(args),body,timeout)
  def endpoint(name,port):return 'http://'+execute('port',name,str(port)).decode().strip()
  import urllib.request
  def ready(url):
   from urllib.parse import urlsplit
   report['readiness_path']=urlsplit(url).path
   deadline=time.monotonic()+180
   while time.monotonic()<deadline:
    try:
     with urllib.request.urlopen(url,timeout=3)as r:
      if r.status==200:return
    except Exception as error:
     report['readiness_last_error']={'type':type(error).__name__,'status':getattr(error,'code',None)}
    (out/'readiness-private.json').write_text(json.dumps({'path':report['readiness_path'],'last_error':report.get('readiness_last_error')}))
    time.sleep(.25)
   raise TimeoutError('HTTP readiness')
  try:
   execute('up','-d','baseline');base=endpoint('baseline',5656);origin=endpoint('origin',8080);ready(origin+'/stats');ready(base+'/api/core/version/');seed=json.loads((out/'seed.json').read_text());identity_raw=execute('exec','-T','-e','LIVE_DISPOSABLE_FIXTURE=playback-fixture-only','baseline','/dispatcharrpy/bin/python','-',body=(live/'seed.py').read_bytes());identity=json.loads(identity_raw.splitlines()[-1]);token=identity['token'];uid=str(identity['user_id'])
   execute('exec','-T','baseline','/dispatcharrpy/bin/python','/verification/supervisor.py','control','stop');execute('up','-d','--no-deps','data-init')
   for viewers in a.viewers:
    if viewers not in plan['viewers']:raise ValueError('Unplanned viewers')
    for backend in ['source','candidate']:
     execute('stop','-t','15','source','candidate',timeout=60);execute('exec','-T','baseline','redis-cli','FLUSHDB');execute('up','-d','--no-deps','--force-recreate',backend);base=endpoint(backend,5656 if backend=='source'else 9192);ready(base+'/api/core/version/')
     cid=execute('ps','-q',backend).decode().strip();inspected=json.loads(command(['docker','inspect',cid]))[0]
     if backend=='candidate':integration.validate_candidate_image(manifest,inspected)
     assert inspected['HostConfig']['Memory']==536870912 and inspected['HostConfig']['NanoCpus']==2000000000
     internal='http://'+backend+(':'+str(5656 if backend=='source'else 9192))+seed['path']
     primer=execute('exec','-T','baseline','ffmpeg','-hide_banner','-loglevel','error','-xerror','-progress','pipe:1','-nostats','-i',internal,'-t','3','-f','null','-',timeout=45);(out/(backend+'-'+str(viewers)+'-prime.log')).write_bytes(primer);prime_frames=max(int(x.split(b'=')[1])for x in primer.splitlines()if x.startswith(b'frame='));assert prime_frames>=50
     def inspect_active(count):
      script="local keys=redis.call('KEYS',ARGV[1]);local n=0;for _,k in ipairs(keys) do if redis.call('TYPE',k).ok=='hash' then if redis.call('HGET',k,'user_id')~=ARGV[2] then return {-1,#keys} end;n=n+1 end end;return {n,#keys}"
      response=execute('exec','-T','baseline','redis-cli','--json','EVAL',script,'0','live:channel:'+seed['channel_uuid']+':clients:*',uid)
      rows=json.loads(response);assert rows==[count,count],('auth metadata mismatch',backend,count,rows);return {'clients':count,'all_owned_user_id':True}
     prime_deadline=time.monotonic()+20
     while True:
      try:inspect_active(0);break
      except AssertionError:
       if time.monotonic()>=prime_deadline:raise
       time.sleep(.1)
     phaseout=out/(backend+'-'+str(viewers));phase=load(base,seed['path'],token,viewers,plan['functional_warmup_seconds'],plan['functional_measure_seconds'],phaseout,lambda:None,lambda *args:None,inspect_active,prefix_limit=plan['decoder_prefix_bytes_per_viewer'])
     phase.update(backend=backend,count=viewers,primer_frames=prime_frames,source_bound_image=inspected['Image']);assert len(phase['viewers'])==viewers
     for index,row in enumerate(phase['viewers']):
      assert row['status']==200 and row['requests']==1 and not row['errors']and not row['unexpected_eof']and row['measured_bytes']>0 and row['prefix_bytes']==plan['decoder_prefix_bytes_per_viewer'] and row['post_window_barrier']
      payload=(phaseout/(str(index)+'.ts')).read_bytes();assert hashlib.sha256(payload).hexdigest()==row['prefix_sha256']
      result=execute('exec','-T','baseline','ffmpeg','-hide_banner','-loglevel','error','-xerror','-progress','pipe:1','-nostats','-i','/evidence/'+phaseout.name+'/'+str(index)+'.ts','-map','0:v:0','-map','0:a:0','-t',str(plan['decoder_output_seconds']),'-f','null','-',timeout=20)
      frames=max([int(x.split(b'=')[1])for x in result.splitlines()if x.startswith(b'frame=')],default=0);row['decoder_frames']=frames;assert frames>=plan['decoder_min_video_frames']
     deadline=time.monotonic()+plan['budgets']['cleanup_seconds']
     while True:
      with urllib.request.urlopen(origin+'/stats',timeout=3)as r:stats=json.load(r)
      if stats['active']==0:break
      if time.monotonic()>deadline:raise TimeoutError('UpstreamCleanup')
      time.sleep(.25)
     phase['cleanup_upstream_active']=0;phase['passed']=True;report['phases'].append(phase);(out/(backend+'-'+str(viewers)+'.json')).write_text(json.dumps(phase,indent=2)+'\n');print(json.dumps({'backend':backend,'viewers':viewers,'passed':True}),flush=True)
   report['passed']=True;status=0
  except Exception as error:
   report['error_type']=type(error).__name__
   if isinstance(error,subprocess.CalledProcessError):(out/'failure-private.log').write_bytes(error.stderr or b'')
   # Assertion diagnostics contain only numeric counts/fixed labels, never tokens.
   if isinstance(error,AssertionError):report['assertion']=str(error)
  finally:
   try:execute('down','--volumes','--remove-orphans');report['cleanup']='owned project removed'
   except Exception as error:report['passed']=False;status=1;report['cleanup_error']=type(error).__name__
   (out/'live-functional.json').write_text(json.dumps(report,indent=2)+'\n')
 print(json.dumps({'passed':report['passed'],'output':str(out)}));return status
if __name__=='__main__':raise SystemExit(main())
