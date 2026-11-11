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
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--repo',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();a.viewers=[1];a.action='sustained-functional'
 assert a.viewers==[1], 'Single Source functional sustained qualification only'
 root=a.repo.resolve();out=a.output.resolve();out.mkdir(parents=True,exist_ok=False);own=Path(__file__).resolve().parent
 tools=out/'tools';tools.mkdir()
 archive=subprocess.check_output(['git','-C',str(root),'archive','28122faf28a0acf1c136ca0d0831961e786cb686','docker','scripts','docs'])
 subprocess.run(['tar','-x','--same-permissions','--no-same-owner','-C',str(tools)],input=archive,check=True)
 live=tools/'scripts/rust-baseline/sustained';shutil.copytree(own,live,dirs_exist_ok=True,ignore=shutil.ignore_patterns('__pycache__'))
 sys.path.insert(0,str(tools/'scripts/rust-integration'));integration=module(tools/'scripts/rust-integration/run.py','_m2_controlled_integration');ports=module(tools/'scripts/rust-integration/fixture_ports.py','_m2_controlled_ports')
 base_live=tools/'scripts/rust-live-perf/image47'
 plan=json.loads((base_live/'protocol.json').read_text())
 plan_sha=hashlib.sha256((base_live/'protocol.json').read_bytes()).hexdigest()
 project='dispatcharr-m2-functional-'+uuid.uuid4().hex[:10]
 report={'passed':False,'project':project,'scope':'Original-only one authenticated sustained reader plus retention state; no resource sampling','performance_verified':False,'observer_contract_only':False,'action':a.action,'source_revision':REV,'Source_image':IMAGE,'phases':[],'protocol_sha256':hashlib.sha256((base_live/'protocol.json').read_bytes()).hexdigest(),'tool_base_commit':'28122faf28a0acf1c136ca0d0831961e786cb686','tools_sha256':{str(x.relative_to(tools)):hashlib.sha256(x.read_bytes()).hexdigest()for x in tools.rglob('*')if x.is_file()and '__pycache__'not in x.parts}}
 status=1
 with integration.fixture_source(report,False,out)as tmp:
  source=Path(tmp)/'source';source.mkdir(mode=0o755);raw=subprocess.check_output(['git','-C',str(root),'archive',REV]);assert hashlib.sha256(raw).hexdigest()=='f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff';subprocess.run(['tar','-x','--same-permissions','--no-same-owner','-C',str(source)],input=raw,check=True);(source/'media').mkdir(exist_ok=True)
  env=dict(os.environ,BASELINE_SOURCE_DIR=str(source),BASELINE_EVIDENCE_DIR=str(out),FIXTURE_SECONDS='300',FIXTURE_CODEC='h264',BENCHMARK_SHUTDOWN_DELAY='5',BENCHMARK_RETENTION_SECONDS='60',BENCHMARK_STARTUP_BEHIND='5')

  config={'services':{'baseline':{'cpus':1,'mem_limit':'1g'},'origin':{'cpus':2,'mem_limit':'512m'},'source':{'image':IMAGE,'working_dir':'/app','entrypoint':['/dispatcharrpy/bin/python','/m4-perf/source_web.py'],'cpus':2,'mem_limit':'512m','environment':{'DJANGO_SECRET_KEY':'playback-fixture-only','POSTGRES_DB':'dispatcharr','POSTGRES_USER':'dispatch','POSTGRES_PASSWORD':'playback-fixture-only','POSTGRES_HOST':'baseline','POSTGRES_PORT':'5432','REDIS_HOST':'baseline','REDIS_PORT':'6379','REDIS_PASSWORD':'playback-fixture-only','DJANGO_SETTINGS_MODULE':'dispatcharr.settings','DISPATCHARR_ENV':'aio','DISPATCHARR_LOG_LEVEL':'WARNING','PYTHONDONTWRITEBYTECODE':'1','DISPATCHARR_TRUSTED_PROXIES':'none','LANG':'C.UTF-8','LC_ALL':'C.UTF-8'},'volumes':[str(source)+':/app:ro',str(tools/'scripts/rust-playback-baseline')+':/verification:ro',str(tools/'scripts/rust-m4-perf')+':/m4-perf:ro'],'tmpfs':['/data:mode=1777,size=67108864','/app/media:mode=1777,size=67108864','/tmp:mode=1777,size=134217728'],'ports':['127.0.0.1::5656','127.0.0.1::9191'],'stop_grace_period':'15s'}},'networks':{'default':{'internal':False}}}
  overlay=out/'live.yml';overlay.write_text(json.dumps(config));portfile=out/'ports.yml';ports.write_port_overlay(portfile,{'baseline':[5656],'origin':[8080],'source':[5656,9191]})
  compose=['docker','compose','-p',project,'-f',str(tools/'docker/compose.rust-playback.yml'),'-f',str(overlay),'-f',str(portfile)]
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
   execute('up','-d','baseline');base=endpoint('baseline',5656);origin=endpoint('origin',8080);ready(origin+'/stats');ready(base+'/api/core/version/');seed=json.loads((out/'seed.json').read_text());identity_raw=execute('exec','-T','-e','LIVE_DISPOSABLE_FIXTURE=playback-fixture-only','baseline','/dispatcharrpy/bin/python','-',body=(base_live/'seed.py').read_bytes());identity=json.loads(identity_raw.splitlines()[-1]);token=identity['token'];uid=str(identity['user_id'])
   execute('exec','-T','baseline','/dispatcharrpy/bin/python','/verification/supervisor.py','control','stop')
   phases=[(1,1,'source')]
   for viewers,trial,backend in phases:
    if viewers not in plan['viewers']:raise ValueError('Unplanned viewers')
    if True:
     execute('stop','-t','15','source',timeout=60);execute('exec','-T','baseline','redis-cli','FLUSHDB');execute('up','-d','--no-deps','--force-recreate',backend);base=endpoint(backend,5656 if backend=='source'else 9192);ready(base+'/api/core/version/')
     cid=execute('ps','-q',backend).decode().strip();inspected=json.loads(command(['docker','inspect',cid]))[0]
     assert inspected['HostConfig']['Memory']==536870912 and inspected['HostConfig']['NanoCpus']==2000000000
     internal='http://'+backend+(':'+str(5656 if backend=='source'else 9192))+seed['path']
     primer=execute('exec','-T','baseline','ffmpeg','-hide_banner','-loglevel','error','-xerror','-progress','pipe:1','-nostats','-i',internal,'-t','3','-f','null','-',timeout=45);(out/(backend+'-'+str(viewers)+'-'+str(trial)+'-prime.log')).write_bytes(primer);prime_frames=max(int(x.split(b'=')[1])for x in primer.splitlines()if x.startswith(b'frame='));assert prime_frames>=50
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
     phaseout=out/(backend+'-'+str(viewers)+'-'+str(trial))
     accounting=[]
     def snapshot(label):
      started=time.monotonic()
      sql="SELECT json_build_object('id',id,'max_streams',max_streams,'is_active',is_active) FROM m3u_m3uaccountprofile WHERE is_default AND m3u_account_id=(SELECT m3u_account_id FROM dispatcharr_channels_stream WHERE id="+str(seed['stream_pk'])+")"
      profile=json.loads(execute('exec','-T','baseline','psql','-U','dispatch','-d','dispatcharr','-t','-A','-c',sql))
      code="local p='live:channel:'..ARGV[1];return {redis.call('SCARD',p..':clients'),tonumber(redis.call('GET','profile_connections:'..ARGV[2])or'0'),redis.call('GET',p..':owner')or'',redis.call('HLEN',p..':metadata'),redis.call('GET',p..':input:buffer:index')or'',redis.call('ZCARD',p..':input:buffer:chunk_timestamps')}"
      state=json.loads(execute('exec','-T','baseline','redis-cli','--json','EVAL',code,'0',seed['channel_uuid'],str(profile['id'])))
      with urllib.request.urlopen(origin+'/stats',timeout=3)as response:upstream=json.load(response)
      value={'label':label,'profile':profile,'clients':state[0],'provider_slots':state[1],'owner':state[2],'metadata_fields':state[3],'input_index':state[4],'timestamps':state[5],'origin_active':upstream['active'],'origin_connections':upstream['connections'],'started_monotonic':started,'completed_monotonic':time.monotonic()};accounting.append(value);return value
     selected={}
     for logical,service in [('backend','source'),('infrastructure','baseline'),('origin','origin')]:
      ident=execute('ps','-q',service).decode().strip();record=json.loads(command(['docker','inspect',ident]))[0]
      selected[logical]={'id':ident,'image':record['Image'],'StartedAt':record['State']['StartedAt'],'NanoCpus':record['HostConfig']['NanoCpus'],'Memory':record['HostConfig']['Memory']}
     snapshot('before')
     phase=load(base,seed['path'],token,viewers,15,60,phaseout,lambda:snapshot('active'),lambda *args:None,inspect_active,prefix_limit=plan['decoder_prefix_bytes_per_viewer'],physical_close_required=True)
     closed_at=time.monotonic();snapshot('post-physical-close')
     assert all(x['physical_socket_closed']for x in phase['viewers'])
     phase.update(backend=backend,count=viewers,trial=trial,primer_frames=prime_frames,source_bound_image=inspected['Image']);assert len(phase['viewers'])==viewers
     for index,row in enumerate(phase['viewers']):
      assert row['status']==200 and row['requests']==1 and not row['errors']and not row['unexpected_eof']and row['measured_bytes']>0 and row['prefix_bytes']==plan['decoder_prefix_bytes_per_viewer'] and row['post_window_barrier']
      payload=(phaseout/(str(index)+'.ts')).read_bytes();assert hashlib.sha256(payload).hexdigest()==row['prefix_sha256']
      decoder=subprocess.run(compose+['exec','-T','baseline','ffmpeg','-hide_banner','-loglevel','error','-xerror','-progress','pipe:1','-nostats','-i','/evidence/'+phaseout.name+'/'+str(index)+'.ts','-map','0:v:0','-map','0:a:0','-t',str(plan['decoder_output_seconds']),'-f','null','-'],env=env,capture_output=True,timeout=20)
      (phaseout/(str(index)+'.decode.stdout')).write_bytes(decoder.stdout);(phaseout/(str(index)+'.decode.stderr')).write_bytes(decoder.stderr)
      row['decoder_exit_code']=decoder.returncode;row['decoder_stdout_sha256']=hashlib.sha256(decoder.stdout).hexdigest();row['decoder_stderr_sha256']=hashlib.sha256(decoder.stderr).hexdigest();row['decoder_probe_diagnostic_bytes']=len(decoder.stderr);decoder.check_returncode();result=decoder.stdout
      frames=max([int(x.split(b'=')[1])for x in result.splitlines()if x.startswith(b'frame=')],default=0);row['decoder_frames']=frames;assert frames>=plan['decoder_min_video_frames']
     deadline=time.monotonic()+plan['budgets']['cleanup_seconds']
     while True:
      with urllib.request.urlopen(origin+'/stats',timeout=3)as r:stats=json.load(r)
      if stats['active']==0:break
      if time.monotonic()>deadline:raise TimeoutError('UpstreamCleanup')
      time.sleep(.25)
     while time.monotonic()<closed_at+70:time.sleep(min(.25,max(0,closed_at+70-time.monotonic())))
     final=snapshot('retention-settled');assert final['clients']==final['provider_slots']==final['origin_active']==0
     active=next(x for x in accounting if x['label']=='active');assert active['clients']==1 and active['profile']['max_streams']==0 and active['provider_slots']==0 and active['origin_active']==1
     phase['accounting']={'states':accounting,'selected':selected,'resource_sampler':False,'retention_seconds':70,'absolute_CPU_ceiling':None,'absolute_memory_ceiling':None,'postclose_growth_ceiling':None,'fully_verified':False,'resource_acceptance':False,'scope':'Same-run15s/60s sustained functional driver plus70s retention, no CPU/memory measurements or gain claim.'}
     phase['cleanup_upstream_active']=0;phase['passed']=True
     report['phases'].append(phase);(out/(backend+'-'+str(viewers)+'-'+str(trial)+'.json')).write_text(json.dumps(phase,indent=2)+'\n');print(json.dumps({'backend':backend,'viewers':viewers,'trial':trial,'passed':phase['passed']}),flush=True)
     if not phase['passed']:raise ValueError('Phase qualification failed; stop and retain')
   report['scope']='Original-only one authenticated sustained reader15s warmup/60s active, physical-close and70s retention state; no resource sampling';report['passed']=True;status=0
  except Exception as error:
   report['error_type']=type(error).__name__
   if isinstance(error,subprocess.CalledProcessError):(out/'failure-private.log').write_bytes(error.stderr or b'')
   # Assertion diagnostics contain only numeric counts/fixed labels, never tokens.
   if isinstance(error,AssertionError):report['assertion']=str(error)
  finally:
   try:execute('down','--volumes','--remove-orphans');report['cleanup']='owned project removed'
   except Exception as error:report['passed']=False;status=1;report['cleanup_error']=type(error).__name__
   if status!=0 or not report['passed']:report['performance_verified']=False
   (out/'live-functional.json').write_text(json.dumps(report,indent=2)+'\n')
 print(json.dumps({'passed':report['passed'],'output':str(out)}));return status
if __name__=='__main__':raise SystemExit(main())
