#!/usr/bin/env python3
"""Functional authenticated live playback fixture; no resource measurements.
Original Dispatcharr authors retain application/algorithm credit and licensing.
"""
import argparse,hashlib,importlib.util,json,os,shutil,subprocess,sys,tempfile,time,uuid
from pathlib import Path
from http_adapter import HTTPAdapter
import outage_protocol
EXECUTION_BLOCKED=True

REV='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
def module(path,name):
 s=importlib.util.spec_from_file_location(name,path);m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m

def original_warm_decode_qualified(decoded):
 return any(v.get('prefix')=='original-prefix.ts'and v.get('decoder_exit')==0 and v.get('frames',0)>=25 for v in decoded)

def main():
 if EXECUTION_BLOCKED:raise RuntimeError("ProlongedSourceExecutionBlockedPendingExactRootReview")
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--repo',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();a.viewers=[1];a.action='prolonged-outage-functional'
 assert a.viewers==[1], 'Single Source functional sustained qualification only'
 root=a.repo.resolve();out=a.output.resolve();out.mkdir(parents=True,exist_ok=False);own=Path(__file__).resolve().parent
 tools=out/'tools';tools.mkdir()
 archive=subprocess.check_output(['git','-C',str(root),'archive','28122faf28a0acf1c136ca0d0831961e786cb686','docker','scripts','docs'])
 subprocess.run(['tar','-x','--same-permissions','--no-same-owner','-C',str(tools)],input=archive,check=True)
 live=tools/'scripts/rust-baseline/prolonged-outage';shutil.copytree(own,live,dirs_exist_ok=True,ignore=shutil.ignore_patterns('__pycache__'))
 sys.path.insert(0,str(tools/'scripts/rust-integration'));integration=module(tools/'scripts/rust-integration/run.py','_m2_controlled_integration');ports=module(tools/'scripts/rust-integration/fixture_ports.py','_m2_controlled_ports')
 base_live=tools/'scripts/rust-live-perf/image47'
 plan=json.loads((base_live/'protocol.json').read_text())
 plan_sha=hashlib.sha256((base_live/'protocol.json').read_bytes()).hexdigest()
 project='dispatcharr-m0-concurrent-'+uuid.uuid4().hex[:10]
 report={'passed':False,'project':project,'scope':'ONE draft Original prolonged-outage characterization; no resource sampling','performance_verified':False,'observer_contract_only':False,'action':a.action,'source_revision':REV,'Source_image':IMAGE,'phases':[],'legacy_seed_decoder_protocol_sha256':hashlib.sha256((base_live/'protocol.json').read_bytes()).hexdigest(),'tool_base_commit':'28122faf28a0acf1c136ca0d0831961e786cb686','tools_sha256':{str(x.relative_to(tools)):hashlib.sha256(x.read_bytes()).hexdigest()for x in tools.rglob('*')if x.is_file()and '__pycache__'not in x.parts}}
 status=1
 with integration.fixture_source(report,False,out)as tmp:
  source=Path(tmp)/'source';source.mkdir(mode=0o755);raw=subprocess.check_output(['git','-C',str(root),'archive',REV]);assert hashlib.sha256(raw).hexdigest()=='f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff';subprocess.run(['tar','-x','--same-permissions','--no-same-owner','-C',str(source)],input=raw,check=True);(source/'media').mkdir(exist_ok=True)
  env=dict(os.environ,BASELINE_SOURCE_DIR=str(source),BASELINE_EVIDENCE_DIR=str(out),FIXTURE_SECONDS='600',FIXTURE_CODEC='h264',BENCHMARK_SHUTDOWN_DELAY='5',BENCHMARK_RETENTION_SECONDS='60',BENCHMARK_STARTUP_BEHIND='5')

  config={'services':{'baseline':{'cpus':1,'mem_limit':'1g'},'origin':{'cpus':2,'mem_limit':'512m'},'source':{'image':IMAGE,'working_dir':'/app','entrypoint':['/dispatcharrpy/bin/python','/m4-perf/source_web.py'],'cpus':2,'mem_limit':'512m','environment':{'DJANGO_SECRET_KEY':'playback-fixture-only','POSTGRES_DB':'dispatcharr','POSTGRES_USER':'dispatch','POSTGRES_PASSWORD':'playback-fixture-only','POSTGRES_HOST':'baseline','POSTGRES_PORT':'5432','REDIS_HOST':'baseline','REDIS_PORT':'6379','REDIS_PASSWORD':'playback-fixture-only','DJANGO_SETTINGS_MODULE':'dispatcharr.settings','DISPATCHARR_ENV':'aio','DISPATCHARR_LOG_LEVEL':'WARNING','PYTHONDONTWRITEBYTECODE':'1','DISPATCHARR_TRUSTED_PROXIES':'none','LANG':'C.UTF-8','LC_ALL':'C.UTF-8'},'volumes':[str(source)+':/app:ro',str(tools/'scripts/rust-playback-baseline')+':/verification:ro',str(tools/'scripts/rust-m4-perf')+':/m4-perf:ro',str(live)+':/outage-tools:ro'],'tmpfs':['/data:mode=1777,size=67108864','/app/media:mode=1777,size=67108864','/tmp:mode=1777,size=134217728'],'ports':['127.0.0.1::5656','127.0.0.1::9191'],'stop_grace_period':'15s'}},'networks':{'default':{'internal':False}}}
  config['services']['origin']['volumes']=[str(live/'origin_fixture.py')+':/verification/origin.py:ro']
  overlay=out/'live.yml';overlay.write_text(json.dumps(config));portfile=out/'ports.yml';ports.write_port_overlay(portfile,{'baseline':[5656],'origin':[8080],'source':[5656,9191]})
  compose=['docker','compose','-p',project,'-f',str(tools/'docker/compose.rust-playback.yml'),'-f',str(overlay),'-f',str(portfile)]
  from supervision import Registry,PayloadBudget,WorkDeadline
  from bounded_process import Collected
  from owned_stop import stop_project
  limits={'maximum_commands':4096,'cleanup_reserved_commands':32,'maximum_live_commands':16,'cleanup_reserved_live_commands':4,'final_join_seconds':30,'cleanup_wall_quality_ceiling_seconds':420}
  registry=Registry(Collected,env,out,lambda:stop_project(compose,env,project,15,run=registry.stop_control),limits,PayloadBudget(268435456,4194304),WorkDeadline(900))
  def command(argv,body=None,timeout=180):return registry.run(argv,body,timeout)
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
      selected[logical]={'id':ident,'image':record['Image'],'StartedAt':record['State']['StartedAt'],'NanoCpus':record['HostConfig']['NanoCpus'],'Memory':record['HostConfig']['Memory'],'IPs':[ip for net in record['NetworkSettings']['Networks'].values()for ip in[net.get('IPAddress'),net.get('GlobalIPv6Address')]if ip]}
      expected={'backend':(2000000000,536870912),'infrastructure':(1000000000,1073741824),'origin':(2000000000,536870912)}[logical]
      if(record['HostConfig']['NanoCpus'],record['HostConfig']['Memory'])!=expected:raise ValueError('ExactFixtureServiceCaps')
     phaseout.mkdir();cleanup_errors=[]
     def effective():
      code="import sys,json,redis,django;django.setup();sys.path.insert(0,'/outage-tools');import runtime_witness;r=redis.Redis(host='baseline',password='playback-fixture-only');print(json.dumps(runtime_witness.capture("+repr(seed['channel_uuid'])+",r)))"
      raw=execute('exec','-T','-e','LIVE_DISPOSABLE_FIXTURE=playback-fixture-only','source','/dispatcharrpy/bin/python','-c',code,timeout=20)
      value=json.loads(raw.splitlines()[-1]);return value[0],value[1]
     def candidates():
      code="import django,json;django.setup();from apps.channels.models import ChannelStream;from urllib.parse import urlsplit;rows=list(ChannelStream.objects.filter(channel__uuid="+repr(seed['channel_uuid'])+").order_by('order','pk').values_list('stream_id','stream__url'));assert len(rows)==2;assert all(urlsplit(u).hostname=='origin'and urlsplit(u).port==8080 and not urlsplit(u).query and not urlsplit(u).fragment for _,u in rows);print(json.dumps([[i,urlsplit(u).path]for i,u in rows]))"
      rows=json.loads(execute('exec','-T','source','/dispatcharrpy/bin/python','-c',code,timeout=20).splitlines()[-1])
      if [r[0]for r in rows]!=[seed['stream_pk'],seed['backup_stream_pk']]:raise ValueError('ExactSeedCandidateIDsRequired')
      return [r[1]for r in rows]
     def origin_stats():
      with urllib.request.urlopen(origin+'/stats',timeout=3)as response:
       raw=response.read(1048577)
       if len(raw)>1048576:raise ValueError('OriginStatsCapacity')
       return json.loads(raw)
     def retention(seconds):
      closed=time.monotonic();snapshot('post-physical-close')
      while time.monotonic()<closed+seconds:time.sleep(min(.25,max(0,closed+seconds-time.monotonic())))
      value=snapshot('retention-settled')
      value['Source_retention_settled']=value['clients']==0 and value['provider_slots']==0 and value['origin_active']==0
      return value
     def cleanup_result(errors):cleanup_errors.extend(errors)
     adapter=HTTPAdapter(base,origin,seed['path'],token,{'effective':effective,'candidates':candidates,'snapshot':snapshot,'origin_stats':origin_stats,'retention':retention,'cleanup_result':cleanup_result,'verify_user':inspect_active,'source_IPs':selected['backend']['IPs']},phaseout)
     observed=outage_protocol.run(adapter,time.monotonic,time.sleep);observed['events']=adapter.events;observed['same_run_states']=accounting;observed['selected_services']=selected;observed['resource_sampler']=False;observed['cleanup_errors']=cleanup_errors
     # Actual fresh decoder is independent of the old body's outcome.
     decoded=[]
     for name in['original-prefix.ts','fresh-prefix.ts']:
      payload=phaseout/name;result={'capture_scope':'warmup_original_prefix_only'if name=='original-prefix.ts'else'independent_fresh_postrestore_prefix','prefix':name,'bytes':payload.stat().st_size,'sha256':hashlib.sha256(payload.read_bytes()).hexdigest()}
      if payload.stat().st_size:
       try:
        raw=execute('exec','-T','baseline','ffmpeg','-hide_banner','-loglevel','error','-xerror','-progress','pipe:1','-nostats','-i','/evidence/'+phaseout.name+'/'+name,'-map','0:v:0','-map','0:a:0','-t','5','-f','null','-',timeout=20);result['decoder_exit']=0;result['frames']=max([int(x.split(b'=')[1])for x in raw.splitlines()if x.startswith(b'frame=')],default=0);(phaseout/(name+'.decode.stdout')).write_bytes(raw)
       except Exception as error:result['decoder_error']=type(error).__name__
      decoded.append(result)
     observed['snapshot_audit_scope']='selected SQL account/profile/channel fields; not full model rows/sequences';observed['runtime_path_scope']='separate Django exec+pinned files+metadata+owned socket imply initial Proxy branch; not instrumentation of the live HTTPStreamReader';observed['decodes']=decoded;report['phases'].append(observed);report['scope']='ONE Source102s all-candidate outage/88s restore characterization; no resource sampler';report['original_warm_AV_decode_qualified']=original_warm_decode_qualified(decoded);report['observation_validity']=not cleanup_errors and report['original_warm_AV_decode_qualified']
     report['Source_original_body_outcome']=observed['original_body_outcome'];report['Source_fresh_probe_outcome']=observed['fresh_body_outcome'];report['Source_budget_acceptance']=None;report['M0_complete']=False
     report['passed']=report['observation_validity']and observed.get('retention_outcome',{}).get('Source_retention_settled')is True and observed['fresh_body_outcome']['status']==200 and decoded[1].get('decoder_exit')==0 and decoded[1].get('frames',0)>=25;status=0 if report['passed']else 1

  except Exception as error:
   report['error_type']=type(error).__name__;report['error_message']=str(error)[:1024]
   if 'adapter'in locals():report['failed_adapter_observations']={'events':adapter.events,'original_body':None if adapter.old is None else adapter.old.snapshot(),'fresh_body':None if adapter.new is None else adapter.new.snapshot(),'cleanup_errors':cleanup_errors}
   if isinstance(error,subprocess.CalledProcessError):(out/'failure-private.log').write_bytes(error.stderr or b'')
   # Assertion diagnostics contain only numeric counts/fixed labels, never tokens.
   if isinstance(error,AssertionError):report['assertion']=str(error)
  finally:
   from supervision import terminal
   registry.close_admission()
   def cleanup_command(argv,timeout=30):return registry.run(argv,timeout=timeout,cleanup=True)
   def drain():
    with registry.lock:pending=bool(registry.active)
    value=registry.drain(30,escalate=pending);report['physical_process_cleanup']=value
    if not value['drained']:raise RuntimeError('PhysicalProcessesUndrained')
    return value
   def logs():
    raw=cleanup_command(compose+['logs','--no-color','--tail','200']);(out/'compose-private.log').write_bytes(raw);return{'bytes':len(raw)}
   def inventory(kind):
    argv={'containers':['ps','-aq'],'networks':['network','ls','-q'],'volumes':['volume','ls','-q']}[kind];value=cleanup_command(['docker',*argv,'--filter','label=com.docker.compose.project='+project],15).decode().strip()
    if value:raise RuntimeError('OwnedObjectsRemain:'+kind)
    return value
   owned_adapter=locals().get('adapter')
   def body_drain():
    if owned_adapter is not None:owned_adapter.close_owned_bodies_and_join()
    return {'scope':'after-down final host HTTP drain','started_bodies':owned_adapter is not None,'all_started_bodies_drained':True}
   actions=[('host-process-drain',drain),('logs',logs),('down',lambda:cleanup_command(compose+['down','--volumes','--remove-orphans','--timeout','10'],120)),('after-down-HTTP-drain',body_drain)]+[(kind,lambda kind=kind:inventory(kind))for kind in['containers','networks','volumes']]
   terminal(report,out,actions)
   report['host_collector_budget']=registry.budget.snapshot();report['host_command_settlements']=registry.retired;terminal(report,out,[])
 print(json.dumps({'passed':report['passed'],'output':str(out)}));return 0 if report['passed']else 1
if __name__=='__main__':raise SystemExit(main())
