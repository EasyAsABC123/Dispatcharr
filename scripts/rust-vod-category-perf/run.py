#!/usr/bin/env python3
"""Category metadata functional fixture, Source qualification and scoped pairs.
Functional mode starts no sampler; resource phases require separate fresh cues.
Original Dispatcharr authors retain all application credit and AGPL licensing.
"""
import argparse,concurrent.futures,hashlib,importlib.util,json,os,subprocess,sys,threading,time,uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT/'scripts/rust-integration'))
from run import BASELINE,fixture_source,candidate_overlay,validate_candidate_image
from fixture_ports import write_port_overlay
from tool_snapshot import snapshot_sources
import protocol

def main():
 global protocol
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',required=True,type=Path);p.add_argument('--build-manifest',type=Path);p.add_argument('--policy-record',type=Path);p.add_argument('--revision');p.add_argument('--action',choices=['bind','source-functional','functional','experiment'],default='functional');p.add_argument('--quiet-authorization',type=Path);p.add_argument('--measurement-authorization',type=Path);a=p.parse_args()
 out=a.output.resolve();out.mkdir(parents=True,exist_ok=False)
 manifest=json.loads(a.build_manifest.read_text())if a.build_manifest else None
 if a.action=='bind':
  if manifest is None or not a.revision:p.error('Bind requires passing canonical manifest and exact revision')
  value=protocol.bind(manifest,a.revision,out/'bound-policy.json');print(json.dumps({'policy_sha256':value['policy_sha256'],'binding':str(out/'bound-policy.json')}));return 0
 source_only=a.action=='source-functional'
 binding=json.loads(a.policy_record.read_text())if a.policy_record else None
 if not source_only:
  if manifest is None or binding is None:p.error('Candidate functional/experiment requires manifest and bound policy record')
  protocol.identity(binding,manifest)
 policy=protocol.SPEC if source_only else binding['policy']
 if manifest is not None:(out/'build-manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
 if binding is not None:(out/'bound-policy.json').write_text(json.dumps(binding,indent=2)+'\n')
 if a.action=='experiment':
  if not a.quiet_authorization or not a.measurement_authorization:p.error('Experiment requires separate parent qualification/measurement cues')
  protocol.authorize(a.quiet_authorization,binding,manifest,'vod-category-source-qualification')
 tools=out/'tools';hashes=snapshot_sources(ROOT,tools)
 for folder in ['scripts/rust-vod-category-perf','scripts/rust-guide-perf','scripts/rust-m4-perf']:
  for path in (ROOT/folder).rglob('*'):
   if path.is_file()and'__pycache__'not in path.parts:
    data=path.read_bytes();target=tools/path.relative_to(ROOT);target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(data);hashes[str(path.relative_to(ROOT))]=hashlib.sha256(data).hexdigest()
 def module(name,path):
  spec=importlib.util.spec_from_file_location(name,path);value=importlib.util.module_from_spec(spec);spec.loader.exec_module(value);return value
 captured=module('captured_vod_protocol',tools/'scripts/rust-vod-category-perf/protocol.py');protocol=captured;sys.modules['protocol']=captured
 reader=module('captured_vod_reader',tools/'scripts/rust-vod-category-perf/reader.py')
 project='dispatcharr-category-functional-'+uuid.uuid4().hex[:10];report={'passed':False,'scope':policy['scope'],'baseline_commit':BASELINE,'baseline_image':'ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f','action':a.action,'performance_verified':False,'measurements_started':False,'project':project,'tool_source_sha256':hashes,'policy_sha256':protocol.digest(policy),'build_manifest_sha256':hashlib.sha256(a.build_manifest.read_bytes()).hexdigest()if a.build_manifest else None,'checks':[]}
 with fixture_source(report,False,out)as directory:
  source=Path(directory)/'source';source.mkdir();source.chmod(0o755);archive=subprocess.check_output(['git','archive',BASELINE],cwd=ROOT);report['baseline_archive_sha256']=hashlib.sha256(archive).hexdigest();subprocess.run(['tar','-x','--no-same-owner','-C',str(source)],input=archive,check=True);(source/'media').mkdir(exist_ok=True)
  env=dict(os.environ,BASELINE_SOURCE_DIR=str(source),BASELINE_EVIDENCE_DIR=str(out),DISPATCHARR_ENV='aio');candidate=candidate_overlay(tools/'docker/compose.rust-candidate.yml',out/'candidate.yml',manifest['tag']if manifest else'ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f')
  ports=out/'ports.yml';report['ports']=write_port_overlay(ports,{'baseline':[5656,6379],'source':[5656,9191],'candidate':[9192],'origin':[8080]})
  overlay=out/'vod-origin.yml';overlay.write_text('services:\n  origin:\n    entrypoint: [/dispatcharrpy/bin/python, /vod-origin-tools/logo_origin.py]\n    volumes:\n      - '+str(tools/'scripts/rust-integration')+':/vod-origin-tools:ro\n')
  compose=['docker','compose','-p',project,'-f',str(tools/'docker/compose.rust-playback.yml'),'-f',str(candidate),'-f',str(tools/'scripts/rust-m4-perf/compose.yml'),'-f',str(overlay),'-f',str(ports)]
  def command(args,body=None,timeout=180):
   result=subprocess.run(args,env=env,input=body,text=True,capture_output=True,timeout=timeout)
   if result.returncode:(out/'failed-command-private.log').write_text(result.stdout+'\n'+result.stderr);result.check_returncode()
   return result.stdout.strip()
  def endpoint(service,port):return'http://'+command(compose+['port',service,str(port)])
  def origin_counts():
   import urllib.request
   with urllib.request.urlopen(endpoint('origin',8080)+'/counts',timeout=2)as response:
    data=response.read(65537)
    if len(data)>65536:raise ValueError('Origin counter bound')
    value=json.loads(data)
    if value!={}:raise ValueError('Ordinary metadata reads unexpectedly contacted origin')
    return value
  def helper(op):
   result=command(compose+['exec','-T','-e','VOD_CATEGORY_PERF_DISPOSABLE=playback-fixture-only','baseline','/dispatcharrpy/bin/python','-c',(tools/'scripts/rust-vod-category-perf/fixture.py').read_text()],body=json.dumps({'op':op}),timeout=180);value=json.loads(result.splitlines()[-1]);
   if op=='phase':origin_counts();value['origin_requests']=0
   return value
  try:
   command(compose+['up','-d','baseline']);deadline=time.monotonic()+180
   while True:
    try:
     import urllib.request
     with urllib.request.urlopen(endpoint('baseline',5656)+'/api/core/version/',timeout=2)as response:assert response.status==200
     break
    except(OSError,AssertionError):
     if time.monotonic()>=deadline:raise RuntimeError('Baseline readiness deadline')
     time.sleep(.2)
   deadline=time.monotonic()+30
   while True:
    try:origin_counts();break
    except OSError:
     if time.monotonic()>=deadline:raise RuntimeError('Owned idle origin not ready')
     time.sleep(.1)
   report['idle_origin_verified']=True
   seed=helper('seed');(out/'fixture-private.json').write_text(json.dumps(seed,indent=2)+'\n');assert seed['categories']==242 and seed['nested_links']==501 and seed['content_tables_empty']
   report['fixture']={key:seed[key]for key in ['categories','nested_links','owned_accounts','active_XC_accounts','owned_profiles','content_tables_empty','fingerprinted_table_counts','fixture_sha256','schema_sha256']}
   command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/verification/supervisor.py','control','stop'])
   expected={}
   for backend,service,port in ([('source','source',5656)]if source_only else[('source','source',5656),('candidate','candidate',9192)]):
    command(compose+['up','-d','--no-deps','--no-build',service]);base=endpoint(service,port);deadline=time.monotonic()+90
    phase=helper('phase');assert phase['fixture_sha256']==seed['fixture_sha256']and phase['schema_sha256']==seed['schema_sha256']
    while True:
     try:
      with urllib.request.urlopen(base+'/api/core/version/',timeout=2)as response:assert response.status==200
      break
     except(OSError,AssertionError,TimeoutError):
      if time.monotonic()>=deadline:raise RuntimeError('VOD backend readiness deadline')
      time.sleep(.2)
    identifier=command(compose+['ps','-q',service]);inspection=json.loads(command(['docker','inspect',identifier]))[0]
    if backend=='candidate':report['candidate_binding']=validate_candidate_image(manifest,inspection)
    else:
     if inspection['Config']['Image']!=report['baseline_image']:raise ValueError('Original image provenance changed')
     report['source_binding']={'image_id':inspection['Image'],'configured_image':inspection['Config']['Image'],'container_id':inspection['Id']}
    assert inspection['HostConfig']['Memory']==512*1024**2 and inspection['HostConfig']['NanoCpus']==2*10**9
    report.setdefault('actual_limits',{})
    for selected_service,kind in [('baseline','infrastructure'),('origin','origin'),(service,'backend')]:
     selected_id=command(compose+['ps','--status','running','-q',selected_service])
     if not selected_id:raise ValueError('Required owned service not running: '+selected_service)
     selected_inspection=json.loads(command(['docker','inspect',selected_id]))[0];declared=policy['service_limits'][kind]
     if selected_inspection['HostConfig']['Memory']!=declared['memory_bytes']or selected_inspection['HostConfig']['NanoCpus']!=declared['nano_cpus']:raise ValueError('Declared functional service limits differ')
     report['actual_limits'][selected_service]={'id':selected_id,'memory_bytes':selected_inspection['HostConfig']['Memory'],'nano_cpus':selected_inspection['HostConfig']['NanoCpus']}

    for path_index,path in enumerate(protocol.PATHS):
     value,_,_=reader.request(base,path,phase['token'],expected=expected.get(path))
     if backend=='source':expected[path]=value
     for consumers in [1,10]:
      barrier=threading.Barrier(consumers+1)
      def fetch(index):
       barrier.wait(timeout=5);actual,_,_=reader.request(base,path,phase['token'],expected=expected[path]);return {'reader':index,'passed':True,'bytes':actual['bytes'],'sha256':actual['sha256']}
      with concurrent.futures.ThreadPoolExecutor(max_workers=consumers)as executor:
       jobs=[executor.submit(fetch,i)for i in range(consumers)];barrier.wait(timeout=5);rows=[job.result(timeout=15)for job in jobs]
      report['checks'].append({'backend':backend,'path_index':path_index,'consumers':consumers,'passed':all(row['passed']for row in rows),'body_bytes':value['bytes'],'body_sha256':value['sha256']})
    command(compose+['stop','--timeout','15',service]);after=helper('phase');assert after['fixture_sha256']==seed['fixture_sha256']and after['schema_sha256']==seed['schema_sha256']
   (out/'reference-private.json').write_text(json.dumps(expected,indent=2,ensure_ascii=False)+'\n')
   if a.action=='experiment':
    # Functional mode never imports this observer/orchestrator branch.
    sys.path.insert(0,str(tools/'scripts/rust-vod-category-perf'))
    from orchestrator import execute
    cache_spec=importlib.util.spec_from_file_location('vod_existing_owned_redis',tools/'scripts/rust-guide-perf/cache.py');cache_module=importlib.util.module_from_spec(cache_spec);cache_spec.loader.exec_module(cache_module)
    cache=cache_module.Cache(int(command(compose+['port','baseline','6379']).rsplit(':',1)[1]))
    def stop():
     command(compose+['stop','--timeout','15','source','candidate'])
     if command(compose+['ps','--status','running','-q','source','candidate']):raise ValueError('Both backends must stop before Redis reset')
    def start(backend):
     stop();cache.command('FLUSHDB')
     if cache.command('DBSIZE')!=0:raise ValueError('Owned Redis not cleared')
     service='source'if backend=='source'else'candidate';port=5656 if backend=='source'else 9192
     command(compose+['up','-d','--no-deps','--no-build','--force-recreate',service]);base=endpoint(service,port);phase=helper('phase');
     if any(phase[key]!=seed[key]for key in ['fixture_sha256','schema_sha256']):raise ValueError('Phase fixture/schema changed from functional reference')
     deadline=time.monotonic()+90
     while True:
      try:
       with urllib.request.urlopen(base+'/api/core/version/',timeout=2)as response:assert response.status==200
       break
      except(OSError,AssertionError):
       if time.monotonic()>=deadline:raise RuntimeError('Phase readiness deadline')
       time.sleep(.1)
     for path in protocol.PATHS:reader.request(base,path,phase['token'],expected=expected[path])
     return base,phase
    def selected(backend):
     result={}
     for service,kind in [('baseline','infrastructure'),('origin','origin'),('source'if backend=='source'else'candidate','backend')]:
      identifier=command(compose+['ps','--status','running','-q',service]);
      if not identifier:raise ValueError('Selected owned service not running: '+service)
      item=json.loads(command(['docker','inspect',identifier]))[0];limits=policy['service_limits'][kind]
      if item['HostConfig']['Memory']!=limits['memory_bytes']or item['HostConfig']['NanoCpus']!=limits['nano_cpus']:raise ValueError('Actual service limit mismatch')
      if service=='candidate':validate_candidate_image(manifest,item)
      if service=='source'and item['Config']['Image']!='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f':raise ValueError('Original image changed')
      result[service]={'id':item['Id'],'limits':limits}
     return result
    report['experiment']=execute(output=out/'experiment',manifest=manifest,binding=binding,tools=tools,reference=expected,start=start,stop=stop,helper=helper,select_services=selected,reader=reader,quiet=a.quiet_authorization,measurement_cue=a.measurement_authorization)
    report['measurements_started']=(out/'experiment/measurement-started.json').exists();report['performance_verified']=report['experiment'].get('performance_verified',False)
    assert report['experiment']['passed'],'Predeclared VOD experiment failed'
   if source_only and command(compose+['ps','--status','running','-q','candidate']):raise ValueError('Source-only action started candidate')
   report['source_only']=source_only;report['sampler_started']=False if a.action!='experiment'else True
   if a.action!='experiment'and any(name.endswith('observer')for name in sys.modules):raise ValueError('Functional mode imported an observer')
   report['passed']=True
  except Exception as error:report.update(error_type=type(error).__name__,error_message=str(error)[:1024])
  finally:
   report['measurements_started']=(out/'experiment/measurement-started.json').exists()
   try:command(compose+['down','--volumes','--remove-orphans'],timeout=60);report['cleanup']='owned project removed'
   except Exception:report.update(cleanup_error=True,passed=False)
 print(json.dumps({'passed':report['passed'],'output':str(out)}));return 0 if report['passed']else 1
if __name__=='__main__':raise SystemExit(main())
