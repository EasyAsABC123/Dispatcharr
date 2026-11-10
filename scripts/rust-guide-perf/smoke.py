#!/usr/bin/env python3
"""Functional stored-guide fixture/load smoke only; never samples performance.
Original Dispatcharr authors retain full application credit. No matrix/quiet authorization implied.
"""
import argparse,concurrent.futures,hashlib,importlib.util,json,os,subprocess,sys,time,uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT/'scripts/rust-integration'))
from run import BASELINE,fixture_source,candidate_overlay,validate_candidate_image
from fixture_ports import write_port_overlay
from tool_snapshot import snapshot_sources
from plan import TREE

def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',required=True,type=Path);p.add_argument('--candidate-build-manifest',required=True,type=Path);p.add_argument('--action',choices=['functional','observer-smoke','instrumentation-smoke','experiment'],default='functional');p.add_argument('--quiet-authorization',type=Path);p.add_argument('--measurement-authorization',type=Path);a=p.parse_args();out=a.output.resolve();out.mkdir(parents=True,exist_ok=True)
 if any(out.iterdir()):p.error('Output must be empty')
 manifest=json.loads(a.candidate_build_manifest.read_text())
 if manifest.get('passed')is not True or manifest.get('source_tree_sha256')!=TREE:p.error('Passing exact23 source/recovery manifest required')
 if a.action=='experiment':
  if not a.quiet_authorization or not a.measurement_authorization:p.error('Experiment needs parent qualification and subsequent measurement quiet-cue paths')
  from qualification import authorize
  authorize(a.quiet_authorization,manifest)
 tools=out/'tool-source';hashes=snapshot_sources(ROOT,tools)
 for folder in ['scripts/rust-guide-perf','scripts/rust-m4-perf']:
  for path in (ROOT/folder).rglob('*'):
   if path.is_file()and'__pycache__'not in path.parts:
    data=path.read_bytes();target=tools/path.relative_to(ROOT);target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(data);hashes[str(path.relative_to(ROOT))]=hashlib.sha256(data).hexdigest()
 spec=importlib.util.spec_from_file_location('captured_guide_reader',tools/'scripts/rust-guide-perf/reader.py');reader=importlib.util.module_from_spec(spec);spec.loader.exec_module(reader)
 project='dispatcharr-guide-smoke-'+uuid.uuid4().hex[:10];report={'passed':False,'scope':'Stored-guide functional request/cache concurrency smoke; not performance measurement','measurements_started':False,'performance_verified':False,'project':project,'tool_source_sha256':hashes,'build_manifest_sha256':hashlib.sha256(a.candidate_build_manifest.read_bytes()).hexdigest(),'checks':[]};status=1
 with fixture_source(report,False,out)as directory:
  source=Path(directory)/'source';source.mkdir();source.chmod(0o755);subprocess.run(['tar','-x','--same-permissions','--no-same-owner','-C',str(source)],input=subprocess.check_output(['git','archive',BASELINE],cwd=ROOT),check=True);(source/'media').mkdir(exist_ok=True)
  environment=dict(os.environ,BASELINE_SOURCE_DIR=str(source),BASELINE_EVIDENCE_DIR=str(out),DISPATCHARR_ENV='aio');candidate=candidate_overlay(tools/'docker/compose.rust-candidate.yml',out/'candidate-overlay.yml',manifest['tag'])
  overlay=out/'guide-fixture.yml';overlay.write_text('services:\n  origin:\n    entrypoint: [/dispatcharrpy/bin/python, /guide-tools/logo_origin.py]\n    volumes:\n      - '+str(tools/'scripts/rust-integration')+':/guide-tools:ro\n')
  ports=out/'fixture-ports.yml';report['ports']=write_port_overlay(ports,{'baseline':[5656,6379],'source':[5656,9191],'candidate':[9192],'origin':[8080]});compose=['docker','compose','-p',project,'-f',str(tools/'docker/compose.rust-playback.yml'),'-f',str(candidate),'-f',str(tools/'scripts/rust-m4-perf/compose.yml'),'-f',str(overlay),'-f',str(ports)]
  def command(args,body=None,timeout=180):return subprocess.check_output(args,env=environment,input=body,text=True,stderr=subprocess.PIPE,timeout=timeout)
  def endpoint(service,port):return'http://'+command(compose+['port',service,str(port)]).strip()
  def helper(op):
   result=subprocess.run(compose+['exec','-T','-e','GUIDE_PERF_DISPOSABLE=playback-fixture-only','baseline','/dispatcharrpy/bin/python','-c',(tools/'scripts/rust-guide-perf/fixture.py').read_text()],env=environment,input=json.dumps({'op':op}),capture_output=True,text=True,timeout=120);(out/'last-fixture-private.log').write_text(result.stderr);result.check_returncode();return json.loads(result.stdout.splitlines()[-1])
  try:
   command(compose+['up','-d','baseline']);deadline=time.monotonic()+180
   while True:
    try:
     import urllib.request
     with urllib.request.urlopen(endpoint('baseline',5656)+'/api/core/version/',timeout=2)as response:
      if response.status==200:break
    except OSError:
     if time.monotonic()>deadline:raise RuntimeError('Infrastructure readiness failed')
     time.sleep(.2)
   seed=helper('seed');(out/'fixture-private.json').write_text(json.dumps(seed,indent=2)+'\n');report['fixture_counts']={key:seed[key]for key in ['channels','guides','stored_programmes']};command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/verification/supervisor.py','control','stop'])
   expected={};current={'kind':'json','method':'POST','path':'/api/epg/current-programs/','body':{'channel_uuids':seed['channel_uuids']}};xml={'kind':'xml','method':'GET','path':'/output/epg?days=1&prev_days=0'}
   for backend,service,port in [('source','source',5656),('candidate','candidate',9192)]:
    command(compose+['up','-d','--no-deps','--no-build',service]);base=endpoint(service,port);deadline=time.monotonic()+90
    while True:
     try:value,_,_=reader.request(base,current,seed['token']);break
     except (OSError,ValueError,TimeoutError):
      if time.monotonic()>deadline:raise RuntimeError('Guide backend readiness failed')
      time.sleep(.2)
    identifier=command(compose+['ps','-q',service]).strip();inspection=json.loads(command(['docker','inspect',identifier]))[0]
    if backend=='candidate':report['candidate_image_binding']=validate_candidate_image(manifest,inspection)
    limits=inspection['HostConfig'];assert limits['Memory']==512*1024**2 and limits['NanoCpus']==2*10**9
    if backend=='source':expected['current']=value;assert len(value['value'])==500
    assert reader.canonical(value)==reader.canonical(expected['current']);report['checks'].append({'backend':backend,'kind':'current500','passed':True})
    if backend=='source':
     checked,_,_=reader.request(base,{**xml,'validate_xml':True});counts=checked.pop('xml_counts');assert counts=={'channels':500,'programmes':22500};expected['xml']=checked;report['reference_xml_counts']=counts
    for cache,users in [('cold',1),('hot',1),('cold',10),('hot',10)]:
     if cache=='cold':helper('clear_cache')
     reference=expected.get('xml',reader.UNSET)
     def fetch(_):return reader.request(base,xml,timeout=10,expected=reference)[0]
     with concurrent.futures.ThreadPoolExecutor(max_workers=users)as pool:values=list(pool.map(fetch,range(users)))
     if 'xml'not in expected:expected['xml']=values[0]
     assert all(reader.canonical(v)==reader.canonical(expected['xml'])for v in values)
     report['checks'].append({'backend':backend,'kind':'xml-'+cache,'consumers':users,'passed':True,'body_bytes':expected['xml']['bytes'],'body_sha256':expected['xml']['sha256']})
    command(compose+['stop','--timeout','15',service])
   if a.action!='functional':
    sys.path.insert(0,str(tools/'scripts/rust-guide-perf'))
    from cache import Cache
    import orchestrator
    from plan import POLICY
    cache=Cache(int(command(compose+['port','baseline','6379']).strip().rsplit(':',1)[1]))
    def stop_backends():command(compose+['stop','--timeout','15','source','candidate'])
    def start_backend(backend):
     stop_backends();cache.command('FLUSHDB')
     if cache.command('DBSIZE')!=0:raise RuntimeError('Owned Redis reset incomplete')
     service='source'if backend=='source'else'candidate';port=5656 if backend=='source'else 9192
     command(compose+['up','-d','--no-deps','--no-build','--force-recreate',service])
     base=endpoint(service,port);deadline=time.monotonic()+90
     phase=helper('phase')
     while True:
      try:reader.request(base,current,phase['token'],expected=expected['current']);break
      except(OSError,ValueError,TimeoutError):
       if time.monotonic()>deadline:raise RuntimeError('Guide phase backend readiness failed')
       time.sleep(.2)
     return base
    def selected_services(backend):
     result={}
     for service,kind in [('baseline','infrastructure'),('origin','origin'),('source'if backend=='source'else'candidate','backend')]:
      identifier=command(compose+['ps','--status','running','-q',service]).strip()
      item=json.loads(command(['docker','inspect',identifier]))[0];limit=POLICY['service_limits'][kind]
      if item['HostConfig']['Memory']!=limit['memory_bytes']or item['HostConfig']['NanoCpus']!=limit['nano_cpus']:raise ValueError('Inspected service limits differ from frozen policy')
      if service=='candidate':validate_candidate_image(manifest,item)
      if service=='source'and item['Config']['Image']!='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f':raise ValueError('Source image changed')
      result[service]={'id':item['Id'],'limits':limit}
     return result
    shared=dict(manifest=manifest,tools=tools,seed=seed,reference=expected,current=current,xml=xml,helper=helper,cache=cache,start=start_backend,stop=stop_backends,select_services=selected_services)
    if a.action in ['observer-smoke','instrumentation-smoke']:
     observed=orchestrator.run(action=a.action,output=out,**shared)
     report['observer_smoke']=observed
     if not observed['passed']:raise RuntimeError('Guide observer smoke failed')
    else:
     qualification_dir=out/'qualification';qualification_dir.mkdir()
     qualified=orchestrator.run(action='qualify',output=qualification_dir,quiet_path=a.quiet_authorization,**shared)
     if not qualified['passed']:raise RuntimeError('Source qualification failed')
     report['qualification']=qualified
     print(json.dumps({'state':'awaiting-parent-measurement-quiet-cue','frozen_plan':str(qualification_dir/'frozen-plan.json'),'plan_sha256':qualified['plan_sha256']}),flush=True)
     deadline=time.monotonic()+1200
     while not a.measurement_authorization.exists():
      if time.monotonic()>deadline:raise TimeoutError('Parent measurement cue not issued within20minutes')
      time.sleep(1)
     matrix_dir=out/'matrix';matrix_dir.mkdir()
     measured=orchestrator.run(action='matrix',output=matrix_dir,quiet_path=a.measurement_authorization,plan_path=qualification_dir/'frozen-plan.json',qualification_path=qualification_dir/'source-qualification.json',**shared)
     report['matrix']=measured
     if not measured['passed']:raise RuntimeError('Guide predeclared matrix failed')
   report['passed']=True;status=0
  except (OSError,ValueError,AssertionError,RuntimeError,subprocess.SubprocessError)as error:
   report['error_type']=type(error).__name__
   if isinstance(error,subprocess.CalledProcessError):(out/'failed-command-private.log').write_text(str(error.stderr or '')[:65536])
  finally:
   try:command(compose+['down','--volumes','--remove-orphans']);report['cleanup']='project removed'
   except subprocess.SubprocessError:report.update(cleanup_error=True,passed=False);status=1
 print(json.dumps({'passed':report['passed'],'evidence':str(out)}));return status
if __name__=='__main__':raise SystemExit(main())
