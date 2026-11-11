#!/usr/bin/env python3
"""Owned Original-first unchanged React live playback/statistics gate.
Original Dispatcharr authors retain all application/algorithm credit/licenses.
"""
import argparse, hashlib, json, os, shutil, subprocess, sys, time, uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts/rust-integration'))
from fixture_ports import write_port_overlay
from run import candidate_overlay, validate_candidate_image
REV='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
ARCHIVE='f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff'
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',type=Path,required=True);p.add_argument('--build-manifest',type=Path,required=True);p.add_argument('--source-only',action='store_true');a=p.parse_args()
 out=a.output.resolve();out.mkdir(parents=True,exist_ok=False);manifest=json.loads(a.build_manifest.read_text());assert manifest['passed']
 tools=out/'tools';shutil.copytree(ROOT/'scripts',tools/'scripts',ignore=shutil.ignore_patterns('__pycache__'));shutil.copytree(ROOT/'docker',tools/'docker');own=tools/'scripts/rust-live-browser'
 report={'passed':False,'scope':__doc__,'project':'dispatcharr-live-browser-'+uuid.uuid4().hex[:10],'source_only':a.source_only,'observations':{},'tools_sha256':{str(p.relative_to(tools)):hashlib.sha256(p.read_bytes()).hexdigest()for p in tools.rglob('*')if p.is_file()},'build_manifest_sha256':hashlib.sha256(a.build_manifest.read_bytes()).hexdigest(),'candidate_image':manifest['image_id']}
 source=out/'original-source';source.mkdir();raw=subprocess.check_output(['git','archive',REV],cwd=ROOT);assert hashlib.sha256(raw).hexdigest()==ARCHIVE;subprocess.run(['tar','-x','--no-same-owner','-C',str(source)],input=raw,check=True);(source/'media').mkdir(exist_ok=True);(source/'frontend/dist').mkdir(parents=True,exist_ok=True)
 env=dict(os.environ,BASELINE_SOURCE_DIR=str(source),BASELINE_EVIDENCE_DIR=str(out),FIXTURE_SECONDS='900',FIXTURE_CODEC='h264',BENCHMARK_SHUTDOWN_DELAY='5',DISPATCHARR_ENV='aio')
 def command(argv,timeout=180,body=None):
  result=subprocess.run(argv,env=env,input=body,text=True,capture_output=True,timeout=timeout)
  if result.returncode:(out/'failed-command-private.log').write_text(result.stdout+'\n'+result.stderr);result.check_returncode()
  return result.stdout
 compiled=out/'compiled-frontend';compiled.mkdir();donor=command(['docker','create',manifest['image_id']]).strip()
 try:command(['docker','cp',donor+':/opt/dispatcharr-frontend/.',str(compiled)])
 finally:command(['docker','rm',donor])
 report['frontend_sha256']={str(p.relative_to(compiled)):hashlib.sha256(p.read_bytes()).hexdigest()for p in compiled.rglob('*')if p.is_file()};assert 'index.html'in report['frontend_sha256']
 ini=out/'browser.ini';ini.write_text((tools/'scripts/rust-playback-baseline/uwsgi.ini').read_text()+'\nstatic-map = /static=/app/frontend/dist\nstatic-map = /assets=/app/frontend/dist/assets\ncheck-static = /app/frontend/dist\n')
 overlay=out/'browser.yml';overlay.write_text(json.dumps({'services':{'baseline':{'environment':{'DISPATCHARR_ENABLE_IP_LOOKUP':'false'},'volumes':[str(own)+':/live-browser:ro',str(compiled)+':/app/frontend/dist:ro',str(ini)+':/verification/uwsgi.ini:ro']},**({}if a.source_only else{'candidate':{'environment':{'DISPATCHARR_ENABLE_IP_LOOKUP':'false'}}})}}))
 ports=out/'ports.yml';write_port_overlay(ports,{'baseline':[5656],'origin':[8080],**({}if a.source_only else{'candidate':[9192]})})
 compose=['docker','compose','-p',report['project'],'-f',str(tools/'docker/compose.rust-playback.yml')]
 if not a.source_only:compose+=['-f',str(candidate_overlay(tools/'docker/compose.rust-candidate.yml',out/'candidate.yml',manifest['image_id']))]
 compose+=['-f',str(overlay),'-f',str(ports)]
 def endpoint(service,port):return 'http://'+command(compose+['port',service,str(port)]).strip()
 def ready(service,port):
  import urllib.request
  end=time.monotonic()+180
  while time.monotonic()<end:
   try:
    with urllib.request.urlopen(endpoint(service,port)+'/api/core/version/',timeout=2)as r:
     if r.status==200:return
   except OSError:pass
   time.sleep(.25)
  raise TimeoutError('Readiness '+service)
 try:
  command(compose+['up','-d','--no-build','baseline']);ready('baseline',5656)
  identity=json.loads(command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/live-browser/fixture.py']).splitlines()[-1]);seed=json.loads((out/'seed.json').read_text())
  for name,service,port in [('Original','baseline',5656)]+([]if a.source_only else[('native','candidate',9192)]):
   if service=='candidate':
    command(compose+['up','-d','--no-build','candidate']);ready(service,port);inspect=json.loads(command(['docker','inspect',command(compose+['ps','-q','candidate']).strip()]))[0];report['candidate_binding']=validate_candidate_image(manifest,inspect)
   config={'url':endpoint(service,port),'username':identity['username'],'password':identity['password'],'channel_name':'Offline benchmark channel 0','channel_uuid':seed['channel_uuid']}
   process=subprocess.run(['node',str(own/'browser.js')],input=json.dumps(config)+'\n',env=env,text=True,capture_output=True,timeout=180);values=[json.loads(line)for line in process.stdout.splitlines()if line.startswith('{')];observation={'exit_code':process.returncode,'capability':values[0]if values else None,'result':values[-1]if values else None};report['observations'][name]=observation;(out/('browser-'+name+'.json')).write_text(json.dumps(observation,indent=2)+'\n');assert process.returncode==0 and observation['result']['passed'],name+' browser gate failed'
   # Browser exit physically closes its sockets. Wait for Source/native upstream
   # shutdown before the next backend; do not overlap independent player phases.
   origin=endpoint('origin',8080);deadline=time.monotonic()+60
   import urllib.request
   while True:
    with urllib.request.urlopen(origin+'/stats',timeout=3)as r:stats=json.load(r)
    active=stats.get('active',stats.get('active_connections'))
    if active==0:break
    if time.monotonic()>deadline:raise TimeoutError('Upstream cleanup')
    time.sleep(.2)
  report['passed']=True
 except BaseException as error:report['failure']=type(error).__name__+': '+str(error);raise
 finally:
  try:(out/'container-logs-private.log').write_text(command(compose+['logs','--no-color','--tail','100'],timeout=30))
  finally:
   command(compose+['down','--volumes','--remove-orphans','--timeout','5'],timeout=60);report['cleanup']='Owned Compose project removed';report['remaining_owned_resources']={kind:command(['docker',kind,'ls','-q','--filter','label=com.docker.compose.project='+report['project']]).splitlines()for kind in ['container','network','volume']};assert not any(report['remaining_owned_resources'].values());(out/'integration.json').write_text(json.dumps(report,indent=2)+'\n')
if __name__=='__main__':main()
