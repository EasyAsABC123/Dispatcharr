#!/usr/bin/env python3
"""Dedicated owned Original refresh capture and optional native-transport gate. No builds/resource sampling.
Original Dispatcharr authors retain application credit. Secrets only via stdin.
"""
import argparse,hashlib,json,os,shutil,subprocess,sys,tempfile,time,uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/'scripts/rust-integration')]
from fixture_ports import write_port_overlay
from run import candidate_overlay,validate_candidate_image
BASELINE='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
ARCHIVE='f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff'
def main():
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',type=Path,required=True);parser.add_argument('--build-manifest',type=Path,required=True);parser.add_argument('--source-only',action='store_true',default=True);parser.add_argument('--native-http-binary',type=Path);parser.add_argument('--native-actor-binary',type=Path);parser.add_argument('--case-filter');parser.add_argument('--schedule-case',choices=['movie_third_recent_skip','series_third_old_snapshot','movie_caller_lost','series_caller_lost','series_third_old_account_agent_relation']);parser.add_argument('--observer',choices=['provider_refresh_schedule_source.py','provider_refresh_full_http_source.py','provider_refresh_source.py','provider_refresh_mixed_source.py','provider_refresh_mixed_series_source.py','provider_refresh_fair_physical.py'],default='provider_refresh_source.py');args=parser.parse_args()
 manifest=json.loads(args.build_manifest.read_text());assert manifest['passed']
 out=args.output.resolve();out.mkdir(parents=True,exist_ok=False);tools=out/'tools';tools.mkdir()
 report={'passed':False,'project':'dispatcharr-provider-refresh-'+uuid.uuid4().hex[:10],'scope':__doc__,'source_only':args.source_only,'commands':[]}
 selected=[*[Path(__file__).with_name(name)for name in ['provider_refresh_schedule_source.py','provider_refresh_full_http_source.py','provider_refresh_http_driver.py','provider_refresh_http_assertions.py']],Path(__file__).with_name('provider_refresh_mixed_series_source.py'),Path(__file__).with_name('provider_refresh_fair_physical.py'),Path(__file__).with_name('provider_refresh_mixed_source.py'),Path(__file__).with_name('provider_refresh_source.py'),Path(__file__).with_name('provider_refresh_helpers_source.py'),Path(__file__),ROOT/'scripts/rust-vod-api-contracts/source.py',*[ROOT/'scripts/rust-integration'/n for n in ['fixture_ports.py','run.py','tool_snapshot.py']],ROOT/'docker/compose.rust-playback.yml',ROOT/'docker/compose.rust-candidate.yml',ROOT/'scripts/rust-baseline/dependencies.py',*[ROOT/'scripts/rust-playback-baseline'/n for n in ['seed.py','origin.py','range_http.py','start.sh','supervisor.py','uwsgi.ini']]]
 for file in selected:
  if file.is_file():
   target=tools/file.relative_to(ROOT);target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(file,target)
 if args.native_http_binary:
  target=tools/'scripts/rust-vod-api-contracts/native-runtime/refresh-http-tests';target.parent.mkdir(parents=True);shutil.copyfile(args.native_http_binary,target);target.chmod(0o755)
 if args.native_actor_binary:
  target=tools/'scripts/rust-vod-api-contracts/native-runtime/refresh-actor-tests';target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(args.native_actor_binary,target);target.chmod(0o755)
 report['tools_sha256']={str(p.relative_to(tools)):hashlib.sha256(p.read_bytes()).hexdigest()for p in tools.rglob('*')if p.is_file()}
 shutil.copyfile(args.build_manifest,out/'build-manifest.json');report['manifest_sha256']=hashlib.sha256(args.build_manifest.read_bytes()).hexdigest()
 source=out/'original-source';source.mkdir();archive=subprocess.check_output(['git','archive',BASELINE],cwd=ROOT);assert hashlib.sha256(archive).hexdigest()==ARCHIVE
 subprocess.run(['tar','-x','--no-same-owner','-C',str(source)],input=archive,check=True);(source/'media').mkdir(exist_ok=True);(source/'frontend/dist').mkdir(parents=True,exist_ok=True)
 report['Original_archive_sha256']=ARCHIVE
 env=dict(os.environ,BASELINE_SOURCE_DIR=str(source),BASELINE_EVIDENCE_DIR=str(out),FIXTURE_SECONDS='900',DISPATCHARR_ENV='aio')
 def command(argv,timeout=180,payload=None):
  report['commands'].append(argv);result=subprocess.run(argv,cwd=tools,env=env,input=payload,text=True,capture_output=True,timeout=timeout)
  if result.returncode:(out/'failed-command.log').write_text(result.stdout+'\n'+result.stderr);result.check_returncode()
  return result
 ports=out/'ports.yml';report['ports']=write_port_overlay(ports,{'baseline':[5656],'origin':[8080],**({}if args.source_only else{'candidate':[9192]})})
 compiled=out/'compiled-frontend';compiled.mkdir();image=json.loads(command(['docker','image','inspect',manifest['tag']]).stdout)[0];assert image['Id']==manifest['image_id']
 donor=command(['docker','create',manifest['tag']]).stdout.strip()
 try:command(['docker','cp',donor+':/opt/dispatcharr-frontend/.',str(compiled)])
 finally:command(['docker','rm',donor])
 report['frontend_sha256']={str(p.relative_to(compiled)):hashlib.sha256(p.read_bytes()).hexdigest()for p in compiled.rglob('*')if p.is_file()};assert 'index.html'in report['frontend_sha256']
 ini=out/'browser.ini';ini.write_text((tools/'scripts/rust-playback-baseline/uwsgi.ini').read_text()+'\nstatic-map = /static=/app/frontend/dist\nstatic-map = /assets=/app/frontend/dist/assets\ncheck-static = /app/frontend/dist\n')
 overlay=out/'modal.yml';overlay.write_text(json.dumps({'services':{'baseline':{'environment':{'DISPATCHARR_ENABLE_IP_LOOKUP':'false'},'volumes':[str(tools/'scripts/rust-vod-api-contracts')+':/vod-api-probe:ro',str(compiled)+':/app/frontend/dist:ro',str(ini)+':/verification/uwsgi.ini:ro']},**({}if args.source_only else{'candidate':{'environment':{'DISPATCHARR_ENABLE_IP_LOOKUP':'false'}}})}}))
 compose=['docker','compose','-p',report['project'],'-f',str(tools/'docker/compose.rust-playback.yml')]
 if not args.source_only:compose+=['-f',str(candidate_overlay(tools/'docker/compose.rust-candidate.yml',out/'candidate.yml',manifest['tag']))]
 compose+=['-f',str(overlay),'-f',str(ports)]
 def ready(service,port):
  deadline=time.monotonic()+180
  while True:
   import urllib.request
   try:
    with urllib.request.urlopen('http://'+command(compose+['port',service,str(port)]).stdout.strip()+'/api/core/version/',timeout=2)as r:assert r.status==200
    return
   except Exception:
    if time.monotonic()>deadline:raise RuntimeError(service+' readiness deadline')
    time.sleep(.25)
 try:
  command(compose+['up','-d','--no-build','baseline']);ready('baseline',5656)
  seeded=command(compose+['exec','-T','-e','VOD_API_DISPOSABLE_FIXTURE=playback-fixture-only','-e','VOD_API_OBSERVATION=http','baseline','/dispatcharrpy/bin/python','/vod-api-probe/source.py'],timeout=300)
  corpus=json.loads(seeded.stdout.splitlines()[-1]);assert corpus['capture_complete'] and not corpus['provider_trap_attempts'];(out/'Source-private.json').write_text(json.dumps(corpus,indent=2)+'\n');os.chmod(out/'Source-private.json',0o600)
  source_base='http://'+command(compose+['port','baseline','5656']).stdout.strip();candidate_base=None
  if not args.source_only:
   command(compose+['up','-d','--no-build','candidate']);ready('candidate',9192)
   inspection=json.loads(command(['docker','inspect',command(compose+['ps','-q','candidate']).stdout.strip()]).stdout)[0];report['candidate_binding']=validate_candidate_image(manifest,inspection);candidate_base='http://'+command(compose+['port','candidate','9192']).stdout.strip()
  baseline=json.loads(command(['docker','inspect',command(compose+['ps','-q','baseline']).stdout.strip()]).stdout)[0];networks=baseline['NetworkSettings']['Networks'];assert len(networks)==1;ip=next(iter(networks.values()))['IPAddress']
  observed=command(compose+['exec','-T','-e','VOD_API_DISPOSABLE_FIXTURE=playback-fixture-only',*(['-e','VOD_REFRESH_HTTP_NATIVE=/vod-api-probe/native-runtime/refresh-http-tests']if args.native_http_binary else[]),*(['-e','VOD_REFRESH_ACTOR_NATIVE=/vod-api-probe/native-runtime/refresh-actor-tests']if args.native_actor_binary else[]),*(['-e','VOD_REFRESH_CASE_FILTER='+args.case_filter]if args.case_filter else[]),*(['-e','VOD_REFRESH_SCHEDULE_CASE='+args.schedule_case]if args.schedule_case else[]),'baseline','/dispatcharrpy/bin/python','/vod-api-probe/'+args.observer],timeout=300)
  (out/'refresh-worker-private.log').write_text(observed.stderr);os.chmod(out/'refresh-worker-private.log',0o600)
  captured=json.loads(observed.stdout.splitlines()[-1]);(out/'provider-refresh-Source-private.json').write_text(json.dumps(captured,indent=2)+'\n');os.chmod(out/'provider-refresh-Source-private.json',0o600)
  report['observer']=args.observer;report['refresh_cases']=len(captured['records']);report['restored18']=captured['restored18'];report['passed']=captured['passed']and captured['restored18']
  helpers=command(compose+['exec','-T','-e','VOD_API_DISPOSABLE_FIXTURE=playback-fixture-only','baseline','/dispatcharrpy/bin/python','/vod-api-probe/provider_refresh_helpers_source.py'],timeout=30)
  helper_data=json.loads(helpers.stdout.splitlines()[-1]);(out/'provider-refresh-helpers-Source.json').write_text(json.dumps(helper_data,indent=2)+'\n');report['helper_cases']=helper_data['count']

 except BaseException as error:report['failure']=type(error).__name__+': '+str(error);raise
 finally:
  try:
   report['container_logs']=command(compose+['logs','--no-color','--tail','100'],timeout=30).stdout;(out/'container-logs-private.log').write_text(report.pop('container_logs'));os.chmod(out/'container-logs-private.log',0o600)
  finally:
   command(compose+['down','--volumes','--remove-orphans','--timeout','5'],timeout=60);report['cleanup']='Owned Compose containers/networks/volumes removed';report['remaining_owned_containers']=command(['docker','ps','-aq','--filter','label=com.docker.compose.project='+report['project']]).stdout.strip();assert not report['remaining_owned_containers'];(out/'integration.json').write_text(json.dumps(report,indent=2)+'\n')
if __name__=='__main__':main()
