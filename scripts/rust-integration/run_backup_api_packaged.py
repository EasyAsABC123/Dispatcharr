#!/usr/bin/env python3
"""Actual original job-free backup HTTP/filesystem contracts versus immutable app.
Original authors retain full credit. Owned shared fixture directory only; no jobs.
"""
import argparse,hashlib,json,os,subprocess,time,urllib.request,uuid
from pathlib import Path
from run import BASELINE,fixture_source,candidate_overlay,validate_candidate_image
from fixture_ports import write_port_overlay
from tool_snapshot import snapshot_sources
ROOT=Path(__file__).resolve().parents[2]
def main():
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',required=True,type=Path);parser.add_argument('--candidate-build-manifest',required=True,type=Path);args=parser.parse_args();out=args.output.resolve();out.mkdir(parents=True,exist_ok=True)
 if any(out.iterdir()):parser.error('Output must be empty')
 manifest=json.loads(args.candidate_build_manifest.read_text())
 if not manifest.get('passed'):parser.error('Passing immutable app manifest required')
 tools=out/'tool-source';hashes=snapshot_sources(ROOT,tools);project='dispatcharr-backup-files-'+uuid.uuid4().hex[:10];report={'passed':False,'runtime_spool_configuration':{'TMPDIR':'/data','root_filesystem_read_only':True},'scope':'job-free original/native backup HTTP/filesystem effects; raw exact responses and five explicitly named invalid204 header deltas recorded separately','project':project,'tool_source_sha256':hashes,'application_image_verified':True,'performance_verified':False,'backup_jobs_verified':False,'build_manifest_sha256':hashlib.sha256(args.candidate_build_manifest.read_bytes()).hexdigest()};status=1
 with fixture_source(report,False,out)as directory:
  source=Path(directory)/'source';source.mkdir();source.chmod(0o755);archive=subprocess.check_output(['git','archive',BASELINE],cwd=ROOT);subprocess.run(['tar','-x','--same-permissions','--no-same-owner','-C',str(source)],input=archive,check=True);(source/'media').mkdir(exist_ok=True)
  shared=out/'backup-shared-fixture';shared.mkdir();shared.chmod(0o777);env=dict(os.environ,BASELINE_SOURCE_DIR=str(source),BASELINE_EVIDENCE_DIR=str(out),DISPATCHARR_ENV='aio');candidate=candidate_overlay(tools/'docker/compose.rust-candidate.yml',out/'candidate-overlay.yml',manifest['tag'])
  overlay=out/'backup-files-fixture.yml';overlay.write_text('services:\n  baseline:\n    volumes:\n      - '+str(shared)+':/data/backups\n  candidate:\n    environment:\n      BACKUP_ROOT: /data/backups\n      TMPDIR: /data\n    volumes:\n      - '+str(shared)+':/data/backups\n  origin:\n    entrypoint: [/dispatcharrpy/bin/python, /backup-tools/logo_origin.py]\n    volumes:\n      - '+str(tools/'scripts/rust-integration')+':/backup-tools:ro\n')
  ports=out/'fixture-ports.yml';report['fixture_ports']=write_port_overlay(ports,{'baseline':[5656],'candidate':[9192],'origin':[8080]});compose=['docker','compose','-p',project,'-f',str(tools/'docker/compose.rust-playback.yml'),'-f',str(candidate),'-f',str(overlay),'-f',str(ports)]
  def cmd(command,body=None,timeout=180):return subprocess.check_output(command,env=env,input=body,text=True,stderr=subprocess.PIPE,timeout=timeout)
  def ready(service,path):
   port='5656'if service=='baseline'else'9192';base='http://'+cmd(compose+['port',service,port]).strip();deadline=time.monotonic()+180
   while True:
    try:
     with urllib.request.urlopen(base+path,timeout=2)as response:
      if response.status==200:return
    except OSError:
     if time.monotonic()>deadline:raise RuntimeError('Fixture readiness failed')
     time.sleep(.2)
  try:
   cmd(compose+['up','-d','baseline']);ready('baseline','/api/core/version/')
   program=(tools/'scripts/rust-integration/backup_api_fixture.py').read_text();result=subprocess.run(compose+['exec','-T','-e','BACKUP_API_DISPOSABLE_FIXTURE=playback-fixture-only','baseline','/dispatcharrpy/bin/python','-'],env=env,input=program,text=True,capture_output=True,timeout=240);(out/'source-worker.log').write_text(result.stderr);result.check_returncode();golden=json.loads(result.stdout.splitlines()[-1]);(out/'backup-api-fixture.json').write_text(json.dumps(golden,indent=2)+'\n')
   cmd(compose+['up','-d','--no-build','candidate']);container=cmd(compose+['ps','-q','candidate']).strip();report['candidate_image_binding']=validate_candidate_image(manifest,json.loads(cmd(['docker','inspect',container]))[0]);ready('candidate','/health/ready')
   program=(tools/'scripts/rust-integration/backup_api_packaged_probe.py').read_text();result=subprocess.run(compose+['exec','-T','-e','BACKUP_API_DISPOSABLE_FIXTURE=playback-fixture-only','baseline','/dispatcharrpy/bin/python','-'],env=env,input=program,text=True,capture_output=True,timeout=240);(out/'packaged-probe.log').write_text(result.stderr);result.check_returncode();actual=json.loads(result.stdout.splitlines()[-1]);(out/'native-checks.json').write_text(json.dumps(actual,indent=2)+'\n');passed=actual['secondary_passed']and actual['declared_source_invalid_204_length_deltas']==5
   report.update(passed=passed,raw_exact_wire_parity=actual['passed'],secondary_contract_acceptance=actual['secondary_passed'],declared_source_invalid_204_content_length_deltas=actual['declared_source_invalid_204_length_deltas'],comparison_cases=actual['source_cases'],large_upload_cases=len(golden['large_uploads']),exact_wire_cases=sum(v['passed']for v in actual['checks']),native_checks_sha256=hashlib.sha256((out/'native-checks.json').read_bytes()).hexdigest());status=0 if passed else 1
  except (OSError,ValueError,RuntimeError,subprocess.SubprocessError)as error:
   report['error_type']=type(error).__name__
   if isinstance(error,subprocess.CalledProcessError):(out/'failure-private.log').write_text(str(error.stderr or '')[:65536])
  finally:
   try:cmd(compose+['down','--volumes','--remove-orphans']);report['cleanup']='project removed'
   except subprocess.SubprocessError:report.update(cleanup_error=True,passed=False);status=1
 print(json.dumps({'passed':report['passed'],'evidence':str(out)}));return status
if __name__=='__main__':raise SystemExit(main())
