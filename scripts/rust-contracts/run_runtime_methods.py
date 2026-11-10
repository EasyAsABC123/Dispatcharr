#!/usr/bin/env python3
"""Discover pinned dispatch candidates in one owned original Compose fixture.
Original Dispatcharr authors retain all application credit and AGPL licensing.
"""
import argparse,hashlib,json,os,subprocess,sys,time,urllib.request,uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT/'scripts/rust-integration'))
from run import BASELINE,fixture_source
from tool_snapshot import snapshot_sources
from fixture_ports import write_port_overlay

def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',type=Path,required=True);a=p.parse_args();out=a.output.resolve();out.mkdir(parents=True,exist_ok=False);tools=out/'tool-source';hashes=snapshot_sources(ROOT,tools)
 for name in ['runtime_methods.py','run_runtime_methods.py']:
  source=Path(__file__).with_name(name);b=source.read_bytes();dest=tools/'scripts/rust-contracts'/name;dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes(b);hashes[str(source.relative_to(ROOT))]=hashlib.sha256(b).hexdigest()
 project='dispatcharr-contract-methods-'+uuid.uuid4().hex[:10];report={'passed':False,'scope':__doc__,'project':project,'original_revision':BASELINE,'tools_sha256':hashes,'parity_acceptance':False};status=1
 with fixture_source(report,False,out)as temp:
  source=Path(temp)/'source';source.mkdir();source.chmod(0o755);archive=subprocess.check_output(['git','-C',str(ROOT),'archive',BASELINE]);assert hashlib.sha256(archive).hexdigest()=='f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff';subprocess.run(['tar','-x','--same-permissions','--no-same-owner','-C',str(source)],input=archive,check=True);(source/'media').mkdir(exist_ok=True);report['original_archive_sha256']=hashlib.sha256(archive).hexdigest()
  env=dict(os.environ,BASELINE_SOURCE_DIR=str(source),BASELINE_EVIDENCE_DIR=str(out),DISPATCHARR_ENV='aio');overlay=out/'method-fixture.yml';overlay.write_text('services:\n  baseline:\n    volumes: ["'+str(tools/'scripts/rust-contracts')+':/method-tools:ro"]\nnetworks:\n  default:\n    internal: true\n');ports=out/'ports.yml';report['ports']=write_port_overlay(ports,{'baseline':[5656],'origin':[8080]});compose=['docker','compose','-p',project,'-f',str(tools/'docker/compose.rust-playback.yml'),'-f',str(overlay),'-f',str(ports)]
  def command(args,timeout=180):return subprocess.check_output(args,env=env,text=True,stderr=subprocess.PIPE,timeout=timeout)
  try:
   command(compose+['up','-d','baseline']);deadline=time.monotonic()+180
   while True:
    try:
     command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','-c',"import urllib.request; assert urllib.request.urlopen('http://127.0.0.1:5656/api/core/version/',timeout=2).status==200"],timeout=5);break
    except subprocess.SubprocessError:
     if time.monotonic()>deadline:raise RuntimeError('Original readiness failed')
     time.sleep(.2)
   raw=command(compose+['exec','-T','-e','CONTRACT_METHOD_DISPOSABLE_FIXTURE=playback-fixture-only','baseline','/dispatcharrpy/bin/python','/method-tools/runtime_methods.py']);data=json.loads(raw.splitlines()[-1]);old=json.loads((ROOT/'docs/rust-runtime-contracts.json').read_text())['routes'];identity=lambda r:(r['pattern'],r['name'],r['view']);assert [identity(r)for r in data['routes']]==[identity(r)for r in old];assert data['view_invocations']==data['inspection_SQL_statements']==0
   result=out/'runtime-methods.json';result.write_text(json.dumps(data,indent=2)+'\n');report.update(passed=True,routes=len(data['routes']),handler_candidates_available=sum(r['handler_method_candidates']is not None for r in data['routes']),methodless_before=sum(r['methods']is None for r in old),new_handler_candidates_for_methodless=sum(o['methods']is None and r['handler_method_candidates']is not None for o,r in zip(old,data['routes'])),result_sha256=hashlib.sha256(result.read_bytes()).hexdigest());status=0
  except (OSError,ValueError,AssertionError,RuntimeError,subprocess.SubprocessError)as error:
   report['error_type']=type(error).__name__
   if isinstance(error,subprocess.CalledProcessError):(out/'failure.log').write_text(error.stderr or str(error))
  finally:
   try:command(compose+['down','--volumes','--remove-orphans']);report['cleanup']='owned project removed'
   except subprocess.SubprocessError:report.update(passed=False,cleanup='failed');status=1
   (out/'method-inventory.json').write_text(json.dumps(report,indent=2)+'\n')
 print(json.dumps({'passed':report['passed'],'evidence':str(out)}));return status
if __name__=='__main__':raise SystemExit(main())
