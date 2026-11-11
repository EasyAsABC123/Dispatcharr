#!/usr/bin/env python3
"""Owned actual Original/native channel UUID action gate; no resource claim.
Original Dispatcharr authors retain all application/algorithm credit/licenses.
"""
import argparse,hashlib,json,os,subprocess,sys,time,uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts/rust-integration'))
from run import fixture_source
from fixture_ports import write_port_overlay
REV='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',type=Path,required=True);p.add_argument('--target',type=Path,required=True);a=p.parse_args();out=a.output.resolve();out.mkdir(parents=True,exist_ok=False)
 report={'passed':False,'project':'dispatcharr-channel-uuids-'+uuid.uuid4().hex[:10],'performance_verified':False,'tools_sha256':{str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest()for folder in ['scripts','docker','rust']for p in (ROOT/folder).rglob('*')if p.is_file()and not set(p.parts)&{'__pycache__','target'}},'Source_revision':REV}
 with fixture_source(report,False,out)as temporary:
  source=Path(temporary)/'source';source.mkdir();raw=subprocess.check_output(['git','archive',REV],cwd=ROOT);assert hashlib.sha256(raw).hexdigest()=='f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff';subprocess.run(['tar','-x','--no-same-owner','-C',str(source)],input=raw,check=True);(source/'media').mkdir(exist_ok=True)
  env=dict(os.environ,BASELINE_SOURCE_DIR=str(source),BASELINE_EVIDENCE_DIR=str(out),FIXTURE_SECONDS='900')
  overlay=out/'fixture.yml';overlay.write_text(json.dumps({'services':{'baseline':{'volumes':[str(ROOT/'scripts/rust-channel-uuids')+':/uuid-tools:ro']}}}));ports=out/'ports.yml';write_port_overlay(ports,{'baseline':[5656,5432],'origin':[8080]});compose=['docker','compose','-p',report['project'],'-f',str(ROOT/'docker/compose.rust-playback.yml'),'-f',str(overlay),'-f',str(ports)]
  def command(argv,timeout=180):
   result=subprocess.run(argv,env=env,text=True,capture_output=True,timeout=timeout)
   if result.returncode:(out/'failed-command-private.log').write_text(result.stdout+'\n'+result.stderr);result.check_returncode()
   return result.stdout
  try:
   command(compose+['up','-d','--no-build','baseline']);deadline=time.monotonic()+180
   import urllib.request
   base='http://'+command(compose+['port','baseline','5656']).strip()
   while True:
    try:
     with urllib.request.urlopen(base+'/api/core/version/',timeout=2)as r:
      if r.status==200:break
    except OSError:
     if time.monotonic()>deadline:raise TimeoutError('Original readiness')
     time.sleep(.25)
   capture=command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/uuid-tools/source.py']);golden=json.loads(capture.splitlines()[-1]);path=out/'Source.json';path.write_text(json.dumps(golden,indent=2)+'\n');report['Source_cases']=len(golden['cases']);report['Source_sha256']=hashlib.sha256(path.read_bytes()).hexdigest();print('Actual Original cases captured:',len(golden['cases']),flush=True)
   pg=command(compose+['port','baseline','5432']).strip();testenv=dict(env,DATABASE_TEST_URL='postgresql://dispatch:playback-fixture-only@'+pg+'/dispatcharr',CHANNEL_UUIDS_DISPOSABLE_FIXTURE='playback-fixture-only',CHANNEL_UUIDS_FIXTURE=str(path),CARGO_TARGET_DIR=str(a.target.resolve()),CARGO_BUILD_JOBS='2')
   for label,args in [('clippy',['clippy','--all-targets','--','-D','warnings']),('test',['test','--test','channel_uuids_sql','--','--ignored','--nocapture'])]:
    prefix=['cargo',args[0],'--manifest-path',str(ROOT/'rust/Cargo.toml'),'-p','dispatcharr-store','--locked','--offline']
    with (out/(label+'.log')).open('w')as log:result=subprocess.run(prefix+args[1:],env=testenv,stdout=log,stderr=subprocess.STDOUT,timeout=900)
    report[label+'_exit_code']=result.returncode
    if result.returncode:raise RuntimeError(label+' failed')
   report['passed']=True
  except BaseException as error:report['failure']=type(error).__name__+': '+str(error);raise
  finally:
   try:(out/'container-logs-private.log').write_text(command(compose+['logs','--no-color','--tail','100'],timeout=30))
   finally:
    command(compose+['down','--volumes','--remove-orphans','--timeout','5'],timeout=60);report['cleanup']='Owned project removed';report['remaining_owned_resources']={kind:command(['docker',kind,'ls','-q','--filter','label=com.docker.compose.project='+report['project']]).splitlines()for kind in ['container','network','volume']};assert not any(report['remaining_owned_resources'].values());(out/'integration.json').write_text(json.dumps(report,indent=2)+'\n')
if __name__=='__main__':main()
