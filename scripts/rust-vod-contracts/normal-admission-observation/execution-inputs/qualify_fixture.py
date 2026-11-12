"""One owned origin/decode control container, never the Source application."""
import argparse,hashlib,importlib.util,json,os,shutil,subprocess,uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parent
IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
def main():
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',type=Path,required=True);parser.add_argument('--framework-controls',action='store_true');args=parser.parse_args();out=args.output.resolve();out.mkdir(parents=True,exist_ok=False);tools=out/'mounted-tools';tools.mkdir(mode=0o755);tools.chmod(0o755);hashes={}
 for p in(ROOT/'tools').glob('*.py'):
  destination=tools/p.name;destination.write_bytes(p.read_bytes());destination.chmod(0o444);hashes[p.name]=hashlib.sha256(p.read_bytes()).hexdigest()
 evidence=out/'evidence';evidence.mkdir(mode=0o777);evidence.chmod(0o777);name='dispatcharr-normal-origin-control-'+uuid.uuid4().hex[:10]
 spec=importlib.util.spec_from_file_location('normal_bounded',ROOT/'tools/bounded_process.py');bounded=importlib.util.module_from_spec(spec);spec.loader.exec_module(bounded);report={'passed':False,'name':name,'Source_application_executed':False,'Native_executed':False,'tools_sha256':hashes,'image':IMAGE};clean=False
 def stop():subprocess.run(['docker','rm','-f',name],stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=15,check=True)
 try:
  argv=['docker','run','--name',name,'--pull','never','--network','none','--read-only','--user','65532:65532','--cpus','1','--memory','256m','--tmpfs','/tmp:rw,size=64m,mode=1777','--mount','type=bind,src='+str(tools)+',dst=/normal-tools,readonly','--mount','type=bind,src='+str(evidence)+',dst=/evidence','-e','NORMAL_CONTROL_ONLY=playback-fixture-only','--entrypoint','/dispatcharrpy/bin/python',IMAGE,'/normal-tools/framework_controls.py'if args.framework_controls else'/normal-tools/fixture_preflight.py']
  job=bounded.Collected(argv,dict(os.environ),1048576,stop,stdout_path=out/'stdout.log',stderr_path=out/'stderr.log');job.wait(120);proof=json.loads((evidence/('framework-controls.json'if args.framework_controls else'fixture-preflight.json')).read_text());report['proof']=proof;report['passed']=proof['passed']
 except BaseException as error:report['error_type']=type(error).__name__
 finally:
  try:stop();clean=not subprocess.check_output(['docker','ps','-aq','--filter','name=^/'+name+'$'],timeout=15).decode().strip()
  except Exception as error:report['cleanup_error']=type(error).__name__
  report['owned_container_removed']=clean;report['passed']=report['passed']and clean;(out/'qualification.json').write_text(json.dumps(report,indent=2)+'\n')
 print(json.dumps({'passed':report['passed'],'output':str(out),'removed':clean}));return 0 if report['passed']else 1
if __name__=='__main__':raise SystemExit(main())
