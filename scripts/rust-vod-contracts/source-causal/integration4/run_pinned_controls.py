"""One owned pinned interpreter control container; no SourceWSGI/case/sampler."""
import argparse,hashlib,importlib.util,json,os,shutil,subprocess,uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parent
IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
# Reuse the qualified in-flight capped local process collector, immutable copy.
spec=importlib.util.spec_from_file_location('causal_capped_process',ROOT/'bounded_process.py');bounded=importlib.util.module_from_spec(spec);spec.loader.exec_module(bounded)
parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',type=Path,required=True);parser.add_argument('--native-threads-wire',action='store_true');args=parser.parse_args();output=args.output.resolve();output.mkdir(parents=True,exist_ok=False)
mounted=output/'mounted-inputs';mounted.mkdir(mode=0o755);mounted.chmod(0o755);input_hashes={}
for directory in['controller','pinned-source']:
 target=mounted/directory;target.mkdir(mode=0o755);target.chmod(0o755)
 for source in sorted((ROOT/directory).rglob('*.py')):
  destination=target/source.relative_to(ROOT/directory);destination.parent.mkdir(parents=True,exist_ok=True)
  for parent in[destination.parent,*destination.parent.parents]:
   if parent==mounted:break
   parent.chmod(0o755)
  destination.write_bytes(source.read_bytes());destination.chmod(0o444);input_hashes[str(destination.relative_to(mounted))]=hashlib.sha256(destination.read_bytes()).hexdigest()
name='dispatcharr-causal-pinned-'+uuid.uuid4().hex[:12];evidence=output/'pinned-evidence';evidence.mkdir(mode=0o777);evidence.chmod(0o777);report={'passed':False,'name':name,'image':IMAGE,'Source_WSGI_case_executed':False,'native_threads_wire_only':args.native_threads_wire,'resource_sampling':False,'input_sha256':input_hashes};clean=False

def stop():
 subprocess.run(['docker','rm','-f',name],stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=15,check=True)
try:
 argv=['docker','run','--name',name,'--pull','never','--network','none','--read-only','--user','65532:65532','--cpus','1','--memory','256m','--tmpfs','/tmp:rw,size=32m,mode=1777','--workdir','/tmp','--mount','type=bind,src='+str(mounted/'controller')+',dst=/probe,readonly','--mount','type=bind,src='+str(mounted/'pinned-source')+',dst=/pinned-source,readonly','--mount','type=bind,src='+str(evidence)+',dst=/evidence','-e','CAUSAL_PINNED_PROBE=playback-fixture-only','-e','CAUSAL_PINNED_SOURCE=/pinned-source','--entrypoint','/dispatcharrpy/bin/python',IMAGE,'/probe/std_wire_probe.py'if args.native_threads_wire else'/probe/pinned_probe.py']
 job=bounded.Collected(argv,dict(os.environ),1048576,stop,stderr_path=output/'pinned-stderr.log');raw=job.wait(60);(output/'pinned-stdout.log').write_bytes(raw);proof=json.loads((evidence/'pinned-control-proof.json').read_text());report['proof']=proof;report['passed']=proof['passed'];report['output_cap_bytes']=1048576
except BaseException as error:report.update(error_type=type(error).__name__,passed=False)
finally:
 try:
  subprocess.run(['docker','rm','-f',name],stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=15,check=True);remaining=subprocess.check_output(['docker','ps','-aq','--filter','name=^/'+name+'$'],timeout=15).decode().strip();clean=not remaining
 except Exception as error:report['cleanup_error']=type(error).__name__
 report['owned_container_removed']=clean;report['passed']=report['passed']and clean;(output/'pinned-run.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps({'passed':report['passed'],'output':str(output),'container_removed':clean}));raise SystemExit(0 if report['passed']else 1)
