#!/usr/bin/env python3
"""Bounded read-only pinned image-ID receipt; never pulls/starts containers."""
import argparse,hashlib,json,os,sys,time
from pathlib import Path
ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT/'controllers/physical/tools'))
from bounded_process import Collected
IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
EXPECTED='sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
def inspect(docker,output):
 docker=Path(docker).resolve();output=Path(output).resolve()
 if output.exists():raise FileExistsError('Fresh receipt required')
 argv=[str(docker),'image','inspect','--format','{{json .Id}}',IMAGE];env={k:v for k,v in os.environ.items()if not k.startswith('GIT_')};began=time.time();job=Collected(argv,env,65536,lambda:None)
 report={'argv':argv,'actual_exit':None,'process_settled':False,'Docker_host':env.get('DOCKER_HOST'),'docker_executable':str(docker),'docker_executable_sha256':hashlib.sha256(docker.read_bytes()).hexdigest(),'scope':'Read-only image ID only, no pull/container/application','image':{}}
 try:
  job.wait(15)
  ident=json.loads(bytes(job.output['stdout']))
  if ident!=EXPECTED:raise ValueError('Pinned Original image ID differs')
  report['image']={'Id':ident}
 finally:
  report.update(actual_exit=job.poll(),process_settled=job.physically_settled,pipes_settled=not any(t.is_alive()for t in job.threads),collection_invalid=job.invalid,began_epoch=began,finished_epoch=time.time(),stdout_sha256=hashlib.sha256(bytes(job.output['stdout'])).hexdigest(),stderr_sha256=hashlib.sha256(bytes(job.output['stderr'])).hexdigest())
  output.parent.mkdir(parents=True,exist_ok=True);output.write_text(json.dumps(report,indent=2)+'\n')
 return report
if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--docker',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();print(json.dumps(inspect(a.docker,a.output)))
