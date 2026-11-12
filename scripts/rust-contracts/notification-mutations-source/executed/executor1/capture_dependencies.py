"""Explicitly authorized static Docker create/cp/remove only; NEVER starts container."""
import hashlib,json,subprocess,uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parent
IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
def main():
 out=ROOT/'dependency-capture';out.mkdir(exist_ok=False);cid=None;rows=[];report={'Source_started':False,'app_imports':False,'commands':rows,'passed':False}
 def run(argv):
  p=subprocess.run(argv,capture_output=True,timeout=30);assert len(p.stdout)+len(p.stderr)<=1048576
  rows.append({'argv':argv,'exit':p.returncode,'stdout':p.stdout.decode(errors='replace'),'stderr':p.stderr.decode(errors='replace')});assert p.returncode==0;return p.stdout
 try:
  report['image_inspect']=json.loads(run(['docker','image','inspect',IMAGE]))
  cid=run(['docker','create','--pull','never','--network','none','--read-only','--name','dispatcharr-notifmut-deps-'+uuid.uuid4().hex[:10],'--label','dispatcharr.notifmut.static=true',IMAGE,'/bin/false']).decode().strip();report['container_id']=cid
  state=json.loads(run(['docker','inspect',cid]))[0];report['created_inspect']=state
  assert state['State']['Status']=='created' and not state['State']['Running'] and state['State']['Pid']==0 and state['HostConfig']['NetworkMode']=='none' and state['HostConfig']['ReadonlyRootfs'] is True
  refs=json.loads((ROOT.parent/'m5-notification-mutations-source-plan-1/upstream-authorities.json').read_text());proof=[]
  for ref in refs:
   rel=ref['path'].removeprefix('inputs/upstream-tag/');dest=ROOT/'inputs/actual-image'/rel;dest.parent.mkdir(parents=True,exist_ok=True)
   run(['docker','cp',cid+':/dispatcharrpy/lib/python3.13/site-packages/'+rel,str(dest)])
   b=dest.read_bytes();proof.append({'path':str(dest.relative_to(ROOT)),'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest(),'upstream_tag_sha256':ref['sha256'],'byte_equal':hashlib.sha256(b).hexdigest()==ref['sha256'],'upstream_url':ref['url']})
  report['files']=proof;report['all_nine_equal']=len(proof)==9 and all(p['byte_equal']for p in proof);report['passed']=report['all_nine_equal']
 finally:
  if cid:
   report['final_before_remove']=json.loads(run(['docker','inspect',cid]))[0]['State'];assert report['final_before_remove']['Status']=='created' and report['final_before_remove']['Pid']==0
   run(['docker','rm',cid]);p=subprocess.run(['docker','inspect',cid],capture_output=True,timeout=15)
   report['absence_probe']={'exit':p.returncode,'stdout':p.stdout.decode(),'stderr':p.stderr.decode()};report['exact_container_absent']=p.returncode!=0 and json.loads(p.stdout)==[]
  (out/'proof.json').write_text(json.dumps(report,indent=2)+'\n')
 print(json.dumps({'passed':report['passed'],'all_nine_equal':report.get('all_nine_equal'),'exact_container_absent':report.get('exact_container_absent')}))
if __name__=='__main__':main()
