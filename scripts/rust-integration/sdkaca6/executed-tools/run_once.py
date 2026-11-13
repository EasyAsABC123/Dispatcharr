"""Single unchanged committed SDK dispatch with independent lifecycle/input receipts."""
import hashlib,json,os,subprocess,threading,time
from pathlib import Path
ROOT=Path.cwd();OUT=ROOT/'artifacts/rust/sdk-aca6-qualified-1';plan=json.loads((OUT/'plan.json').read_text());SOURCE=OUT/'source';environment=dict(os.environ,**json.loads((OUT/'private-git-environment.json').read_text()),RUST_EVIDENCE_DIR=str(OUT/'evidence'));project=plan['project'];closure=json.loads((OUT/'SDK-copy-closure.json').read_text())
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=SOURCE,env=environment,text=True).strip()==plan['ref'];assert not subprocess.check_output(['git','status','--short'],cwd=SOURCE,env=environment,text=True).strip()
all_before=json.loads((OUT/'all-source-inputs.json').read_text());assert all(sha(SOURCE/k)==v for k,v in all_before.items())
pre={}
for name,args in [('containers',['docker','ps','-aq']),('networks',['docker','network','ls','-q']),('volumes',['docker','volume','ls','-q'])]:
 values=subprocess.check_output(args+['--filter','label=com.docker.compose.project='+project],text=True).splitlines();assert not values,'Fresh SDK project must be absent';pre[name]=values
(OUT/'preflight-project-empty.json').write_text(json.dumps(pre,indent=2)+'\n');assert not(OUT/'execution-started.json').exists(),'NoSecondSDKDispatch'
(OUT/'execution-started.json').write_text(json.dumps({'project':project,'time_unix':time.time(),'command':plan['command'],'ref':plan['ref'],'private_git':True,'run_number':1},indent=2)+'\n')
event_file=(OUT/'Docker-events.jsonl').open('w');events=subprocess.Popen(['docker','events','--since',str(int(time.time())-1),'--filter','label=com.docker.compose.project='+project,'--format','{{json .}}'],stdout=event_file,stderr=subprocess.PIPE,text=True);stop=threading.Event();observed={};errors=[]
code="""import sys,json,hashlib,os
expected=json.load(sys.stdin);actual={};missing=[]
for name in expected:
 p='/workspace/'+name
 if not os.path.isfile(p):missing.append(name);continue
 with open(p,'rb')as h:actual[name]=hashlib.sha256(h.read()).hexdigest()
print(json.dumps({'passed':not missing and actual==expected,'files':actual,'missing':missing,'count':len(actual)}))
"""
def monitor():
 try:
  while not stop.is_set():
   ids=subprocess.check_output(['docker','ps','-aq','--filter','label=com.docker.compose.project='+project],text=True).splitlines()
   if ids:
    rows=json.loads(subprocess.check_output(['docker','inspect',*ids],text=True))
    for row in rows:
     if row['Config']['Labels'].get('com.docker.compose.service')!='verify':continue
     observed.update(container_id=row['Id'],image_id=row['Image'],network_mode=row['HostConfig']['NetworkMode'],command=row['Config']['Cmd'],state=row['State'],mounts=row['Mounts']);(OUT/'independent-container-inspect.json').write_text(json.dumps(observed,indent=2)+'\n');assert observed['network_mode']=='none'
     if row['State']['Running']and not(OUT/'independent-image-copy-hashes.json').exists():
      value=subprocess.run(['docker','exec','-i',row['Id'],'python3','-c',code],input=json.dumps(closure),capture_output=True,text=True,timeout=60);(OUT/'independent-image-copy-command.json').write_text(json.dumps({'exit':value.returncode,'stderr':value.stderr},indent=2)+'\n');value.check_returncode();hashes=json.loads(value.stdout);(OUT/'independent-image-copy-hashes.json').write_text(json.dumps(hashes,indent=2)+'\n');assert hashes['passed'],'Copied SDK bytes mismatch'
   if(OUT/'independent-image-copy-hashes.json').exists():return
   stop.wait(.25)
 except Exception as error:errors.append(type(error).__name__+':'+str(error))
thread=threading.Thread(target=monitor,daemon=True);thread.start();result=None;verifier=None;began=time.time()
try:
 with(OUT/'host-run.log').open('x')as log:
  verifier=subprocess.Popen(plan['command'],cwd=SOURCE,env=environment,stdout=log,stderr=subprocess.STDOUT);row={'pid':verifier.pid,'argv':plan['command'],'started_unix':began,'private_HEAD':plan['ref'],'default_timeout1800_unchanged':True};(OUT/'host-process-start.json').write_text(json.dumps(row,indent=2)+'\n');code_exit=verifier.wait();result=subprocess.CompletedProcess(plan['command'],code_exit)
finally:
 cleanup_env=dict(environment,RUST_EVIDENCE_DIR=str(OUT/'evidence'));cleanup=subprocess.run(['docker','compose','-p',project,'-f','docker/compose.rust.yml','down','--volumes','--remove-orphans'],cwd=SOURCE,env=cleanup_env,capture_output=True,text=True,timeout=60);(OUT/'outer-owned-cleanup.json').write_text(json.dumps({'argv':cleanup.args,'exit':cleanup.returncode,'stdout':cleanup.stdout,'stderr':cleanup.stderr},indent=2)+'\n')
 stop.set();thread.join(timeout=65);events.terminate()
 try:events.wait(timeout=5)
 except subprocess.TimeoutExpired:events.kill();events.wait(timeout=5)
 event_file.close();(OUT/'independent-monitor.json').write_text(json.dumps({'errors':errors,'monitor_completed':not thread.is_alive(),'event_process_exit':events.returncode},indent=2)+'\n')
 (OUT/'actual-process-exit.json').write_text(json.dumps({'returncode':None if result is None else result.returncode,'time_unix':time.time(),'wait_completed':verifier is not None and verifier.poll()is not None,'host_pid':None if verifier is None else verifier.pid,'started_unix':began},indent=2)+'\n')
print(json.dumps({'actual_exit':None if result is None else result.returncode,'output':str(OUT),'monitor_errors':errors}));raise SystemExit(1 if result is None else result.returncode)
