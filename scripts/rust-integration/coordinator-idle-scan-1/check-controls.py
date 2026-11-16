"""Root-run bounded host namespace coordinator controls. No application workload."""
import argparse,hashlib,importlib.util,json,os,signal,sys,time
from pathlib import Path
H=Path(__file__).resolve().parent;R=H.parents[2]
p=argparse.ArgumentParser();p.add_argument('--output',type=Path,required=True);p.add_argument('--namespace-anchor-pid',type=int,required=True);a=p.parse_args();a.output.mkdir(exist_ok=False)
namespace=os.readlink('/proc/self/ns/pid');assert namespace==os.readlink('/proc/'+str(a.namespace_anchor_pid)+'/ns/pid')
meta=json.loads((H/'overlay-map.json').read_text());base=R/meta['target'];assert hashlib.sha256(base.read_bytes()).hexdigest()==meta['base_sha256'];assert hashlib.sha256((H/'coordinator.py').read_bytes()).hexdigest()==meta['candidate_sha256']
def load(path,name):
 spec=importlib.util.spec_from_file_location(name,path);module=importlib.util.module_from_spec(spec);sys.modules[name]=module;spec.loader.exec_module(module);return module
modules={'original':load(base,'owned_original_coordinator'),'candidate':load(H/'coordinator.py','owned_candidate_coordinator')}
report={'passed':False,'state':'actual_controls_running','pid_namespace':namespace,'namespace_anchor_pid':a.namespace_anchor_pid,'actual_observer_pid':os.getpid(),'proc_numeric_entries_at_start':sum(p.name.isdigit()for p in Path('/proc').iterdir()),'application_started':False,'cases':[]}
def save():
 (a.output/'report.json').write_text(json.dumps(report,indent=2)+'\n')
def generation(pid):
 try:return Path('/proc/'+str(pid)+'/stat').read_text().rsplit(')',1)[1].split()[19]
 except FileNotFoundError:return None

def run(module,variant,name,code,seconds=3,output=65536):
 directory=a.output/(variant+'-'+name);directory.mkdir();cpu=time.process_time();start=time.monotonic();receipt=module.command([sys.executable,'-B','-c',code],{'output_bytes':output},directory,'command',seconds);row={'variant':variant,'name':name,'receipt':receipt,'observer_CPU_seconds':time.process_time()-cpu,'wall_seconds':time.monotonic()-start,'passed':False};report['cases'].append(row);save();return row

def eof_settled(row):
 r=row['receipt'];assert r['stdout_EOF']and r['stderr_EOF']and r['process_group_settled']and not r['remaining_group_members']

try:
 # Compare identical actual5s idle children WITHOUT cProfile overhead.
 for variant,module in modules.items():
  row=run(module,variant,'idle','import time;time.sleep(5)',8);eof_settled(row);assert row['receipt']['actual_exit']==0 and row['receipt']['failure']is None;row['passed']=True;save()
 for variant,module in modules.items():
  code="import subprocess,sys;p=subprocess.Popen([sys.executable,'-c','import time;time.sleep(20)'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL);print(p.pid,flush=True)"
  row=run(module,variant,'orphan',code);eof_settled(row);assert row['receipt']['failure']=='controller left process-group descendants'and row['receipt']['reaped_descendants'];row['passed']=True;save()
  code="import os,time;p=os.fork();\nif p==0:os._exit(0)\nprint(p,flush=True);time.sleep(.15)"
  row=run(module,variant,'zombie',code);eof_settled(row);assert row['receipt']['actual_exit']==0 and row['receipt']['failure']is None and row['receipt']['reaped_descendants'];row['passed']=True;save()
  child="import os,signal,time;signal.signal(signal.SIGTERM,signal.SIG_IGN);print(os.getpid(),flush=True);time.sleep(20)"
  code="import subprocess,sys;p=subprocess.Popen([sys.executable,'-c',"+repr(child)+"],stdout=subprocess.PIPE,text=True);print(p.stdout.readline().strip(),flush=True)"
  row=run(module,variant,'ignore-TERM-orphan',code);eof_settled(row);assert row['receipt']['failure']=='controller left process-group descendants';assert any(os.WIFSIGNALED(r['wait_status'])and os.WTERMSIG(r['wait_status'])==signal.SIGKILL for r in row['receipt']['reaped_descendants']);row['passed']=True;save()
  row=run(module,variant,'timeout','import time;time.sleep(20)',.4);eof_settled(row);assert row['receipt']['failure']=='timeout'and row['receipt']['actual_exit']!=0;row['passed']=True;save()
  row=run(module,variant,'output-budget',"import sys,time;sys.stdout.write('x'*131072);sys.stdout.flush();time.sleep(20)",2,1024);eof_settled(row);assert row['receipt']['failure']=='output budget exceeded'and row['receipt']['output_bytes_written']<=1024 and row['receipt']['output_bytes_discarded']>0;row['passed']=True;save()
  # This intentionally exposes the existing un-settled escaped-pipe behavior.
  # Targeted external cleanup uses actual emitted PID+generation, not killpg guesses.
  child="import os,time;from pathlib import Path;print('OWNED_ESCAPED '+str(os.getpid())+' '+Path('/proc/self/stat').read_text().rsplit(')',1)[1].split()[19],flush=True);time.sleep(25)"
  code="import subprocess,sys;subprocess.Popen([sys.executable,'-c',"+repr(child)+"],start_new_session=True)"
  row=run(module,variant,'escaped-inherited-pipe',code,15)
  try:
   r=row['receipt'];assert r['actual_exit']==0 and r['failure']=='controller left inherited pipes open'and not r['process_group_settled']and(not r['stdout_EOF']or not r['stderr_EOF'])
  finally:
   lines=Path(row['receipt']['stdout']).read_text().splitlines();identity=[line.split()for line in lines if line.startswith('OWNED_ESCAPED ')];assert len(identity)==1;pid=int(identity[0][1]);actual=identity[0][2]
   if generation(pid)==actual:
    os.kill(pid,signal.SIGKILL)
    try:os.waitpid(pid,0)
    except ChildProcessError:pass
   row['exact_escaped_generation_absent_after_external_cleanup']=generation(pid)!=actual;assert row['exact_escaped_generation_absent_after_external_cleanup'];save()
  row['passed']=True;save()
 idle={r['variant']:r['observer_CPU_seconds']for r in report['cases']if r['name']=='idle'};report['idle_CPU_ratio']=idle['candidate']/max(idle['original'],1e-9)
 assert report['idle_CPU_ratio']<.5,'Measured idle observerCPU mustmateriallyreduce'
 assert all(r['passed']for r in report['cases']);report.update(passed=True,state='actual_controls_passed',application_performance_gain_claimed=False)
finally:save()
print(json.dumps({'passed':report['passed'],'cases':len(report['cases']),'idle_CPU_ratio':report.get('idle_CPU_ratio'),'application_started':False}));raise SystemExit(0 if report['passed']else 1)
