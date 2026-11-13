#!/usr/bin/env python3
"""One authorized finite private build, no Docker/SQL/app or target transplant."""
import datetime,hashlib,json,os,signal,subprocess,time
from pathlib import Path
ROOT=Path('/home/justin/github/Dispatcharr');QA=ROOT/'artifacts/rust/m5-sqlx-close-dependency-qa-1';OUT=Path(__file__).resolve().parent;SOURCE=QA/'baseline-source';TARGET=Path('/tmp/dispatcharr-sqlx-close-baseline-qa1-target-1')
def save(name,v):(OUT/name).write_text(json.dumps(v,indent=2)+'\n')
def now():return datetime.datetime.now(datetime.timezone.utc).isoformat()
def start(pid):
 try:return Path('/proc',str(pid),'stat').read_text().rsplit(')',1)[1].split()[19]
 except FileNotFoundError:return None
def mapping():return {str(f.relative_to(SOURCE)):hashlib.sha256(f.read_bytes()).hexdigest()for f in sorted(SOURCE.rglob('*'))if f.is_file()}
expected=json.loads((QA/'inputs/baseline-full898-map.json').read_text());before=mapping();save('source-before.json',before);assert before==expected and len(before)==898;assert not TARGET.exists(),'FreshTargetOnly';TARGET.mkdir();env=dict(os.environ,CARGO_TARGET_DIR=str(TARGET),CARGO_BUILD_JOBS='2',CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0',CARGO_INCREMENTAL='0',CARGO_NET_OFFLINE='true');commands=[('fmt',['cargo','fmt','--all','--check']),('non-test-store',['cargo','build','--offline','--locked','-p','dispatcharr-store','--lib']),('strict-alltargets',['cargo','clippy','--offline','--locked','-p','dispatcharr-store','--all-targets','--','-D','warnings']),('existing-pure-notifications',['cargo','test','--offline','--locked','-p','dispatcharr-store','--lib','notifications::tests::']),('dependency-test-ELF-no-run',['cargo','test','--offline','--locked','-p','dispatcharr-store','--test','sqlx_pool_close_dependency','--no-run'])];summary={'state':'RUNNING','source_root':str(SOURCE),'target':str(TARGET),'source_count':898,'jobs':2,'debug':0,'incremental':False,'Cargo_network':'offline','format_note':'cargo fmt performs no dependency resolution; offline env applies; build/clippy/test commands explicitly --offline --locked','commands':[],'started_utc':now(),'wrapper_pid':os.getpid()};save('build.json',summary);failure=None
try:
 for index,(name,argv)in enumerate(commands):
  row={'name':name,'argv':argv,'started_utc':now(),'state':'STARTING'};summary['commands'].append(row);save('build.json',summary)
  with(OUT/(f'{index:02d}-{name}.stdout')).open('xb')as stdout,(OUT/(f'{index:02d}-{name}.stderr')).open('xb')as stderr:
   process=subprocess.Popen(argv,cwd=SOURCE/'rust',env=env,stdin=subprocess.DEVNULL,stdout=stdout,stderr=stderr,start_new_session=True);row.update(pid=process.pid,starttime=start(process.pid),state='RUNNING',stdio='regular files; no pipe readers');save('build.json',summary);print(json.dumps({'stage':name,'pid':process.pid,'starttime':row['starttime']}),flush=True)
   try:code=process.wait(timeout=1800)
   except BaseException:
    os.killpg(process.pid,signal.SIGTERM)
    try:process.wait(timeout=15)
    except subprocess.TimeoutExpired:os.killpg(process.pid,signal.SIGKILL);process.wait(timeout=15)
    raise
   finally:row.update(actual_wait=process.returncode,actual_poll=process.poll(),after_starttime=start(process.pid),settled_utc=now(),state='SETTLED');save('build.json',summary)
  print(json.dumps({'stage':name,'exit':code}),flush=True)
  if code!=0:raise RuntimeError('ActualStageFailed:'+name+':'+str(code))
except BaseException as error:failure=type(error).__name__+':'+str(error);summary['failure']=failure
finally:
 after=mapping();save('source-after.json',after);summary.update(source_bytes_unchanged=after==before==expected,state='SETTLED',completed_utc=now(),passed=failure is None and after==expected,all_started_commands_settled=all(r.get('state')=='SETTLED'for r in summary['commands']),actual_wrapper_exit=0 if failure is None and after==expected else 1);save('build.json',summary)
raise SystemExit(summary['actual_wrapper_exit'])
