"""Only the two unfinished compile stages, fresh home target; preserve ENOSPC."""
import datetime,hashlib,json,os,signal,subprocess
from pathlib import Path
BASE=Path('/home/justin/github/Dispatcharr/artifacts/rust');QA=BASE/'m5-sqlx-close-dependency-qa-2';SOURCE=QA/'candidate-source';OUT=Path(__file__).resolve().parent;TARGET=BASE/'target-sqlx-close-dependency-candidate-qa2-recovery-1';TMP=BASE/'tmp-sqlx-close-dependency-candidate-qa2-recovery-1'
def now():return datetime.datetime.now(datetime.timezone.utc).isoformat()
def save(name,value):(OUT/name).write_text(json.dumps(value,indent=2)+'\n')
def mapping():return {str(p.relative_to(SOURCE)):hashlib.sha256(p.read_bytes()).hexdigest()for p in sorted(SOURCE.rglob('*'))if p.is_file()}
def start(pid):
 try:return Path('/proc',str(pid),'stat').read_text().rsplit(')',1)[1].split()[19]
 except FileNotFoundError:return None
expected=json.loads((QA/'inputs/candidate-full898-map.json').read_text());before=mapping();assert before==expected and len(before)==898;assert not TARGET.exists()and not TMP.exists();TARGET.mkdir();TMP.mkdir();save('source-before.json',before);prior=json.loads((BASE/'m5-sqlx-close-dependency-build-candidate-qa2-1/build.json').read_text());assert prior['passed']is False and prior['commands'][-1]['actual_wait']==101 and all(r['actual_wait']==0 for r in prior['commands'][:3]);commands=[('existing-pure-notifications',['cargo','test','--offline','--locked','-p','dispatcharr-store','--lib','notifications::tests::']),('dependency-test-ELF-no-run',['cargo','test','--offline','--locked','-p','dispatcharr-store','--test','sqlx_pool_close_dependency','--no-run'])];environment=dict({k:v for k,v in os.environ.items()if not k.startswith('GIT_')},CARGO_TARGET_DIR=str(TARGET),TMPDIR=str(TMP),CARGO_BUILD_JOBS='2',CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0',CARGO_INCREMENTAL='0',CARGO_NET_OFFLINE='true');summary={'state':'RUNNING','recovery_of':'preserved candidate QA2 unit compile ENOSPC; no source change','source_root':str(SOURCE),'target':str(TARGET),'TMPDIR':str(TMP),'source_count':898,'new_Cargo_commands':2,'repeated_successful_fmt_non_test_strict_commands':0,'target_reused_or_transplanted':False,'commands':[],'started_utc':now()};save('recovery.json',summary);failure=None
try:
 for number,(name,argv)in enumerate(commands):
  row={'name':name,'argv':argv,'state':'STARTING','started_utc':now()};summary['commands'].append(row);save('recovery.json',summary)
  with(OUT/f'{number:02d}-{name}.stdout').open('xb')as stdout,(OUT/f'{number:02d}-{name}.stderr').open('xb')as stderr:
   process=subprocess.Popen(argv,cwd=SOURCE/'rust',env=environment,stdin=subprocess.DEVNULL,stdout=stdout,stderr=stderr,start_new_session=True);row.update(pid=process.pid,starttime=start(process.pid),state='RUNNING');save('recovery.json',summary);print(json.dumps({'stage':name,'pid':process.pid,'starttime':row['starttime']}),flush=True)
   try:code=process.wait(timeout=1800)
   except BaseException:
    os.killpg(process.pid,signal.SIGTERM)
    try:process.wait(timeout=15)
    except subprocess.TimeoutExpired:os.killpg(process.pid,signal.SIGKILL);process.wait(timeout=15)
    raise
   finally:row.update(actual_wait=process.returncode,actual_poll=process.poll(),after_starttime=start(process.pid),state='SETTLED',completed_utc=now());save('recovery.json',summary)
  print(json.dumps({'stage':name,'exit':code}),flush=True)
  if code!=0:raise RuntimeError('ActualRecoveryStageFailed:'+name+':'+str(code))
except BaseException as error:failure=type(error).__name__+':'+str(error);summary['error']=failure
finally:
 after=mapping();save('source-after.json',after);summary.update(state='SETTLED',source_898_unchanged=after==before==expected,completed_utc=now(),passed=failure is None and after==expected,all_started_commands_settled=all(r['state']=='SETTLED'for r in summary['commands']),actual_wrapper_exit=0 if failure is None and after==expected else 1);save('recovery.json',summary)
raise SystemExit(summary['actual_wrapper_exit'])
