"""Finite reviewed compile only; no app/Docker or outside ELF copy here."""
import datetime,hashlib,json,os,signal,subprocess,time
from pathlib import Path
ROOT=Path('/home/justin/github/Dispatcharr')
QA=ROOT/'artifacts/rust/m8-backup-job-physical-private-1'
OUT=Path(__file__).resolve().parent
PLAN=json.loads((QA/'compile-plan.json').read_text())
MAP=json.loads((QA/'source-map.json').read_text())
def utc():return datetime.datetime.now(datetime.timezone.utc).isoformat()
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def save(name,value):(OUT/name).write_text(json.dumps(value,indent=2)+'\n')
def start(pid):
    try:return Path('/proc',str(pid),'stat').read_text().rsplit(')',1)[1].split()[19]
    except FileNotFoundError:return None
def verify():
    actual={n:sha(QA/'source'/n)for n in MAP}
    if actual!=MAP:raise ValueError('Frozen830SourceDrift')
    return hashlib.sha256((QA/'source-map.json').read_bytes()).hexdigest()
def members(group):
    p=subprocess.run(['ps','-eo','pid=,pgid='],stdout=subprocess.PIPE,check=True)
    return [int(a)for line in p.stdout.decode().splitlines()for a,b in [line.split()]if int(b)==group]
assert not Path(PLAN['fresh_target']).exists(),'FreshTargetRequired'
Path(PLAN['TMPDIR']).mkdir(parents=True,exist_ok=False)
env=dict(os.environ,**PLAN['environment'],TMPDIR=PLAN['TMPDIR'])
original_home=os.environ.get('HOME');original_codex=os.environ.get('CODEX_HOME')
record={'state':'RUNNING','began_utc':utc(),'runner_pid':os.getpid(),'runner_starttime':start(os.getpid()),'PID_namespace':os.readlink('/proc/self/ns/pid'),'PID_scope':'actual tool execution namespace, not inferred host namespace','HOME_unchanged':env.get('HOME')==original_home,'CODEX_HOME_unchanged':env.get('CODEX_HOME')==original_codex,'source_map_sha256':verify(),'source_inputs':830,'target_transplanted':False,'target':PLAN['fresh_target'],'commands':[],'Cargo_exit':None,'app_runs':0}
save('build.json',record)
for i,argv in enumerate(PLAN['commands'],1):
    pre=verify();tag=f'command-{i:02d}';began=utc()
    with (OUT/(tag+'.stdout')).open('xb')as stdout,(OUT/(tag+'.stderr')).open('xb')as stderr:
        child=subprocess.Popen(argv,cwd=PLAN['cwd'],env=env,stdin=subprocess.DEVNULL,stdout=stdout,stderr=stderr,start_new_session=True)
        row={'argv':argv,'began_utc':began,'pid':child.pid,'starttime':start(child.pid),'PID_namespace':record['PID_namespace'],'pre_source_map_sha256':pre,'state':'RUNNING'};record['commands'].append(row);save('build.json',record)
        invalid=None
        while child.poll()is None:
            if any((OUT/(tag+suffix)).stat().st_size>33554432 for suffix in ('.stdout','.stderr')):
                invalid='BuildLog32MiBCapacity';os.killpg(child.pid,signal.SIGTERM);break
            time.sleep(.1)
        try:waited=child.wait(timeout=10)
        except subprocess.TimeoutExpired:
            os.killpg(child.pid,signal.SIGKILL);waited=child.wait(timeout=10);invalid=invalid or'ForcedBuildTermination'
    remaining=members(child.pid)
    row.update(state='SETTLED',completed_utc=utc(),actual_wait=waited,actual_poll=child.poll(),process_group_members=remaining,stdout_bytes=(OUT/(tag+'.stdout')).stat().st_size,stderr_bytes=(OUT/(tag+'.stderr')).stat().st_size,post_source_map_sha256=verify(),invalid=invalid,streams='direct files closed after actual wait; no collector pipes')
    save(tag+'.json',row);save('build.json',record)
    if waited!=0 or remaining or invalid:
        record.update(state='SETTLED_NEGATIVE',Cargo_exit=waited if waited else 1,completed_utc=utc());save('build.json',record);raise SystemExit(record['Cargo_exit'])
record.update(state='SETTLED_PASS',Cargo_exit=0,completed_utc=utc(),all5_wait_poll0=True,post_source_map_sha256=verify(),outside_ELF_copied=False);save('build.json',record)
print(json.dumps({'state':record['state'],'actual_exit':0,'commands':len(record['commands']),'source_inputs':830,'outside_ELF_copied':False}),flush=True)
