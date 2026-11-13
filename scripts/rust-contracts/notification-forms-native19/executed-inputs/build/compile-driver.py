import os,json,hashlib,subprocess,time,datetime
from pathlib import Path
root=Path('/home/justin/github/Dispatcharr/artifacts/rust/m5-notification-forms-native-qa-2');plan=json.loads((root/'compile-plan.json').read_text());expected=json.loads((root/'full-source-map.json').read_text());build=Path(plan['fresh_build_root']);build.mkdir(exist_ok=False);(build/'tmp').mkdir();source=root/'source';env=dict(os.environ);env.update(plan['environment']);env.update(CARGO_TARGET_DIR=str(build/'target'),TMPDIR=str(build/'tmp'))
def now():return datetime.datetime.now(datetime.timezone.utc).isoformat()
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def save(p,v):p.write_text(json.dumps(v,indent=2)+'\n')
def start(pid):
 try:return Path('/proc',str(pid),'stat').read_text().rsplit(')',1)[1].split()[19]
 except FileNotFoundError:return None
def mapped():return {str(p.relative_to(source)):sha(p)for p in sorted(source.rglob('*'))if p.is_file()}
def group(pgid):
 r=subprocess.run(['ps','-eo','pid=,pgid=,args='],capture_output=True,check=True,timeout=5);return [l for l in r.stdout.decode().splitlines()if len(l.split())>1 and l.split()[1]==str(pgid)]
assert mapped()==expected
save(build/'pre-source-map.json',expected)
report={'state':'RUNNING','runner_pid':os.getpid(),'runner_starttime':start(os.getpid()),'began_utc':now(),'QA_seal_sha256':sha(root/'seal.json'),'full_source_map_sha256':sha(root/'full-source-map.json'),'source_files':len(expected),'source_root':str(source),'target_root':env['CARGO_TARGET_DIR'],'target_transplanted':False,'HOME_overridden':False,'environment':{k:env.get(k)for k in list(plan['environment'])+['CARGO_TARGET_DIR','TMPDIR','CARGO_HOME','RUSTUP_HOME']},'application_runs':0,'commands':[]}
save(build/'build.json',report)
for command in [['rustc','-vV'],['cargo','--version']]:
 p=subprocess.run(command,capture_output=True,env=env,check=True);(build/(command[0]+'-toolchain.stdout')).write_bytes(p.stdout);(build/(command[0]+'-toolchain.stderr')).write_bytes(p.stderr)
for index,argv in enumerate(plan['commands'],1):
 stdout=build/f'command-{index:02d}.stdout';stderr=build/f'command-{index:02d}.stderr';began=now();assert mapped()==expected
 with stdout.open('xb')as out,stderr.open('xb')as err:
  child=subprocess.Popen(argv,cwd=source/'rust',env=env,stdout=out,stderr=err,start_new_session=True);identity=start(child.pid)
  row={'index':index,'argv':argv,'pid':child.pid,'starttime':identity,'began_utc':began,'state':'RUNNING'};report['commands'].append(row);save(build/'build.json',report);print(json.dumps(row),flush=True);wait=child.wait();poll=child.poll()
 row.update(state='SETTLED',actual_wait=wait,actual_poll=poll,completed_utc=now(),after_starttime=start(child.pid),process_group_after_wait=group(child.pid),stdout_sha256=sha(stdout),stderr_sha256=sha(stderr),stdout_bytes=stdout.stat().st_size,stderr_bytes=stderr.stat().st_size)
 after=mapped();save(build/f'command-{index:02d}-source-map.json',after);row['source_map_unchanged']=after==expected;save(build/f'command-{index:02d}.json',row);save(build/'build.json',report);print(json.dumps(row),flush=True)
 if wait!=0 or poll!=0 or row['process_group_after_wait']or after!=expected:
  report.update(state='SETTLED_NEGATIVE',actual_exit=wait or 1,completed_utc=now(),later_commands_unexecuted=list(range(index+1,len(plan['commands'])+1)));save(build/'build.json',report);raise SystemExit(wait or 1)
report.update(state='SETTLED_ALL_FIVE_ACTUAL0',actual_exit=0,completed_utc=now(),outside_ELF_copied=False);save(build/'post-source-map.json',mapped());save(build/'build.json',report);print(json.dumps({'state':report['state'],'actual_exit':0,'commands':len(report['commands'])}),flush=True)
