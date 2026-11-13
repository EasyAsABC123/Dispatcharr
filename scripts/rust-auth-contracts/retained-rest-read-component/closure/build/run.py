from pathlib import Path
import json,hashlib,subprocess,os,time,datetime
R=Path('/home/justin/github/Dispatcharr');B=R/'artifacts/rust/m5-bounded-rest-auth-physical-build-qa1-1';Q=R/'artifacts/rust/m5-bounded-rest-auth-physical-qa-1';S=Q/'source';env=os.environ.copy();env.update(CARGO_TARGET_DIR=str(R/'artifacts/rust/target-m8-backup-source16-native-qa4-1'),TMPDIR=str(R/'artifacts/rust/tmp-rest-auth-build-1'),CARGO_BUILD_JOBS='2',CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0',CARGO_INCREMENTAL='0');H=lambda p:hashlib.sha256(p.read_bytes()).hexdigest();save=lambda n,v:(B/n).write_text(json.dumps(v,indent=2)+'\n');expected=json.load(open(Q/'source-inputs.json'))
def verify():
 actual={str(p.relative_to(S)):H(p)for p in sorted(S.rglob('*'))if p.is_file()};assert actual==expected;return actual
save('source-before.json',verify());save('host-start.json',{'pid':os.getpid(),'starttime':Path('/proc/self/stat').read_text().rsplit(')',1)[1].split()[19],'started_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'source_seal_sha256':H(Q/'compile-seal.json'),'target_reused_in_place_only':env['CARGO_TARGET_DIR'],'HOME_unchanged':True,'CODEX_HOME_unchanged':True})
for tool in ['rustc','cargo']:
 p=subprocess.run([tool,'--version'],capture_output=True,text=True);save(tool+'-version.json',{'argv':[tool,'--version'],'actual_exit':p.returncode,'stdout':p.stdout,'stderr':p.stderr});assert p.returncode==0
commands=[['cargo','fmt','--all','--','--check'],['cargo','check','--offline','--locked','-p','dispatcharr-store'],['cargo','clippy','--offline','--locked','-p','dispatcharr-store','--all-targets','--','-D','warnings'],['cargo','test','--offline','--locked','-p','dispatcharr-store','--lib','rest_reads::tests','--','--nocapture'],['cargo','test','--offline','--locked','-p','dispatcharr-store','--lib','--no-run','--message-format=json']];rows=[]
for i,argv in enumerate(commands,1):
 start=time.time()
 with (B/f'command-{i}.stdout').open('wb')as out,(B/f'command-{i}.stderr').open('wb')as err:
  process=subprocess.Popen(argv,cwd=S/'rust',env=env,stdout=out,stderr=err);stat=Path('/proc')/str(process.pid)/'stat';identity=stat.read_text().rsplit(')',1)[1].split()[19];save('progress.json',{'command':i,'argv':argv,'pid':process.pid,'observed_starttime':identity,'host_pid':os.getpid(),'state':'RUNNING'});code=process.wait();poll=process.poll()
 row={'command':i,'argv':argv,'pid':process.pid,'observed_starttime':identity,'actual_wait_exit':code,'actual_poll_exit':poll,'began_epoch':start,'completed_epoch':time.time(),'stdout_sha256':H(B/f'command-{i}.stdout'),'stderr_sha256':H(B/f'command-{i}.stderr'),'source_map_equal_after_wait':verify()==expected};rows.append(row);save(f'command-{i}.json',row);save('waits.json',rows);print('settled',i,code,flush=True)
 if code:save('build.json',{'passed':False,'actual_host_exit':1,'commands':rows,'source_after':verify(),'outside_ELF_copied':False});raise SystemExit(1)
# Copy only after ALL actual waits/polls0; no target transplant.
artifacts=[]
for line in (B/'command-5.stdout').read_text().splitlines():
 try:v=json.loads(line)
 except ValueError:continue
 if v.get('reason')=='compiler-artifact'and v.get('executable')and v.get('target',{}).get('name')=='dispatcharr_store':artifacts.append(v['executable'])
assert len(set(artifacts))==1
import shutil
src=Path(artifacts[0]);dest=B/'outside-rest-auth-store-tests';shutil.copyfile(src,dest);dest.chmod(0o755);save('outside-peer.json',{'compiled_path':str(src),'outside_path':str(dest),'sha256':H(dest),'bytes':dest.stat().st_size,'copied_epoch':time.time(),'all_commands_completed_before_copy':all(x['completed_epoch']<=time.time()for x in rows),'source_map_sha256':H(Q/'source-inputs.json')});save('source-after.json',verify());save('build.json',{'passed':True,'actual_host_exit':0,'commands':rows,'source_map_sha256':H(Q/'source-inputs.json'),'outside_ELF_copied':True});print('ALL_SETTLED0',flush=True)
