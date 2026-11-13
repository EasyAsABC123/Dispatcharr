import hashlib,json,os,subprocess,time,shutil
from pathlib import Path
ROOT=Path(__file__).resolve().parents[3];out=Path(__file__).resolve().parent;src=out/'source';target=ROOT/'artifacts/rust/target-configured-recordings-graduation-797-2';tmp=ROOT/'artifacts/rust/tmp-configured-recordings-graduation-797-2';inputs=json.loads((out/'source-inputs.json').read_text())
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def verify():
 assert {str(p.relative_to(src)):sha(p)for p in src.rglob('*')if p.is_file()}==inputs,'Exact811SourceRequired'
def save():(out/'receipt.json').write_text(json.dumps(report,indent=2)+'\n')
verify();assert not target.exists();tmp.mkdir(exist_ok=False)
env={k:v for k,v in os.environ.items()if not k.startswith('GIT_')};env.update(CARGO_TARGET_DIR=str(target),CARGO_INCREMENTAL='0',CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0',CARGO_PROFILE_DEV_INCREMENTAL='false',CARGO_PROFILE_TEST_INCREMENTAL='false',CARGO_BUILD_JOBS='2',TMPDIR=str(tmp))
report={'source_map_sha256':sha(out/'source-inputs.json'),'target':str(target),'TMPDIR':str(tmp),'commands':[],'copied_ELFs':False,'passed':False}
phases=[('fmt',['cargo','fmt','--all','--','--check']),('check',['cargo','check','--workspace','--offline','--locked']),('strict',['cargo','clippy','--workspace','--all-targets','--offline','--locked','--','-D','warnings']),('defaults',['cargo','test','--workspace','--offline','--locked'])]
for label,package,filter in [('store-caught','dispatcharr-store','recording_schedule::caught_insert_tests::'),('job-caught','dispatcharr-job-transport','configured_recording_service::caught_insert_tests::'),('decisions','dispatcharr-job-transport','configured_recordings::'),('artwork','dispatcharr-job-transport','recording_artwork::'),('route','dispatcharr-job-transport','configured_parent_fixture::')]:phases.append((label,['cargo','test','--offline','--locked','-p',package,'--lib',filter]))
for label,argv in phases:
 verify();row={'label':label,'argv':argv,'cwd':str(src/'rust'),'began_epoch':time.time(),'settled':False,'exit':None};report['commands'].append(row);save()
 with(out/(label+'.log')).open('xb')as log:
  process=subprocess.Popen(argv,cwd=src/'rust',env=env,stdout=log,stderr=subprocess.STDOUT);row.update(pid=process.pid,starttime=Path('/proc',str(process.pid),'stat').read_text().rsplit(')',1)[1].split()[19]);save();row['exit']=process.wait()
 row.update(settled=True,finished_epoch=time.time());verify();save();print(label,row['exit'],flush=True)
 if row['exit']:raise SystemExit(row['exit'])
# Only after every authorized check/test actually settled0, retain the unique
# tested package library harnesses outside generated target output.
peers=[];outside=out/'outside-peers';outside.mkdir()
for name,stem in [('job-library','dispatcharr_job_transport'),('store-library','dispatcharr_store')]:
 matches=[p for p in(target/'debug/deps').glob(stem+'-*')if p.is_file()and os.access(p,os.X_OK)and p.open('rb').read(4)==b'\x7fELF'];assert len(matches)==1,(name,matches);original=matches[0];copied=outside/name;shutil.copy2(original,copied);assert sha(copied)==sha(original);peers.append({'name':name,'path':str(copied),'sha256':sha(copied),'bytes':copied.stat().st_size,'tested_original':str(original)})
verify();record={'source_map_sha256':sha(out/'source-inputs.json'),'all811_inputs_verified':True,'copied_after_all_waits_zero':True,'actual_build_and_test_waits':report['commands'],'binaries':peers};(out/'build-record.json').write_text(json.dumps(record,indent=2)+'\n');report.update(copied_ELFs=True,passed=True);save();print('complete',flush=True)
