#!/usr/bin/env python3
"""Build/copy the exact reconstructed package's QA peer; never runs app tests.
The new ELF gets its own receipt; no historical binary-identity/performance transfer.
"""
import argparse,hashlib,json,os,shutil,signal,subprocess,time
from pathlib import Path
ROOT=Path(__file__).resolve().parent
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
def build(source,target,output):
 source=Path(source).resolve();target=Path(target).resolve();output=Path(output).resolve();mapping=json.loads((ROOT/'source-inputs.json').read_text())
 def verify():
  actual={str(p.relative_to(source)):sha(p)for p in source.rglob('*')if p.is_file()};assert actual==mapping,'Exact reconstructed source814 required'
 verify();assert not target.exists()and not output.exists()and target!=source and source not in target.parents and target not in source.parents and source not in output.parents and target not in output.parents and output not in target.parents,'Fresh separate target/output required';output.mkdir(parents=True);tmp=output/'tmp';tmp.mkdir()
 env={k:v for k,v in os.environ.items()if not k.startswith('GIT_')};env.update(CARGO_TARGET_DIR=str(target),TMPDIR=str(tmp),CARGO_INCREMENTAL='0',CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0',CARGO_PROFILE_DEV_INCREMENTAL='false',CARGO_PROFILE_TEST_INCREMENTAL='false',CARGO_BUILD_JOBS='2')
 compiler=subprocess.run(['rustc','--version','--verbose'],env=env,check=True,capture_output=True,text=True).stdout
 if not compiler.startswith('rustc 1.90.0 '):raise ValueError('Pinned Rust1.90.0 required')
 (output/'compiler.txt').write_text(compiler)
 argv=['cargo','test','--offline','--locked','-p','dispatcharr-job-transport','--lib','--no-run','--message-format=json-render-diagnostics'];record={'passed':False,'argv':argv,'cwd':str(source/'rust'),'source_map_sha256':sha(ROOT/'source-inputs.json'),'actual_wait':None,'app_or_tests_run':False,'historical_binary_identity_claim':False,'began_epoch':time.time()}
 with(output/'cargo.jsonl').open('xb')as stdout,(output/'cargo.stderr').open('xb')as stderr:
  process=subprocess.Popen(argv,cwd=source/'rust',env=env,stdout=stdout,stderr=stderr,start_new_session=True);record['pid']=process.pid
  try:record['actual_wait']=process.wait(timeout=1800)
  except BaseException:
   os.killpg(process.pid,signal.SIGTERM)
   try:process.wait(timeout=10)
   except subprocess.TimeoutExpired:os.killpg(process.pid,signal.SIGKILL);process.wait(timeout=5)
   raise
  finally:record.update(actual_poll=process.poll(),finished_epoch=time.time());(output/'build.json').write_text(json.dumps(record,indent=2)+'\n')
 verify()
 if record['actual_wait']!=0:raise RuntimeError('Cargo did not finish zero')
 artifacts=[]
 for line in(output/'cargo.jsonl').read_text().splitlines():
  item=json.loads(line)
  if item.get('reason')=='compiler-artifact'and item['target']['name']=='dispatcharr_job_transport'and item['profile']['test']and item.get('executable'):artifacts.append(Path(item['executable']))
 if len(set(artifacts))!=1:raise ValueError('Exactly one Cargo JSON-selected peer required')
 original=artifacts[0]
 if original.open('rb').read(4)!=b'\x7fELF':raise ValueError('Expected real ELF')
 peer=output/'native-parented-peer';shutil.copy2(original,peer)
 if sha(peer)!=sha(original):raise ValueError('Outside peer copy differs')
 record.update(passed=True,binary={'path':str(peer),'sha256':sha(peer),'bytes':peer.stat().st_size,'actual_Cargo_artifact':str(original)},binaries=[{'name':'job-library','path':str(peer),'sha256':sha(peer),'bytes':peer.stat().st_size,'actual_Cargo_artifact':str(original)}],copied_after_wait0=True,copied_after_all_waits_zero=True);(output/'build.json').write_text(json.dumps(record,indent=2)+'\n');return record
if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--source',type=Path,required=True);p.add_argument('--target',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();print(json.dumps(build(a.source,a.target,a.output)))
