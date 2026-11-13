#!/usr/bin/env python3
"""Fresh offlineRust1.90 no-run harness rebuild on reconstructed source.
No target transplant, test/app/Docker execution or private inputs. Source authors
retain full algorithm/licensing credit; historical executedELFs are hashbound.
"""
import argparse,hashlib,json,os,shutil,subprocess,time
from pathlib import Path
from reconstruct import validate_tree
HERE=Path(__file__).resolve().parent
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def save(p,v):p.write_text(json.dumps(v,indent=2)+'\n')
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--source',type=Path,required=True);p.add_argument('--target',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();source=a.source.resolve();target=a.target.resolve();out=a.output.resolve();expected=json.loads((HERE/'source-inputs.json').read_text());validate_tree(source,expected)
 if target.exists()or out.exists():raise ValueError('FreshTargetAndOutputRequired')
 out.mkdir(parents=True,exist_ok=False);compiler=subprocess.run(['rustc','--version','--verbose'],capture_output=True,text=True,check=True)
 if not compiler.stdout.startswith('rustc 1.90.0 '):raise ValueError('PinnedRust1.90Required')
 save(out/'compiler.json',{'argv':['rustc','--version','--verbose'],'actual_exit':compiler.returncode,'stdout':compiler.stdout,'stderr':compiler.stderr});env={k:v for k,v in os.environ.items()if not k.startswith('GIT_')};env.update(CARGO_TARGET_DIR=str(target),CARGO_BUILD_JOBS='2',CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0',CARGO_PROFILE_DEV_INCREMENTAL='false',CARGO_PROFILE_TEST_INCREMENTAL='false')
 phases=[]
 for name,args in [('job-library',['--lib']),('coordination-residual',['--test','task_lock_detailed'])]:
  package='dispatcharr-job-transport'if name=='job-library'else'dispatcharr-coordination';argv=['cargo','test','--offline','--locked','--no-run','--message-format=json','-p',package,*args];row={'name':name,'argv':argv,'cwd':str(source/'rust'),'began_epoch':time.time(),'process_settled':False};validate_tree(source,expected)
  with(out/(name+'.messages.jsonl')).open('xb')as stdout,(out/(name+'.stderr.log')).open('xb')as stderr:
   process=subprocess.Popen(argv,cwd=source/'rust',env=env,stdout=stdout,stderr=stderr);row['pid']=process.pid;save(out/(name+'.started.json'),row);row['actual_exit']=process.wait();row['process_settled']=True
  row['finished_epoch']=time.time();validate_tree(source,expected);save(out/(name+'.result.json'),row);phases.append(row)
  if row['actual_exit']!=0:save(out/'build-record.json',{'passed':False,'phases':phases,'no_ELFs_copied':True});raise SystemExit(row['actual_exit'])
 peers=[];outside=out/'outside-peers';outside.mkdir()
 for row in phases:
  records=[json.loads(line)for line in(out/(row['name']+'.messages.jsonl')).read_text().splitlines()];artifacts=[v for v in records if v.get('reason')=='compiler-artifact'and v.get('executable')and v.get('profile',{}).get('test')]
  if len(artifacts)!=1:raise ValueError('ExactOneHarness:'+row['name'])
  compiled=Path(artifacts[0]['executable']);copied=outside/row['name'];shutil.copy2(compiled,copied)
  if copied.read_bytes()[:4]!=b'\x7fELF'or sha(compiled)!=sha(copied):raise ValueError('ExactOutsideELF')
  peers.append({'name':row['name'],'path':str(copied),'sha256':sha(copied),'bytes':copied.stat().st_size,'actual_compiler_artifact':artifacts[0]})
 validate_tree(source,expected);save(out/'build-record.json',{'passed':True,'source_map_sha256':sha(HERE/'source-inputs.json'),'source_inputs':len(expected),'compiler_sha256':sha(out/'compiler.json'),'phases':phases,'outside_peers':peers,'no_tests_or_fixtures_executed':True,'historical_ELF_hash_equality_claim':False})
if __name__=='__main__':main()
