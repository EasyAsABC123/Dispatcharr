#!/usr/bin/env python3
"""Offline fresh-directory reconstruction only. Never builds or starts apps."""
import argparse,hashlib,json,shutil,subprocess,tarfile,tempfile
from pathlib import Path
ROOT=Path(__file__).resolve().parent
ORIGINAL='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430';RUNTIME='044f3ad0fb3ee68ffb17b23bbb796ccdae954298'
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def require(ok,label):
 if not ok:raise ValueError(label)
def archive(repository,revision,dest,paths=()):
 with dest.open('xb')as sink:
  result=subprocess.run(['git','-C',str(repository),'archive','--format=tar',revision,*paths],stdin=subprocess.DEVNULL,stdout=sink,stderr=subprocess.PIPE,timeout=120)
 require(result.returncode==0,'PinnedGitArchive:'+revision+':'+result.stderr.decode(errors='replace'))
def original_source_executor(repository,output):
 output.mkdir(parents=True,exist_ok=False);shutil.copytree(ROOT/'executed-definitions/source-executor',output,dirs_exist_ok=True);target=output/'inputs/original-source.tar';archive(repository,ORIGINAL,target);require(sha(target)=='f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff','ExactOriginalArchive')
 seal=json.loads((output/'seal.json').read_text());require(sha(output/'seal.json')=='9c09896c7cc71d57ff152bb3fc4c93d5bd158179da7b136c0e23c96c38e4c9fc','ExactOriginalExecutorSeal')
 for row in seal['files']:require((output/row['path']).stat().st_size==row['bytes']and sha(output/row['path'])==row['sha256'],'ExactOriginal92Input:'+row['path'])
 return {'passed':True,'reconstructed_original_input_count':len(seal['files']),'Original_executor_seal_sha256':sha(output/'seal.json'),'Source_application_runs':0,'Native_application_runs':0}
def native_source(repository,output):
 output.mkdir(parents=True,exist_ok=False);base=json.loads((ROOT/'base797-map.json').read_text());expected=json.loads((ROOT/'full798-map.json').read_text())
 with tempfile.TemporaryDirectory()as temporary:
  tar=Path(temporary)/'runtime.tar';archive(repository,RUNTIME,tar,sorted(base))
  with tarfile.open(tar)as bundle:
   for name,digest in base.items():
    member=bundle.getmember(name);require(member.isfile()and not Path(name).is_absolute()and'..'not in Path(name).parts,'RegularRelativeGitMember');data=bundle.extractfile(member).read();require(hashlib.sha256(data).hexdigest()==digest,'Exact044fBlob:'+name);target=output/name;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(data)
 for relative in('rust/crates/store/src/notifications.rs','rust/crates/store/src/notifications_credentials_sql_tests.rs'):
  target=output/relative;target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(ROOT/'native-overlay'/relative,target)
 actual={str(p.relative_to(output)):sha(p)for p in output.rglob('*')if p.is_file()};require(actual==expected and len(actual)==798,'ExactQualified798Source')
 return {'passed':True,'qualified_source_count':798,'Rust_count':793,'coupled_count':5,'full798_map_sha256':sha(ROOT/'full798-map.json'),'Cargo_or_application_runs':0}
def projected_native_reference(output):
 output.mkdir(parents=True,exist_ok=False)
 for f in(ROOT/'projected-native-reference').iterdir():shutil.copy2(f,output/f.name)
 binding=json.loads((output/'Source-binding.json').read_text());require(binding['reference_mode']=='projected-source-reference-v1'and binding['projected_reference_executed']is False,'ExplicitNewProjectedReferenceOnly');require(sha(output/'Source20-private.json')==binding['Source20_sha256']and sha(output/'plan.json')==binding['plan_sha256'],'StrictProjectedInputChecksum')
 return {'passed':True,'reference_mode':binding['reference_mode'],'Source20_sha256':binding['Source20_sha256'],'Original_Source20_sha256':binding['Original_Source20_sha256'],'new_reference_was_executed':False,'fresh_build_and_seal_review_required':True}
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--mode',choices=['original-source-executor','qualified-native-source','new-projected-native-reference'],required=True);p.add_argument('--git-repository',type=Path);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
 if a.mode!='new-projected-native-reference':require(a.git_repository is not None,'PinnedGitRepositoryRequired')
 result=original_source_executor(a.git_repository,a.output)if a.mode=='original-source-executor'else native_source(a.git_repository,a.output)if a.mode=='qualified-native-source'else projected_native_reference(a.output);print(json.dumps(result,indent=2));return 0
if __name__=='__main__':raise SystemExit(main())
