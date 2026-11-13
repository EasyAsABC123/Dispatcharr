"""Reconstruct historical sources/definitions from local Git, no apps/builds/network."""
import argparse,hashlib,io,json,shutil,subprocess
from pathlib import Path
ROOT=Path(__file__).resolve().parent
BASE='8d60b91b878b939fc3c7ea26c0a84e3de3eef60a'
ORIGINAL='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
ARCHIVE='f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff'

def require(ok,label):
 if not ok:raise ValueError(label)
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def git_blobs(repo,revision,names):
 names=list(names);p=subprocess.run(['git','-C',str(repo),'cat-file','--batch'],input=''.join(revision+':'+name+'\n'for name in names).encode(),stdout=subprocess.PIPE,stderr=subprocess.PIPE);require(p.returncode==0,'GitBatchReadOnlyExit');stream=io.BytesIO(p.stdout);out={}
 for name in names:
  line=stream.readline().decode().strip().split()
  if line[-1]=='missing':out[name]=None;continue
  require(len(line)==3 and line[1]=='blob','GitBlobType');raw=stream.read(int(line[2]));require(stream.read(1)==b'\n','GitBlobBoundary');out[name]=raw
 require(not stream.read(),'AllGitBatchConsumed');return out

def reconstruct(destination,repo):
 require(not destination.exists(),'FreshDestination');destination.mkdir(parents=True,exist_ok=False);result=[]
 for label in('QA2-case1','QA4-cases2-4'):
  definition=ROOT/'historical-builds'/label;mapping=json.loads((definition/'source-map.json').read_text());require(len(mapping)==812,'Historical812Map')
  names=sorted(mapping);blobs=git_blobs(repo,BASE,names);out=destination/label/'source'
  for name in names:
   path=Path(name);require(not path.is_absolute()and'..'not in path.parts,'SafeSourcePath');raw=blobs[name]
   if path.name in('backup_files.rs','backup_callback_physical_tests.rs'):raw=(definition/'runtime'/path.name).read_bytes()
   require(raw is not None and hashlib.sha256(raw).hexdigest()==mapping[name],'ExactHistoricalSourceBlob')
   target=out/path;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(raw)
  result.append({'label':label,'exact_source_inputs':812,'Rust_inputs':807,'coupled_inputs':5,'source_map_sha256':sha(definition/'source-map.json')})
 for attempt in(1,2,3):
  origin=ROOT/'executed'/f'attempt{attempt}';target=destination/f'attempt{attempt}';shutil.copytree(origin,target)
  execution=json.loads((target/'execution-inputs.json').read_text());support=json.loads((target/'controller/support-inputs.json').read_text())
  require(len(support)==31 and all(sha(target/'support'/name)==digest for name,digest in support.items()),'ExactExecutedSupport31')
  require(all(sha(target/'controller'/name)==digest for name,digest in execution['tools_sha256'].items()),'ExactExecutedControllerTools')
  require(all(sha(target/name)==digest for name,digest in execution['fixture_sha256'].items()),'ExactExecutedFixtures')
  proof=json.loads((target/'pinned-source-proof.json').read_text());blobs=git_blobs(repo,ORIGINAL,proof['modules'])
  for name,row in proof['modules'].items():
   raw=blobs[name];require(raw is not None and hashlib.sha256(raw).hexdigest()==row['sha256'],'PinnedOriginalModuleGitBytes')
  require(execution['Original_archive_sha256']==ARCHIVE,'ExactOriginalArchiveRecipeBinding')
 archive=subprocess.run(['git','-C',str(repo),'archive','--format=tar',ORIGINAL],stdout=subprocess.PIPE,stderr=subprocess.PIPE);require(archive.returncode==0 and hashlib.sha256(archive.stdout).hexdigest()==ARCHIVE,'ExactPinnedGitArchiveRecipe')
 return {'state':'EXACT_GIT_SOURCE_AND_EXECUTED_DEFINITIONS_RECONSTRUCTED_NO_ELF','native_sources':result,'executed_definition_sets':3,'Original_archive_sha256':ARCHIVE,'Original_archive_recipe_verified_in_memory_no_binary_published':True,'historical_two_ELFs_explicitly_omitted':True,'new_app_build_or_network_runs':0}
if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--git-repo',type=Path,required=True);p.add_argument('--destination',type=Path,required=True);a=p.parse_args();print(json.dumps(reconstruct(a.destination,a.git_repo),indent=2))
