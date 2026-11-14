#!/usr/bin/env python3
"""Immutable Git reconstruction of exact1353 image60 app bytes/archive; never builds."""
import argparse,hashlib,io,json,os,tarfile
from pathlib import Path
from bounded_process import Collected
HERE=Path(__file__).resolve().parent
REVISION='2dd210cf37877d9e132c864a54889fc66d6dd3b1'
ORIGINAL='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
BUILD_SHA='2581baefe38af925502a29ef625ac978f7a1ae91866c10e8441b07de0b47c234'
QUALIFIED_SHA='e549f0ecad6a9a2072a9e210c96bbc31070c97cb402a934ee45c54647929d2fb'
SUPPORT_SHA='0ae9133ec2a3671c17b16fd40afd34489bd65c80607c343afdcbf5c5f27a33a3'
def need(v,name):
 if not v:raise ValueError(name)
def readtar(raw):
 need(len(raw)<=33554432,'BoundedImmutableGitArchive');found={}
 with tarfile.open(fileobj=io.BytesIO(raw),mode='r:')as archive:
  for row in archive:
   path=Path(row.name);need(not path.is_absolute()and'..'not in path.parts,'SafeGitPath')
   if row.isdir():continue
   need(row.isfile()and not row.issym()and not row.islnk()and row.name not in found,'RegularUniqueGitBlob');found[row.name]=archive.extractfile(row).read()
 return found
def normalized(files):
 out=io.BytesIO()
 with tarfile.open(fileobj=out,mode='w')as archive:
  for name,data in sorted(files.items()):
   row=tarfile.TarInfo(name);row.size,row.mode,row.mtime=len(data),0o644,0;archive.addfile(row,io.BytesIO(data))
 return out.getvalue()
def collect(repo,revision,paths):
 env={k:v for k,v in os.environ.items()if not k.startswith('GIT_')};job=Collected(['git','--git-dir='+str(Path(repo).resolve()/'.git'),'archive',revision,'--']+list(paths),env,33554432,lambda:None);job.wait(30);need(job.physically_settled and all(not t.is_alive()for t in job.threads),'GitChildPipesSettled');return readtar(bytes(job.output['stdout']))
def reconstruct(repo,output):
 output=Path(output);need(not output.exists()and not output.is_symlink(),'AbsentOutputOnly');raw=(HERE/'build.json').read_bytes();need(hashlib.sha256(raw).hexdigest()==BUILD_SHA,'ExactBuildManifest');build=json.loads(raw);support_raw=(HERE/'builder-support-inputs.json').read_bytes();need(hashlib.sha256(support_raw).hexdigest()==SUPPORT_SHA,'ExactBuilderSupportMap');support=json.loads(support_raw)
 need(build['git_revision']==support['revision']==REVISION and len(build['source_sha256'])==1353,'ExactImmutable1353Revision')
 qualified_raw=(HERE/'qualified-inputs.json').read_bytes();need(hashlib.sha256(qualified_raw).hexdigest()==QUALIFIED_SHA,'ExactQualified861MapBytes');qualified=json.loads(qualified_raw);need(len(qualified)==861 and sum(n.startswith('rust/')for n in qualified)==856,'FullQualified861Map');coupled={n:v for n,v in qualified.items()if not n.startswith('rust/')}
 files=collect(repo,REVISION,['LICENSE','ATTRIBUTION.md','rust','frontend']+list(support['files'])+list(coupled));expected={**build['source_sha256'],**support['files'],**coupled};need({n:hashlib.sha256(b).hexdigest()for n,b in files.items()}==expected,'All1353AppPlusTwoBuilderGitBlobsExact')
 original=collect(repo,ORIGINAL,['frontend','LICENSE']);frontend={n:hashlib.sha256(b).hexdigest()for n,b in original.items()if n.startswith('frontend/')};need(len(frontend)==495 and frontend=={n:s for n,s in build['source_sha256'].items()if n.startswith('frontend/')},'ActualOriginalFrontend495GitIdentity');need(hashlib.sha256(original['LICENSE']).hexdigest()==build['source_sha256']['LICENSE'],'ActualOriginalLicenseGitIdentity')
 app={n:files[n]for n in build['source_sha256']};archive=normalized(app);need(hashlib.sha256(archive).hexdigest()==build['source_archive_sha256'],'ExactNormalizedSourceArchive')
 output.mkdir(parents=False,exist_ok=False)
 for name,data in files.items():
  p=output/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(data)
 (output/'source.tar').write_bytes(archive)
 return {'app_inputs':1353,'Rust_inputs':856,'Original_frontend_inputs':495,'credits':2,'builder_support_files':2,'separately_restored_coupled_inputs':5,'normalized_source_archive_sha256':build['source_archive_sha256'],'Git_revision':REVISION,'no_private_mutable_or_HEAD_fallback':True,'Docker_Cargo_or_app_executed':False,'SDK_package28_performance_result_transfer':False}
if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--repo',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();print(json.dumps(reconstruct(a.repo,a.output),indent=2))
