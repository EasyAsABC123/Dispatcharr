#!/usr/bin/env python3
"""Immutable Git reconstruction of exact1351 image59 app bytes/archive; never builds."""
import argparse,hashlib,io,json,os,tarfile
from pathlib import Path
from bounded_process import Collected
HERE=Path(__file__).resolve().parent
REVISION='c0b824e115665a17823ac6511d465f1aaed2a6f1'
ORIGINAL='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
BUILD_SHA='a366ae13b8312cd20f69e09090b55031152f4240c26df4f7b6cbdd96294a8acf'
SUPPORT_SHA='4d4108f0de65be15229e5441d83367f92032de042362531d7bdbabc84694966c'
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
 need(build['git_revision']==support['revision']==REVISION and len(build['source_sha256'])==1351,'ExactImmutable1351Revision')
 files=collect(repo,REVISION,['LICENSE','ATTRIBUTION.md','rust','frontend']+list(support['files']));expected={**build['source_sha256'],**support['files']};need({n:hashlib.sha256(b).hexdigest()for n,b in files.items()}==expected,'All1351AppPlusTwoBuilderGitBlobsExact')
 original=collect(repo,ORIGINAL,['frontend','LICENSE']);frontend={n:hashlib.sha256(b).hexdigest()for n,b in original.items()if n.startswith('frontend/')};need(len(frontend)==495 and frontend=={n:s for n,s in build['source_sha256'].items()if n.startswith('frontend/')},'ActualOriginalFrontend495GitIdentity');need(hashlib.sha256(original['LICENSE']).hexdigest()==build['source_sha256']['LICENSE'],'ActualOriginalLicenseGitIdentity')
 app={n:files[n]for n in build['source_sha256']};archive=normalized(app);need(hashlib.sha256(archive).hexdigest()==build['source_archive_sha256'],'ExactNormalizedSourceArchive')
 output.mkdir(parents=False,exist_ok=False)
 for name,data in files.items():
  p=output/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(data)
 (output/'source.tar').write_bytes(archive)
 return {'app_inputs':1351,'Rust_inputs':854,'Original_frontend_inputs':495,'credits':2,'builder_support_files':2,'normalized_source_archive_sha256':build['source_archive_sha256'],'Git_revision':REVISION,'no_private_mutable_or_HEAD_fallback':True,'Docker_Cargo_or_app_executed':False,'SDK_package28_performance_result_transfer':False}
if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--repo',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();print(json.dumps(reconstruct(a.repo,a.output),indent=2))
