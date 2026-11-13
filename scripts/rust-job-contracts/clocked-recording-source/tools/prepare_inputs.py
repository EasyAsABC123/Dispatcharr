"""Local explicit metadata only; no subprocess, Docker or app dispatch."""
import argparse,ast,hashlib,json,os,shutil
from pathlib import Path
def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def main():
 p=argparse.ArgumentParser(description=__doc__)
 for name in ('repo','support-root','output','inputs','image-inspect','tooling-script'):p.add_argument('--'+name,type=Path,required=True)
 a=p.parse_args();root=Path(__file__).resolve().parent;repo=a.repo.resolve();output=a.output.resolve();inputs=a.inputs.resolve();support=a.support_root.resolve()
 if output.exists()or inputs.exists():raise ValueError('FreshOutputAndInputs')
 tree=ast.parse((root/'run_source.py').read_text());required=next(ast.literal_eval(n.value)for n in tree.body if isinstance(n,ast.Assign)and any(isinstance(t,ast.Name)and t.id=='REQUIRED'for t in n.targets))
 for name,h in json.loads((root/'support-inputs.json').read_text()).items():
  if sha(support/name)!=h:raise ValueError('ExactSupport:'+name)
 full_support=json.loads((root.parent/'support-full-inputs.json').read_text())
 if len(full_support['files'])!=6148 or full_support['file_count']!=6148 or full_support['revision']!='f0bfc2f200dcfd1f64ae32a9855ea250731ce67f':raise ValueError('ExactFullSupport6148')
 for name,h in full_support['files'].items():
  if sha(support/name)!=h:raise ValueError('FullSupport:'+name)
 image=json.loads(a.image_inspect.read_text())
 if image['actual_exit']!=0 or image['process_settled']is not True or image['image']['Id']!='sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f':raise ValueError('ActualRetainedPinnedImageReceipt')
 docker=shutil.which('docker');compose=Path(os.environ['DOCKER_CONFIG'])/'cli-plugins/docker-compose'
 if not docker or not compose.is_file():raise ValueError('ActualToolingPaths')
 row={'authorized':False,'status':'UNEXECUTED; Root exact once admission required','repo':str(repo),'support_root':str(support),'output':str(output),'Original_archive_sha256':'f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff','Original_image_id':image['image']['Id'],'Original_image_inspect':{'path':str(a.image_inspect.resolve()),'sha256':sha(a.image_inspect)},'support_full_manifest_sha256':sha(root.parent/'support-full-inputs.json'),'pinned_source_proof_sha256':sha(root.parent/'pinned-source-proof.json'),'case_plan_sha256':sha(root.parent/'case-plan.json'),'tools_sha256':{name:sha(root/name)for name in sorted(required)},'Docker_host':os.environ['DOCKER_HOST'],'docker_executable_sha256':sha(docker),'tooling_environment':{key:os.environ[key]for key in ('PATH','DOCKER_HOST','DOCKER_CONFIG','CARGO_HOME','RUSTUP_HOME','RUSTUP_TOOLCHAIN')},'tooling_environment_script':{'path':str(a.tooling_script.resolve()),'sha256':sha(a.tooling_script)},'compose_executable':{'path':str(compose.resolve()),'sha256':sha(compose)}}
 inputs.write_text(json.dumps(row,indent=2)+'\n');print(json.dumps({'inputs':str(inputs),'sha256':sha(inputs),'authorized':False,'app_executed':False}))
if __name__=='__main__':main()
