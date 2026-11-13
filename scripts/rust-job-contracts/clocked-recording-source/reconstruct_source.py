"""Full pinned Original Git source, never a live/private working-tree fallback."""
import argparse,hashlib,json,os,subprocess,tarfile,tempfile
from pathlib import Path
HERE=Path(__file__).resolve().parent
BASE='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
ARCHIVE='f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff'
def need(value,label):
 if value is not True:raise ValueError(label)
def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def reconstruct(repo,output):
 out=Path(output).resolve();need(not out.exists(),'FreshSourceOutput');env={k:v for k,v in os.environ.items()if not k.startswith('GIT_')}
 with tempfile.TemporaryDirectory(dir=out.parent,prefix='clocked-Original-source-')as tmp:
  archive=Path(tmp)/'source.tar';result=subprocess.run(['git','-C',str(Path(repo).resolve()),'archive','--format=tar','--output',str(archive),BASE],env=env,capture_output=True,timeout=30);need(result.returncode==0 and len(result.stdout)+len(result.stderr)<=65536,'ActualPinnedGitArchive');need(sha(archive)==ARCHIVE,'ExactWholeOriginalArchive');out.mkdir()
  with tarfile.open(archive)as tar:tar.extractall(out,filter='data')
  proof=json.loads((HERE/'pinned-source-proof.json').read_text())
  for name,row in proof['modules'].items():need(sha(out/name)==row['sha256']==sha(HERE/'original'/name),'ExactOriginalFullModule:'+name)
 return {'passed':True,'Original_revision':BASE,'Original_full_archive_sha256':ARCHIVE,'full_Git_source_not_live_WIP':True,'checked_Original_modules':len(proof['modules']),'app_or_Django_setup_or_Source_executed':False}
if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--repo',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();print(json.dumps(reconstruct(a.repo,a.output),indent=2))
