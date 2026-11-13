"""Local Git reconstruction of the historical830 source; no app/Cargo/network."""
import argparse,hashlib,io,json,shutil,subprocess
from pathlib import Path
ROOT=Path(__file__).resolve().parent
BASE='18bf274cda795a0177e78b999fccfd4126d0a791'
def require(ok,label):
 if not ok:raise ValueError(label)
def reconstruct(destination,repo):
 require(not destination.exists(),'FreshDestination');mapping=json.loads((ROOT/'compiled830-map.json').read_text());require(len(mapping)==830,'ExactHistorical830Count');names=sorted(mapping)
 result=subprocess.run(['git','-C',str(repo),'cat-file','--batch'],input=''.join(BASE+':'+name+'\n'for name in names).encode(),stdout=subprocess.PIPE,stderr=subprocess.PIPE);require(result.returncode==0,'LocalReadOnlyGitExit');stream=io.BytesIO(result.stdout);destination.mkdir(parents=True,exist_ok=False)
 for name in names:
  path=Path(name);require(not path.is_absolute()and'..'not in path.parts,'SafeSourcePath');header=stream.readline().decode().strip().split();raw=None
  if header[-1]!='missing':require(header[1]=='blob','GitBlob');raw=stream.read(int(header[2]));require(stream.read(1)==b'\n','GitBoundary')
  overlay=ROOT/'runtime'/path
  if overlay.is_file():raw=overlay.read_bytes()
  require(raw is not None and hashlib.sha256(raw).hexdigest()==mapping[name],'ExactHistoricalSourceBlob:'+name);out=destination/'source'/path;out.parent.mkdir(parents=True,exist_ok=True);out.write_bytes(raw)
 require(not stream.read(),'GitBatchConsumed');shutil.copytree(ROOT/'executed',destination/'executed')
 return {'state':'EXACT_HISTORICAL830_SOURCE_AND_EXECUTION_DEFINITIONS_RECONSTRUCTED','Rust_files':825,'coupled_files':5,'base_Git':BASE,'ELF_explicitly_omitted_not_rebuilt':True,'private_credential_raw_captures_omitted':True,'new_app_Cargo_or_network_runs':0}
if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--git-repo',type=Path,required=True);p.add_argument('--destination',type=Path,required=True);a=p.parse_args();print(json.dumps(reconstruct(a.destination,a.git_repo)))
