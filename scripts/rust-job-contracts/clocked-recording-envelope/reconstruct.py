"""Portable Git6fa+three explicit shipped overlays; no private source fallback."""
import argparse,hashlib,json,os,subprocess,tarfile,tempfile
from pathlib import Path
HERE=Path(__file__).resolve().parent
BASE='6fa3c05b61fbbfc63c33ec742424fb8fcf788fb6'
MAP='293c25102e2f2e75363e6a29c790d2542c198f30c06d5194e8c924efb2a25429'
OVERLAYS={'rust/crates/job-transport/src/lib.rs','rust/crates/job-transport/src/clocked_recording_envelope.rs','rust/crates/job-transport/datasets/source-clocked-recording-producer.json'}
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def require(ok,label):
 if ok is not True:raise ValueError(label)
def reconstruct(repo,output):
 repo=Path(repo).resolve();out=Path(output).resolve();require(not out.exists(),'FreshOutputRequired');manifest=json.loads((HERE/'binding.json').read_text());require(manifest['base']==BASE,'ImmutableBase');require(set(manifest['overlays'])==OVERLAYS,'ExactThreeOverlayPaths');require(sha(HERE/'source-inputs.json')==MAP,'ExactQualified849Map')
 for path,row in manifest['overlays'].items():
  payload=Path(row['payload']);require(not payload.is_absolute()and len(payload.parts)==2 and payload.parts[0]=='overlays'and'..'not in payload.parts,'OwnedRelativePayload');require(sha(HERE/payload)==row['sha256'],'ExactOverlay:'+path)
 env={key:value for key,value in os.environ.items()if not key.startswith('GIT_')}
 with tempfile.TemporaryDirectory(prefix='clocked-parser-reconstruct-',dir=out.parent)as tmp:
  archive=Path(tmp)/'source.tar';result=subprocess.run(['git','-C',str(repo),'archive','--format=tar','--output',str(archive),BASE,'rust',*manifest['coupled']],env=env,capture_output=True,timeout=30)
  require(result.returncode==0,'ActualGitArchiveZero');require(len(result.stdout)+len(result.stderr)<=65536,'GitDiagnosticCapacity');require(sha(archive)==manifest['base_archive_sha256'],'ExactBaseArchive');out.mkdir()
  try:
   with tarfile.open(archive)as tar:tar.extractall(out,filter='data')
   for path,row in manifest['overlays'].items():
    dest=out/path;before=sha(dest)if dest.exists()else None;require(before==row['preimage_sha256'],'ExactPreimage:'+path);dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes((HERE/row['payload']).read_bytes())
   expected=json.loads((HERE/'source-inputs.json').read_text());actual={str(p.relative_to(out)):sha(p)for p in out.rglob('*')if p.is_file()};require(actual==expected and len(actual)==849,'All849ExactSourceAndCoupledInputs')
  except BaseException:
   # Retain any partial local reconstruction/negative; never call it verified.
   raise
 return {'passed':True,'base':BASE,'source_inputs':849,'Rust_inputs':844,'coupled_inputs':5,'source_map_sha256':sha(HERE/'source-inputs.json'),'app_or_Cargo_or_Source_executed':False}
if __name__=='__main__':
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--repo',type=Path,required=True);parser.add_argument('--output',type=Path,required=True);args=parser.parse_args();print(json.dumps(reconstruct(args.repo,args.output),indent=2))
