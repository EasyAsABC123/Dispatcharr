"""Exact local Git+overlay/input reconstruction; never builds or runs apps."""
import argparse,hashlib,io,json,subprocess,tarfile
from pathlib import Path
ROOT=Path(__file__).resolve().parent
need=lambda ok,label:None if ok else (_ for _ in()).throw(ValueError(label))
H=lambda raw:hashlib.sha256(raw).hexdigest()
COUPLED=['python-busy-control-vectors.json','python-busy-seek-goldens.json','python-helper-goldens.json','python-range-goldens.json','python-session-goldens.json']
def source(repo,output,negative=False):
 m=json.loads((ROOT/'manifest.json').read_bytes());output=Path(output);need(not output.exists(),'FreshSourceOutput');r=subprocess.run(['git','-C',str(repo),'archive','--format=tar',m['source_base_git'],'rust',*['scripts/rust-timeshift-contracts/'+name for name in COUPLED]],stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=60);need(r.returncode==0 and len(r.stdout)<=134217728,'BoundedGitArchive0');output.mkdir(parents=True)
 with tarfile.open(fileobj=io.BytesIO(r.stdout))as tar:tar.extractall(output,filter='data')
 policy=json.loads((ROOT/'negative-closure-materialization.json').read_bytes())if negative else m
 for name,h in policy['source_overlay'].items():
  raw=(ROOT/('negative-source-overlay'if negative else'source-overlay')/name).read_bytes();need(H(raw)==h,'ExactOverlay');p=output/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(raw)
 mapfile=ROOT/('negative-source-inputs.json'if negative else'actual-source-inputs.json');need(H(mapfile.read_bytes())==policy['source_map_sha256'],'ExactSourceMap');expected=json.loads(mapfile.read_bytes());actual={str(p.relative_to(output)):H(p.read_bytes())for p in output.rglob('*')if p.is_file()};need(actual==expected and len(actual)==883,'Every883ActualSourceBlob')
 return {'passed':True,'epoch':'negative'if negative else'corrected','Git':m['source_base_git'],'inputs':883,'apps_builds_Docker':0}
def materialize(output,negative=False):
 output=Path(output);need(not output.exists(),'FreshInputOutput');output.mkdir(parents=True);policy=json.loads((ROOT/('negative-closure-materialization.json'if negative else'closure-materialization.json')).read_bytes());pending=[];count=0
 for name,row in policy['original_inputs'].items():
  need(not Path(name).is_absolute()and'..'not in Path(name).parts,'SafeOriginalInputPath')
  if'exact_public_blob'not in row:pending.append({'path':name,**row});continue
  raw=(ROOT/row['exact_public_blob']).read_bytes();need(H(raw)==row['original_sha256']and len(raw)==row['original_bytes'],'EveryOriginalInputByte');p=output/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(raw);count+=1
 (output/'PENDING-NEW-BINDING-NOT-ADMISSION.json').write_text(json.dumps({'state':'UNEXECUTED','pending':pending,'required':'NEW explicitly selected publicSource reference/credential+fingerprint policy, rebuilt883 ELF actualwait proof, source role paths/freshnamespaces/output/mounted closure/newseal. Historical hashes do not accept a projected private-reference fallback. No Source replay or app admission.','apps_builds_Docker':0},indent=2)+'\n');return {'passed':True,'exact_original_inputs':count,'pending_NEW_binding_inputs':len(pending),'execution_ready':False,'apps_builds_Docker':0}
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--repo',type=Path);p.add_argument('--output',type=Path,required=True);p.add_argument('--epoch',choices=['corrected','negative'],default='corrected');p.add_argument('--inputs',action='store_true');a=p.parse_args();need(a.inputs or a.repo is not None,'RepoRequiredForSource');print(json.dumps(materialize(a.output,a.epoch=='negative')if a.inputs else source(a.repo,a.output,a.epoch=='negative')))
