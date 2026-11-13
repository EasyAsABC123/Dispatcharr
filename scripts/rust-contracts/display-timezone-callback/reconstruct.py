"""Reconstruct exact historical845 source and257inputs; no build/app/network."""
import argparse,hashlib,io,json,subprocess,shutil
from pathlib import Path
ROOT=Path(__file__).resolve().parent
BASE='f1fee14c665557c00771165b2bcc90d62274e1e7'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def need(ok,label):
 if not ok:raise ValueError(label)
def inputs(destination,repo,elf=None):
 need(not destination.exists(),'FreshInputsOnly');closure=json.loads((ROOT/'input-closure.json').read_text())['historical_inputs'];missing=[];destination.mkdir(parents=True)
 for name,row in closure.items():
  rel=Path(name);need(not rel.is_absolute()and'..'not in rel.parts,'SafeInputPath')
  if row['kind']=='historical-ELF-not-shipped':
   if elf is None:missing.append(name);continue
   origin=elf;need(origin.stat().st_size==row['bytes'],'HistoricalELFSize')
  elif row['kind']=='existing-published-Git-blob':origin=repo/row['path']
  else:need(row['kind']=='exact-embedded','DeclaredInputKind');origin=ROOT/row['path']
  need(origin.is_file()and sha(origin)==row['sha256'],'ExactSingleInput:'+name);target=destination/rel;target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(origin,target)
 return {'exact_local_inputs':len(closure)-len(missing),'unshipped_historical_ELF':missing,'no_application_runs':True}
def source(destination,repo):
 need(not destination.exists(),'FreshSourceOnly');mapping=json.loads((ROOT/'historical/source-map.json').read_text());selected=json.loads((ROOT/'historical/selected-source.json').read_text());need(selected['base_git']==BASE and len(mapping)==845,'Historical845Scope');names=sorted(mapping);run=subprocess.run(['git','-C',str(repo),'cat-file','--batch'],input=''.join(BASE+':'+n+'\n'for n in names).encode(),capture_output=True);need(run.returncode==0,'GitBatchExit');stream=io.BytesIO(run.stdout);destination.mkdir(parents=True)
 for name in names:
  rel=Path(name);need(not rel.is_absolute()and'..'not in rel.parts,'SafeSourcePath');header=stream.readline().decode().strip().split();original=None
  if header[-1]!='missing':need(len(header)==3 and header[1]=='blob','ExpectedBlob');original=stream.read(int(header[2]));need(stream.read(1)==b'\n','BlobBoundary')
  if name in selected['selected']:
   row=selected['selected'][name];need((None if original is None else hashlib.sha256(original).hexdigest())==row['preimage_sha256'],'HistoricalOverlayPreimage');candidate=ROOT/'historical/runtime'/name;need(sha(candidate)==row['sha256'],'HistoricalOverlayHash');data=candidate.read_bytes()
  else:data=original
  need(data is not None and hashlib.sha256(data).hexdigest()==mapping[name],'ExactHistoricalSourceBlob');target=destination/rel;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(data)
 need(not stream.read(),'AllBatchBytesConsumed');return {'source_inputs':845,'Git':BASE,'current842_union_physical_transfer':False}
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--destination',type=Path,required=True);p.add_argument('--repo',type=Path,required=True);p.add_argument('--elf',type=Path);a=p.parse_args();need(not a.destination.exists(),'FreshDestinationOnly');a.destination.mkdir();print(json.dumps({'source':source(a.destination/'source',a.repo),'inputs':inputs(a.destination/'execution',a.repo,a.elf)}))
