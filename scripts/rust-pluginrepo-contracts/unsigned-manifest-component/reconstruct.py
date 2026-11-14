"""Git+exact overlay restoration only; never compile or execute an application."""
import argparse,hashlib,io,json,subprocess,tarfile
from pathlib import Path
ROOT=Path(__file__).resolve().parent
COUPLED=['python-busy-control-vectors.json','python-busy-seek-goldens.json','python-helper-goldens.json','python-range-goldens.json','python-session-goldens.json']
def need(ok,label):
 if not ok:raise ValueError(label)
def source(repo,output,epoch):
 m=json.loads((ROOT/'source-provenance.json').read_bytes());output=Path(output);need(not output.exists(),'FreshSourceOutput');physical=epoch=='Physical';policy=json.loads((ROOT/'physical-policy.json').read_bytes())if physical else None
 if physical:m.update(Native_base_git=policy['source_base_git'],native_overlay=policy['source_overlay'],Native_source_map_sha256=policy['source_map_sha256'])
 paths=[]if epoch=='Original'else['rust',*['scripts/rust-timeshift-contracts/'+n for n in COUPLED]];git=m['Original_git']if epoch=='Original'else m['Native_base_git'];r=subprocess.run(['git','-C',str(repo),'archive','--format=tar',git,*paths],stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=60);need(r.returncode==0 and len(r.stdout)<=128*1048576,'BoundedGitArchive0');raw=r.stdout
 if epoch=='Original':need(hashlib.sha256(raw).hexdigest()==m['original_archive_sha256'],'ExactOriginalArchive')
 output.mkdir(parents=True)
 with tarfile.open(fileobj=io.BytesIO(raw))as tar:
  for row in tar.getmembers():need(not row.name.startswith('/')and'..'not in Path(row.name).parts,'SafeGitMember')
  tar.extractall(output,filter='data')
 if epoch!='Original':
  for name,h in m['native_overlay'].items():
   data=(ROOT/('physical-overlay'if physical else'native-overlay')/name).read_bytes();need(hashlib.sha256(data).hexdigest()==h,'ExactHistoricalOverlay');p=output/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(data)
  need(hashlib.sha256((ROOT/('physical-source-inputs.json'if physical else'native-source-inputs.json')).read_bytes()).hexdigest()==m['Native_source_map_sha256'],'ExactExpectedMap');expected=json.loads((ROOT/('physical-source-inputs.json'if physical else'native-source-inputs.json')).read_bytes());actual={str(p.relative_to(output)):hashlib.sha256(p.read_bytes()).hexdigest()for p in output.rglob('*')if p.is_file()};need(actual==expected,'EveryActualNative881PathBlob')
 return {'passed':True,'epoch':epoch,'Git':git,'inputs':None if epoch=='Original'else len(actual),'apps_builds_Docker':0}
def materialize(repo,output,epoch):
 output=Path(output);need(not output.exists(),'FreshInputOutput');mapping=json.loads((ROOT/'closure-materialization.json').read_bytes())['epochs'];need(epoch in mapping,'FiniteExecutedClosure');output.mkdir(parents=True);pending=[];exact=0
 for name,row in mapping[epoch]['entries'].items():
  need(not Path(name).is_absolute()and'..'not in Path(name).parts,'SafeInputMember');target=output/name
  if 'exact_public_blob'in row:data=(ROOT/row['exact_public_blob']).read_bytes()
  elif row['omitted_kind']=='immutable_Original_git_archive_recipe':
   result=subprocess.run(['git','-C',str(repo),'archive','--format=tar','1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'],stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=60);need(result.returncode==0 and len(result.stdout)<=128*1048576,'BoundedOriginalGitArchive');data=result.stdout
  else:pending.append({'path':name,**row});continue
  need(hashlib.sha256(data).hexdigest()==row['original_sha256']and len(data)==row['original_bytes'],'EveryRestoredOriginalInputByte');target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(data);exact+=1
 historical=(ROOT/mapping[epoch]['historical_seal']).read_bytes();need(hashlib.sha256(historical).hexdigest()==mapping[epoch]['original_seal_sha256'],'ExactHistoricalSeal');(output/'HISTORICAL-SEAL-NOT-ADMISSION.json').write_bytes(historical);(output/'PENDING-NEW-BINDING.json').write_text(json.dumps({'state':'UNEXECUTED_NOT_APP_ADMISSION','epoch':epoch,'pending':pending,'required':'Explicit NEW selected-public reference/credential aliases, rebuilt ELF and actual wait proof, source-role path, fresh namespace/output, full mounted closure and NEW seal. No original-or-projected SHA fallback. No Source replay instruction.','apps_builds_Docker':0},indent=2)+'\n');return {'passed':True,'exact_restored_inputs':exact,'pending_new_bindings':len(pending),'app_admission':False,'apps_builds_Docker':0}
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--repo',required=True,type=Path);p.add_argument('--output',required=True,type=Path);p.add_argument('--epoch',required=True,choices=['Original','Native','Physical','Source24','Source404','Native24','Native404','Physical2']);p.add_argument('--inputs',action='store_true');a=p.parse_args();need(a.inputs or a.epoch in('Original','Native','Physical'),'SourceEpochOrInputsRequired');print(json.dumps(materialize(a.repo,a.output,a.epoch)if a.inputs else source(a.repo,a.output,a.epoch)))
