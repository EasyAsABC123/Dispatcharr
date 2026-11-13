"""Exact localGit source/archive and executed-input restoration; never runs app."""
from pathlib import Path
import argparse,gzip,hashlib,io,json,subprocess,tarfile
ROOT=Path(__file__).resolve().parent
COUPLED=['python-busy-control-vectors.json','python-busy-seek-goldens.json','python-helper-goldens.json','python-range-goldens.json','python-session-goldens.json']
def need(ok,label):
 if not ok:raise ValueError(label)
def sha(data):return hashlib.sha256(data).hexdigest()
def archive(repo,git,paths=()):
 data=subprocess.check_output(['git','-C',str(repo),'archive','--format=tar',git,*paths]);need(len(data)<=128*1048576,'GitArchive128MiB');return data
def source(repo,output,epoch):
 manifest=json.loads((ROOT/'manifest.json').read_text());output=Path(output);need(not output.exists(),'FreshSourceOutput')
 if epoch=='Original':git=manifest['Original_git'];raw=archive(repo,git);need(sha(raw)==manifest['original_archive_sha256'],'ExactOriginalArchiveSHA');expected=None
 else:
  git=manifest[epoch]['base_git']if epoch=='Native24'else manifest[epoch]['canonical_Git'];raw=archive(repo,git,['rust',*['scripts/rust-timeshift-contracts/'+n for n in COUPLED]]);expected=json.loads((ROOT/(epoch.lower()+'-source-inputs.json')).read_text())
 output.mkdir(parents=True)
 with tarfile.open(fileobj=io.BytesIO(raw))as tar:tar.extractall(output,filter='data')
 if epoch=='Native24':
  for rel,h in manifest[epoch]['overlay'].items():data=(ROOT/'native24-overlay'/rel).read_bytes();need(sha(data)==h,'ExactHistoricalNativeOverlay');p=output/rel;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(data)
 if expected is not None:
  actual={str(p.relative_to(output)):sha(p.read_bytes())for p in output.rglob('*')if p.is_file()};need(actual==expected,'EverySourcePathBlobExact')
 return {'passed':True,'epoch':epoch,'Git':git,'source_inputs':None if expected is None else len(expected),'apps_or_builds':0}
def materialize(repo,output,epoch):
 output=Path(output);need(not output.exists(),'FreshInputOutput');output.mkdir(parents=True);manifest=json.loads((ROOT/'manifest.json').read_text());mapping=json.loads((ROOT/'closure-materialization.json').read_text())[epoch];pending=[]
 for rel,row in mapping['original_files'].items():
  target=output/rel;need(not Path(rel).is_absolute()and'..'not in Path(rel).parts,'SafeOriginalInputPath');target.parent.mkdir(parents=True,exist_ok=True)
  if'exact_public_blob'in row:
   data=(ROOT/row['exact_public_blob']).read_bytes();need(sha(data)==row['original_sha256'],'ExactOriginalInputBlob');target.write_bytes(data)
  elif row['omitted_kind']=='immutableOriginalGitArchive':data=archive(repo,manifest['Original_git']);need(sha(data)==row['original_sha256'],'ExactOriginalArchiveRecipe');target.write_bytes(data)
  else:pending.append({'path':rel,**row})
 # No seal.json at assembled root: references/binaries/namespaces/build need
 # explicit NEW binding+actual qualification, not a fabricated execution seal.
 if(output/'seal.json').exists():(output/'seal.json').rename(output/'historical-executed-seal-NOT-ADMISSION.json')
 (output/'PENDING-NEW-EXECUTION-BINDING.json').write_text(json.dumps({'state':'UNEXECUTED_NO_APP_ADMISSION','epoch':epoch,'pending':pending,'public_projection_refs':'load_projection strict selectedSHA only; writing aliases is NEW credential/inputbinding, originalNative consumed originalprivatekeys','binary':'compile reconstructedSource andretain actualnewwaits/ELFchecksum; historicalELFabsent, noautomaticfallback','source_and_output_namespace':'rebind explicitly andfreezeNEWseal aftercompleteinputs, no fixedoldoutput reuse','Source_Native_app_calls':0},indent=2)+'\n')
 return {'passed':True,'exact_original_input_records':len(mapping['original_files'])-sum(r['omitted_kind']!='immutableOriginalGitArchive'for r in pending),'pending_new_binding_records':len(pending),'apps_or_builds':0,'ready_execution_seal':False}
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--repo',type=Path,required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--epoch',choices=('Original','Native24','Native2','Source24','Source2'),required=True);p.add_argument('--inputs',action='store_true');a=p.parse_args();print(json.dumps(materialize(a.repo,a.output,a.epoch)if a.inputs else source(a.repo,a.output,a.epoch),indent=2))
