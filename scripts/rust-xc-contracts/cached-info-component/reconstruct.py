"""Exact historical cached-info29 source; explicit public catalogue dependency, no private/HEAD fallback."""
import argparse,hashlib,importlib.util,json,sys
from pathlib import Path
HERE=Path(__file__).resolve().parent
INPUTS_SHA='311b307e2408c72599fe534794af04904774bef78057e8183613f24d656f7864'
def sha(raw):return hashlib.sha256(raw).hexdigest()
def need(ok,label):
 if not ok:raise ValueError(label)
def load():
 raw=(HERE/'source-inputs.json').read_bytes();need(sha(raw)==INPUTS_SHA,'ExactHistoricalInfoInputs');spec=json.loads(raw);parent=HERE.parent/spec['parent_namespace'];need(parent.name=='cached-catalogue-component'and sha((parent/'source-inputs.json').read_bytes())==spec['parent_inputs_sha256']and sha((parent/'reconstruct.py').read_bytes())==spec['parent_reconstructor_sha256'],'ExactPublicCatalogueDependency')
 old=json.loads((parent/'source-inputs.json').read_bytes())['inputs'];new=spec['inputs'];changes={r['path']:r for r in spec['overlay']};need(len(new)==830 and sum(k.startswith('rust/')for k in new)==825 and len(changes)==7 and len(spec['overlay'])==7 and set(old)<=set(new),'ExactHistorical825AndSevenDelta')
 for path,h in new.items():
  p=Path(path);need(not p.is_absolute()and'..'not in p.parts,'SafeInputPath');row=changes.get(path)
  if row is None:need(old.get(path)==h,'UnchangedPublicCatalogueByte')
  else:
   need(row['preimage_sha256']==old.get(path)and row['sha256']==h,'ExactInfoPreimage');f=HERE/'overlay'/p;need(f.is_file()and not f.is_symlink()and sha(f.read_bytes())==h,'ExactInfoPayload')
 need(spec['canonical_production_gateway_Main_deltas_are_separate']is True,'NoCanonicalTransplant');return spec,parent

def reconstruct(repo,out):
 spec,parent=load();out=Path(out);need(not out.exists()and not out.is_symlink(),'FreshOutputRequired');sys.path.insert(0,str(parent));module=importlib.util.spec_from_file_location('qualified_catalogue_reconstructor',parent/'reconstruct.py');base=importlib.util.module_from_spec(module);module.loader.exec_module(base);base.reconstruct(Path(repo),out)
 for row in spec['overlay']:
  dest=out/row['path']
  if row['preimage_sha256']is None:need(not dest.exists(),'NewInfoPathOnly')
  else:need(dest.is_file()and sha(dest.read_bytes())==row['preimage_sha256'],'RestoredCataloguePreimage')
  dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes((HERE/'overlay'/row['path']).read_bytes())
 need(all(sha((out/p).read_bytes())==h for p,h in spec['inputs'].items()),'FullHistorical830InputEquality');result={'passed':True,'scope':'Historical825Rust+5coupled info29, not canonical830/Main','full_inputs':830,'Rust_inputs':825,'source_manifest_sha256':INPUTS_SHA,'public_catalogue_dependency_exact':True,'private_fallback':False,'app_build_or_fixture':False};(out/'reconstruction.json').write_text(json.dumps(result,indent=2)+'\n');return result
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--repo',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();print(json.dumps(reconstruct(a.repo,a.output)))
