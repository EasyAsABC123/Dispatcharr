"""Git-only exact860 source or hash-bound historical input materialization.
No compiler, Docker, Source or Native application is ever started here.
"""
from pathlib import Path
import argparse,gzip,hashlib,json,subprocess
ROOT=Path(__file__).resolve().parent

def sha(data):return hashlib.sha256(data).hexdigest()
def need(ok,label):
 if not ok:raise ValueError(label)
def source(output,repo):
 expected=json.loads((ROOT/'source-inputs860.json').read_text());plan=json.loads((ROOT/'source-reconstruction.json').read_text());need(not output.exists()and len(expected)==860,'FreshExact860Output');output.mkdir(parents=True)
 for name,h in expected.items():
  need(not Path(name).is_absolute()and'..'not in Path(name).parts,'BoundedRelativeSourcePath');data=subprocess.check_output(['git','-C',str(repo),'show',plan['exact_published_source_Git']+':'+name]);need(sha(data)==h,'ExactCommittedBlob:'+name);p=output/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(data);p.chmod(0o444)
 need({str(p.relative_to(output)):sha(p.read_bytes())for p in output.rglob('*')if p.is_file()}==expected,'ExactAll860NoWIPFallback');return {'passed':True,'source_count':860,'Git':plan['exact_published_source_Git'],'no_compiler_or_app':True}
def inputs(name,output):
 manifest=json.loads((ROOT/'manifest.json').read_text());need(name in manifest['closures']and not output.exists(),'ExactClosureFreshOutput');output.mkdir(parents=True);records=json.loads((ROOT/manifest['closures'][name]['input_records']).read_text());pending=[];newbindings=[]
 for rel,v in records.items():
  need(not Path(rel).is_absolute()and'..'not in Path(rel).parts,'RelativeInputPath')
  if v['kind']=='ELF_REBUILD_REQUIRED':pending.append(rel);continue
  data=(ROOT/v['blob']).read_bytes()
  if v.get('blob_encoding')=='gzip':need(sha(data)==v['selected_blob_compressed_sha256'],'ExactCompressedInputBlob');data=gzip.decompress(data)
  need(sha(data)==v['selected_sha256'],'ExactSelectedInputBlob');p=output/rel;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(data);p.chmod(0o444)
  if v['kind']=='PROJECTED_NEW_INPUT_BINDING':newbindings.append({'path':rel,'original_SHA256':v['original_sha256'],'selected_NEW_SHA256':v['selected_sha256']})
 result={'passed_materialization_only':True,'original_executed_closure_count':len(records),'materialized':len(records)-len(pending),'pending_ELF_rebuild':pending,'new_public_input_bindings':newbindings,'original_execution_seal_NOT_valid_for_NEW_reproduction':True,'requires_new_compilation_source_ELF_clock_namespace_and_reviewed_seal':True,'not_an_app_admission':True};(output/'NEW-reproduction-bindings.json').write_text(json.dumps(result,indent=2)+'\n');return result
if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--source',type=Path);p.add_argument('--repo',type=Path,default=Path.cwd());p.add_argument('--inputs');p.add_argument('--output',type=Path);a=p.parse_args()
 if a.source:result=source(a.source,a.repo)
 else:need(a.inputs and a.output,'SourceOrInputsRequired');result=inputs(a.inputs,a.output)
 print(json.dumps(result,indent=2))
