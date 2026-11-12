"""Bind unchanged frozen QA semantics to freshly rebuilt exact788 peers.
Original Dispatcharr/dependency authors retain code/algorithm/license credit.
This writes a new explicit execution receipt; historical seals/results stay intact.
"""
import argparse,hashlib,json,shutil
from pathlib import Path
HERE=Path(__file__).parent
FAMILIES=['interop','store','physical','legacy-admission','legacy-store','legacy-physical']
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def verify(build):
 build=Path(build).resolve();record=json.loads((build/'build.json').read_text());expected=sha(HERE/'source-inputs-788.json')
 if record.get('passed')is not True or record.get('source_file_count')!=788 or record.get('input_manifest_sha256')!=expected:raise ValueError('Exact reconstructed788 build required')
 if set(record.get('elf_sha256',{}))!={'dispatcharr_store','dispatcharr_job_transport','import_sql'}:raise ValueError('Exact three outside peers required')
 for name,digest in record['elf_sha256'].items():
  if sha(build/name)!=digest:raise ValueError('Changed outside peer: '+name)
 for name,key in [('compiler.jsonl','compiler_json_sha256'),('legacy-compiler.jsonl','legacy_compiler_json_sha256')]:
  if sha(build/name)!=record.get(key):raise ValueError('Changed compiler record')
 return record

def bind(build,output):
 build=Path(build).resolve();record=verify(build);output=Path(output);output.mkdir(parents=True,exist_ok=False);proof={'status':'FRESH_PREEXECUTION_BINDING_NOT_HISTORICAL_RESULT','source_map_sha256':record['input_manifest_sha256'],'build_record_sha256':sha(build/'build.json'),'outside_ELFs':record['elf_sha256'],'families':{}}
 for family in FAMILIES:
  before=HERE/family;after=output/family;shutil.copytree(before,after,ignore=shutil.ignore_patterns('__pycache__'));original={p.name:sha(p)for p in before.iterdir()if p.is_file()};changes={}
  receipt=after/'execution-input-receipt-1.json'
  if receipt.exists():
   data=json.loads(receipt.read_text());data['status']='FRESH_BUILD_PREEXECUTION_INPUT_RECEIPT';data['historical_receipt_sha256']=sha(before/receipt.name);data['compiler_record']={'path':str(build/'build.json'),'sha256':proof['build_record_sha256']}
   for name in ['dispatcharr_store','dispatcharr_job_transport']:data['outside_ELFs'][name]={'path':str(build/name),'sha256':record['elf_sha256'][name]}
   receipt.write_text(json.dumps(data,indent=2)+'\n');changes[receipt.name]={'before':original[receipt.name],'after':sha(receipt),'only':'Explicit source-bound compiler/ELF receipt; workload/semantics unchanged'}
  if family=='legacy-store':
   p=after/'run_pair.py';text=p.read_text();old="'1f2b183851e29c9df1460a255109c1a92541b2f98442d2544212d8fd3922ee48'";assert text.count(old)==1;text=text.replace(old,repr(record['elf_sha256']['import_sql']));p.write_text(text);changes[p.name]={'before':original[p.name],'after':sha(p),'only':'Legacy integration ELF digest constant, no case/assertion/timeout change'}
  proof['families'][family]={'historical_tool_hashes':original,'explicit_binding_changes':changes,'new_tools':{p.name:sha(p)for p in after.iterdir()if p.is_file()}}
 (output/'fresh-execution-inputs.json').write_text(json.dumps(proof,indent=2)+'\n');return proof
if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--build',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();bind(a.build,a.output);print(json.dumps({'passed':True,'output':str(a.output),'scope':'Fresh binding only; no fixture executed'}))
