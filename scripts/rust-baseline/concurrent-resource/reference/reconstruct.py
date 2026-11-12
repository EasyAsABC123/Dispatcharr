#!/usr/bin/env python3
"""Reconstruct exact private activated controller; does not execute it or cue it.
Published Source7 and its two full-resource entry blocks remain unchanged.
"""
import argparse,hashlib,json,shutil
from pathlib import Path
HERE=Path(__file__).resolve().parent
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def reconstruct(output,directory=HERE):
 directory=Path(directory);source=directory.parent/'controller';output=Path(output).resolve()
 if output.exists()or output==directory.resolve()or source.resolve()in output.parents:raise ValueError('FreshExternalOutputRequired')
 expected=json.loads((directory/'expected-activated-files.json').read_text())['file_sha256'];base={p.name:sha(p)for p in source.iterdir()if p.is_file()}
 blocked=json.loads((directory/'blocked-controller-sha256.json').read_text())
 if base!=blocked or len(base)!=36 or set(base)!=set(expected)or any(base[n]!=h for n,h in expected.items()if n!='run.py'):raise ValueError('PublishedBase36Mismatch')
 template=directory/'activated-run.py.in'
 if sha(template)!=expected['run.py']or'RESOURCE_EXECUTION_BLOCKED=True'not in(source/'run.py').read_text():raise ValueError('ActivationBinding')
 output.mkdir(parents=True)
 for name in expected:shutil.copyfile(template if name=='run.py'else source/name,output/name)
 actual={p.name:sha(p)for p in output.iterdir()if p.is_file()}
 if actual!=expected:raise ValueError('ReconstructionHash')
 return {'reconstructed_files':36,'expected_hashes_match':True,'execution_performed':False,'fresh_Root_review_and_quiet_cue_still_required':True,'public_entries_still_blocked':True}
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',type=Path,required=True);a=p.parse_args();print(json.dumps(reconstruct(a.output),indent=2))
if __name__=='__main__':main()
