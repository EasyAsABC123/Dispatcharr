#!/usr/bin/env python3
"""Recompute the actual Source contract from public evidence, with no app import."""
import argparse,importlib.util,json,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT/'tools'))
from contract import validate_source_capture,same

def review(evidence):
 p=Path(evidence);raw=json.loads((p/'source-parented-recordings-private.json').read_text());integration=json.loads((p/'integration.json').read_text());root=json.loads((p/'Root-independent-review.json').read_text())
 actual=validate_source_capture(0,raw)
 if not same(actual,raw['source_acceptance']):raise ValueError('Actual Source acceptance differs')
 if integration['passed']is not True or integration['Native_verified']is not False or root['actual_wrapper_exit']!=0:raise ValueError('Actual Source-only process status')
 if (p/'default-before-private.json').read_bytes()!=(p/'default-after-private.json').read_bytes():raise ValueError('Default manifest byte difference')
 return {'passed':True,'Source_verified':True,'Native_verified':False,'cases':actual['cases'],'tasks':actual['actual_tasks'],'checks':len(raw['checks']),'root_inheritance_inferred_or_normalized':False,'default_manifest_bytes_equal':True,'new_app_execution':False}
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--evidence',type=Path,required=True);a=p.parse_args();print(json.dumps(review(a.evidence)))
