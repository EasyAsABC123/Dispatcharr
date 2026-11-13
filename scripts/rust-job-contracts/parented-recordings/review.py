#!/usr/bin/env python3
"""Recompute bounded actual component comparisons without Docker/apps."""
import argparse,json,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT/'tools'))
from pair_contract import validate

def review(path):
 path=Path(path);actual=json.loads((path/'source-parented-recordings-private.json').read_text());host=json.loads((path/'integration.json').read_text());reference=json.loads((ROOT/'tools/reference.json').read_text());result=validate(0,actual,reference)
 if host['actual_host_exit']!=0 or host['actual_controller_exit']!=0 or host['actual_controller_settled']is not True or host['passed']is not True:raise ValueError('Actual successful host and guest settlement required')
 if (path/'default-before-private.json').read_bytes()!=(path/'default-after-private.json').read_bytes():raise ValueError('Default logical manifest bytes differ')
 return {'passed':True,'cases':result['cases'],'tasks':result['tasks'],'Native_tasks':result['Native_tasks'],'Original_tasks':result['Original_tasks'],'capture_checks':result['capture_checks'],'retained_SQL_clock_fields':len(result['comparison']['temporal_observations']),'default_manifest_bytes_equal':True,'new_app_execution':False,'scope':result['scope']}
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--evidence',type=Path,required=True);a=p.parse_args();print(json.dumps(review(a.evidence)))
