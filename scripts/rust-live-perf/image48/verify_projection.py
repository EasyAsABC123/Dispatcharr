#!/usr/bin/env python3
"""Recompute public live-pair numbers without Docker/private files."""
import argparse,importlib.util,json
from pathlib import Path
spec=importlib.util.spec_from_file_location('_live_public_numeric',Path(__file__).with_name('verify.py'));v=importlib.util.module_from_spec(spec);spec.loader.exec_module(v)
def verify(path):
 data=json.loads(Path(path).read_text());actual=v.verify(data['report'],data['protocol']);v.require(actual==data['verified'],'public summary');return actual
if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--projection',type=Path,required=True);a=p.parse_args();print(json.dumps(verify(a.projection)))
