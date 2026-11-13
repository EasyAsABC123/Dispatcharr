#!/usr/bin/env python3
"""Read-only draft closure/AST verifier; never imports Source/Django/fixture tools."""
import argparse,ast,hashlib,json
from pathlib import Path
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--support-root',type=Path,required=True);a=p.parse_args();root=Path(__file__).resolve().parent
 selected=json.loads((root/'selected-files.json').read_text());seal=json.loads((root/'seal.json').read_text())
 if sha(root/'selected-files.json')!=seal['selection_sha256']:raise ValueError('ChangedSelection')
 for name,digest in selected.items():
  if sha(root/name)!=digest:raise ValueError('ChangedPreparedInput:'+name)
  if name.endswith('.py'):ast.parse((root/name).read_text(),filename=name)
 for name,digest in json.loads((root/'tools/support-inputs.json').read_text()).items():
  if sha(a.support_root/name)!=digest:raise ValueError('ChangedPinnedSupport:'+name)
 source=json.loads((root/'pinned-source-proof.json').read_text())
 for name,record in source['modules'].items():
  if sha(root/'original'/name)!=record['sha256']:raise ValueError('ChangedPinnedOriginal:'+name)
 print(json.dumps({'passed':True,'scope':'Closure/AST/source identity only; no actual Source capture','selected_files':len(selected),'pinned_Original_modules':len(source['modules'])}))
if __name__=='__main__':main()
