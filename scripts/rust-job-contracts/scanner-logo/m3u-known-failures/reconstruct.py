#!/usr/bin/env python3
"""Reconstruct exact historical793Rust+5coupled source from committedGit+10overlays.
Never checks out/changes refs/index; no private artifact/source fallback.
"""
import argparse,hashlib,json,subprocess,tarfile,tempfile
from pathlib import Path
HERE=Path(__file__).resolve().parent
BASE='044f3ad0fb3ee68ffb17b23bbb796ccdae954298'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def validate_tree(root,expected):
 actual={str(p.relative_to(root)):sha(p)for p in root.rglob('*')if p.is_file()}
 if actual!=expected:raise ValueError('ExactHistorical798SourceRequired')
def reconstruct(repo,out):
 expected=json.loads((HERE/'source-inputs.json').read_text());selection=json.loads((HERE/'runtime-selection.json').read_text())
 if out.exists():raise ValueError('FreshOutputRequired')
 for row in selection['files']:
  if sha(HERE/'historical'/row['path'])!=row['sha256']:raise ValueError('ChangedHistoricalOverlay:'+row['path'])
 base=dict(expected)
 for row in selection['files']:
  if row['preimage_sha256']is None:base.pop(row['path'])
  else:base[row['path']]=row['preimage_sha256']
 out.mkdir(parents=True,exist_ok=False)
 with tempfile.TemporaryFile()as stream:
  argv=['git','-C',str(repo),'archive','--format=tar',BASE,'--','rust',*[name for name in expected if not name.startswith('rust/')]]
  value=subprocess.run(argv,stdout=stream,stderr=subprocess.PIPE);value.check_returncode();stream.seek(0)
  with tarfile.open(fileobj=stream)as archive:archive.extractall(out,filter='data')
 validate_tree(out,base)
 for row in selection['files']:
  destination=out/row['path'];destination.parent.mkdir(parents=True,exist_ok=True);destination.write_bytes((HERE/'historical'/row['path']).read_bytes())
 validate_tree(out,expected)
 return {'passed':True,'scope':'Exacthistoricalsource reconstruction only; no build/app/Sourceexecution','base_git':BASE,'inputs':len(expected),'Rust_inputs':sum(name.startswith('rust/')for name in expected),'source_map_sha256':sha(HERE/'source-inputs.json'),'output':str(out),'git_argv':argv}
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--repo',type=Path,required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--record',type=Path,required=True);a=p.parse_args();result=reconstruct(a.repo.resolve(),a.output.resolve());a.record.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
if __name__=='__main__':main()
