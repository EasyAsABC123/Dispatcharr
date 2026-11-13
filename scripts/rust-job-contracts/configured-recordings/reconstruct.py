#!/usr/bin/env python3
"""Exact historical configured-recording component reconstruction. No private fallback.
Original Dispatcharr and dependency authors retain full application/algorithm credit.
"""
import argparse,hashlib,json,subprocess,tarfile,tempfile
from pathlib import Path
HERE=Path(__file__).resolve().parent
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def validate_tree(root,expected):
 actual={str(p.relative_to(root)):sha(p)for p in root.rglob('*')if p.is_file()}
 if actual!=expected:raise ValueError('ExactHistoricalSourceRequired')
def reconstruct(repo,out,variant):
 manifest=json.loads((HERE/'historical-sources.json').read_text());entry=manifest['variants'][variant];map_path=HERE/entry['source_map']
 if sha(map_path)!=entry['source_map_sha256']:raise ValueError('BoundSourceMap')
 expected=json.loads(map_path.read_text());base=dict(expected)
 if out.exists():raise ValueError('FreshOutputRequired')
 for row in entry['overlays']:
  path=Path(row['path'])
  if path.is_absolute()or'..'in path.parts:raise ValueError('RelativeSourcePathRequired')
  if sha(HERE/'historical-blobs'/row['sha256'])!=row['sha256']:raise ValueError('ChangedHistoricalOverlay')
  if row['preimage_sha256']is None:base.pop(row['path'])
  else:base[row['path']]=row['preimage_sha256']
 out.mkdir(parents=True,exist_ok=False)
 with tempfile.TemporaryFile()as stream:
  argv=['git','-C',str(repo),'archive','--format=tar',manifest['base_git'],'--','rust',*[name for name in expected if not name.startswith('rust/')]]
  command=subprocess.run(argv,stdout=stream,stderr=subprocess.PIPE);command.check_returncode();stream.seek(0)
  with tarfile.open(fileobj=stream)as archive:archive.extractall(out,filter='data')
 validate_tree(out,base)
 for row in entry['overlays']:
  destination=out/row['path'];destination.parent.mkdir(parents=True,exist_ok=True);destination.write_bytes((HERE/'historical-blobs'/row['sha256']).read_bytes())
 validate_tree(out,expected)
 return {'passed':True,'variant':variant,'base_git':manifest['base_git'],'source_map_sha256':entry['source_map_sha256'],'inputs':len(expected),'Rust_inputs':entry['Rust_inputs'],'scope':'Exact historical source reconstruction only; no build/fixture/Main/default graph execution','git_argv':argv,'output':str(out)}
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--repo',type=Path,required=True);p.add_argument('--variant',choices=['qa9','source10'],required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--record',type=Path,required=True);a=p.parse_args();result=reconstruct(a.repo.resolve(),a.output.resolve(),a.variant);a.record.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
if __name__=='__main__':main()
