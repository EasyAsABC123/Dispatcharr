"""Reconstruct the current 850-input QA source from immutable Git and seven files."""
import argparse,hashlib,json,subprocess
from pathlib import Path
HERE=Path(__file__).resolve().parent
def need(v,label):
 if not v:raise ValueError(label)
def sha(b):return hashlib.sha256(b).hexdigest()
def reconstruct(repo,out):
 need(sha((HERE/'current-union/source-inputs.json').read_bytes())=='42412e7845659bc793bf6d3f1b5d6f3c910b7dc513384b10ec7d437582f9adad','PinnedSourceMap');need(sha((HERE/'current-union/selected-hunks.json').read_bytes())=='7fbe6e2e97c462bf08b2bfea348698264ce3a2b2e96fa1e235543949a6d88d47','PinnedOverlayManifest');manifest=json.loads((HERE/'current-union/source-inputs.json').read_text());sel=json.loads((HERE/'current-union/selected-hunks.json').read_text());revision=sel['base_revision'];need(len(revision)==40 and len(manifest)==850,'ImmutableRevisionAndFullMap')
 need(not out.exists(),'FreshOutputOnly');overlays={r['path']:r for r in sel['files']};need(len(overlays)==7,'ExactlySevenOverlays');payloads={}
 for path,expected in manifest.items():
  need(not Path(path).is_absolute()and '..'not in Path(path).parts,'SafeRelativePath')
  if path in overlays:
   r=overlays[path]
   if r['preimage_sha256']is not None:
    old=subprocess.run(['git','-C',str(repo),'show',revision+':'+path],capture_output=True,check=True).stdout;need(sha(old)==r['preimage_sha256'],'ExactOverlayPreimage')
   else:need(subprocess.run(['git','-C',str(repo),'cat-file','-e',revision+':'+path],capture_output=True).returncode!=0,'NewPathAbsentAtBase')
   body=(HERE/r['source_path']).read_bytes();need(sha(body)==r['sha256'],'ExactOverlayBytes')
  else:body=subprocess.run(['git','-C',str(repo),'show',revision+':'+path],capture_output=True,check=True).stdout
  need(sha(body)==expected,'ExactFullInputMap:'+path);payloads[path]=body
 out.mkdir(parents=True)
 for path,body in payloads.items():p=out/path;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(body)
 return {'inputs':len(payloads),'Rust_inputs':sum(p.startswith('rust/')for p in payloads),'base_revision':revision,'source_map_sha256':sha((HERE/'current-union/source-inputs.json').read_bytes()),'historical_execution_transferred':False}
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--repo',required=True,type=Path);p.add_argument('--output',required=True,type=Path);a=p.parse_args();print(json.dumps(reconstruct(a.repo,a.output),indent=2))
