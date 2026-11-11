"""Reconstruct exact qualified regular-library inputs from immutable Git blobs.
Original Dispatcharr and dependency authors retain code/algorithm/license credit.
"""
import argparse,hashlib,io,json,subprocess,tarfile
from pathlib import Path,PurePosixPath

def digest(data):return hashlib.sha256(data).hexdigest()
def owned(name):
 path=PurePosixPath(name)
 if path.is_absolute()or'..'in path.parts or '\\'in name:raise ValueError('Unsafe overlay path')
 if name!='rust/Cargo.lock'and not name.startswith('rust/crates/job-transport/'):raise ValueError('Unowned overlay')
 return path

def freeze(repo,output,manifest=None):
 repo=Path(repo).resolve();output=Path(output);record=json.loads(Path(manifest or Path(__file__).with_name('source-overlays.json')).read_text())
 output.mkdir(parents=True,exist_ok=False)
 raw=subprocess.check_output(['git','-C',str(repo),'archive',record['base'],'rust'])
 if digest(raw)!=record['base_rust_archive_sha256']:raise ValueError('Changed base archive')
 with tarfile.open(fileobj=io.BytesIO(raw))as tar:
  for item in tar.getmembers():
   path=PurePosixPath(item.name)
   if path.is_absolute()or'..'in path.parts or not(item.isfile()or item.isdir()):raise ValueError('Unsafe base member')
  tar.extractall(output,filter='data')
 if len(record['overlays'])!=20:raise ValueError('Overlay count')
 for name,expected in record['overlays'].items():
  path=owned(name);blob=expected['git_blob']
  if len(blob)!=40 or any(c not in '0123456789abcdef'for c in blob):raise ValueError('Invalid blob identity')
  data=subprocess.check_output(['git','-C',str(repo),'cat-file','blob',blob])
  if len(data)>2*1024*1024 or digest(data)!=expected['sha256']:raise ValueError('Changed overlay')
  target=output/path;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(data)
 files={str(p.relative_to(output)):digest(p.read_bytes())for p in (output/'rust').rglob('*')if p.is_file()}
 if files!=record['expected_full_file_map']or len(files)!=713:raise ValueError('Changed full source map')
 result={'base':record['base'],'files':files,'overlay_manifest_sha256':digest(Path(manifest or Path(__file__).with_name('source-overlays.json')).read_bytes()),'source':'immutable committed Git history; no mutable Rust/private artifact fallback'}
 (output/'source-inputs.json').write_text(json.dumps(result,indent=2)+'\n');return result
if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--repo',type=Path,default=Path.cwd());p.add_argument('--output',type=Path,required=True);p.add_argument('--manifest',type=Path);a=p.parse_args();r=freeze(a.repo,a.output,a.manifest);print(json.dumps({'passed':True,'files':len(r['files'])}))
