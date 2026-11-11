"""Exact bounded-scanner759 reconstruction from immutable committed Git objects.
Original Dispatcharr/dependency authors retain code, algorithm and license credit.
No mutable/private-source fallback, Cargo, Docker or network access.
"""
import argparse,hashlib,io,json,subprocess,tarfile
from pathlib import Path,PurePosixPath

def digest(data):return hashlib.sha256(data).hexdigest()
def safe(name):
 p=PurePosixPath(name)
 if p.is_absolute()or'..'in p.parts or'\\'in name:raise ValueError('Unsafe member')
 return p
def freeze(repo,output,manifest=None):
 repo=Path(repo).resolve();output=Path(output);manifest=Path(manifest or Path(__file__).with_name('source-overlays.json'));r=json.loads(manifest.read_text());base=r['base']
 if len(base)!=40 or any(c not in'0123456789abcdef'for c in base):raise ValueError('Invalid immutable base')
 output.mkdir(parents=True,exist_ok=False)
 raw=subprocess.check_output(['git','-C',str(repo),'archive',r['base'],'rust'])
 with tarfile.open(fileobj=io.BytesIO(raw))as archive:
  for member in archive.getmembers():
   name=safe(member.name)
   if name.parts[0]!='rust'or not(member.isfile()or member.isdir()):raise ValueError('Unsafe base archive')
  archive.extractall(output,filter='data')
 actual={str(p.relative_to(output)):digest(p.read_bytes())for p in(output/'rust').rglob('*')if p.is_file()}
 if actual!=r['base_file_map']or len(actual)!=748 or len(r['overlays'])!=18:raise ValueError('Unexpected frozen base/selection')
 for name,item in r['overlays'].items():
  safe(name)
  if not(name.startswith('rust/crates/job-transport/src/')or name.startswith('rust/crates/store/src/')):raise ValueError('Unowned overlay')
  if actual.get(name)!=item['before_sha256']:raise ValueError('Overlap mismatch')
  blob=item['git_blob']
  if len(blob)!=40 or any(c not in'0123456789abcdef'for c in blob):raise ValueError('Invalid blob')
  data=subprocess.check_output(['git','-C',str(repo),'cat-file','blob',blob])
  if len(data)>2*1024*1024 or digest(data)!=item['sha256']:raise ValueError('Changed overlay')
  target=output/name;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(data)
 actual={str(p.relative_to(output)):digest(p.read_bytes())for p in(output/'rust').rglob('*')if p.is_file()}
 if actual!=r['expected_full_file_map']or len(actual)!=759:raise ValueError('Changed complete759 map')
 record={'files':actual,'base':r['base'],'manifest_sha256':digest(manifest.read_bytes()),'source':'Immutable Git history only'};(output/'source-inputs.json').write_text(json.dumps(record,indent=2)+'\n');return record
if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--repo',type=Path,default=Path.cwd());p.add_argument('--output',type=Path,required=True);p.add_argument('--manifest',type=Path);a=p.parse_args();r=freeze(a.repo,a.output,a.manifest);print(json.dumps({'passed':True,'files':len(r['files'])}))
