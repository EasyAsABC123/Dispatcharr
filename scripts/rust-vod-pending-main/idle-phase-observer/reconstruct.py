"""Reconstruct exact qualified767 from immutable Git208 +sealed three-path patch.
No live/private fallback, network, Git mutation or Cargo execution. Original
Dispatcharr and dependency authors retain their application/algorithm credit.
"""
import argparse,hashlib,io,json,os,re,subprocess,tarfile
from pathlib import Path
HERE=Path(__file__).resolve().parent
MANIFEST_SHA256='9da5e0a3b59f7275e64d935e981bf2a689693d134168f442929928bd222a5de0'
BASE='208598ee1872732cbe8a0d846efa5a40b59ae1d2'
def sha(raw):return hashlib.sha256(raw).hexdigest()
def manifest():
 raw=(HERE/'reconstruction-manifest.json').read_bytes()
 if sha(raw)!=MANIFEST_SHA256:raise ValueError('SealedManifestMismatch')
 m=json.loads(raw)
 if m['base_revision']!=BASE or m['expected_base_rust_count']!=766 or m['expected_final_rust_count']!=767 or len(m['selected_overlay'])!=3 or len(m['coupled_Golden_Git_inputs'])!=5:raise ValueError('Exact767Schema')
 return m

def base_expected(m):
 result=dict(m['final_rust_files'])
 if len(result)!=767:raise ValueError('Final767Required')
 for row in m['selected_overlay']:
  name=row['path']
  if result.get(name)!=row['sha256']:raise ValueError('OverlayFinalBinding')
  if row['base_sha256']is None:result.pop(name)
  else:result[name]=row['base_sha256']
 if len(result)!=766:raise ValueError('Base766Required')
 return result

def verify_files(files,expected,label):
 if set(files)!=set(expected):raise ValueError(label+'PathSet')
 for name,value in expected.items():
  if sha(files[name])!=value:raise ValueError(label+'Hash '+name)

def overlay(files,patch,rows):
 """Strict unified hunks: exactly the3sealed paths; old context/counts verified."""
 allowed={row['path']:row for row in rows};lines=patch.decode('utf-8').splitlines(True);i=0;seen=set();result=dict(files)
 while i<len(lines):
  if not lines[i].startswith('--- 208598ee/'):raise ValueError('PatchOldHeader')
  name=lines[i][len('--- 208598ee/'):].rstrip('\n');i+=1
  if name not in allowed or name in seen or i>=len(lines)or lines[i]!='+++ prototype2/'+name+'\n':raise ValueError('PatchPathBinding')
  seen.add(name);i+=1;old=result.get(name,b'').decode('utf-8').splitlines(True);out=[];cursor=0;hunks=0
  while i<len(lines)and not lines[i].startswith('--- 208598ee/'):
   match=re.fullmatch(r'@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@[^\n]*\n',lines[i])
   if match is None:raise ValueError('PatchHunkHeader')
   start=int(match[1]);oldcount=int(match[2]or 1);newstart=int(match[3]);newcount=int(match[4]or 1);i+=1;at=start-1 if oldcount else start;newat=newstart-1 if newcount else newstart
   if not cursor<=at<=len(old):raise ValueError('PatchOverlapOrRange')
   out+=old[cursor:at];cursor=at
   if len(out)!=newat:raise ValueError('PatchNewRange')
   used_old=used_new=0
   while used_old<oldcount or used_new<newcount:
    if i>=len(lines)or not lines[i]or lines[i][0]not in' +-':raise ValueError('PatchTruncated')
    kind=lines[i][0];body=lines[i][1:];i+=1
    if kind in' -':
     if cursor>=len(old)or old[cursor]!=body:raise ValueError('PatchOldContext')
     cursor+=1;used_old+=1
    if kind in' +':out.append(body);used_new+=1
    if used_old>oldcount or used_new>newcount:raise ValueError('PatchHunkCount')
   hunks+=1
  if not hunks:raise ValueError('PatchEmptyFile')
  out+=old[cursor:];raw=''.join(out).encode('utf-8')
  if sha(raw)!=allowed[name]['sha256']:raise ValueError('PatchSelectedHash '+name)
  result[name]=raw
 if seen!=set(allowed):raise ValueError('PatchExactlyThreePathsRequired')
 return result

def reconstruct(repo,output):
 repo=Path(repo).resolve(strict=True);output=Path(output).resolve()
 if output.exists()or output==repo or output==repo/'.git'or repo/'.git'in output.parents:raise ValueError('FreshNonGitOutputRequired')
 m=manifest();patch=(HERE/'selected-source.patch').read_bytes()
 if len(patch)>262144 or sha(patch)!=m['patch_sha256']:raise ValueError('SealedPatchMismatch')
 expected_base=base_expected(m);coupled=m['coupled_Golden_Git_inputs']
 env={k:v for k,v in os.environ.items()if not k.startswith('GIT_')}
 raw=subprocess.check_output(['git','-C',str(repo),'archive',BASE,'rust',*sorted(coupled)],env=env,timeout=30)
 if len(raw)>134217728:raise ValueError('ImmutableArchiveCapacity')
 files={};modes={}
 with tarfile.open(fileobj=io.BytesIO(raw))as archive:
  for member in archive:
   path=Path(member.name)
   if path.is_absolute()or'..'in path.parts or not(member.isfile()or member.isdir()):raise ValueError('ImmutableArchiveMember')
   if member.isdir():continue
   name=path.as_posix()
   if name in files or not(name.startswith('rust/')or name in coupled):raise ValueError('ImmutableArchivePath')
   files[name]=archive.extractfile(member).read();modes[name]=member.mode&0o777
 base_files={n:v for n,v in files.items()if n.startswith('rust/')};goldens={n:v for n,v in files.items()if n in coupled}
 verify_files(base_files,expected_base,'Base766');verify_files(goldens,coupled,'CoupledFive')
 final=overlay(base_files,patch,m['selected_overlay']);verify_files(final,m['final_rust_files'],'Final767')
 output.mkdir(parents=True,exist_ok=False)
 for name,data in{**final,**goldens}.items():
  target=output/name;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(data);target.chmod(modes.get(name,0o644))
 disk={str(f.relative_to(output)):f.read_bytes()for f in(output/'rust').rglob('*')if f.is_file()};verify_files(disk,m['final_rust_files'],'Written767')
 proof={'passed':True,'immutable_base_revision':BASE,'base_Rust_count':766,'final_Rust_count':767,'coupled_Golden_Git_inputs':coupled,'manifest_sha256':MANIFEST_SHA256,'selected_patch_sha256':sha(patch),'Git_archive_sha256':sha(raw),'private_or_mutable_fallback':False,'Git_worktree_index_refs_changed':False,'Cargo_or_fixture_executed':False}
 (output/'reconstruction.json').write_text(json.dumps(proof,indent=2)+'\n');return proof

def main():
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--repo',type=Path,required=True);parser.add_argument('--output',type=Path,required=True);args=parser.parse_args();print(json.dumps(reconstruct(args.repo,args.output)))
if __name__=='__main__':main()
