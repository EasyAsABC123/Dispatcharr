#!/usr/bin/env python3
"""Freeze a committed tree, compile six functional peers and bind every input.
Original Dispatcharr authors retain application/algorithm credit and licenses.
No Docker, resource measurements, Git mutation or mutable Rust tree capture.
"""
import argparse,hashlib,io,json,os,re,shutil,subprocess,tarfile
from pathlib import Path
ALLOWED_OVERLAYS={'rust/crates/input-store/src/lib.rs','rust/crates/input-store/src/retained.rs','rust/crates/stream-core/src/lib.rs','rust/crates/stream-runtime/src/lib.rs','rust/crates/stream-http/src/engine.rs','rust/crates/stream-http/src/input_writer.rs','rust/crates/stream-http/src/history.rs','rust/crates/stream-http/src/core_trace_peer.rs'}
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def digest(v):return hashlib.sha256(json.dumps(v,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()
def invoke(args,**kw):return subprocess.check_output(args,**kw)
def input_map(source):return {str(p.relative_to(source)):sha(p)for p in sorted((source/'rust').rglob('*'))if p.is_file()}
def extract(raw,dest):
 with tarfile.open(fileobj=io.BytesIO(raw))as tar:
  for m in tar.getmembers():
   p=Path(m.name)
   if p.is_absolute()or '..'in p.parts or not(m.isfile()or m.isdir()):raise ValueError('Only regular immutable source members allowed')
  tar.extractall(dest,filter='data')
def freeze(repo,revision,output,overlay=None,overlay_sha=None):
 repo=Path(repo).resolve();output=Path(output).resolve();output.mkdir(parents=True,exist_ok=True)
 if any(output.iterdir()):raise ValueError('Fresh empty output required')
 commit=invoke(['git','rev-parse','--verify','--end-of-options',revision+'^{commit}'],cwd=repo,text=True).strip()
 if not re.fullmatch('[0-9a-f]{40}',commit):raise ValueError('Immutable commit required')
 source=output/'source';source.mkdir();raw=invoke(['git','archive',commit,'rust'],cwd=repo);extract(raw,source);overlays={}
 if overlay is not None:
  if overlay_sha is None or sha(overlay)!=overlay_sha:raise ValueError('Explicit reviewed overlay manifest hash required')
  for name,row in json.loads(Path(overlay).read_text())['files'].items():
   if name not in ALLOWED_OVERLAYS:raise ValueError('Only scoped core Rust overlays accepted')
   p=Path(row['source'])
   if p.is_symlink():raise ValueError('Frozen overlay changed')
   data=p.read_bytes()
   if hashlib.sha256(data).hexdigest()!=row['sha256']:raise ValueError('Frozen overlay changed')
   target=source/name;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(data);overlays[name]=row['sha256']
 if not(source/'rust/crates/stream-http/src/core_trace_peer.rs').is_file():raise ValueError('Revision lacks registered core trace peer; select qualified commit or explicitly reviewed core overlays')
 external={}
 for p in(source/'rust').rglob('*.rs'):
  for m in re.finditer(r'include_(?:str|bytes)!\(\s*"([^"]+)"',p.read_text()):
   target=(p.parent/m[1]).resolve()
   if target.is_relative_to(source/'rust'):continue
   if not target.is_relative_to(source):raise ValueError('External include escapes frozen source')
   name=str(target.relative_to(source));data=invoke(['git','show',commit+':'+name],cwd=repo);target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(data);external[name]=sha(target)
 record={'revision':commit,'archive_sha256':hashlib.sha256(raw).hexdigest(),'reviewed_overlays':overlays,'overlay_manifest_sha256':overlay_sha,'external_compile_fixtures':external}
 return source,record

def build(repo,revision,output,target,overlay=None,overlay_sha=None):
 output=Path(output).resolve();source,record=freeze(repo,revision,output,overlay,overlay_sha);target=Path(target).resolve()
 if target.is_relative_to(source):raise ValueError('Target must be outside frozen source')
 before=input_map(source);(output/'source-map.json').write_text(json.dumps(before,indent=2)+'\n')
 env=dict(os.environ,CARGO_TARGET_DIR=str(target));artifacts={};logs={}
 commands=[['-p','dispatcharr-stream-http','--lib','--test','http'],['-p','dispatcharr-input-store','--tests']]
 for i,selection in enumerate(commands,1):
  command=['cargo','test','--manifest-path',str(source/'rust/Cargo.toml'),'--locked','--offline',*selection,'--no-run','--message-format=json']
  log=output/f'cargo-{i}.jsonl';err=output/f'cargo-{i}.log'
  with log.open('w')as stdout,err.open('w')as stderr:subprocess.run(command,env=env,stdout=stdout,stderr=stderr,check=True)
  logs[log.name]=sha(log);logs[err.name]=sha(err)
  for line in log.read_text().splitlines():
   try:v=json.loads(line)
   except ValueError:continue
   if v.get('executable'):artifacts[v['target']['name']]=v
 expected={'dispatcharr_stream_http':'native-core-peer','http':'http-peer','gc':'input-gc-peer','redis':'input-redis-peer','retained':'input-retained-peer','source_expiry':'input-source-expiry-peer'}
 if not set(expected)<=artifacts.keys():raise ValueError('Six actual Cargo executable artifacts required')
 peers=output/'peers';peers.mkdir();binaries={};profiles={}
 for name,filename in expected.items():
  entry=artifacts[name];dest=peers/filename;shutil.copy2(entry['executable'],dest);dest.chmod(0o755);binaries[filename]=sha(dest);profiles[filename]=entry['profile']
 if input_map(source)!=before:raise ValueError('Frozen source changed during compilation')
 record.update(passed=True,source_map_canonical_sha256=digest(before),source_map_file_sha256=sha(output/'source-map.json'),source_unchanged_after_build=True,binaries=binaries,profiles=profiles,compiler_artifact_records=logs,compile_root=str(source),target=str(target),rustc=invoke(['rustc','--version','--verbose'],text=True),cargo=invoke(['cargo','--version'],text=True).strip())
 (output/'build-record.json').write_text(json.dumps(record,indent=2)+'\n');return record
if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--repo',type=Path,default=Path.cwd());p.add_argument('--revision',required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--target',type=Path,required=True);p.add_argument('--overlay-manifest',type=Path);p.add_argument('--overlay-manifest-sha256');a=p.parse_args();v=build(a.repo,a.revision,a.output,a.target,a.overlay_manifest,a.overlay_manifest_sha256);print(json.dumps({'passed':v['passed'],'source_map_canonical_sha256':v['source_map_canonical_sha256'],'build_record':str(a.output/'build-record.json')}))
