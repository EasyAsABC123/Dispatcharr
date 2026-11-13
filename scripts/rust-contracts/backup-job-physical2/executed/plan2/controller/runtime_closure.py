"""Read-only recursive mounted runtime-file closure; never imports application code."""
import ast,hashlib,json,re
from pathlib import Path,PurePosixPath
ROOT=Path(__file__).resolve().parents[1]
SCHEMA_SHA='88e887321483a57093cc309ecd783aed7368b125c566a8e595a75fb8a92fc893'

def require(ok,label):
 if not ok:raise ValueError(label)

def resolve_mount(path,root,source):
 name=PurePosixPath(path);require(name.is_absolute()and'..'not in name.parts,'AbsoluteSafeRuntimePath')
 mounts={'/native-root':root,'/native-inputs':root/'inputs','/native-tools':root/'controller','/native-source':source,'/pg-bootstrap':root/'inputs/pg-init'}
 for prefix,host in sorted(mounts.items(),key=lambda row:-len(row[0])):
  base=PurePosixPath(prefix)
  if name==base or base in name.parents:return host.joinpath(*name.relative_to(base).parts)
 raise ValueError('UndeclaredRuntimeMount:'+path)

def validate(root=ROOT,source=None):
 root=Path(root)
 declarations=json.loads((root/'inputs/runtime-file-declarations.json').read_text())
 source=Path(source)if source is not None else Path(declarations['native_source_root'])
 require(source.is_dir(),'DeclaredSourceMountExists')
 files=set();queue=[('/native-root/controller/run.py','python'),('/native-tools/start.sh','shell'),('/native-tools/audit.py','python'),('/native-root/controller/owned_resp_proxy.py','python'),('/native-inputs/native-job-tests','binary'),('/native-root/inputs/resp-contracts.json','json')]
 visited=set()
 while queue:
  runtime,kind=queue.pop();key=(runtime,kind)
  if key in visited:continue
  visited.add(key);local=resolve_mount(runtime,root,source);require(local.is_file()and not local.is_symlink(),'MountedRuntimeFileMissing:'+runtime);files.add(runtime)
  if kind=='shell':
   text=local.read_text()
   for path in re.findall(r'/(?:pg-bootstrap|native-source)/[A-Za-z0-9_./-]+',text):queue.append((path,'shell'if path.endswith('.sh')else'sql'))
  elif kind=='python':
   text=local.read_text();tree=ast.parse(text)
   # Local imports recurse rather than assuming the top-level file is sufficient.
   for node in ast.walk(tree):
    if isinstance(node,ast.ImportFrom)and node.module:
     candidate=root/'controller'/(node.module+'.py')
     if candidate.is_file()or node.module in {'public_manifest','bounded_process','bounded_stop','validate_physical','runtime_closure'}:queue.append(('/native-tools/'+candidate.name,'python'))
    if isinstance(node,ast.Call)and isinstance(node.func,ast.Name)and node.func.id=='Path'and node.args and isinstance(node.args[0],ast.Constant)and type(node.args[0].value)is str:
     path=node.args[0].value
     if path.startswith(('/native-root/','/native-inputs/','/native-tools/','/native-source/','/pg-bootstrap/')):queue.append((path,'json'if path.endswith('.json')else'file'))
 # The actual compiled Rust peer uses env-root joins, which cannot be inferred from Python alone.
 rust=source/'rust/crates/job-transport/src/backup_job_physical_tests.rs';text=rust.read_text();joins=set(re.findall(r'root\.join\("([A-Za-z0-9_./-]+)"\)',text))
 require(joins=={'controller/owned_resp_proxy.py','inputs/resp-contracts.json'},'ExactCompiledPeerRuntimeRootJoins')
 for relative in joins:
  runtime='/native-root/'+relative;require(resolve_mount(runtime,root,source).is_file(),'CompiledPeerJoinedInputMissing');files.add(runtime)
 schema=resolve_mount('/native-inputs/schema62-authority.json',root,source);require(schema.is_file()and hashlib.sha256(schema.read_bytes()).hexdigest()==SCHEMA_SHA,'ExactPinnedSchema62Bytes')
 value=json.loads(schema.read_text());require(len(value['tables'])==62 and len(value['sequences'])==59 and len(value['columns'])==501 and len(value['constraints'])==198 and len(value['indexes'])==255,'ExactSchema6259Manifest')
 return {'passed':True,'recursive_mounted_files':sorted(files),'mounted_file_count':len(files),'schema62_sha256':SCHEMA_SHA,'compiled_peer_root_joins':sorted(joins),'image_only_externals_declared_not_host_mounts':['/dispatcharrpy/bin/python','/usr/bin/psql','/usr/lib/postgresql/17/bin/pg_dump','/usr/lib/postgresql/17/bin/pg_restore','/usr/lib/postgresql/17/bin/pg_ctl','/usr/lib/postgresql/17/bin/initdb'],'Source_or_Native_app_runs':0}
