"""Finite recursive mounted closure, exact two compiled peer definitions; no imports of apps."""
import ast,hashlib,json,re
from pathlib import Path,PurePosixPath
ROOT=Path(__file__).resolve().parents[1]
SCHEMA_SHA='88e887321483a57093cc309ecd783aed7368b125c566a8e595a75fb8a92fc893'
def require(ok,label):
 if not ok:raise ValueError(label)
def resolve_mount(path,root,source):
 name=PurePosixPath(path);require(name.is_absolute()and'..'not in name.parts,'AbsoluteSafeRuntimePath')
 mounts={'/native-root':root,'/native-inputs':root/'inputs','/native-tools':root/'controller','/native-source':source,'/pg-bootstrap':root/'inputs/pg-init'}
 for prefix,host in sorted(mounts.items(),key=lambda r:-len(r[0])):
  base=PurePosixPath(prefix)
  if name==base or base in name.parents:return host.joinpath(*name.relative_to(base).parts)
 raise ValueError('UndeclaredRuntimeMount:'+path)
def validate(root=ROOT,source=None):
 root=Path(root);source=Path(source)if source is not None else Path(json.loads((root/'inputs/runtime-file-declarations.json').read_text())['native_source_root'])
 queue=[('/native-root/controller/run.py','python'),('/native-tools/start.sh','shell'),('/native-tools/audit.py','python'),('/native-tools/validate_native.py','python'),('/native-tools/validate_physical.py','python'),('/native-tools/owned_resp_proxy.py','python'),('/native-tools/owned_pg_proxy.py','python'),('/native-inputs/native-store-tests','binary')];visited=set();files=set()
 while queue:
  runtime,kind=queue.pop()
  if(runtime,kind)in visited:continue
  visited.add((runtime,kind));p=resolve_mount(runtime,root,source);require(p.is_file()and not p.is_symlink(),'MountedRuntimeFileMissing:'+runtime);files.add(runtime)
  if kind=='shell':
   for path in re.findall(r'/(?:pg-bootstrap|native-source)/[A-Za-z0-9_./-]+',p.read_text()):queue.append((path,'shell'if path.endswith('.sh')else'sql'))
  elif kind=='python':
   tree=ast.parse(p.read_text())
   for node in ast.walk(tree):
    if isinstance(node,ast.ImportFrom)and node.module:
     local=root/'controller'/(node.module+'.py')
     if local.is_file():queue.append(('/native-tools/'+local.name,'python'))
    if isinstance(node,ast.Call)and isinstance(node.func,ast.Name)and node.func.id=='Path'and node.args and isinstance(node.args[0],ast.Constant)and type(node.args[0].value)is str:
     path=node.args[0].value
     if path.startswith(('/native-root/','/native-inputs/','/native-tools/','/native-source/','/pg-bootstrap/')):queue.append((path,'file'))
 declarations={'display_timezone_physical_tests.rs':{'controller/owned_resp_proxy.py','controller/owned_pg_proxy.py','inputs/resp-contracts.json','inputs/pg-contracts.json'},'notifications_callback_sql_tests.rs':{'inputs/Source12-projection.json','inputs/Source-binding.json','inputs/plan.json'}}
 for name,required in declarations.items():
  text=(source/'rust/crates/store/src'/name).read_text()
  require(all('"'+v+'"'in text for v in required),'CompiledPeerLiteralJoins:'+name)
  actual=set(re.findall(r'root\.join\("([A-Za-z0-9_./-]+)"\)',text));require(actual<=(required|({'inputs'}if name=='notifications_callback_sql_tests.rs'else set())),'NoUndeclaredCompiledRootJoin:'+name)
  for rel in required:runtime='/native-root/'+rel;require(resolve_mount(runtime,root,source).is_file(),'CompiledRuntimeInputMissing:'+runtime);files.add(runtime)
 plan=json.loads((root/'inputs/plan.json').read_text());require(len(plan['cases'])==12,'LiteralTwelveBodyDependencies')
 for case in plan['cases']:
  relative=PurePosixPath(case['body_path']);require(not relative.is_absolute()and'..'not in relative.parts,'SafeSealedBodyPath')
  path=root/'inputs'/relative;require(path.is_file()and len(path.read_bytes())==case['body_bytes']and hashlib.sha256(path.read_bytes()).hexdigest()==case['body_sha256'],'ExactDynamicBodyInput:'+str(relative));files.add('/native-inputs/'+str(relative))
 callback=(source/'rust/crates/store/src/display_timezone.rs').read_text();match=re.search(r'const SYSTEM_SQL: &str = "([^"]+)";',callback);require(match is not None,'ActualCompiledFusedSystemSQL')
 contracts=json.loads((root/'inputs/pg-contracts.json').read_text());require(set(contracts)=={'system_ready','system_error_ready'}and all(c['sql']==match.group(1)for c in contracts.values()),'ExactCompiledPGContractsBothStages');require('octet_length(value::text)<=65536'in match.group(1)and 'CASE WHEN'in match.group(1),'FusedBoundedMetadataPayloadSameStatement')
 schema=resolve_mount('/native-inputs/schema62-authority.json',root,source);require(hashlib.sha256(schema.read_bytes()).hexdigest()==SCHEMA_SHA,'ExactPinnedSchema62Bytes')
 v=json.loads(schema.read_text());require([len(v[k])for k in('tables','sequences','columns','constraints','indexes')]==[62,59,501,198,255],'Schema6259Complete')
 # Dynamic pure authority imports are explicitly included in the sealed tree.
 authority=root/'inputs/source-authority';require((authority/'verify.py').is_file(),'ExactSourceAuthorityPackage')
 for p in authority.rglob('*'):
  if p.is_file()and'__pycache__'not in p.parts:require(not p.is_symlink(),'NoAuthoritySymlinks');files.add('/native-root/'+p.relative_to(root).as_posix())
 return {'passed':True,'recursive_mounted_files':sorted(files),'count':len(files),'compiled_peer_dependencies':{k:sorted(v)for k,v in declarations.items()},'Source_or_Native_app_runs':0,'externals':'pinned image Python/psql/PostgreSQL17/Redis; no Django setup/application import'}
