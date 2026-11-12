"""Private committed SDK input capture; canonical Git metadata is read-only."""
import hashlib,io,json,os,shlex,subprocess,tarfile
from pathlib import Path,PurePosixPath
ROOT=Path.cwd();OUT=ROOT/'artifacts/rust/sdk-044f-qualified-1';REF='044f3ad0fb3ee68ffb17b23bbb796ccdae954298';SOURCE=OUT/'source';META=OUT/'git-metadata';SOURCE.mkdir(exist_ok=False)
def sha(b):return hashlib.sha256(b).hexdigest()
raw=subprocess.check_output(['git','-c','tar.umask=0002','-c','core.attributesFile=/dev/null','archive','--format=tar',REF],cwd=ROOT);(OUT/'committed-source.tar').write_bytes(raw)
with tarfile.open(fileobj=io.BytesIO(raw))as archive:
 for m in archive.getmembers():
  p=PurePosixPath(m.name)
  if p.is_absolute()or'..'in p.parts or not(m.isfile()or m.isdir()):raise ValueError('Unsafe/nonregular committed archive member')
 archive.extractall(SOURCE,filter='data')
env=dict(os.environ,GIT_CONFIG_GLOBAL='/dev/null',GIT_CONFIG_NOSYSTEM='1');subprocess.run(['git','init','--bare','--template=',str(META)],env=env,check=True,capture_output=True)
(META/'HEAD').write_text(REF+'\n');alternate=(ROOT/'.git/objects').resolve();(META/'objects/info/alternates').write_text(str(alternate)+'\n');(META/'objects/info/alternates').chmod(0o444)
subprocess.run(['git','config','--file',str(META/'config'),'core.bare','false'],env=env,check=True);subprocess.run(['git','config','--file',str(META/'config'),'gc.auto','0'],env=env,check=True)
private={'GIT_DIR':str(META),'GIT_WORK_TREE':str(SOURCE),'GIT_INDEX_FILE':str(META/'index'),'GIT_OBJECT_DIRECTORY':str(META/'objects'),'GIT_CONFIG_GLOBAL':'/dev/null','GIT_CONFIG_NOSYSTEM':'1','GIT_OPTIONAL_LOCKS':'0'};env.update(private);subprocess.run(['git','read-tree',REF],cwd=SOURCE,env=env,check=True)
def git(*args):return subprocess.check_output(['git',*args],cwd=SOURCE,env=env,text=True).strip()
assert git('rev-parse','HEAD')==REF;assert not git('status','--short');tracked=git('ls-files','--cached','--others','--exclude-standard').splitlines();actual={str(p.relative_to(SOURCE)):sha(p.read_bytes())for p in SOURCE.rglob('*')if p.is_file()};assert set(actual)==set(tracked)
# Independently compare full file contents and executable modes to the archive,
# not live HEAD or Source WIP.
with tarfile.open(fileobj=io.BytesIO(raw))as archive:
 expected={m.name:sha(archive.extractfile(m).read())for m in archive.getmembers()if m.isfile()}
assert actual==expected
rust={k:v for k,v in actual.items()if k.startswith('rust/')};parent=json.loads((ROOT/'artifacts/rust/m5-notification-mutations-native-qa-2/fixture/inputs/rust792-map.json').read_text());assert len(rust)==792 and rust==parent
coupled=json.loads((ROOT/'artifacts/rust/m5-notification-mutations-native-qa-2/fixture/inputs/coupled5-map.json').read_text());assert all(actual[k]==v for k,v in coupled.items())
full={**rust,**{k:actual[k]for k in coupled}};assert full==json.loads((ROOT/'artifacts/rust/m5-notification-mutations-native-qa-2/fixture/inputs/full797-map.json').read_text())
selected={p:actual[p]for p in tracked if p.startswith(('rust/','scripts/rust-','scripts/verify-rust-','docker/compose.rust','docs/rust-','apps/proxy/live_proxy/input/ts_discontinuity.py'))}
copy={}
for line in(SOURCE/'rust/Dockerfile').read_text().splitlines():
 if not line.startswith('COPY '):continue
 tokens=shlex.split(line);assert len(tokens)==3,'Explicit COPY authority only';p=SOURCE/tokens[1]
 if p.is_file():copy[str(p.relative_to(SOURCE))]=actual[str(p.relative_to(SOURCE))]
 elif p.is_dir():copy.update({k:v for k,v in actual.items()if k.startswith(str(p.relative_to(SOURCE))+'/')})
 else:raise ValueError('Missing committed SDK COPY input '+str(p))
for name in ['rust/Dockerfile','rust/Dockerfile.dockerignore']:copy[name]=actual[name]
host_only={name:actual[name]for name in ['docker/compose.rust.yml','scripts/verify-rust-compose.py']}
for name,data in [('all-source-inputs.json',actual),('rust-inputs792.json',rust),('verifier-selected-inputs.json',selected),('SDK-copy-closure.json',copy),('host-only-controls.json',host_only),('coupled5-inputs.json',coupled),('full797-inputs.json',full)]: (OUT/name).write_text(json.dumps(data,indent=2)+'\n')
(OUT/'private-git-environment.json').write_text(json.dumps(private,indent=2)+'\n');(OUT/'evidence').mkdir();project='dispatcharr-sdk-044f-'+str(os.getpid());command=['python3',str(SOURCE/'scripts/verify-rust-compose.py'),'--project',project,'--evidence-dir',str(OUT/'evidence')]
plan={'status':'SELF_CHECKED_BEFORE_ONE_FULL_SDK_RUN','ref':REF,'archive_sha256':sha(raw),'all_source_files':len(actual),'Rust_inputs':len(rust),'same_QA2_792_plus_coupled5_map':True,'parent_map_sha256':sha((ROOT/'artifacts/rust/m5-notification-mutations-native-qa-2/fixture/inputs/rust792-map.json').read_bytes()),'selected_inputs':len(selected),'SDK_copy_closure_files':len(copy),'host_only_control_files':len(host_only),'coupled5_count':len(coupled),'full797_QA2_map_sha256':'46d649d3503e6ee202f1393c3981871fe74da75f4a554ae275247e84915090b9','private_git_HEAD':git('rev-parse','HEAD'),'private_git_status':git('status','--short'),'object_alternate':'Read-only lookup of canonical objects; writes only private object directory/index; no canonical refs/index mutations','object_alternates_mode':'0444','private_metadata_path':str(META),'frozen_verifier_sha256':actual['scripts/verify-rust-compose.py'],'frozen_compose_sha256':actual['docker/compose.rust.yml'],'frozen_Dockerfile_sha256':actual['rust/Dockerfile'],'command':command,'cwd':str(SOURCE),'project':project,'timeout':'unchanged default1800 seconds','network_mode':'none runtime per exact committed Compose','Cargo_target':'Docker image internal /tmp/dispatcharr-target only','standard_SDK_helper_artifacts':'Unchanged rust-verify.sh includes fanout/TS-helper microbench artifacts; no separate application/resource experiment or performance claim','no_Main_or_Source_application':True,'manifest_sha256':{name:sha((OUT/name).read_bytes())for name in ['all-source-inputs.json','rust-inputs792.json','verifier-selected-inputs.json','SDK-copy-closure.json','host-only-controls.json','coupled5-inputs.json','full797-inputs.json']}}
(OUT/'plan.json').write_text(json.dumps(plan,indent=2)+'\n');print(json.dumps(plan))
