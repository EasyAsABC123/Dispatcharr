"""UNEXECUTED immutable Main input guard; no fallback to mutable Rust sources."""
import hashlib,json,tarfile
from pathlib import Path

def candidate(manifest_path,image,expected_path,source_archive):
 manifest_path=Path(manifest_path).resolve();raw=manifest_path.read_bytes();record=json.loads(raw);expected=json.loads(Path(expected_path).read_text())
 if record.get('passed')is not True or record.get('exit_code')!=0:raise ValueError('Successful immutable build required')
 if image not in[record['tag'],record['image_id']]:raise ValueError('CandidateIdentityBinding')
 for name,sha in expected.items():
  if record['source_sha256'].get(name)!=sha:raise ValueError('UnqualifiedCandidateRuntime '+name)
 archive=Path(source_archive).resolve(strict=True)
 if hashlib.sha256(archive.read_bytes()).hexdigest()!=record['source_archive_sha256']:raise ValueError('CandidateArchiveBinding')
 name='rust/crates/coordination/src/scripts.rs'
 with tarfile.open(archive)as source:
  for runtime,sha in expected.items():
   if hashlib.sha256(source.extractfile(runtime).read()).hexdigest()!=sha:raise ValueError("CandidateRuntimeArchiveBinding "+runtime)
  data=source.extractfile(name).read()
 if hashlib.sha256(data).hexdigest()!=record['source_sha256'][name]:raise ValueError('CoordinatorScriptBinding')
 text=data.decode()
 def part(label):return text.split('const '+label+': &str = r#"',1)[1].split('"#;',1)[0]
 script=(part('REAP')+part('RESERVE_PROVIDER')).encode()
 return{'manifest_sha256':hashlib.sha256(raw).hexdigest(),'image_id':record['image_id'],'source_tree_sha256':record['source_tree_sha256'],'git_revision':record['git_revision'],'reserve_script_sha1':hashlib.sha1(script).hexdigest(),'coordinator_source_sha256':hashlib.sha256(data).hexdigest(),'runtime_sha256':expected}


def bound_candidate(manifest_path,image,expected_path,source_archive):
 """Exact immutable image59 854 runtime inputs, no mutable/extra Rust fallback."""
 manifest_raw=Path(manifest_path).read_bytes();record=json.loads(manifest_raw);expected_raw=Path(expected_path).read_bytes();expected=json.loads(expected_raw)
 if hashlib.sha256(manifest_raw).hexdigest()!=MANIFEST_SHA:raise ValueError('ExactImage59ManifestRequired')
 if hashlib.sha256(expected_raw).hexdigest()!=EXPECTED_SHA or len(expected)!=854:raise ValueError('ExactReviewedUnion854Required')
 require_runtime_set(record,expected)
 for key,value in IDENTITY.items():
  if record.get(key)!=value:raise ValueError('ExactImage59IdentityRequired '+key)
 if image not in[IDENTITY['tag'],IDENTITY['image_id']]:raise ValueError('ExactImage59ImageRequired')
 archive=Path(source_archive)
 if hashlib.sha256(archive.read_bytes()).hexdigest()!=IDENTITY['source_archive_sha256']:raise ValueError('CandidateArchiveBinding')
 with tarfile.open(archive)as source:
  members=[m for m in source.getmembers()if m.isfile()]
  if len(members)!=1351 or len({m.name for m in members})!=1351 or {m.name:hashlib.sha256(source.extractfile(m).read()).hexdigest()for m in members}!=record['source_sha256']:raise ValueError('ExactApp1351ArchiveRequired')
  names=[m.name for m in members if m.name.startswith('rust/')]
  if len(names)!=854 or len(set(names))!=854 or set(names)!=set(expected):raise ValueError('ExactArchiveUnion854Required')
 result=candidate(manifest_path,image,expected_path,source_archive)
 if result['reserve_script_sha1']!='d757c8db4c0aa5a26a91e84d2dcbfb4047589be0':raise ValueError('ExactReserveScriptRequired')
 return result

MANIFEST_SHA='a366ae13b8312cd20f69e09090b55031152f4240c26df4f7b6cbdd96294a8acf'
EXPECTED_SHA='f4159113797d1a35a5b3ecf576a8f26c695158f267c79b0fe677215c7d607321'
IDENTITY={'tag': 'dispatcharr-rust-candidate:image59', 'image_id': 'sha256:7fed56b01c41c4657c25bad95f2b12eb0b6dc1726c8e874dc9c561afcf06cf27', 'git_revision': 'c0b824e115665a17823ac6511d465f1aaed2a6f1', 'source_tree_sha256': 'ff08e3f61d8196bb73863ddb4a9df262732fa7b8f76f68f312686aa9af25bdd8', 'source_archive_sha256': 'f81dad4566c26cb10b7f31f086ceb0a64e7355782ffabe703d1862ec2eda652c'}

def require_runtime_set(record,expected):
 actual={k:v for k,v in record.get('source_sha256',{}).items()if k.startswith('rust/')}
 if len(expected)!=854 or actual!=expected:raise ValueError('ExactReviewedUnion854Required')
 return True
