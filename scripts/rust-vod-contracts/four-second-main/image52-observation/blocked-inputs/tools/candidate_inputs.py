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
 """Exact immutable image52 788 runtime inputs, no mutable/extra Rust fallback."""
 manifest_raw=Path(manifest_path).read_bytes();record=json.loads(manifest_raw);expected_raw=Path(expected_path).read_bytes();expected=json.loads(expected_raw)
 if hashlib.sha256(manifest_raw).hexdigest()!=MANIFEST_SHA:raise ValueError('ExactImage52ManifestRequired')
 if hashlib.sha256(expected_raw).hexdigest()!=EXPECTED_SHA or len(expected)!=788:raise ValueError('ExactReviewedUnion788Required')
 require_runtime_set(record,expected)
 for key,value in IDENTITY.items():
  if record.get(key)!=value:raise ValueError('ExactImage52IdentityRequired '+key)
 if image not in[IDENTITY['tag'],IDENTITY['image_id']]:raise ValueError('ExactImage52ImageRequired')
 archive=Path(source_archive)
 if hashlib.sha256(archive.read_bytes()).hexdigest()!=IDENTITY['source_archive_sha256']:raise ValueError('CandidateArchiveBinding')
 with tarfile.open(archive)as source:
  names=[m.name for m in source.getmembers()if m.isfile()and m.name.startswith('rust/')]
  if len(names)!=788 or len(set(names))!=788 or set(names)!=set(expected):raise ValueError('ExactArchiveUnion788Required')
 result=candidate(manifest_path,image,expected_path,source_archive)
 if result['reserve_script_sha1']!='d757c8db4c0aa5a26a91e84d2dcbfb4047589be0':raise ValueError('ExactReserveScriptRequired')
 return result

MANIFEST_SHA='6b5ab542dfc6cda4f9a2187d1669f06ccd007ee7f43a1359e7812ff353afa549'
EXPECTED_SHA='8e73d1541fa86ed311f9b965fb7618979de250c33e368a4242767e42a5703bd7'
IDENTITY={'tag': 'dispatcharr-rust-app:hash-admission-image52', 'image_id': 'sha256:983dc2ba0b740d97abf45cd3a51bf96633b75fbce8cbe9ea678bad36647d9b44', 'git_revision': '98e447186af099dd17496539aec46312f6cf4f62', 'source_tree_sha256': 'ca3c42acb72734330a103d192f65f84a1e59044540f5abaa74e4faaa1e548bcc', 'source_archive_sha256': 'b019ead2a4d897ece19e0e1ce12703397dc91181ebf6112983ec9d25190fbf04'}

def require_runtime_set(record,expected):
 actual={k:v for k,v in record.get('source_sha256',{}).items()if k.startswith('rust/')}
 if len(expected)!=788 or actual!=expected:raise ValueError('ExactReviewedUnion788Required')
 return True
