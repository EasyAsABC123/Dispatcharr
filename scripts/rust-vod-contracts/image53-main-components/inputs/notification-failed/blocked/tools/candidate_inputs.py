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
 """Exact immutable image53 791 runtime inputs, no mutable/extra Rust fallback."""
 manifest_raw=Path(manifest_path).read_bytes();record=json.loads(manifest_raw);expected_raw=Path(expected_path).read_bytes();expected=json.loads(expected_raw)
 if hashlib.sha256(manifest_raw).hexdigest()!=MANIFEST_SHA:raise ValueError('ExactImage53ManifestRequired')
 if hashlib.sha256(expected_raw).hexdigest()!=EXPECTED_SHA or len(expected)!=791:raise ValueError('ExactReviewedUnion791Required')
 require_runtime_set(record,expected)
 for key,value in IDENTITY.items():
  if record.get(key)!=value:raise ValueError('ExactImage53IdentityRequired '+key)
 if image not in[IDENTITY['tag'],IDENTITY['image_id']]:raise ValueError('ExactImage53ImageRequired')
 archive=Path(source_archive)
 if hashlib.sha256(archive.read_bytes()).hexdigest()!=IDENTITY['source_archive_sha256']:raise ValueError('CandidateArchiveBinding')
 with tarfile.open(archive)as source:
  names=[m.name for m in source.getmembers()if m.isfile()and m.name.startswith('rust/')]
  if len(names)!=791 or len(set(names))!=791 or set(names)!=set(expected):raise ValueError('ExactArchiveUnion791Required')
 result=candidate(manifest_path,image,expected_path,source_archive)
 if result['reserve_script_sha1']!='d757c8db4c0aa5a26a91e84d2dcbfb4047589be0':raise ValueError('ExactReserveScriptRequired')
 return result

MANIFEST_SHA='d373bb672bc6f61357fa64c30d233b00356f06f0ff329498296a7416992f4c13'
EXPECTED_SHA='a729127b8f57f261433152d958b60695fb9e25ce824b65678b614378ac6bc8c5'
IDENTITY={'tag': 'dispatcharr-rust-app:worker-url-notifications-image53', 'image_id': 'sha256:3f599a036bf0ba15f7d47f4f28016777fba9586cb328afd10a25cac16293f4f0', 'git_revision': 'f0bfc2f200dcfd1f64ae32a9855ea250731ce67f', 'source_tree_sha256': 'e581a28540d0e64e4007d0bf2a01feb3a1d611f5cde16d77b68e006c28fa9443', 'source_archive_sha256': '92cd08964a0cfd0d0349c5c0dcb265912563c7a743bc5fab4476cdb8019b7103'}

def require_runtime_set(record,expected):
 actual={k:v for k,v in record.get('source_sha256',{}).items()if k.startswith('rust/')}
 if len(expected)!=791 or actual!=expected:raise ValueError('ExactReviewedUnion791Required')
 return True
