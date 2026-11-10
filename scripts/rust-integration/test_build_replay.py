"""Immutable archive replay gates: no Docker build or mutable-source capture."""
import copy,hashlib,importlib.util,io,json,sys,tarfile,tempfile,unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
spec=importlib.util.spec_from_file_location('candidate_build_replay',Path(__file__).with_name('build.py'));build=importlib.util.module_from_spec(spec);spec.loader.exec_module(build)
class Replay(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name);self.archive=self.root/'source.tar';self.files={name:('fixture:'+name).encode()for name in ['LICENSE','ATTRIBUTION.md','rust/Cargo.toml','rust/Dockerfile.app','frontend/package.json','frontend/package-lock.json']};self.write_archive();hashes={name:hashlib.sha256(value).hexdigest()for name,value in self.files.items()};self.manifest={'format_version':1,'passed':True,'source_sha256':hashes,'source_tree_sha256':hashlib.sha256(json.dumps(hashes,sort_keys=True,separators=(',',':')).encode()).hexdigest(),'source_archive_sha256':build._digest_file(self.archive),'git_revision':'original-revision','worktree_status':'original-only','image_id':'sha256:original'}
 def write_archive(self,entries=None):
  with tarfile.open(self.archive,'w')as archive:
   for name,data,kind in entries or[(name,data,tarfile.REGTYPE)for name,data in self.files.items()]:
    info=tarfile.TarInfo(name);info.type=kind;info.size=len(data)if kind==tarfile.REGTYPE else 0
    if kind in [tarfile.SYMTYPE,tarfile.LNKTYPE]:info.linkname='/outside'
    archive.addfile(info,io.BytesIO(data)if kind==tarfile.REGTYPE else None)
 def validate(self,manifest=None):return build.validate_replay_archive(self.archive,json.dumps(manifest or self.manifest).encode())
 def test_exact_replay_validates(self):self.assertEqual(self.validate(),self.manifest)
 def test_archive_byte_member_and_tree_corruption_rejected(self):
  for mode in ['archive','member','tree']:
   with self.subTest(mode=mode):
    manifest=copy.deepcopy(self.manifest)
    if mode=='archive':manifest['source_archive_sha256']='0'*64
    elif mode=='tree':manifest['source_tree_sha256']='0'*64
    else:
     key='LICENSE';manifest['source_sha256'][key]='0'*64;manifest['source_tree_sha256']=hashlib.sha256(json.dumps(manifest['source_sha256'],sort_keys=True,separators=(',',':')).encode()).hexdigest()
    with self.assertRaises(ValueError):self.validate(manifest)
 def test_duplicate_unexpected_missing_and_links_rejected(self):
  base=[(n,v,tarfile.REGTYPE)for n,v in self.files.items()]
  for entries in [base+[base[0]],base+[('rust/new.rs',b'extra',tarfile.REGTYPE)],base[:-1],base+[('rust/link',b'',tarfile.SYMTYPE)],base+[('rust/link',b'',tarfile.LNKTYPE)]]:
   with self.subTest(last=entries[-1][0]):
    self.write_archive(entries);manifest=copy.deepcopy(self.manifest);manifest['source_archive_sha256']=build._digest_file(self.archive)
    with self.assertRaises(ValueError):self.validate(manifest)
 def test_manifest_path_escape_and_scope_rejected(self):
  for name in ['../outside','/absolute','rust/../outside','rust//file','rust\\file','frontend/node_modules/key','rust/target/file','other/file']:
   manifest=copy.deepcopy(self.manifest);manifest['source_sha256'][name]='0'*64
   with self.subTest(name=name),self.assertRaises(ValueError):self.validate(manifest)
 def test_typed_manifest_and_bounded_member_rejected(self):
  for value in [[],{'format_version':True,'passed':True},{'format_version':1,'passed':False}]:
   with self.assertRaises(ValueError):build.validate_replay_archive(self.archive,json.dumps(value).encode())
  with patch.object(build,'MAX_MEMBER_BYTES',1),self.assertRaises(ValueError):self.validate()
 def test_main_replay_never_reads_mutable_application_or_git_and_uses_current_overlay(self):
  source_manifest=self.root/'original.json';source_manifest.write_text(json.dumps(self.manifest));workspace=self.root/'workspace';(workspace/'docker').mkdir(parents=True);overlay='services:\n  candidate:\n    image: dispatcharr-rust-candidate:milestone2\n    environment: {TMPDIR: /data}\n';(workspace/'docker/compose.rust-candidate.yml').write_text(overlay);out=self.root/'recovered';original=self.archive.read_bytes()
  def output(args,**kwargs):
   if args[:3]==['docker','image','inspect']:return json.dumps([{'Config':{'Labels':{'org.opencontainers.image.source-tree-sha256':self.manifest['source_tree_sha256']}},'Id':'sha256:recovered','Os':'linux','Architecture':'amd64'}])
   if args[0]!='docker':raise AssertionError('Git or mutable application capture attempted')
   return 'ffmpeg pinned\n'if args[-1]=='-version'else'pinned notices\n'
  def run(args,**kwargs):self.assertEqual(kwargs['stdin'].read(),original);return SimpleNamespace(returncode=0)
  argv=['build.py','--output',str(out),'--tag','fixture:recovered','--source-archive',str(self.archive),'--source-manifest',str(source_manifest)]
  with patch.object(build,'ROOT',workspace),patch.object(sys,'argv',argv),patch.object(build.os,'walk',side_effect=AssertionError('Mutable Rust read')),patch.object(build.subprocess,'check_output',side_effect=output),patch.object(build.subprocess,'run',side_effect=run):self.assertEqual(build.main(),0)
  report=json.loads((out/'build.json').read_text());self.assertEqual(report['source_input_mode'],'validated-archive-replay');self.assertFalse(report['mutable_application_source_read']);self.assertEqual(report['git_revision'],'original-revision');self.assertEqual((out/'source.tar').read_bytes(),original);self.assertIn('TMPDIR: /data',(out/'candidate-overlay.yml').read_text());self.assertEqual(report['candidate_overlay_scope'],'current workspace deployment template; not archived application source')
 def test_default_workspace_snapshot_still_captures_original_bytes_and_excludes_build_trees(self):
  workspace=self.root/'workspace-default';(workspace/'docker').mkdir(parents=True);(workspace/'docker/compose.rust-candidate.yml').write_text('services:\n  candidate:\n    image: dispatcharr-rust-candidate:milestone2\n')
  for name,data in self.files.items():
   path=workspace/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(data)
  for name in ['rust/src/lib.rs','rust/target/ignored','rust/__pycache__/ignored','frontend/.env.local']:
   path=workspace/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(b'original workspace content')
  state={};out=self.root/'workspace-output'
  def output(args,**kwargs):
   if args[:2]==['git','ls-files']:return b'frontend/package.json\0frontend/package-lock.json\0'
   if args[:2]==['git','rev-parse']:return 'current-revision\n'
   if args[:2]==['git','status']:return 'current-status\n'
   if args[:3]==['docker','image','inspect']:return json.dumps([{'Config':{'Labels':{'org.opencontainers.image.source-tree-sha256':state['tree']}},'Id':'sha256:current','Os':'linux','Architecture':'amd64'}])
   return 'ffmpeg pinned\n'if args[-1]=='-version'else'pinned notices\n'
  def run(args,**kwargs):
   data=kwargs['stdin'].read();state['tree']=args[args.index('--label')+1].split('=',1)[1]
   with tarfile.open(fileobj=io.BytesIO(data))as archive:
    names=archive.getnames();self.assertIn('rust/src/lib.rs',names);self.assertFalse(any('target'in name or'__pycache__'in name or'.env.local'in name for name in names));self.assertEqual(archive.extractfile('rust/src/lib.rs').read(),b'original workspace content')
   (workspace/'rust/src/lib.rs').write_bytes(b'changed after capture');return SimpleNamespace(returncode=0)
  with patch.object(build,'ROOT',workspace),patch.object(sys,'argv',['build.py','--output',str(out)]),patch.object(build.subprocess,'check_output',side_effect=output),patch.object(build.subprocess,'run',side_effect=run):self.assertEqual(build.main(),0)
  report=json.loads((out/'build.json').read_text());self.assertEqual(report['source_input_mode'],'workspace-snapshot');self.assertEqual(report['git_revision'],'current-revision');self.assertEqual(report['source_sha256']['rust/src/lib.rs'],hashlib.sha256(b'original workspace content').hexdigest())
 def test_cli_requires_both_inputs_before_source_or_docker(self):
  for flag,path in [('--source-archive',self.archive),('--source-manifest',self.root/'manifest')]:
   with patch.object(sys,'argv',['build.py','--output',str(self.root/'unused'),flag,str(path)]),patch.object(build.subprocess,'check_output',side_effect=AssertionError('Unexpected command')),self.assertRaises(SystemExit)as result:build.main()
   self.assertEqual(result.exception.code,2)
if __name__=='__main__':unittest.main()
