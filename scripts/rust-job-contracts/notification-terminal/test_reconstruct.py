"""Git-only actual closure/corruption controls. No Cargo, Docker or network.
Original Dispatcharr and dependency authors retain code/algorithm/license credit.
"""
import importlib.util,json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
HERE=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('_notification_terminal_reconstruct',HERE/'reconstruct.py');r=importlib.util.module_from_spec(spec);spec.loader.exec_module(r)
spec=importlib.util.spec_from_file_location('_notification_terminal_builder',HERE/'build_peer.py');builder=importlib.util.module_from_spec(spec);spec.loader.exec_module(builder)
class Closure(unittest.TestCase):
 def test_actual_full748_git_only_closure(self):
  with tempfile.TemporaryDirectory()as tmp:
   result=r.freeze(Path.cwd(),Path(tmp)/'source');self.assertEqual(len(result['files']),748);self.assertEqual(result['files'],json.loads((HERE/'source-overlays.json').read_text())['expected_full_file_map'])
 def bad(self,change):
  record=json.loads((HERE/'source-overlays.json').read_text());change(record)
  with tempfile.TemporaryDirectory()as tmp:
   manifest=Path(tmp)/'bad.json';manifest.write_text(json.dumps(record))
   with self.assertRaises(ValueError):r.freeze(Path.cwd(),Path(tmp)/'source',manifest)
 def test_base_rejects_git_options_before_any_command(self):
  with patch.object(r.subprocess,'check_output',side_effect=AssertionError('Git must not run')):self.bad(lambda x:x.update(base='--output=/tmp/not-permitted'))
 def test_changed_base_map(self):self.bad(lambda x:x['base_file_map'].update({'rust/Cargo.lock':'0'*64}))
 def test_wrong_overlap(self):self.bad(lambda x:next(iter(x['overlays'].values())).update(before_sha256='0'*64))
 def test_invalid_blob(self):self.bad(lambda x:next(iter(x['overlays'].values())).update(git_blob='x'*40))
 def test_changed_blob_sha(self):self.bad(lambda x:next(iter(x['overlays'].values())).update(sha256='0'*64))
 def test_missing_overlay(self):self.bad(lambda x:x['overlays'].pop(next(iter(x['overlays']))))
 def test_changed_full_map(self):self.bad(lambda x:x['expected_full_file_map'].update({'rust/Cargo.lock':'0'*64}))
 def test_unowned_overlay(self):
  def edit(x):item=x['overlays'].pop(next(iter(x['overlays'])));x['overlays']['rust/crates/app/src/main.rs']=item
  self.bad(edit)
 def test_traversal(self):
  for name in ['/absolute','../escape','rust/../../escape','rust\\escape']:
   with self.assertRaises(ValueError):r.safe(name)
 def test_existing_directory_is_never_overwritten(self):
  with tempfile.TemporaryDirectory()as tmp:
   marker=Path(tmp)/'keep';marker.write_text('preserve')
   with self.assertRaises(FileExistsError):r.freeze(Path.cwd(),Path(tmp))
   self.assertEqual(marker.read_text(),'preserve')
 def test_build_rejects_wrong_source_before_cargo_or_output(self):
  with tempfile.TemporaryDirectory()as tmp:
   source=Path(tmp)/'source';source.mkdir();output=Path(tmp)/'out'
   with patch('sys.argv',['build_peer','--source',str(source),'--target',str(Path(tmp)/'target'),'--output',str(output)]),patch.object(builder.subprocess,'run',side_effect=AssertionError('Cargo must not run')):
    with self.assertRaises(ValueError):builder.main()
   self.assertFalse(output.exists())
if __name__=='__main__':unittest.main(verbosity=2)
