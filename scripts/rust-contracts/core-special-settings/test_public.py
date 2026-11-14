import copy,hashlib,json,unittest,shutil,tempfile
from pathlib import Path
import cache_primitives as cache
import verify
import reconstruct
HERE=Path(__file__).resolve().parent
class Evidence(unittest.TestCase):
 @classmethod
 def setUpClass(cls):cls.data=json.loads((HERE/'retained.json').read_text())
 def test_positive(self):self.assertEqual(verify.verify(self.data)['corrected_literal_matches'],51)
 def test_corruptions(self):
  challenges={
   'missing_epoch':lambda d:d['epochs'].pop('Native-first-HOST1'),
   'missing_group':lambda d:d['epochs']['Native-corrected'].pop(),
   'missing_case':lambda d:d['epochs']['Native-corrected'][0]['cases'].pop(),
   'empty_cases':lambda d:d['epochs']['Native-corrected'][0].update(cases=[]),
   'missing_origin':lambda d:d['origins'].pop(),
   'raw_passed_relabel':lambda d:d['epochs']['Source51'][0].update(raw_recorder_passed=True),
   'empty_controls':lambda d:d['epochs']['Native-corrected'][0].update(controls=[]),
   'missing_control':lambda d:d['epochs']['Native-first-HOST1'][0]['controls'].pop(),
   'bad_control_socket':lambda d:d['epochs']['Native-corrected'][0]['controls'][0]['physical_close'].update(original_inode_absent=False),
   'bad_warm':lambda d:d['epochs']['Native-corrected'][0]['controls'][2]['json'].update(loaded=False),
   'missing_tables':lambda d:d['epochs']['Native-corrected'][0]['cases'][0]['before']['SQL']['full6259'].update(tables={}),
   'missing_sequences':lambda d:d['epochs']['Native-corrected'][0]['cases'][0]['before']['SQL']['full6259'].update(sequences={}),
   'missing_schema':lambda d:d.update(schema_catalog={}),
   'schema_mutation':lambda d:next(iter(d['schema_catalog'].values())).update(columns=[]),
   'wrong_input':lambda d:d['epochs']['Native-corrected'][0]['cases'][0].update(role='guest'),
   'wrong_path':lambda d:d['epochs']['Native-corrected'][0]['cases'][0].update(path='/wrong/'),
   'response_bytes':lambda d:d['epochs']['Native-corrected'][0]['cases'][0].update(response_body_hex='00'),
   'request_digest':lambda d:d['epochs']['Native-corrected'][0]['cases'][0].update(body_sha256='0'*64),
   'status':lambda d:d['epochs']['Native-corrected'][0]['cases'][0].update(status=501),
   'headers':lambda d:d['epochs']['Native-corrected'][0]['cases'][0]['headers'].update({'Content-Type':'wrong','content-type':'wrong'}),
   'negative_clock':lambda d:d['epochs']['Native-corrected'][0]['cases'][0].update(duration_seconds=-1),
   'infinite_clock':lambda d:d['epochs']['Native-corrected'][0]['cases'][0].update(duration_seconds=float('inf')),
   'bad_UTC':lambda d:d['epochs']['Native-corrected'][0]['cases'][0].update(completed_utc='2000-01-01T00:00:00+00:00'),
   'bad_epoch':lambda d:d['epochs']['Native-corrected'][0]['cases'][0].update(began_epoch=1),
   'API_close':lambda d:d['epochs']['Native-corrected'][0]['cases'][0]['physical_close'].update(timer_joined=False),
   'cache_removed':lambda d:d['epochs']['Native-corrected'][0]['cases'][0]['before'].update(named_group_cache={}),
   'version_changed':lambda d:d['epochs']['Native-corrected'][0]['cases'][0]['after']['named_group_cache'].update({'coresettings:groupver:system_settings':1}),
   'settings_changed':lambda d:d['epochs']['Native-corrected'][0]['cases'][0]['after']['SQL'].update(settings=[]),
   'notices_removed':lambda d:d['epochs']['Native-corrected'][1]['cases'][2]['after']['SQL'].update(notices=[]),
   'timezone_changed':lambda d:d['epochs']['Native-corrected'][0]['cases'][0]['after']['local'].update(display_zone='Invalid/NotOwned'),
   'file_digest':lambda d:d['epochs']['Native-corrected'][0]['cases'][0]['after']['local']['owned_log_files'][0].update(sha256='0'*64),
   'proxy_rename':lambda d:d['epochs']['Native-corrected'][3]['cases'][6]['after']['local'].update(process_proxy_cache=None),
   'broker_empty':lambda d:d['epochs']['Native-corrected'][4]['cases'][3]['after'].update(broker_frames=[]),
   'broker_task':lambda d:d['epochs']['Native-corrected'][4]['cases'][3]['after']['broker_frames'][-1].update(task='wrong'),
   'lock_missing':lambda d:d['epochs']['Native-corrected'][4]['cases'][3]['after']['raw_effects'].update(locks=[]),
   'lock_wrong_value':lambda d:d['epochs']['Native-corrected'][4]['cases'][3]['after']['raw_effects']['locks'][0].update(private_value_sha256='0'*64),
   'Channels_empty':lambda d:d['epochs']['Native-corrected'][1]['cases'][2]['after'].update(Channels_frames=[]),
   'UA_value':lambda d:d['epochs']['Native-corrected'][4]['cases'][0]['after']['raw_effects']['default_UA'][0].update(value_hex='313233'),
   'UA_coverage':lambda d:d['epochs']['Native-corrected'][4]['cases'][0]['after']['raw_effects'].update(default_UA=[]),
   'condition_empty':lambda d:d['epochs']['Native-corrected'][1]['cases'][0]['before']['raw_effects'].update(conditions=[]),
   'condition_value':lambda d:d['epochs']['Native-corrected'][1]['cases'][0]['before']['raw_effects']['conditions'][0].update(value_hex='313233'),
   'first_failure_relabel':lambda d:d['epochs']['Native-first-HOST1'][1]['cases'][3].update(status=403,response_body_hex=d['epochs']['Source51'][1]['cases'][3]['response_body_hex']),
  }
  for name,mutate in challenges.items():
   with self.subTest(name=name):
    value=copy.deepcopy(self.data);mutate(value)
    with self.assertRaises((ValueError,KeyError,IndexError,TypeError)):verify.verify(value)
 def test_metadata_and_Original_code_corruptions(self):
  authority=json.loads((HERE/'authority.json').read_text())
  with tempfile.TemporaryDirectory()as d:
   root=Path(d)
   for name in ['authority.json',*authority['metadata']]:shutil.copyfile(HERE/name,root/name)
   shutil.copytree(HERE/'original',root/'original');verify.verify_metadata(root)
   for name in authority['metadata']:
    with self.subTest(name=name):
     old=(root/name).read_bytes();value=json.loads(old)
     if name=='provenance.json':value['Original_image']='wrong'
     elif name=='qa.json':value['default_counts']['passed']=0
     elif name=='agent-review-receipts.json':value[0]['projection']['passed']=False
     elif name=='privacy-inventory.json':value['matches']=1
     elif name=='privacy-full-packet.json':value['matches']=1
     elif name=='verification.json':value['private_raw_predicates_passed']=0
     elif name=='additive-UA-review.json':value['passed']=False
     elif name=='original-inputs.json':value['git']='wrong'
     (root/name).write_text(json.dumps(value))
     with self.assertRaises(ValueError):verify.verify_metadata(root)
     (root/name).write_bytes(old)
   file=next(p for p in(root/'original').rglob('*')if p.is_file());old=file.read_bytes();file.write_bytes(old+b' ')
   with self.assertRaises(ValueError):verify.verify_metadata(root)
   file.write_bytes(old);file.unlink()
   with self.assertRaises(ValueError):verify.verify_metadata(root)
 def test_safe_decoder(self):
  self.assertEqual(cache.decode(bytes.fromhex('8004950d000000000000007d948c056f776e65649488732e')),{'owned':True})
  for raw in [b'cos\nsystem\n.',b'\x80\x04h\x00.',b'\x80\x04N',b'\x80\x04N.x',b'x'*65537,b'\x80\x04N\x85R.',b'\x80\x04\x95'+(65537).to_bytes(8,'little')+b'N.']:
   with self.subTest(raw=raw[:16]),self.assertRaises(ValueError):cache.decode(raw)
 def test_source_composition(self):
  base,final,selected=reconstruct.maps();files={p:(HERE/'source/selected'/p).read_bytes()for p in selected if p in base}
  # Check exact real selected postimages and mismatching preimage rejection;
  # full Git materialization is a separate actual command, not faked here.
  self.assertEqual(len(selected),16)
  for p,row in selected.items():self.assertEqual(hashlib.sha256((HERE/'source/selected'/p).read_bytes()).hexdigest(),row['after'])
  with self.assertRaises(ValueError):reconstruct.compose({},base,final,selected)
if __name__=='__main__':unittest.main()
