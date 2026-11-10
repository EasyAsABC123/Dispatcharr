import hashlib,json,unittest
import result_codec
class ResultCodec(unittest.TestCase):
 def test_exception_metadata_and_dynamic_fields_redacted(self):
  value={'task_id':'PRIVATE_TASK','status':'FAILURE','date_done':'2026-01-02T03:04:05+00:00','result':{'exc_type':'ValueError','exc_module':'builtins','exc_message':['fixture',3]},'traceback':'PRIVATE_TRACEBACK','children':[]}
  raw=json.dumps(value).encode();output=result_codec.project(raw,'PRIVATE_TASK');serialized=json.dumps(output)
  self.assertNotIn('PRIVATE_TASK',serialized);self.assertNotIn('PRIVATE_TRACEBACK',serialized)
  self.assertTrue(output['metadata']['fields']['task_id']['matches_requested_task'])
  self.assertEqual(output['raw_sha256'],hashlib.sha256(raw).hexdigest())
  fields=output['metadata']['fields']['result']['fields']
  self.assertEqual(fields['exc_type']['value'],'ValueError')
  self.assertEqual(fields['exc_message']['items'][1],{'type':'int','value':3})
 def test_raw_tree_string_and_tagged_json_bounds(self):
  with self.assertRaises(ValueError):result_codec.project(b'x'*(result_codec.MAX_RAW+1),'id')
  value=None
  for _ in range(34):value=[value]
  with self.assertRaises(ValueError):result_codec.project(json.dumps(value).encode(),'id')
  with self.assertRaises(ValueError):result_codec.project(json.dumps('x'*65537).encode(),'id')
  output=result_codec.project(b'{"result":{"__type__":"decimal","__value__":"1.20"}}','id')
  fields=output['metadata']['fields']['result']['fields']
  self.assertEqual(fields['__type__']['value'],'decimal');self.assertEqual(fields['__value__']['value'],'1.20')
if __name__=='__main__':unittest.main()
