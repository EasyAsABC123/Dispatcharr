import copy,hashlib,io,json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch,MagicMock
import plan,reader
class Policy(unittest.TestCase):
 def rows(self,work):return[{'backend':'source','period_seconds':p,'errors':0,'correct_responses':True,'pacing_fraction':1,'cpu_percent_mean':100,'memory_fraction':.5,'throttled_period_fraction':0}for p in work['period_candidates_seconds']]
 def test_source_only_complete_grid_and_caps(self):
  work=plan.POLICY['workloads'][0];rows=self.rows(work);self.assertEqual(plan.select_period(work,rows),min(work['period_candidates_seconds']))
  with self.assertRaises(ValueError):plan.select_period(work,rows[:-1])
  rows[0]['backend']='candidate'
  with self.assertRaises(ValueError):plan.select_period(work,rows)
  rows=self.rows(work)
  for row in rows:row['cpu_percent_mean']=180
  with self.assertRaises(ValueError):plan.select_period(work,rows)
 def test_recovery_manifest_changes_image_not_source(self):
  qualification={work['name']:self.rows(work)for work in plan.POLICY['workloads']};manifest={'passed':True,'source_tree_sha256':plan.TREE,'source_archive_sha256':plan.ARCHIVE,'image_id':'sha256:recovered'}
  with tempfile.TemporaryDirectory()as root:
   record=plan.freeze(Path(root)/'plan.json',qualification,manifest);self.assertEqual(record['candidate_image'],'sha256:recovered');self.assertFalse(record['measurements_started']);self.assertEqual(plan.validate_frozen(record,qualification,manifest),record['plan_sha256'])
   altered=copy.deepcopy(record);altered['selected_periods_seconds'][plan.POLICY['workloads'][0]['name']]=123
   with self.assertRaises(ValueError):plan.validate_frozen(altered,qualification,manifest)
   manifest['source_tree_sha256']='different'
   with self.assertRaises(ValueError):plan.freeze(Path(root)/'bad.json',qualification,manifest)
class Response:
 def __init__(self,data,mime):self.body=io.BytesIO(data);self.length=len(data);self.status=200;self.mime=mime
 def read(self,n):return self.body.read(n)
 def isclosed(self):return self.body.tell()==self.length
 def getheader(self,key,default=None):return self.mime if key=='Content-Type'else default
class Reader(unittest.TestCase):
 def call(self,data,spec,expected=reader.UNSET):
  connection=MagicMock();connection.getresponse.return_value=Response(data,'application/json'if spec['kind']=='json'else'application/xml')
  with patch('reader.http.client.HTTPConnection',return_value=connection):result=reader.request('http://127.0.0.1:1',spec,'private-fixture',expected=expected)
  return result,connection
 def test_post_and_boolean_numeric_semantics(self):
  spec={'method':'POST','kind':'json','path':'/api/epg/current-programs/','body':{'channel_uuids':[]}}
  result,connection=self.call(b'[{"v":1}]',spec);self.assertEqual(result[0],{'kind':'json','value':[{'v':1}]});self.assertEqual(connection.request.call_args.kwargs['body'],b'{"channel_uuids":[]}')
  with self.assertRaises(ValueError):self.call(b'[{"v":1}]',spec,{'kind':'json','value':[{'v':True}]})
 def test_xml_incremental_hash_and_structure(self):
  data=b'<tv><channel id="one"/><programme channel="one"/></tv>';value=self.call(data,{'method':'GET','kind':'xml','path':'/output/epg','validate_xml':True})[0][0];self.assertEqual(value['sha256'],hashlib.sha256(data).hexdigest());self.assertEqual(value['xml_counts'],{'channels':1,'programmes':1})
  with self.assertRaises(ValueError):self.call(data,{'method':'GET','kind':'xml','path':'/output/epg'}, {'kind':'xml','bytes':len(data),'sha256':'wrong'})
 def test_remote_targets_empty_and_wrong_content_rejected(self):
  spec={'method':'GET','kind':'xml','path':'/output/epg'}
  with self.assertRaises(ValueError):reader.request('http://production.invalid',spec)
  with self.assertRaises(ValueError):self.call(b'',spec)

class Cgroup(unittest.TestCase):
 def fixture(self):
  import cgroup
  return cgroup.counters(json.dumps({'cpu':'usage_usec 100\nnr_periods 10\nnr_throttled 0\nthrottled_usec 0\n','events':'oom 0\noom_kill 0\n','cpu_max':'200000 100000','memory_max':str(512*1024**2),'memory_current':'1000'}))
 def test_full_counter_delta_and_reset_or_oom_rejected(self):
  import cgroup
  before=self.fixture();before['sampled_monotonic']=1;after=copy.deepcopy(before);after['sampled_monotonic']=61;after['cpu']['usage_usec']+=60000000;after['cpu']['nr_periods']+=600;self.assertEqual(cgroup.delta(before,after)['cpu_percent_mean'],100)
  after['memory_events']['oom_kill']=1
  with self.assertRaises(ValueError):cgroup.delta(before,after)
  after=copy.deepcopy(before);after['sampled_monotonic']=61;after['cpu']['usage_usec']=0
  with self.assertRaises(ValueError):cgroup.delta(before,after)

if __name__=='__main__':unittest.main()
