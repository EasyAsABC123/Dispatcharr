"""Pure/loopback-independent controls only, not Source/runtime evidence."""
import copy,datetime,importlib.util,json,tempfile,unittest
from pathlib import Path
import protocol,reader,acceptance,orchestrator
class Controls(unittest.TestCase):
 def manifest(self):return {'passed':True,'git_revision':'2'*40,'image_id':'sha256:'+'1'*64,'source_tree_sha256':'3'*64,'source_archive_sha256':'4'*64}
 def bound(self):
  directory=tempfile.TemporaryDirectory();self.addCleanup(directory.cleanup);manifest=self.manifest();record=protocol.bind(manifest,manifest['git_revision'],Path(directory.name)/'bound.json');return record,manifest
 def test_identity_and_budget_cannot_change(self):
  record,manifest=self.bound();protocol.identity(record,manifest)
  changed=copy.deepcopy(manifest);changed['image_id']='sha256:'+'5'*64
  with self.assertRaises(ValueError):protocol.identity(record,changed)
  changed=copy.deepcopy(record);changed['policy']['budgets']['max_cpu_ratio']=2
  with self.assertRaises(ValueError):protocol.identity(changed,manifest)
 def test_source_only_rate_selection(self):
  record,_=self.bound();work=record['policy']['workloads'][0];rows=[{'backend':'source','period_seconds':period,'pacing_fraction':1,'cpu_percent_mean':20,'memory_fraction':.2,'throttled_period_fraction':0,'errors':0,'correct_responses':True}for period in [10,5,2,1]]
  self.assertEqual(protocol.select_period(record,work,rows),1)
  with self.assertRaises(ValueError):protocol.select_period(record,work,rows[:3])
  changed=copy.deepcopy(rows);changed[0]['candidate_cpu']=1
  with self.assertRaises(ValueError):protocol.select_period(record,work,changed)
 def test_every_resource_and_phase_budget_rejects(self):
  baseline=dict(cpu_percent_mean=10,peak_working_memory_bytes=1024,ttfb_p95_ms=10,completion_p95_ms=10,throughput_per_second=1,pacing_fraction=1,samples=60,errors=0,sample_errors=0,late_completed=0,oom_kills=0,counter_resets=0,correct_responses=True,cache_state_verified=True,fixture_unchanged=True,limits_verified=True)
  self.assertTrue(acceptance.compare(baseline,baseline)['passed'])
  for field,value in [('cpu_percent_mean',11),('peak_working_memory_bytes',1025),('ttfb_p95_ms',56),('completion_p95_ms',12),('throughput_per_second',.9),('pacing_fraction',.9),('samples',59),('errors',1),('sample_errors',1),('late_completed',1),('oom_kills',1),('counter_resets',1),('fixture_unchanged',False),('limits_verified',False),('correct_responses',False)]:
   changed=dict(baseline);changed[field]=value
   self.assertFalse(acceptance.compare(baseline,changed)['passed'],field)
  changed=dict(baseline);changed['cpu_percent_mean']=float('nan')
  with self.assertRaises(ValueError):acceptance.compare(baseline,changed)
 def test_frozen_plan_and_separate_fresh_measurement_cue(self):
  record,manifest=self.bound();work=record['policy']['workloads'];rows=[{'backend':'source','period_seconds':period,'pacing_fraction':1,'cpu_percent_mean':20,'memory_fraction':.2,'throttled_period_fraction':0,'errors':0,'correct_responses':True}for period in [10,5,2,1]];qualification={value['name']:copy.deepcopy(rows)for value in work};fixture={'fixture_sha256':'f'*64,'schema_sha256':'a'*64}
  with tempfile.TemporaryDirectory()as directory:
   root=Path(directory);plan=protocol.freeze(root/'plan.json',record,qualification,manifest,fixture);protocol.validate_plan(plan,record,qualification,manifest,fixture)
   altered=copy.deepcopy(plan);altered['selected_periods'][work[0]['name']]=10
   with self.assertRaises(ValueError):protocol.validate_plan(altered,record,qualification,manifest,fixture)
   cue={'scope':'vod-category-source-qualification','authorized':True,'candidate_image':manifest['image_id'],'candidate_manifest_canonical_sha256':protocol.digest(manifest),'policy_sha256':record['policy_sha256'],'boot_id':Path('/proc/sys/kernel/random/boot_id').read_text().strip(),'expires_utc':(datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(minutes=1)).isoformat()};path=root/'cue.json';path.write_text(json.dumps(cue))
   with self.assertRaises(ValueError):protocol.authorize(path,record,manifest,'vod-category-resource-measurement',plan['plan_sha256'])
   cue.update(scope='vod-category-resource-measurement',plan_sha256=plan['plan_sha256']);path.write_text(json.dumps(cue));protocol.authorize(path,record,manifest,'vod-category-resource-measurement',plan['plan_sha256'])
   cue['expires_utc']='2000-01-01T00:00:00+00:00';path.write_text(json.dumps(cue))
   with self.assertRaises(ValueError):protocol.authorize(path,record,manifest,'vod-category-resource-measurement',plan['plan_sha256'])
 def test_orchestrator_rejects_missing_cue_before_any_callback(self):
  record,manifest=self.bound();calls=[]
  def forbidden(*args,**kwargs):calls.append(True);raise AssertionError('Work began before cue')
  with tempfile.TemporaryDirectory()as directory:
   with self.assertRaises(FileNotFoundError):orchestrator.execute(output=Path(directory)/'experiment',manifest=manifest,binding=record,tools=Path(directory),reference={},start=forbidden,stop=forbidden,helper=forbidden,select_services=forbidden,reader=None,quiet=Path(directory)/'absent',measurement_cue=Path(directory)/'future')
  self.assertEqual(calls,[])
 def body(self):
  result=[]
  for index in range(242):
   kind='movie'if index<121 else'series';name='Uncategorized'if index%121==0 else f'category-{index:04}';row={'id':index+1,'name':name,'category_type':kind,'category_type_display':'Movie'if kind=='movie'else'Series','m3u_accounts':[]};result.append(row)
  for index in range(501):result[index%242]['m3u_accounts'].append({'category':result[index%242]['id'],'m3u_account':index%5+1,'enabled':index>=177,'custom_properties':{}})
  return json.dumps(result,separators=(',',':')).encode()
 def test_exact_body_not_only_equivalent_JSON(self):
  body=self.body();reference=reader.validate(body)
  with self.assertRaises(ValueError):reader.validate(json.dumps(json.loads(body),indent=2).encode(),reference)
  value=json.loads(body);value[0]['m3u_accounts'].pop()
  with self.assertRaises(ValueError):reader.validate(json.dumps(value).encode())
  value=json.loads(body);value[0]['m3u_accounts'][0]['enabled']=0
  with self.assertRaises(ValueError):reader.validate(json.dumps(value).encode())
  value=json.loads(body);value[0]['m3u_accounts'][0]['category']=True
  with self.assertRaises(ValueError):reader.validate(json.dumps(value).encode())
 def test_unbound_and_absent_cues_do_not_authorize(self):
  record,manifest=self.bound()
  with self.assertRaises(FileNotFoundError):protocol.authorize('/no-such-category-cue',record,manifest,'vod-category-source-qualification')
  with self.assertRaises(ValueError):protocol.authorize('/no-such-category-cue',record,manifest,'other-scope')
if __name__=='__main__':unittest.main()
