"""GRID uses the shared orchestrator without touching XML/cache request paths."""
import datetime,json,math,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
import grid_plan,grid_qualification,grid_acceptance,orchestrator
class GridOrchestration(unittest.TestCase):
 def test_source_only_eight_rate_checks_then_six_pairs_without_xml(self):
  now=datetime.datetime.now(datetime.timezone.utc);starts=[];stops=[];selected={'baseline':{'id':'a'*64},'origin':{'id':'b'*64},'source':{'id':'c'*64}}
  seed={'clock':now.isoformat(),'channel_uuids':['private-fixture']};manifest={'passed':True,'image_id':grid_plan.IMAGE,'source_tree_sha256':grid_plan.TREE,'source_archive_sha256':grid_plan.ARCHIVE};current={'kind':'json','method':'GET','path':'/api/epg/grid/?start=fixture&end=fixture'};reference={'current':{'kind':'json','value':{'data':[]}}}
  class NoXmlCache:
   def __getattr__(self,name):raise AssertionError('GRID must not access XML cache: '+name)
  class Observer:
   def __init__(self,*args):pass
   def counters(self):return{}
   def sample(self):return{}
   def finish(self,_):return {'cpu_percent_mean':100,'selected_services':selected,'services':{'source':{'cpu_percent_mean':100,'throttled_period_fraction':0}}}
  def helper(op):
   self.assertEqual(op,'phase');return dict(token='fixture',channels=500,stored_programmes=2250,first=now.isoformat(),last=(now+datetime.timedelta(days=1)).isoformat(),current_until=(now+datetime.timedelta(hours=2)).isoformat(),fixture_sha256='fixture',schema_sha256='schema')
  def start(backend):starts.append(backend);return'http://127.0.0.1:1'
  def paced(_base,spec,_token,expected,*,users,period,duration,sample=None):
   self.assertEqual(spec,current);self.assertEqual(expected,reference['current']);return {'summary':dict(completed=math.ceil(duration/period)*users,errors=0,late_completed=0,ttfb_ms_p95=1,roundtrip_ms_p95=2),'resource_samples':[{'memory_bytes':300,'containers':[{'id':'c'*12,'memory_bytes':100}]}for _ in range(60)],'sample_errors':[]}
  with tempfile.TemporaryDirectory()as folder,patch('orchestrator.observer.Observer',Observer),patch('orchestrator.load.paced',side_effect=paced),patch('orchestrator.load.request')as raw,patch('orchestrator.load.cold_waves')as cold,patch('grid_qualification.authorize',return_value='qualification-cue'),patch('grid_plan.authorize_quiet',return_value='matrix-cue'):
   root=Path(folder);q=root/'qualification';q.mkdir();m=root/'matrix';m.mkdir();shared=dict(manifest=manifest,tools='.',seed=seed,reference=reference,current=current,xml=None,helper=helper,cache=NoXmlCache(),start=start,stop=lambda:stops.append(True),select_services=lambda _:selected,quiet_path=root/'cue',plan_module=grid_plan,qualification_module=grid_qualification,acceptance_module=grid_acceptance)
   qualified=orchestrator.run(action='qualify',output=q,**shared);self.assertTrue(qualified['passed']);self.assertEqual(starts,['source']*8)
   sealed=json.loads((q/'frozen-plan.json').read_text());self.assertEqual(set(sealed['selected_periods_seconds'].values()),{1})
   measured=orchestrator.run(action='matrix',output=m,plan_path=q/'frozen-plan.json',qualification_path=q/'source-qualification.json',**shared);self.assertTrue(measured['passed']);self.assertEqual(len(measured['acceptance']['pairs']),6);self.assertEqual(len(measured['trials']),12);self.assertEqual(len(starts),20);self.assertEqual(len(stops),20);raw.assert_not_called();cold.assert_not_called()
if __name__=='__main__':unittest.main()
