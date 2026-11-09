"""Packaged event evidence must reject missing/fake/stale media state."""
import copy,json,unittest
from vod_ws_probe import compare
class Evidence(unittest.TestCase):
    def setUp(self):
        stats={'vod_connections':[{'content_name':'Fixture','connections':[{'client_id':'fixture','bytes_sent':0,'connected_at':'1.0','last_activity':'1.5','duration':0}]}],'total_connections':1,'timestamp':2.0}
        self.source={'capture_complete':True,'cases':[{'case':'fixture'}],'events':[{'type':'update','data':{'type':'vod_started','content_uuid':'fixture'}},{'type':'update','data':{'type':'vod_stats','stats':json.dumps(stats)}},{'type':'update','data':{'type':'vod_stopped','content_uuid':'fixture'}},{'type':'update','data':{'type':'vod_stats','stats':json.dumps({'vod_connections':[],'total_connections':0,'timestamp':3.0})}}]}
    def test_exact_passes(self):self.assertTrue(compare(self.source,copy.deepcopy(self.source))['captured_packaged_vod_events_match'])
    def test_missing_and_extra_events_fail(self):
        for extra in [False,True]:
            candidate=copy.deepcopy(self.source)
            if extra:candidate['events'].append(copy.deepcopy(candidate['events'][0]))
            else:candidate['events'].pop()
            self.assertFalse(compare(self.source,candidate)['captured_packaged_vod_events_match'])
    def test_fake_empty_start_and_wrong_stable_dto_and_bad_progress_fail(self):
        for mode in ['empty','stable','progress','timestamp']:
            candidate=copy.deepcopy(self.source);value=json.loads(candidate['events'][1]['data']['stats'])
            if mode=='empty':value.update(vod_connections=[],total_connections=0)
            if mode=='stable':value['vod_connections'][0]['content_name']='Changed'
            if mode=='progress':value['vod_connections'][0]['connections'][0]['bytes_sent']=1234
            if mode=='timestamp':value['vod_connections'][0]['connections'][0]['connected_at']='NaN'
            candidate['events'][1]['data']['stats']=json.dumps(value)
            self.assertFalse(compare(self.source,candidate)['captured_packaged_vod_events_match'])
