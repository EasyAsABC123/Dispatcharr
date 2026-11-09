"""Catch-up evidence must reject omitted/changed/malformed statistics."""
import copy,unittest
from timeshift_ws_probe import compare
class Evidence(unittest.TestCase):
    def setUp(self):
        stats={'timestamp':1.0,'total_connections':1,'timeshift_sessions':[{'session_id':'<session-1>','stats_channel_id':'1_<session-1>','channel_id':1,'channel_name':'Fixture','position_anchor_at':1.0,'connections':[{'connected_at':1.0,'duration':0,'bytes_streamed':0,'avg_bitrate_kbps':0.0,'client_id':'<session-1>'}]}]}
        self.source={'capture_complete':True,'events':[{'type':'update','data':{'success':True,'type':'timeshift_stats','stats':stats}},{'type':'update','data':{'success':True,'type':'timeshift_stats','stats':{'timestamp':2.0,'total_connections':0,'timeshift_sessions':[]}}}]}
    def test_exact_passes(self):self.assertTrue(compare(self.source,copy.deepcopy(self.source))['captured_packaged_timeshift_events_match'])
    def test_missing_snapshot_and_changed_full_dto_fail(self):
        for field in ['missing','name','count','bytes','rate']:
            candidate=copy.deepcopy(self.source)
            if field=='missing':candidate['events'].pop()
            else:
                stats=candidate['events'][0]['data']['stats']
                if field=='name':stats['timeshift_sessions'][0]['channel_name']='Changed'
                if field=='count':stats['total_connections']=0
                if field=='bytes':stats['timeshift_sessions'][0]['connections'][0]['bytes_streamed']=-1
                if field=='rate':stats['timeshift_sessions'][0]['connections'][0]['avg_bitrate_kbps']=float('nan')
            self.assertFalse(compare(self.source,candidate)['captured_packaged_timeshift_events_match'])
