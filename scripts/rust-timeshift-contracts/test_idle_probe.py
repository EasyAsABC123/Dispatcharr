import copy
import json
from pathlib import Path
import unittest
from idle_probe import compare


class IdleComparison(unittest.TestCase):
    def test_preserves_lifecycle_bytes_and_timestamp_relationships(self):
        source=json.loads((Path(__file__).parent/'python-idle-reopen-goldens.json').read_text())
        candidate=copy.deepcopy(source)
        # Different wall-clock start is allowed only when its preservation and
        # ordered reanchor relationships still agree with the original.
        for event in candidate['events']:
            for record in event['state']['clients']:
                record['connected_at']=str(float(record['connected_at'])+1000)
                record['position_anchor_at']=str(float(record['position_anchor_at'])+1000)
            for record in event['state']['metadata']:
                record['init_time']=str(float(record['init_time'])+1000)
        self.assertTrue(compare(source,candidate)['captured_idle_reopen_matches'])
        for field in ['payload','connected','bytes','seek','capacity','grace','origin']:
            modified=copy.deepcopy(source)
            if field=='payload':modified['events'][1]['sha256']='wrong'
            if field=='connected':modified['events'][1]['state']['clients'][0]['connected_at']='0'
            if field=='bytes':modified['events'][2]['state']['metadata'][0]['total_bytes']='256'
            if field=='seek':modified['events'][2]['state']['clients'][0]['playback_base_secs']='0'
            if field=='capacity':modified['events'][1]['state']['profile_connections']=1
            if field=='grace':modified['after_grace']['pool_exists']=False
            if field=='origin':modified['origin']['requests'][1]['shape']='query'
            self.assertFalse(compare(source,modified)['captured_idle_reopen_matches'],field)


if __name__=='__main__':
    unittest.main()
