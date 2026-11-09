"""Acceptance must reject incomplete or falsely cleaned-up admin controls."""
import copy
import json
from pathlib import Path
import unittest
from management_probe import compare,playback_complete


class ManagementAcceptance(unittest.TestCase):
    def setUp(self):
        self.source=json.loads((Path(__file__).parent/'python-management-goldens.json').read_text())

    def test_source_controls_and_allowed_method_permutation(self):
        self.assertTrue(playback_complete(self.source['active_and_idle_playback']))
        candidate=copy.deepcopy(self.source)
        for event in candidate['events']:
            if 'allow' in event['headers']:event['headers']['allow']=', '.join(reversed(event['headers']['allow'].split(', ')))
        self.assertTrue(compare(self.source,candidate)['captured_management_matches'])

    def test_raw_grace_lifecycle_is_compared(self):
        candidate=copy.deepcopy(self.source)
        candidate['active_and_idle_playback'][0]['immediate_after_stop']['grace_only_client_count']=0
        self.assertFalse(compare(self.source,candidate)['captured_management_matches'])

    def test_cleanup_or_cancellation_failure_cannot_complete_capture(self):
        for key,value in [('closed_after_stop',False),('stopped_before_complete_media',False)]:
            candidate=copy.deepcopy(self.source['active_and_idle_playback']);candidate[0][key]=value
            self.assertFalse(playback_complete(candidate))
        candidate=copy.deepcopy(self.source['active_and_idle_playback']);candidate[1]['after_grace']['profile_connections']=1
        self.assertFalse(playback_complete(candidate))

    def test_clock_and_control_mutation_are_not_ignored(self):
        for mutation in ['clock','body','cleanup']:
            candidate=copy.deepcopy(self.source)
            if mutation=='clock':candidate['events'][6]['timestamp_within_request']=False
            if mutation=='body':candidate['events'][6]['body']['total_connections']=1
            if mutation=='cleanup':candidate['synthetic_stop_effects']['unrelated']=False
            self.assertFalse(compare(self.source,candidate)['captured_management_matches'])
