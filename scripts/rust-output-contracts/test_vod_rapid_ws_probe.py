"""Rapid timer evidence must retain duplicate-event and physical cleanup checks."""
import copy
import json
import unittest
from vod_rapid_ws_probe import compare


class RapidEvidence(unittest.TestCase):
    def setUp(self):
        def event(kind, **fields):
            return {'type': 'update', 'data': {'type': kind, **fields}}
        stats = {'vod_connections': [{'content_name': 'Fixture', 'connections': [
            {'client_id': 'owned', 'bytes_sent': 0, 'connected_at': '1',
             'last_activity': '1.5', 'duration': 0}]}], 'total_connections': 1, 'timestamp': 2.0}
        empty = {'vod_connections': [], 'total_connections': 0, 'timestamp': 3.0}
        self.source = {'capture_complete': True, 'reclose_elapsed_seconds': .05,
            'events': [event('vod_started', content_uuid='fixture'),
                       event('vod_stats', stats=json.dumps(stats)),
                       event('vod_stopped', content_uuid='fixture'),
                       event('vod_stopped', content_uuid='fixture'),
                       event('vod_stats', stats=json.dumps(empty)),
                       event('vod_stats', stats=json.dumps(empty))],
            'final': {'remaining_metadata': False, 'provider_connections': 0,
                      'audit': [{'event_type': kind, 'details': {'content_name': 'Fixture'}}
                                for kind in ['vod_start', 'vod_stop', 'vod_stop']]}}

    def passes(self, candidate):
        return compare(self.source, candidate)['captured_packaged_rapid_reclose_match']

    def test_independent_stop_statistics_interleaving_is_permitted(self):
        self.assertTrue(self.passes(copy.deepcopy(self.source)))
        candidate = copy.deepcopy(self.source)
        candidate['events'][3], candidate['events'][4] = candidate['events'][4], candidate['events'][3]
        self.assertTrue(self.passes(candidate))

    def test_missing_or_extra_stop_and_changed_payload_are_rejected(self):
        for mode in ['missing', 'extra', 'payload']:
            candidate = copy.deepcopy(self.source)
            if mode == 'missing':
                candidate['events'].pop(3)
            elif mode == 'extra':
                candidate['events'].append(copy.deepcopy(candidate['events'][3]))
            else:
                candidate['events'][3]['data']['content_uuid'] = 'changed'
            self.assertFalse(self.passes(candidate))

    def test_wrong_stats_or_nonfinite_time_are_rejected(self):
        for mode in ['empty', 'stable', 'time', 'progress']:
            candidate = copy.deepcopy(self.source)
            stats = json.loads(candidate['events'][1]['data']['stats'])
            if mode == 'empty':
                stats.update(vod_connections=[], total_connections=0)
            elif mode == 'stable':
                stats['vod_connections'][0]['content_name'] = 'changed'
            elif mode == 'time':
                stats['timestamp'] = float('nan')
            else:
                stats['vod_connections'][0]['connections'][0]['bytes_sent'] = 123
            candidate['events'][1]['data']['stats'] = json.dumps(stats)
            self.assertFalse(self.passes(candidate))

    def test_audit_loss_or_metadata_provider_leak_or_missed_grace_are_rejected(self):
        for mode in ['audit', 'metadata', 'provider', 'grace']:
            candidate = copy.deepcopy(self.source)
            if mode == 'audit':
                candidate['final']['audit'].pop()
            elif mode == 'metadata':
                candidate['final']['remaining_metadata'] = True
            elif mode == 'provider':
                candidate['final']['provider_connections'] = 1
            else:
                candidate['reclose_elapsed_seconds'] = 1.01
            self.assertFalse(self.passes(candidate))
