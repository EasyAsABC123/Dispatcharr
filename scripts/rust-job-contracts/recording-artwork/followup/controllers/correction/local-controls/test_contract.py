"""Pure validator fault injection over retained actual normal raw context.

Synthetic refusal responses exercise validator behavior, not Native execution.
"""
import base64
import copy
import json
from pathlib import Path
import unittest

import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
from refusal_contract import REASONS, raw_child_context, validate

RAW = Path(__file__).resolve().parents[1] / 'tools/reference.json'
QUEUE = 'rust-owned-parented-artwork'


class Controls(unittest.TestCase):
    def setUp(self):
        raw = json.loads(RAW.read_text())['Source']
        root, parent, normal = raw['cases'][:3]
        self.before = normal['before']
        self.context = raw_child_context(self.before, parent['tasks'][0], root['tasks'][0], QUEUE)
        self.after = copy.deepcopy(self.before)
        self.mode = 'missing-recording'
        self.response = {'ok': False, 'result': 'Err(Domain(Sql(Unsupported("recording snapshot"))))', 'typed_result': None, 'caller_released': True, 'available_slots': 1, 'began_utc': self.before['utc'], 'finished_utc': self.before['utc']}
        self.process = {'exit': 0, 'settled': True, 'pipes_settled': True, 'writer_settled': True, 'collector_invalid': None}

    def accept(self):
        return validate(self.mode, self.before, self.after, self.response, self.process, QUEUE, self.context)

    def test_actual_raw_parented_context(self):
        self.assertIn(self.context['frame_base64'], self.before['queues'][QUEUE])
        self.assertTrue(self.accept()['passed'])

    def test_each_declared_reason_is_distinct(self):
        for mode, reason in REASONS.items():
            self.mode = mode
            self.response['result'] = 'Err(Domain(Sql(Unsupported(' + json.dumps(reason) + '))))'
            self.assertTrue(self.accept()['passed'])

    def test_actual_frame_wrong_task_rejected(self):
        raw = json.loads(RAW.read_text())['Source']; root, parent, _ = raw['cases'][:3]
        frame = json.loads(base64.b64decode(self.before['queues'][QUEUE][-1]))
        frame['headers']['task'] = 'apps.channels.tasks.run_recording'
        self.before['queues'][QUEUE][-1] = base64.b64encode(json.dumps(frame).encode()).decode()
        with self.assertRaisesRegex(ValueError, 'ActualArtworkTask'):
            raw_child_context(self.before, parent['tasks'][0], root['tasks'][0], QUEUE)

    def test_actual_candidate_ACK_rejected(self):
        self.after['queues'][QUEUE].pop()
        with self.assertRaises(ValueError): self.accept()

    def test_other_child_removed_rejected(self):
        self.after['queues'][QUEUE].pop(0)
        with self.assertRaises(ValueError): self.accept()

    def test_SQL_effect_rejected(self):
        self.after['recordings'][0]['custom_properties']['poster_logo_id'] = 1
        with self.assertRaises(ValueError): self.accept()

    def test_bool_int_projection_rejected(self):
        self.after['recordings'][0]['custom_properties']['program']['id'] = True
        with self.assertRaises(ValueError): self.accept()

    def test_missing_null_backend_field_rejected(self):
        del self.after['backend'][self.context['task_id']]['metadata']
        with self.assertRaises(ValueError): self.accept()

    def test_new_metadata_rejected(self):
        self.after['backend'][self.context['task_id']]['raw_base64'] = 'e30='
        with self.assertRaises(ValueError): self.accept()

    def test_wrong_error_reason_rejected(self):
        self.response['result'] = 'Err(Transport(Corrupt))'
        with self.assertRaises(ValueError): self.accept()

    def test_success_not_refusal(self):
        self.response['ok'] = True
        with self.assertRaises(ValueError): self.accept()

    def test_unsettled_peer_rejected(self):
        self.process['writer_settled'] = False
        with self.assertRaises(ValueError): self.accept()

    def test_failed_peer_exit_rejected(self):
        self.process['exit'] = -15
        with self.assertRaises(ValueError): self.accept()

    def test_caller_or_slot_still_held_rejected(self):
        self.response['caller_released'] = False
        with self.assertRaises(ValueError): self.accept()

    def test_clock_outside_own_window_rejected(self):
        self.response['finished_utc'] = '2000-01-01T00:00:00+00:00'
        with self.assertRaises(ValueError): self.accept()


if __name__ == '__main__':
    unittest.main()
