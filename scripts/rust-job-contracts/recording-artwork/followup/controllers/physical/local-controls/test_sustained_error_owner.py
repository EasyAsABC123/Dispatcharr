"""Corrupt retained-style synthetic samples; no processes or fixture evidence."""
import copy
import unittest
from test_physical_contract import model, c, REF


class SustainedErrorOwner(unittest.TestCase):
    def reject(self, mutate):
        report = model('update-error-Ready')
        mutate(report['cases'][2])
        with self.assertRaises((ValueError, KeyError, TypeError)):
            c.validate(0, report, REF)

    def test_complete_two_actual_style_samples(self):
        self.assertTrue(c.validate(0, model('update-error-Ready'), REF)['passed'])

    def test_missing_samples(self):
        self.reject(lambda case: case.pop('post_error_held_samples'))

    def test_one_sample(self):
        self.reject(lambda case: case['post_error_held_samples'].pop())

    def test_nonfinite_sample(self):
        self.reject(lambda case: case['post_error_held_samples'][0].update(sampled_monotonic=float('nan')))

    def test_infinite_gate(self):
        self.reject(lambda case: case['proxy_close_receipts']['PG']['timeline'][0].update(monotonic=float('inf')))

    def test_boolean_clock(self):
        self.reject(lambda case: case['post_error_held_samples'][0].update(sampled_monotonic=True))

    def test_boolean_zero_slot(self):
        self.reject(lambda case: case['post_error_held_samples'][0]['response'].update(available_slots=False))

    def test_wrong_actual_backend(self):
        self.reject(lambda case: case['post_error_held_samples'][1].update(gate_backend_pid=124))

    def test_wrong_connection(self):
        self.reject(lambda case: case['post_error_held_samples'][1].update(gate_connection=2))

    def test_wrong_peer_PID(self):
        self.reject(lambda case: case['post_error_held_samples'][1].update(peer_pid=457))

    def test_wrong_peer_starttime(self):
        self.reject(lambda case: case['post_error_held_samples'][1].update(peer_starttime='999999'))

    def test_short_interval_even_with_matching_response(self):
        def change(case):
            case['post_error_held_samples'][1]['sampled_monotonic'] = 102.3
            case['responses'][1]['monotonic'] = 102.3
            case['observations'][-1]['monotonic'] = 102.31
        self.reject(change)

    def test_before_existing_two_point_two_second_schedule(self):
        self.reject(lambda case: case['post_error_held_samples'][0].update(sampled_monotonic=100.1))

    def test_after_release(self):
        self.reject(lambda case: case['post_error_held_samples'][1].update(sampled_monotonic=107.0))

    def test_weak_owner_already_released(self):
        self.reject(lambda case: case['post_error_held_samples'][1]['response'].update(caller_released=True))

    def test_caller_finished_early(self):
        self.reject(lambda case: case['post_error_held_samples'][1]['response'].update(caller_pending=False))

    def test_no_actual_response_receipt(self):
        self.reject(lambda case: case['responses'].pop(1))

    def test_missing_explicit_null_backend(self):
        self.reject(lambda case: case['post_error_held_samples'][1].pop('backend_raw_base64'))

    def test_metadata_not_absent(self):
        self.reject(lambda case: case['post_error_held_samples'][1].update(backend_raw_base64='e30='))

    def test_wrong_actual_snapshot(self):
        self.reject(lambda case: case['observations'][-1]['backend'].update(raw_base64='e30='))


if __name__ == '__main__':
    unittest.main()
