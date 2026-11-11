"""Corruption controls for the actual published CLI observations."""
import copy
import json
from pathlib import Path
import unittest
import verify

class Controls(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        here = Path(__file__).resolve().parent
        cls.source = json.loads((here / 'source-createsuperuser-4.json').read_text())
        cls.native = json.loads((here / 'native-createsuperuser-3.json').read_text())
    def setUp(self):
        self.value = copy.deepcopy(self.native)
    def reject(self):
        with self.assertRaises((ValueError, KeyError)):
            verify.verify(self.source, self.value)
    def test_complete(self):
        self.assertEqual(verify.verify(self.source, self.value)['strict_typed_comparisons'], 154)
    def test_boolean_integer_substitution(self):
        self.value['cases'][5]['after']['users'][0]['is_staff'] = 1
        self.reject()
    def test_missing_row(self):
        self.value['cases'][5]['after']['users'].pop()
        self.reject()
    def test_changed_sequence(self):
        self.value['cases'][0]['after']['user_sequence'][0] += 1
        self.reject()
    def test_wrong_exit(self):
        self.value['cases'][0]['exit_code'] = 1
        self.reject()
    def test_outside_clock(self):
        self.value['cases'][5]['after']['users'][0]['date_joined'] = '2000-01-01 00:00:00+00:00'
        self.reject()
    def test_changed_clock_later(self):
        self.value['cases'][6]['before']['users'][0]['date_joined'] = '2000-01-01 00:00:00+00:00'
        self.reject()
    def test_hidden_failed_check(self):
        self.value['checks'][0]['passed'] = False
        self.reject()
    def test_missing_case(self):
        self.value['cases'].pop()
        self.reject()
    def test_duplicate_case(self):
        self.value['cases'][1] = copy.deepcopy(self.value['cases'][0])
        self.reject()
    def test_transcript_hash(self):
        self.value['cases'][0]['stdout'] += 'changed'
        self.reject()
    def test_scope(self):
        self.value['performance_verified'] = True
        self.reject()

if __name__ == '__main__':
    unittest.main()
