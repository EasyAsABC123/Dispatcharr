"""Unexecuted wire-state negatives, no socket/DB/app needed."""
import hashlib
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"tools"))
import unittest
from pg_preflight_gate import Gate

SQL = b'SELECT EXISTS(SELECT 1 FROM owned_preflight_guard)'
ROW = b'\x00\x01\x00\x00\x00\x01f'

def prepared(gate, sql=SQL):
    gate.frontend(b'P', b'q\0' + sql + b'\0\x00\x00')
    gate.frontend(b'B', b'\0q\0\x00\x00')

def execute(gate):
    gate.frontend(b'E', b'\0\x00\x00\x00\x00')

class Controls(unittest.TestCase):
    def gate(self):return Gate(hashlib.sha256(SQL).hexdigest())
    def test_real_target_shape_requires_data_command_and_idle_ready_once(self):
        gate=self.gate();prepared(gate);execute(gate)
        self.assertFalse(gate.backend(b'D',ROW));self.assertFalse(gate.backend(b'C',b'SELECT 1\0'))
        self.assertTrue(gate.backend(b'Z',b'I'));self.assertFalse(gate.backend(b'Z',b'I'))
    def test_earlier_prepare_describe_sync_ready_not_held(self):
        gate=self.gate();prepared(gate)
        self.assertFalse(gate.backend(b'Z',b'I'));execute(gate)
        gate.backend(b'D',ROW);gate.backend(b'C',b'SELECT 1\0')
        self.assertTrue(gate.backend(b'Z',b'I'))
    def test_unrelated_query_matching_row_not_held(self):
        gate=self.gate();prepared(gate,b'SELECT false');execute(gate)
        gate.backend(b'D',ROW);gate.backend(b'C',b'SELECT 1\0')
        self.assertFalse(gate.backend(b'Z',b'I'))
    def test_missing_duplicate_true_or_wrong_command_cannot_hold(self):
        for variant in ('no-row','two-rows','true','wrong-complete','transaction-ready'):
            gate=self.gate();prepared(gate);execute(gate)
            if variant!='no-row':gate.backend(b'D',ROW if variant!='true' else ROW[:-1]+b't')
            if variant=='two-rows':gate.backend(b'D',ROW)
            gate.backend(b'C',b'UPDATE 1\0' if variant=='wrong-complete' else b'SELECT 1\0')
            self.assertFalse(gate.backend(b'Z',b'T' if variant=='transaction-ready' else b'I'))
    def test_reused_statement_name_invalidates_old_target(self):
        gate=self.gate();prepared(gate);prepared(gate,b'SELECT false');execute(gate)
        gate.backend(b'D',ROW);gate.backend(b'C',b'SELECT 1\0')
        self.assertFalse(gate.backend(b'Z',b'I'))
    def test_error_cannot_masquerade_as_successful_guard(self):
        gate=self.gate();prepared(gate);execute(gate)
        with self.assertRaisesRegex(ValueError,'UnexpectedPreflightErrorResponse'):gate.backend(b'E',b'error')
    def test_no_credential_or_bind_value_is_retained_in_events(self):
        gate=self.gate();prepared(gate)
        gate.frontend(b'B',b'\0q\0private-synthetic-Bind-value')
        execute(gate);gate.backend(b'D',ROW);gate.backend(b'C',b'SELECT 1\0');gate.backend(b'Z',b'I')
        self.assertNotIn('private-synthetic',str(gate.__dict__['events']))

if __name__ == '__main__':unittest.main()
