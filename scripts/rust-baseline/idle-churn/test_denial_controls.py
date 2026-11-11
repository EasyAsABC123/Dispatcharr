"""Captured EACCES is incomplete coverage; never an accepted or fatal sample."""
import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import types
import unittest
from unittest import mock
from test_controls import controller, samples, POLICY

spec = importlib.util.spec_from_file_location('m0_denial_census', Path(__file__).with_name('proc_snapshot.py'))
census = importlib.util.module_from_spec(spec)
spec.loader.exec_module(census)

class DenialControls(unittest.TestCase):
    def collect(self, denied, processes=(754, 900), inventory=False):
        def link(path):
            if str(path).split('/')[2] in {str(x) for x in denied}:
                raise PermissionError(13, 'Permission denied', str(path))
            return 'socket:[1]'
        def path(*parts):
            return types.SimpleNamespace(iterdir=lambda: [Path('/proc', parts[1], 'fd/0')])
        with mock.patch.object(census, 'processes', return_value=list(processes)), \
             mock.patch.object(census, 'identity', return_value=(0, 0, 1)), \
             mock.patch.object(census, 'Path', side_effect=path), \
             mock.patch.object(census.os, 'geteuid', return_value=0), \
             mock.patch.object(census.os, 'getegid', return_value=0), \
             mock.patch.object(census.os, 'readlink', side_effect=link):
            return census.collect(inventory)

    def test_captured_754_fd_denied_does_not_abort_other_pid(self):
        value = self.collect({754})
        self.assertEqual(value['process_ids'], [900])
        self.assertEqual(value['denied_process_ids'], [754])
        self.assertEqual(value['permission_denied_descriptors'], 1)
        self.assertEqual(value['permission_denied_processes'], 1)
        self.assertEqual(value['fds'], 1)
        self.assertFalse(value['coverage_complete'])

    def test_all_denied_never_reports_processes_complete(self):
        value = self.collect({754, 900})
        self.assertEqual(value['processes'], 0)
        self.assertEqual(value['fds'], 0)
        self.assertEqual(value['permission_denied_processes'], 2)
        self.assertFalse(value['coverage_complete'])

    def test_denied_identity_inventory_is_incomplete(self):
        with mock.patch.object(census, 'processes', return_value=[754]), \
             mock.patch.object(census, 'identity', side_effect=PermissionError(13, 'denied')), \
             mock.patch.object(census, 'tcp_states', return_value={}):
            value = census.collect(True)
        self.assertEqual(value['identities'], [])
        self.assertEqual(value['denied_process_ids'], [754])
        self.assertFalse(value['coverage_complete'])

    def test_merge_partial_denial_cannot_be_complete(self):
        part = self.collect({754})
        inventory = dict(process_ids=[754, 900], raced_processes=0, tcp_states={})
        merged = controller.merge_census(inventory, [part])
        self.assertEqual(merged['permission_denied_descriptors'], 1)
        self.assertFalse(merged['complete'])

    def test_persistent_denials_fail_original_minimum_samples(self):
        rows = samples()
        for row in rows:
            row['census']['source'].update(permission_denied_descriptors=1, permission_denied_processes=1, complete=False)
        checks = controller.evaluate_samples(rows, POLICY)
        self.assertEqual([x['observed'] for x in checks if x['id'].endswith('/samples')], [0, 0, 0])
        self.assertFalse(all(x['passed'] for x in checks))

    def test_transient_denied_row_excluded_with_enough_complete_rows(self):
        rows = samples()
        extra = copy.deepcopy(rows[0])
        extra['census']['source'].update(permission_denied_processes=1, complete=False)
        rows.append(extra)
        self.assertFalse(controller.complete_census(extra))
        self.assertTrue(all(x['passed'] for x in controller.evaluate_samples(rows, POLICY)))

    def test_unrelated_os_error_remains_fatal(self):
        with mock.patch.object(census, 'processes', return_value=[754]), \
             mock.patch.object(census, 'identity', side_effect=OSError(5, 'I/O')):
            with self.assertRaises(OSError):
                census.collect()

    def test_unrelated_command_and_json_errors_still_stop_sampler(self):
        for output in [RuntimeError('FixtureCommand'), 'invalid JSON']:
            with self.subTest(output=type(output).__name__), tempfile.TemporaryDirectory() as tmp:
                observer = types.SimpleNamespace(sample=lambda: {})
                command = mock.Mock(side_effect=output) if isinstance(output, Exception) else mock.Mock(return_value=output)
                sampler = controller.Sampler(observer, {'baseline': {'id': 'test'}}, command, POLICY, Path(tmp)/'samples.jsonl')
                sampler.loop()
                self.assertEqual(len(sampler.errors), 1)
                self.assertIn(sampler.errors[0], ['RuntimeError', 'JSONDecodeError'])

if __name__ == '__main__':
    unittest.main()
