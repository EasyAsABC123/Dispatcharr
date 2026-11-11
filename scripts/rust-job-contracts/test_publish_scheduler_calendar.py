"""Evidence publication fails closed on failed, mismatched or incomplete proof."""
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location('calendar_publisher', Path(__file__).with_name('publish_scheduler_calendar.py'))
PUBLISHER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PUBLISHER)


class CalendarPublication(unittest.TestCase):
    def invoke(self, mutation=None):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            argv = ['publish']
            reports = {}
            for label, suffix, count in [
                ('vectors', 'calendar-vectors', 123), ('database', 'scheduler-calendar', 157),
                ('workers', 'scheduler-calendar-worker', 54), ('single', 'scheduler-tick', 108),
                ('multi', 'scheduler-multi', 184), ('physical', 'scheduler-database', 47),
            ]:
                target = root / label
                target.mkdir()
                integration = dict(passed=True, default_database_unchanged=True, cleanup='project removed',
                                   rust_source_sha256={'rust/source.rs': 'same'}, rust_source_archive_sha256='archive',
                                   binary_sha256='binary', original_revision='original', original_archive_sha256='source',
                                   tool_source_sha256={})
                private = dict(passed=True, checks=[{'name': str(i), 'passed': True} for i in range(count)])
                if label == 'physical':
                    private['physical_resource_tests'] = 6
                    (target / 'native-scheduler-physical.log').write_text('retained')
                if label == 'workers':
                    private.update(counts=[1, 1], results=[dict(status='SUCCESS', result=value, children=[], traceback=None)
                                                          for value in [None, {'scheduled': 0, 'details': []}]])
                if label == 'vectors':
                    private.update(tzdb={'version': '2026e'}, vectors=[])
                if label == 'database':
                    private['cases'] = []
                reports[label] = integration, private, target, suffix
                argv.extend(['--' + label, str(target)])
            if mutation:
                mutation(reports)
            for integration, private, target, suffix in reports.values():
                (target / 'integration.json').write_text(json.dumps(integration))
                (target / ('source-native-' + suffix + '-private.json')).write_text(json.dumps(private))
            output = root / 'public.json'
            argv.extend(['--output', str(output)])
            with patch.object(sys, 'argv', argv), contextlib.redirect_stdout(io.StringIO()):
                PUBLISHER.main()
            return json.loads(output.read_text())

    def test_complete_same_source_accepts(self):
        self.assertTrue(self.invoke()['same_exact_source_and_tests_between_proofs'])

    def test_different_runtime_rejected(self):
        with self.assertRaises(AssertionError):
            self.invoke(lambda reports: reports['workers'][0]['rust_source_sha256'].update(other='changed'))

    def test_failed_check_rejected(self):
        with self.assertRaises(AssertionError):
            self.invoke(lambda reports: reports['database'][1]['checks'][0].update(passed=False))

    def test_missing_calendar_physical_case_rejected(self):
        with self.assertRaises(AssertionError):
            self.invoke(lambda reports: reports['physical'][1].update(physical_resource_tests=5))


if __name__ == '__main__':
    unittest.main()
