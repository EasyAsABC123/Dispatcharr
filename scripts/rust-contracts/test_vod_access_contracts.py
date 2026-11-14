"""Reject false route, role, fixture and historical-result claims."""
import copy
from pathlib import Path
import shutil
import tempfile
import unittest

import vod_access_contracts as module

ROOT = Path(__file__).resolve().parents[2]


class VODAccessControls(unittest.TestCase):
    def setUp(self):
        self.value = module.build(ROOT)

    def reject(self, mutate):
        value = copy.deepcopy(self.value)
        mutate(value)
        with self.assertRaises(AssertionError):
            module.validate(ROOT, value)

    def test_exact_four_historical_successes(self):
        self.assertEqual(module.validate(ROOT, self.value)['Original_named_successes'], 4)

    def test_format_alias_cannot_replace_ordinary_route(self):
        self.reject(lambda v: v['contracts'][0].update(pattern_exact='api/vod/^movies\\.json/$'))

    def test_wrong_operation_identity(self):
        self.reject(lambda v: v['contracts'][0].update(contract_id='route-not-observed:GET'))

    def test_wrong_method(self):
        self.reject(lambda v: v['contracts'][0].update(method='POST'))

    def test_wrong_action(self):
        self.reject(lambda v: v['contracts'][0].update(source_action='retrieve'))

    def test_no_anonymous_HTTP_role_from_queryset_test(self):
        self.reject(lambda v: v['contracts'][0].update(observed_roles=['anonymous']))

    def test_no_backend_authentication_claim(self):
        self.reject(lambda v: v['contracts'][0]['source_role_specs']['forced_standard'].update(authentication_backend_tested=True))

    def test_no_detail_positive_claim_from_list_workflow(self):
        self.reject(lambda v: v['contracts'][1]['source_test_methods'].append(
            'apps.vod.tests.test_vod_rest_access_flag.VodRestAccessFlagTests.test_absent_flags_keep_rest_access'))

    def test_no_native_or_TCP_acceptance(self):
        self.reject(lambda v: v['contracts'][0]['result'].update(Native_or_TCP_acceptance=True))

    def test_no_new_execution_claim(self):
        self.reject(lambda v: v['contracts'][0]['result'].update(new_Source_runs=1))

    def test_no_complete_roles(self):
        self.reject(lambda v: v['contracts'][0]['result'].update(complete_role_matrix=True))

    def test_no_removed_fixture_helpers(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in module.dependencies()[1:]:
                target = root / name
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(ROOT / name, target)
            original = root / module.BASE / 'original' / module.ORIGINAL
            original.write_text(original.read_text().replace('def _results(', 'def _removed_results('))
            with self.assertRaisesRegex(AssertionError, 'PinnedOriginalTests'):
                module.authority(root)

    def test_wrong_raw_suite_log_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'tests.log'
            path.write_text('invented ok\n')
            with self.assertRaisesRegex(AssertionError, 'ActualRawFullSuiteHash'):
                module.authority(ROOT, path)


if __name__ == '__main__':
    unittest.main()
