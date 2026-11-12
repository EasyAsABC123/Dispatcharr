"""Public-only register reproduction and nineteen evidence rejection controls.
Original Dispatcharr authors retain application/algorithm credit and AGPL.
Set DISPATCHARR_CONTRACT_DEEP_EVIDENCE=1 for the optional retained-raw check.
"""
import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import tempfile
import unittest

# These portable Source-input classes are included in the existing SDK module.
from test_source_contracts import SourceContracts
from test_framework_auth_contracts import Controls as FrameworkAuthControls
from test_account_settings_contracts import AccountSettingsControls
from test_channel_guide_contracts import ChannelGuideControls
from test_dvr_import_contracts import DvrImportControls
from test_provider_output_contracts import ProviderOutputControls
from test_admin_rest_contracts import AdminRestControls
from test_proxy_plugin_contracts import ProxyPluginControls
from test_notification_source_contracts import NotificationSourceControls
from test_scanner_task_contracts import ScannerTaskControls

REPO = Path(__file__).resolve().parents[2]


class CapturedEvidenceRegister(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.bindings = json.loads((REPO / 'scripts/rust-contracts/case-bindings.json').read_text())
        refs = {'docs/rust-runtime-contracts.json', 'docs/rust-runtime-methods.json', 'docs/rust-contract-inventory.json'}
        refs.update({'scripts/rust-contracts/executable-source-inputs.json', 'scripts/rust-contracts/executable-source-bindings.json'})
        refs.update({'scripts/rust-contracts/framework-auth/' + name for name in ['bindings.json', 'full-suite-reference.json', 'observations.json', 'original/test_accounts.py', 'original/test_login_rate_limit_inbox.py']})
        account_input = json.loads((REPO / 'scripts/rust-contracts/accounts-settings/bindings.json').read_text())
        refs.add('docs/rust-contract-register.json')
        refs.add('scripts/rust-contracts/accounts-settings/bindings.json')
        refs.update(x['path'] for x in account_input['authorities'])
        refs.update(x['path'] for x in account_input['original_test_files'])
        refs.update(item['packaged_report'] for item in self.bindings['bindings'])
        refs.update(item['attestation']['path'] for item in self.bindings.get('evidence_datasets', []))
        import scanner_task_contracts
        refs.update(scanner_task_contracts.dependencies())
        import channel_guide_contracts
        refs.update(channel_guide_contracts.dependencies())
        import dvr_import_contracts
        refs.update(dvr_import_contracts.dependencies())
        import provider_output_contracts
        refs.update(provider_output_contracts.dependencies())
        import admin_rest_contracts
        refs.update(admin_rest_contracts.dependencies())
        import proxy_plugin_contracts
        refs.update(proxy_plugin_contracts.dependencies())
        import notification_source_contracts
        refs.update(notification_source_contracts.dependencies())
        for relative in refs:
            target = self.root / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(REPO / relative, target)
        spec = importlib.util.spec_from_file_location('register_evidence_test', REPO / 'scripts/rust-contracts/register.py')
        self.module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.module)
        self.module.ROOT = self.root
        self.baseline = json.loads((REPO / 'docs/rust-contract-register.json').read_text())

    def build(self, value, deep=False):
        path = self.root / 'scripts/rust-contracts/case-bindings.json'
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value, ensure_ascii=False))
        return self.module.build(path, deep_evidence=deep)

    def rejected(self, change):
        value = copy.deepcopy(self.bindings)
        change(value)
        with self.assertRaises((AssertionError, KeyError, ValueError, IndexError)):
            self.build(value)

    def corrupt_attestation(self, value, mutation):
        reference = value['evidence_datasets'][0]['attestation']
        path = self.root / reference['path']
        original = path.read_bytes()
        self.addCleanup(path.write_bytes, original)
        data = json.loads(original)
        mutation(data)
        projection = self.module.digest(data['case_projection'])
        data['validation_authority']['case_projection_sha256'] = projection
        path.write_text(json.dumps(data, ensure_ascii=False))
        for dataset in value['evidence_datasets']:
            dataset['attestation']['sha256'] = hashlib.sha256(path.read_bytes()).hexdigest()
            dataset['attestation']['case_projection_sha256'] = projection

    def test_public_only_reproduction_and_nineteen_negative_controls(self):
        actual = self.build(self.bindings)
        self.assertFalse((self.root / 'artifacts').exists())
        self.assertFalse(any(path.is_symlink() for path in self.root.rglob('*')))
        self.assertEqual([r['route_id'] for r in actual['runtime_routes']], [r['route_id'] for r in self.baseline['runtime_routes']])
        for old, new in zip(self.baseline['runtime_routes'], actual['runtime_routes']):
            self.assertEqual(old['resolver_ordinal'], new['resolver_ordinal'])
            self.assertEqual([(o['contract_id'], o['method'], o['source_action']) for o in old['operations']], [(o['contract_id'], o['method'], o['source_action']) for o in new['operations']])
        self.assertEqual(actual['openapi_operations'], self.baseline['openapi_operations'])
        self.assertEqual(actual['non_HTTP_candidates'], self.baseline['non_HTTP_candidates'])
        counts = actual['counts']
        self.assertEqual((counts['runtime_routes'], counts['runtime_operation_work_items'], counts['operations_with_partial_case_bindings'], counts['mapped_packaged_case_observations'], counts['fully_verified_route_operations']), (592, 798, 11, 403, 0))
        self.assertEqual(len(actual['observations_for_methods_not_declared_in_discovery']), 265)
        first = lambda v: v['bindings'][2]
        dataset = lambda v: v['evidence_datasets'][0]

        def alias(value, fragment):
            binding = next(b for b in value['bindings'] if b.get('evidence_dataset_id') == 'images240' and b['source_handler'].endswith('MovieViewSet'))
            route = next(r for r in self.baseline['runtime_routes'] if fragment in r['pattern_exact'] and r['source_handler'] == binding['source_handler'])
            binding.update(contract_id=route['route_id'] + ':GET', pattern_exact=route['pattern_exact'])

        controls = [
            ('image_id_substitution', lambda v: dataset(v)['runtime'].__setitem__('image_id', 'sha256:' + '0' * 64)),
            ('tree_substitution', lambda v: dataset(v)['runtime'].__setitem__('source_tree_sha256', '0' * 64)),
            ('manifest_hash_substitution', lambda v: dataset(v)['runtime'].__setitem__('manifest_sha256', '0' * 64)),
            ('case_cardinality_substitution', lambda v: dataset(v).__setitem__('case_count', 151)),
            ('method_cardinality_substitution', lambda v: dataset(v)['method_counts'].__setitem__('HEAD', 13)),
            ('wrong_role', lambda v: first(v)['cases'][0].__setitem__('role', 'uncaptured-role')),
            ('wrong_body_hash', lambda v: first(v)['cases'][0].__setitem__('body_sha256', '0' * 64)),
            ('wrong_source_record', lambda v: first(v)['cases'][0].__setitem__('source_index', 149)),
            ('HEAD_promoted_into_GET', lambda v: first(v)['cases'].__setitem__(0, copy.deepcopy(next(o['case'] for o in v['observations_for_methods_not_declared_in_discovery'] if o.get('evidence_dataset_id') == 'provider150' and o['method'] == 'HEAD')))),
            ('duplicate_case', lambda v: first(v)['cases'].append(copy.deepcopy(first(v)['cases'][0]))),
            ('missing_case', lambda v: first(v)['cases'].pop()),
            ('endpoint_completion_from_partial_cases', lambda v: first(v).__setitem__('fully_verified', True)),
            ('no_slash_alias_substitution', lambda v: alias(v, 'movies/<int:pk>/image')),
            ('format_alias_substitution', lambda v: alias(v, '/image\\.')),
            ('undeclared_method_operation_invention', lambda v: next(o for o in v['observations_for_methods_not_declared_in_discovery'] if o.get('evidence_dataset_id')).__setitem__('contract_id', 'invented:HEAD')),
            ('duplicate_dataset', lambda v: v['evidence_datasets'].append(copy.deepcopy(v['evidence_datasets'][0]))),
            ('native_status_mismatch', lambda v: self.corrupt_attestation(v, lambda a: a['case_projection'][0]['candidate_response'].__setitem__('status', 418))),
            ('fresh_authority_not_same_state', lambda v: self.corrupt_attestation(v, lambda a: next(x for x in a['case_projection'] if x['source_authority'] == 'predeclared_fresh_same_state').__setitem__('fresh_no_intervening_state_change', False))),
        ]
        self.assertEqual(len(controls), 18)
        for name, change in controls:
            with self.subTest(control=name):
                self.rejected(change)
                # Each control begins from the original committed public bytes.
                for item in self.bindings['evidence_datasets']:
                    path = item['attestation']['path']
                    shutil.copyfile(REPO / path, self.root / path)
        with self.subTest(control='deep_mode_requires_missing_private_artifacts'):
            with self.assertRaises(FileNotFoundError):
                self.build(self.bindings, deep=True)

    @unittest.skipUnless(os.environ.get('DISPATCHARR_CONTRACT_DEEP_EVIDENCE') == '1', 'optional retained private raw evidence')
    def test_optional_private_raw_revalidation(self):
        self.module.ROOT = REPO
        result = self.module.build(REPO / 'scripts/rust-contracts/case-bindings.json', deep_evidence=True)
        self.assertEqual(result['counts']['fully_verified_route_operations'], 0)
        self.assertEqual(result['counts']['mapped_packaged_case_observations'], 403)


if __name__ == '__main__':
    unittest.main()
