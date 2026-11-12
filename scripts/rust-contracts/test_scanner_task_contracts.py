"""Portable corruption and typed-application controls; no Source processes."""
import copy
import json
from pathlib import Path
import tempfile
import shutil
import unittest
import scanner_task_contracts as module
import source_contracts

ROOT = Path(__file__).resolve().parents[2]


class ScannerTaskControls(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.value = json.loads((ROOT / module.BINDING_PATH).read_text())

    def reject(self, change):
        value = copy.deepcopy(self.value)
        change(value)
        with self.assertRaises((AssertionError, KeyError, ValueError)):
            module.validate(ROOT, value)

    def test_exact_six74_and57(self):
        module.validate(ROOT, self.value)
        self.assertEqual(sum(x['result']['actual_worker_observations'] for x in self.value['non_HTTP_contracts']), 74)
        self.assertEqual(sum(x['result']['received_signals'] for x in self.value['non_HTTP_contracts']), 57)

    def test_candidate_substitution(self):
        self.reject(lambda v: v['non_HTTP_contracts'][0].update(candidate_id='invented'))

    def test_source_line_substitution(self):
        self.reject(lambda v: v['non_HTTP_contracts'][0]['source'].update(line=61))

    def test_source_kind_substitution(self):
        self.reject(lambda v: v['non_HTTP_contracts'][0]['source'].update(kind='model_candidate'))

    def test_task_name_substitution(self):
        self.reject(lambda v: v['non_HTTP_contracts'][0].update(task_name='apps.m3u.tasks.process_groups'))

    def test_task_uuid_substitution(self):
        self.reject(lambda v: v['non_HTTP_contracts'][0]['observations'][0].update(task_id='invented'))

    def test_received_signal_fabrication(self):
        self.reject(lambda v: v['non_HTTP_contracts'][0]['observations'][-1].update(received_signal_retained=True))

    def test_backend_status_change(self):
        self.reject(lambda v: v['non_HTTP_contracts'][0]['observations'][0]['metadata'].update(status='FAILURE'))

    def test_backend_result_change(self):
        self.reject(lambda v: v['non_HTTP_contracts'][0]['observations'][0]['metadata'].update(result='fake'))

    def test_lineage_change(self):
        self.reject(lambda v: v['non_HTTP_contracts'][0]['observations'][0]['prerun'].update(parent_id='foreign'))

    def test_ttl_overclaim(self):
        self.reject(lambda v: v['non_HTTP_contracts'][0]['observations'][0].update(observed_ttl=86400))

    def test_SQL_effect_change(self):
        self.reject(lambda v: v['non_HTTP_contracts'][0]['observations'][0].update(after_sha256='0'*64))

    def test_fixture_execution_kind_change(self):
        self.reject(lambda v: v['fixtures']['children'].update(execution_kind='new TCP replay'))

    def test_received_count_change(self):
        self.reject(lambda v: v.update(received_signals=74))

    def test_unconsumed_child_execution_claim(self):
        self.reject(lambda v: v.update(unconsumed_file_branch_children=0))

    def test_Native_authority_promotion(self):
        self.reject(lambda v: v.update(Native_mapped_parent621='Original executed'))

    def test_full_verification_claim(self):
        self.reject(lambda v: v['non_HTTP_contracts'][0].update(fully_verified=True))

    def test_exception_preserved(self):
        rows = [r for c in self.value['non_HTTP_contracts'] for r in c['observations'] if r['metadata']['status'] == 'FAILURE']
        self.assertTrue(rows)
        self.assertTrue(all(r['metadata']['traceback'] and r['metadata']['result']['exc_type'] for r in rows))

    def test_exact250_application_idempotent(self):
        before = json.loads((ROOT / 'docs/rust-contract-register.json').read_text())['non_HTTP_candidates']
        after = copy.deepcopy(before)
        result = source_contracts.apply_non_HTTP(ROOT, after)
        self.assertEqual(result['candidates'], 8)
        self.assertEqual(before, after)
        self.assertEqual(len(after), 250)
        self.assertTrue(all(not r['fully_verified'] for r in after))

    def test_changed_static_identity_rejected(self):
        rows = json.loads((ROOT / 'docs/rust-contract-register.json').read_text())['non_HTTP_candidates']
        next(x for x in rows if x['candidate_id'] == module.IDENTITIES[0]['candidate_id'])['source']['line'] += 1
        with self.assertRaisesRegex(AssertionError, 'source identity'):
            source_contracts.apply_non_HTTP(ROOT, rows)

    def test_exact_authority_bytes_fail_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            ref = module.AUTHORITIES[0]
            path = root / ref['path']; path.parent.mkdir(parents=True)
            path.write_bytes((ROOT / ref['path']).read_bytes() + b'\n')
            with self.assertRaisesRegex(AssertionError, 'authority bytes'):
                module.validate(root, self.value)

    def test_missing_authority_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(FileNotFoundError):
                module.validate(Path(tmp), self.value)
