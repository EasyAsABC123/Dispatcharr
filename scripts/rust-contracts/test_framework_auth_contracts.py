"""Pure rejection controls for scoped Original framework-test assignments."""
import ast
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import tempfile
import unittest

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
spec = importlib.util.spec_from_file_location('_framework_auth_contracts', HERE / 'framework_auth_contracts.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
ORIGINAL = json.loads((HERE / 'framework-auth/bindings.json').read_text())


class Controls(unittest.TestCase):
    def reject(self, change):
        value = copy.deepcopy(ORIGINAL)
        change(value)
        with self.assertRaises((AssertionError, KeyError, StopIteration)):
            module.validate(ROOT, value)

    def test_exact_historical_assignment(self):
        self.assertEqual(module.validate(ROOT, ORIGINAL)['historical_Original_tests'], 18)

    def test_original_request_role(self):
        self.reject(lambda v: v['contracts'][0]['source_role_specs'].update(anonymous={'user_level': 10}))

    def test_unresolved_method_not_invented(self):
        self.reject(lambda v: v['contracts'][0].update(method='GET'))

    def test_wrong_handler(self):
        self.reject(lambda v: v['contracts'][0].update(source_handler='apps.accounts.api_views.TokenObtainPairView'))

    def test_wrong_owner(self):
        self.reject(lambda v: v['contracts'][0].update(owning_milestone='M4'))

    def test_duplicate_operation(self):
        self.reject(lambda v: v['contracts'].__setitem__(1, copy.deepcopy(v['contracts'][0])))

    def test_missing_test(self):
        self.reject(lambda v: v['contracts'][0]['cases'].pop())

    def test_wrong_executable_method(self):
        self.reject(lambda v: v['contracts'][0]['cases'][0].update(method='test_not_a_real_assertion'))

    def test_wrong_source_ast(self):
        self.reject(lambda v: v['contracts'][0]['cases'][0].update(source_ast_sha256='0' * 64))

    def test_missing_original_assertion(self):
        self.reject(lambda v: v['contracts'][0]['cases'][0]['assertion_expressions'].pop())

    def test_skipped_is_not_pass(self):
        def change(value):
            case = value['contracts'][0]['cases'][0]
            case['framework_log_block'] = case['framework_log_block'].replace('... ok', "... skipped 'fixture missing'")
            case['framework_log_block_sha256'] = hashlib.sha256(case['framework_log_block'].encode()).hexdigest()
        self.reject(change)

    def test_new_execution_not_fabricated(self):
        self.reject(lambda v: v['contracts'][0]['result'].update(new_Source_cases_this_task=1))

    def test_bool_not_zero_execution_count(self):
        self.reject(lambda v: v.update(fresh_Source_case_count=False))

    def test_native_completion_not_fabricated(self):
        self.reject(lambda v: v.update(Native_coverage_increased=True))

    def test_operation_completion_not_fabricated(self):
        self.reject(lambda v: v['contracts'][0].update(fully_verified=True))

    def test_wrong_unsafe_fixture_command(self):
        self.reject(lambda v: v['fixtures']['framework-auth18']['execution'].update(entrypoint='production.py'))

    def test_rehashed_original_source_replacement_rejected(self):
        self.reject(lambda v: v['original_test_files'][0].update(sha256='0' * 64))

    def test_wrong_baseline_authority(self):
        self.reject(lambda v: v['authorities'][0].update(sha256='0' * 64))

    def test_missing_authority_has_no_fallback(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            shutil.copytree(HERE / 'framework-auth', root / 'scripts/rust-contracts/framework-auth')
            (root / ORIGINAL['authorities'][0]['path']).unlink()
            with self.assertRaises(FileNotFoundError):
                module.validate(root, ORIGINAL)

    def test_corrupt_source_file_has_no_fallback(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            shutil.copytree(HERE / 'framework-auth', root / 'scripts/rust-contracts/framework-auth')
            path = root / ORIGINAL['original_test_files'][0]['path']
            path.chmod(0o600)  # Only the owned temporary corruption copy.
            path.write_bytes(path.read_bytes() + b'\n# changed\n')
            with self.assertRaisesRegex(AssertionError, 'unmodified Original'):
                module.validate(root, ORIGINAL)


    def test_empty_fields_and_none_are_preserved(self):
        node = ast.parse('def f(): pass').body[0].args
        fields = dict(module.stable_ast(node)['fields'])
        self.assertEqual(fields['args'], [])
        self.assertEqual(fields['vararg'], {'scalar': 'NoneType', 'value': None})

    def test_only_absent_empty_generic_field_reconciled(self):
        node = ast.parse('def f(): pass').body[0]
        initial = module.ast_fingerprint(node)
        node._fields = tuple(name for name in node._fields if name != 'type_params')
        self.assertEqual(module.ast_fingerprint(node), initial)
        node._fields = node._fields + ('type_params',)
        node.type_params = []
        self.assertEqual(module.ast_fingerprint(node), initial)

    def test_nonempty_generic_field_rejected(self):
        node = ast.parse('def f(): pass').body[0]
        if 'type_params' not in node._fields:
            node._fields = node._fields + ('type_params',)
        node.type_params = [ast.Name(id='Unqualified', ctx=ast.Load())]
        with self.assertRaisesRegex(ValueError, 'NonemptyGeneric'):
            module.ast_fingerprint(node)


if __name__ == '__main__':
    unittest.main()
