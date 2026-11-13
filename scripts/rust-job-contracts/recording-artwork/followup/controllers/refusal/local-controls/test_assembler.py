"""Finite host/guest refusal wiring checks; no Docker/process/socket fixtures."""
import ast
import builtins
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import runpy
import symtable
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
import test_contract as contract_tests


class Assembly(unittest.TestCase):
    def test_exact_mounted_tools_and_input_hashes(self):
        tree = ast.parse((ROOT / 'tools/run_source.py').read_text())
        required = next(ast.literal_eval(n.value) for n in tree.body if isinstance(n, ast.Assign) and any(isinstance(v, ast.Name) and v.id == 'REQUIRED' for v in n.targets))
        actual = {p.name for p in (ROOT / 'tools').iterdir() if p.is_file() and p.suffix in ('.py', '.json')}
        self.assertEqual(required, actual)
        self.assertEqual(len(required), 18)
        for i in range(1, 5):
            value = json.loads((ROOT / f'historical-proposed-inputs-{i}.json').read_text())
            self.assertIs(value['authorized'], False)
            self.assertEqual(set(value['tools_sha256']), required)
            for name in required:
                self.assertEqual(hashlib.sha256((ROOT / 'tools' / name).read_bytes()).hexdigest(), value['tools_sha256'][name])

    def test_guest_has_no_unresolved_global_constructor(self):
        text = (ROOT / 'tools/source_configured.py').read_text()
        table = symtable.symtable(text, 'guest', 'exec')
        declared = set(table.get_identifiers()) | set(dir(builtins))
        declared.update(runpy.run_path(str(ROOT / 'tools/seed_spec.py')))
        function = next(t for t in table.get_children() if t.get_name() == 'physical_artwork')
        unknown = {s.get_name() for s in function.get_symbols() if s.is_global() and s.is_referenced() and s.get_name() not in declared}
        self.assertEqual(unknown, set())

    def test_both_missing_snapshot_fields_rejected(self):
        c = contract_tests.Controls(); c.setUp()
        c.before.pop('files'); c.after.pop('files')
        with self.assertRaises(ValueError): c.accept()

    def test_only_four_distinct_fresh_owned_outputs(self):
        plan = json.loads((ROOT / 'historical-once-plan.json').read_text())
        self.assertEqual(len(plan['cases']), 4)
        self.assertEqual(len({c['output'] for c in plan['cases']}), 4)
        self.assertEqual({c['mode'] for c in plan['cases']}, {'missing-recording', 'duplicate-casefold-movie', 'oversized-recording-properties', 'nonlocal-logo'})
        self.assertEqual(plan['maximum_own_projects'], 1)

    def test_no_proxy_or_normal_replay_in_refusal_guest(self):
        text = (ROOT / 'tools/source_configured.py').read_text()
        self.assertNotIn('PGProxy(', text)
        self.assertNotIn('RedisProxy(', text)
        self.assertNotIn("phase(CASES[3]", text)
        self.assertIn('mutate_owned(mode,context,connection,Recording,Movie,VODLogo)', text)

    def test_unauthorized_host_stops_before_tooling_or_output_claim(self):
        spec = importlib.util.spec_from_file_location('owned_refusal_host', ROOT / 'tools/run_source.py')
        host = importlib.util.module_from_spec(spec); spec.loader.exec_module(host)
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); inputs = base / 'inputs.json'; inputs.write_text('{"authorized":false}')
            output = base / 'uncreated'
            argv = ['run_source.py', '--repo', directory, '--support-root', directory, '--output', str(output), '--execution-inputs', str(inputs)]
            with patch.object(sys, 'argv', argv), patch.object(host.shutil, 'which', side_effect=AssertionError('No Docker executable lookup')):
                with self.assertRaisesRegex(ValueError, 'ExplicitRootParentedPairAuthority'): host.main()
            self.assertFalse(output.exists())

    def test_fixed_actual_runtime_and_process_budgets(self):
        plan = json.loads((ROOT / 'case-plan.json').read_text())
        self.assertEqual((plan['case_seconds'], plan['guest_seconds'], plan['cleanup_seconds'], plan['per_process_seconds']), (90, 600, 90, 35))
        value = json.loads((ROOT / 'historical-proposed-execution-inputs.json').read_text())
        self.assertEqual(value['Native_elf']['sha256'], '9e3b613caad7a7d56c02fe3720ac971f527b276d9e2ea916851469193564e1ce')


if __name__ == '__main__':
    unittest.main()
