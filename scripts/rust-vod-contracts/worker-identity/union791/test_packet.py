import copy
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import unittest
from validate import validate, HERE
from reconstruct import reconstruct, verify
from build_peers import compile_bounded


class Controls(unittest.TestCase):
    def test_retained_actual95(self): self.assertEqual(validate()['actual_passed'], 95)

    def test_meaningful_retained_corruptions(self):
        changes = [('regressions.json', lambda x: x['tests'].pop()), ('regressions.json', lambda x: x['tests'][0].update(passed_count=14)), ('regressions.json', lambda x: x['binary_sha256'].pop('http')), ('regressions.json', lambda x: x.update(passed=False)), ('review.json', lambda x: x.update(default_workers_activated=True)), ('review.json', lambda x: x.update(controller_observed_exit_code=1)), ('outside-peers.json', lambda x: x.update(source_map_sha256='0' * 64)), ('independent-cleanup.json', lambda x: x['containers'].pop()), ('independent-cleanup.json', lambda x: x['containers'][0].update(stdout='present')), ('provenance.json', lambda x: x.update(source_revision='wrong'))]
        for name, modify in changes:
            with tempfile.TemporaryDirectory() as temp:
                root = Path(temp) / 'packet'; shutil.copytree(HERE, root)
                value = json.loads((root / name).read_text()); modify(value); (root / name).write_text(json.dumps(value))
                with self.subTest(name=name), self.assertRaises(ValueError): validate(root)
        for name in ['logs/http-tests.log', 'run_regressions.py', 'source-inputs.json', 'coupled-inputs.json']:
            with tempfile.TemporaryDirectory() as temp:
                root = Path(temp) / 'packet'; shutil.copytree(HERE, root); (root / name).write_text('changed')
                with self.subTest(name=name), self.assertRaises(ValueError): validate(root)

    def test_git_only_reconstruction_and_corruption(self):
        repo = Path(os.environ.get('RECONSTRUCTION_REPO', str(HERE.parents[3])))
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / 'source'; self.assertEqual(reconstruct(repo, output)['runtime_inputs'], 791)
            self.assertEqual(verify(output)['coupled_inputs'], 5)
            with self.assertRaises(ValueError): reconstruct(repo, output)
            (output / 'rust/Cargo.toml').write_text('changed')
            with self.assertRaises(ValueError): verify(output)

    def test_owned_compiler_collector_local_python_only(self):
        with tempfile.TemporaryDirectory() as temp:
            paths = compile_bounded([sys.executable, '-c', 'print("local collector only")'], HERE, dict(os.environ), temp, timeout=2)
            self.assertEqual(paths['stdout'].read_text().strip(), 'local collector only')
        for argv, error in [([sys.executable, '-c', 'import time; time.sleep(10)'], TimeoutError), ([sys.executable, '-c', 'import sys; sys.stdout.write("x"*65536);sys.stdout.flush()'], ValueError)]:
            with tempfile.TemporaryDirectory() as temp:
                with self.assertRaises(error): compile_bounded(argv, HERE, dict(os.environ), temp, timeout=.2, limit=1024)


if __name__ == '__main__': unittest.main()
