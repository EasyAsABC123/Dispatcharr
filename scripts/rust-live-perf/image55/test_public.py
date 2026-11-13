"""Run the same complete-matrix controls using only published observations."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


class PublicEvidence(unittest.TestCase):
    def test_complete_matrix_controls(self):
        here = Path(__file__).resolve().parent
        data = json.loads((here / 'projection.json').read_text())
        with tempfile.TemporaryDirectory(prefix='dispatcharr-live-controls-') as directory:
            root = Path(directory)
            tools = root / 'tools/scripts/rust-live-perf'
            tools.mkdir(parents=True)
            (root / 'live-functional.json').write_text(json.dumps(data['report']))
            (tools / 'protocol.json').write_text(json.dumps(data['protocol'], indent=2) + '\n')
            result = subprocess.run(
                [sys.executable, str(here / 'test_verify.py')],
                env=dict(os.environ, LIVE_RETAINED_COMPLETE_RUN=str(root)),
                capture_output=True, text=True, timeout=60,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn('Ran 30 tests', result.stderr)
            self.assertNotIn('skipped', result.stderr)


if __name__ == '__main__':
    unittest.main(verbosity=2)
