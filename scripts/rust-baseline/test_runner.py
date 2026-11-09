"""Failure-path checks with a fake Docker executable; starts no containers."""
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

RUNNER = Path(__file__).with_name('run.py')


class RunnerTests(unittest.TestCase):
    def invoke(self, mode):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            docker = root / 'docker'
            calls = root / 'calls.jsonl'
            docker.write_text(f'''#!{sys.executable}
import json, os, signal, sys, time
with open({str(calls)!r}, 'a') as stream:
    stream.write(json.dumps(sys.argv[1:]) + '\\n')
if sys.argv[1] == 'compose':
    if {mode!r} == 'timeout': time.sleep(10)
    if {mode!r} == 'interrupt': os.kill(os.getppid(), signal.SIGINT)
''')
            docker.chmod(0o755)
            output = root / 'evidence'
            path = str(root / 'missing-docker') if mode == 'missing' else str(docker)
            result = subprocess.run([sys.executable, str(RUNNER), '--output', str(output),
                                     '--docker', path, '--timeout', '.1'],
                                    capture_output=True, text=True, timeout=15)
            evidence = json.loads((output / 'run.json').read_text())
            if mode == 'missing':
                self.assertEqual(evidence['failure'], 'FileNotFoundError')
                self.assertEqual(result.returncode, 1)
                self.assertIn('cleanup_error', evidence)
            else:
                expected = 124 if mode == 'timeout' else 130
                self.assertEqual(result.returncode, expected)
                self.assertEqual(evidence['exit_code'], expected)
                invocations = [json.loads(line) for line in calls.read_text().splitlines()]
                self.assertEqual(invocations[-1], ['rm', '-f', evidence['container']])
            self.assertIn('git_archive_sha256', evidence)
            self.assertIn('command', evidence)

    def test_timeout_preserves_evidence_and_removes_only_owned_container(self):
        self.invoke('timeout')

    def test_interrupt_preserves_evidence_and_removes_only_owned_container(self):
        self.invoke('interrupt')

    def test_missing_docker_preserves_failure_evidence(self):
        self.invoke('missing')


if __name__ == '__main__':
    unittest.main()
