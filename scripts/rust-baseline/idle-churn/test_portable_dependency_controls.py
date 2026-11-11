"""Public sampler dependency must exist and match before any Docker operation."""
import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock
HERE=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('m0_public_kernel_dependency',HERE/'kernel_denial_control.py')
kernel=importlib.util.module_from_spec(spec)
spec.loader.exec_module(kernel)

class DependencyControls(unittest.TestCase):
    def test_changed_public_sampler_refused_before_docker(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);dependency=root/'scripts/rust-guide-perf/docker_stats.py'
            dependency.parent.mkdir(parents=True);dependency.write_text('# changed\n')
            with mock.patch.object(kernel,'ROOT',root),mock.patch.object(sys,'argv',['kernel','--output',str(root/'out')]),mock.patch.object(kernel.subprocess,'run') as run:
                with self.assertRaisesRegex(ValueError,'sampler authority changed'):
                    kernel.main()
                run.assert_not_called()

    def test_missing_public_sampler_has_no_private_fallback(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary)
            with mock.patch.object(kernel,'ROOT',root),mock.patch.object(sys,'argv',['kernel','--output',str(root/'out')]),mock.patch.object(kernel.subprocess,'run') as run:
                with self.assertRaises(FileNotFoundError):
                    kernel.main()
                run.assert_not_called()
