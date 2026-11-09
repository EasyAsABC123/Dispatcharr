import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import ts_discontinuity as parity


class ReferenceTests(unittest.TestCase):
    def test_modified_reference_is_rejected_before_execution(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            helper = root / 'apps/proxy/live_proxy/input/ts_discontinuity.py'
            helper.parent.mkdir(parents=True)
            helper.write_text('raise AssertionError("reference code executed")\n')
            with patch.object(parity, 'ROOT', root):
                with self.assertRaisesRegex(RuntimeError, 'pinned compatibility baseline'):
                    parity.load_legacy()

    def test_pinned_reference_loads_without_django_startup(self):
        helper = parity.load_legacy()
        self.assertEqual(helper.TS_PACKET_SIZE, 188)
        self.assertEqual(helper.set_discontinuity_indicator(b'bad'), b'bad')


if __name__ == '__main__':
    unittest.main()
