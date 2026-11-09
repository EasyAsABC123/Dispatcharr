import unittest
from collect_fixture_memory import parse_rollup
class MemoryParserTests(unittest.TestCase):
    def test_private_and_proportional_are_not_rss(self):
        row=parse_rollup('000-fff [rollup]\nRss: 120 kB\nPss: 70 kB\nPrivate_Clean: 10 kB\nPrivate_Dirty: 15 kB\nShared_Clean: 90 kB\n')
        self.assertEqual(row['private_bytes'],25*1024);self.assertEqual(row['pss_bytes'],70*1024);self.assertEqual(row['rss_bytes'],120*1024)
    def test_invalid_or_unbounded_process_samples_fail(self):
        for text in ['Rss: -1 kB\nPss: 2 kB','Rss: 1 bytes\nPss: 2 kB','Rss: 1 kB','x'*65537]:
            with self.assertRaises(ValueError):parse_rollup(text)
