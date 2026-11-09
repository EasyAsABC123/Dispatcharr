import unittest
from run import decode_json_lines


class FramingTests(unittest.TestCase):
    def test_unicode_line_separators_remain_inside_json_strings(self):
        self.assertEqual(decode_json_lines('{"v":"\u0085"}\n{"v":"\u2028\u2029"}\n'),
                         [{'v': '\u0085'}, {'v': '\u2028\u2029'}])


if __name__ == '__main__':
    unittest.main()
