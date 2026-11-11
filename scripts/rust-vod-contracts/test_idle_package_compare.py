"""Lossless header controls; synthetic records are verifier tests, not parity proof."""
import copy
import unittest
from idle_package_compare import compare


class LosslessHeaderControls(unittest.TestCase):
    def record(self):
        return {'controlled_idle_entry': {'wire': {
            'response_header_pairs': [['Vary', 'Accept, origin']],
            'response_headers': {'vary': 'Accept, origin', 'allow': 'GET, OPTIONS'}}}}

    def checks(self, native):
        return compare(self.record(), native, {}, False)['checks']

    def test_one_field_matches(self):
        checks = self.checks(self.record())
        self.assertTrue(checks['selected_headers_exact'])
        self.assertTrue(checks['native_single_Source_Vary_field'])

    def test_flattened_dictionary_cannot_hide_duplicate_lines(self):
        native = self.record()
        native['controlled_idle_entry']['wire']['response_header_pairs'] = [
            ['Vary', 'Accept'], ['Vary', 'origin']]
        checks = self.checks(native)
        self.assertFalse(checks['selected_headers_exact'])
        self.assertFalse(checks['native_single_Source_Vary_field'])

    def test_value_order_and_extra_field_are_not_normalized(self):
        for pairs in [[['Vary', 'origin, Accept']],
                      [['Vary', 'Accept, origin'], ['vary', 'Accept, origin']], []]:
            native = copy.deepcopy(self.record())
            native['controlled_idle_entry']['wire']['response_header_pairs'] = pairs
            self.assertFalse(self.checks(native)['selected_headers_exact'])

    def test_allow_observed_variants_do_not_authorize_extra_methods(self):
        for value, expected in [('GET, OPTIONS', True), ('OPTIONS, GET', True),
                                ('GET, HEAD, OPTIONS', False), ('GET,OPTIONS', False)]:
            native = self.record()
            native['controlled_idle_entry']['wire']['response_headers']['allow'] = value
            self.assertEqual(self.checks(native)['native_Allow_is_exact_observed_Source_variant'], expected)


if __name__ == '__main__':
    unittest.main()
