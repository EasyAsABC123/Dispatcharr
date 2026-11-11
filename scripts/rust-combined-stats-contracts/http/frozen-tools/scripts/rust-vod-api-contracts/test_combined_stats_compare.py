"""Offline controls for comparison policy; no Django/network/fixtures required."""
import copy
import unittest
import combined_stats_compare as oracle


def row(value):
    return dict(label='owned-control',method='GET',status=200,started_at=100.1,finished_at=100.9,
        raw_header_pairs=[('Content-Type','application/json'),('Allow','GET, OPTIONS')],
        body_bytes=2,body_base64='e30=',body_sha256='fixture-only',json=value)


class Controls(unittest.TestCase):
    def test_identical(self):
        source=row({'x':[1,'a',None]})
        self.assertTrue(oracle.compare(source,copy.deepcopy(source))['passed'])

    def test_integer_float_type_difference_is_not_hidden(self):
        self.assertFalse(oracle.compare(row({'x':0}),row({'x':0.0}))['passed'])

    def test_array_order_is_not_normalized(self):
        self.assertFalse(oracle.compare(row({'x':[1,2]}),row({'x':[2,1]}))['passed'])

    def test_extra_keys_fail(self):
        self.assertFalse(oracle.compare(row({'x':1}),row({'x':1,'y':2}))['passed'])

    def test_independent_timestamp_windows_pass(self):
        self.assertTrue(oracle.compare(row({'timestamp':100.2}),row({'timestamp':100.7}))['passed'])

    def test_out_of_window_timestamp_fails(self):
        self.assertFalse(oracle.compare(row({'timestamp':100.2}),row({'timestamp':101}))['passed'])

    def test_unrelated_timestamp_key_is_not_ignored(self):
        self.assertFalse(oracle.compare(row({'data':{'timestamp':100.2}}),row({'data':{'timestamp':100.7}}))['passed'])

    def test_duplicate_header_is_not_flattened(self):
        source=row({});native=copy.deepcopy(source)
        native['raw_header_pairs'].append(('Vary','origin'))
        native['raw_header_pairs'].append(('Vary','Accept'))
        source['raw_header_pairs'].append(('Vary','Accept, origin'))
        self.assertFalse(oracle.compare(source,native)['passed'])

    def test_exact_observed_allow_variants(self):
        source=row({});native=copy.deepcopy(source)
        native['raw_header_pairs'][-1]=('Allow','OPTIONS, GET')
        result=oracle.compare(source,native)
        self.assertTrue(result['passed'])
        self.assertEqual(len(result['observed_allow_variants']),1)

    def test_invented_head_method_fails(self):
        source=row({});native=copy.deepcopy(source)
        native['raw_header_pairs'][-1]=('Allow','GET, HEAD, OPTIONS')
        self.assertFalse(oracle.compare(source,native)['passed'])

    def test_bad_content_length_fails(self):
        source=row({});native=copy.deepcopy(source)
        native['raw_header_pairs'].append(('Content-Length','3'))
        self.assertFalse(oracle.compare(source,native)['passed'])


if __name__=='__main__':
    unittest.main()
