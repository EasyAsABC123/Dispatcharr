import unittest
from operator_compare import typed_equal, ttl_class_matches


class TypedObservationControls(unittest.TestCase):
    def test_boolean_integer_and_float_are_distinct(self):
        for left, right in [(True, 1), (False, 0), (1, 1.0), ({'id': 1}, {'id': True})]:
            self.assertFalse(typed_equal(left, right))

    def test_nested_property_types_and_array_order_survive(self):
        original = {'keep': {'typed': [1, True, None]}}
        self.assertTrue(typed_equal(original, {'keep': {'typed': [1, True, None]}}))
        self.assertFalse(typed_equal(original, {'keep': {'typed': [True, 1, None]}}))
        self.assertFalse(typed_equal([1, 2], [2, 1]))

    def test_missing_keys_are_not_null_or_empty_objects(self):
        self.assertFalse(typed_equal({'a': None}, {}))
        self.assertFalse(typed_equal([], {}))
        self.assertFalse(typed_equal(None, {}))

    def test_jsonb_object_field_order_is_structural(self):
        self.assertTrue(typed_equal({'a': 1, 'b': 2}, {'b': 2, 'a': 1}))

    def test_cache_expiry_classes_are_not_collapsed(self):
        for expected, actual in [(-2, -1), (-2, 10), (-1, -2), (-1, 10), (299, -1), (299, -2), (299, 0), (299, 301), (1, True)]:
            self.assertFalse(ttl_class_matches(expected, actual))
        for expected, actual in [(-2, -2), (-1, -1), (298, 299), (299, 1)]:
            self.assertTrue(ttl_class_matches(expected, actual))


if __name__ == '__main__':
    unittest.main()
