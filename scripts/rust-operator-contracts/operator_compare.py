"""Typed observation comparisons; Original Dispatcharr authors retain credit."""
def typed_equal(left, right):
    """Compare observed JSON types and array order; objects have no SQL JSONB order."""
    if type(left) is not type(right):
        return False
    if isinstance(left, dict):
        return left.keys() == right.keys() and all(typed_equal(value, right[key]) for key, value in left.items())
    if isinstance(left, list):
        return len(left) == len(right) and all(typed_equal(a, b) for a, b in zip(left, right))
    return left == right


def ttl_class_matches(expected, actual):
    """Preserve missing/persistent/positive expiry classes, not elapsed seconds."""
    if type(expected) is not int or type(actual) is not int:
        return False
    return expected == actual if expected < 0 else 0 < actual <= 300
